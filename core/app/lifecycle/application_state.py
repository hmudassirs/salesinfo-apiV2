"""Owns the application state store and the ApplicationServices built
on top of it.
"""

import asyncio
import contextlib
import functools
import logging
from typing import Any, Dict, Optional

from core.app.lifecycle.base import LifecycleStep
from core.app.settings import AppSettings
from core.application_services import ApplicationServices
from core.auth.admin_bootstrap import AdminBootstrapService
from core.caching.cache_maintenance import CacheMaintenance
from core.concurrency.executors import run_in_state_executor
from core.observability.context import write_observability_record
from core.observability.write_queue import ObservabilityWriteQueue
from core.services.maintenance_service import ApplicationMaintenanceService
from core.storage.application_state_store import ApplicationStateStore
from core.storage.schema import ApplicationStateSchema

logger = logging.getLogger(__name__)


class ApplicationStateStep(LifecycleStep):
    """Owns the application state store (api keys, users, logging, tracing,
    caching, audit), the ApplicationServices built on top of it, and a
    periodic background maintenance sweep.

    The service tables live in the same PostgreSQL database as the
    application data store (`ApplicationStateStore.for_postgres`, using
    `db_config.extra_options` -- the same host/port/database/user/
    password ApplicationDataStep connects with) -- see
    core.storage.application_state_store's module docstring. Table creation
    (`ApplicationStateSchema.create()`, called below) is the sync-mode/
    standalone fallback migration path, not the primary one -- in the
    async mode this application runs in, `ApplicationDataStep` (which
    always starts before this step -- see `core.app.lifecycle.manager`'s
    fixed step order) has already applied the same migrations. Both
    paths are idempotent, so calling this here is always safe.

    ApplicationStateStore itself only knows connect/execute/fetch/
    transaction; schema creation, admin bootstrap, and cache cleanup are
    each a separate collaborator (ApplicationStateSchema,
    AdminBootstrapService, CacheMaintenance) that this step is what
    decides *when* to run, alongside handing the resulting domain
    services to the container.

    Scheduled maintenance (`core.services.maintenance_service
    .ApplicationMaintenanceService.cleanup()` -- expired cache entries,
    old logs, old traces, VACUUM) runs here too, on a fixed interval
    for the lifetime of the app, rather than once at startup: startup
    is when there's the *least* to clean up (a process that's been
    running and serving requests is what accumulates old logs/traces/
    cache in the first place), and a one-shot cleanup at boot would
    mean everything generated since the process last cleaned up stays
    unbounded until the next restart -- which, for a long-lived
    process, could be a very long time. See `_run_maintenance_loop`.
    Controlled by `MAINTENANCE_ENABLED` (default on) and
    `MAINTENANCE_INTERVAL_SECONDS` (default 86400 = 24h); only runs in
    async mode (see `_run_maintenance_loop`'s docstring for why).
    """

    name = "application state store"

    def __init__(
        self,
        db_config,
        *,
        settings: AppSettings,
        pool_min_size: int = 2,
        pool_max_size: int = 8,
    ):
        self.db_config = db_config
        # Required, not Optional: this is an application integration
        # component (core.app.lifecycle), not a standalone reusable
        # package -- see the framework review's "standalone classes
        # versus application classes" distinction, and
        # `PerformanceStep`'s constructor for the same reasoning.
        # Threaded through to ApplicationServices, which uses it to
        # build authentication_service (JWT secret/algorithm/expiry).
        # There used to be an `if settings is not None else
        # os.getenv(...)` fallback here; every real construction site
        # (core.app.lifecycle.manager.ApplicationLifespan, which also
        # now requires settings) always passed one anyway, so the
        # fallback path was dead in production and only weakened the
        # "AppSettings.from_env() is the one place that reads the
        # environment" invariant. Tests that want standalone behavior
        # now construct an explicit `AppSettings(...)` instead.
        self.settings = settings
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.maintenance_enabled = settings.maintenance_enabled
        self.maintenance_interval_seconds = settings.maintenance_interval_seconds
        self.application_state: Optional[ApplicationStateStore] = None
        self.application_services: Optional[ApplicationServices] = None
        self.observability_queue: Optional[ObservabilityWriteQueue] = None
        self._maintenance_task: Optional["asyncio.Task[None]"] = None

    def _build_application_state(self) -> Optional[ApplicationStateStore]:
        if not self.db_config:
            return None
        return ApplicationStateStore.for_postgres(
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
            **self.db_config.extra_options,
        )

    def startup_sync(self) -> Dict[str, Any]:
        self.application_state = self._build_application_state()
        if self.application_state is None:
            return {}

        self.application_state.connect()
        ApplicationStateSchema(self.application_state).create()
        AdminBootstrapService(self.application_state, settings=self.settings).initialize()
        CacheMaintenance(self.application_state).cleanup_expired_cache()

        self.application_services = ApplicationServices(
            self.application_state, settings=self.settings
        )

        # Background flush queue for request logging/tracing/audit — see
        # core/observability/write_queue.py's docstring for why this
        # exists: batching writes into one transaction per request
        # still meant every request holding a pooled connection open
        # for its own write. This takes the write out of the request
        # path entirely.
        self.observability_queue = ObservabilityWriteQueue(
            self.application_state,
            write_record=functools.partial(
                write_observability_record, self.application_services
            ),
        )
        self.observability_queue.start()
        # emit_request_observability() looks for this attribute on
        # application_services to decide whether to enqueue (fast path) or
        # write synchronously (fallback, e.g. in tests).
        self.application_services.observability_queue = self.observability_queue
        # Same queue, wired into AuditTrail directly too -- so
        # `log_audit_event()` calls made *outside* a full request-
        # observability record (AuthenticationService.authenticate's
        # login-audit call, in particular) also enqueue instead of
        # blocking the caller on a synchronous write. See
        # AuditTrail.set_queue's docstring.
        self.application_services.audit.set_queue(self.observability_queue)

        # Only application_state/application_services are registered on
        # the container -- everything else here (query cache, request
        # logger/tracer, audit trail, api key/user repositories, the
        # api key/authentication services) is already reachable as
        # `application_services.<name>` (see core/application_services.py).
        # A second, duplicate top-level container field per service was
        # removed as consistency debt -- see ApplicationContainer's
        # docstring.
        return {
            "application_state": self.application_state,
            "application_services": self.application_services,
        }

    async def startup_async(self) -> Dict[str, Any]:
        # ApplicationStateStore's psycopg2 connections are blocking; run the
        # real startup off the event loop thread rather than faking
        # async support for it.
        registrations = await asyncio.to_thread(self.startup_sync)
        if registrations and self.maintenance_enabled:
            self._maintenance_task = asyncio.ensure_future(self._run_maintenance_loop())
            logger.info(
                "Scheduled maintenance started (interval=%.0fs)",
                self.maintenance_interval_seconds,
            )
        return registrations

    async def _run_maintenance_loop(self) -> None:
        """Run `ApplicationMaintenanceService.cleanup()` every
        `maintenance_interval_seconds`, for the lifetime of the app.

        Only started from `startup_async` (never `startup_sync`): sync
        mode (tests, scripts, `LifecycleMode.SYNC`) has no running
        event loop to host a background task on -- the same constraint
        `PerformanceStep`'s docstring notes for its own background
        loops. `cleanup()` does blocking psycopg2 I/O (including a
        `VACUUM`, which can take real time on a large table), so each
        run is offloaded via `run_in_state_executor` rather than
        called directly on the event loop.

        Sleeps first, then cleans up -- not the other way around -- so
        a freshly-started process doesn't immediately spend a `VACUUM`
        on a database that (by definition) has had at most one
        interval's worth of activity since the last cleanup. Cancellation
        (on shutdown) is the normal, expected way this loop ends.
        """
        maintenance = ApplicationMaintenanceService(self.application_state)
        while True:
            await asyncio.sleep(self.maintenance_interval_seconds)
            try:
                result = await run_in_state_executor(maintenance.cleanup)
                logger.info("Scheduled maintenance cleanup completed: %s", result)
            except Exception:
                logger.warning("Scheduled maintenance cleanup failed", exc_info=True)

    def shutdown_sync(self) -> None:
        if self.observability_queue:
            # Stop the flush thread and write anything still queued —
            # otherwise the last batch of request logs before shutdown
            # would silently be lost.
            self.observability_queue.stop()
        if self.application_state:
            self.application_state.disconnect()

    async def shutdown_async(self) -> None:
        if self._maintenance_task is not None:
            self._maintenance_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._maintenance_task
            self._maintenance_task = None
        if self.observability_queue:
            await asyncio.to_thread(self.observability_queue.stop)
        if self.application_state:
            await asyncio.to_thread(self.application_state.disconnect)
