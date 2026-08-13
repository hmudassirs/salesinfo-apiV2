"""Owns the application state store and the ApplicationServices built
on top of it.
"""

import asyncio
import functools
import logging
from typing import Any, Dict, Optional

from core.app.lifecycle.base import LifecycleStep
from core.app.settings import AppSettings
from core.application_services import ApplicationServices
from core.auth.admin_bootstrap import AdminBootstrapService
from core.caching.cache_maintenance import CacheMaintenance
from core.observability.context import write_observability_record
from core.observability.write_queue import ObservabilityWriteQueue
from core.storage.application_state_store import ApplicationStateStore
from core.storage.schema import ApplicationStateSchema

logger = logging.getLogger(__name__)


class ApplicationStateStep(LifecycleStep):
    """Owns the application state store (api keys, users, logging, tracing,
    caching, audit) and the ApplicationServices built on top of it.

    The service tables live in the same PostgreSQL database as the
    application data store (`ApplicationStateStore.for_postgres`, using
    `db_config.extra_options` -- the same host/port/database/user/
    password ApplicationDataStep connects with) -- see
    core.storage.application_state_store's module docstring. Table creation is
    idempotent (`CREATE TABLE IF NOT EXISTS`) and safe to run whether
    or not ApplicationDataStep's own migration pass has already applied
    `migrations/postgresql/0002_application_state.sql` --
    whichever step runs first creates the tables, the other is a no-op
    against an already-current schema.

    ApplicationStateStore itself only knows connect/execute/fetch/
    transaction; schema creation, admin bootstrap, and cache cleanup are
    each a separate collaborator (ApplicationStateSchema,
    AdminBootstrapService, CacheMaintenance) that this step is what
    decides *when* to run, alongside handing the resulting domain
    services to the container.
    """

    name = "application state store"

    def __init__(
        self,
        db_config,
        *,
        settings: Optional[AppSettings] = None,
        pool_min_size: int = 2,
        pool_max_size: int = 8,
    ):
        self.db_config = db_config
        # Threaded through to ApplicationServices, which uses it to
        # build authentication_service (JWT secret/algorithm/expiry).
        # See ApplicationServices.__init__'s docstring for the
        # AppSettings.from_env() fallback when this is left None.
        self.settings = settings
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.application_state: Optional[ApplicationStateStore] = None
        self.application_services: Optional[ApplicationServices] = None
        self.observability_queue: Optional[ObservabilityWriteQueue] = None

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
        AdminBootstrapService(self.application_state).initialize()
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

        return {
            "application_state": self.application_state,
            "application_services": self.application_services,
            "query_cache": self.application_services.caching,
            "request_logger": self.application_services.logging,
            "request_tracer": self.application_services.tracing,
            "audit_trail": self.application_services.audit,
            "api_key_repository": self.application_services.api_keys,
            "user_repository": self.application_services.users,
            "api_key_service": self.application_services.api_key_service,
            "authentication_service": self.application_services.authentication_service,
        }

    async def startup_async(self) -> Dict[str, Any]:
        # ApplicationStateStore's psycopg2 connections are blocking; run the
        # real startup off the event loop thread rather than faking
        # async support for it.
        return await asyncio.to_thread(self.startup_sync)

    def shutdown_sync(self) -> None:
        if self.observability_queue:
            # Stop the flush thread and write anything still queued —
            # otherwise the last batch of request logs before shutdown
            # would silently be lost.
            self.observability_queue.stop()
        if self.application_state:
            self.application_state.disconnect()

    async def shutdown_async(self) -> None:
        if self.observability_queue:
            await asyncio.to_thread(self.observability_queue.stop)
        if self.application_state:
            await asyncio.to_thread(self.application_state.disconnect)
