"""Composition root wiring the domain repositories/services together
around one shared ApplicationStateStore connection.

This file intentionally contains no business logic of its own — it is
the one place that knows every domain service exists and hands them
out as attributes (application_services.api_keys, .users, .logging, ...).
Each individual service now lives with its own domain (auth/,
observability/, caching/) instead of all six being defined in one
`application_services.py`, which is what made this hard to navigate before.

Also constructs the request-facing orchestration services
(`api_key_service`, `authentication_service`) once here, rather than
per-request in `core.app.api.dependencies` -- they're stateless aside
from the repository/settings references they're built from, so there's
nothing to gain from rebuilding them on every request, and building
them here means "what services exist" has exactly one place to look,
matching every repository/service already listed above.
"""

from typing import Any, Dict, Optional

from core.app.settings import AppSettings
from core.auth.api_key_repository import APIKeyRepository
from core.auth.api_key_service import APIKeyService
from core.auth.authentication_service import AuthenticationService
from core.auth.user_repository import UserRepository
from core.caching.query_cache_coordinator import QueryCacheCoordinator
from core.caching.query_result_cache import QueryResultCache
from core.db.logger import get_logger
from core.observability.audit import AuditTrail
from core.observability.request_logger import RequestLogger
from core.observability.request_tracer import RequestTracer
from core.performance.adapters.application_state import InstrumentedApplicationStateStore
from core.services.application_diagnostics import ApplicationDiagnostics
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)


class ApplicationServices:
    """Groups domain services (users, api keys, logging, tracing, caching,
    audit) and the request-facing services built on top of them
    (api_key_service, authentication_service) around a single
    application state store connection.

    ApplicationServices does not connect, create tables, or disconnect. It
    expects an already-connected ApplicationStateStore and simply wires up the
    domain services on top of it. Connection lifecycle is owned by
    ApplicationLifespan (see lifespan.py's ApplicationStateStep).
    """

    def __init__(
        self,
        application_state: ApplicationStateStore,
        settings: Optional[AppSettings] = None,
    ):
        """Initialize application services.

        Args:
            application_state: An already-connected ApplicationStateStore instance.
            settings: AppSettings used to build `authentication_service`
                (JWT secret/algorithm/expiry). Falls back to
                `AppSettings.from_env()` when not given -- mirrors
                `core.app.api.app.create_app`'s own fallback, so a
                caller that doesn't have settings on hand yet (a
                script, a test) still gets a working instance rather
                than a `None` attribute to guard against everywhere
                else.
        """
        self.application_state = application_state
        # Every domain service below is built on this *instrumented*
        # wrapper, not `self.application_state` directly -- a no-op
        # passthrough when core.performance is disabled or this request
        # wasn't sampled (see InstrumentedApplicationStateStore._timed),
        # but when active it times each SQL call and tags it with the
        # statement text onto the current request's performance
        # profiler. That's what lets
        # core.observability.context.build_request_context populate a
        # request's trace record with real db_query/db_duration_ms
        # values instead of the hardcoded empty placeholders it used to
        # -- see that function's docstring. Administrative code
        # (ApplicationDiagnostics, AdminBootstrapService,
        # ApplicationStateSchema, ApplicationMaintenanceService, the
        # ObservabilityWriteQueue's own background-thread writes, ...)
        # uses the raw `self.application_state` instead: there's no
        # per-request trace to attribute a startup or background task's
        # queries to.
        instrumented_state = InstrumentedApplicationStateStore(application_state)
        self.api_keys = APIKeyRepository(instrumented_state)
        self.users = UserRepository(instrumented_state)
        self.logging = RequestLogger(instrumented_state)
        self.tracing = RequestTracer(instrumented_state)
        self.caching = QueryResultCache(instrumented_state)
        # L1 in-process cache + single-flight coalescing in front of
        # `self.caching` (the L2/application-state cache) -- see
        # query_cache_coordinator.py's module docstring. Routes should
        # go through this, not `self.caching` directly, for anything on
        # the request hot path.
        self.query_cache = QueryCacheCoordinator(self.caching)
        self.audit = AuditTrail(instrumented_state)

        settings = settings or AppSettings.from_env()
        self.api_key_service = APIKeyService(self.api_keys, audit=self.audit)
        self.authentication_service = AuthenticationService(
            self.users, settings, audit=self.audit
        )

        # Set by ApplicationLifespan's ApplicationStateStep after construction,
        # once the background flush thread is actually running. None here
        # means "no queue yet" — emit_request_observability() falls back to
        # writing synchronously in that case (see core/observability/context.py).
        self.observability_queue = None

    # Scheduled/background maintenance (expired cache, old logs/traces,
    # VACUUM) used to live here as `cleanup()`. That's maintenance
    # policy, not composition -- see this module's docstring -- so it
    # moved to core.services.maintenance_service.ApplicationMaintenanceService.
    # Construct one directly from `.application_state` when a periodic
    # maintenance job needs to run:
    #
    #   ApplicationMaintenanceService(application_services.application_state).cleanup()

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive service statistics.

        Returns:
            Service statistics
        """
        stats = ApplicationDiagnostics(self.application_state).table_stats()
        stats.update(self.caching.get_cache_stats())
        return stats
