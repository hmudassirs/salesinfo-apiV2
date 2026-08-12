"""Composition root wiring the domain repositories/services together
around one shared ApplicationStateStore connection.

This file intentionally contains no business logic of its own — it is
the one place that knows every domain service exists and hands them
out as attributes (application_services.api_keys, .users, .logging, ...).
Each individual service now lives with its own domain (auth/,
observability/, caching/) instead of all six being defined in one
`application_services.py`, which is what made this hard to navigate before.
"""

from typing import Any, Dict

from core.auth.api_key_repository import APIKeyRepository
from core.auth.user_repository import UserRepository
from core.caching.query_cache_coordinator import QueryCacheCoordinator
from core.caching.query_result_cache import QueryResultCache
from core.db.logger import get_logger
from core.observability.audit import AuditTrail
from core.observability.request_logger import RequestLogger
from core.observability.request_tracer import RequestTracer
from core.services.application_diagnostics import ApplicationDiagnostics
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)


class ApplicationServices:
    """Groups domain services (users, api keys, logging, tracing, caching,
    audit) around a single application state store connection.

    ApplicationServices does not connect, create tables, or disconnect. It
    expects an already-connected ApplicationStateStore and simply wires up the
    domain services on top of it. Connection lifecycle is owned by
    ApplicationLifespan (see lifespan.py's ApplicationStateStep).
    """

    def __init__(self, application_state: ApplicationStateStore):
        """Initialize application services.

        Args:
            application_state: An already-connected ApplicationStateStore instance.
        """
        self.application_state = application_state
        self.api_keys = APIKeyRepository(self.application_state)
        self.users = UserRepository(self.application_state)
        self.logging = RequestLogger(self.application_state)
        self.tracing = RequestTracer(self.application_state)
        self.caching = QueryResultCache(self.application_state)
        # L1 in-process cache + single-flight coalescing in front of
        # `self.caching` (the L2/application-state cache) -- see
        # query_cache_coordinator.py's module docstring. Routes should
        # go through this, not `self.caching` directly, for anything on
        # the request hot path.
        self.query_cache = QueryCacheCoordinator(self.caching)
        self.audit = AuditTrail(self.application_state)
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
