"""Application-state diagnostics: table counts and database size.

Extracted out of `ApplicationStateStore.get_stats()`. That method knew
the application's table names (`api_keys`, `users`, `logs`, `traces`,
`query_cache`, `audit_log`) -- domain schema the store's docstring
explicitly says it shouldn't know, the same class of leak
`create_tables()`/`initialize_admin_user()`/`cleanup_expired_cache()`
had before Step 3 moved those out (see `core.storage.schema`,
`core.auth.admin_bootstrap`, `core.caching.cache_maintenance`).
`ApplicationStateStore` itself now only has to know connect/disconnect/
execute/fetch/transaction; this is what to run against those
primitives, not a primitive itself.

Lives in `core.services` (alongside
`core.services.maintenance_service.ApplicationMaintenanceService`,
another periodic/operational concern over the same store) rather than
`core.storage`, since it's application-schema-aware diagnostics, not a
generic storage capability.
"""

from typing import Any, Dict

from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)

# Tables this diagnostics pass reports counts for. Kept as an explicit
# list (rather than introspecting information_schema) so a stats call
# never surprises the caller with an unexpected table showing up, and
# so the set of "the app's own tables" is declared in exactly one
# place a reader can find it.
_APPLICATION_TABLES = ("api_keys", "users", "logs", "traces", "query_cache", "audit_log")


class ApplicationDiagnostics:
    """Table counts and overall database size for the application state
    store -- used by `ApplicationServices.get_stats()` for a
    /health or /debug-style endpoint, not on any request hot path.
    """

    def __init__(self, application_state: ApplicationStateStore):
        self.application_state = application_state

    def table_stats(self) -> Dict[str, Any]:
        """Row counts for each application table, plus the shared
        database's overall size.

        Returns:
            Dictionary with `<table>_count` per table in
            `_APPLICATION_TABLES`, plus `db_size_bytes`.
        """
        stats: Dict[str, Any] = {}

        for table in _APPLICATION_TABLES:
            result = self.application_state.fetch_one(
                f"SELECT COUNT(*) as count FROM {table}"
            )
            stats[f"{table}_count"] = result["count"] if result else 0

        # Database size. There's no single file to stat, and it's
        # shared with the application data's own tables, so report the
        # whole database's size.
        size_result = self.application_state.fetch_one(
            "SELECT pg_database_size(current_database()) AS size"
        )
        stats["db_size_bytes"] = size_result["size"] if size_result else 0

        return stats
