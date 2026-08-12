"""Scheduled/background maintenance over the application state store.

Extracted out of `ApplicationServices.cleanup()` (see that class's
docstring): `ApplicationServices` is a composition root and claims to
contain no business logic, but its `cleanup()` method ran cache
cleanup, log/trace deletion, and a VACUUM directly. That's maintenance
policy, not wiring, so it lives here instead -- a plain domain service,
same pattern as `QueryService`/`AuthenticationService`, just for
periodic housekeeping rather than a request path.

Nothing currently schedules this automatically (there was no caller of
`ApplicationServices.cleanup()` either); wire a call to `.cleanup()`
into a periodic task (e.g. an APScheduler job, a cron-triggered admin
endpoint, or a background asyncio loop started by a LifecycleStep) when
scheduled maintenance is needed.
"""

import time
from typing import Any, Dict

from core.caching.cache_maintenance import CacheMaintenance
from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)

_THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60
_SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60


class ApplicationMaintenanceService:
    """Periodic maintenance over the application state store: expired
    cache entries, old logs, old traces, and reclaiming space.

    Distinct from `core.caching.cache_maintenance.CacheMaintenance`
    (which this class uses for the cache half of the job): that class
    owns only the cache table's definition of "expired"; this class
    owns the overall maintenance *policy* -- what to clean up, in what
    order, and on what retention window -- across every table that
    accumulates unbounded history (logs, traces, cache).
    """

    def __init__(self, application_state: ApplicationStateStore):
        self.application_state = application_state
        self.cache_maintenance = CacheMaintenance(application_state)

    def cleanup(self) -> Dict[str, Any]:
        """Clean up expired entries and reclaim space.

        Returns:
            Summary counts of what was cleaned up.
        """
        try:
            expired_cache = self.cache_maintenance.cleanup_expired_cache()

            # Clean up old logs (keep last 30 days)
            thirty_days_ago = int(time.time()) - _THIRTY_DAYS_SECONDS
            old_logs_result = self.application_state.execute(
                "DELETE FROM logs WHERE timestamp < ?", (thirty_days_ago,)
            )
            old_logs_count = old_logs_result.rowcount

            # Clean up old traces (keep last 7 days)
            seven_days_ago = int(time.time()) - _SEVEN_DAYS_SECONDS
            old_traces_result = self.application_state.execute(
                "DELETE FROM traces WHERE start_time < ?",
                (seven_days_ago * 1000000,),  # Convert to microseconds
            )
            old_traces_count = old_traces_result.rowcount

            # Vacuum database to reclaim space
            self.application_state.execute("VACUUM")

            logger.info(
                f"Maintenance cleanup: {expired_cache} expired cache, "
                f"{old_logs_count} old logs, {old_traces_count} old traces"
            )

            return {
                "expired_cache": expired_cache,
                "old_logs": old_logs_count,
                "old_traces": old_traces_count,
            }
        except Exception as e:
            logger.error(f"Maintenance cleanup failed: {e}")
            return {}
