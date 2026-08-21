"""Cache-table maintenance, extracted out of
`ApplicationStateStore.cleanup_expired_cache()` -- see
core/storage/application_state_store.py's module docstring for why
that method moved out of the store itself.
"""

import time

from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)


class CacheMaintenance:
    """Housekeeping over the `query_cache` table (the L2 application
    state store cache -- see core.caching.query_result_cache.QueryResultCache,
    which owns normal read/write access to this table). Distinct from
    that class because this is periodic maintenance, not request-path
    cache access.
    """

    def __init__(self, application_state: ApplicationStateStore):
        self.application_state = application_state

    def cleanup_expired_cache(self) -> int:
        """Delete expired cache entries.

        Returns:
            Number of entries deleted.
        """
        current_time = int(time.time())
        result = self.application_state.execute(
            "DELETE FROM query_cache WHERE expires_at IS NOT NULL AND expires_at < %s",
            (current_time,),
        )
        deleted_count = result.rowcount
        if deleted_count > 0:
            logger.info("Cleaned up %s expired cache entries", deleted_count)
        return deleted_count
