"""Owns the PostgreSQL application data connection."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from core.app.lifecycle.base import LifecycleStep
from core.db.migrations import apply_migrations_async
from core.db.session import DatabaseSession

logger = logging.getLogger(__name__)

# migrations/postgresql/ relative to the project root (this file lives
# at core/app/lifecycle/application_data.py -- three parents up).
_POSTGRESQL_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[3] / "migrations" / "postgresql"
)


class ApplicationDataStep(LifecycleStep):
    """Owns the PostgreSQL application data connection.

    Also applies the `migrations/postgresql/` schema (see
    core.db.migrations): the auth coordination tables
    core.auth.shared_state.PersistentAuthState needs (JWT revocation,
    rate limiting), plus the application state store's own tables when
    ApplicationStateStep hasn't already created them -- see that step's
    docstring.
    """

    name = "application_data"

    def __init__(self, db_config):
        self.db_config = db_config
        self.db_session: Optional[DatabaseSession] = None

    def startup_sync(self) -> Dict[str, Any]:
        if not self.db_config:
            return {}
        self.db_session = DatabaseSession(self.db_config)
        self.db_session.initialize_sync()
        # No sync equivalent of apply_migrations_async today -- log
        # clearly rather than silently skip, so a sync-mode deployment
        # doesn't quietly run against an unmigrated schema.
        logger.warning(
            "Database session initialized in sync mode: "
            "migrations/postgresql/ was NOT applied "
            "(apply_migrations_async requires async mode). Run "
            "core.db.migrations.apply_migrations_async manually, "
            "or use mode='async' (what run_api.py uses)."
        )
        return {"db_session": self.db_session}

    async def startup_async(self) -> Dict[str, Any]:
        if not self.db_config:
            return {}
        self.db_session = DatabaseSession(self.db_config)
        await self.db_session.initialize()
        logger.info("Database session initialized (async mode)")

        await apply_migrations_async(
            self.db_session.get_async_session,
            _POSTGRESQL_MIGRATIONS_DIR,
        )

        return {"db_session": self.db_session}

    def shutdown_sync(self) -> None:
        if self.db_session and self.db_session._sync_pool:
            logger.info("Closing sync database connection")

    async def shutdown_async(self) -> None:
        if self.db_session and self.db_session._async_pool:
            logger.info("Closing async database connection")
