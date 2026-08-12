"""Schema/migration management for the application state store.

Extracted out of `ApplicationStateStore.create_tables()` (see that
class's docstring) so the store itself only has to know
connect/disconnect/execute/fetch/transaction -- a plain infrastructure
primitive -- rather than also deciding what tables should exist.
"""

from pathlib import Path

from core.db.logger import get_logger
from core.db.migrations import apply_migrations_sync
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)

# migrations/postgresql/ relative to the project root -- shared with
# core.auth.shared_state's coordination-table migration, and with
# core.app.lifecycle.application_data.ApplicationDataStep's own
# migration pass, since they're tables in the same database. This file
# lives at core/storage/schema.py, two parents up.
_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations" / "postgresql"


class ApplicationStateSchema:
    """Applies the application state store's schema migrations.

    Idempotent: whichever of ApplicationDataStep's migration pass or
    this one runs first creates the schema; the other is a no-op
    against an already-current one (see `apply_migrations_sync`'s
    tracking table).
    """

    def __init__(self, application_state: ApplicationStateStore):
        self.application_state = application_state

    def create(self) -> None:
        """Bring the application state store schema up to date.

        Was `ApplicationStateStore.create_tables()`'s implementation,
        which was in turn six separate `_create_*_table()` methods
        before that -- see core.db.migrations' module docstring for why
        numbered SQL files tracked in a `schema_migrations` table
        replaced both.
        """
        applied = apply_migrations_sync(self.application_state, _MIGRATIONS_DIR)
        if not applied:
            logger.debug("Application state store schema already up to date")
