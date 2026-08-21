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

    This is the *fallback* migration path, not the primary one in the
    mode this application actually runs in -- see
    `core/db/migrations.py`'s module docstring for the full ownership
    picture. `ApplicationDataStep.startup_async()` (which always runs
    before `ApplicationStateStep` -- fixed step order, not a race, see
    `core.app.lifecycle.manager`) already applies the same migrations
    in async mode; this path exists for sync-mode deployments and for
    standalone use of `ApplicationStateStore` (e.g. `bootstrap_admin.py`)
    where `ApplicationDataStep` never runs at all. Idempotent either
    way (see `apply_migrations_sync`'s tracking table), so calling this
    when the schema is already current is always safe, just a no-op.
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
