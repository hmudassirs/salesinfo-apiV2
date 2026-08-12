"""First-admin bootstrap, extracted out of
`ApplicationStateStore.initialize_admin_user()` -- see that method's
former docstring for the full history (this replaced a hardcoded
`admin`/`admin123!` account created on every fresh database). Lives in
`core.auth` rather than `core.storage` because it's about seeding a
*user*, not a generic store responsibility.
"""

import os
import time

from core.auth.passwords import hash_password
from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)


class AdminBootstrapService:
    """Creates the initial admin user, but only from an explicitly
    configured password -- never a hardcoded default.

    A no-op unless `INITIAL_ADMIN_PASSWORD` is set in the environment;
    the recommended way to create the first admin is the standalone
    `bootstrap_admin.py` script (interactive password prompt, no
    plaintext in shell history/process list). This env-var path exists
    mainly for scripted/CI first-boot setups where a prompt isn't
    possible.
    """

    def __init__(self, application_state: ApplicationStateStore):
        self.application_state = application_state

    def initialize(self) -> None:
        password = os.getenv("INITIAL_ADMIN_PASSWORD")
        username = os.getenv("INITIAL_ADMIN_USERNAME", "admin")

        if not password:
            result = self.application_state.fetch_one(
                "SELECT COUNT(*) AS count "
                "FROM users "
                "WHERE roles LIKE ?",
                ("%admin%",),
            )

            if result and result["count"] == 0:
                logger.warning(
                    "No admin user exists yet. Run bootstrap_admin.py, or set "
                    "INITIAL_ADMIN_USERNAME/INITIAL_ADMIN_EMAIL/"
                    "INITIAL_ADMIN_PASSWORD before startup, to create one. "
                    "No default admin account will be created automatically."
                )
            return

        if len(password) < 8:
            logger.error(
                "INITIAL_ADMIN_PASSWORD is shorter than 8 characters; refusing "
                "to create an admin user with a weak password."
            )
            return

        # INSERT ... ON CONFLICT DO NOTHING rather than a separate
        # "does this username already exist" SELECT beforehand: that
        # older check-then-insert shape is a race across concurrent
        # `--workers N > 1` processes all calling this method at
        # startup (see apply_migrations_sync's docstring for the same
        # class of bug in migrations) -- two workers can both see zero
        # matching rows and both attempt the INSERT, and the loser
        # crashes on `users_pkey`/`users_username_key`'s unique
        # constraint instead of quietly finding the user already
        # bootstrapped by whichever worker won the race. Postgres's own
        # conflict handling makes this atomic without needing a
        # separate advisory lock the way the migration race did.
        email = os.getenv("INITIAL_ADMIN_EMAIL", f"{username}@preparedata.local")
        user_id = f"user_admin_{int(time.time())}"
        password_hash = hash_password(password)
        created_at = int(time.time())

        sql = """
        INSERT INTO users (user_id, username, email, password_hash, roles, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (username) DO NOTHING
        """
        result = self.application_state.execute(
            sql,
            (user_id, username, email, password_hash, "admin", created_at, created_at),
        )

        if result.rowcount == 0:
            logger.info(f"User '{username}' already exists; skipping admin bootstrap.")
        else:
            logger.info(f"Created initial admin user '{username}' (user_id={user_id})")
