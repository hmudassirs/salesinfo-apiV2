"""Cross-process/instance coordination for auth-related ephemeral state.

Two pieces of state on the auth path need to be visible to every worker
process -- and, for a multi-instance deployment, every instance -- not
just the one that produced them:

  1. JWT role-revocation records (`core.auth.middleware`) -- a role
     change must stop being honored by every process holding a JWT for
     that user, not just the process that handled the role-change
     request. Previously this was an in-memory dict, so a demoted
     admin's JWT would keep working against any *other* worker process
     until the token naturally expired.
  2. The auth rate limiter (`core.auth.rate_limiter`) -- a credential-
     stuffing attempt spread across requests that happen to land on
     different worker processes should still be caught, not get a
     fresh allowance on each one.

`PostgresAuthState` uses two small tables in the application's one
PostgreSQL database (see `core.storage.service_db`'s module docstring)
as the shared coordination point -- no new infrastructure dependency
(Redis, etc.) beyond what a multi-worker deployment already requires
for the database itself. Table names are prefixed `_app_` and
documented as internal control-plane state, not warehouse business
data, since they live in the same database as the customer's tables.

Deliberately NOT extended to `core.auth.middleware._USER_CACHE` (the
per-request user-record cache) or the query result cache's L1 layer:
both are short-TTL (30s / configurable) local caches sitting in front
of an already-shared source of truth (the service database, and the L2
query cache respectively), so they self-heal within their TTL on every
process regardless of instance count -- the same "local cache in front
of a shared store" pattern used by plenty of production systems. JWT
revocation and rate-limit counters have no such backing shared store
today, which is what actually made them process-local bugs rather than
an intentional cache hierarchy.
"""

from __future__ import annotations

import time
from typing import Optional, Protocol

from core.db.logger import get_logger

logger = get_logger(__name__)


class AuthSharedState(Protocol):
    """Interface `PostgresAuthState` satisfies. Callers (middleware.py,
    rate_limiter.py) go through `get_auth_state()` below and never
    construct it directly."""

    async def get_revoked_before(self, user_id: str) -> Optional[float]:
        """Epoch seconds before which any JWT for `user_id` should be
        rejected, or None if that user has no recorded revocation."""
        ...

    async def set_revoked_before(self, user_id: str, at: float) -> None:
        """Record that any JWT for `user_id` issued before `at` should
        be rejected."""
        ...

    async def check_and_record_attempt(
        self, key: str, *, max_attempts: int, window_seconds: float
    ) -> bool:
        """Record one attempt for `key` and report whether it's still
        within the allowed rate. Returns False once `key` has already
        hit `max_attempts` within the current window."""
        ...

    async def reset_attempts(self, key: str) -> None:
        """Clear recorded attempts for `key`, e.g. after a successful
        login -- so a few earlier failures don't count against a
        caller who then authenticates correctly."""
        ...


_REVOCATIONS_TABLE = "_app_jwt_revocations"
_RATE_LIMIT_TABLE = "_app_rate_limit_windows"

_CREATE_TABLES_SQL = (
    # Kept in sync with migrations/warehouse_postgres/0001_auth_shared_state.sql
    # by hand -- this copy exists only for _ensure_tables()'s defensive
    # fallback (see its docstring), not as the source of truth for the
    # schema. A future column/index change belongs in a new numbered
    # migration file, not here.
    f"""
    CREATE TABLE IF NOT EXISTS {_REVOCATIONS_TABLE} (
        user_id TEXT PRIMARY KEY,
        revoked_at DOUBLE PRECISION NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {_RATE_LIMIT_TABLE} (
        rate_key TEXT PRIMARY KEY,
        window_start DOUBLE PRECISION NOT NULL,
        attempt_count INTEGER NOT NULL
    )
    """,
)


class PostgresAuthState:
    """Postgres-backed implementation -- shared across every worker
    process and instance connected to the same warehouse. See this
    module's docstring for why this piggybacks on the warehouse
    connection instead of adding a new dependency like Redis.

    `?`-style placeholders are used throughout (translated to `%s` by
    `core.db.adapters.postgresql.translate_qmark_placeholders`, same
    as every other query in this codebase, including the service
    database's tables -- see `core.storage.service_db`) so this reads
    like the rest of the SQL here, not like Postgres-specific code.
    """

    def __init__(self, db_session, *, revocation_cache_ttl_seconds: float = 10.0) -> None:
        self._db = db_session
        self._tables_ready = False
        # Local read-through cache in front of get_revoked_before(),
        # same pattern as core.auth.middleware._USER_CACHE: paying a
        # Postgres round trip on every single authenticated JWT request
        # just to check "has this user been revoked lately" is real,
        # avoidable latency for the overwhelming majority of requests
        # where the answer is (and stays) "no". Shorter TTL than
        # _USER_CACHE's 30s (this is the security-sensitive one) --
        # still a bounded, documented staleness window, not an
        # unbounded one like before this module existed.
        self._revocation_cache_ttl = revocation_cache_ttl_seconds
        self._revocation_cache: dict[str, tuple[Optional[float], float]] = {}

    async def _ensure_tables(self) -> None:
        """Defensive fallback only. The real schema-management path is
        migrations/warehouse_postgres/ (core.db.migrations), applied
        once at startup by DataWarehouseStep before any request is
        served -- see core/app/lifespan.py. This just guards against
        PostgresAuthState ever being constructed outside that startup
        path (a script, a test, a future caller) without failing on a
        missing table; CREATE TABLE IF NOT EXISTS is cheap and
        idempotent, so paying for it once per process here costs
        nothing extra in the normal case where migrations already ran.
        """
        if self._tables_ready:
            return
        async with self._db.get_async_session() as session:
            for statement in _CREATE_TABLES_SQL:
                await session.execute(statement)
        self._tables_ready = True

    async def get_revoked_before(self, user_id: str) -> Optional[float]:
        cached = self._revocation_cache.get(user_id)
        if cached is not None:
            value, expires_at = cached
            if time.monotonic() < expires_at:
                return value
            del self._revocation_cache[user_id]

        await self._ensure_tables()
        async with self._db.get_async_session() as session:
            row = await session.fetch_one(
                f"SELECT revoked_at FROM {_REVOCATIONS_TABLE} WHERE user_id = ?",
                (user_id,),
            )
        value = row["revoked_at"] if row else None
        self._revocation_cache[user_id] = (
            value,
            time.monotonic() + self._revocation_cache_ttl,
        )
        return value

    async def set_revoked_before(self, user_id: str, at: float) -> None:
        await self._ensure_tables()
        async with self._db.get_async_session() as session:
            await session.execute(
                f"""
                INSERT INTO {_REVOCATIONS_TABLE} (user_id, revoked_at)
                VALUES (?, ?)
                ON CONFLICT (user_id) DO UPDATE
                    SET revoked_at = EXCLUDED.revoked_at
                """,
                (user_id, at),
            )
        # Write-through: this process's own cache reflects the change
        # immediately. Other processes still catch up within
        # `_revocation_cache_ttl` -- bounded staleness, not the
        # previous unbounded (until JWT expiry) staleness.
        self._revocation_cache[user_id] = (
            at,
            time.monotonic() + self._revocation_cache_ttl,
        )

    async def check_and_record_attempt(
        self, key: str, *, max_attempts: int, window_seconds: float
    ) -> bool:
        await self._ensure_tables()
        now = time.time()
        cutoff = now - window_seconds
        # Single atomic UPSERT: if the existing window has expired
        # (window_start <= cutoff), reset the bucket to 1 and restart
        # the window at `now`; otherwise increment the existing
        # bucket's count in place. Doing this as one statement with
        # Postgres's row-level locking on the UPSERT is what makes it
        # race-free across concurrent workers -- an app-level
        # "SELECT then INSERT/UPDATE" would have a check-then-act gap
        # two processes could both slip through at once.
        async with self._db.get_async_session() as session:
            row = await session.fetch_one(
                f"""
                INSERT INTO {_RATE_LIMIT_TABLE} (rate_key, window_start, attempt_count)
                VALUES (?, ?, 1)
                ON CONFLICT (rate_key) DO UPDATE SET
                    attempt_count = CASE
                        WHEN {_RATE_LIMIT_TABLE}.window_start <= ? THEN 1
                        ELSE {_RATE_LIMIT_TABLE}.attempt_count + 1
                    END,
                    window_start = CASE
                        WHEN {_RATE_LIMIT_TABLE}.window_start <= ? THEN ?
                        ELSE {_RATE_LIMIT_TABLE}.window_start
                    END
                RETURNING attempt_count
                """,
                (key, now, cutoff, cutoff, now),
            )
        attempt_count = row["attempt_count"] if row else 1
        return attempt_count <= max_attempts

    async def reset_attempts(self, key: str) -> None:
        await self._ensure_tables()
        async with self._db.get_async_session() as session:
            await session.execute(
                f"DELETE FROM {_RATE_LIMIT_TABLE} WHERE rate_key = ?", (key,)
            )


_shared_state: Optional[AuthSharedState] = None


def get_auth_state(db_session) -> AuthSharedState:
    """Return the process-wide `AuthSharedState`, constructing it
    against `db_session` on first call and caching it for the life of
    the process.

    Args:
        db_session: the app's `DatabaseSession` -- required. There's
            one backend (PostgreSQL; see `core.db.config.DatabaseConfig`'s
            module docstring), so this is always `PostgresAuthState(db_session)`.
    """
    global _shared_state
    if _shared_state is not None:
        return _shared_state

    if db_session is None:
        raise RuntimeError(
            "get_auth_state() called before the database session was "
            "initialized -- this must run after ApplicationLifespan "
            "startup, not before it."
        )

    logger.info("Auth shared state: PostgreSQL-backed (cross-process/instance)")
    _shared_state = PostgresAuthState(db_session)
    return _shared_state


def reset_auth_state_for_testing() -> None:
    """Clear the cached singleton. Only meaningful in tests -- normal
    operation initializes it once at first use and keeps it for the
    life of the process."""
    global _shared_state
    _shared_state = None
