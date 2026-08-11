"""Service database for API keys, users, logging, tracing, caching, and
audit -- the operational/control-plane data this API needs alongside
the data warehouse itself.

Lives in the same PostgreSQL database as the warehouse (see
`ServiceDatabase.for_postgres`, and `core.db.config.DatabaseConfig`'s
module docstring for why there's only one backend at all): one
database to run, back up, and reason about consistency for, right
alongside the warehouse's own tables and `core.auth.shared_state`'s
`_app_*` coordination tables -- not a second database to keep in sync
with it.

Exposes a synchronous execute/fetch_one/fetch_all/transaction surface
(a plain psycopg2 connection pool, not the async warehouse pool in
`core.db.session`), so none of the repositories built on top of it
(core.auth.*_repository, core.observability.*,
core.caching.query_result_cache) need any `await`.
"""

import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.auth.passwords import hash_password
from core.db.adapters.postgresql import translate_qmark_placeholders
from core.db.logger import get_logger
from core.db.migrations import POSTGRES_TRACKING_TABLE_DDL, apply_migrations_sync
from core.db.pool import MaxConnectionsExceeded, SyncConnectionPool

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

# Same optional Prometheus instrumentation session.py applies around the
# warehouse pool's acquire/release. Optional because — same as
# session.py — this must not become a hard dependency just to run a
# query; if prometheus_client isn't installed, these stay None and the
# instrumentation below is skipped.
try:
    from core.observability import POOL_ACTIVE, POOL_REQUESTS, POOL_TIMEOUTS, POOL_WAIT
except Exception:
    POOL_REQUESTS = POOL_WAIT = POOL_ACTIVE = POOL_TIMEOUTS = None

logger = get_logger(__name__)

# migrations/warehouse_postgres/ relative to the project root -- shared
# with core.auth.shared_state's coordination tables, since they're
# tables in the same database. This file lives at
# core/storage/service_db.py, three parents up.
_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations" / "warehouse_postgres"


class _Row(dict):
    """A single result row that supports both `row["col"]` and
    positional `row[0]` access -- several call sites (this module and
    its repositories) index results positionally. Relies on dicts
    preserving insertion order (guaranteed since Python 3.7), which
    matches the column order `cursor.description` -- and therefore
    `psycopg2.extras.RealDictCursor` -- returns them in.
    """

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _CursorResult:
    """Adapts a psycopg2 cursor to a small, stable shape --
    `.lastrowid`, `.rowcount`, `.fetchone()`, `.fetchall()` -- that
    `ServiceDatabase.execute()`/`.fetch_one()`/`.fetch_all()` (below)
    are written against.

    `.lastrowid` has no psycopg2 equivalent (no ServiceDatabase caller
    actually reads `ExecuteResult.lastrowid` today -- grep the
    repositories built on this module -- so returning None here loses
    nothing; a future caller that needs a generated ID should use
    `INSERT ... RETURNING` explicitly, the Postgres-native way, rather
    than this getting silently reintroduced as a fake lastrowid).
    """

    __slots__ = ("_cursor",)

    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def lastrowid(self) -> Optional[int]:
        return None

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def fetchone(self) -> Optional[_Row]:
        row = self._cursor.fetchone()
        return _Row(row) if row is not None else None

    def fetchall(self) -> List[_Row]:
        return [_Row(row) for row in self._cursor.fetchall()]


class _ServiceDBAdapter:
    """Sync psycopg2 connection used by `ServiceDatabase`'s pool.

    Deliberately not `core.db.adapters.postgresql.PostgreSQLAdapter`:
    that adapter's `execute()` returns already-fetched rows (built for
    the ad-hoc `/api/query` console endpoint), not a live cursor with
    `.rowcount`/`.lastrowid` -- the contract every `ServiceDatabase`
    method is written against. This reuses that adapter's `?`->`%s`
    placeholder translation (`translate_qmark_placeholders`) so SQL
    text reads identically in both places.

    Connects with `autocommit=True`: `ServiceDatabase` pools connections
    and reuses them across many short, independent calls (one API
    request may run several), so leaving reads (`fetch_one`/`fetch_all`)
    to accumulate as an open, never-committed transaction until the
    next write would tie up a Postgres backend connection
    "idle in transaction" between calls for no benefit -- there is no
    multi-statement transaction here except inside `transaction()`,
    which still works correctly since `.commit()`/`.rollback()` remain
    valid (harmless no-ops) on an autocommit connection when nothing is
    open.
    """

    def __init__(self, **connect_kwargs: Any):
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 package is required for the service "
                "database. Install with: pip install psycopg2-binary"
            )
        self._connect_kwargs = connect_kwargs
        self.connection: Optional[Any] = None

    def connect(self, timeout: int = 30) -> None:
        self.connection = psycopg2.connect(
            connect_timeout=int(timeout), **self._connect_kwargs
        )
        self.connection.autocommit = True
        self.connection.cursor_factory = psycopg2.extras.RealDictCursor
        logger.info(
            "Connected to service database: "
            f"{self._connect_kwargs.get('user', '?')}"
            f"@{self._connect_kwargs.get('host', '?')}:"
            f"{self._connect_kwargs.get('port', '?')}/"
            f"{self._connect_kwargs.get('database', '?')}"
        )

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from service database")

    def execute(self, sql: str, params: tuple = ()) -> _CursorResult:
        cursor = self.connection.cursor()
        cursor.execute(translate_qmark_placeholders(sql), params)
        return _CursorResult(cursor)


@dataclass
class ExecuteResult:
    """What execute() returns instead of a live psycopg2 cursor.

    A pooled connection is released the instant execute() returns, so a
    cursor handed back to the caller could end up being read from a
    connection another thread has since borrowed and reused. lastrowid/
    rowcount are plain values captured before release, so they stay
    valid to read afterward; fetching more rows off the cursor is not.
    """

    lastrowid: Optional[int]
    rowcount: int


class ServiceDatabase:
    """Service database manager for API keys, users, logging, tracing,
    caching, and audit. See this module's docstring for why this
    always shares the warehouse's PostgreSQL database rather than
    choosing a backend.

    Composes SyncConnectionPool (core.db.pool) rather than hand-rolling
    connection pooling here, over the adapter defined above. Each
    pooled connection is a fully independent connection to the
    database — sharing one connection across concurrent callers is
    what corrupts a connection under concurrent access.
    """

    def __init__(
        self,
        *,
        min_size: int = 2,
        max_size: int = 8,
        timeout: float = 30.0,
        **connect_kwargs: Any,
    ):
        """Initialize the service database.

        Most callers should use `ServiceDatabase.for_postgres(...)`
        rather than this constructor directly -- it exists mainly so
        `for_postgres()` has something to build.

        Args:
            min_size: Connections the pool may keep idle
            max_size: Ceiling on connections ever opened against this
                database
            timeout: Seconds to wait for a free pooled connection, and
                passed to psycopg2 as the new connection's
                `connect_timeout`.
            **connect_kwargs: psycopg2 connection kwargs (host, port,
                database, user, password, sslmode, ...).
        """
        if not connect_kwargs:
            raise ValueError(
                "ServiceDatabase requires PostgreSQL connect_kwargs "
                "(host, port, database, user, password, ...) -- use "
                "ServiceDatabase.for_postgres(...)"
            )
        self._connect_kwargs = connect_kwargs
        self.min_size = min_size
        self.max_size = max_size
        self.timeout = timeout
        self._pool: Optional[SyncConnectionPool] = None

    @classmethod
    def for_postgres(
        cls,
        *,
        min_size: int = 2,
        max_size: int = 8,
        timeout: float = 30.0,
        **connect_kwargs: Any,
    ) -> "ServiceDatabase":
        """A `ServiceDatabase` whose tables live in the application's
        PostgreSQL database, alongside the data warehouse's own tables
        -- see this module's docstring.

        Args:
            min_size, max_size, timeout: same as the constructor.
            **connect_kwargs: forwarded to `psycopg2.connect()` --
                typically `host`, `port`, `database`, `user`, `password`,
                `sslmode`. Callers already building a `DatabaseConfig`
                for the warehouse (see `core.db.config.DatabaseConfig
                .from_postgresql`) should pass its `.extra_options`
                here, so both connections are guaranteed to point at
                the same database.
        """
        return cls(min_size=min_size, max_size=max_size, timeout=timeout, **connect_kwargs)

    def _create_connection(self) -> _ServiceDBAdapter:
        """Factory passed to SyncConnectionPool. Must open a genuinely
        independent connection on every call — see the class docstring."""
        adapter = _ServiceDBAdapter(**self._connect_kwargs)
        adapter.connect(timeout=int(self.timeout))
        return adapter

    def connect(self) -> None:
        """Open the connection pool against the service database."""
        self._pool = SyncConnectionPool(
            create_connection=self._create_connection,
            min_conn=self.min_size,
            max_conn=self.max_size,
            timeout=self.timeout,
        )
        host = self._connect_kwargs.get("host", "?")
        port = self._connect_kwargs.get("port", "?")
        database = self._connect_kwargs.get("database", "?")
        logger.info(
            f"Connected to service database: postgresql://{host}:{port}/{database} "
            f"(pool min={self.min_size}, max={self.max_size})"
        )

    def disconnect(self) -> None:
        """Close every pooled connection."""
        if self._pool:
            self._pool.close_all()
            logger.info("Disconnected from service database")

    def metrics(self) -> Dict[str, Any]:
        """Pool metrics, for the /api/health endpoint."""
        if not self._pool:
            raise RuntimeError("Not connected to service database")
        return dict(self._pool.metrics())

    @contextmanager
    def _acquire(self):
        """Acquire a pooled connection with the same Prometheus
        instrumentation session.py applies around the warehouse
        pool's acquire/release — this pool was otherwise invisible to
        the same metrics. All failures here are non-fatal to the
        instrumentation itself; a broken metrics backend must never
        break a query."""
        if not self._pool:
            raise RuntimeError("Not connected to service database")

        if POOL_REQUESTS is not None:
            try:
                POOL_REQUESTS.inc()
            except Exception:
                logger.debug("Failed to increment POOL_REQUESTS", exc_info=True)

        start = time.monotonic()
        try:
            adapter = self._pool.acquire()
        except MaxConnectionsExceeded:
            if POOL_TIMEOUTS is not None:
                try:
                    POOL_TIMEOUTS.inc()
                except Exception:
                    logger.debug("Failed to increment POOL_TIMEOUTS", exc_info=True)
            raise
        finally:
            if POOL_WAIT is not None:
                try:
                    POOL_WAIT.observe(time.monotonic() - start)
                except Exception:
                    logger.debug("Failed to observe POOL_WAIT", exc_info=True)

        if POOL_ACTIVE is not None:
            try:
                POOL_ACTIVE.inc()
            except Exception:
                logger.debug("Failed to increment POOL_ACTIVE", exc_info=True)

        try:
            yield adapter
        finally:
            self._pool.release(adapter)
            if POOL_ACTIVE is not None:
                try:
                    POOL_ACTIVE.dec()
                except Exception:
                    logger.debug("Failed to decrement POOL_ACTIVE", exc_info=True)

    def execute_on(self, adapter, sql: str, params: tuple = ()) -> ExecuteResult:
        """Run a statement on an already-acquired adapter, without
        acquiring a new connection or committing — for use inside
        `transaction()`, where the caller controls the commit boundary.
        """
        cursor = adapter.execute(sql, params)
        return ExecuteResult(lastrowid=cursor.lastrowid, rowcount=cursor.rowcount)

    def fetch_one_on(self, adapter, sql: str, params: tuple = ()):
        """Like fetch_one, but against an already-acquired adapter — see
        execute_on()."""
        cursor = adapter.execute(sql, params)
        return cursor.fetchone()

    @contextmanager
    def transaction(self):
        """Acquire one connection for multiple statements, committing
        once at the end instead of once per statement.

        Built for emit_request_observability(), which does ~4 writes
        (log_request, start_trace, end_trace, audit) per request. Each
        used to go through its own execute() call — its own acquire,
        its own commit, its own release — meaning 4 separate pooled
        connections acquired and released per request. Batching them
        into one transaction takes one connection for the whole
        request instead of four, which is what actually matters once
        the service database can be a genuinely remote PostgreSQL
        server rather than a local file: four round trips instead of
        one.

        Usage:
            with service_db.transaction() as adapter:
                service_db.execute_on(adapter, sql1, params1)
                service_db.execute_on(adapter, sql2, params2)
            # commits here on success, rolls back on exception
        """
        with self._acquire() as adapter:
            try:
                yield adapter
                adapter.connection.commit()
            except Exception:
                try:
                    adapter.connection.rollback()
                except Exception:
                    logger.debug("Rollback failed", exc_info=True)
                raise

    def execute(self, sql: str, params: tuple = ()) -> ExecuteResult:
        """Execute a write statement (INSERT/UPDATE/DELETE/DDL) and commit.

        Not meant for SELECTs — see ExecuteResult's docstring for why a
        live cursor isn't handed back. Use fetch_one/fetch_all for reads.

        Args:
            sql: SQL statement string
            params: Statement parameters

        Returns:
            ExecuteResult(lastrowid, rowcount)
        """
        with self._acquire() as adapter:
            try:
                cursor = adapter.execute(sql, params)
                if not sql.strip().upper().startswith("SELECT"):
                    adapter.connection.commit()
                return ExecuteResult(
                    lastrowid=cursor.lastrowid, rowcount=cursor.rowcount
                )
            except Exception as e:
                logger.error(f"Service database query failed: {e}")
                raise

    def fetch_one(self, sql: str, params: tuple = ()):
        """Fetch a single row.

        Returns a `_Row` (not a plain dict): several call sites in
        service_manager.py index results positionally (e.g. result[0]),
        which a plain dict doesn't support — `_Row` supports both
        row["col"] and row[0].

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            Single row or None
        """
        with self._acquire() as adapter:
            try:
                cursor = adapter.execute(sql, params)
                return cursor.fetchone()
            except Exception as e:
                logger.error(f"Service database query failed: {e}")
                raise

    def fetch_all(self, sql: str, params: tuple = ()):
        """Fetch all rows. See fetch_one() for the `_Row` note.

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            List of rows
        """
        with self._acquire() as adapter:
            try:
                cursor = adapter.execute(sql, params)
                return cursor.fetchall()
            except Exception as e:
                logger.error(f"Service database query failed: {e}")
                raise

    def create_tables(self) -> None:
        """Bring the service database schema up to date.

        Was six `_create_*_table()` methods each issuing their own
        `CREATE TABLE IF NOT EXISTS` -- idempotent, but with no record
        of *what* schema had been applied where, which is exactly what
        makes coordinated schema changes across dev/staging/prod risky:
        nothing here could tell you whether a given deployment's schema
        actually matched the code running against it, or safely apply
        an incremental change without redefining the whole table.
        Delegates to core.db.migrations now instead -- numbered SQL
        files, tracked in a `schema_migrations` table. See that
        module's docstring for the full reasoning (including why this
        isn't Alembic).

        Applies `migrations/warehouse_postgres/` -- the same directory,
        and the same `schema_migrations` table, `core.auth.shared_state`'s
        coordination-table migration uses, since they're tables in the
        same database. Whichever of the two runs first at startup
        creates the schema; the other is a no-op against an
        already-current one.
        """
        applied = apply_migrations_sync(self, _MIGRATIONS_DIR)
        if not applied:
            logger.debug("Service database schema already up to date")

    def initialize_admin_user(self) -> None:
        """Create an initial admin user, but only from an explicitly
        configured password — never a hardcoded default.

        The previous version of this method created a well-known
        `admin` / `admin123!` account (hashed with unsalted SHA-256) on
        every fresh database with no forced rotation. Any deployment
        that didn't immediately notice and change it had a public,
        guessable admin login sitting in the database. This version is
        a no-op unless `INITIAL_ADMIN_PASSWORD` is set in the
        environment; the recommended way to create the first admin is
        the standalone `bootstrap_admin.py` script (interactive
        password prompt, no plaintext in shell history/process list).
        This env-var path exists mainly for scripted/CI first-boot
        setups where a prompt isn't possible.
        """
        password = os.getenv("INITIAL_ADMIN_PASSWORD")
        username = os.getenv("INITIAL_ADMIN_USERNAME", "admin")

        if not password:
            result = self.fetch_one(
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
        result = self.execute(
            sql,
            (user_id, username, email, password_hash, "admin", created_at, created_at),
        )

        if result.rowcount == 0:
            logger.info(f"User '{username}' already exists; skipping admin bootstrap.")
        else:
            logger.info(f"Created initial admin user '{username}' (user_id={user_id})")

    def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries.

        Returns:
            Number of entries cleaned up
        """
        current_time = int(time.time())
        result = self.execute(
            "DELETE FROM query_cache WHERE expires_at IS NOT NULL AND expires_at < ?",
            (current_time,),
        )
        deleted_count = result.rowcount
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} expired cache entries")
        return deleted_count

    def get_stats(self) -> dict:
        """Get database statistics.

        Returns:
            Dictionary with table counts and sizes
        """
        stats = {}

        # Table counts
        tables = ["api_keys", "users", "logs", "traces", "query_cache", "audit_log"]
        for table in tables:
            result = self.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
            stats[f"{table}_count"] = result["count"] if result else 0

        # Database size. There's no single file to stat, and it's
        # shared with the warehouse's own tables, so report the whole
        # database's size.
        size_result = self.fetch_one("SELECT pg_database_size(current_database()) AS size")
        stats["db_size_bytes"] = size_result["size"] if size_result else 0

        return stats

    # ============= API KEY METHODS =============

    def create_api_key(
        self,
        key_id: str,
        api_key_hash: str,
        owner_id: str,
        created_at: int,
        expires_at: Optional[int] = None,
        scopes: Optional[str] = None,
        is_active: bool = True,
    ) -> bool:
        """Create a new API key.

        Returns:
            True if created successfully
        """
        sql = """
        INSERT INTO api_keys (key_id, api_key_hash, owner_id, created_at, expires_at, scopes, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        try:
            self.execute(
                sql,
                (
                    key_id,
                    api_key_hash,
                    owner_id,
                    created_at,
                    expires_at,
                    scopes,
                    is_active,
                ),
            )
            return True
        except Exception as e:
            logger.error(f"Failed to create API key: {e}")
            return False

    def validate_api_key(
        self, api_key_hash: str, current_time: int
    ) -> Optional[Dict[str, Any]]:
        """Validate an API key hash.

        Args:
            api_key_hash: Hashed API key
            current_time: Current timestamp

        Returns:
            API key data if valid, None otherwise
        """
        sql = """
        SELECT key_id, owner_id, created_at, expires_at, scopes, is_active
        FROM api_keys
        WHERE api_key_hash = ? AND is_active = true
        """
        try:
            result = self.fetch_one(sql, (api_key_hash,))
            if not result:
                return None

            key_data = dict(result)

            # Check if key is expired
            if key_data.get("expires_at") and key_data["expires_at"] < current_time:
                logger.warning(f"API key {key_data['key_id']} has expired")
                return None

            return key_data
        except Exception as e:
            logger.error(f"Failed to validate API key: {e}")
            return None

    def list_api_keys_by_owner(self, owner_id: str) -> List[Dict[str, Any]]:
        """List all API keys for an owner.

        Args:
            owner_id: Owner user ID

        Returns:
            List of API key data
        """
        sql = """
        SELECT key_id, owner_id, created_at, expires_at, scopes, is_active
        FROM api_keys
        WHERE owner_id = ?
        ORDER BY created_at DESC
        """
        try:
            results = self.fetch_all(sql, (owner_id,))
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Failed to list API keys: {e}")
            return []

    def revoke_api_key(self, key_id: str, owner_id: str) -> bool:
        """Revoke an API key.

        Args:
            key_id: API key ID
            owner_id: Owner user ID

        Returns:
            True if revoked successfully
        """
        sql = "UPDATE api_keys SET is_active = false WHERE key_id = ? AND owner_id = ?"
        try:
            self.execute(sql, (key_id, owner_id))
            return True
        except Exception as e:
            logger.error(f"Failed to revoke API key: {e}")
            return False

    def delete_api_key(self, key_id: str, owner_id: str) -> bool:
        """Delete an API key.

        Args:
            key_id: API key ID
            owner_id: Owner user ID

        Returns:
            True if deleted successfully
        """
        sql = "DELETE FROM api_keys WHERE key_id = ? AND owner_id = ?"
        try:
            self.execute(sql, (key_id, owner_id))
            return True
        except Exception as e:
            logger.error(f"Failed to delete API key: {e}")
            return False

    # ============= USER METHODS =============

    def create_user(
        self,
        user_id: str,
        username: str,
        email: str,
        password_hash: str,
        role: str = "user",
        created_at: int = None,
    ) -> bool:
        """Create a new user.

        Returns:
            True if created successfully
        """
        if created_at is None:
            created_at = int(time.time())

        sql = """
        INSERT INTO users (user_id, username, email, password_hash, roles, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, true, ?, ?)
        """
        try:
            self.execute(
                sql,
                (user_id, username, email, password_hash, role, created_at, created_at),
            )
            return True
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            return False

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID.

        Args:
            user_id: User ID

        Returns:
            User data if found
        """
        sql = "SELECT * FROM users WHERE user_id = ?"
        try:
            result = self.fetch_one(sql, (user_id,))
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get user by ID: {e}")
            return None

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user by username.

        Args:
            username: Username

        Returns:
            User data if found
        """
        sql = "SELECT * FROM users WHERE username = ?"
        try:
            result = self.fetch_one(sql, (username,))
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get user by username: {e}")
            return None

    def update_user_role(self, user_id: str, role: str) -> bool:
        """Update user role.

        Args:
            user_id: User ID
            role: New role

        Returns:
            True if updated successfully
        """
        sql = "UPDATE users SET roles = ?, updated_at = ? WHERE user_id = ?"
        try:
            self.execute(sql, (role, int(time.time()), user_id))
            return True
        except Exception as e:
            logger.error(f"Failed to update user role: {e}")
            return False

    def delete_user(self, user_id: str) -> bool:
        """Delete a user.

        Args:
            user_id: User ID

        Returns:
            True if deleted successfully
        """
        sql = "DELETE FROM users WHERE user_id = ?"
        try:
            self.execute(sql, (user_id,))
            return True
        except Exception as e:
            logger.error(f"Failed to delete user: {e}")
            return False


# NOTE: There is intentionally no init_service_database()/__main__ helper here.
# Connecting, creating tables, seeding the admin user, and running startup
# cleanup is lifecycle *sequencing* — it belongs to ApplicationLifespan
# (via a LifecycleStep), not to the database module itself. ServiceDatabase
# only knows how to connect, create its own tables, and run queries; it
# doesn't decide when those things happen.
