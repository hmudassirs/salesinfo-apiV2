"""Application state store for API keys, users, logging, tracing, caching, and
audit -- the operational/control-plane data this API needs alongside
the application data store itself.

Lives in the same PostgreSQL database as the application data store (see
`ApplicationStateStore.for_postgres`, and `core.db.config.DatabaseConfig`'s
module docstring for why there's only one backend at all): one
database to run, back up, and reason about consistency for, right
alongside the application data's own tables and `core.auth.shared_state`'s
`_app_*` coordination tables -- not a second database to keep in sync
with it.

Exposes a synchronous execute/fetch_one/fetch_all/transaction surface
(a plain psycopg2 connection pool, not the async data pool in
`core.db.session`), so none of the repositories built on top of it
(core.auth.*_repository, core.observability.*,
core.caching.query_result_cache) need any `await`.
"""

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.db.adapters.postgresql import translate_qmark_placeholders
from core.db.logger import get_logger
from core.db.pool import MaxConnectionsExceeded, SyncConnectionPool

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

# Same optional Prometheus instrumentation session.py applies around the
# data pool's acquire/release. Optional because — same as
# session.py — this must not become a hard dependency just to run a
# query; if prometheus_client isn't installed, these stay None and the
# instrumentation below is skipped.
try:
    from core.observability import POOL_ACTIVE, POOL_REQUESTS, POOL_TIMEOUTS, POOL_WAIT
except Exception:
    POOL_REQUESTS = POOL_WAIT = POOL_ACTIVE = POOL_TIMEOUTS = None

logger = get_logger(__name__)


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
    `ApplicationStateStore.execute()`/`.fetch_one()`/`.fetch_all()` (below)
    are written against.

    `.lastrowid` has no psycopg2 equivalent (no ApplicationStateStore caller
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


class _ApplicationStateAdapter:
    """Sync psycopg2 connection used by `ApplicationStateStore`'s pool.

    Deliberately not `core.db.adapters.postgresql.PostgreSQLAdapter`:
    that adapter's `execute()` returns already-fetched rows (built for
    the ad-hoc `/api/query` console endpoint), not a live cursor with
    `.rowcount`/`.lastrowid` -- the contract every `ApplicationStateStore`
    method is written against. This reuses that adapter's `?`->`%s`
    placeholder translation (`translate_qmark_placeholders`) so SQL
    text reads identically in both places.

    Connects with `autocommit=True`: `ApplicationStateStore` pools connections
    and reuses them across many short, independent calls (one API
    request may run several), so leaving reads (`fetch_one`/`fetch_all`)
    to accumulate as an open, never-committed transaction until the
    next write would tie up a Postgres backend connection
    "idle in transaction" between calls for no benefit. `transaction()`
    is the one exception: it explicitly flips `autocommit` off for the
    duration of the `with` block (and back on before the connection
    returns to the pool) so its multiple statements share one real
    BEGIN/COMMIT-or-ROLLBACK instead of each auto-committing itself the
    instant it runs -- see `transaction()`'s docstring for why an
    earlier version of this class got that wrong.
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
            "Connected to application state store: "
            f"{self._connect_kwargs.get('user', '?')}"
            f"@{self._connect_kwargs.get('host', '?')}:"
            f"{self._connect_kwargs.get('port', '?')}/"
            f"{self._connect_kwargs.get('database', '?')}"
        )

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from application state store")

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


class ApplicationStateStore:
    """Application state store manager for API keys, users, logging, tracing,
    caching, and audit. See this module's docstring for why this
    always shares the application data's PostgreSQL database rather than
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
        """Initialize the application state store.

        Most callers should use `ApplicationStateStore.for_postgres(...)`
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
                "ApplicationStateStore requires PostgreSQL connect_kwargs "
                "(host, port, database, user, password, ...) -- use "
                "ApplicationStateStore.for_postgres(...)"
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
    ) -> "ApplicationStateStore":
        """A `ApplicationStateStore` whose tables live in the application's
        PostgreSQL database, alongside the application data's own tables
        -- see this module's docstring.

        Args:
            min_size, max_size, timeout: same as the constructor.
            **connect_kwargs: forwarded to `psycopg2.connect()` --
                typically `host`, `port`, `database`, `user`, `password`,
                `sslmode`. Callers already building a `DatabaseConfig`
                for the application data store (see `core.db.config.DatabaseConfig
                .from_postgresql`) should pass its `.extra_options`
                here, so both connections are guaranteed to point at
                the same database.
        """
        return cls(min_size=min_size, max_size=max_size, timeout=timeout, **connect_kwargs)

    def _create_connection(self) -> _ApplicationStateAdapter:
        """Factory passed to SyncConnectionPool. Must open a genuinely
        independent connection on every call — see the class docstring."""
        adapter = _ApplicationStateAdapter(**self._connect_kwargs)
        adapter.connect(timeout=int(self.timeout))
        return adapter

    def connect(self) -> None:
        """Open the connection pool against the application state store."""
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
            f"Connected to application state store: postgresql://{host}:{port}/{database} "
            f"(pool min={self.min_size}, max={self.max_size})"
        )

    def disconnect(self) -> None:
        """Close every pooled connection."""
        if self._pool:
            self._pool.close_all()
            logger.info("Disconnected from application state store")

    def metrics(self) -> Dict[str, Any]:
        """Pool metrics, for the /api/health endpoint."""
        if not self._pool:
            raise RuntimeError("Not connected to application state store")
        return dict(self._pool.metrics())

    @contextmanager
    def _acquire(self):
        """Acquire a pooled connection with the same Prometheus
        instrumentation session.py applies around the application data store
        pool's acquire/release — this pool was otherwise invisible to
        the same metrics. All failures here are non-fatal to the
        instrumentation itself; a broken metrics backend must never
        break a query."""
        if not self._pool:
            raise RuntimeError("Not connected to application state store")

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
        """Acquire one connection for multiple statements, sharing a
        single real transaction across them (one BEGIN, one final
        COMMIT or ROLLBACK) instead of the pool's normal per-connection
        `autocommit=True` (see `_ApplicationStateAdapter`'s docstring),
        under which each statement commits itself the instant it runs
        -- an earlier version of this method left `autocommit=True` in
        place inside the `with` block, so its `.commit()`/`.rollback()`
        calls were harmless no-ops and a failure partway through left
        every already-run statement permanently committed instead of
        rolled back. Explicitly toggling `autocommit` off/on around the
        block, rather than trusting the connection's default, is what
        actually enforces the boundary the docstring below promises.

        Built for emit_request_observability(), which does ~4 writes
        (log_request, start_trace, end_trace, audit) per request. Each
        used to go through its own execute() call — its own acquire,
        its own commit, its own release — meaning 4 separate pooled
        connections acquired and released per request. Batching them
        into one transaction takes one connection for the whole
        request instead of four, which is what actually matters once
        the application state store can be a genuinely remote PostgreSQL
        server rather than a local file: four round trips instead of
        one.

        Usage:
            with application_state.transaction() as adapter:
                application_state.execute_on(adapter, sql1, params1)
                application_state.execute_on(adapter, sql2, params2)
            # commits here on success, rolls back on exception
        """
        with self._acquire() as adapter:
            adapter.connection.autocommit = False
            try:
                yield adapter
                adapter.connection.commit()
            except Exception:
                try:
                    adapter.connection.rollback()
                except Exception:
                    logger.debug("Rollback failed", exc_info=True)
                raise
            finally:
                # Restore the pool's autocommit=True invariant before
                # this connection goes back to _acquire()'s release()
                # (its `finally`, which runs after this one) -- every
                # other method on this class assumes autocommit=True on
                # a connection it pulls from the pool.
                adapter.connection.autocommit = True

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
                logger.error(f"Application state store query failed: {e}")
                raise

    def fetch_one(self, sql: str, params: tuple = ()):
        """Fetch a single row.

        Returns a `_Row` (not a plain dict): several call sites in
        application_services.py index results positionally (e.g. result[0]),
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
                logger.error(f"Application state store query failed: {e}")
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
                logger.error(f"Application state store query failed: {e}")
                raise

    # create_tables()/initialize_admin_user()/cleanup_expired_cache()
    # used to live here as convenience methods. They've moved to
    # core.storage.schema.ApplicationStateSchema,
    # core.auth.admin_bootstrap.AdminBootstrapService, and
    # core.caching.cache_maintenance.CacheMaintenance respectively --
    # each is application-specific policy (what schema to apply, how to
    # seed an admin, what "expired" means for the cache), not a generic
    # store primitive. get_stats() (table counts/db size) moved out the
    # same way, to core.services.application_diagnostics.ApplicationDiagnostics
    # -- it knew the application's table names, the same class of leak
    # the other three had. ApplicationStateStore's job stops at
    # connect/disconnect/execute/fetch/transaction; see this module's
    # docstring.

    # API key and user CRUD used to live here as convenience methods.
    # They've moved to core.auth.api_key_repository.APIKeyRepository and
    # core.auth.user_repository.UserRepository respectively, which now
    # own their SQL directly against execute()/fetch_one()/fetch_all()
    # above -- the same pattern core.observability.audit.AuditTrail,
    # core.observability.request_logger.RequestLogger,
    # core.observability.request_tracer.RequestTracer, and
    # core.caching.query_result_cache.QueryResultCache already followed.
    # ApplicationStateStore's job is connection/pool management and
    # generic execute/fetch/transaction primitives, not domain-specific
    # queries for every table sharing this database.


# NOTE: There is intentionally no init_application_state()/__main__ helper here.
# Connecting, creating tables, seeding the admin user, and running startup
# cleanup is lifecycle *sequencing* — it belongs to ApplicationLifespan
# (via a LifecycleStep), not to the database module itself. ApplicationStateStore
# only knows how to connect, disconnect, execute, fetch, and manage
# transactions; it doesn't decide when those things happen, and it
# doesn't create tables itself anymore either -- see
# core.storage.schema.ApplicationStateSchema.
