from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Optional, Protocol, Sequence

from core.db.logger import get_logger

# Optional observability integrations
try:
    from core.observability import (
        APP_LATENCY,
        POOL_ACTIVE,
        POOL_REQUESTS,
        POOL_TIMEOUTS,
        POOL_WAIT,
    )
except Exception:
    POOL_REQUESTS = POOL_WAIT = POOL_ACTIVE = POOL_TIMEOUTS = APP_LATENCY = None

# Optional OpenTelemetry tracer. Routed through core.observability.otel's
# OpenTelemetryManager rather than calling `opentelemetry.trace.get_tracer()`
# directly — the direct call was the bug: it produces a real Tracer object,
# but without something calling `trace.set_tracer_provider(...)` first
# (which only OpenTelemetryManager.initialize() ever does), that tracer has
# no exporter wired up and every span it creates goes nowhere. Getting the
# tracer from the manager guarantees .initialize() has run first.
try:
    from core.observability.otel import get_otel_manager

    _TRACER = get_otel_manager().tracer
except Exception:
    _TRACER = None

import core.db.adapters  # noqa: F401 — side effect: PostgreSQLAdapter

# self-registers with the AdapterRegistry on import (see
# core/db/adapters/__init__.py and PostgreSQLAdapter's bottom-of-file
# `get_adapter_registry().register(...)` call). session.py deliberately
# never imports PostgreSQLAdapter by name — see _create_adapter().
from core.db.config import DatabaseConfig
from core.db.pool import AsyncConnectionPool, SyncConnectionPool
from core.db.pool.base import MaxConnectionsExceeded, is_connection_level_error
from core.db.registry import get_adapter_registry

# Optional core.performance integration: pool acquire/release timing +
# pool contention gauges (adapters.pool), per-connection SQL execute/
# fetch timing (adapters.sql_adapter), and an opt-in instrumented
# transaction helper (adapters.transactions) — see
# docs/performance/adapters.md. Guarded the same way the
# core.observability integration above is: core.db must keep working
# with core.performance absent.
try:
    from core.performance.adapters.pool import (
        AsyncPoolTimingAdapter,
        SyncPoolTimingAdapter,
    )
    from core.performance.adapters.sql_adapter import InstrumentedSQLAdapter
    from core.performance.adapters.transactions import (
        instrumented_async_transaction,
        instrumented_transaction,
    )
except Exception:
    AsyncPoolTimingAdapter = SyncPoolTimingAdapter = None  # type: ignore[assignment,misc]
    InstrumentedSQLAdapter = None  # type: ignore[assignment,misc]
    instrumented_async_transaction = instrumented_transaction = None  # type: ignore[assignment]

logger = get_logger(__name__)

# Dedicated bounded executor for application data work -- see
# core/concurrency/executors.py's module docstring for why this
# replaced routing every blocking call through asyncio.to_thread's
# single shared default executor (roadmap rule #5: no one oversized
# global thread pool for unrelated workloads).
from core.concurrency.executors import run_in_application_data_executor

from core.db.protocols import DatabaseAdapter as DBAdapter

# ---------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------


async def _run_in_thread(fn, *args, **kwargs):
    """Centralized async → thread execution with optional metrics and tracing.

    Measures elapsed time and records it to `APP_LATENCY` when available.
    If OpenTelemetry is configured, wraps the call in a span named
    `db.{fn.__name__}` (e.g. `db.execute`, `db.fetch_all`) — previously
    every call through here used the single generic name `db.sync_call`,
    so a trace couldn't tell an execute() from a fetch_all() from a
    disconnect(). The first positional arg is attached as `db.statement`
    when it looks like SQL, truncated to keep spans small.

    `**kwargs` exists specifically for `statement_timeout_seconds` (see
    `AsyncSessionWrapper.fetch_all`) -- forwarded through
    `run_in_application_data_executor` unchanged.
    """
    start = time.perf_counter()

    if _TRACER:
        span_name = f"db.{getattr(fn, '__name__', 'call')}"
        with _TRACER.start_as_current_span(span_name) as span:
            if args and isinstance(args[0], str):
                span.set_attribute("db.statement", args[0][:200])
            result = await run_in_application_data_executor(fn, *args, **kwargs)
    else:
        result = await run_in_application_data_executor(fn, *args, **kwargs)

    elapsed = time.perf_counter() - start
    if APP_LATENCY is not None:
        try:
            APP_LATENCY.observe(elapsed)
        except Exception:
            logger.debug("Failed to record APP_LATENCY", exc_info=True)

    return result


def _safe_rollback(adapter: DBAdapter) -> None:
    try:
        if getattr(adapter, "connection", None):
            adapter.connection.rollback()
    except Exception:
        logger.debug("Rollback failed", exc_info=True)


def _begin_transaction(adapter: DBAdapter) -> None:
    """Turn off this connection's normal per-statement autocommit (see
    `core.db.adapters.postgresql.PostgreSQLAdapter.connect()`) so the
    statements run before the matching `_commit_transaction`/
    `_rollback_transaction` share one real, atomic transaction instead
    of each committing itself immediately -- see
    `core.performance.adapters.transactions.instrumented_transaction`'s
    docstring for why SQL `BEGIN`/`COMMIT`/`ROLLBACK` statements routed
    through `execute()` never actually did this.
    """
    adapter.connection.autocommit = False


def _commit_transaction(adapter: DBAdapter) -> None:
    """End a transaction started by `_begin_transaction`, keeping every
    statement run since. Always restores `autocommit=True` afterward --
    every other method on this wrapper, and the pool itself, assumes
    that as the connection's resting state (see
    `core.storage.application_state_store.ApplicationStateStore
    .transaction()`'s matching restore)."""
    try:
        adapter.connection.commit()
    finally:
        adapter.connection.autocommit = True


def _rollback_transaction(adapter: DBAdapter) -> None:
    """Best-effort rollback (swallows its own failure, like
    `_safe_rollback`) that also restores `autocommit=True` afterward --
    see `_commit_transaction`'s docstring for why that restore matters
    regardless of which of commit/rollback ends the transaction."""
    try:
        _safe_rollback(adapter)
    finally:
        adapter.connection.autocommit = True


# ---------------------------------------------------------------------
# Session wrappers
# ---------------------------------------------------------------------


class AsyncSessionWrapper:
    """Async-friendly wrapper over a sync adapter."""

    __slots__ = ("_adapter",)

    def __init__(self, adapter: DBAdapter):
        self._adapter = adapter

    async def execute(self, sql: str, params=None, *, statement_timeout_seconds=None):
        return await _run_in_thread(
            self._adapter.execute,
            sql,
            params or (),
            statement_timeout_seconds=statement_timeout_seconds,
        )

    async def fetch_all(self, sql: str, params=None, *, statement_timeout_seconds=None):
        """See `PostgreSQLAdapter.fetch_all`'s `statement_timeout_seconds`
        docstring -- this is the one parameter worth knowing about here:
        passing it makes PostgreSQL itself, not this coroutine's
        caller, the thing that stops a runaway query. `core.services
        .query_service.QueryService._execute()` is the caller that
        matters -- see that module's docstring for why `asyncio.wait_for()`
        around this call is *not* a safe substitute."""
        return await _run_in_thread(
            self._adapter.fetch_all,
            sql,
            params or (),
            statement_timeout_seconds=statement_timeout_seconds,
        )

    async def fetch_one(self, sql: str, params=None, *, statement_timeout_seconds=None):
        return await _run_in_thread(
            self._adapter.fetch_one,
            sql,
            params or (),
            statement_timeout_seconds=statement_timeout_seconds,
        )

    async def fetch_bounded(
        self,
        sql: str,
        params=None,
        *,
        max_rows: int,
        max_bytes: int,
        batch_size: int = 500,
        statement_timeout_seconds=None,
    ):
        """See `PostgreSQLAdapter.fetch_bounded`'s docstring -- streams
        via a server-side cursor and stops as soon as `max_rows`/
        `max_bytes` is reached, instead of `fetch_all`'s "materialize
        everything, truncate afterward." `core.services.query_service
        .QueryService._execute` is the caller that matters here."""
        return await _run_in_thread(
            self._adapter.fetch_bounded,
            sql,
            params or (),
            max_rows=max_rows,
            max_bytes=max_bytes,
            batch_size=batch_size,
            statement_timeout_seconds=statement_timeout_seconds,
        )

    async def begin(self):
        """Start an explicit transaction -- see `_begin_transaction`'s
        docstring. Pairs with `commit()`/`rollback()`."""
        await _run_in_thread(_begin_transaction, self._adapter)

    async def commit(self):
        """End a transaction started by `begin()`. See
        `_commit_transaction`'s docstring."""
        await _run_in_thread(_commit_transaction, self._adapter)

    async def rollback(self):
        await _run_in_thread(_rollback_transaction, self._adapter)

    async def close(self):
        await _run_in_thread(self._adapter.disconnect)


class SyncSessionWrapper:
    """Sync wrapper over adapter."""

    __slots__ = ("_adapter",)

    def __init__(self, adapter: DBAdapter):
        self._adapter = adapter

    def execute(self, sql: str, params=None, *, statement_timeout_seconds=None):
        return self._adapter.execute(
            sql, params or (), statement_timeout_seconds=statement_timeout_seconds
        )

    def fetch_all(self, sql: str, params=None, *, statement_timeout_seconds=None):
        return self._adapter.fetch_all(
            sql, params or (), statement_timeout_seconds=statement_timeout_seconds
        )

    def fetch_one(self, sql: str, params=None, *, statement_timeout_seconds=None):
        return self._adapter.fetch_one(
            sql, params or (), statement_timeout_seconds=statement_timeout_seconds
        )

    def fetch_bounded(
        self,
        sql: str,
        params=None,
        *,
        max_rows: int,
        max_bytes: int,
        batch_size: int = 500,
        statement_timeout_seconds=None,
    ):
        """See `AsyncSessionWrapper.fetch_bounded`'s docstring -- same
        contract, no `async`/`await`."""
        return self._adapter.fetch_bounded(
            sql,
            params or (),
            max_rows=max_rows,
            max_bytes=max_bytes,
            batch_size=batch_size,
            statement_timeout_seconds=statement_timeout_seconds,
        )

    def begin(self):
        """See `AsyncSessionWrapper.begin()`."""
        _begin_transaction(self._adapter)

    def commit(self):
        """See `AsyncSessionWrapper.commit()`."""
        _commit_transaction(self._adapter)

    def rollback(self):
        _rollback_transaction(self._adapter)

    def close(self):
        self._adapter.disconnect()


# ---------------------------------------------------------------------
# DatabaseSession
# ---------------------------------------------------------------------


class DatabaseSession:
    """Manages pooled sync & async database sessions."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._adapter: Optional[DBAdapter] = None
        self._async_pool: Optional[AsyncConnectionPool] = None
        self._sync_pool: Optional[SyncConnectionPool] = None
        # core.performance-wrapped pools, used by get_async_session()/
        # get_sync_session() and get_async_transaction()/
        # get_sync_transaction(). Falls back to the raw pool itself when
        # core.performance isn't installed (see the try/except import
        # above), so callers can always go through `self._async_pool_adapter`
        # / `self._sync_pool_adapter` uniformly.
        self._async_pool_adapter: Optional[Any] = None
        self._sync_pool_adapter: Optional[Any] = None

    # ------------------ adapter factory ------------------

    def _create_adapter(self) -> DBAdapter:
        """Build a `PostgreSQLAdapter` for this config. Looks the class
        up via the registry rather than importing `PostgreSQLAdapter`
        by name -- see `core/db/adapters/__init__.py` -- so this stays a
        plain name -> class lookup rather than a hardcoded import; a
        second backend would only need to register itself the same way
        (see `core.db.registry.AdapterRegistry.register`), not change
        this method.
        """
        db_type = self.config.db_type.value  # "postgresql"
        try:
            adapter_class = get_adapter_registry().get_adapter_class(db_type)
        except ValueError as e:
            raise ValueError(
                f"Unsupported database type configured: {db_type!r}"
            ) from e

        return adapter_class(
            echo=getattr(self.config, "echo", False),
            **self.config.extra_options,
        )

    # ------------------ initialization ------------------

    async def initialize(self) -> None:
        # A dedicated, unpooled adapter for introspection-only calls
        # (get_tables()) that don't go through get_async_session().
        self._adapter = self._create_adapter()
        await _run_in_thread(self._adapter.connect)

        async def create_session():
            # IMPORTANT: build a fresh adapter (and open its own connection)
            # per call, not a closure over one shared adapter. Every
            # connection this pool hands out must be physically
            # independent — sharing one connection across concurrently
            # -used pooled "slots" corrupts it under concurrent access
            # (this is the same class of bug found and fixed in
            # ApplicationStateStore's connection layer).
            pooled_adapter = self._create_adapter()
            await _run_in_thread(pooled_adapter.connect)
            if InstrumentedSQLAdapter is not None:
                pooled_adapter = InstrumentedSQLAdapter(pooled_adapter)
            return AsyncSessionWrapper(pooled_adapter)

        # Use pool settings from config if available
        pool_settings = self.config.settings.pool if self.config.settings else None
        min_conn = pool_settings.min_size if pool_settings else 1
        max_conn = pool_settings.max_size if pool_settings else self.config.pool_size
        timeout = pool_settings.timeout if pool_settings else self.config.timeout

        self._async_pool = AsyncConnectionPool(
            create_connection=create_session,
            min_conn=min_conn,
            max_conn=max_conn,
            timeout=timeout,
        )
        self._async_pool_adapter = (
            AsyncPoolTimingAdapter(self._async_pool)
            if AsyncPoolTimingAdapter is not None
            else self._async_pool
        )

        logger.info("Async DB initialized (%s)", self.config.db_type.value)

    def initialize_sync(self) -> None:
        # A dedicated, unpooled adapter for introspection-only calls
        # (get_tables()) that don't go through get_sync_session().
        self._adapter = self._create_adapter()
        self._adapter.connect()

        def create_session():
            # See the comment in initialize() — must be a fresh, independent
            # connection per pooled slot, not the same shared adapter.
            pooled_adapter = self._create_adapter()
            pooled_adapter.connect()
            if InstrumentedSQLAdapter is not None:
                pooled_adapter = InstrumentedSQLAdapter(pooled_adapter)
            return SyncSessionWrapper(pooled_adapter)

        # Use pool settings from config if available
        pool_settings = self.config.settings.pool if self.config.settings else None
        min_conn = pool_settings.min_size if pool_settings else 1
        max_conn = pool_settings.max_size if pool_settings else self.config.pool_size
        timeout = pool_settings.timeout if pool_settings else self.config.timeout

        self._sync_pool = SyncConnectionPool(
            create_connection=create_session,
            min_conn=min_conn,
            max_conn=max_conn,
            timeout=timeout,
        )
        self._sync_pool_adapter = (
            SyncPoolTimingAdapter(self._sync_pool)
            if SyncPoolTimingAdapter is not None
            else self._sync_pool
        )

        logger.info("Sync DB initialized (%s)", self.config.db_type.value)

    def warmup_sync(self, target: int | None = None) -> int:
        """Pre-create sync connections up to `target` (defaults to configured max).

        This method temporarily increases the adaptive sizer current value so the
        pool will create connections, then acquires and immediately releases them
        to populate the pool's available list.

        Returns the number of connections successfully created.
        """
        if not self._sync_pool:
            raise RuntimeError("Sync pool not initialized")

        pool = self._sync_pool
        cfg_max = (
            self.config.settings.pool.max_size
            if getattr(self.config, "settings", None) and self.config.settings.pool
            else self.config.pool_size
        )
        target = min(target or cfg_max, pool._sizer.max)

        created = 0
        # Temporarily set the sizer current so we can create the connections
        with pool._lock:
            original = pool._sizer.current
            pool._sizer.current = max(pool._sizer.current, target)

        conns = []
        try:
            for _ in range(target):
                try:
                    conn = pool.acquire()
                except MaxConnectionsExceeded:
                    break
                else:
                    conns.append(conn)
                    created += 1
        finally:
            for c in conns:
                try:
                    pool.release(c)
                except Exception:
                    logger.debug("Failed to release warmed connection", exc_info=True)

        # leave sizer at warmed level (so pool stays warm)
        return created

    async def warmup_async(self, target: int | None = None) -> int:
        """Async variant of warmup_sync: pre-create async connections up to `target`.

        Returns the number of connections successfully created.
        """
        if not self._async_pool:
            raise RuntimeError("Async pool not initialized")

        pool = self._async_pool
        cfg_max = (
            self.config.settings.pool.max_size
            if getattr(self.config, "settings", None) and self.config.settings.pool
            else self.config.pool_size
        )
        target = min(target or cfg_max, pool._sizer.max)

        created = 0

        # Temporarily bump sizer to allow creations
        async with pool._lock:
            original = pool._sizer.current
            pool._sizer.current = max(pool._sizer.current, target)

        conns = []
        try:
            for _ in range(target):
                try:
                    conn = await pool.acquire()
                except MaxConnectionsExceeded:
                    break
                else:
                    conns.append(conn)
                    created += 1
        finally:
            for c in conns:
                try:
                    await pool.release(c)
                except Exception:
                    logger.debug("Failed to release warmed connection", exc_info=True)

        return created

    # ------------------ session context ------------------

    @asynccontextmanager
    async def get_async_session(self):
        if not self._async_pool:
            raise RuntimeError("Async pool not initialized")

        # Instrument pool acquire
        if POOL_REQUESTS is not None:
            try:
                POOL_REQUESTS.inc()
            except Exception:
                logger.debug("Failed to increment POOL_REQUESTS", exc_info=True)

        acquire_start = time.perf_counter()
        try:
            if _TRACER:
                with _TRACER.start_as_current_span("db.acquire"):
                    session = await self._async_pool_adapter.acquire()
            else:
                session = await self._async_pool_adapter.acquire()
        except MaxConnectionsExceeded:
            if POOL_TIMEOUTS is not None:
                try:
                    POOL_TIMEOUTS.inc()
                except Exception:
                    logger.debug("Failed to increment POOL_TIMEOUTS", exc_info=True)
            raise
        finally:
            wait_time = time.perf_counter() - acquire_start
            if POOL_WAIT is not None:
                try:
                    POOL_WAIT.observe(wait_time)
                except Exception:
                    logger.debug("Failed to observe POOL_WAIT", exc_info=True)

        # Mark active
        if POOL_ACTIVE is not None:
            try:
                POOL_ACTIVE.inc()
            except Exception:
                logger.debug("Failed to increment POOL_ACTIVE", exc_info=True)

        try:
            broken_connection = False
            yield session
        except asyncio.CancelledError:
            # A cancelled caller (client disconnect mid-request, task
            # cancellation, Ctrl-C propagating through an in-flight
            # request) can leave `session` mid-transaction. CancelledError
            # is a BaseException, not an Exception, since Python 3.8 --
            # it does NOT hit the `except Exception` branch below, so
            # without this branch the connection used to go straight to
            # `finally` and back into the pool with an open transaction
            # still holding whatever row locks it had acquired (e.g. an
            # auth rate-limit UPSERT). The next caller to acquire that
            # same pooled connection and touch the same row would then
            # hang indefinitely waiting on the lock -- surfacing far away
            # as a client-side read timeout, with no server-side error at
            # all, and only after the connection had already been quietly
            # handed back as "healthy". Roll back like any other
            # exception path so cancellation can't leak an open
            # transaction into the pool.
            try:
                await session.rollback()
            except Exception:
                broken_connection = True
            raise
        except Exception as exc:
            # P1-1: a connection-level failure (transport/connection
            # actually dead) must never go back to `_available` for the
            # next caller to inherit -- only an ordinary query error
            # (bad SQL, constraint violation) is safe to rollback and
            # reuse the connection for. See
            # core.db.pool.base.is_connection_level_error.
            broken_connection = is_connection_level_error(exc)
            if not broken_connection:
                try:
                    await session.rollback()
                except Exception:
                    # rollback() itself failing on a connection that
                    # otherwise looked fine is itself a connection-level
                    # signal -- treat it as broken rather than silently
                    # swallowing and returning a connection that can't
                    # even roll back.
                    broken_connection = True
            raise
        finally:
            # Release and mark inactive
            try:
                await self._async_pool_adapter.release(
                    session, broken=broken_connection
                )
            finally:
                if POOL_ACTIVE is not None:
                    try:
                        POOL_ACTIVE.dec()
                    except Exception:
                        logger.debug("Failed to decrement POOL_ACTIVE", exc_info=True)

    @contextmanager
    def get_sync_session(self):
        if not self._sync_pool:
            raise RuntimeError("Sync pool not initialized")

        # Instrument pool acquire
        if POOL_REQUESTS is not None:
            try:
                POOL_REQUESTS.inc()
            except Exception:
                logger.debug("Failed to increment POOL_REQUESTS", exc_info=True)

        acquire_start = time.perf_counter()
        try:
            if _TRACER:
                with _TRACER.start_as_current_span("db.acquire"):
                    session = self._sync_pool_adapter.acquire()
            else:
                session = self._sync_pool_adapter.acquire()
        except MaxConnectionsExceeded:
            if POOL_TIMEOUTS is not None:
                try:
                    POOL_TIMEOUTS.inc()
                except Exception:
                    logger.debug("Failed to increment POOL_TIMEOUTS", exc_info=True)
            raise
        finally:
            wait_time = time.perf_counter() - acquire_start
            if POOL_WAIT is not None:
                try:
                    POOL_WAIT.observe(wait_time)
                except Exception:
                    logger.debug("Failed to observe POOL_WAIT", exc_info=True)

        # Mark active
        if POOL_ACTIVE is not None:
            try:
                POOL_ACTIVE.inc()
            except Exception:
                logger.debug("Failed to increment POOL_ACTIVE", exc_info=True)

        try:
            broken_connection = False
            yield session
        except (asyncio.CancelledError, KeyboardInterrupt):
            # Same reasoning as the async path above: both are
            # BaseException, not Exception, so they'd otherwise skip the
            # rollback below and go straight to `finally`, handing a
            # connection with an open transaction back to the pool for
            # the next caller to silently inherit (and potentially block
            # on, if it locked any rows).
            try:
                session.rollback()
            except Exception:
                broken_connection = True
            raise
        except Exception as exc:
            broken_connection = is_connection_level_error(exc)
            if not broken_connection:
                try:
                    session.rollback()
                except Exception:
                    broken_connection = True
            raise
        finally:
            try:
                self._sync_pool_adapter.release(session, broken=broken_connection)
            finally:
                if POOL_ACTIVE is not None:
                    try:
                        POOL_ACTIVE.dec()
                    except Exception:
                        logger.debug("Failed to decrement POOL_ACTIVE", exc_info=True)

    # ------------------ instrumented transactions (opt-in) ------------------

    def get_async_transaction(self):
        """Return an instrumented transaction over the async pool.

        Acquire (`POOL_ACQUIRE`) → `BEGIN` (`TRANSACTION_BEGIN`) → yield
        the connection → `COMMIT`/`ROLLBACK`
        (`TRANSACTION_COMMIT`/`TRANSACTION_ROLLBACK`) → release
        (`POOL_RELEASE`), each step timed when a profiler is bound — see
        `core.performance.adapters.transactions.instrumented_async_transaction`.
        Opt-in and not called by any route in this codebase yet;
        available for any write path that wants explicit transactional
        semantics with full observability, e.g.:

            async with db_session.get_async_transaction() as conn:
                await conn.execute("INSERT INTO ...", params)
        """
        if not self._async_pool_adapter:
            raise RuntimeError("Async pool not initialized")
        if instrumented_async_transaction is None:
            raise RuntimeError(
                "core.performance is not installed; instrumented transactions "
                "are unavailable"
            )
        return instrumented_async_transaction(self._async_pool_adapter)

    def get_sync_transaction(self):
        """Sync counterpart of `get_async_transaction`; see its docstring."""
        if not self._sync_pool_adapter:
            raise RuntimeError("Sync pool not initialized")
        if instrumented_transaction is None:
            raise RuntimeError(
                "core.performance is not installed; instrumented transactions "
                "are unavailable"
            )
        return instrumented_transaction(self._sync_pool_adapter)

    # ------------------ health & metadata ------------------

    async def health_check(self) -> bool:
        try:
            async with self.get_async_session() as s:
                await s.execute("SELECT 1")
            return True
        except Exception:
            logger.warning("Async health check failed", exc_info=True)
            return False

    def health_check_sync(self) -> bool:
        try:
            with self.get_sync_session() as s:
                s.execute("SELECT 1")
            return True
        except Exception:
            logger.warning("Sync health check failed", exc_info=True)
            return False

    async def get_tables(self) -> list[str]:
        if not self._adapter:
            raise RuntimeError("Adapter not initialized")
        return await _run_in_thread(self._adapter.get_tables)

    async def get_schema(self, table: str) -> list[dict]:
        """Column info for `table`, off the event loop thread (same
        reasoning as `get_tables()` above)."""
        if not self._adapter:
            raise RuntimeError("Adapter not initialized")
        return await _run_in_thread(self._adapter.get_schema, table)

    # ------------------ shutdown ------------------

    async def close(self) -> None:
        if self._async_pool:
            await self._async_pool.close_all()
        if self._adapter:
            await _run_in_thread(self._adapter.disconnect)
        self._async_pool = None
        logger.info("Async DB closed")

    def close_sync(self) -> None:
        if self._sync_pool:
            self._sync_pool.close_all()
        if self._adapter:
            self._adapter.disconnect()
        self._sync_pool = None
        logger.info("Sync DB closed")
