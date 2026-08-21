"""Instrumented, pool-level transaction context managers.

`instrumented_transaction`/`instrumented_async_transaction` are the
only transaction-context-manager implementation in this codebase --
acquire, `begin()`, yield the connection, `commit()` on normal exit or
best-effort `rollback()` and re-raise on exception, always release --
with `PerformanceStage.TRANSACTION_BEGIN/COMMIT/ROLLBACK` timers around
each step when a profiler is bound (`get_current_profiler()` is `None`
outside of instrumented request handling, in which case these behave
exactly like a plain, uninstrumented transaction context manager --
the timing is pure overhead-when-absent, never a behavior change).

Calls `conn.begin()`/`commit()`/`rollback()` (see
`core.db.session.AsyncSessionWrapper`/`SyncSessionWrapper`), not SQL
`BEGIN`/`COMMIT`/`ROLLBACK` statements routed through `execute()` — the
latter looked equivalent but never actually delimited one atomic
transaction, since `execute()` commits immediately after every
statement it runs, including a `"BEGIN"` or `"COMMIT"` string itself.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from core.performance.adapters.pool import _AsyncPoolLike, _SyncPoolLike
from core.performance.context import get_current_profiler
from core.performance.enums import PerformanceStage
from core.performance.types import MetricName

logger = logging.getLogger(__name__)


@contextmanager
def instrumented_transaction(pool: _SyncPoolLike) -> Iterator[Any]:
    """Acquire one connection from `pool` for the duration of the
    `with` block, sharing a single real transaction across every
    statement run against it: commits on normal exit, best-effort
    rolls back and re-raises on exception, always releases the
    connection back to the pool.
    """
    profiler = get_current_profiler()
    conn = pool.acquire()
    try:
        if hasattr(conn, "begin"):
            if profiler is None:
                conn.begin()
            else:
                with profiler.stage(
                    PerformanceStage.TRANSACTION_BEGIN, MetricName("transaction_begin")
                ):
                    conn.begin()
        yield conn
        if hasattr(conn, "commit"):
            if profiler is None:
                conn.commit()
            else:
                with profiler.stage(
                    PerformanceStage.TRANSACTION_COMMIT,
                    MetricName("transaction_commit"),
                ):
                    conn.commit()
    except Exception:
        if hasattr(conn, "rollback"):
            try:
                if profiler is None:
                    conn.rollback()
                else:
                    with profiler.stage(
                        PerformanceStage.TRANSACTION_ROLLBACK,
                        MetricName("transaction_rollback"),
                    ):
                        conn.rollback()
            except Exception:
                # The connection is already being torn down over a
                # real error (about to be re-raised below); a
                # rollback failure on top of that is logged, not
                # raised, so it can't mask the original exception.
                logger.debug("Rollback failed during transaction cleanup", exc_info=True)
        raise
    finally:
        pool.release(conn)


@asynccontextmanager
async def instrumented_async_transaction(pool: _AsyncPoolLike) -> AsyncIterator[Any]:
    """Async counterpart of `instrumented_transaction`; see its
    docstring."""
    profiler = get_current_profiler()
    conn = await pool.acquire()
    try:
        if hasattr(conn, "begin"):
            if profiler is None:
                await conn.begin()
            else:
                with profiler.stage(
                    PerformanceStage.TRANSACTION_BEGIN, MetricName("transaction_begin")
                ):
                    await conn.begin()
        yield conn
        if hasattr(conn, "commit"):
            if profiler is None:
                await conn.commit()
            else:
                with profiler.stage(
                    PerformanceStage.TRANSACTION_COMMIT,
                    MetricName("transaction_commit"),
                ):
                    await conn.commit()
    except Exception:
        if hasattr(conn, "rollback"):
            try:
                if profiler is None:
                    await conn.rollback()
                else:
                    with profiler.stage(
                        PerformanceStage.TRANSACTION_ROLLBACK,
                        MetricName("transaction_rollback"),
                    ):
                        await conn.rollback()
            except Exception:
                # The connection is already being torn down over a
                # real error (about to be re-raised below); a
                # rollback failure on top of that is logged, not
                # raised, so it can't mask the original exception.
                logger.debug("Rollback failed during transaction cleanup", exc_info=True)
        raise
    finally:
        await pool.release(conn)
