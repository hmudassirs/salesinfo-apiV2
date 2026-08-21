"""Policy shared between `SyncConnectionPool` and `AsyncConnectionPool`
that doesn't actually depend on which one you're in.

The framework review's P2 pool item was explicit that the two pools
should stay separate -- their acquire()/release() control flow
(`threading.RLock`+`Condition` vs. `asyncio.Lock`+a hand-rolled waiter
list) is a real mechanism difference, not incidental duplication. What
*was* incidental duplication -- identical code, copy-pasted, with no
mechanism-specific reason for the copy -- lives here instead:

  - `new_wait_histogram()`: the same try/import/fallback dance both
    pools' modules used to each carry independently for degrading
    gracefully when `core.performance` isn't installed.
  - `record_wait()`: the rolling-window + histogram bookkeeping in what
    used to be each pool's own `_record_acquire()`.
  - `build_pool_metrics()` (in `core/db/pool/metrics.py`, next to the
    `PoolMetrics` shape it builds): the wait-time/percentile/
    utilization arithmetic in what used to be each pool's own
    `metrics()`.
  - `best_effort_close_sync()` / `best_effort_close_async()`: the
    "close this already-broken-or-shutting-down connection, and if
    that itself fails, log a debug breadcrumb instead of swallowing it
    silently" pattern each pool's `release(broken=True)` and
    `close_all()` needed twice.

`AdaptiveSizer` (capacity policy) already lives in its own
`core/db/pool/adaptive.py` for the same reason -- this module is that
same idea applied to the metrics/close-handling pieces that hadn't
been split out yet.
"""

from __future__ import annotations

from typing import List

try:
    from core.performance.histogram import StreamingHistogram
    from core.performance.types import MetricName

    def new_wait_histogram() -> "StreamingHistogram":
        return StreamingHistogram(name=MetricName("pool_wait_ms"))

except Exception:  # core.performance not installed -- degrade gracefully
    StreamingHistogram = None  # type: ignore[assignment,misc]

    def new_wait_histogram():
        return None


def record_wait(
    *,
    wait_histogram,
    recent_waits_ms: List[float],
    recent_window: int,
    wait_seconds: float,
) -> None:
    """Feed one acquire()'s wait time into the histogram (if
    `core.performance` is installed) and the small rolling window
    `AdaptiveSizer` uses for a same-tick pressure signal. Mutates
    `recent_waits_ms` in place -- it's each pool's own instance list,
    passed in rather than owned here, so this stays a pure bookkeeping
    step rather than something that needs its own state.
    """
    wait_ms = wait_seconds * 1000.0
    if wait_histogram is not None:
        wait_histogram.observe(wait_ms)
    recent_waits_ms.append(wait_ms)
    if len(recent_waits_ms) > recent_window:
        recent_waits_ms.pop(0)


def best_effort_close_sync(conn, *, logger, context: str) -> None:
    """Close `conn`, logging (not raising) if the close itself fails.

    Used for connections we're discarding anyway (a broken connection
    being evicted, or every connection during pool shutdown) -- a
    close failure there isn't actionable and shouldn't stop the rest
    of cleanup, but it's still worth a debug breadcrumb if someone's
    chasing a pattern of connections going bad. `context` names which
    of those two call sites this is, for that breadcrumb.
    """
    if not hasattr(conn, "close"):
        return
    try:
        conn.close()
    except Exception:
        logger.debug("Failed to close connection (%s)", context, exc_info=True)


async def best_effort_close_async(conn, *, logger, context: str) -> None:
    """Async counterpart of `best_effort_close_sync()` -- same
    reasoning, handling both a plain sync `close()` and an awaitable
    one (mirrors the `hasattr(maybe_close, "__await__")` check the
    async pool's `release()` already used for this)."""
    if not hasattr(conn, "close"):
        return
    try:
        maybe_close = conn.close()
        if hasattr(maybe_close, "__await__"):
            await maybe_close
    except Exception:
        logger.debug("Failed to close connection (%s)", context, exc_info=True)
