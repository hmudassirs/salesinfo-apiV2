import heapq
import threading
from typing import Any, Dict, List, Tuple

from core.db.logger import get_logger

from .adaptive import AdaptiveSizer
from .base import ConnectionState, MaxConnectionsExceeded, PoolConnection, now
from .metrics import PoolMetrics, build_pool_metrics
from .policy import best_effort_close_sync, new_wait_histogram, record_wait

logger = get_logger(__name__)


class SyncConnectionPool:
    """Sync counterpart of `AsyncConnectionPool` -- see its docstring for
    the correctness properties both pools share (no lock held during
    connection creation, no waiter-wakeup leak, queue-pressure-based
    adaptive sizing, and the extended Phase-2 metrics surface)."""

    def __init__(self, create_connection, min_conn=1, max_conn=20, timeout=5):
        self._create = create_connection
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._available: List[Tuple[float, int, Any]] = []
        self._in_use: Dict[int, PoolConnection] = {}
        self._timeout = timeout
        self._counter = 0  # Tiebreaker for heap comparisons

        # Count of threads currently blocked waiting for a connection.
        # Unlike the async pool (which needs one Event per waiter to wake
        # a specific coroutine), every blocked thread here already shares
        # the same `_not_empty` Condition, so a plain counter is enough
        # to report queue depth -- there's no separate "stale waiter"
        # object that can leak.
        self._current_waiters = 0
        self._total_waiters_ever = 0

        self._requests = 0
        self._hits = 0
        self._misses = 0
        self._wait_time = 0.0
        self._timeouts = 0
        self._successful_acquires = 0
        self._connection_creation_count = 0
        self._connection_creation_time = 0.0
        # P1-1: connections evicted because release(conn, broken=True)
        # was called -- see core.db.pool.base.is_connection_level_error
        # for the classifier, and core/db/session.py for where it's applied.
        self._broken_evicted = 0

        self._wait_histogram = new_wait_histogram()
        self._recent_waits_ms: List[float] = []
        self._recent_window = 50

        self._sizer = AdaptiveSizer(min_conn, max_conn)

    def acquire(self):
        start = now()
        self._requests += 1
        deadline = start + self._timeout
        counted_waiter = False

        try:
            with self._lock:
                while True:
                    if self._available:
                        ts, _, conn = heapq.heappop(self._available)
                        self._record_acquire(start, hit=True)
                        self._in_use[id(conn)] = PoolConnection(
                            now(), conn, now(), state=ConnectionState.IN_USE
                        )
                        return conn
                    elif len(self._in_use) < self._sizer.current:
                        # Reserve the slot under the lock, but create the
                        # connection (potentially slow) after releasing
                        # it -- see class docstring.
                        break

                    if not counted_waiter:
                        self._current_waiters += 1
                        self._total_waiters_ever += 1
                        counted_waiter = True

                    remaining = deadline - now()
                    if remaining <= 0:
                        self._timeouts += 1
                        self._wait_time += now() - start
                        raise MaxConnectionsExceeded()
                    # Every connection is checked out -- actually wait for
                    # one to be released (up to what's left of `timeout`)
                    # instead of failing the instant the pool is
                    # momentarily full. release() notifies this condition
                    # when a slot frees up.
                    self._not_empty.wait(timeout=remaining)

                reserved_key = object()
                self._in_use[id(reserved_key)] = None  # placeholder reservation

            # --- creation path: lock already released above ---
            create_start = now()
            try:
                conn = self._create()
            except Exception:
                with self._lock:
                    self._in_use.pop(id(reserved_key), None)
                raise
            creation_elapsed = now() - create_start

            with self._lock:
                self._in_use.pop(id(reserved_key), None)
                self._connection_creation_count += 1
                self._connection_creation_time += creation_elapsed
                self._misses += 1
                self._record_acquire(start, hit=False)
                self._in_use[id(conn)] = PoolConnection(
                    now(), conn, now(), state=ConnectionState.IN_USE
                )
            return conn
        finally:
            if counted_waiter:
                with self._lock:
                    self._current_waiters -= 1

    def _record_acquire(self, start: float, *, hit: bool) -> None:
        wait = now() - start
        self._wait_time += wait
        self._successful_acquires += 1
        if hit:
            self._hits += 1
        record_wait(
            wait_histogram=self._wait_histogram,
            recent_waits_ms=self._recent_waits_ms,
            recent_window=self._recent_window,
            wait_seconds=wait,
        )

    def release(self, conn, *, broken: bool = False) -> None:
        """Return `conn` to the pool, or -- if `broken=True` -- discard
        it instead. See AsyncConnectionPool.release()'s docstring for the
        full reasoning (roadmap 6.4/P1-1); the same logic applies here,
        just under a threading.Lock instead of an asyncio.Lock."""
        with self._lock:
            pc = self._in_use.pop(id(conn), None)
            if pc is None:
                return

            if broken:
                pc.state = ConnectionState.BROKEN
                self._broken_evicted += 1
                best_effort_close_sync(
                    conn, logger=logger, context="broken connection, already discarded"
                )
            else:
                pc.state = ConnectionState.IDLE
                pc.consecutive_errors = 0
                # Use counter as tiebreaker so heapq doesn't compare wrapper objects
                heapq.heappush(self._available, (pc.created_at, self._counter, conn))
                self._counter += 1

            # Notify whether or not the connection went back to
            # `_available`: a broken connection still frees a capacity
            # slot for a fresh one to be created, which is what a
            # waiter is blocked on.
            self._not_empty.notify()

    def close_all(self) -> None:
        """Close every connection this pool ever handed out -- idle and
        in-use alike. Nothing previously closed pooled connections at
        all; DatabaseSession.close_sync() only ever disconnected a
        separate standalone adapter, so every connection actually
        created via this pool's create_connection() leaked its
        underlying handle on shutdown."""
        with self._lock:
            conns = [c for _, _, c in self._available]
            conns += [pc.connection for pc in self._in_use.values() if pc is not None]
            self._available.clear()
            self._in_use.clear()

        for conn in conns:
            best_effort_close_sync(conn, logger=logger, context="pool shutdown")

    def metrics(self) -> PoolMetrics:
        with self._lock:
            recent_avg_ms = (
                sum(self._recent_waits_ms) / len(self._recent_waits_ms)
                if self._recent_waits_ms
                else 0.0
            )
            self._sizer.adjust(
                current_waiters=self._current_waiters,
                recent_wait_ms=recent_avg_ms,
            )

            active = len([v for v in self._in_use.values() if v is not None])
            reserved = len(self._in_use) - active

            return build_pool_metrics(
                min_connections=self._sizer.min,
                current_size=self._sizer.current,
                active=active,
                idle=len(self._available),
                reserved=reserved,
                current_waiters=self._current_waiters,
                total_waiters=self._total_waiters_ever,
                requests=self._requests,
                successful_acquires=self._successful_acquires,
                timeouts=self._timeouts,
                hits=self._hits,
                misses=self._misses,
                wait_time=self._wait_time,
                wait_histogram=self._wait_histogram,
                connection_creation_count=self._connection_creation_count,
                connection_creation_time=self._connection_creation_time,
                broken_evicted=self._broken_evicted,
            )
