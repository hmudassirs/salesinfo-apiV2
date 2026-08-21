from typing import Optional, TypedDict


class PoolMetrics(TypedDict):
    # Sizing
    min_connections: int
    max_connections: int
    current_connections: int
    active_connections: int
    idle_connections: int

    # Queueing (Phase 2 additions -- see roadmap section 5)
    queue_depth: int
    current_waiters: int
    total_waiters: int

    # Acquire outcomes
    total_requests: int
    total_acquires: int
    successful_acquires: int
    timed_out_acquires: int
    pool_hits: int
    pool_misses: int
    timeout_errors: int

    # Wait-time distribution
    total_wait_time: float
    avg_wait_time_ms: float
    p50_wait_time_ms: Optional[float]
    p95_wait_time_ms: Optional[float]
    p99_wait_time_ms: Optional[float]

    # Connection creation cost
    connection_creation_count: int
    connection_creation_time_ms: float
    broken_connections_evicted: int

    # Pressure summary
    utilization: float
    saturation: float


def build_pool_metrics(
    *,
    min_connections: int,
    current_size: int,
    active: int,
    idle: int,
    reserved: int,
    current_waiters: int,
    total_waiters: int,
    requests: int,
    successful_acquires: int,
    timeouts: int,
    hits: int,
    misses: int,
    wait_time: float,
    wait_histogram,
    connection_creation_count: int,
    connection_creation_time: float,
    broken_evicted: int,
) -> PoolMetrics:
    """Turn a pool's raw counters into the `PoolMetrics` shape.

    `SyncConnectionPool.metrics()` and `AsyncConnectionPool.metrics()`
    were previously two independent, hand-written copies of this exact
    computation (average/percentile wait time, utilization, saturation)
    -- the review flagged this as the one part of the two pools'
    "genuinely differ, don't merge them" split that actually *was* pure
    duplicated policy rather than a real sync/async difference. Locking
    and adaptive-sizing (`AdaptiveSizer.adjust()`) stay in each pool's
    own `metrics()` -- those two are the parts that really do differ
    (a `threading.RLock` vs. no lock needed, since the async pool's
    `metrics()` doesn't await anything) -- only the arithmetic below,
    which is identical either way, moved here.
    """
    avg_wait_ms = (wait_time / max(1, successful_acquires + timeouts)) * 1000.0

    p50 = p95 = p99 = None
    if wait_histogram is not None and wait_histogram.histogram.count:
        snap = wait_histogram.snapshot()
        p50, p95, p99 = snap.get("p50"), snap.get("p95"), snap.get("p99")

    return {
        "min_connections": min_connections,
        "max_connections": current_size,
        "current_connections": active + idle,
        "active_connections": active,
        "idle_connections": idle,
        "queue_depth": current_waiters,
        "current_waiters": current_waiters,
        "total_waiters": total_waiters,
        "total_requests": requests,
        "total_acquires": requests,
        "successful_acquires": successful_acquires,
        "timed_out_acquires": timeouts,
        "pool_hits": hits,
        "pool_misses": misses,
        "timeout_errors": timeouts,
        "total_wait_time": round(wait_time, 4),
        "avg_wait_time_ms": round(avg_wait_ms, 4),
        "p50_wait_time_ms": p50,
        "p95_wait_time_ms": p95,
        "p99_wait_time_ms": p99,
        "connection_creation_count": connection_creation_count,
        "connection_creation_time_ms": round(connection_creation_time * 1000.0, 4),
        "broken_connections_evicted": broken_evicted,
        "utilization": round(active / max(1, current_size), 4),
        "saturation": round((active + reserved) / max(1, current_size), 4),
    }
