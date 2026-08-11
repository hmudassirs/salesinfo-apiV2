"""FastAPI wiring for the read-only debug dashboard (Phase 11).

The only FastAPI-dependent module in `dashboard/`, mirroring
`middleware/fastapi.py`'s split: `summary.py` holds the
framework-independent data shaping, this module only wires the routes
onto an `APIRouter` and includes it on the app.

Like the middleware, it knows nothing about auth by default — pass
`dependencies` if the two *data* routes should require it (e.g.
`dependencies=[Depends(require_admin_user)]`), matching `APIRouter`'s
own parameter of that name. Deliberately **not** applied to `{prefix}/`
(the HTML shell page): a browser navigating there has no way to attach
an `x-api-key`/`Authorization` header, so if the shell page required
the same auth as its own data, nobody could ever load it far enough to
see the API-key input box that lets them authenticate its `fetch()`
calls. The shell page itself renders no data — only `{prefix}/performance`
and `{prefix}/request/{{id}}` do, and those stay behind `dependencies`.
See `core.auth.middleware.PUBLIC_PATHS` for the matching exemption at
the auth-middleware level (this router's own dependency isn't the only
wall the shell page has to get past in this app).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.params import Depends
from fastapi.responses import HTMLResponse

from core.performance.dashboard.live import render_dashboard_html
from core.performance.dashboard.merge import merge_performance_summaries
from core.performance.dashboard.summary import (
    DEFAULT_RECENT_REQUESTS_LIMIT,
    build_performance_summary,
)
from core.performance.registry import PerformanceRegistry, get_default_registry

logger = logging.getLogger(__name__)


def install_performance_dashboard(
    app: FastAPI,
    registry: PerformanceRegistry | None = None,
    prefix: str = "/debug",
    dependencies: Sequence[Depends] | None = None,
    refresh_interval_seconds: float = 2.0,
    cross_process_max_age_seconds: float = 6.0,
) -> None:
    """Register the read-only debug dashboard routes: `{prefix}/`
    (a lightweight, auto-refreshing live HTML view — throughput, latency
    percentiles, pool contention, SQL/trace-stage timing, CPU/memory, and
    recent request traces), `{prefix}/performance` (its JSON data
    source), and `{prefix}/request/{{id}}` (one retained request's full
    trace).

    `registry` defaults to the process-wide default registry — the same
    default `install_performance_middleware` uses — so wiring both with
    no arguments is enough for the dashboard to reflect what the
    middleware recorded.

    `dependencies` gates only `{prefix}/performance` and
    `{prefix}/request/{{id}}` — not `{prefix}/` itself; see the module
    docstring for why.

    `cross_process_max_age_seconds`: with `--workers N > 1`, `{prefix}/
    performance` merges in every *other* worker's most recently
    published snapshot too (see `core.performance.cross_process` and
    `core.performance.dashboard.merge`) — not just whichever one
    process happened to serve this particular request — as long as
    that worker published within this many seconds. Should stay a
    small multiple of `PerformanceStep`'s publish interval
    (`PERF_CROSS_PROCESS_PUBLISH_INTERVAL_SECONDS`, default 2s) so a
    worker that's crashed or been killed ages out of the merged view
    promptly instead of leaving a stale ghost entry.
    """
    resolved_registry = registry or get_default_registry()
    router = APIRouter(prefix=prefix, tags=["performance"])
    data_route_dependencies = list(dependencies or [])

    @router.get("/", response_class=HTMLResponse)
    def get_live_dashboard() -> str:
        """Serve the live, auto-refreshing dashboard page (no auth: static, no data)."""
        return render_dashboard_html(
            prefix=prefix, refresh_interval_seconds=refresh_interval_seconds
        )

    @router.get("/performance", dependencies=data_route_dependencies)
    def get_performance_summary(
        request: Request,
        recent_limit: int = DEFAULT_RECENT_REQUESTS_LIMIT,
    ) -> dict[str, object]:
        """Return current counters, gauges, histograms, and recent request history.

        With more than one worker process, this is a fleet-wide merge
        across every worker that's published a snapshot recently (see
        `cross_process_max_age_seconds` above), not just this one
        process — falling back to this process's own local registry
        alone if the cross-process store isn't reachable (e.g. running
        via `create_app()` without `ApplicationLifespan`, as tests and
        benchmarks do, or the very first requests before migrations
        have created `perf_worker_snapshots`).
        """
        local_summary = build_performance_summary(
            resolved_registry, recent_limit=recent_limit
        )
        container = getattr(request.app.state, "container", None)
        service_db = container.get_service_db() if container is not None else None
        if service_db is None:
            return local_summary

        from core.performance.cross_process import WorkerSnapshotStore

        try:
            store = WorkerSnapshotStore(service_db)
            snapshots = store.collect_fresh(cross_process_max_age_seconds)
        except Exception:
            logger.warning(
                "Cross-process performance snapshot collection failed; "
                "falling back to this worker's own data only.",
                exc_info=True,
            )
            return local_summary

        if not snapshots:
            # No fresh rows yet (e.g. PerformanceStep's publish loop
            # hasn't run its first iteration) -- this worker's own live
            # registry is still the best available answer.
            return local_summary
        return merge_performance_summaries(snapshots, recent_limit=recent_limit)

    @router.get("/request/{request_id}", dependencies=data_route_dependencies)
    def get_request_detail(request_id: str) -> dict[str, object]:
        """Return one retained completed request's full trace and metrics."""
        profile = resolved_registry.get_request(request_id)
        if profile is None:
            raise HTTPException(
                status_code=404, detail=f"no retained request with id {request_id!r}"
            )
        return profile.to_dict()

    app.include_router(router)
