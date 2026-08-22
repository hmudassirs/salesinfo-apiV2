#!/usr/bin/env python3
"""Benchmark a representative FastAPI app's request cycle end to end.

Builds one minimal app with two routes — `/fast` (returns immediately)
and `/query` (a SQL read via `InstrumentedApplicationStateStore.fetch_one`,
dispatched through `asyncio.to_thread` the way this codebase's real
query route keeps synchronous DB work off the event loop) — served
in-process over `httpx.ASGITransport` (no real socket, no external
process) under concurrent load.

Compares two named runs against the *same* app and routes:

- `baseline` : `install_performance_middleware` with profiling disabled
               (`PerformanceConfig(enabled=False)`).
- `candidate`: the same app, profiling enabled at a 100% sample rate
               (`PerformanceConfig(enabled=True, sample_rate_percent=100)`).

This is the shape `docs/PerformancePlan.md` Phase 12 asks for:
throughput, latency percentiles, and errors for each named run, a
before/after `compare()`, and — for `candidate`, the only run that
retains request history — a trace-stage breakdown showing where time
actually went (`pool_acquire`, `sql_execute`, ...).

Usage:
    python -m benchmarks.benchmark_api
    python -m benchmarks.benchmark_api --iterations 1000 --concurrency 20
"""

from __future__ import annotations

import asyncio

import httpx
from fastapi import FastAPI

from benchmarks._common import (
    BenchmarkResult,
    build_application_state_store,
    build_arg_parser,
    compare,
    render_report,
    render_stage_breakdown,
    stage_breakdown,
    write_json_results,
)
from core.app.settings import AppSettings
from core.performance.adapters.application_state import InstrumentedApplicationStateStore
from core.performance.config import PerformanceConfig
from core.performance.middleware.fastapi import install_performance_middleware
from core.performance.registry import PerformanceRegistry
from core.storage.application_state_store import ApplicationStateStore
from core.storage.schema import ApplicationStateSchema

_SELECT_SQL = "SELECT username FROM users WHERE user_id = %s"
_USER_ID = "benchmark_api_u1"


def _build_seeded_database(settings: AppSettings) -> ApplicationStateStore:
    """Needs a reachable PostgreSQL server -- the application state store has
    no other backend (see core/storage/application_state_store.py's module
    docstring). Configure it through the same `AppSettings.from_env()` path
    as run_api.py (the `DATABASE_URL` or `PG*` environment variables;
    defaults: localhost:5432/postgres/postgres)."""
    db = build_application_state_store(settings)
    db.connect()
    ApplicationStateSchema(db).create()
    db.execute(
        "INSERT INTO users "
        "(user_id, username, email, password_hash, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (user_id) DO NOTHING",
        (_USER_ID, "benchmark_api_alice", "benchmark_api_alice@example.com", "hash", 1, 1),
    )
    return db


def _build_app(
    config: PerformanceConfig, registry: PerformanceRegistry, settings: AppSettings
) -> FastAPI:
    app = FastAPI()
    install_performance_middleware(app, config=config, registry=registry)
    instrumented_db = InstrumentedApplicationStateStore(_build_seeded_database(settings))
    app.state.application_state = instrumented_db.application_state  # for cleanup in _run_named

    @app.get("/fast")
    async def fast() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/query")
    async def query() -> dict[str, object]:
        row = await asyncio.to_thread(instrumented_db.fetch_one, _SELECT_SQL, (_USER_ID,))
        return {"row": dict(row) if row else None}

    return app


async def _drive_load(
    app: FastAPI, iterations: int, concurrency: int
) -> tuple[list[float], int]:
    """Issue `iterations` requests split across `concurrency` workers.

    Alternates `/fast` and `/query` per request so both a no-DB and a
    DB-backed route contribute to the aggregate numbers, the same way a
    real API's traffic mix would.
    """
    durations: list[float] = []
    errors = 0
    lock = asyncio.Lock()

    async def _worker(worker_iterations: int, client: httpx.AsyncClient) -> None:
        nonlocal errors
        for i in range(worker_iterations):
            path = "/fast" if i % 2 == 0 else "/query"
            start = asyncio.get_running_loop().time()
            try:
                response = await client.get(path)
                response.raise_for_status()
            except Exception:
                async with lock:
                    errors += 1
            durations.append(asyncio.get_running_loop().time() - start)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://benchmark"
    ) as client:
        per_worker = max(1, iterations // concurrency)
        await asyncio.gather(
            *(_worker(per_worker, client) for _ in range(concurrency))
        )
    return durations, errors


async def _run_named(
    name: str,
    config: PerformanceConfig,
    settings: AppSettings,
    iterations: int,
    concurrency: int,
) -> tuple[BenchmarkResult, PerformanceRegistry]:
    registry = PerformanceRegistry()
    app = _build_app(config, registry, settings)
    loop_start = asyncio.get_running_loop().time()
    try:
        durations, errors = await _drive_load(app, iterations, concurrency)
    finally:
        application_state: ApplicationStateStore = app.state.application_state
        application_state.execute("DELETE FROM users WHERE user_id = %s", (_USER_ID,))
        application_state.disconnect()
    total_seconds = asyncio.get_running_loop().time() - loop_start
    result = BenchmarkResult(
        name=name,
        iterations=len(durations),
        total_seconds=total_seconds,
        errors=errors,
        durations_seconds=tuple(durations),
    )
    return result, registry


async def _main_async(
    settings: AppSettings, iterations: int, concurrency: int
) -> tuple[
    list[BenchmarkResult], dict[str, dict[str, float]]
]:
    baseline_result, _baseline_registry = await _run_named(
        "baseline", PerformanceConfig(enabled=False), settings, iterations, concurrency
    )
    candidate_result, candidate_registry = await _run_named(
        "candidate",
        PerformanceConfig(enabled=True, sample_rate_percent=100),
        settings,
        iterations,
        concurrency,
    )
    return [baseline_result, candidate_result], stage_breakdown(candidate_registry)


def main() -> None:
    parser = build_arg_parser(__doc__ or "")
    parser.add_argument(
        "--concurrency", type=int, default=10, help="Concurrent in-flight requests."
    )
    args = parser.parse_args()
    settings = AppSettings.from_env(require_jwt_secret=False)

    results, breakdown = asyncio.run(
        _main_async(settings, args.iterations, args.concurrency)
    )

    print(render_report(results))  # noqa: T201
    print()  # noqa: T201
    print(compare(results[0], results[1]))  # noqa: T201
    print()  # noqa: T201
    print("candidate trace-stage breakdown:")  # noqa: T201
    print(render_stage_breakdown(breakdown))  # noqa: T201

    if args.json_out:
        write_json_results(args.json_out, results)


if __name__ == "__main__":
    main()
