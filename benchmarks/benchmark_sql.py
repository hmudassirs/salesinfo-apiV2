#!/usr/bin/env python3
"""Benchmark `core.storage.application_state_store.ApplicationStateStore` execute/fetch throughput.

Compares, against a real PostgreSQL database seeded with 200 rows:

- `raw-sql`         : call `ApplicationStateStore.fetch_one` directly.
- `instrumented-sql`: the same database wrapped in
                       `core.performance.adapters.application_state.InstrumentedApplicationStateStore`,
                       with no profiler bound (the common, unsampled case).
- `instrumented-sql-profiled`: the same wrapped database, with a real
                       `RequestProfiler` bound for every call — what a
                       *sampled* request actually pays.

Needs a reachable PostgreSQL server -- the application state store has no
other backend (see core/storage/application_state_store.py's module docstring).
Point it at one with the same PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
env vars run_api.py uses (defaults: localhost:5432/postgres/postgres).
Seeded rows are cleaned up (best-effort) after each run, so this is
safe to point at a real database, not just a disposable local one.

Usage:
    python -m benchmarks.benchmark_sql
    python -m benchmarks.benchmark_sql --iterations 2000
"""

from __future__ import annotations

import os

from benchmarks._common import (
    build_arg_parser,
    render_report,
    run_benchmark,
    write_json_results,
)
from core.performance.adapters.application_state import InstrumentedApplicationStateStore
from core.performance.context import bind_profiler
from core.performance.request_profiler import RequestProfiler
from core.storage.application_state_store import ApplicationStateStore
from core.storage.schema import ApplicationStateSchema

_SEED_ROWS = 200
_SELECT_SQL = "SELECT username FROM users WHERE user_id = ?"
_USER_ID_PREFIX = "benchmark_sql_u"


def _pg_kwargs() -> dict:
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "database": os.getenv("PGDATABASE", "postgres"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
    }


def _build_seeded_database() -> ApplicationStateStore:
    db = ApplicationStateStore.for_postgres(min_size=2, max_size=8, **_pg_kwargs())
    db.connect()
    ApplicationStateSchema(db).create()
    for i in range(_SEED_ROWS):
        db.execute(
            "INSERT INTO users "
            "(user_id, username, email, password_hash, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (user_id) DO NOTHING",
            (
                f"{_USER_ID_PREFIX}{i}",
                f"benchmark_sql_user{i}",
                f"benchmark_sql_user{i}@example.com",
                "hash",
                1,
                1,
            ),
        )
    return db


def _cleanup_seeded_rows(db: ApplicationStateStore) -> None:
    db.execute(
        "DELETE FROM users WHERE user_id LIKE ?", (f"{_USER_ID_PREFIX}%",)
    )


def main() -> None:
    parser = build_arg_parser(__doc__ or "")
    args = parser.parse_args()

    raw_db = _build_seeded_database()
    instrumented_db = InstrumentedApplicationStateStore(_build_seeded_database())
    profiled_db = InstrumentedApplicationStateStore(_build_seeded_database())

    def _run_profiled() -> None:
        with bind_profiler(RequestProfiler()):
            profiled_db.fetch_one(_SELECT_SQL, (f"{_USER_ID_PREFIX}1",))

    try:
        results = [
            run_benchmark(
                "raw-sql",
                args.iterations,
                lambda: raw_db.fetch_one(_SELECT_SQL, (f"{_USER_ID_PREFIX}1",)),
            ),
            run_benchmark(
                "instrumented-sql",
                args.iterations,
                lambda: instrumented_db.fetch_one(
                    _SELECT_SQL, (f"{_USER_ID_PREFIX}1",)
                ),
            ),
            run_benchmark(
                "instrumented-sql-profiled", args.iterations, _run_profiled
            ),
        ]
    finally:
        for db in (raw_db, instrumented_db.application_state, profiled_db.application_state):
            _cleanup_seeded_rows(db)
        raw_db.disconnect()
        instrumented_db.disconnect()
        profiled_db.disconnect()

    print(render_report(results))  # noqa: T201
    if args.json_out:
        write_json_results(args.json_out, results)


if __name__ == "__main__":
    main()
