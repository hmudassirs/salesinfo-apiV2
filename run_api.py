"""FastAPI example runner with proper lifecycle management.

Usage:
    python run_api.py
    # or: uvicorn run_api:app --host 127.0.0.1 --port 8000
    # Then visit http://localhost:8000/docs for interactive API docs

`app` is defined at module level (rather than built inside an async
main()) specifically so this can be launched via `uvicorn run_api:app`
-- which is what uvicorn's own `--workers N` / multi-process launcher
requires, since it needs to import `app` fresh in each worker process.

One PostgreSQL database backs everything -- the application data store and the
application state store (users, API keys, logs, traces, query cache L2, audit
log; see core/storage/application_state_store.py's module docstring for why this
isn't a per-subsystem choice). Configure it with `DATABASE_URL`, or the
discrete `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER`/`PGPASSWORD`/
`PGSSLMODE` vars. Being one real database (not an embedded, on-disk,
single-process file) is also what makes `--workers` safe to set above
1, and what makes the per-process JWT revocation record and auth rate
limiter (core/auth/shared_state.py) cross-process/instance-coordinated
automatically, since they piggyback on the same connection.

The API-key validation cache and the query result cache's L1 layer are
still per-process (see their own docstrings) and don't coordinate
across workers/instances. Multiple workers each get their own
(smaller, less effective) copy of each; it's a correctness-preserving
efficiency/consistency tradeoff, worth knowing before scaling out, but
not a startup-time failure.
"""

import asyncio
import concurrent.futures
from contextlib import asynccontextmanager

from dotenv import load_dotenv

# Must run before any `core.*` import below. core.db.session imports
# core.observability.otel at *module load time* (`_TRACER =
# get_otel_manager().tracer`), which constructs the process-wide
# OpenTelemetryManager singleton and calls .initialize() immediately --
# reading OTEL_SDK_DISABLED / PERF_EXPORT_OTEL from os.environ right
# then. Since that singleton only ever initializes once, loading
# .env.dev any later than this (e.g. after `from core.app.api.app
# import create_app`) means initialize() already ran against whatever
# was in the real shell environment, and .env.dev's OTEL_SDK_DISABLED
# has no effect -- it arrives too late to change a decision that's
# already been made and cached.
load_dotenv(".env.dev")

import uvicorn
from fastapi import FastAPI

from core.app.api.app import create_app
from core.app.lifespan import ApplicationLifespan
from core.app.settings import AppSettings
from core.concurrency.cpu import recommended_sizing
from core.concurrency.executors import configure_executors
from core.db.config import DatabaseConfig, DatabaseSettings
from core.db.settings import PoolSettings


# Historical note: this used to set ONE 300-worker default executor via
# `asyncio.get_running_loop().set_default_executor(...)` and route every
# blocking call in the process through it via `asyncio.to_thread`. That
# fixed the immediate "only 32 threads for everything" ceiling, but still
# left application data queries, state-store calls (auth, query cache), and
# fire-and-forget background writes all competing for the same pool of
# threads -- a burst of slow application data queries could still starve
# API-key validation, purely because they share a thread pool that has
# nothing to do with either workload (roadmap rule #5: no one oversized
# global thread pool for unrelated workloads).
#
# `configure_executors()` (core/concurrency/executors.py) replaces this
# with three separate bounded pools -- application data / application
# state / background -- each
# sized to the connection pool it actually fronts, so one workload's
# load can't starve the others' threads. A small default executor is
# still set for the handful of one-off startup/shutdown calls
# (`core/app/lifespan.py`) and observability's synchronous fallback path
# that don't go through any of the three dedicated executors.
_DEFAULT_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=max(4, recommended_sizing().cpu_count * 2),
    thread_name_prefix="default-io-worker",
)

# --- Module-level setup -----------------------------------------------
# Everything below used to live inside `async def main()`. None of it
# actually needs a running event loop (it's all plain sync config/object
# construction), so it's hoisted to module scope -- the one exception,
# `set_default_executor`, needs `asyncio.get_running_loop()` and is done
# inside `lifespan()` below instead, where a loop is guaranteed to be
# running.
#
# This file is the composition root: `AppSettings.from_env()` is the
# *only* place environment variables are read for runtime
# configuration. Everything below (DB connection, pool sizes, executor
# sizes) is built from the resulting `settings` object, not from a
# second, parallel set of `os.getenv()` calls -- that duplication (one
# tree in AppSettings, a second ad hoc one here) was flagged as the
# biggest remaining consistency problem in the framework review. Pool/
# executor defaults are still ultimately derived from the actual CPU
# count available to this process (core/concurrency/cpu.py) -- that
# derivation now happens once, inside `AppSettings.from_env()`, rather
# than being duplicated here.
settings = AppSettings.from_env()

sizing = recommended_sizing()
print(
    f"🧮 Detected {sizing.cpu_count} usable CPU(s) -- deriving pool/executor "
    f"sizes from that (override with env vars to pin explicit values)"
)

pool_config = PoolSettings(
    min_size=settings.pool.application_data_min_size,
    max_size=settings.pool.application_data_max_size,
    timeout=settings.pool.timeout,
)
db_settings = DatabaseSettings(pool=pool_config)

db_config = DatabaseConfig.from_postgresql(
    dsn=settings.database.dsn,
    host=settings.database.host,
    port=settings.database.port,
    database=settings.database.database,
    user=settings.database.user,
    password=settings.database.password,
    sslmode=settings.database.sslmode,
    settings=db_settings,
)
print(f"🐘 Database: PostgreSQL ({db_config.connection_string})")
print("🗄️  Application data and application state tables (users, API keys, logs, traces, cache, audit) share this one database")

# Size the DB/state executors off the *actual* pool configuration
# above, plus a little headroom for in-flight reservation
# bookkeeping -- there's no benefit to more worker threads than
# there are connections for them to use. `state_workers` covers
# ApplicationStateStore's own pool (see core/storage/application_state_store.py).
configure_executors(
    application_data_workers=settings.executors.application_data_workers,
    state_workers=settings.executors.application_state_workers,
    background_workers=settings.executors.background_workers,
    # CPU-bound, not pool-backed -- see core/concurrency/cpu.py's
    # password_executor_workers docstring for why this isn't derived
    # from state pool size the way application_state_workers is.
    password_workers=settings.executors.password_workers,
)

# Create lifespan manager (async mode for pooled database access).
#
# `settings` (built once, above) is threaded into both
# ApplicationLifespan (so ApplicationServices can build
# authentication_service with the real JWT settings at startup -- see
# ApplicationStateStep's docstring) and create_app() below -- one
# AppSettings instance for the whole process, rather than each
# independently calling AppSettings.from_env() and ending up with two
# separately-parsed copies of the same environment.
lifespan_mgr = ApplicationLifespan(
    db_config,
    mode="async",
    state_pool_min_size=settings.pool.application_state_min_size,
    state_pool_max_size=settings.pool.application_state_max_size,
    settings=settings,
)

# This runner is async-only: every route in core/app/api/routes.py is
# `async def` and calls db_session.get_async_session() / check_async(),
# not the sync equivalents. ApplicationLifespan.mode defaults to
# "sync" (it's shared infra also used by non-async callers), so a
# future edit here that drops the explicit mode="async" kwarg above
# would silently put this server back in sync mode — and the DB
# pool mismatch wouldn't surface until the first request that
# touches the database. Fail here, at import time, instead of there.
if lifespan_mgr.mode != "async":
    # A plain `assert` here is a production invariant, not a debug
    # check -- `python -O` (or PYTHONOPTIMIZE) strips asserts, which
    # would silently let this run in sync mode instead of failing at
    # import time. Use an explicit exception instead so this can never
    # be optimized away.
    raise RuntimeError(
        "run_api.py's routes require ApplicationLifespan(mode='async'); "
        f"got mode={lifespan_mgr.mode!r}"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan: startup and shutdown."""
    # Needs a running loop, so this happens here rather than at module
    # import time (see the module-level comment above).
    asyncio.get_running_loop().set_default_executor(_DEFAULT_EXECUTOR)

    # Startup phase
    await lifespan_mgr.startup_async()

    # Get db_session after startup and attach to app.state
    db_session = lifespan_mgr.get_db_session()
    app.state.db_session = db_session

    # Application services were built by ApplicationLifespan's
    # ApplicationStateStep during startup_async() above — reuse them
    # rather than constructing a second connection. Note
    # ApplicationStateStep.startup_async() still runs its (blocking)
    # psycopg2 setup via asyncio.to_thread internally, so this is
    # safe to call from an async context.
    app.state.application_services = lifespan_mgr.get_application_services()
    app.state.container = lifespan_mgr.get_container()

    if db_session and db_session._async_pool:
        created = await db_session.warmup_async()
        print(f"✓ Pool warmed up: {created} connections pre-created")
        print(f"📊 Pool metrics: {db_session._async_pool.metrics()}")

    print("\n" + "=" * 60)
    print("🚀 PrepareData API Server")
    print("=" * 60)
    print("📍 Starting on http://localhost:8000")
    print("📚 API Docs: http://localhost:8000/docs")
    print("🔍 ReDoc: http://localhost:8000/redoc")
    print("🔑 API Keys: http://localhost:8000/docs#/authentication")
    print("💓 Liveness: http://localhost:8000/live  (unauthenticated)")
    print("✅ Readiness: http://localhost:8000/ready  (unauthenticated)")
    print("=" * 60 + "\n")

    yield

    # Shutdown phase
    await lifespan_mgr.shutdown_async()


# Module-level app object -- this is what `uvicorn run_api:app` imports.
# Building it here (rather than inside main()) is what makes this file
# usable both as `python run_api.py` and as a target for uvicorn's CLI
# (dev reload, or a process manager in front of it), including with
# `--workers > 1` -- see this module's docstring.
app = create_app(
    settings=settings,
    title="PrepareData API",
    version="1.0.0",
    description="Database preparation and management API",
)
app.router.lifespan_context = lifespan


def main():
    """Run via `python run_api.py`. Equivalent to:
    `uvicorn run_api:app --host 127.0.0.1 --port 8000` (single worker --
    `main()`/`python run_api.py` always runs one process either way.
    Multi-worker deployment goes through `uvicorn run_api:app --workers N`
    directly, not this function)."""
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info",
        reload=False,
        loop="uvloop",       # was implicit via "auto"; fails loudly if unavailable (e.g. Windows) instead of silently degrading
        http="httptools",    # same idea — was implicit via "auto"
    )
    server = uvicorn.Server(config)
    asyncio.run(server.serve())


if __name__ == "__main__":
    main()
