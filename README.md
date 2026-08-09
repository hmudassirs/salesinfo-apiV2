# salesdata-api

A FastAPI service that exposes a data warehouse (DuckDB or PostgreSQL) over
HTTP: authenticated ad-hoc SQL (`/api/query`), table introspection,
user/API-key management, and operational metrics (pool/executor/cache/
concurrency health at `/api/health` and `/api/dashboard`).

## Requirements

- Python >= 3.9
- [uv](https://docs.astral.sh/uv/) (dependencies are pinned in `uv.lock` /
  `requirements.txt`), or plain `pip install -e .`
- PostgreSQL, if using that warehouse backend (see below) — `psycopg2-binary`
  is already in `requirements.txt`.

## Setup

```bash
uv sync                    # or: pip install -e .

# Required: signing secret for JWTs issued by /api/auth/users/login.
# There is no default -- the app refuses to start without one.
export JWT_SECRET_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")

python run_api.py          # http://localhost:8000, docs at /docs
```

`run_api.py` creates the auxiliary `data/service.db` SQLite database (users,
API keys, query result cache, audit/observability tables) on first run, and
seeds an initial admin user -- see `bootstrap_admin.py` for the seeding
logic and the printed startup output for its credentials.

### Warehouse backend: DuckDB vs PostgreSQL

Set via `WAREHOUSE_BACKEND` (`duckdb` by default, or inferred as
`postgresql` when `DATABASE_URL` is set without an explicit
`WAREHOUSE_BACKEND`):

- **`duckdb`** (default) -- opens `data/mydatabase.duckdb` directly. Good
  for local dev and single-instance deployments. **Run with a single
  worker.** DuckDB's on-disk file only supports one writing OS process at
  a time; `uvicorn run_api:app --workers N>1` will make N-1 workers fail
  at startup. See `run_api.py`'s module docstring for details.
- **`postgresql`** -- connects to a real Postgres server. Use this for
  anything that needs more than one worker process or instance:

  ```bash
  export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
  # or the discrete PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD / PGSSLMODE vars
  uvicorn run_api:app --workers 4
  ```

  The query console (`/api/query`) accepts the same `?`-placeholder SQL
  either way -- the Postgres adapter translates `?` to psycopg2's `%s`
  style internally (`core/db/adapters/postgresql.py:_qmark_to_pyformat`).

Either way, a few things stay **per-process** regardless of backend: the
API-key validation cache, JWT role-revocation record, auth rate limiter,
and the query cache's L1 layer (see their own docstrings). Multiple
workers/instances each get their own smaller copy of these rather than a
shared one -- a correctness-preserving efficiency/consistency tradeoff,
not a startup-time failure like the DuckDB file lock above, but worth
knowing before scaling out. The auxiliary SQLite service database (users,
API keys, cache L2, audit log) is unaffected by `WAREHOUSE_BACKEND` -- it
always uses SQLite in WAL mode, which does support multiple processes.

See `run_api.py`'s module docstring for the full list of pool/executor-sizing
environment variables (`DB_POOL_MIN_SIZE`, `DB_EXECUTOR_WORKERS`, etc.).

## Schema migrations

Both databases' schemas are managed through versioned SQL files under
`migrations/`, tracked in a `schema_migrations` table -- not applied ad hoc
at startup, and not Alembic (this codebase talks to its databases through
hand-written adapters, not an ORM, so Alembic's SQLAlchemy-metadata model
doesn't have anything to hook into here). See `core/db/migrations.py`'s
module docstring for the full design.

- `migrations/service_db/` (SQLite) -- applied synchronously at startup by
  `ServiceDatabaseStep`.
- `migrations/warehouse_postgres/` (PostgreSQL) -- applied at startup by
  `DataWarehouseStep`, only when `WAREHOUSE_BACKEND=postgresql`.

To add a schema change: drop a new `NNNN_description.sql` file (next
version number, zero-padded to 4 digits) into the relevant directory. Every
statement in it should be idempotent (`CREATE TABLE IF NOT EXISTS`, etc.) --
see the migrations module docstring for why that matters here specifically.

## Key configuration

Everything below is read from the environment at startup
(`core/app/settings.py`/`run_api.py`); all except `JWT_SECRET_KEY` have sane
defaults.

| Variable | Default | Purpose |
|---|---|---|
| `JWT_SECRET_KEY` | *(required)* | Signs session JWTs issued at login. |
| `WAREHOUSE_BACKEND` | `duckdb` (or `postgresql` if `DATABASE_URL` is set) | Which warehouse adapter to use. |
| `DATABASE_URL` / `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE` | — | Postgres connection, when `WAREHOUSE_BACKEND=postgresql`. |
| `REQUIRE_WRITE_SCOPE_FOR_MUTATIONS` | `true` | Require the caller's API key/JWT to carry a "write" scope before running INSERT/UPDATE/DELETE/DDL through `/api/query`. |
| `CACHE_INVALIDATION_PRECISE` | `true` | Scope query-cache invalidation to the tables a write statement touched, instead of clearing the entire cache on every write. |
| `AUTH_RATE_LIMIT_ENABLED` | `true` | Rate-limit `/api/auth/users/login` and `/register` per client IP. |
| `AUTH_RATE_LIMIT_MAX_ATTEMPTS` / `AUTH_RATE_LIMIT_WINDOW_SECONDS` | `10` / `60.0` | Limiter thresholds. |

## API surface

- `POST /api/auth/users/register`, `POST /api/auth/users/login` -- no auth
  required (that's how you get your first credential).
- `POST /api/auth/keys` and friends -- mint/list/revoke/delete API keys.
- `GET /api/auth/users`, `PATCH /api/auth/users/{id}/role` -- admin only.
- `POST /api/query` -- run SQL. SELECT/WITH always allowed; writes require
  the "write" scope (see `REQUIRE_WRITE_SCOPE_FOR_MUTATIONS` above).
- `GET /api/tables`, `/api/tables/{name}/schema`, `/api/tables/{name}/count`.
- `GET /api/health`, `GET /api/dashboard` -- operational metrics.

Everything under `/api` (and `/debug`) requires either an `x-api-key`
header or an `Authorization: Bearer <jwt>` header, except the two
registration/login routes above. See `core/auth/middleware.py`.

## Project layout

```
core/
  app/          FastAPI app factory, settings, lifespan, routes
  auth/         passwords, JWT/API-key auth middleware, rate limiting,
                cross-process shared state (shared_state.py)
  db/           connection pool, adapters (DuckDB/Postgres/SQLite), SQL
                policy, migration runner (migrations.py)
  caching/      two-level (in-process + SQLite) query result cache
  concurrency/  per-workload thread pools, query concurrency limits
  observability/ request context, alerts, tracing
  services/     QueryService -- the core /api/query execution path
  storage/      SQLite-backed service database (users, keys, cache, audit)
migrations/
  service_db/           SQLite schema (see "Schema migrations" above)
  warehouse_postgres/   PostgreSQL schema, WAREHOUSE_BACKEND=postgresql only
run_api.py      entrypoint
bootstrap_admin.py  seed/reset the initial admin user
load_test.py    standalone load-test script
```

## Testing

No automated test suite exists yet.
