# salesdata-api

A FastAPI service that exposes a PostgreSQL database over HTTP:
authenticated ad-hoc SQL (`/api/query`), table introspection, user/API-key
management, and operational metrics (pool/executor/cache/concurrency health
at `/api/health` and `/api/dashboard`).

## Requirements

- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) (dependencies are pinned in `uv.lock` /
  `requirements.txt`), or plain `pip install -e .`
- A PostgreSQL server — `psycopg2-binary` is already in `requirements.txt`.

## Setup

```bash
uv sync                    # or: pip install -e .

# Required: signing secret for JWTs issued by /api/auth/users/login.
# There is no default -- the app refuses to start without one.
export JWT_SECRET_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")

# Point at your Postgres server -- one DSN, or the discrete PG* vars.
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
# or: export PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... PGPASSWORD=... PGSSLMODE=...

python run_api.py          # http://localhost:8000, docs at /docs
# or: uvicorn run_api:app --host 127.0.0.1 --port 8000 --workers 4
```

A `.env.dev` file at the project root (loaded automatically by `run_api.py`
via `load_dotenv(".env.dev")`) is a convenient place to set all of the above
for local development instead of exporting them by hand.

There is one PostgreSQL database backing everything -- the application data
and the application state tables (users, API keys, query result cache L2, logs,
traces, audit log) live side by side in it; see
`core/storage/application_state_store.py`'s module docstring for why that's a deliberate
choice, not a temporary one. Being one real database (not an embedded,
on-disk, single-process file) is what makes `--workers N>1` safe, and what
makes the per-process JWT revocation record and auth rate limiter
(`core/auth/shared_state.py`) cross-process/instance-coordinated
automatically, since they piggyback on the same connection.

`run_api.py` applies pending migrations and seeds an initial admin user on
first run -- see `bootstrap_admin.py` for the seeding logic, and the
printed startup output for its credentials (or set
`INITIAL_ADMIN_USERNAME`/`INITIAL_ADMIN_EMAIL`/`INITIAL_ADMIN_PASSWORD`
before startup to control them yourself).

A few things stay **per-process** regardless of instance count: the
API-key validation cache, JWT role-revocation record's local cache, and
the query cache's L1 layer (see their own docstrings). Multiple
workers/instances each get their own smaller copy of these rather than a
shared one -- a correctness-preserving efficiency/consistency tradeoff,
worth knowing before scaling out, not a startup-time failure.

See `run_api.py`'s module docstring for the full list of pool/executor-sizing
environment variables (`APPLICATION_DATA_POOL_MIN_SIZE`, `APPLICATION_DATA_EXECUTOR_WORKERS`, etc.).

## Schema migrations

The schema is managed through versioned SQL files under
`migrations/postgresql/`, tracked in a `schema_migrations` table --
not applied ad hoc at startup, and not Alembic (this codebase talks to
Postgres through a hand-written adapter, not an ORM, so Alembic's
SQLAlchemy-metadata model doesn't have anything to hook into here). Applied
at startup by `ApplicationDataStep`. See `core/db/migrations.py`'s module
docstring for the full design.

To add a schema change: drop a new `NNNN_description.sql` file (next
version number, zero-padded to 4 digits) into `migrations/postgresql/`.
Every statement in it should be idempotent (`CREATE TABLE IF NOT EXISTS`,
etc.) -- see the migrations module docstring for why that matters here
specifically.

## Key configuration

Everything below is read from the environment at startup
(`core/app/settings.py`/`run_api.py`); all except `JWT_SECRET_KEY` have sane
defaults. See `.env.dev` for the full annotated list, including
performance/OTel and pool-sizing knobs.

| Variable | Default | Purpose |
|---|---|---|
| `JWT_SECRET_KEY` | *(required)* | Signs session JWTs issued at login. |
| `DATABASE_URL` / `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE` | `PGHOST=localhost` etc. | Postgres connection. `DATABASE_URL` takes precedence over the discrete vars when both are set. |
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
  the "write" scope (see `REQUIRE_WRITE_SCOPE_FOR_MUTATIONS` above). Accepts
  `?`-placeholder SQL, translated to psycopg2's `%s` style internally
  (`core/db/adapters/postgresql.py:translate_qmark_placeholders`).
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
  db/           connection pool, PostgreSQL adapter, SQL policy,
                migration runner (migrations.py)
  caching/      two-level (in-process + Postgres) query result cache
  concurrency/  per-workload thread pools, query concurrency limits
  observability/ request context, logging/tracing/audit, alerts
  services/     QueryService -- the core /api/query execution path
  storage/      Postgres-backed application state store (users, keys, cache, audit)
migrations/
  postgresql/   PostgreSQL schema (see "Schema migrations" above)
run_api.py      entrypoint
bootstrap_admin.py  seed/reset the initial admin user
load_test.py    standalone load-test script
```

## Testing

No automated test suite exists yet.

