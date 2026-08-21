"""Lightweight, dependency-free SQL migration runner.

This codebase is accessed through a hand-written PostgreSQL adapter
rather than an ORM, so a tool like Alembic -- built around SQLAlchemy's
engine and table metadata -- doesn't have anything to hook into here
without first wrapping this codebase in SQLAlchemy just to get
migrations, which would be a bigger change than the problem calls for.
This is the minimum viable alternative: numbered, plain-SQL migration
files, tracked in a `schema_migrations` table, applied in order, each
file as one all-or-nothing unit. Same idea as golang-migrate/dbmate/
Flyway, scaled down to fit a raw-SQL codebase.

Two call sites use this, both against `migrations/postgresql/`
and the same `schema_migrations` table -- there is one PostgreSQL
database for the whole application (see `core.storage.application_state_store`'s
module docstring), so both are tracking migrations against that same
database:

- `core.app.lifecycle.application_data.ApplicationDataStep.startup_async()`
  -- the primary owner. Applied asynchronously (`apply_migrations_async`),
  and always runs first: `core.app.lifecycle.manager.ApplicationLifespan`'s
  hardcoded step list puts `ApplicationDataStep` before
  `ApplicationStateStep` (see that module's docstring), so in the
  async mode this application actually runs in (`run_api.py`), this
  call site is what creates the schema.
- `core.storage.schema.ApplicationStateSchema.create()` -- a
  synchronous fallback (`apply_migrations_sync`), called from
  `ApplicationStateStep.startup_sync()`. This exists for two cases
  `ApplicationDataStep`'s async pass doesn't cover: `mode="sync"`
  deployments (where `ApplicationDataStep.startup_sync()` explicitly
  does *not* apply migrations -- see that step's docstring -- since
  there's no sync equivalent of `apply_migrations_async` today), and
  standalone/test usage of `ApplicationStateStore` without
  `ApplicationDataStep` in the picture at all (e.g. `bootstrap_admin.py`).

Because the step order above is fixed, not a race, the schema is
created by exactly one deterministic call site in any given run --
async mode: `ApplicationDataStep`; sync mode or standalone: whichever
caller reaches `ApplicationStateSchema.create()`. Both call sites are
still idempotent no-ops against an already-current schema (via the
`schema_migrations` tracking table), which matters for the standalone
case above where nothing enforces call order.

A migration file is named `NNNN_description.sql` (four-digit,
zero-padded, strictly increasing -- enforced at load time, since a
duplicate or out-of-order version number is exactly the kind of thing
that should fail loudly at startup rather than apply in a different
order than whoever wrote it intended). Statements within a file are
split on top-level semicolons -- fine for this codebase's migrations
(plain DDL, no stored procedures/triggers with embedded semicolons).

A migration is only recorded as applied in `schema_migrations` after
every statement in its file succeeds, so a file that fails partway
through is retried in full on the next startup -- but that retry is
only *safe* because every statement here is required to be idempotent
(`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, etc.).
That requirement isn't just a style preference: DDL statements
autocommit immediately with psycopg2 as used by
`PostgreSQLAdapter.execute()` (commits after every statement, not once
per migration file) -- so a `CREATE TABLE` earlier in a since-failed
file is NOT rolled back by anything in this module. Non-idempotent DDL
(e.g. `CREATE TABLE` without `IF NOT EXISTS`) would make a retry-after-
partial-failure error out permanently instead of completing on the
next attempt. Every migration file in this repository follows the
idempotent convention; keep doing so for any new one.
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

from core.db.logger import get_logger

logger = get_logger(__name__)

_FILENAME_RE = re.compile(r"^(\d{4})_([A-Za-z0-9_]+)\.sql$")

_INSERT_APPLIED_SQL = (
    "INSERT INTO schema_migrations (version, name, applied_at, checksum) "
    "VALUES (%s, %s, %s, %s)"
)
_BACKFILL_CHECKSUM_SQL = (
    "UPDATE schema_migrations SET checksum = %s WHERE version = %s"
)

# Fixed Postgres advisory-lock key serializing concurrent migration
# application (see apply_migrations_sync's docstring for why this is
# needed at all: `--workers N > 1` starting simultaneously otherwise
# race on `CREATE TABLE IF NOT EXISTS`). An arbitrary constant is
# sufficient -- `pg_advisory_lock`'s bigint keyspace is a flat
# namespace shared by every advisory lock on the connection's current
# database, so this only needs to not collide with some *other* use of
# advisory locks against the same database, which this codebase has
# none of today. Picked by taking a fixed string through Python's hash
# truncated to signed-32-bit range (pg_advisory_lock accepts a single
# bigint or an int, int pair; kept to one 32-bit-safe int for the
# widest driver compatibility) rather than an arbitrary decimal, so
# the origin of the number is obvious from reading this comment rather
# than needing one.
_MIGRATION_LOCK_KEY = 0x6D696772 & 0x7FFFFFFF  # "migr" as bytes, masked positive

# `applied_at` is a Unix timestamp (float, time.time()); DOUBLE
# PRECISION is Postgres's 64-bit float type. `checksum` is nullable so
# rows applied before this column existed (pre-checksum deployments)
# don't need a backfill to remain valid -- see `_check_checksum`'s
# docstring for how a NULL is handled differently from a mismatch.
POSTGRES_TRACKING_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at DOUBLE PRECISION NOT NULL
)
"""

# Runs every time, even against a table that already existed before
# this column was introduced -- `ADD COLUMN IF NOT EXISTS` (Postgres
# 9.6+) makes this idempotent the same way `CREATE TABLE IF NOT
# EXISTS` above is, so upgrading to a checksum-aware version of this
# module needs no separate manual migration step of its own.
POSTGRES_ADD_CHECKSUM_COLUMN_DDL = """
ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT
"""


def _checksum(sql: str) -> str:
    """Content hash identifying exactly what was applied for a given
    migration version, so a file quietly edited after deployment (same
    version number, different SQL) is detected instead of silently
    considered "already applied" -- see `_check_checksum`."""
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    sql: str
    checksum: str


def load_migrations(directory: Path) -> List[Migration]:
    """Read and validate every `NNNN_description.sql` file in
    `directory`, sorted by version.

    Raises:
        ValueError: a filename doesn't match the required pattern, or
            two files share a version number. Both are configuration
            errors in the migrations directory itself -- fail at
            startup, not partway through applying them.
    """
    migrations: List[Migration] = []
    seen_versions: dict[int, Path] = {}

    for path in sorted(directory.glob("*.sql")):
        match = _FILENAME_RE.match(path.name)
        if not match:
            raise ValueError(
                f"Migration file {path} doesn't match the required "
                "'NNNN_description.sql' naming pattern (e.g. "
                "'0001_initial_schema.sql')"
            )
        version = int(match.group(1))
        if version in seen_versions:
            raise ValueError(
                f"Duplicate migration version {version}: "
                f"{seen_versions[version]} and {path}"
            )
        seen_versions[version] = path
        sql = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=version,
                name=match.group(2),
                path=path,
                sql=sql,
                checksum=_checksum(sql),
            )
        )

    migrations.sort(key=lambda m: m.version)
    return migrations


class MigrationChecksumMismatch(RuntimeError):
    """A migration already recorded as applied no longer matches the
    file on disk -- someone edited `NNNN_description.sql` after it was
    deployed (framework review item "migration checksums"). Since
    `schema_migrations` only ever recorded `version`/`name`/
    `applied_at` before, that edit would otherwise be silently invisible:
    the version is already in the tracking table, so the runner would
    just skip it and the database would drift from what the repository
    says it should look like, with no signal anywhere that they'd
    diverged. Raised at startup instead so this fails loudly, the same
    way a duplicate/out-of-order version number already does in
    `load_migrations`.
    """


def _check_checksum(migration: Migration, applied_checksum: Optional[str]) -> None:
    """Compare a migration file's current checksum against what's
    recorded for that version in `schema_migrations`.

    A `None` `applied_checksum` means the row predates this column
    (upgraded from a version of this module without checksums) --
    treated as "unknown, not a mismatch" and only logged, since there's
    no historical value to compare against and refusing to start would
    turn a one-time schema upgrade into a hard outage for every
    existing deployment.
    """
    if applied_checksum is None:
        logger.warning(
            "Migration %04d_%s has no recorded checksum (applied before "
            "checksum tracking existed); recording its current checksum "
            "now. If this file was edited after it was originally "
            "applied, that edit cannot be detected retroactively.",
            migration.version,
            migration.name,
        )
        return
    if applied_checksum != migration.checksum:
        raise MigrationChecksumMismatch(
            f"Migration {migration.version:04d}_{migration.name} has changed "
            "since it was applied: recorded checksum "
            f"{applied_checksum[:12]}... does not match the current file's "
            f"checksum {migration.checksum[:12]}.... Do not edit an "
            "already-applied migration file -- add a new migration "
            "instead. If this change was intentional and every affected "
            "database has already been reconciled by hand, update "
            "schema_migrations.checksum directly to acknowledge it."
        )


def _split_statements(sql: str) -> List[str]:
    """Split a migration file's SQL into individual statements on
    top-level `;`, dropping empty/comment-only fragments and `--`
    line comments. See this module's docstring for the "no embedded
    semicolons in string literals" assumption this relies on."""
    statements = []
    for raw in sql.split(";"):
        lines = [
            line
            for line in raw.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def apply_migrations_sync(db, directory: Path) -> List[str]:
    """Apply any not-yet-applied migrations in `directory` against any
    `db` shaped like `core.storage.application_state_store.ApplicationStateStore`:
    `.execute(sql, params)`, `.fetch_all(sql, params)`, `.transaction()`
    as a context manager yielding an adapter, `.execute_on(adapter,
    sql, params)`.

    Each migration file's statements run through one `db.transaction()`
    (so the tracking-table INSERT is bundled with whatever DML the file
    contains). This does NOT make DDL atomic -- see this module's
    docstring for why (psycopg2 autocommits each DDL statement) -- it's
    only a true all-or-nothing unit for files that are pure DML. What
    actually keeps a DDL migration safe to retry after a partial
    failure is every statement in it being idempotent; see the same
    docstring section.

    The whole call runs under one Postgres session-level advisory lock
    (`_MIGRATION_LOCK_KEY`) held on a single dedicated connection for
    the call's full duration. Without it, `--workers N > 1` starting
    simultaneously (every worker is a separate process, each running
    its own copy of this function against the same database) race on
    `CREATE TABLE IF NOT EXISTS`: the existence check and the create
    aren't atomic across sessions, so two workers can both see "table
    absent" and both attempt the create, and the loser fails with
    Postgres's `duplicate key value violates unique constraint
    "pg_type_typname_nsp_index"` -- a well-known concurrent-DDL
    footgun, not a corrupted migration file. The lock serializes
    workers here instead: the first to arrive applies whatever's
    pending, the rest block briefly then find nothing left to do.
    Session-level (not `pg_advisory_xact_lock`) because `execute_on`
    below already commits per-statement (see the module docstring) --
    a transaction-scoped lock would release after the very first
    statement instead of holding for the whole call. Released
    explicitly in `finally` (not by connection close) so the pooled
    connection returns to the pool immediately, unlocked, even if a
    migration fails partway through.

    Returns:
        Names of migrations actually applied this call (empty if the
        schema was already current).
    """
    newly_applied: List[str] = []
    with db.transaction() as adapter:
        db.execute_on(adapter, "SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_KEY,))
        try:
            db.execute_on(adapter, POSTGRES_TRACKING_TABLE_DDL)
            db.execute_on(adapter, POSTGRES_ADD_CHECKSUM_COLUMN_DDL)
            # No fetch_all_on() sibling to fetch_one_on() exists on
            # ApplicationStateStore, so this goes straight to the adapter's
            # own .execute(...).fetchall() -- the same primitive
            # fetch_one_on() itself is built on (see that method above).
            applied_checksums = {
                row["version"]: row["checksum"]
                for row in adapter.execute(
                    "SELECT version, checksum FROM schema_migrations"
                ).fetchall()
            }

            for migration in load_migrations(directory):
                if migration.version in applied_checksums:
                    applied_checksum = applied_checksums[migration.version]
                    _check_checksum(migration, applied_checksum)
                    if applied_checksum is None:
                        db.execute_on(
                            adapter,
                            _BACKFILL_CHECKSUM_SQL,
                            (migration.checksum, migration.version),
                        )
                    continue

                label = f"{migration.version:04d}_{migration.name}"
                logger.info("Applying migration %s", label)
                try:
                    for statement in _split_statements(migration.sql):
                        db.execute_on(adapter, statement)
                    db.execute_on(
                        adapter,
                        _INSERT_APPLIED_SQL,
                        (
                            migration.version,
                            migration.name,
                            time.time(),
                            migration.checksum,
                        ),
                    )
                except Exception:
                    logger.exception(
                        "Migration %s failed; no partial changes from "
                        "this file were recorded as applied",
                        label,
                    )
                    raise
                newly_applied.append(label)
        finally:
            db.execute_on(
                adapter, "SELECT pg_advisory_unlock(%s)", (_MIGRATION_LOCK_KEY,)
            )

    if newly_applied:
        logger.info(
            "Applied %s migration(s): %s", len(newly_applied), ", ".join(newly_applied)
        )
    return newly_applied


async def apply_migrations_async(
    session_factory: Callable, directory: Path
) -> List[str]:
    """Async equivalent of `apply_migrations_sync`, for a Postgres
    connection accessed via an async context manager (e.g.
    `db_session.get_async_session`), which is expected to expose
    `execute(sql, params)` / `fetch_all(sql, params)`.

    The whole call runs under one dedicated session for its full
    duration (unlike a prior version of this function, which opened a
    fresh session per migration file) so the session-level advisory
    lock acquired at the top stays held on the same physical connection
    throughout -- see `apply_migrations_sync`'s docstring for why the
    lock exists and why it must be session- rather than
    transaction-scoped here.

    Returns:
        Names of migrations actually applied this call.
    """
    newly_applied: List[str] = []
    async with session_factory() as session:
        await session.execute("SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_KEY,))
        try:
            await session.execute(POSTGRES_TRACKING_TABLE_DDL)
            await session.execute(POSTGRES_ADD_CHECKSUM_COLUMN_DDL)
            applied_rows = await session.fetch_all(
                "SELECT version, checksum FROM schema_migrations"
            )
            applied_checksums = {
                row["version"]: row["checksum"] for row in applied_rows
            }

            for migration in load_migrations(directory):
                if migration.version in applied_checksums:
                    applied_checksum = applied_checksums[migration.version]
                    _check_checksum(migration, applied_checksum)
                    if applied_checksum is None:
                        await session.execute(
                            _BACKFILL_CHECKSUM_SQL,
                            (migration.checksum, migration.version),
                        )
                    continue

                label = f"{migration.version:04d}_{migration.name}"
                logger.info("Applying migration %s", label)
                try:
                    for statement in _split_statements(migration.sql):
                        await session.execute(statement)
                    await session.execute(
                        _INSERT_APPLIED_SQL,
                        (
                            migration.version,
                            migration.name,
                            time.time(),
                            migration.checksum,
                        ),
                    )
                except Exception:
                    logger.exception(
                        "Migration %s failed; no partial changes from "
                        "this file were recorded as applied",
                        label,
                    )
                    raise
                newly_applied.append(label)
        finally:
            await session.execute(
                "SELECT pg_advisory_unlock(%s)", (_MIGRATION_LOCK_KEY,)
            )

    if newly_applied:
        logger.info(
            "Applied %s migration(s): %s", len(newly_applied), ", ".join(newly_applied)
        )
    return newly_applied
