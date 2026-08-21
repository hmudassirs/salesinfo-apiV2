"""The application-data query contract: acquire a session, run
statements against it, materialize results.

This formalizes a distinction the framework review identified as
already real in the code but not written down anywhere: `DatabaseSession`
(this contract) and `core.storage.application_state_store.ApplicationStateStore`
(`core/db/cursor_executor.py`) are two different, intentional ways of
talking to PostgreSQL, sharing the same underlying connection-pool/
adapter machinery. This is *not* leftover duplication to consolidate --
see `ApplicationStateStore`'s module docstring for why the split is a
contract choice, not an accident. This file (and its counterpart) exist
so that choice has a name and a stable shape, instead of being
something you can only infer by reading both implementations side by
side.

Shape of this contract, as implemented by
`core.db.session.AsyncSessionWrapper` / `SyncSessionWrapper` (obtained
via `DatabaseSession.get_async_session()` / `.get_sync_session()`):

  - Caller acquires a *session* first (an async/sync context manager
    that checks out one pooled connection for the duration of the
    `with`/`async with` block), then issues zero or more statements
    against that same session.
  - `execute()` is for statements without a tabular result (DDL,
    INSERT/UPDATE/DELETE without RETURNING); `fetch_one()`/`fetch_all()`
    are for statements that return rows.
  - Every read returns already-materialized Python data (a dict per
    row, a list of dicts for `fetch_all()`) -- never a live cursor a
    caller could read from after the session (and its underlying
    pooled connection) has closed. See `core/db/cursor_executor.py`'s
    docstring for the sibling contract's different call shape, and why
    that one *does* need to guard against exactly this.
  - Callers needing an explicit transaction use
    `DatabaseSession.get_async_transaction()` /
    `.get_sync_transaction()` instead of a plain session -- see those
    methods' docstrings.

This module intentionally defines Protocols, not base classes:
`AsyncSessionWrapper`/`SyncSessionWrapper` already implement this
shape structurally and aren't meant to inherit from anything here. The
value of writing it down is documentation-as-code (a reviewer or a
static checker can see the contract in one place) plus an explicit
target for `isinstance()` checks in tests -- see
`tests/unit/test_db_contracts.py`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Sequence, runtime_checkable


@runtime_checkable
class QueryExecutor(Protocol):
    """A single application-data statement, run against an already-
    acquired session, returning materialized results.

    Implemented by `core.db.session.AsyncSessionWrapper` (async) and
    `SyncSessionWrapper` (sync) -- see this module's docstring.
    """

    async def execute(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> Any:
        """Run a statement with no tabular result (DDL, a write without
        RETURNING). `statement_timeout_seconds` bounds it with the
        backend's own server-side cancellation, when given -- see
        `core.db.adapters.postgresql._apply_statement_timeout`'s
        docstring for why that's the only cancellation mechanism this
        codebase trusts for a query already running against a pooled
        connection."""
        ...

    async def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> List[Dict]:
        """Run a statement and return every resulting row, already
        materialized into plain dicts. See `execute`'s docstring for
        `statement_timeout_seconds`."""
        ...

    async def fetch_one(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> Optional[Dict]:
        """Run a statement and return its first row (or None), already
        materialized into a plain dict. See `execute`'s docstring for
        `statement_timeout_seconds`."""
        ...


@runtime_checkable
class SyncQueryExecutor(Protocol):
    """Sync counterpart of `QueryExecutor` -- same contract, no
    `async`/`await`. Implemented by
    `core.db.session.SyncSessionWrapper`.
    """

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> Any: ...

    def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> List[Dict]: ...

    def fetch_one(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> Optional[Dict]: ...
