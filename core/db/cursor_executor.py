"""The application-state query contract: acquire a connection *per
call*, run one statement, release it immediately.

Sibling of `core/db/query_executor.py`'s `QueryExecutor` contract --
see that module's docstring for why these are two intentional
contracts sharing infrastructure, not duplication to merge.

Shape of this contract, as implemented by
`core.storage.application_state_store.ApplicationStateStore`:

  - No separate "acquire a session" step -- each of `execute()`,
    `fetch_one()`, `fetch_all()` acquires its own connection from the
    pool, runs exactly one statement, and releases the connection
    before returning. There is no multi-statement session object a
    caller holds onto (contrast `QueryExecutor`, where a session spans
    several calls).
  - `execute()` is for writes (INSERT/UPDATE/DELETE/DDL) and returns
    `ExecuteResult(lastrowid, rowcount)` -- plain values captured
    *before* the connection is released, not a live cursor. A cursor
    handed back here specifically could not be safely read from
    afterward: the pooled connection backing it may already have been
    checked out by another caller by the time this returns (see
    `ExecuteResult`'s own docstring in
    `core.storage.application_state_store`). This is the "live cursor"
    hazard this contract's name refers to -- not that a live cursor
    ever crosses the method boundary, but that the per-call acquire/
    release shape is what makes that hazard real here in a way it
    isn't for `QueryExecutor`'s session-scoped calls.
  - `fetch_one()`/`fetch_all()` similarly materialize into `_Row`
    objects (dict- and index-subscriptable) before releasing the
    connection.
  - An explicit multi-statement transaction uses
    `ApplicationStateStore.transaction()` instead -- see that method's
    docstring for how it holds one connection across several
    statements, the one case this contract *does* span more than a
    single acquire/release.

Defined as a Protocol, not a base class, for the same reason
`query_executor.py` is: `ApplicationStateStore` already implements this
shape structurally and isn't meant to inherit from it. See
`tests/unit/test_db_contracts.py` for the corresponding `isinstance()`
check.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CursorExecutor(Protocol):
    """A single application-state statement, each call acquiring and
    releasing its own pooled connection.

    Implemented by
    `core.storage.application_state_store.ApplicationStateStore`.
    """

    def execute(self, sql: str, params: tuple = ()) -> Any:
        """Run a write statement (INSERT/UPDATE/DELETE/DDL); returns a
        materialized result (e.g. lastrowid/rowcount), never a live
        cursor -- see this module's docstring for why."""
        ...

    def fetch_one(self, sql: str, params: tuple = ()) -> Any:
        """Run a read statement and return its first row (or None),
        already materialized."""
        ...

    def fetch_all(self, sql: str, params: tuple = ()) -> Any:
        """Run a read statement and return every resulting row,
        already materialized."""
        ...
