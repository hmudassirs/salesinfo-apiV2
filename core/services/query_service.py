"""Orchestrates a single `/api/query` request: authorization, cost
classification/concurrency gating, cache lookup (L1 -> single-flight ->
L2), application data execution, result-limit enforcement, and cache
invalidation on writes.

Pulled out of the route handler (roadmap P0-9: "routes should be thin
request/response translation, not where caching/pooling/execution
policy lives") so `core/app/api/routes/query.py` only has to decode the
request, call this, and encode the response.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import List, Optional

from core.caching.query_cache_coordinator import QueryCacheCoordinator
from core.db.adapters.postgresql import StatementTimeoutError
from core.db.session import DatabaseSession
from core.db.sql_policy import classify_cost, classify_operation, extract_tables, has_scope
from core.services.query_limits import get_limits, semaphore_for

logger = logging.getLogger(__name__)


class QueryAuthorizationError(Exception):
    """Caller's role/scope doesn't permit this statement (roadmap 13.1)."""


class QueryTimeoutError(Exception):
    """Statement exceeded max_query_duration_seconds (roadmap 13.4).

    Raised only when PostgreSQL itself cancels the statement via
    server-side `statement_timeout` (see `_execute_gated`'s docstring)
    -- never merely because this coroutine's caller stopped waiting.
    """


@dataclass
class QueryOutcome:
    data: List[dict]
    cached: bool
    truncated: bool = False


class QueryService:
    """Stateless per-request orchestrator; cheap to construct on every
    call since all actual state (L1 cache, single-flight registry, DB
    pool, concurrency semaphores) lives in the injected `db_session` /
    `cache_coordinator` / module-level `query_limits` singletons."""

    def __init__(
        self,
        db_session: DatabaseSession,
        cache_coordinator: QueryCacheCoordinator,
        *,
        require_write_scope: bool = True,
        precise_cache_invalidation: bool = True,
    ):
        """
        Args:
            db_session: application data connection session.
            cache_coordinator: L1/L2 query result cache.
            require_write_scope: gate write statements behind the
                caller's "write" scope (AppSettings.require_write_scope_for_mutations).
                When False, any authenticated caller may run writes --
                only meaningful for deployments that enforce scope
                elsewhere (e.g. at a gateway) and want this check off.
            precise_cache_invalidation: scope cache invalidation to the
                tables a write statement touched (AppSettings.cache_invalidation_precise)
                instead of always clearing the whole cache.
        """
        self._db_session = db_session
        self._cache = cache_coordinator
        self._require_write_scope = require_write_scope
        self._precise_cache_invalidation = precise_cache_invalidation

    async def run(
        self,
        sql: str,
        params: tuple,
        *,
        user_id: Optional[str] = None,
        scopes: str = "",
    ) -> QueryOutcome:
        """Execute `sql`, using the cache for SELECT statements.

        Non-SELECT statements (INSERT/UPDATE/DDL/etc.) are never cached
        or coalesced -- they have side effects, so two "identical"
        write statements are not interchangeable the way two identical
        reads are -- and, on success, invalidate the query cache
        (roadmap 16.3). When `precise_cache_invalidation` is enabled
        and the statement's affected table(s) can be resolved, only
        the cache entries tagged with those tables are dropped;
        otherwise (or when disabled) the entire cache is cleared, since
        we can't otherwise tell which cached SELECTs the write could
        have affected.

        Raises:
            QueryAuthorizationError: write statement without the
                "write" scope, when `require_write_scope` is enabled
                (roadmap 13.1).
            QueryTimeoutError: execution exceeded the configured
                max_query_duration_seconds (roadmap 13.4).
        """
        operation = classify_operation(sql)
        if (
            operation == "write"
            and self._require_write_scope
            and not has_scope(scopes, "write")
        ):
            raise QueryAuthorizationError(
                "This statement requires the 'write' scope. Mint an API "
                "key with scopes='read,write', or use a SELECT/WITH "
                "statement instead."
            )

        cost = classify_cost(sql)

        if operation == "write":
            data, truncated = await self._execute_gated(sql, params, cost)
            # Best-effort: an invalidation failure shouldn't fail the
            # write itself, just risk a stale read until the next
            # write or TTL expiry.
            try:
                await self._invalidate_after_write(sql)
            except Exception:
                logger.debug("Post-write cache invalidation failed", exc_info=True)
            return QueryOutcome(data=data, cached=False, truncated=truncated)

        cache_key = self._cache.cache_key(sql, params)
        tables = extract_tables(sql)

        # Only set when `_run_query` actually ran (a cache miss) --
        # stays False on a cache hit, since nothing was freshly
        # executed to truncate.
        fresh_truncated = False

        async def _run_query() -> list:
            nonlocal fresh_truncated
            data, fresh_truncated = await self._execute_gated(sql, params, cost)
            return data

        async def _persist(data: list) -> None:
            await self._cache.persist_l2(sql, data, params, user_id or None)

        result = await self._cache.get_or_execute(
            cache_key, _run_query, on_miss_persist=_persist, tables=tables
        )
        # `_apply_result_limits` re-checks whatever came back from the
        # cache (or from a fresh execution already bounded by
        # `_execute`'s `fetch_bounded` call) against the *current*
        # limits -- cheap and normally a no-op, but still meaningful
        # if `configure_query_limits()` lowered the limits after this
        # entry was cached. `fresh_truncated` carries the "was this
        # execution itself cut short" signal `_apply_result_limits`
        # alone can't see once cache persistence only ever stores
        # already-bounded data (see `_execute`'s docstring).
        data, limit_truncated = _apply_result_limits(result.data)
        truncated = fresh_truncated or limit_truncated
        return QueryOutcome(data=data, cached=result.cached, truncated=truncated)

    async def _invalidate_after_write(self, sql: str) -> None:
        """Invalidate cached reads after a successful write, as narrowly
        as configuration and the statement allow."""
        if self._precise_cache_invalidation:
            tables = extract_tables(sql)
            if await self._cache.invalidate_tables(tables):
                return
        await self._cache.clear_all()

    async def _execute_gated(
        self, sql: str, params: tuple, cost
    ) -> tuple[list, bool]:
        """Acquire the cost-class semaphore (Phase 14) and enforce the
        query duration ceiling (13.4) around the actual DB call.

        The duration ceiling is enforced by PostgreSQL's own
        server-side `statement_timeout` (`self._execute` passes it
        through to `db.fetch_bounded`), not by wrapping this call in
        `asyncio.wait_for()`. That distinction matters: `fetch_bounded()`
        runs the actual blocking `psycopg2` call in a worker thread
        (`core.concurrency.executors`), and Python has no way to
        forcibly stop a running thread. Cancelling the *coroutine*
        awaiting that thread (what `asyncio.wait_for()`'s timeout
        does) doesn't stop the thread -- it keeps running the query to
        completion in the background, unsupervised, while the
        cancelled coroutine's caller believes the query is over. If
        the pooled connection is then returned to the pool (or even
        just used for a rollback from a different thread) while that
        background thread is still reading from the same socket, two
        unrelated callers can end up sharing one physical connection
        at the same time -- silent protocol/response corruption, not
        just a slow query. Only PostgreSQL itself, via
        `statement_timeout`, can guarantee the statement has actually
        stopped running before this method returns.

        This does *not* bound how long a request can wait to start
        running at all -- queued behind other work in
        `application_data_executor` (sized to track the connection
        pool, so that queue is normally short) or waiting to acquire a
        pooled connection in the first place (bounded separately by
        `DatabaseSettings.pool.timeout`, which raises
        `MaxConnectionsExceeded` rather than silently hanging). Once a
        worker thread actually starts running the statement, though,
        `statement_timeout` is the only clock that matters.

        Returns:
            `(rows, truncated)` -- see `_execute`'s docstring.
        """
        semaphore = semaphore_for(cost)
        async with semaphore:
            limits = get_limits()
            try:
                return await self._execute(
                    sql,
                    params,
                    statement_timeout_seconds=limits.max_query_duration_seconds,
                )
            except StatementTimeoutError as exc:
                raise QueryTimeoutError(
                    f"Query exceeded the {limits.max_query_duration_seconds}s "
                    "execution limit"
                ) from exc

    async def _execute(
        self, sql: str, params: tuple, *, statement_timeout_seconds: float
    ) -> tuple[list, bool]:
        """Run `sql` and return `(rows, truncated)`, streaming from
        PostgreSQL via a server-side cursor rather than materializing
        the full result before applying `max_result_rows`/
        `max_result_bytes` -- see `PostgreSQLAdapter.fetch_bounded`'s
        docstring for why `fetch_all()` followed by a Python-side
        truncation doesn't actually bound this process's memory use
        the way it looks like it does."""
        limits = get_limits()
        async with self._db_session.get_async_session() as db:
            return await db.fetch_bounded(
                sql,
                params,
                max_rows=limits.max_result_rows,
                max_bytes=limits.max_result_bytes,
                statement_timeout_seconds=statement_timeout_seconds,
            )


def _apply_result_limits(data: list) -> tuple[list, bool]:
    """Cap `data` at max_result_rows/max_result_bytes (roadmap 13.4).

    Byte size is estimated by incrementally accumulating each row's
    JSON-serialized length rather than serializing the whole result up
    front -- for a result that's actually oversized, that would defeat
    the point of the limit (you'd pay the full serialization cost you
    were trying to bound).
    """
    limits = get_limits()
    if len(data) <= limits.max_result_rows:
        row_limited, truncated = data, False
    else:
        row_limited, truncated = data[: limits.max_result_rows], True

    out = []
    total_bytes = 0
    for row in row_limited:
        row_bytes = len(json.dumps(row, default=str).encode("utf-8"))
        if total_bytes + row_bytes > limits.max_result_bytes:
            truncated = True
            break
        out.append(row)
        total_bytes += row_bytes

    return out, truncated
