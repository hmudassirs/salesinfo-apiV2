"""Timing wrappers around `core.storage.application_state_store.ApplicationStateStore` and
`core.caching.query_result_cache.QueryResultCache`.

Wraps the existing object behind an identical interface, delegating
every call — arguments, return value, and exception — unchanged, and
only adding timing when a profiler is bound to the current context.

Two seams are covered, matching `docs/PerformancePlan.md` Phase 8 and
the `SQL` / `CACHE_LOOKUP` / `CACHE_STORE` nodes in
`docs/RequestFlow.md`'s trace model:

- `InstrumentedApplicationStateStore` wraps `ApplicationStateStore`. `execute`,
  `fetch_one`, and `fetch_all` (each of which acquires its own pooled
  connection) are timed under `SQL_EXECUTE`/`SQL_FETCH`. `execute_on`
  and `fetch_one_on` (used against an already-acquired connection
  inside `transaction()`) get the same stages under a distinct metric
  name so the two call shapes stay distinguishable in a trace.
  `transaction()` itself — acquire, yield, commit-or-rollback — is
  timed as one `TRANSACTION_COMMIT` span: `ApplicationStateStore.transaction`
  relies on the underlying connection's `.commit()`/`.rollback()` API
  rather than executing explicit `BEGIN`/`COMMIT`/`ROLLBACK`
  statements (contrast `core.db.transactions.transaction`, which
  `adapters.transactions` times per SQL statement), so there is no
  separate begin/commit boundary to instrument without duplicating
  `ApplicationStateStore`'s private connection-acquisition logic. Everything
  else (`connect`, `disconnect`, `metrics`, `create_tables`, the
  API-key/user CRUD helpers, `cleanup_expired_cache`, `get_stats`,
  `initialize_admin_user`) is administrative or one-time-per-process
  rather than per-request and is forwarded untimed through
  `__getattr__`.
- `InstrumentedQueryResultCache` wraps `QueryResultCache`.
  `get_cached_result` is timed under `CACHE_LOOKUP` and `cache_result`
  under `CACHE_STORE` — the two operations on the `/api/query` request
  path per `docs/RequestFlow.md`. `generate_cache_key` (pure, in-memory
  hashing), `invalidate_cache`, and `get_cache_stats` (administrative)
  are forwarded untimed.

Neither `core.storage.application_state_store` nor `core.caching.query_result_cache`
is modified; adopting either wrapper is opt-in at the call site.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from core.performance.context import get_current_profiler
from core.performance.enums import PerformanceStage
from core.performance.types import MetricName

if TYPE_CHECKING:
    from core.caching.query_result_cache import QueryResultCache
    from core.storage.application_state_store import ExecuteResult, ApplicationStateStore

_T = TypeVar("_T")

# Longest SQL text kept on a metric point's tags -- see
# `_sql_tag`'s docstring for why this is truncated rather than stored
# in full.
_MAX_SQL_TAG_LENGTH = 200


def _sql_tag(sql: str) -> dict[str, str]:
    """Build the `tags={"sql": ...}` passed to `profiler.stage(...)`.

    Truncated to `_MAX_SQL_TAG_LENGTH`: this ends up as a tag on an
    in-memory `MetricPoint` (and, if `core.observability.context`
    reads it back for a request's trace record, in the `traces.db_query`
    column) -- unbounded query text (e.g. a large `IN (...)` list)
    would bloat both for no real diagnostic benefit beyond "which
    statement was this."
    """
    text = " ".join(sql.split())  # collapse embedded newlines/indentation
    if len(text) > _MAX_SQL_TAG_LENGTH:
        text = text[:_MAX_SQL_TAG_LENGTH] + "…"
    return {"sql": text}


class InstrumentedApplicationStateStore:
    """Wrap a `ApplicationStateStore`, timing its SQL execute/fetch operations.

    Also tags each timed operation with the SQL text it ran (see
    `_sql_tag`), which is what lets
    `core.observability.context.build_request_context` populate a
    request's trace record with real `db_query`/`db_duration_ms`
    values instead of the empty placeholders it previously hardcoded --
    see that function's docstring.
    """

    __slots__ = ("_application_state",)

    def __init__(self, application_state: ApplicationStateStore) -> None:
        self._application_state = application_state

    @property
    def application_state(self) -> ApplicationStateStore:
        """The wrapped, un-instrumented store -- e.g. for cleanup/teardown
        code that needs the real object rather than a timed call through
        this wrapper. Prefer this over reaching for the private
        `_application_state` attribute directly."""
        return self._application_state

    def __getattr__(self, name: str) -> Any:
        """Forward anything not explicitly wrapped (untimed) to the database."""
        return getattr(self._application_state, name)

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> ExecuteResult:
        """Execute a write statement and commit, timed under `SQL_EXECUTE`."""
        return self._timed(
            PerformanceStage.SQL_EXECUTE,
            MetricName("application_state_execute"),
            lambda: self._application_state.execute(sql, params),
            sql=sql,
        )

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Fetch a single row, timed under `SQL_FETCH`."""
        return self._timed(
            PerformanceStage.SQL_FETCH,
            MetricName("application_state_fetch_one"),
            lambda: self._application_state.fetch_one(sql, params),
            sql=sql,
        )

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Fetch all rows, timed under `SQL_FETCH`."""
        return self._timed(
            PerformanceStage.SQL_FETCH,
            MetricName("application_state_fetch_all"),
            lambda: self._application_state.fetch_all(sql, params),
            sql=sql,
        )

    def execute_on(
        self, adapter: Any, sql: str, params: tuple[Any, ...] = ()
    ) -> ExecuteResult:
        """Run a statement on an already-acquired adapter, timed under `SQL_EXECUTE`.

        For use inside `transaction()`, matching `ApplicationStateStore.execute_on`:
        does not acquire a new connection or commit.
        """
        return self._timed(
            PerformanceStage.SQL_EXECUTE,
            MetricName("application_state_execute_on"),
            lambda: self._application_state.execute_on(adapter, sql, params),
            sql=sql,
        )

    def fetch_one_on(self, adapter: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Fetch one row on an already-acquired adapter, timed under `SQL_FETCH`."""
        return self._timed(
            PerformanceStage.SQL_FETCH,
            MetricName("application_state_fetch_one_on"),
            lambda: self._application_state.fetch_one_on(adapter, sql, params),
            sql=sql,
        )

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """Run `ApplicationStateStore.transaction()`, timed as one `TRANSACTION_COMMIT` span.

        Yields the same already-acquired adapter as the original, and
        commits on normal exit or rolls back and re-raises on
        exception — identical behaviour to the wrapped method. See the
        module docstring for why this is timed as a single span rather
        than split into begin/commit/rollback stages. No `sql` tag here
        (unlike the methods above) -- the statements run *inside* the
        transaction go through `execute_on`/`fetch_one_on`, which each
        tag their own SQL; this span is only the acquire/commit-or-
        rollback wrapper around them.
        """
        profiler = get_current_profiler()
        if profiler is None:
            with self._application_state.transaction() as adapter:
                yield adapter
            return
        with (
            profiler.stage(
                PerformanceStage.TRANSACTION_COMMIT,
                MetricName("application_state_transaction"),
            ),
            self._application_state.transaction() as adapter,
        ):
            yield adapter

    @staticmethod
    def _timed(
        stage: PerformanceStage,
        name: MetricName,
        call: Callable[[], _T],
        *,
        sql: str | None = None,
    ) -> _T:
        """Run `call()` under `stage`/`name` when a profiler is bound."""
        profiler = get_current_profiler()
        if profiler is None:
            return call()
        tags = _sql_tag(sql) if sql is not None else None
        with profiler.stage(stage, name, tags=tags):
            return call()


class InstrumentedQueryResultCache:
    """Wrap a `QueryResultCache`, timing lookup/store under cache stages."""

    __slots__ = ("_cache",)

    def __init__(self, cache: QueryResultCache) -> None:
        self._cache = cache

    def __getattr__(self, name: str) -> Any:
        """Forward anything not explicitly wrapped (untimed) to the cache."""
        return getattr(self._cache, name)

    def get_cached_result(self, cache_key: str) -> dict[str, Any] | None:
        """Look up one cache entry, timed under `CACHE_LOOKUP`."""
        return self._timed(
            PerformanceStage.CACHE_LOOKUP,
            MetricName("query_cache_lookup"),
            lambda: self._cache.get_cached_result(cache_key),
        )

    def cache_result(  # noqa: PLR0913, PLR0917
        self,
        query_sql: str,
        result_data: list[dict[str, Any]],
        params: tuple[Any, ...] = (),
        user_id: str = "",
        session_id: str = "",
        execution_time_ms: int = 0,
        ttl_seconds: int = 3600,
    ) -> str:
        """Store one cache entry, timed under `CACHE_STORE`."""
        return self._timed(
            PerformanceStage.CACHE_STORE,
            MetricName("query_cache_store"),
            lambda: self._cache.cache_result(
                query_sql,
                result_data,
                params,
                user_id,
                session_id,
                execution_time_ms,
                ttl_seconds,
            ),
        )

    @staticmethod
    def _timed(stage: PerformanceStage, name: MetricName, call: Callable[[], _T]) -> _T:
        """Run `call()` under `stage`/`name` when a profiler is bound."""
        profiler = get_current_profiler()
        if profiler is None:
            return call()
        with profiler.stage(stage, name):
            return call()
