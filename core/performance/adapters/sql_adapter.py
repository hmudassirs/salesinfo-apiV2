"""Timing wrapper around the per-connection SQL adapter
`core.db.session.DatabaseSession._create_adapter()` returns
(`core.db.adapters.postgresql.PostgreSQLAdapter`).

`core.db.session` doesn't hardcode that class name -- `_create_adapter()`
looks it up via `core.db.registry.get_adapter_registry()` based on
configured `db_type` -- so `InstrumentedSQLAdapter` wraps the
`execute`/`fetch_one`/`fetch_all`/`connect`/`disconnect`/`.connection`
shape generically too, rather than naming `PostgreSQLAdapter` directly.
Distinct from `adapters.service_db.InstrumentedServiceDatabase` (a
different, unrelated concrete class: the auth/API-key/audit service
database, not the per-request query database, even though both now
live in the same PostgreSQL database -- see
core.storage.service_db's module docstring). This is deliberately the
minimal surface `AsyncSessionWrapper`/`SyncSessionWrapper` in
`core.db.session` actually call.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, TypeVar

from core.performance.context import get_current_profiler
from core.performance.enums import PerformanceStage
from core.performance.types import MetricName

_T = TypeVar("_T")


class _SQLAdapterLike(Protocol):
    """The subset of the per-connection backend adapter this wrapper uses."""

    def execute(self, sql: str, params: Any = None) -> Any: ...

    def fetch_one(self, sql: str, params: Any = None) -> Any: ...

    def fetch_all(self, sql: str, params: Any = None) -> Any: ...


class InstrumentedSQLAdapter:
    """Wrap a per-connection SQL adapter, timing execute/fetch calls.

    `execute` is timed under `SQL_EXECUTE`; `fetch_one`/`fetch_all`
    under `SQL_FETCH`. Everything else (`connect`, `disconnect`,
    `.connection`, backend-specific helpers like `get_tables`) is
    forwarded untimed via `__getattr__` — `core.db.session.
    AsyncSessionWrapper.close()`/`_safe_rollback` depend on
    `disconnect()`/`.connection` working exactly as they do on the
    unwrapped adapter.
    """

    __slots__ = ("_adapter",)

    def __init__(self, adapter: _SQLAdapterLike) -> None:
        self._adapter = adapter

    def __getattr__(self, name: str) -> Any:
        """Forward anything not explicitly wrapped (untimed) to the adapter."""
        return getattr(self._adapter, name)

    def execute(self, sql: str, params: Any = None) -> Any:
        """Execute a statement, timed under `SQL_EXECUTE`."""
        return self._timed(
            PerformanceStage.SQL_EXECUTE,
            MetricName("sql_execute"),
            lambda: self._adapter.execute(sql, params),
        )

    def fetch_one(self, sql: str, params: Any = None) -> Any:
        """Fetch a single row, timed under `SQL_FETCH`."""
        return self._timed(
            PerformanceStage.SQL_FETCH,
            MetricName("sql_fetch_one"),
            lambda: self._adapter.fetch_one(sql, params),
        )

    def fetch_all(self, sql: str, params: Any = None) -> Any:
        """Fetch all rows, timed under `SQL_FETCH`."""
        return self._timed(
            PerformanceStage.SQL_FETCH,
            MetricName("sql_fetch_all"),
            lambda: self._adapter.fetch_all(sql, params),
        )

    @staticmethod
    def _timed(stage: PerformanceStage, name: MetricName, call: Callable[[], _T]) -> _T:
        """Run `call()` under `stage`/`name` when a profiler is bound."""
        profiler = get_current_profiler()
        if profiler is None:
            return call()
        with profiler.stage(stage, name):
            return call()
