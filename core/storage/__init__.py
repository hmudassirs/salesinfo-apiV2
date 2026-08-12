"""The pooled PostgreSQL engine backing auth/observability/caching.

Storage is deliberately domain-agnostic: it knows how to connect, pool,
and run SQL — nothing about api_keys or users. Renamed from
core/services/ to make that scope explicit; "services" told you nothing
about what was inside.
"""

from core.storage.application_state_store import ExecuteResult, ApplicationStateStore

__all__ = ["ApplicationStateStore", "ExecuteResult"]
