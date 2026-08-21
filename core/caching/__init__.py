"""Query-result caching: what's here, and which piece to reach for.

Four classes, four distinct responsibilities -- each one answers
exactly one of the questions below, and none of them overlap:

- `QueryResultCache` -- where/how are cached query results stored?
  (L2: one row per cache key in the `query_cache` Postgres table)
- `QueryCacheCoordinator` -- when should the cache be consulted vs.
  the query actually run? (L1 in-process TTL/LRU in front of L2, plus
  single-flight coalescing of concurrent identical misses)
- `CacheMaintenance` -- how are expired entries cleaned up? (periodic
  sweep of the `query_cache` table, called from `ApplicationStateStep`'s
  maintenance loop -- not on the request path)
- `CachePersistenceQueue` -- how are L2 writes and access-stat updates
  persisted without blocking a request? (bounded background queue
  with drop-not-block backpressure)

`QueryCacheCoordinator` is the one entry point request-handling code
should use -- see `core.application_services.ApplicationServices`,
which exposes it as `.query_cache` and keeps its own `QueryResultCache`
instance private (`._query_result_cache`) for exactly this reason: L2
is an implementation detail of the coordinator sitting in front of it,
not something a route should reach for directly. `CacheMaintenance` and
`CachePersistenceQueue` are collaborators the coordinator and
`ApplicationStateStep` use internally, not something request-handling
code touches at all.

Deliberately not exported from here: `core.db.cache.HybridQueryCache`
(the in-process L1 cache `QueryCacheCoordinator` uses internally) and
`core.storage.application_state_store.ApplicationStateStore` (what L2
persists into) -- both belong to other packages and are implementation
details of the classes above, not part of this package's own public
surface. If you're looking for "the L1 in-memory cache" or "the L2
Postgres-backed cache," `QueryCacheCoordinator`/`QueryResultCache`
above are still the classes to use; `HybridQueryCache` in
`core.db.cache` is intentionally not re-exported as a caching entry
point in its own right (see that module's docstring: as of this
writing nothing but `QueryCacheCoordinator` constructs one).
"""

from core.caching.cache_maintenance import CacheMaintenance
from core.caching.persistence_queue import CachePersistenceQueue
from core.caching.query_cache_coordinator import QueryCacheCoordinator
from core.caching.query_result_cache import QueryResultCache

__all__ = [  # noqa: RUF022 -- ordered to match the docstring's L2 ->
    # coordinator -> maintenance -> persistence reading order, not alphabetized.
    "QueryResultCache",
    "QueryCacheCoordinator",
    "CacheMaintenance",
    "CachePersistenceQueue",
]
