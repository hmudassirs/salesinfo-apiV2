"""Query result caching strategies: LRU, TTL, and a combined LRU+TTL
policy in a single backing store.

**Which one is actually used in production:** `HybridQueryCache`, as
the L1 (in-process) layer inside
`core.caching.query_cache_coordinator.QueryCacheCoordinator` -- see
that module's docstring for the full L1/L2/single-flight picture.
`LRUQueryCache`/`TTLQueryCache` are its building blocks (and are
individually correct, complete cache implementations you could use
directly), but as of this writing nothing in this codebase does --
`HybridQueryCache` is the only one with a real caller.
"""

import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any, Callable, Optional

from core.db.logger import get_logger

logger = get_logger(__name__)


class CacheEntry:
    """Represents a cached query result with metadata."""

    def __init__(self, value: Any, ttl: Optional[int] = None):
        """Initialize cache entry.

        Args:
            value: The cached value
            ttl: Time-to-live in seconds (None = no expiry)
        """
        self.value = value
        self.ttl = ttl
        self.created_at = time.time()
        self.accessed_at = time.time()
        self.access_count = 0

    def is_expired(self) -> bool:
        """Check if entry has expired.

        Returns:
            True if expired, False otherwise
        """
        if self.ttl is None:
            return False
        elapsed = time.time() - self.created_at
        return elapsed > self.ttl

    def record_access(self) -> None:
        """Record access for LRU tracking."""
        self.accessed_at = time.time()
        self.access_count += 1


class QueryCache(ABC):
    """Abstract base class for query caching strategies."""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        pass

    @abstractmethod
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Put value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
        """
        pass

    @abstractmethod
    def invalidate(self, key: str) -> None:
        """Invalidate cache entry.

        Args:
            key: Cache key to invalidate
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear entire cache."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Get current cache size.

        Returns:
            Number of cached entries
        """
        pass

    @abstractmethod
    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        pass


class LRUQueryCache(QueryCache):
    """Least Recently Used cache implementation."""

    def __init__(self, max_size: int = 256):
        """Initialize LRU cache.

        Args:
            max_size: Maximum number of entries
        """
        self.max_size = max_size
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        if key not in self.cache:
            self.misses += 1
            return None

        entry = self.cache[key]
        if entry.is_expired():
            del self.cache[key]
            self.misses += 1
            return None

        # Move to end (most recently used)
        self.cache.move_to_end(key)
        entry.record_access()
        self.hits += 1
        return entry.value

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Put value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
        """
        if key in self.cache:
            self.cache.move_to_end(key)
            self.cache[key] = CacheEntry(value, ttl)
            return

        if len(self.cache) >= self.max_size:
            # Remove least recently used (first item)
            removed_key = next(iter(self.cache))
            del self.cache[removed_key]
            logger.debug("Evicted LRU entry: %s", removed_key)

        self.cache[key] = CacheEntry(value, ttl)
        logger.debug("Cached query: %s", key)

    def invalidate(self, key: str) -> None:
        """Invalidate cache entry.

        Args:
            key: Cache key
        """
        if key in self.cache:
            del self.cache[key]
            logger.debug("Invalidated cache: %s", key)

    def clear(self) -> None:
        """Clear entire cache."""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        logger.info("Cache cleared")

    def size(self) -> int:
        """Get cache size.

        Returns:
            Number of entries
        """
        return len(self.cache)

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Statistics dictionary
        """
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            "strategy": "LRU",
            "size": self.size(),
            "max_size": self.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.2f}%",
            "total_requests": total,
        }


class TTLQueryCache(QueryCache):
    """Time-To-Live cache implementation."""

    def __init__(self, max_size: int = 256, default_ttl: int = 3600):
        """Initialize TTL cache.

        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache: dict[str, CacheEntry] = {}
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        if key not in self.cache:
            self.misses += 1
            return None

        entry = self.cache[key]
        if entry.is_expired():
            del self.cache[key]
            self.misses += 1
            logger.debug("Cache entry expired: %s", key)
            return None

        entry.record_access()
        self.hits += 1
        return entry.value

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Put value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        if len(self.cache) >= self.max_size:
            # Remove entry with earliest expiry
            earliest_key = min(
                self.cache.keys(),
                key=lambda k: (
                    self.cache[k].created_at + (self.cache[k].ttl or self.default_ttl)
                ),
            )
            del self.cache[earliest_key]
            logger.debug("Evicted TTL entry: %s", earliest_key)

        ttl_seconds = ttl or self.default_ttl
        self.cache[key] = CacheEntry(value, ttl_seconds)
        logger.debug("Cached query with TTL %ss: %s", ttl_seconds, key)

    def invalidate(self, key: str) -> None:
        """Invalidate cache entry.

        Args:
            key: Cache key
        """
        if key in self.cache:
            del self.cache[key]
            logger.debug("Invalidated cache: %s", key)

    def clear(self) -> None:
        """Clear entire cache."""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        logger.info("Cache cleared")

    def size(self) -> int:
        """Get cache size.

        Returns:
            Number of entries
        """
        return len(self.cache)

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Statistics dictionary
        """
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            "strategy": "TTL",
            "size": self.size(),
            "max_size": self.max_size,
            "default_ttl": self.default_ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.2f}%",
            "total_requests": total,
        }


class HybridQueryCache(QueryCache):
    """LRU eviction (bounded size) plus per-entry TTL expiry, in one
    backing store.

    Earlier versions of this class composed a *second*, independent
    `TTLQueryCache` alongside the `LRUQueryCache` and wrote every
    `put()` into both -- doubling memory for every cached entry and
    doubling the work on every `get()`, for zero behavioral benefit:
    `LRUQueryCache` already expires entries via
    `CacheEntry.is_expired()`, so the second store's own TTL tracking
    was redundant with the first, and `get()` only ever read values
    from the LRU side anyway (the TTL side's `.get()` result was
    computed and discarded, just to trigger its own independent
    eviction of a copy nothing read from). There is exactly one
    strategy here now: `LRUQueryCache`, which already provides both
    eviction under memory pressure (LRU) and per-entry expiry (TTL).
    """

    def __init__(self, max_size: int = 256, default_ttl: int = 3600):
        """Initialize hybrid cache.

        Args:
            max_size: Maximum number of entries
            default_ttl: Default time-to-live in seconds, applied
                whenever `put()` is called without an explicit `ttl`.
        """
        self._store = LRUQueryCache(max_size)
        self.default_ttl = default_ttl

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        return self._store.get(key)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Put value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds; falls back to this
                instance's `default_ttl` when omitted (unlike
                `LRUQueryCache.put()`, which treats an omitted `ttl`
                as "never expires" -- see this class's docstring for
                why an earlier version of this class needed every
                caller to pass `ttl` explicitly to avoid that).
        """
        self._store.put(key, value, ttl if ttl is not None else self.default_ttl)

    def invalidate(self, key: str) -> None:
        """Invalidate a cache entry.

        Args:
            key: Cache key
        """
        self._store.invalidate(key)

    def clear(self) -> None:
        """Clear the cache."""
        self._store.clear()

    def size(self) -> int:
        """Get cache size.

        Returns:
            Number of entries
        """
        return self._store.size()

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Statistics dictionary
        """
        stats = dict(self._store.stats())
        stats["strategy"] = "HYBRID"
        stats["default_ttl"] = self.default_ttl
        return stats


def query_cache(
    cache: QueryCache,
    ttl: Optional[int] = None,
) -> Callable:
    """Decorator for caching query results.

    Args:
        cache: Cache instance to use
        ttl: Time-to-live in seconds

    Returns:
        Decorator function

    Example:
        cache = LRUQueryCache()

        @query_cache(cache)
        def get_users():
            return db.fetch_all("SELECT * FROM users")
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            # Generate cache key
            key = f"{func.__name__}:{args}:{kwargs}"

            # Try to get from cache
            cached = cache.get(key)
            if cached is not None:
                return cached

            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.put(key, result, ttl)
            return result

        wrapper.cache = cache  # type: ignore
        return wrapper

    return decorator
