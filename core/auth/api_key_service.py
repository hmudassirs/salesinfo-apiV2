"""API key management service (business logic: generate/hash keys,
call the repository layer). Renamed file from `service.py` — that name
gave no indication this was about API keys specifically, and it sat in
core/auth/ alongside models.py/passwords.py with no way to tell which
file did what without opening each one."""

import asyncio

from core.concurrency.executors import run_in_state_executor
from core.performance.context import get_current_profiler
from core.performance.enums import PerformanceStage
from core.performance.types import MetricName
import hashlib
import secrets
import time
from typing import Optional

from core.auth.api_key_repository import APIKeyRepository
from core.db.logger import get_logger
from core.observability.audit import AuditTrail

logger = get_logger(__name__)


class APIKeyService:
    """Service for managing API keys, including the audit-log side
    effect of create/revoke/delete.

    Takes an `APIKeyRepository` (and optionally an `AuditTrail`)
    directly rather than the whole `ApplicationServices` composition
    aggregate -- this used to receive `ApplicationServices` and reach
    into `.api_keys`, which meant every caller had to build (or have on
    hand) the entire aggregate just to get this one repository, and
    coupled this service's dependency chain to composition-root
    internals it has no other reason to know about. Built once at
    startup by `core.application_services.ApplicationServices`, not
    per-request -- see `core.app.api.dependencies.get_api_key_service`
    for the FastAPI-side wiring, which just reads the shared instance
    off `application_services` now.

    Every call into `self.repository` here is offloaded via
    `asyncio.to_thread` — those calls are blocking psycopg2 I/O underneath
    (see core/storage/application_state_store.py), and this class is always called
    from async request-handling code (the auth middleware, in particular,
    on literally every request). Calling them directly would block the
    whole event loop for the duration of each application-state-store round-trip — this
    was a real, measured bottleneck earlier in this codebase's history
    (500 concurrent requests collapsing to ~8 req/s) before being fixed.
    """

    # Per-process, short-TTL cache of validated keys: api_key_hash ->
    # (validated_dict, expires_at_monotonic). Avoids an application-state-store round-trip
    # on every request from a client reusing the same key repeatedly.
    # TTL bounds the window in which a revoked/deleted key would still
    # authenticate — keep this short (seconds, not minutes); the goal is
    # collapsing repeated validation of the *same* key under load, not
    # making revocation slow. Per-process: if you ever run multiple
    # uvicorn workers or instances, each has its own independent cache,
    # so a revoke isn't instantly consistent across all of them either —
    # only a shared cache (e.g. Redis) would fix that.
    #
    # This cache is single-flight protected as well: if many requests
    # arrive simultaneously with the same raw API key and the entry is
    # not in the cache, only one of them does the state-store validation.
    _validation_cache: dict = {}
    _validation_lock = asyncio.Lock()
    _validation_inflight: dict = {}
    _CACHE_TTL_SECONDS = 30.0
    _validation_cache_hits: int = 0
    _validation_cache_misses: int = 0

    def __init__(
        self, repository: APIKeyRepository, audit: Optional[AuditTrail] = None
    ):
        """Initialize API key service.

        Args:
            repository: APIKeyRepository instance to read/write api_keys
                through.
            audit: Optional AuditTrail for logging create/revoke/delete
                events. Was routes/auth.py's job (`application_services.audit
                .log_audit_event(...)` after each call into this
                service) -- moved here for the same reason
                AuthenticationService's audit/revocation calls moved
                out of routes/auth.py's user-management endpoints: a
                route that forgot to call it would produce a silent
                gap, so it belongs with the operation itself, not with
                every caller of it. Optional (defaults to None) so a
                caller without an AuditTrail on hand (a script, a
                test) still gets a working service.
        """
        self.repository = repository
        self._audit = audit

    @staticmethod
    def generate_api_key() -> str:
        """Generate a random API key.

        Returns:
            Random 32-character API key
        """
        return secrets.token_urlsafe(32)

    @staticmethod
    def hash_api_key(api_key: str) -> str:
        """Hash an API key using SHA256.

        Args:
            api_key: The raw API key

        Returns:
            Hashed API key (hex digest)
        """
        return hashlib.sha256(api_key.encode()).hexdigest()

    async def create_api_key(
        self,
        owner_id: str,
        scopes: Optional[str] = None,
        expires_at: Optional[int] = None,
    ) -> dict:
        """Create a new API key for a user.

        Args:
            owner_id: User ID who owns the key
            scopes: Comma-separated scopes (e.g., "read,write")
            expires_at: Unix timestamp when key expires (None = never)

        Returns:
            Dictionary with key_id, api_key (raw, shown once), and metadata
        """
        api_key = self.generate_api_key()
        api_key_hash = self.hash_api_key(api_key)
        key_id = f"key_{int(time.time())}_{secrets.token_hex(4)}"
        created_at = int(time.time())

        try:
            await run_in_state_executor(
                self.repository.create,
                key_id=key_id,
                api_key_hash=api_key_hash,
                owner_id=owner_id,
                created_at=created_at,
                expires_at=expires_at,
                scopes=scopes,
                is_active=True,
            )
            logger.info("Created API key %s for user %s", key_id, owner_id)

            if self._audit is not None:
                self._audit.log_audit_event(
                    event_type="api_key.create",
                    user_id=owner_id,
                    resource_type="api_key",
                    resource_id=key_id,
                    action="create",
                    success=True,
                    metadata={"scopes": scopes, "expires_at": expires_at},
                )

            return {
                "key_id": key_id,
                "api_key": api_key,  # Show only once
                "owner_id": owner_id,
                "created_at": created_at,
                "expires_at": expires_at,
                "scopes": scopes,
                "is_active": True,
            }
        except Exception:
            logger.exception("Error creating API key")
            raise

    async def validate_api_key(self, api_key: str) -> Optional[dict]:
        """Validate an API key and return its metadata.

        Cached for `_CACHE_TTL_SECONDS`: a revoked key can remain valid
        for up to that long after revocation. See the class docstring
        for the full trade-off.

        Args:
            api_key: The raw API key to validate

        Returns:
            API key metadata if valid, None otherwise
        """
        api_key_hash = self.hash_api_key(api_key)
        now_monotonic = time.monotonic()

        cached = self._validation_cache.get(api_key_hash)
        if cached is not None:
            validated, expires_at = cached
            if now_monotonic < expires_at:
                profiler = get_current_profiler()
                if profiler is not None:
                    profiler.counter(MetricName("api_key_validation_cache_hit"))
                self._validation_cache_hits += 1
                return validated
            del self._validation_cache[api_key_hash]

        profiler = get_current_profiler()
        if profiler is not None:
            profiler.counter(MetricName("api_key_validation_cache_miss"))
        self._validation_cache_misses += 1

        loop = asyncio.get_running_loop()
        is_leader = False
        async with self._validation_lock:
            fut = self._validation_inflight.get(api_key_hash)
            if fut is None:
                fut = loop.create_future()
                self._validation_inflight[api_key_hash] = fut
                is_leader = True

        if not is_leader:
            if profiler is not None:
                with profiler.stage(
                    PerformanceStage.SINGLE_FLIGHT_WAIT,
                    MetricName("api_key_validation_wait"),
                ):
                    return await fut
            return await fut

        try:
            now = int(time.time())
            result = await run_in_state_executor(
                self.repository.validate, api_key_hash, now
            )
            if not result:
                validated = None
            else:
                validated = {
                    "key_id": result["key_id"],
                    "owner_id": result["owner_id"],
                    "created_at": result["created_at"],
                    "expires_at": result["expires_at"],
                    "scopes": result["scopes"],
                    "is_active": result["is_active"],
                }
                self._validation_cache[api_key_hash] = (
                    validated,
                    now_monotonic + self._CACHE_TTL_SECONDS,
                )

            if not fut.done():
                fut.set_result(validated)
            return validated
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            async with self._validation_lock:
                if self._validation_inflight.get(api_key_hash) is fut:
                    del self._validation_inflight[api_key_hash]

    @classmethod
    def validation_cache_metrics(cls) -> dict[str, int]:
        """Return per-process API key cache stats for diagnostics."""
        return {
            "cache_hits": cls._validation_cache_hits,
            "cache_misses": cls._validation_cache_misses,
            "inflight_validations": len(cls._validation_inflight),
        }

    async def list_api_keys(self, owner_id: str) -> list[dict]:
        """List all API keys for a user (without showing raw keys).

        Args:
            owner_id: User ID to list keys for

        Returns:
            List of API key metadata (hashed keys)
        """
        try:
            results = await run_in_state_executor(
                self.repository.list_by_owner, owner_id
            )
            return [
                {
                    "key_id": row["key_id"],
                    "owner_id": row["owner_id"],
                    "created_at": row["created_at"],
                    "expires_at": row["expires_at"],
                    "scopes": row["scopes"],
                    "is_active": row["is_active"],
                }
                for row in results
            ]
        except Exception:
            logger.exception("Error listing API keys")
            raise

    async def revoke_api_key(self, key_id: str, owner_id: str) -> bool:
        """Revoke an API key.

        Args:
            key_id: ID of the key to revoke
            owner_id: Owner user ID (for authorization check)

        Returns:
            True if revoked, False if not found
        """
        try:
            result = await run_in_state_executor(
                self.repository.revoke, key_id, owner_id
            )
            if result:
                logger.info("Revoked API key %s", key_id)
                self._evict_by_key_id(key_id)
            if self._audit is not None:
                self._audit.log_audit_event(
                    event_type="api_key.revoke",
                    user_id=owner_id,
                    resource_type="api_key",
                    resource_id=key_id,
                    action="revoke",
                    success=True,
                )
            return result
        except Exception:
            logger.exception("Error revoking API key")
            raise

    async def delete_api_key(self, key_id: str, owner_id: str) -> bool:
        """Delete an API key permanently.

        Args:
            key_id: ID of the key to delete
            owner_id: Owner user ID (for authorization check)

        Returns:
            True if deleted, False if not found
        """
        try:
            result = await run_in_state_executor(
                self.repository.delete, key_id, owner_id
            )
            if result:
                logger.info("Deleted API key %s", key_id)
                self._evict_by_key_id(key_id)
            if self._audit is not None:
                self._audit.log_audit_event(
                    event_type="api_key.delete",
                    user_id=owner_id,
                    resource_type="api_key",
                    resource_id=key_id,
                    action="delete",
                    success=True,
                )
            return result
        except Exception:
            logger.exception("Error deleting API key")
            raise

    def _evict_by_key_id(self, key_id: str) -> None:
        """Best-effort cache eviction on revoke/delete.

        The validation cache is keyed by api_key_hash, but revoke/delete
        only know key_id — we don't have the raw key (it's never stored)
        to re-derive the hash. So this scans cached entries' key_id field
        instead of doing an O(1) lookup. Fine at this scale (the cache
        only ever holds recently-used keys, not the whole table) but
        worth knowing it's O(n) in cache size, not O(1).
        """
        stale = [
            h
            for h, (validated, _) in self._validation_cache.items()
            if validated.get("key_id") == key_id
        ]
        for h in stale:
            del self._validation_cache[h]
