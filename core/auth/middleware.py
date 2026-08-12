"""API-key / JWT authentication middleware.

Extracted from core/app/api/app.py's single `log_requests` middleware,
which mixed auth enforcement with observability logging in one
function. This is the half that decides who's allowed to make the
request; see core/observability/middleware.py for the half that
records what happened after.
"""

import asyncio

from core.concurrency.executors import run_in_state_executor
from core.performance.adapters.auth import InstrumentedAPIKeyService, instrumented_jwt_decode
from core.performance.context import get_current_profiler
from core.performance.enums import PerformanceStage
from core.performance.types import MetricName
import time

import jwt
from fastapi import FastAPI, Request, Response
from starlette.status import HTTP_401_UNAUTHORIZED

from core.auth.api_key_service import APIKeyService
from core.auth.shared_state import get_auth_state

# Per-process, short-TTL cache of user records: user_id -> (user_dict,
# expires_at_monotonic). Every authenticated request was doing a fresh
# application-state-store read here even when the API key itself was already
# cached — same trade-off as APIKeyService's validation cache: a role
# change or deactivation can take up to this long to take effect for a
# user already mid-session. routes/auth.py calls invalidate_user_cache()
# after update_user_role/delete to shrink that window in practice.
_USER_CACHE: dict = {}
_USER_CACHE_TTL_SECONDS = 30.0
_USER_CACHE_HITS = 0
_USER_CACHE_MISSES = 0
_USER_LOOKUP_LOCK = asyncio.Lock()
_USER_LOOKUP_INFLIGHT: dict = {}


def invalidate_user_cache(user_id: str) -> None:
    """Evict a cached user record, e.g. after a role change or deletion."""
    _USER_CACHE.pop(user_id, None)


def user_cache_metrics() -> dict[str, int]:
    """Return per-process user cache stats for diagnostics."""
    return {
        "cache_hits": _USER_CACHE_HITS,
        "cache_misses": _USER_CACHE_MISSES,
        "inflight_user_lookups": len(_USER_LOOKUP_INFLIGHT),
    }


# Per-process record of the most recent role change per user: user_id
# -> epoch seconds. JWTs are self-contained (see the JWT branch below)
# and normally trusted for their full lifetime once issued, so without
# this a demoted admin would keep admin-level `scopes` in any token
# minted before the demotion, for up to `jwt_expiry_seconds` after
# being demoted -- unlike the API-key path, which re-checks the
# caller's current role via `_get_cached_user`/`invalidate_user_cache`
# on (near enough) every request. `routes/auth.py`'s `update_user_role`
# calls `revoke_tokens_issued_before()` so any JWT already issued to
# that user stops being honored immediately.
#
# Backed by core.auth.shared_state.get_auth_state(), which is
# Postgres-shared across every worker process/instance (see that
# module's docstring for the reasoning).


async def revoke_tokens_issued_before(
    db_session, user_id: str, at: float | None = None
) -> None:
    """Invalidate any JWT for `user_id` issued before now (or `at`).
    Call this whenever a user's role changes."""
    await get_auth_state(db_session).set_revoked_before(
        user_id, at if at is not None else time.time()
    )


def _get_cached_user(user_id: str) -> dict | None:
    global _USER_CACHE_HITS, _USER_CACHE_MISSES
    cached = _USER_CACHE.get(user_id)
    if cached is None:
        _USER_CACHE_MISSES += 1
        profiler = get_current_profiler()
        if profiler is not None:
            profiler.counter(MetricName("user_cache_miss"))
        return None
    user, expires_at = cached
    if time.monotonic() >= expires_at:
        del _USER_CACHE[user_id]
        _USER_CACHE_MISSES += 1
        profiler = get_current_profiler()
        if profiler is not None:
            profiler.counter(MetricName("user_cache_miss"))
        return None
    _USER_CACHE_HITS += 1
    profiler = get_current_profiler()
    if profiler is not None:
        profiler.counter(MetricName("user_cache_hit"))
    return user


def _cache_user(user_id: str, user: dict) -> None:
    _USER_CACHE[user_id] = (user, time.monotonic() + _USER_CACHE_TTL_SECONDS)


async def _get_user_with_single_flight(owner_id: str, application_services) -> dict | None:
    user = _get_cached_user(owner_id)
    if user is not None:
        return user

    loop = asyncio.get_running_loop()
    is_leader = False
    async with _USER_LOOKUP_LOCK:
        fut = _USER_LOOKUP_INFLIGHT.get(owner_id)
        if fut is None:
            fut = loop.create_future()
            _USER_LOOKUP_INFLIGHT[owner_id] = fut
            is_leader = True

    if not is_leader:
        profiler = get_current_profiler()
        if profiler is not None:
            with profiler.stage(
                PerformanceStage.SINGLE_FLIGHT_WAIT,
                MetricName("user_lookup_wait"),
            ):
                return await fut
        return await fut

    try:
        profiler = get_current_profiler()
        if profiler is not None:
            with profiler.stage(
                PerformanceStage.AUTHENTICATION,
                MetricName("user_lookup"),
            ):
                user = await run_in_state_executor(
                    application_services.users.get_by_id, owner_id
                )
        else:
            user = await run_in_state_executor(
                application_services.users.get_by_id, owner_id
            )
        if user:
            _cache_user(owner_id, user)
        if not fut.done():
            fut.set_result(user)
        return user
    except BaseException as exc:
        if not fut.done():
            fut.set_exception(exc)
        raise
    finally:
        async with _USER_LOOKUP_LOCK:
            if _USER_LOOKUP_INFLIGHT.get(owner_id) is fut:
                del _USER_LOOKUP_INFLIGHT[owner_id]


# Routes reachable with no API key at all. This must stay a short,
# explicit allowlist: everything else under /api requires a key.
# Without this, a brand-new user has no way to register or log in,
# since both endpoints previously sat behind the same "/api" prefix
# check as the protected data routes.
#
# /debug and /debug/ (the performance dashboard's HTML shell — see
# core.performance.dashboard.live) are included for a different reason:
# a browser navigating there has no way to attach an x-api-key/Bearer
# header, so if this page were behind the same wall as its own data,
# nobody could ever load it to see the API-key input box that lets them
# authenticate the page's own fetch() calls. The shell page contains no
# data itself (just static HTML/CSS/JS); /debug/performance and
# /debug/request/{id}, the endpoints that actually return data, are
# NOT exempted — see install_performance_dashboard's per-route
# `dependencies`.
PUBLIC_PATHS = {
    "/api/auth/users/register",
    "/api/auth/users/login",
    "/debug",
    "/debug/",
}


def install_auth_middleware(app: FastAPI) -> None:
    """Register the auth-enforcement middleware on `app`."""

    @app.middleware("http")
    async def enforce_auth(request: Request, call_next):
        """Enforce API-key or JWT access on protected routes.

        `/debug` (the read-only `core.performance` dashboard installed
        by `core.app.api.app.create_app`, see
        `docs/performance/collectors-exporters-dashboard.md`) is
        included here alongside `/api` — without it, that dashboard's
        `dependencies=[Depends(require_admin_user)]` gate would never
        see a caller's role at all (this middleware is the only thing
        that ever populates `request.state.user_id`/`.role`), so it
        would 403 every request, admin or not, rather than actually
        enforcing "admin only".
        """
        if (
            (
                request.url.path.startswith("/api")
                or request.url.path.startswith("/debug")
            )
            and not request.url.path.startswith("/docs")
            and request.url.path not in PUBLIC_PATHS
        ):
            raw_api_key = (request.headers.get("x-api-key") or "").strip()
            auth_header = request.headers.get("authorization", "")
            bearer_token = (
                auth_header.replace("Bearer ", "", 1).strip() if auth_header else ""
            )

            application_services = getattr(app.state, "application_services", None)
            if application_services is None:
                response = Response(status_code=HTTP_401_UNAUTHORIZED)
                response.headers["www-authenticate"] = "Bearer"
                return response

            authenticated = False

            # 1) An explicit x-api-key is the normal, long-lived credential
            # for machine/service callers.
            api_key_service = InstrumentedAPIKeyService(
                APIKeyService(application_services.api_keys)
            )
            if raw_api_key:
                validated_key = await api_key_service.validate_api_key(raw_api_key)
            else:
                validated_key = None

            if validated_key:
                owner_id = validated_key.get("owner_id", "")
                request.state.user_id = owner_id
                request.state.username = ""
                request.state.role = ""
                # API keys carry their own scopes (e.g. "read,write"),
                # independent of the owning user's role -- see
                # core.db.sql_policy.has_scope for how routes use
                # this to gate mutating statements.
                request.state.scopes = validated_key.get("scopes", "") or ""
                request.state.session_id = (
                    request.headers.get("x-session-id", "") or "api-key"
                )

                if owner_id and hasattr(application_services, "users"):
                    user = await _get_user_with_single_flight(owner_id, application_services)
                    if user:
                        request.state.username = user.get("username", "")
                        roles_val = user.get("roles") or user.get("role") or ""
                        request.state.role = (
                            roles_val.split(",")[0]
                            if isinstance(roles_val, str) and roles_val
                            else ""
                        )
                authenticated = True

            # 2) Otherwise, accept the JWT issued at /api/auth/users/login as
            # `Authorization: Bearer <token>`. Without this branch there is
            # no way for a freshly registered/logged-in user to authenticate
            # *any* request — including POST /api/auth/keys, the endpoint
            # that mints their first x-api-key. The JWT is self-contained
            # (signed with settings.jwt_secret_key), so this doesn't need a
            # application_services lookup the way the api-key path does; the
            # trade-off is that it's valid for anything under /api, not just
            # key creation, until it expires (settings.jwt_expiry_seconds).
            if not authenticated and bearer_token:
                settings = getattr(app.state, "settings", None)
                payload = None
                if settings:
                    try:
                        payload = instrumented_jwt_decode(
                            bearer_token,
                            settings.jwt_secret_key,
                            algorithms=[settings.jwt_algorithm],
                        )
                    except jwt.PyJWTError:
                        payload = None

                if payload:
                    user_id = payload.get("user_id", "")
                    issued_at = payload.get("iat", 0)
                    db_session = getattr(app.state, "db_session", None)
                    revoked_before = await get_auth_state(
                        db_session
                    ).get_revoked_before(user_id)
                    if revoked_before is not None and issued_at < revoked_before:
                        # This token predates a role change for its
                        # holder; don't trust the (now stale) role
                        # baked into its payload. Falls through to
                        # the 401 below, forcing a fresh login.
                        payload = None

                if payload:
                    request.state.user_id = payload.get("user_id", "")
                    request.state.username = payload.get("username", "")
                    request.state.role = payload.get("role", "")
                    # JWT sessions have no independent scopes concept
                    # (unlike API keys) -- derive an equivalent from
                    # role instead: admins get full access, everyone
                    # else is read-only until they mint a
                    # write-scoped API key. See core.db.sql_policy.
                    request.state.scopes = (
                        "read,write"
                        if payload.get("role") == "admin"
                        else "read"
                    )
                    request.state.session_id = (
                        request.headers.get("x-session-id", "") or "jwt"
                    )
                    authenticated = True
                elif bearer_token:
                    # Support raw API keys supplied via Authorization: Bearer
                    # <key> in addition to x-api-key. This is useful for
                    # clients or scripts that prefer a single standard header.
                    validated_key = await api_key_service.validate_api_key(
                        bearer_token
                    )
                    if validated_key:
                        owner_id = validated_key.get("owner_id", "")
                        request.state.user_id = owner_id
                        request.state.username = ""
                        request.state.role = ""
                        request.state.scopes = validated_key.get("scopes", "") or ""
                        request.state.session_id = (
                            request.headers.get("x-session-id", "") or "api-key"
                        )

                        if owner_id and hasattr(application_services, "users"):
                            user = await _get_user_with_single_flight(
                                owner_id, application_services
                            )
                            if user:
                                request.state.username = user.get("username", "")
                                roles_val = user.get("roles") or user.get("role") or ""
                                request.state.role = (
                                    roles_val.split(",")[0]
                                    if isinstance(roles_val, str) and roles_val
                                    else ""
                                )
                        authenticated = True

            if not authenticated:
                response = Response(status_code=HTTP_401_UNAUTHORIZED)
                response.headers["www-authenticate"] = "Bearer"
                return response

        return await call_next(request)
