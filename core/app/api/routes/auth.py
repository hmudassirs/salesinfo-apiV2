"""Auth routes: user registration/login, API key management.

Split out from the old combined routes.py, which mixed data-query
routes and auth routes in one 757-line file under the same module —
despite already having a separate `auth_router` for these, they lived
in the same file as /api/query and friends.
"""

from fastapi import APIRouter, HTTPException, Request

from core.app.api.dependencies import (
    CurrentUser,
    GetAPIKeyService,
    GetAuthenticationService,
    GetCurrentUser,
    GetDB,
    GetSettings,
)
from core.app.api.errors import http_exception_for
from core.app.settings import AppSettings
from core.auth import rate_limiter
from core.auth.api_key_service import APIKeyService
from core.auth.authentication_service import (
    AccountDisabledError,
    AuthenticationService,
    InvalidCredentialsError,
    LastAdminError,
    UserNotFoundError,
)
from core.auth.models import (
    APIKeyCreate,
    APIKeyDeleteResponse,
    APIKeyListResponse,
    APIKeyResponse,
    APIKeyRevokeResponse,
    AuthResponse,
    UserCreate,
    UserListResponse,
    UserLogin,
    UserResponse,
    UserRoleUpdate,
)
from core.db.logger import get_logger
from core.db.session import DatabaseSession
from core.observability.context import build_request_context
from core.storage.exceptions import RepositoryError

logger = get_logger(__name__)

auth_router = APIRouter(prefix="/api/auth", tags=["authentication"])


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _require_owner_or_admin(current_user: CurrentUser, owner_id: str) -> None:
    """Authorize a request against a resource's owner_id.

    Enforces "you can only act on your own resources, unless you're an
    admin" — previously any valid API key could list/revoke/delete
    *another user's* keys by supplying a different owner_id.

    Raises:
        HTTPException: 403 if the caller is neither the resource owner
            nor an admin.
    """
    if current_user.is_admin:
        return
    if not current_user.user_id or current_user.user_id != owner_id:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to access this resource",
        )


def _require_admin(current_user: CurrentUser) -> None:
    """Authorize a request as admin-only.

    Raises:
        HTTPException: 403 if the caller's role is not "admin".
    """
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Admin privileges required")


@auth_router.post("/keys", response_model=APIKeyResponse)
async def create_api_key(
    request: Request,
    key_data: APIKeyCreate,
    api_key_service: APIKeyService = GetAPIKeyService,
    current_user: CurrentUser = GetCurrentUser,
) -> APIKeyResponse:
    """Create a new API key for a user.

    A caller may create a key for themselves; only an admin may create
    a key on behalf of a different owner_id.

    Args:
        key_data: APIKeyCreate with owner_id and optional scopes

    Returns:
        APIKeyResponse with the raw API key (shown only once)
    """
    try:
        build_request_context(request)
        _require_owner_or_admin(current_user, key_data.owner_id)

        result = await api_key_service.create_api_key(
            owner_id=key_data.owner_id,
            scopes=key_data.scopes,
            expires_at=key_data.expires_at,
        )

        return APIKeyResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to create API key for owner_id=%s", key_data.owner_id)
        raise http_exception_for(exc, default_detail="Failed to create API key")


@auth_router.get("/keys/{owner_id}", response_model=APIKeyListResponse)
async def list_api_keys(
    request: Request,
    owner_id: str,
    api_key_service: APIKeyService = GetAPIKeyService,
    current_user: CurrentUser = GetCurrentUser,
) -> APIKeyListResponse:
    """List all API keys for a user.

    Args:
        owner_id: User ID to list keys for

    Returns:
        APIKeyListResponse with all non-revoked keys
    """
    try:
        build_request_context(request)
        _require_owner_or_admin(current_user, owner_id)

        keys = await api_key_service.list_api_keys(owner_id)

        return APIKeyListResponse(
            keys=keys,
            count=len(keys),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to list API keys for owner_id=%s", owner_id)
        raise http_exception_for(exc, default_detail="Failed to list API keys")


@auth_router.post("/keys/{key_id}/revoke", response_model=APIKeyRevokeResponse)
async def revoke_api_key(
    request: Request,
    key_id: str,
    owner_id: str,
    api_key_service: APIKeyService = GetAPIKeyService,
    current_user: CurrentUser = GetCurrentUser,
) -> APIKeyRevokeResponse:
    """Revoke an API key (disable without deleting).

    Args:
        key_id: ID of the key to revoke
        owner_id: User ID who owns the key (for authorization)

    Returns:
        APIKeyRevokeResponse with success status
    """
    try:
        build_request_context(request)
        _require_owner_or_admin(current_user, owner_id)

        await api_key_service.revoke_api_key(key_id, owner_id)

        return APIKeyRevokeResponse(
            key_id=key_id,
            message="API key revoked successfully",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to revoke API key key_id=%s", key_id)
        raise http_exception_for(exc, default_detail="Failed to revoke API key")


@auth_router.delete("/keys/{key_id}", response_model=APIKeyDeleteResponse)
async def delete_api_key(
    request: Request,
    key_id: str,
    owner_id: str,
    api_key_service: APIKeyService = GetAPIKeyService,
    current_user: CurrentUser = GetCurrentUser,
) -> APIKeyDeleteResponse:
    """Delete an API key permanently.

    Args:
        key_id: ID of the key to delete
        owner_id: User ID who owns the key (for authorization)

    Returns:
        APIKeyDeleteResponse with success status
    """
    try:
        build_request_context(request)
        _require_owner_or_admin(current_user, owner_id)

        await api_key_service.delete_api_key(key_id, owner_id)

        return APIKeyDeleteResponse(
            key_id=key_id,
            message="API key deleted successfully",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to delete API key key_id=%s", key_id)
        raise http_exception_for(exc, default_detail="Failed to delete API key")


# ============= USER MANAGEMENT ENDPOINTS =============


@auth_router.post("/users/register", response_model=UserResponse)
async def register_user(
    request: Request,
    user_data: UserCreate,
    auth_service: AuthenticationService = GetAuthenticationService,
    settings: AppSettings = GetSettings,
    db_session: DatabaseSession = GetDB,
) -> UserResponse:
    """Register a new user.

    Args:
        user_data: UserCreate with username, email, password, and role

    Returns:
        UserResponse with created user data
    """
    if settings.auth_rate_limit_enabled and not await rate_limiter.check_and_record(
        db_session,
        f"register:{_client_ip(request)}",
        max_attempts=settings.auth_rate_limit_max_attempts,
        window_seconds=settings.auth_rate_limit_window_seconds,
    ):
        raise HTTPException(status_code=429, detail="Too many registration attempts")

    build_request_context(request)

    try:
        registered = await auth_service.register(
            username=user_data.username,
            email=user_data.email,
            password=user_data.password,
        )
    except RepositoryError as exc:
        logger.exception("Failed to register user username=%s", user_data.username)
        raise http_exception_for(exc, default_detail="Failed to register user")
    except Exception as exc:
        logger.exception("Failed to register user username=%s", user_data.username)
        raise HTTPException(status_code=400, detail="Failed to register user") from exc

    return UserResponse(
        user_id=registered.user_id,
        username=registered.username,
        email=registered.email,
        role=registered.role,
        created_at=registered.created_at,
        is_active=registered.is_active,
    )


@auth_router.post("/users/login", response_model=AuthResponse)
async def login_user(
    request: Request,
    login_data: UserLogin,
    auth_service: AuthenticationService = GetAuthenticationService,
    settings: AppSettings = GetSettings,
    db_session: DatabaseSession = GetDB,
) -> AuthResponse:
    """Authenticate a user.

    Args:
        login_data: UserLogin with username and password

    Returns:
        AuthResponse with user data and token
    """
    if settings.auth_rate_limit_enabled and not await rate_limiter.check_and_record(
        db_session,
        f"login:{_client_ip(request)}",
        max_attempts=settings.auth_rate_limit_max_attempts,
        window_seconds=settings.auth_rate_limit_window_seconds,
    ):
        raise HTTPException(status_code=429, detail="Too many login attempts")

    build_request_context(request)

    try:
        authenticated = await auth_service.authenticate(
            username=login_data.username,
            password=login_data.password,
            ip_address=_client_ip(request),
        )
    except InvalidCredentialsError:
        return AuthResponse(
            success=False, user=None, token=None, message="Invalid username or password"
        )
    except AccountDisabledError:
        return AuthResponse(
            success=False, user=None, token=None, message="Account is disabled"
        )
    except RepositoryError as exc:
        logger.exception("Failed to authenticate user username=%s", login_data.username)
        raise http_exception_for(exc, default_detail="Failed to authenticate user")
    except Exception as exc:
        logger.exception("Failed to authenticate user username=%s", login_data.username)
        raise HTTPException(
            status_code=400, detail="Failed to authenticate user"
        ) from exc

    await rate_limiter.reset(db_session, f"login:{_client_ip(request)}")

    return AuthResponse(
        success=True,
        user=UserResponse(
            user_id=authenticated.user_id,
            username=authenticated.username,
            email=authenticated.email,
            role=authenticated.role,
            created_at=authenticated.created_at,
            is_active=authenticated.is_active,
        ),
        token=authenticated.token,
        message="Login successful",
    )


@auth_router.get("/users", response_model=UserListResponse)
async def list_users(
    request: Request,
    auth_service: AuthenticationService = GetAuthenticationService,
    current_user: CurrentUser = GetCurrentUser,
) -> UserListResponse:
    """List all users (admin only).

    Returns:
        UserListResponse with all users
    """
    build_request_context(request)
    _require_admin(current_user)

    try:
        users_data = await auth_service.list_users()
    except RepositoryError as exc:
        logger.exception("Failed to list users")
        raise http_exception_for(exc, default_detail="Failed to list users")
    except Exception as exc:
        logger.exception("Failed to list users")
        raise HTTPException(status_code=400, detail="Failed to list users") from exc

    users = [
        UserResponse(
            user_id=user["user_id"],
            username=user["username"],
            email=user["email"],
            role=user["role"],
            created_at=user["created_at"],
            is_active=user["is_active"],
        )
        for user in users_data
    ]

    return UserListResponse(users=users, count=len(users))


@auth_router.patch("/users/{user_id}/role", response_model=UserResponse)
async def update_user_role(
    request: Request,
    user_id: str,
    role_update: UserRoleUpdate,
    auth_service: AuthenticationService = GetAuthenticationService,
    db_session: DatabaseSession = GetDB,
    current_user: CurrentUser = GetCurrentUser,
) -> UserResponse:
    """Update a user's role (admin only).

    This is the only supported way to grant/revoke admin — self-service
    registration always creates a plain "user" account (see
    register_user), so promoting someone requires an existing admin to
    call this endpoint.

    Args:
        user_id: ID of the user whose role is being changed
        role_update: UserRoleUpdate with the new role

    Returns:
        UserResponse reflecting the updated role
    """
    build_request_context(request)
    _require_admin(current_user)

    try:
        updated_user = await auth_service.update_role(
            user_id=user_id,
            new_role=role_update.role,
            actor_user_id=current_user.user_id,
            db_session=db_session,
        )
    except UserNotFoundError:
        raise HTTPException(status_code=404, detail=f"User not found: {user_id}")
    except LastAdminError:
        raise HTTPException(
            status_code=409, detail="Cannot demote the last remaining admin"
        )
    except RepositoryError as exc:
        logger.exception("Failed to update role for user_id=%s", user_id)
        raise http_exception_for(exc, default_detail="Failed to update user role")
    except Exception as exc:
        logger.exception("Failed to update role for user_id=%s", user_id)
        raise HTTPException(
            status_code=400, detail="Failed to update user role"
        ) from exc

    return UserResponse(
        user_id=updated_user["user_id"],
        username=updated_user["username"],
        email=updated_user["email"],
        role=updated_user["role"],
        created_at=updated_user["created_at"],
        is_active=updated_user["is_active"],
    )
