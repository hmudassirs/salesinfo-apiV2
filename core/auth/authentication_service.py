"""Orchestrates user registration, login, listing, and role management --
including the audit logging and session-revocation side effects that
go with them.

Pulled out of `core/app/api/routes/auth.py` (24K, mixing HTTP
translation with password hashing, JWT issuance, role-change business
rules, audit logging, and cache/JWT-revocation calls) following the
same pattern `core.services.query_service.QueryService` established
for `core/app/api/routes/query.py`: the route decodes the request,
calls this, and encodes the response.

Audit logging and revocation live here now, not in the route, because
they're consequences of the business operation itself (a role change
*must* invalidate stale sessions; a login attempt *must* be audited)
rather than something the caller opts into -- a route that forgot to
call them would produce a silent, hard-to-notice gap. HTTP-layer
authorization (is *this caller* allowed to hit this endpoint at all --
`core.app.api.routes.auth._require_admin`/`_require_owner_or_admin`)
stays in the route: it depends on `CurrentUser`, resolved from
`Request.state` by `core.auth.middleware`, which this service has no
reason to know about.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import jwt

from core.app.settings import AppSettings
from core.auth.middleware import invalidate_user_cache, revoke_tokens_issued_before
from core.auth.passwords import hash_password, verify_password
from core.auth.user_repository import UserRepository
from core.concurrency.executors import run_in_state_executor
from core.db.session import DatabaseSession
from core.observability.audit import AuditTrail

# Fixed-salt hash of a value nobody will ever type as a real password.
# `authenticate()` verifies against this whenever the username lookup
# misses, so the PBKDF2 cost is paid on every login attempt regardless
# of whether the account exists -- otherwise "unknown username" returns
# near-instantly while "known username, wrong password" pays the full
# ~200k-iteration hash, and that latency gap is enough to enumerate
# valid usernames from response timing alone.
_DUMMY_PASSWORD_HASH = hash_password("no-such-account-rate-limiting-placeholder")


class InvalidCredentialsError(Exception):
    """Username doesn't exist, or the password doesn't match."""


class AccountDisabledError(Exception):
    """Credentials were correct, but the account's is_active is False."""


class UserNotFoundError(Exception):
    """No user exists with the given user_id."""


class LastAdminError(Exception):
    """Refusing a role change that would demote the last remaining admin."""


@dataclass
class RegisteredUser:
    user_id: str
    username: str
    email: str
    role: str
    created_at: int
    is_active: bool


@dataclass
class AuthenticatedUser:
    user_id: str
    username: str
    email: str
    role: str
    created_at: int
    is_active: bool
    token: str


def primary_role(user: Dict[str, Any]) -> str:
    """Normalize a user record's role into a single primary role string.

    Role may be stored under either `roles` (comma-separated) or
    `role` depending on schema version; this picks the first value
    present and falls back to "user".
    """
    roles_val = user.get("roles") or user.get("role") or ""
    if isinstance(roles_val, str) and roles_val:
        return roles_val.split(",")[0]
    return "user"


class AuthenticationService:
    """Built once at startup (by `core.application_services.ApplicationServices`)
    and reused across every request -- not per-request state. Its real
    state is the injected `UserRepository`, `AppSettings` (JWT config),
    and `AuditTrail`, all themselves long-lived singletons over the
    shared application state store connection.

    `db_session` is deliberately NOT part of this class's state: unlike
    `users`/`settings`/`audit`, whether a caller needs JWT-revocation
    coordination is per-call, and threading the (async, request-tied)
    coordination database through a startup-constructed singleton's
    constructor would only matter for `update_role`. It's passed as a
    plain argument to that one method instead -- see its docstring.
    """

    def __init__(
        self,
        users: UserRepository,
        settings: AppSettings,
        audit: Optional[AuditTrail] = None,
    ):
        self._users = users
        self._settings = settings
        self._audit = audit

    async def register(
        self, *, username: str, email: str, password: str
    ) -> RegisteredUser:
        """Register a new account. Always creates a "user"-role account --
        deliberately ignoring any caller-supplied role, since registration
        has no auth requirement of its own; `update_role` is the only
        supported path to grant admin.

        Raises:
            core.storage.exceptions.DuplicateRecordError: username already
                taken.
            core.storage.exceptions.DatabaseUnavailableError: store failure.
        """
        # PBKDF2 hashing is CPU-bound and takes real wall-clock time
        # (~200k SHA-256 iterations) -- run it off the event loop so it
        # doesn't stall every other in-flight request for its duration.
        password_hash = await run_in_state_executor(hash_password, password)
        user_id = f"user_{int(time.time())}_{secrets.token_hex(4)}"
        created_at = int(time.time())

        await run_in_state_executor(
            self._users.create,
            user_id=user_id,
            username=username,
            email=email,
            password_hash=password_hash,
            role="user",
            created_at=created_at,
        )

        if self._audit is not None:
            self._audit.log_audit_event(
                event_type="user.register",
                user_id=user_id,
                resource_type="user",
                resource_id=user_id,
                action="create",
                success=True,
                metadata={"username": username, "email": email},
            )

        return RegisteredUser(
            user_id=user_id,
            username=username,
            email=email,
            role="user",
            created_at=created_at,
            is_active=True,
        )

    async def authenticate(
        self, *, username: str, password: str, ip_address: Optional[str] = None
    ) -> AuthenticatedUser:
        """Verify credentials and issue a JWT.

        Args:
            ip_address: Caller's IP, for the audit trail only -- this
                service has no other use for it and doesn't otherwise
                need to know anything HTTP-shaped about the caller.

        Raises:
            InvalidCredentialsError: unknown username or wrong password.
            AccountDisabledError: credentials correct, but is_active is False.
            core.storage.exceptions.DatabaseUnavailableError: store failure.
        """
        user = await run_in_state_executor(self._users.get_by_username, username)

        # Always run the CPU-bound password check, even when `user` is
        # None -- see this module's docstring for the timing
        # side-channel this avoids.
        stored_hash = user.get("password_hash", "") if user else _DUMMY_PASSWORD_HASH
        password_ok = await run_in_state_executor(
            verify_password, password, stored_hash
        )

        if not user or not password_ok:
            if self._audit is not None:
                self._audit.log_audit_event(
                    event_type="user.login",
                    user_id=None,
                    resource_type="user",
                    resource_id="unknown",
                    action="login",
                    success=False,
                    error_message="Invalid credentials",
                    ip_address=ip_address,
                )
            raise InvalidCredentialsError("Invalid username or password")

        if not user.get("is_active"):
            raise AccountDisabledError("Account is disabled")

        role = primary_role(user)
        issued_at = int(time.time())
        token_payload = {
            "user_id": user["user_id"],
            "username": user["username"],
            "role": role,
            "iat": issued_at,
            "exp": issued_at + self._settings.jwt_expiry_seconds,
        }
        token = jwt.encode(
            token_payload,
            self._settings.jwt_secret_key,
            algorithm=self._settings.jwt_algorithm,
        )

        await run_in_state_executor(self._users.update_last_login, user["user_id"])

        if self._audit is not None:
            self._audit.log_audit_event(
                event_type="user.login",
                user_id=user["user_id"],
                resource_type="user",
                resource_id=user["user_id"],
                action="login",
                success=True,
                ip_address=ip_address,
            )

        return AuthenticatedUser(
            user_id=user["user_id"],
            username=user["username"],
            email=user["email"],
            role=role,
            created_at=user["created_at"],
            is_active=user["is_active"],
            token=token,
        )

    async def list_users(self) -> List[Dict[str, Any]]:
        """Return every user, each with a normalized `role` field added."""
        users_data = await run_in_state_executor(self._users.list_all)
        return [{**user, "role": primary_role(user)} for user in users_data]

    async def update_role(
        self,
        *,
        user_id: str,
        new_role: str,
        actor_user_id: str,
        db_session: Optional[DatabaseSession] = None,
    ) -> Dict[str, Any]:
        """Change `user_id`'s role, and invalidate any session that was
        relying on the old one.

        Args:
            actor_user_id: user_id of the admin making this change, for
                the audit trail. Distinct from `user_id` (the target
                being changed) -- this service has no way to know who's
                calling without being told.
            db_session: Used to revoke any JWT already issued to
                `user_id` before now -- otherwise a just-promoted (or
                demoted) user's already-issued tokens would keep their
                stale role baked into `scopes` until they naturally
                expire (see `core.auth.middleware.revoke_tokens_issued_before`'s
                docstring). Optional and skipped when not given (e.g. a
                script/test with no JWT coordination database), not
                required, since the role-change itself must still
                succeed either way -- the cache invalidation and audit
                log below always run regardless.

        Raises:
            UserNotFoundError: no such user_id.
            LastAdminError: this change would demote the only remaining
                admin, leaving no one able to grant admin back.
            core.storage.exceptions.DatabaseUnavailableError: store failure.
        """
        target_user = await run_in_state_executor(self._users.get_by_id, user_id)
        if not target_user:
            raise UserNotFoundError(user_id)

        previous_role = primary_role(target_user)

        if previous_role == "admin" and new_role != "admin":
            all_users = await run_in_state_executor(self._users.list_all)
            remaining_admins = [
                u
                for u in all_users
                if u.get("user_id") != user_id and primary_role(u) == "admin"
            ]
            if not remaining_admins:
                raise LastAdminError("Cannot demote the last remaining admin")

        await run_in_state_executor(self._users.update_role, user_id, new_role)

        updated_user = (
            await run_in_state_executor(self._users.get_by_id, user_id) or target_user
        )

        # Shrink the cache staleness window: without this, a
        # just-promoted (or demoted) user's role stays as
        # core.auth.middleware last cached it for up to the cache's TTL.
        invalidate_user_cache(user_id)

        # Any JWT already issued to this user was minted with the *old*
        # role baked into its payload and, unlike the API-key path,
        # would otherwise keep being honored with that stale role until
        # it naturally expires -- force those tokens to be rejected so
        # the role change takes effect immediately for JWT sessions too.
        if db_session is not None:
            await revoke_tokens_issued_before(db_session, user_id)

        if self._audit is not None:
            self._audit.log_audit_event(
                event_type="user.role_update",
                user_id=actor_user_id,
                resource_type="user",
                resource_id=user_id,
                action="update_role",
                success=True,
                metadata={"previous_role": previous_role, "new_role": new_role},
            )

        return {
            **updated_user,
            "role": primary_role(updated_user) or new_role,
            "previous_role": previous_role,
        }
