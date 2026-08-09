"""Rate limiting for the two unauthenticated auth endpoints
(`/users/login`, `/users/register`), which would otherwise have no
protection against credential stuffing, password-spraying, or
registration spam -- every other route sits behind API-key/JWT auth,
but these two exist specifically so a caller *without* either can get
one.

Kept as its own module (rather than having routes/auth.py call
`core.auth.shared_state` directly) so "rate limiting" stays a named,
independent concept from the JWT-revocation state that happens to
share the same underlying store -- see that module's docstring for
why: cross-process/instance coordination via a couple of tables in the
application's PostgreSQL database.
"""

from __future__ import annotations

from core.auth.shared_state import get_auth_state


async def check_and_record(
    db_session, key: str, *, max_attempts: int, window_seconds: float
) -> bool:
    """Record one attempt for `key` and report whether it's still within
    the allowed rate.

    Args:
        db_session: the app's PostgreSQL DatabaseSession -- see
            core.auth.shared_state.get_auth_state.
        key: identifies the caller+endpoint being limited, e.g.
            "login:203.0.113.4".
        max_attempts: attempts allowed per `window_seconds`.
        window_seconds: length of the window.

    Returns:
        True if this attempt is allowed, False if `key` has already hit
        `max_attempts` within the current window (caller should reject
        the request, typically with 429).
    """
    return await get_auth_state(db_session).check_and_record_attempt(
        key, max_attempts=max_attempts, window_seconds=window_seconds
    )


async def reset(db_session, key: str) -> None:
    """Clear recorded attempts for `key`, e.g. after a successful login."""
    await get_auth_state(db_session).reset_attempts(key)
