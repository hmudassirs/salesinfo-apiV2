"""FastAPI `Depends(...)` callables for use directly in route signatures.

Distinct from `core.app.container.ApplicationContainer` (the typed,
long-lived object holder populated at startup by
`core.app.lifecycle.manager.ApplicationLifespan`). This module
adapts values out of that container/app.state into per-request
dependencies that routes can declare with `Depends(...)`.

CurrentUser/GetCurrentUser moved to core.auth.dependencies — that's an
auth concept, not generic app wiring; re-exported here only for
backward compatibility with any code still importing it from this path.
"""

from typing import AsyncGenerator, Optional

from fastapi import Depends, HTTPException, Request

from core.auth.dependencies import CurrentUser, GetCurrentUser, get_current_user
from core.db.session import DatabaseSession

__all__ = [
    "GetDB",
    "GetApplicationServices",
    "GetSettings",
    "CurrentUser",
    "GetCurrentUser",
    "get_current_user",
    "get_db_session",
    "get_application_services",
    "get_settings",
]


async def get_db_session(request: Request) -> AsyncGenerator[DatabaseSession, None]:
    """Provide DatabaseSession to endpoints via dependency injection.

    Args:
        request: FastAPI request object

    Yields:
        DatabaseSession instance

    Raises:
        HTTPException: 503 if the application data store step never started
            (e.g. no db_config was provided at startup).
    """
    db_session: Optional[DatabaseSession] = getattr(
        request.app.state, "db_session", None
    )
    if not db_session:
        raise HTTPException(
            status_code=503, detail="Database is not configured or unavailable"
        )
    yield db_session


def get_application_services(request: Request):
    """Provide the ApplicationServices (auth, caching, logging, tracing, audit).

    Args:
        request: FastAPI request object

    Returns:
        ApplicationServices instance

    Raises:
        HTTPException: 503 if the application state step never started.
    """
    application_services = getattr(request.app.state, "application_services", None)
    if not application_services:
        raise HTTPException(
            status_code=503, detail="Application services are not configured or unavailable"
        )
    return application_services


def get_settings(request: Request):
    """Provide the application's AppSettings.

    Args:
        request: FastAPI request object

    Returns:
        AppSettings instance
    """
    settings = getattr(request.app.state, "settings", None)
    if not settings:
        raise RuntimeError("AppSettings not configured")
    return settings


# Create dependency callables for endpoints
GetDB = Depends(get_db_session)
GetApplicationServices = Depends(get_application_services)
GetSettings = Depends(get_settings)
