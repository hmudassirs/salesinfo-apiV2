# core/app/container.py
"""Typed startup-time object holder, shared between LifecycleSteps and
copied onto app.state for request handlers.

Renamed from `DependencyContainer` (which used a string-keyed
`dict[str, Any]` -- effectively a service locator: `container.get(
"application_services")` compiled fine even with a typo in the key
string, and just silently returned None at runtime instead of failing
at the typo). `ApplicationContainer` is a plain dataclass with one
field per subsystem instead, so a typo is a real `AttributeError`/
`TypeError` at the call site, not a None discovered three layers away.

This is deliberately NOT the application's general-purpose DI
mechanism. Routes get their dependencies through FastAPI's typed
`Depends(...)` callables in `core.app.api.dependencies`, reading off
`request.app.state` (populated from this container's fields after
startup -- see `run_api.py`). `ApplicationContainer` exists only
because a couple of `LifecycleStep`s need to read another step's
output *during startup*, before `app.state` exists at all -- e.g.
`PerformanceStep`'s cross-process snapshot loop needs
`application_state`, which `ApplicationStateStep` doesn't register
until a later step runs (see `core.app.lifecycle.performance`'s module
docstring). Once startup finishes, code should prefer
`core.app.api.dependencies`'s typed `Depends()` functions over reaching
into this container directly.
"""

import logging
from dataclasses import dataclass, fields
from typing import Any, Optional

from core.application_services import ApplicationServices
from core.db.session import DatabaseSession
from core.storage.application_state_store import ApplicationStateStore

logger = logging.getLogger(__name__)


@dataclass
class ApplicationContainer:
    """One typed field per object a LifecycleStep needs to hand to
    *another* LifecycleStep during startup, before `app.state` exists.

    This is deliberately not a mirror of everything `ApplicationServices`
    already exposes. `query_cache`, `request_logger`, `request_tracer`,
    `audit_trail`, `api_key_repository`, `user_repository`,
    `api_key_service`, and `authentication_service` all live on
    `application_services` (see core/application_services.py) once that
    step has run; a caller who needs one of those reaches
    `container.application_services.<name>` rather than a second,
    duplicate top-level field here. The fields below are only the ones
    a *different* step genuinely needs before `application_services`
    exists yet -- e.g. `PerformanceStep`'s cross-process snapshot loop
    reads `container.application_state` directly, since
    `ApplicationStateStep` (which builds `application_services`) runs
    after it. See this module's top docstring for the full rationale.

    Adding a new field here is the one place a new cross-step
    dependency needs to be declared -- the same "one place to remember"
    property `core.app.lifecycle.base.LifecycleStep` already has for
    startup/shutdown sequencing.
    """

    db_session: Optional[DatabaseSession] = None
    application_state: Optional[ApplicationStateStore] = None
    application_services: Optional[ApplicationServices] = None
    performance_registry: Optional[Any] = None

    def register(self, name: str, service: Any) -> None:
        """Set a field by name.

        Used by `ApplicationLifespan._register`, which receives each
        step's startup return value as a `{name: instance}` dict and
        doesn't itself know the fixed set of valid names -- this is the
        one place that check happens, so an unrecognized name fails
        loudly here instead of being silently dropped.

        Raises:
            AttributeError: if `name` isn't a declared field above --
                catching a typo immediately rather than a `None`
                surfacing much later at the point of use.
        """
        if name not in {f.name for f in fields(self)}:
            raise AttributeError(
                f"ApplicationContainer has no field {name!r}; add it to "
                "the dataclass in core/app/container.py if this is a new "
                "subsystem."
            )
        setattr(self, name, service)
        logger.info("Registered service: %s", name)

    def clear(self) -> None:
        """Reset every field to None (shutdown)."""
        for f in fields(self):
            setattr(self, f.name, None)
        logger.info("Cleared all registered services")
