"""Backward-compatible re-export shim.

`core/app/lifespan.py` used to be a single ~35 KB module containing
every LifecycleStep plus ApplicationLifespan itself. It's been split
into `core/app/lifecycle/` (one module per subsystem — see that
package's docstring). This file now only re-exports the same names
from their new home, so existing imports like
`from core.app.lifespan import ApplicationLifespan` keep working
unchanged. New code should import from `core.app.lifecycle` directly.
"""

from core.app.lifecycle import (
    ApplicationDataStep,
    ApplicationLifespan,
    ApplicationStateStep,
    ExecutorsStep,
    LifecycleStep,
    PerformanceStep,
    PersistenceQueueStep,
)

__all__ = [
    "ApplicationLifespan",
    "LifecycleStep",
    "ApplicationDataStep",
    "ApplicationStateStep",
    "PerformanceStep",
    "PersistenceQueueStep",
    "ExecutorsStep",
]
