"""Application lifecycle package.

Split out of the former monolithic `core/app/lifespan.py` (~35 KB) into
one module per subsystem, following the `LifecycleStep` abstraction
that already existed there. Each step still knows how to start/stop
only its own subsystem; `manager.ApplicationLifespan` still just
iterates the list in order. Nothing about the design changed here,
only which file each piece lives in:

    base.py             LifecycleStep contract
    application_data.py ApplicationDataStep (application-data DB pool)
    application_state.py ApplicationStateStep (api keys/users/logging/
                         tracing/caching/audit + ApplicationServices)
    performance.py      PerformanceStep (core.performance registry,
                         collectors, OTel export, cross-process publish)
    persistence.py      PersistenceQueueStep (cache persistence queue)
    executors.py        ExecutorsStep (shared executor shutdown)
    manager.py          ApplicationLifespan (sequencing/registration)

`core/app/lifespan.py` re-exports everything from here for backward
compatibility with existing imports (e.g. `from core.app.lifespan
import ApplicationLifespan`); new code should import from
`core.app.lifecycle` (or the specific submodule) directly.
"""

from core.app.lifecycle.application_data import ApplicationDataStep
from core.app.lifecycle.application_state import ApplicationStateStep
from core.app.lifecycle.base import LifecycleStep
from core.app.lifecycle.executors import ExecutorsStep
from core.app.lifecycle.manager import ApplicationLifespan
from core.app.lifecycle.performance import PerformanceStep
from core.app.lifecycle.persistence import PersistenceQueueStep

__all__ = [
    "ApplicationLifespan",
    "LifecycleStep",
    "ApplicationDataStep",
    "ApplicationStateStep",
    "PerformanceStep",
    "PersistenceQueueStep",
    "ExecutorsStep",
]
