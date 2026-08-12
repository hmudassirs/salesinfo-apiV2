"""Owns shutdown of the shared application-data/application-state/
background executors.
"""

from typing import Any, Dict

from core.app.lifecycle.base import LifecycleStep
from core.concurrency.executors import shutdown_all_executors


class ExecutorsStep(LifecycleStep):
    """Owns shutdown of the shared application-data/application-state/
    background executors
    (core.concurrency.executors). Configuration/sizing happens eagerly
    at process startup (run_api.py's configure_executors() call, before
    the lifespan even begins) since it only depends on static pool
    config, not anything a LifecycleStep discovers at runtime -- this
    step exists purely to guarantee shutdown happens in the right slot:
    after the persistence queue has drained (so its jobs can still run)
    and after the application state store is closed, but before the main
    application data pool closes (roadmap Phase 10 shutdown ordering).
    """

    name = "executors"

    def startup_sync(self) -> Dict[str, Any]:
        return {}

    def shutdown_sync(self) -> None:
        shutdown_all_executors(wait=True)
