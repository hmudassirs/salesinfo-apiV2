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
    started first (see ApplicationLifespan._steps) so it shuts down
    *last*, after the persistence queue has drained, the application
    state store has closed, and the main application data pool has
    closed -- all three dispatch blocking work through these executors
    (`run_in_application_data_executor` / `run_in_state_executor`) as
    part of their own shutdown, so shutting the executors down any
    earlier makes those calls raise `RuntimeError: cannot schedule new
    futures after shutdown`.
    """

    name = "executors"

    def startup_sync(self) -> Dict[str, Any]:
        return {}

    def shutdown_sync(self) -> None:
        shutdown_all_executors(wait=True)
