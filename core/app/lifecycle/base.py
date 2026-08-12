"""The lifecycle step contract shared by every subsystem step."""

from abc import ABC, abstractmethod
from typing import Any, Dict


class LifecycleStep(ABC):
    """A single subsystem's slice of application startup/shutdown.

    A step is responsible only for its own subsystem: connecting,
    initializing, and tearing down. It reports back what should be
    registered in the container; it never touches the container directly.
    That keeps all registration decisions in ApplicationLifespan, in one
    place, instead of scattered across every subsystem.
    """

    #: Short name used in logs when a step fails.
    name: str = "unnamed_step"

    @abstractmethod
    def startup_sync(self) -> Dict[str, Any]:
        """Run sync startup work.

        Returns:
            Mapping of {registration_key: instance} to register in the
            container. Return an empty dict if this step is disabled
            (e.g. no config was provided for it).
        """
        raise NotImplementedError

    @abstractmethod
    def shutdown_sync(self) -> None:
        """Run sync shutdown/cleanup work. Must be safe to call even if
        startup_sync returned an empty dict (i.e. this step never started)."""
        raise NotImplementedError

    async def startup_async(self) -> Dict[str, Any]:
        """Run async startup work. Default: delegate to the sync version.

        Override this for subsystems with real async connection code
        (e.g. an async DB driver). Subsystems that are inherently
        blocking (e.g. ApplicationStateStore's psycopg2 connections) should
        override this to offload the sync call with
        `asyncio.to_thread`, not pretend to be async.
        """
        return self.startup_sync()

    async def shutdown_async(self) -> None:
        """Run async shutdown work. Default: delegate to the sync version."""
        self.shutdown_sync()
