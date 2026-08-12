"""ApplicationLifespan — sequencing, registration, state only.

Design
------
ApplicationLifespan owns:
    - startup/shutdown sequencing
    - container registration
    - lifecycle state (what has started, in what order)

It does NOT own:
    - how to connect to a given database
    - what tables to create
    - what SQL to run

Each subsystem (application data, application state store, ...) is a small
`LifecycleStep` (see base.py). A step knows how to start and stop
*itself* and returns the objects that should be registered in the
container. ApplicationLifespan just iterates the list of steps in
order on startup, and in reverse order on shutdown. Adding a new
subsystem means adding one LifecycleStep to the list below — there's
no second place to remember to wire it up, which is what caused the
container-registration and shutdown bugs in the previous version (a
subsystem was connected but its shutdown/registration logic lived
somewhere else and got missed).
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Literal, Optional

from core.app.container import ApplicationContainer
from core.app.lifecycle.application_data import ApplicationDataStep
from core.app.lifecycle.application_state import ApplicationStateStep
from core.app.lifecycle.base import LifecycleStep
from core.app.lifecycle.executors import ExecutorsStep
from core.app.lifecycle.performance import PerformanceStep
from core.app.lifecycle.persistence import PersistenceQueueStep
from core.application_services import ApplicationServices
from core.db.session import DatabaseSession

logger = logging.getLogger(__name__)


class ApplicationLifespan:
    """Manages application startup and shutdown lifecycle for sync and
    async contexts.

    ApplicationLifespan is deliberately thin: it holds an ordered list of
    LifecycleSteps, runs them forward on startup and backward on shutdown,
    and registers whatever each step reports. It has no knowledge of what
    a "database" or "service" is beyond that contract.
    """

    def __init__(
        self,
        db_config=None,
        mode: Literal["sync", "async"] = "sync",
        state_pool_min_size: Optional[int] = None,
        state_pool_max_size: Optional[int] = None,
    ):
        """Initialize application lifespan.

        Args:
            db_config: Optional PostgreSQL configuration, shared by the
                application data and the application state store (api keys,
                users, logging, tracing, caching, audit) -- see
                core.storage.application_state_store's module docstring for why
                there's one database rather than a per-subsystem
                choice.
            mode: "sync" for sync operations, "async" for async operations
            state_pool_min_size: Application-state pool floor. Defaults to
                `core.concurrency.cpu.recommended_sizing()` when not
                given, so the pool scales with the host's actual CPU
                count instead of a constant picked for one machine.
            state_pool_max_size: Application-state pool ceiling; same
                default behavior as `state_pool_min_size`.
        """
        self.mode = mode
        self.container = ApplicationContainer()

        if state_pool_min_size is None or state_pool_max_size is None:
            from core.concurrency.cpu import recommended_sizing

            sizing = recommended_sizing()
            state_pool_min_size = state_pool_min_size or sizing.state_pool_min
            state_pool_max_size = state_pool_max_size or sizing.state_pool_max

        # Explicitly initialize OpenTelemetry here rather than relying on
        # it happening implicitly the first time core.db.session gets
        # imported. get_otel_manager() is a memoized singleton, so this
        # is safe to call even if session.py (or anything else) also
        # triggers it — .initialize() only actually runs once, and this
        # makes the wiring an intentional, logged startup step instead of
        # a side effect buried in an unrelated import. It also has to run
        # before PerformanceStep (below), since that step's OTel export
        # loop reads the global meter provider this call sets up.
        try:
            from core.observability.otel import get_otel_manager

            get_otel_manager()
        except Exception:
            logger.warning(
                "OpenTelemetry initialization failed or unavailable; "
                "tracing/metrics export will be a no-op.",
                exc_info=True,
            )

        # The list of subsystems participating in lifecycle. To add a new
        # subsystem: write a LifecycleStep and append it here. Nothing
        # else needs to change.
        #
        # Order here is start order; shutdown runs in reverse. That
        # reversal is what gives the roadmap's required shutdown
        # sequence "drain persistence queue -> close application state
        # store -> close executors -> close main application data pool"
        # just from listing steps in the opposite sequence:
        # ApplicationDataStep (owns the main pool) starts first so it
        # closes *last*; PersistenceQueueStep starts last so it drains
        # *first*, while ApplicationStateStore and the executors it
        # depends on are still up.
        self._steps: List[LifecycleStep] = [
            PerformanceStep(self.container),
            ApplicationDataStep(db_config),
            ExecutorsStep(),
            ApplicationStateStep(
                db_config,
                pool_min_size=state_pool_min_size,
                pool_max_size=state_pool_max_size,
            ),
            PersistenceQueueStep(),
        ]

        # Steps that successfully started, in start order. Shutdown walks
        # this in reverse, so a step that never started (or failed partway
        # through startup) is never asked to shut down out of order.
        self._started_steps: List[LifecycleStep] = []

    # ============= SYNC LIFECYCLE =============

    def startup_sync(self) -> None:
        """Execute sync startup tasks, in step order.

        Raises:
            Exception: If any step's initialization fails
        """
        try:
            for step in self._steps:
                registrations = step.startup_sync()
                self._register(registrations)
                self._started_steps.append(step)
            logger.info("Application startup completed")
        except Exception as e:
            logger.error(f"Application startup failed: {e}")
            raise

    def shutdown_sync(self) -> None:
        """Execute sync shutdown tasks, in reverse step order."""
        try:
            for step in reversed(self._started_steps):
                step.shutdown_sync()
            self._started_steps.clear()
            self.container.clear()
            logger.info("Application shutdown completed")
        except Exception as e:
            logger.error(f"Application shutdown failed: {e}")
            raise

    # ============= ASYNC LIFECYCLE =============

    async def startup_async(self) -> None:
        """Execute async startup tasks, in step order.

        Raises:
            Exception: If any step's initialization fails
        """
        try:
            for step in self._steps:
                registrations = await step.startup_async()
                self._register(registrations)
                self._started_steps.append(step)
            logger.info("Application startup completed")
        except Exception as e:
            logger.error(f"Application startup failed: {e}")
            raise

    async def shutdown_async(self) -> None:
        """Execute async shutdown tasks, in reverse step order."""
        try:
            for step in reversed(self._started_steps):
                await step.shutdown_async()
            self._started_steps.clear()
            self.container.clear()
            logger.info("Application shutdown completed")
        except Exception as e:
            logger.error(f"Application shutdown failed: {e}")
            raise

    @asynccontextmanager
    async def lifespan_context(self) -> AsyncGenerator[None, None]:
        """FastAPI lifespan context manager for proper lifecycle management.

        Usage with FastAPI:
            lifespan = ApplicationLifespan(db_config, mode="sync")

            @asynccontextmanager
            async def lifespan(app):
                async with lifespan.lifespan_context():
                    yield

            app = FastAPI(lifespan=lifespan)

        Yields:
            None after startup is complete
        """
        if self.mode == "sync":
            self.startup_sync()
        else:
            await self.startup_async()

        yield

        if self.mode == "sync":
            self.shutdown_sync()
        else:
            await self.shutdown_async()

    # ============= REGISTRATION =============

    def _register(self, registrations: Dict[str, Any]) -> None:
        """Register a step's outputs into the container.

        This is the single place that knows how the container's API
        works (e.g. that the db session has its own setter). Steps never
        touch the container directly.
        """
        for key, value in registrations.items():
            if key == "db_session":
                self.container.set_database_session(value)
            else:
                self.container.register(key, value)

    # ============= UTILITIES =============

    def get_container(self) -> ApplicationContainer:
        """Get dependency container.

        Returns:
            ApplicationContainer instance
        """
        return self.container

    def get_db_session(self) -> Optional[DatabaseSession]:
        """Get the application data store session, if the application data store step ran.

        Returns:
            DatabaseSession instance or None
        """
        for step in self._steps:
            if isinstance(step, ApplicationDataStep):
                return step.db_session
        return None

    def get_application_services(self) -> Optional[ApplicationServices]:
        """Get application services, if the application state step ran.

        Returns:
            ApplicationServices instance or None
        """
        for step in self._steps:
            if isinstance(step, ApplicationStateStep):
                return step.application_services
        return None
