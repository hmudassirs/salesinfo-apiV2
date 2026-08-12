"""Starts/stops the bounded cache-persistence queue."""

import asyncio
import logging
from typing import Any, Dict

from core.app.lifecycle.base import LifecycleStep
from core.caching.persistence_queue import start_persistence_queue, stop_persistence_queue

logger = logging.getLogger(__name__)


class PersistenceQueueStep(LifecycleStep):
    """Starts/stops the bounded cache-persistence queue
    (core.caching.persistence_queue) that QueryCacheCoordinator uses
    for out-of-band L2 writes and access-stat updates.

    Ordering matters here: this step is started *after*
    ApplicationStateStep (so it shows up later in `_steps`, meaning its
    shutdown runs *before* ApplicationStateStep's in the reversed
    shutdown walk -- see `manager.ApplicationLifespan`'s docstring).
    That's required by the roadmap's shutdown sequence: "stop accepting
    new background jobs, drain queue within timeout, [then] close the
    application state store" -- persistence jobs write to the
    application state store, so it must still be open while the queue
    drains.
    """

    name = "persistence_queue"

    def startup_sync(self) -> Dict[str, Any]:
        start_persistence_queue()
        return {}

    async def startup_async(self) -> Dict[str, Any]:
        start_persistence_queue()
        return {}

    def shutdown_sync(self) -> None:
        # stop_persistence_queue() is async (it awaits queue.join());
        # there's no meaningful sync equivalent, so best-effort run it
        # to completion here rather than skip draining entirely.
        try:
            asyncio.run(stop_persistence_queue())
        except RuntimeError:
            # Already inside a running loop (shouldn't happen from the
            # sync shutdown path, but fail safe rather than crash
            # shutdown over it).
            logger.warning(
                "Could not synchronously drain cache persistence queue "
                "(already inside an event loop); some queued writes may "
                "be lost."
            )

    async def shutdown_async(self) -> None:
        await stop_persistence_queue()
