"""Publish/collect per-worker performance snapshots via Postgres.

Lives under `core.performance.adapters` (moved here from
`core.performance.adapters.cross_process`) for the same reason the rest of this
package does: it's the one piece of `core.performance` with a hard,
unguarded dependency on an application-specific store
(`core.storage.application_state_store.ApplicationStateStore`), so it
belongs with the other adapters rather than in `core.performance`'s
otherwise-standalone core (registry, aggregator, histogram, timer,
metric, types -- none of which import anything outside
`core.performance` itself). Unlike this package's other adapters, it
doesn't wrap an existing object for per-call timing; it's still
grouped here because "reaches outside `core.performance`" is the
dividing line this package draws, not "wraps something for timing"
specifically -- see `core.performance.adapters`'s own docstring.

Each `uvicorn run_api:app --workers N` worker (N > 1) is a separate OS
process with its own `core.performance.registry.PerformanceRegistry` --
there's no shared memory between them (see `registry.py`'s module
docstring: "the only stateful, shared object" is scoped per-process).
Historically that meant `/debug/performance` only ever reflected
whichever single worker the load balancer happened to route that one
request to, silently hiding the other N-1 workers' share of traffic --
this was flagged directly against real `--workers 4` load-test data:
`request_count` in the snapshot came back far below the number of
requests actually fired, because 3 of the 4 workers' history was never
represented at all.

This module closes that gap using the one thing every worker already
shares: the same Postgres database (`core.storage.application_state_store`). Each
worker periodically publishes its own
`core.performance.dashboard.summary.build_performance_summary(...)`
output here, keyed by `"hostname:pid"` (see `PerformanceStep` in
`core/app/lifespan.py` for the publishing loop). Reading back every row
updated within the last few publish intervals and merging them
(`core.performance.dashboard.merge`) gives a fleet-wide view with zero
coordination between workers -- a worker never picked to serve
`/debug/performance` itself still shows up, because it published its
own row independently on its own schedule.

With exactly one worker this degrades to reading back that one worker's
own single row -- i.e. the same numbers `/debug/performance` always
showed, just round-tripped through Postgres. Single-worker behavior is
unchanged (aside from the one-row "merge" recomputing histogram
quantiles from bucket data instead of reusing the streaming estimator --
see `percentile_from_buckets`'s docstring for why that's a deliberate,
small precision trade rather than a bug).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from typing import Any

from core.storage.application_state_store import ApplicationStateStore

logger = logging.getLogger(__name__)


def worker_id() -> str:
    """This process's identity in `perf_worker_snapshots`: "hostname:pid"."""
    return f"{socket.gethostname()}:{os.getpid()}"


class WorkerSnapshotStore:
    """Publish this worker's snapshot / collect every fresh worker's."""

    def __init__(
        self, application_state: ApplicationStateStore, worker_id_override: str | None = None
    ) -> None:
        self._application_state = application_state
        self.worker_id = worker_id_override or worker_id()

    def publish(self, snapshot: dict[str, Any]) -> None:
        """Upsert this worker's current snapshot.

        Failures are logged and swallowed rather than raised: this runs
        on a background loop (see `PerformanceStep._run_snapshot_publish`
        in `core/app/lifespan.py`) with no caller waiting on the
        result, and a transient DB hiccup here should never be able to
        affect request handling or crash the publish loop -- the next
        interval just tries again.
        """
        payload = json.dumps(snapshot)
        try:
            self._application_state.execute(
                """
                INSERT INTO perf_worker_snapshots
                    (worker_id, hostname, pid, updated_at, snapshot_json)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (worker_id) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    snapshot_json = EXCLUDED.snapshot_json
                """,
                (
                    self.worker_id,
                    socket.gethostname(),
                    os.getpid(),
                    int(time.time()),
                    payload,
                ),
            )
        except Exception:
            logger.warning(
                "Failed to publish performance snapshot for worker %s",
                self.worker_id,
                exc_info=True,
            )

    def collect_fresh(self, max_age_seconds: float) -> list[dict[str, Any]]:
        """Return every worker's snapshot updated within `max_age_seconds`.

        A worker that stopped publishing (crashed, killed, or simply
        never started -- e.g. `PERF_ENABLED=false`) ages out of this
        naturally; there's no separate cleanup/expiry job because a
        live table of "current state per worker" only ever needs to be
        as fresh as its last read, not proactively pruned.
        """
        cutoff = int(time.time() - max_age_seconds)
        rows = self._application_state.fetch_all(
            "SELECT worker_id, snapshot_json FROM perf_worker_snapshots "
            "WHERE updated_at >= %s",
            (cutoff,),
        )
        snapshots: list[dict[str, Any]] = []
        for row in rows:
            try:
                data = json.loads(row["snapshot_json"])
            except (TypeError, ValueError):
                logger.warning(
                    "Skipping unparseable performance snapshot for worker %s",
                    row["worker_id"],
                )
                continue
            data["_worker_id"] = row["worker_id"]
            snapshots.append(data)
        return snapshots
