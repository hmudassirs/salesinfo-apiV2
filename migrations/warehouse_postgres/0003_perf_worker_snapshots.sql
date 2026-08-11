-- Per-worker performance-snapshot publishing (see
-- core/performance/cross_process.py's module docstring). With
-- `--workers N > 1`, each uvicorn worker is a separate OS process with
-- its own in-memory `core.performance` registry -- there is no shared
-- memory between them. This table is how they publish their own
-- current snapshot on a short interval so `/debug/performance` can
-- merge all of them into one fleet-wide view instead of only ever
-- showing whichever single worker happened to serve that one request.
--
-- One row per worker (`worker_id` = "hostname:pid"), overwritten in
-- place on every publish rather than accumulating history -- this is a
-- live "what does each worker look like right now" table, not a log.
-- Staleness (a worker that crashed or was killed) is handled entirely
-- by the reader filtering on `updated_at`, not by deleting rows here.

CREATE TABLE IF NOT EXISTS perf_worker_snapshots (
    worker_id VARCHAR PRIMARY KEY,
    hostname VARCHAR NOT NULL,
    pid INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_perf_worker_snapshots_updated
    ON perf_worker_snapshots(updated_at);
