"""Merge multiple workers' `build_performance_summary(...)` outputs into
one fleet-wide view.

Each dict in `snapshots` is exactly what `summary.build_performance_summary`
returns for one worker process (see that module), collected back via
`core.performance.adapters.cross_process.WorkerSnapshotStore.collect_fresh`. This
module has no dependency on Postgres, FastAPI, or the registry itself --
it only combines already-serialized dicts -- so it's directly unit
testable the same way `summary.py` is kept separate from `fastapi.py`.

Merge rules, per field:
  - `request_count` / `total_requests_recorded`: summed -- each is a
    per-process total, so the fleet total is the sum.
  - `max_request_history`: the max across workers, purely informational
    (it's a configured cap, not a measurement).
  - `counters`: summed per (name, tags) -- counters are additive by
    definition.
  - `gauges`: summed per (name, tags), *except* names in
    `HOST_LEVEL_GAUGES` (facts about the shared host, like
    `cpu_logical_count`, that would be double-counted by summing one
    copy per worker) -- those take the max instead.
  - `histograms`: bucket counts summed per (name, tags) -- see
    `core.performance.histogram.percentile_from_buckets` for why this
    is the only mathematically valid way to combine percentile data
    from independent P^2 estimators -- then count/sum/min/max/mean and
    every quantile are recomputed from the merged buckets.
  - `recent_requests`: concatenated across workers and truncated to
    `recent_limit`. Not chronologically interleaved -- `RequestProfile`
    carries no absolute wall-clock timestamp, only a duration, so there
    is no correct cross-worker ordering to sort by. Treat this field as
    "some recent requests from across the fleet", not "the N most
    recent requests overall".
"""

from __future__ import annotations

from core.performance.constants import NANOSECONDS_PER_MILLISECOND
from core.performance.histogram import percentile_from_buckets

HOST_LEVEL_GAUGES = frozenset({"cpu_logical_count"})

_QUANTILES = (0.5, 0.9, 0.95, 0.99)


def _tag_key(tags: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(tags.items()))


def merge_performance_summaries(
    snapshots: list[dict[str, object]], recent_limit: int = 20
) -> dict[str, object]:
    """Combine per-worker summaries into one fleet-wide summary dict.

    Same shape as a single worker's `build_performance_summary(...)`
    output, so callers (the `/debug/performance` route, the live
    dashboard) don't need to know whether they're looking at one
    process or many. With exactly one snapshot in `snapshots`, this is
    a no-op pass-through (recomputing histogram quantiles from that one
    worker's own buckets rather than reusing its P^2 estimator values,
    which will usually match closely but not bit-for-bit -- see
    `percentile_from_buckets`'s docstring).
    """
    if not snapshots:
        return {
            "request_count": 0,
            "total_requests_recorded": 0,
            "max_request_history": 0,
            "counters": [],
            "gauges": [],
            "histograms": [],
            "recent_requests": [],
            "worker_count": 0,
        }

    request_count = 0
    total_requests_recorded = 0
    max_request_history = 0
    counters: dict[tuple, dict[str, object]] = {}
    gauges: dict[tuple, dict[str, object]] = {}
    histograms: dict[tuple, dict[str, object]] = {}
    recent_requests: list[dict[str, object]] = []

    for snapshot in snapshots:
        request_count += int(snapshot.get("request_count", 0))
        total_requests_recorded += int(snapshot.get("total_requests_recorded", 0))
        max_request_history = max(
            max_request_history, int(snapshot.get("max_request_history", 0))
        )

        worker_id = snapshot.get("_worker_id")
        for entry in snapshot.get("recent_requests", []):
            tagged = dict(entry)
            if worker_id is not None:
                tagged["worker_id"] = worker_id
            recent_requests.append(tagged)

        for counter in snapshot.get("counters", []):
            key = (counter["name"], _tag_key(counter.get("tags", {})))
            existing = counters.get(key)
            if existing is None:
                counters[key] = {
                    "name": counter["name"],
                    "tags": dict(counter.get("tags", {})),
                    "value": counter["value"],
                }
            else:
                existing["value"] += counter["value"]

        for gauge in snapshot.get("gauges", []):
            key = (gauge["name"], _tag_key(gauge.get("tags", {})))
            existing = gauges.get(key)
            if existing is None:
                gauges[key] = {
                    "name": gauge["name"],
                    "tags": dict(gauge.get("tags", {})),
                    "value": gauge["value"],
                }
            elif gauge["name"] in HOST_LEVEL_GAUGES:
                existing["value"] = max(existing["value"], gauge["value"])
            else:
                existing["value"] += gauge["value"]

        for hist in snapshot.get("histograms", []):
            bucket_bounds = tuple(hist.get("bucket_bounds", ()))
            bucket_counts = list(hist.get("bucket_counts", []))
            key = (hist["name"], _tag_key(hist.get("tags", {})))
            existing = histograms.get(key)
            if existing is None:
                histograms[key] = {
                    "name": hist["name"],
                    "tags": dict(hist.get("tags", {})),
                    "bucket_bounds": bucket_bounds,
                    "bucket_counts": list(bucket_counts),
                    "count": int(hist.get("count") or 0),
                    "sum": float(hist.get("sum") or 0.0),
                    "min": hist.get("min"),
                    "max": hist.get("max"),
                }
            else:
                if existing["bucket_bounds"] != bucket_bounds:
                    # Different bucket layout (e.g. mixed code versions
                    # mid-rollout) -- can't merge bucket-for-bucket, so
                    # keep whichever worker we saw first rather than
                    # silently producing nonsense counts.
                    continue
                existing["bucket_counts"] = [
                    a + b
                    for a, b in zip(
                        existing["bucket_counts"], bucket_counts, strict=True
                    )
                ]
                existing["count"] += int(hist.get("count") or 0)
                existing["sum"] += float(hist.get("sum") or 0.0)
                if hist.get("min") is not None:
                    existing["min"] = (
                        hist["min"]
                        if existing["min"] is None
                        else min(existing["min"], hist["min"])
                    )
                if hist.get("max") is not None:
                    existing["max"] = (
                        hist["max"]
                        if existing["max"] is None
                        else max(existing["max"], hist["max"])
                    )

    merged_histograms = []
    for entry in histograms.values():
        count = entry["count"]
        result = {
            "name": entry["name"],
            "tags": entry["tags"],
            "count": count,
            "sum": entry["sum"],
            "mean": (entry["sum"] / count) if count else None,
            "min": entry["min"],
            "max": entry["max"],
        }
        for q in _QUANTILES:
            estimate_ms = percentile_from_buckets(
                entry["bucket_bounds"], entry["bucket_counts"], q
            )
            # bucket_bounds is in milliseconds (Histogram.observe()
            # converts before bisecting -- see its docstring); every
            # other field here (mean/sum/min/max, and a single
            # worker's own P^2-estimator-derived p50/p90/p95/p99) is
            # in nanoseconds, so convert back for a consistent,
            # drop-in-compatible result shape.
            result[f"p{int(q * 100)}"] = (
                estimate_ms * NANOSECONDS_PER_MILLISECOND
                if estimate_ms is not None
                else None
            )
        merged_histograms.append(result)

    return {
        "request_count": request_count,
        "total_requests_recorded": total_requests_recorded,
        "max_request_history": max_request_history,
        "counters": list(counters.values()),
        "gauges": list(gauges.values()),
        "histograms": merged_histograms,
        "recent_requests": recent_requests[:recent_limit] if recent_limit > 0 else [],
        "worker_count": len(snapshots),
    }
