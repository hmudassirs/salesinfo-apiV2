"""Bucketed histograms and streaming percentile estimation.

Two complementary shapes are provided:

- `Histogram` keeps fixed bucket boundaries plus running count/sum, in the
  style of a Prometheus histogram. It answers "how many observations fell
  under X" cheaply and is exporter-friendly.
- `StreamingPercentileEstimator` implements the P^2 algorithm (Jain &
  Chlamtac, 1985), which tracks an approximate quantile in O(1) memory
  per quantile without storing samples. It answers "what is p99" for an
  unbounded stream where keeping every sample is not viable.

Both are updated by the aggregator from completed `MetricPoint` events;
neither reads the clock or does I/O.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field

from .constants import DEFAULT_HISTOGRAM_BUCKET_BOUNDS_MS, NANOSECONDS_PER_MILLISECOND
from .exceptions import (
    _HISTOGRAM_INVALID_QUANTILE_MESSAGE,
    _HISTOGRAM_NO_SAMPLES_MESSAGE,
    HistogramError,
)
from .types import MetricName, MetricValue, Tags


@dataclass(slots=True)
class Histogram:
    """A fixed-bucket cumulative histogram over observed values."""

    name: MetricName
    tags: Tags = field(default_factory=dict)
    bucket_bounds: tuple[float, ...] = DEFAULT_HISTOGRAM_BUCKET_BOUNDS_MS
    _bucket_counts: list[int] = field(init=False, repr=False)
    count: int = field(default=0, init=False)
    total: float = field(default=0.0, init=False)
    minimum: float | None = field(default=None, init=False)
    maximum: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Validate bucket boundaries and allocate the (+Inf) counter row."""
        if list(self.bucket_bounds) != sorted(self.bucket_bounds):
            raise HistogramError("bucket_bounds must be sorted ascending")  # noqa: TRY003
        # One extra bucket for the implicit +Inf upper bound.
        self._bucket_counts = [0] * (len(self.bucket_bounds) + 1)

    def observe(self, value: MetricValue) -> None:
        """Record one observation into the running count, sum, and buckets.

        `value` is a raw nanosecond duration everywhere this is called
        from in practice (`MetricPoint.value` is always `duration_ns` --
        see `request_profiler.py`), but `bucket_bounds` is documented
        and defaulted (`DEFAULT_HISTOGRAM_BUCKET_BOUNDS_MS`) in whole
        milliseconds. Bucket assignment converts to match; `count`,
        `total`, `minimum`, `maximum` stay in the original nanosecond
        units to match `StreamingPercentileEstimator`'s quantiles and
        every existing consumer of `snapshot()`'s `sum`/`mean`/`min`/
        `max` fields, which already assume nanoseconds (e.g. the
        dashboard divides them by 1e6 for display). Getting this
        conversion wrong here (as an earlier version of this method
        did, comparing the raw nanosecond value directly against
        millisecond bounds) doesn't throw or otherwise announce
        itself -- every observation just silently lands in the final
        (+Inf) bucket, since even a 1ms duration is already
        1,000,000ns, dwarfing every finite bound. `cumulative_counts()`
        still "works" in that state (all cumulative counts equal
        `count`), which is why it went unnoticed: nothing exercised the
        *distribution* across buckets until `percentile_from_buckets()`
        needed one that wasn't degenerate.
        """
        value = float(value)
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        index = bisect_left(self.bucket_bounds, value / NANOSECONDS_PER_MILLISECOND)
        self._bucket_counts[index] += 1

    @property
    def mean(self) -> float | None:
        """Return the arithmetic mean of observed values, or None if empty."""
        if self.count == 0:
            return None
        return self.total / self.count

    def cumulative_counts(self) -> dict[str, int]:
        """Return cumulative per-bucket counts keyed by upper bound label."""
        labels = [str(bound) for bound in self.bucket_bounds] + ["+Inf"]
        running = 0
        result: dict[str, int] = {}
        for label, bucket_count in zip(labels, self._bucket_counts, strict=True):
            running += bucket_count
            result[label] = running
        return result

    @property
    def bucket_counts(self) -> list[int]:
        """Return non-cumulative per-bucket counts, one per bound plus +Inf.

        Unlike `cumulative_counts()` (a display-friendly, labeled,
        running total), this is the raw per-bucket tally -- exactly
        what's needed to *merge* histograms from multiple processes:
        summing two processes' `bucket_counts` element-wise (same
        `bucket_bounds`, so same shape) gives the bucket counts of the
        combined population, from which `count`/`total`/`min`/`max` and
        an interpolated quantile can all be recomputed. See
        `core.performance.dashboard.merge`.
        """
        return list(self._bucket_counts)


# The five markers P^2 tracks: the two extremes, the target quantile, and
# one marker on either side used to keep the middle marker's spacing
# balanced as new samples arrive.
_MARKER_COUNT = 5


@dataclass(slots=True)
class StreamingPercentileEstimator:
    """Track one approximate quantile over an unbounded stream (P^2)."""

    quantile: float
    _heights: list[float] = field(default_factory=list, init=False, repr=False)
    _positions: list[int] = field(default_factory=list, init=False, repr=False)
    _desired_positions: list[float] = field(
        default_factory=list, init=False, repr=False
    )
    _increments: list[float] = field(default_factory=list, init=False, repr=False)
    _initial_samples: list[float] = field(
        default_factory=list, init=False, repr=False
    )
    observation_count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        """Validate the target quantile is a proper probability."""
        if not 0.0 < self.quantile < 1.0:
            raise HistogramError(_HISTOGRAM_INVALID_QUANTILE_MESSAGE)

    def observe(self, value: MetricValue) -> None:
        """Feed one new sample into the estimator."""
        value = float(value)
        self.observation_count += 1

        if len(self._initial_samples) < _MARKER_COUNT:
            self._initial_samples.append(value)
            if len(self._initial_samples) == _MARKER_COUNT:
                self._initialize_markers()
            return

        self._update_markers(value)

    def _initialize_markers(self) -> None:
        self._initial_samples.sort()
        self._heights = list(self._initial_samples)
        self._positions = [1, 2, 3, 4, 5]
        p = self.quantile
        self._desired_positions = [1, 1 + 2 * p, 1 + 4 * p, 3 + 2 * p, 5]
        self._increments = [0, p / 2, p, (1 + p) / 2, 1]

    def _update_markers(self, value: float) -> None:
        heights = self._heights
        positions = self._positions

        if value < heights[0]:
            heights[0] = value
            cell = 0
        elif value >= heights[-1]:
            heights[-1] = value
            cell = 3
        else:
            cell = bisect_left(heights, value) - 1
            cell = max(0, min(cell, 3))

        for i in range(cell + 1, _MARKER_COUNT):
            positions[i] += 1
        for i in range(_MARKER_COUNT):
            self._desired_positions[i] += self._increments[i]

        for i in range(1, _MARKER_COUNT - 1):
            desired = self._desired_positions[i]
            diff = desired - positions[i]
            right_gap = positions[i + 1] - positions[i]
            left_gap = positions[i] - positions[i - 1]

            if (diff >= 1 and right_gap > 1) or (diff <= -1 and left_gap > 1):
                direction = 1 if diff >= 1 else -1
                adjusted = _parabolic(heights, positions, i, direction)
                if heights[i - 1] < adjusted < heights[i + 1]:
                    heights[i] = adjusted
                else:
                    heights[i] = _linear(heights, positions, i, direction)
                positions[i] += direction

    @property
    def value(self) -> float | None:
        """Return the current quantile estimate, or None with no samples."""
        if self.observation_count == 0:
            return None
        if len(self._initial_samples) < _MARKER_COUNT:
            samples = sorted(self._initial_samples)
            index = min(len(samples) - 1, int(self.quantile * (len(samples) - 1)))
            return samples[index]
        return self._heights[2]


def _parabolic(heights: list[float], positions: list[int], i: int, d: int) -> float:
    n_left, n_mid, n_right = positions[i - 1], positions[i], positions[i + 1]
    h_left, h_mid, h_right = heights[i - 1], heights[i], heights[i + 1]
    term_a = (n_mid - n_left + d) * (h_right - h_mid) / (n_right - n_mid)
    term_b = (n_right - n_mid - d) * (h_mid - h_left) / (n_mid - n_left)
    return h_mid + d / (n_right - n_left) * (term_a + term_b)


def _linear(heights: list[float], positions: list[int], i: int, d: int) -> float:
    neighbor = i + d
    return heights[i] + d * (heights[neighbor] - heights[i]) / (
        positions[neighbor] - positions[i]
    )


def percentile_from_buckets(
    bucket_bounds: tuple[float, ...],
    bucket_counts: list[int],
    quantile: float,
) -> float | None:
    """Estimate a quantile from (already-summed) per-bucket counts.

    Used to recompute p50/p90/p95/p99 for a histogram *merged* across
    processes (see `core.performance.dashboard.merge`): each process's
    P^2 streaming estimator (`StreamingPercentileEstimator` above) has
    internal marker state that cannot be validly averaged or summed
    across processes, but the underlying bucketed `Histogram` can --
    bucket counts are exact integers, and summing them element-wise
    gives the exact bucket counts of the combined population. This
    walks the merged buckets to find the one the target rank falls
    into and linearly interpolates within it, the same technique
    Prometheus's `histogram_quantile()` uses. It trades some precision
    (bounded by bucket width) for being mathematically valid to merge,
    unlike the streaming estimator.
    """
    total = sum(bucket_counts)
    if total == 0:
        return None
    target_rank = quantile * total
    running = 0
    lower_bound = 0.0
    for bound, count in zip(bucket_bounds, bucket_counts[:-1], strict=True):
        if running + count >= target_rank:
            if count == 0:
                return bound
            fraction = (target_rank - running) / count
            return lower_bound + fraction * (bound - lower_bound)
        running += count
        lower_bound = bound
    # Fell into the implicit +Inf bucket: no upper bound to interpolate
    # against, so the best available estimate is the last finite bound.
    return bucket_bounds[-1] if bucket_bounds else lower_bound


@dataclass(slots=True)
class StreamingHistogram:
    """Pair a bucketed `Histogram` with multiple streaming quantiles."""

    name: MetricName
    tags: Tags = field(default_factory=dict)
    quantiles: tuple[float, ...] = (0.5, 0.9, 0.95, 0.99)
    bucket_bounds: tuple[float, ...] = DEFAULT_HISTOGRAM_BUCKET_BOUNDS_MS
    histogram: Histogram = field(init=False)
    _estimators: dict[float, StreamingPercentileEstimator] = field(
        init=False, repr=False
    )

    def __post_init__(self) -> None:
        """Build the underlying bucket histogram and quantile estimators."""
        self.histogram = Histogram(
            name=self.name, tags=self.tags, bucket_bounds=self.bucket_bounds
        )
        self._estimators = {
            q: StreamingPercentileEstimator(q) for q in self.quantiles
        }

    def observe(self, value: MetricValue) -> None:
        """Record one observation into the histogram and every estimator."""
        self.histogram.observe(value)
        for estimator in self._estimators.values():
            estimator.observe(value)

    def quantile(self, q: float) -> float:
        """Return the current estimate for a configured quantile."""
        estimator = self._estimators.get(q)
        if estimator is None:
            raise HistogramError(f"quantile {q} is not tracked by this histogram")  # noqa: TRY003
        value = estimator.value
        if value is None:
            raise HistogramError(_HISTOGRAM_NO_SAMPLES_MESSAGE)
        return value

    def snapshot(self) -> dict[str, float | int | None]:
        """Return count, mean, and every tracked quantile as a flat dict."""
        result: dict[str, float | int | None] = {
            "count": self.histogram.count,
            "sum": self.histogram.total,
            "mean": self.histogram.mean,
            "min": self.histogram.minimum,
            "max": self.histogram.maximum,
        }
        for q, estimator in self._estimators.items():
            result[f"p{int(q * 100)}"] = estimator.value
        return result
