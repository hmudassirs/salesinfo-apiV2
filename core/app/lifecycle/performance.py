"""Owns the `core.performance` registry and its optional background
resource-collector scheduler, OTel export loop, and cross-process
snapshot publishing.
"""

import asyncio
import contextlib
import logging
import os
from dataclasses import replace
from typing import Any, Dict, Optional

from core.app.lifecycle.base import LifecycleStep

# Optional: this codebase's core.performance instrumentation (tracing,
# metrics, pool/SQL adapters, resource collectors — see
# docs/performance/README.md). Guarded the same way the OpenTelemetry
# wiring in manager.py is: this step must keep working with
# core.performance absent.
try:
    from core.performance.collectors import CollectorScheduler, build_enabled_collectors
    from core.performance.config import PerformanceConfig
    from core.performance.registry import get_default_registry
except Exception:
    CollectorScheduler = build_enabled_collectors = None  # type: ignore[assignment,misc]
    PerformanceConfig = get_default_registry = None  # type: ignore[assignment,misc]

# Optional: bridges the performance registry's counters/gauges/
# histograms onto OpenTelemetry metrics. This class existed in the
# codebase but nothing ever called it on any schedule — see this
# step's docstring for how/why that's wired up below.
try:
    from core.performance.exporters.otel_exporter import OTelExporter
except Exception:
    OTelExporter = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    """Parse a boolean env var, matching PerformanceConfig._read_bool's rules."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class PerformanceStep(LifecycleStep):
    """Owns the `core.performance` registry and its optional background
    resource-collector scheduler (CPU/memory/GC/threads/asyncio/process —
    see `docs/performance/collectors-exporters-dashboard.md`), plus an
    optional background task that bridges that registry onto OpenTelemetry.

    Registers the process-wide default registry so
    `install_performance_middleware`/`install_performance_dashboard`
    (installed on the app in `core.app.api.app.create_app`) and this
    step agree on which registry they're both touching, without either
    one having to construct or pass one around explicitly.

    Disabled by default, matching `PerformanceConfig`'s own
    fail-safe-disabled philosophy: with `PERF_ENABLED` unset, this step
    still registers the (empty, inert) registry but starts no
    background work. The collector scheduler and the OTel export loop
    only ever run in async mode — sync mode (e.g. `LifecycleMode.SYNC`,
    used by tests and scripts) has no running event loop to host them
    on, the same constraint `ApplicationStateStep` documents for
    ApplicationStateStore's psycopg2 connections.

    OTel export: `core.performance.exporters.otel_exporter.OTelExporter`
    turns the registry's current counters/gauges/histograms into OTel
    metric instruments on every call to `.export()`, but nothing used to
    call it on any schedule — it sat unused in the codebase. This step
    now runs it every `PERF_OTEL_EXPORT_INTERVAL_SECONDS` (default 15s)
    for the lifetime of the app, so request/DB/pool/auth timings and
    resource-collector samples flow through the same OTLP pipeline
    `core.observability.otel.OpenTelemetryManager` already pushes traces
    (and its own db/cache metrics) through. `OTelExporter` reads the
    global meter via `opentelemetry.metrics.get_meter(...)`, so it picks
    up whatever `MeterProvider` `get_otel_manager()` configured in
    `ApplicationLifespan.__init__` — that call happens first, so
    the provider is already in place by the time this step starts.
    Controlled by `PERF_EXPORT_OTEL` (defaults on whenever the
    performance module itself is enabled); set it to `0`/`false` to keep
    the registry populated (e.g. for the live dashboard) without also
    pushing it to OTel.

    Cross-process snapshot publishing: with `--workers N > 1`, each
    worker is a separate process with its own registry — see
    `core.performance.cross_process`'s module docstring for why that
    used to make `/debug/performance` only ever show one worker's
    slice of traffic. This step also starts a loop that periodically
    publishes this worker's own summary to Postgres (shared by every
    worker via `container`'s `application_state`, registered later by
    `ApplicationStateStep` — this step runs first, so the loop starts
    before `application_state` exists and simply skips publishing until it
    does) so `/debug/performance` can merge every worker's most recent
    snapshot together. Interval: `PERF_CROSS_PROCESS_PUBLISH_INTERVAL_SECONDS`
    (default 2s, matching the dashboard's own refresh interval).
    """

    name = "performance"

    def __init__(self, container: Optional[Any] = None) -> None:
        self.scheduler: Optional[Any] = None
        self._otel_export_task: Optional["asyncio.Task[None]"] = None
        self._snapshot_publish_task: Optional["asyncio.Task[None]"] = None
        self._container = container

    def startup_sync(self) -> Dict[str, Any]:
        if PerformanceConfig is None or get_default_registry is None:
            return {}
        registry = get_default_registry()
        return {"performance_registry": registry}

    async def startup_async(self) -> Dict[str, Any]:
        if PerformanceConfig is None or get_default_registry is None:
            return {}
        registry = get_default_registry()
        # PerformanceConfig.from_env() intentionally does not read the
        # collect_* flags (no env var wired up for them yet — see
        # docs/performance/configuration.md), so pull them in here
        # ourselves via dataclasses.replace rather than silently getting
        # an all-False config and starting no collectors.
        config = replace(
            PerformanceConfig.from_env(),
            collect_memory=_env_flag("PERF_COLLECT_MEMORY"),
            collect_gc=_env_flag("PERF_COLLECT_GC"),
            collect_threads=_env_flag("PERF_COLLECT_THREADS"),
            collect_cpu=_env_flag("PERF_COLLECT_CPU"),
            collect_asyncio=_env_flag("PERF_COLLECT_ASYNCIO"),
            collect_process=_env_flag("PERF_COLLECT_PROCESS"),
        )
        if config.enabled and build_enabled_collectors is not None:
            collectors = build_enabled_collectors(config)
            if collectors:
                self.scheduler = CollectorScheduler(collectors, registry)
                self.scheduler.start()
                logger.info(
                    "Performance resource collectors started: %s",
                    [c.name for c in collectors],
                )

        # Bridge the performance registry onto OpenTelemetry on a fixed
        # interval. Gated on the performance module itself being enabled
        # (no point running an export loop over an always-empty
        # registry) and, separately, on OTelExporter having imported
        # successfully.
        if (
            config.enabled
            and OTelExporter is not None
            and _env_flag("PERF_EXPORT_OTEL", default=True)
        ):
            interval_seconds = float(
                os.getenv("PERF_OTEL_EXPORT_INTERVAL_SECONDS", "15")
            )
            self._otel_export_task = asyncio.ensure_future(
                self._run_otel_export(registry, interval_seconds)
            )
            logger.info(
                "Performance-to-OTel export started (interval=%.1fs)",
                interval_seconds,
            )

        # Cross-process snapshot publishing (see the class docstring):
        # gated the same way as the collectors/OTel export above --
        # only when the performance module itself is enabled, and only
        # when this step was actually given a container to read
        # application_state from later (ApplicationLifespan always passes
        # one; a standalone PerformanceStep() as tests/benchmarks might
        # construct just skips this, same as they already skip the
        # OTel export loop if OTelExporter isn't importable).
        if config.enabled and self._container is not None:
            publish_interval_seconds = float(
                os.getenv("PERF_CROSS_PROCESS_PUBLISH_INTERVAL_SECONDS", "2")
            )
            self._snapshot_publish_task = asyncio.ensure_future(
                self._run_snapshot_publish(
                    registry, self._container, publish_interval_seconds
                )
            )
            logger.info(
                "Cross-process performance snapshot publishing started "
                "(interval=%.1fs)",
                publish_interval_seconds,
            )

        return {"performance_registry": registry}

    @staticmethod
    async def _run_otel_export(registry: Any, interval_seconds: float) -> None:
        """Periodically push the registry's current state onto OTel.

        Runs for the lifetime of the app; cancellation (on shutdown) is
        the normal, expected way this loop ends.
        """
        exporter = OTelExporter()
        while True:
            try:
                exporter.export(registry)
            except Exception:
                logger.warning(
                    "Performance-to-OTel export failed", exc_info=True
                )
            await asyncio.sleep(interval_seconds)

    @staticmethod
    async def _run_snapshot_publish(
        registry: Any, container: Any, interval_seconds: float
    ) -> None:
        """Periodically publish this worker's summary for other workers to read.

        Runs for the lifetime of the app; cancellation (on shutdown) is
        the normal, expected way this loop ends. `application_state` isn't
        registered in `container` yet on this loop's first iterations
        (`PerformanceStep` runs before `ApplicationStateStep` -- see
        `ApplicationLifespan.__init__`'s step-ordering comment), so
        each iteration re-reads it from `container` rather than
        capturing it once; the loop is a no-op until it appears, then
        publishes every interval for as long as the app runs.
        """
        # Imported lazily, matching the rest of this module's pattern
        # for optional/late-available subsystems (see the OTelExporter
        # import guard above): core.performance itself might be absent
        # in a stripped-down environment even when this loop is asked
        # to run, and there's no point failing startup over it.
        from core.performance.cross_process import WorkerSnapshotStore
        from core.performance.dashboard.summary import build_performance_summary

        store: Optional[WorkerSnapshotStore] = None
        while True:
            application_state = container.get_application_state()
            if application_state is not None:
                if store is None:
                    store = WorkerSnapshotStore(application_state)
                try:
                    summary = build_performance_summary(registry)
                    await asyncio.to_thread(store.publish, summary)
                except Exception:
                    logger.warning(
                        "Cross-process performance snapshot publish failed",
                        exc_info=True,
                    )
            await asyncio.sleep(interval_seconds)

    def shutdown_sync(self) -> None:
        pass  # the scheduler/export loop are only ever started in async mode

    async def shutdown_async(self) -> None:
        if self.scheduler is not None:
            await self.scheduler.stop()
        if self._otel_export_task is not None:
            self._otel_export_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._otel_export_task
            self._otel_export_task = None
        if self._snapshot_publish_task is not None:
            self._snapshot_publish_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._snapshot_publish_task
            self._snapshot_publish_task = None
