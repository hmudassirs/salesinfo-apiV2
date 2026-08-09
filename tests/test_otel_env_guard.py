import os
from unittest.mock import patch

from core.observability.otel import OpenTelemetryManager


def test_otel_sdk_disabled_when_perf_export_otel_false(monkeypatch):
    monkeypatch.setenv("PERF_EXPORT_OTEL", "false")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")

    manager = OpenTelemetryManager()
    with patch("core.observability.otel.trace.set_tracer_provider") as set_tracer_provider, patch(
        "core.observability.otel.metrics.set_meter_provider"
    ) as set_meter_provider:
        manager.initialize()

    set_tracer_provider.assert_not_called()
    set_meter_provider.assert_not_called()
