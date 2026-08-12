"""Bounded, purpose-specific thread pools for offloading blocking I/O.

See executors.py for why this exists.
"""

from core.concurrency.cpu import ConcurrencySizing, detect_cpu_count, recommended_sizing
from core.concurrency.executors import (
    ExecutorMetrics,
    background_executor,
    configure_executors,
    application_data_executor,
    fire_and_forget,
    run_in_background,
    run_in_application_data_executor,
    run_in_state_executor,
    application_state_executor,
    shutdown_all_executors,
)

__all__ = [
    "ConcurrencySizing",
    "ExecutorMetrics",
    "background_executor",
    "configure_executors",
    "application_data_executor",
    "detect_cpu_count",
    "fire_and_forget",
    "recommended_sizing",
    "run_in_background",
    "run_in_application_data_executor",
    "run_in_state_executor",
    "application_state_executor",
    "shutdown_all_executors",
]
