# core/config_env.py
"""Shared environment-variable parsing helpers.

This module intentionally has zero internal imports so every layer of
the application -- observability, performance, lifecycle, settings --
can depend on it without creating a cycle.

Previously, three separate modules (`core.observability.otel`,
`core.app.lifecycle.performance`, `core.app.lifecycle.application_state`)
each defined their own private `_env_flag(name, default)` with
identical truthy-string parsing. That duplication is exactly the kind
of configuration boilerplate flagged in the framework review: the same
few lines copy-pasted because no shared, dependency-free home existed
for them. This module is that home.

This does not by itself centralize *all* configuration into one
settings tree (see `core.app.settings.AppSettings` for the started-but-
incomplete version of that) -- it only removes the duplicate parsing
logic. Call sites still each read their own environment variables;
only the *how to parse a bool/int/float* logic is now shared.
"""

from __future__ import annotations

import os


def env_flag(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable.

    Recognizes "1", "true", "yes", "on" (case-insensitive) as truthy;
    anything else, or the variable being unset, falls back to
    `default`.
    """
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    """Parse an integer environment variable, falling back to
    `default` if unset."""
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    """Parse a float environment variable, falling back to `default`
    if unset."""
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)
