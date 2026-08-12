"""Helpers for request-level observability and context propagation."""

from __future__ import annotations

import secrets
import time
import uuid
from typing import Any, Dict, Optional


def build_request_context(request: Any) -> Dict[str, Any]:
    """Create or reuse request context values for logging and tracing."""
    state = getattr(request, "state", None)
    if state is None:
        state = type("State", (), {})()
        request.state = state

    request_id = getattr(state, "request_id", None) or str(uuid.uuid4())
    session_id = getattr(state, "session_id", None) or request.headers.get(
        "x-session-id", ""
    )
    user_id = getattr(state, "user_id", None) or None
    username = getattr(state, "username", None) or request.headers.get("x-username", "")
    ip_address = getattr(state, "ip_address", None) or (
        request.client.host if getattr(request, "client", None) else "unknown"
    )
    user_agent = getattr(state, "user_agent", None) or request.headers.get(
        "user-agent", ""
    )

    state.request_id = request_id
    state.session_id = session_id
    state.user_id = user_id
    state.username = username
    state.ip_address = ip_address
    state.user_agent = user_agent

    return {
        "request_id": request_id,
        "session_id": session_id,
        "user_id": user_id,
        "username": username,
        "ip_address": ip_address,
        "user_agent": user_agent,
    }


def emit_request_observability(
    application_services: Any,
    request: Any,
    status_code: int = 200,
    duration_ms: Optional[float] = None,
    error: Optional[str] = None,
    user_id: Optional[str] = None,
    operation_name: str = "http.request",
) -> None:
    """Emit request log, trace, and audit entries for a completed request."""
    context = build_request_context(request)
    if user_id:
        context["user_id"] = user_id
        request.state.user_id = user_id

    if context.get("username"):
        request.state.username = context["username"]

    if duration_ms is None:
        duration_ms = 0.0

    trace_id = f"trace-{context['request_id']}"
    span_id = f"span-{secrets.token_hex(4)}"

    record = {
        "log": dict(
            method=getattr(request, "method", "REQUEST"),
            url=str(getattr(request, "url", "")),
            status_code=status_code,
            duration_ms=duration_ms,
            user_id=context["user_id"] or None,
            ip_address=context["ip_address"],
            user_agent=context["user_agent"],
            request_id=context["request_id"],
            session_id=context["session_id"],
            error=error,
        ),
        "trace_start": dict(
            trace_id=trace_id,
            span_id=span_id,
            operation_name=operation_name,
            service_name="preparedata-api",
            parent_span_id="",
            user_id=context["user_id"] or None,
            session_id=context["session_id"],
            request_id=context["request_id"],
            service_version="1.0.0",
        ),
        "trace_end": dict(
            trace_id=trace_id,
            span_id=span_id,
            status="OK" if status_code < 400 else "ERROR",
            error_message=error or "",
            http_method=getattr(request, "method", "REQUEST"),
            http_url=str(getattr(request, "url", "")),
            http_status_code=status_code,
            db_query="",
            db_duration_ms=0,
            tags={
                "duration_ms": round(duration_ms, 3),
                "username": context.get("username", ""),
                "session_id": context.get("session_id", ""),
            },
        ),
        "audit": dict(
            event_type="request.completed",
            action="request",
            user_id=context["user_id"] or None,
            session_id=context["session_id"],
            ip_address=context["ip_address"],
            user_agent=context["user_agent"],
            resource_type="request",
            resource_id=context["request_id"],
            success=status_code < 400,
            error_message=error or "",
            metadata={
                "status_code": status_code,
                "duration_ms": round(duration_ms, 3),
                "username": context.get("username", ""),
                "session_id": context.get("session_id", ""),
            },
        ),
    }

    write_queue = getattr(application_services, "observability_queue", None)
    if write_queue is not None:
        # Fast path: hand off to the background flush thread and return
        # immediately. This is the whole point — no application-state-store
        # write, no pool checkout, in the request path at all.
        write_queue.enqueue(record)
        return

    # Fallback for setups without a queue wired in (e.g. simpler test
    # doubles for application_services): write synchronously, still batched
    # into one transaction per request as before queuing was added.
    application_state = getattr(application_services, "application_state", None)
    if application_state is not None:
        with application_state.transaction() as adapter:
            write_observability_record(application_services, adapter, record)
    else:
        write_observability_record(application_services, None, record)


def write_observability_record(
    application_services: Any, adapter: Any, record: Dict[str, Any]
) -> None:
    """Perform the actual log/trace/audit writes for one record.

    Called either synchronously (fallback path above) or, normally, once
    per queued record from ObservabilityWriteQueue's background flush —
    in that case `adapter` is shared across many records in one
    transaction, which is the actual latency fix: many requests' worth
    of writes happen under one write-lock acquisition instead of one
    each.
    """
    application_services.logging.log_request(**record["log"], _adapter=adapter)
    application_services.tracing.start_trace(**record["trace_start"], _adapter=adapter)
    application_services.tracing.end_trace(**record["trace_end"], _adapter=adapter)
    application_services.audit.log_audit_event(**record["audit"], _adapter=adapter)
