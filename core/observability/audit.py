"""Security/compliance audit trail (who did what, when), persisted to the application state store."""

import json
import time
from typing import Any, Dict, List, Optional

from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)


class AuditTrail:
    """Service for managing audit logs.

    `log_audit_event()` is called directly (not via
    `run_in_state_executor`) from several async call sites --
    `core.auth.authentication_service.AuthenticationService.authenticate`
    in particular, on every login. Before `set_queue()` was wired in,
    that meant every login blocked the *event loop thread itself* on a
    real psycopg2 INSERT + commit -- not just adding latency to that
    one request, but stalling every other in-flight request on the
    same worker for the duration of the round trip, since nothing else
    could run on that thread until it returned. `run_in_state_executor`
    would have fixed the "blocks this request" half of that (it just
    moves the block to a worker thread), but not the "audit is on
    login's critical path at all" half -- see `set_queue()`.
    """

    def __init__(
        self,
        application_state: ApplicationStateStore,
        write_queue: Optional[Any] = None,
    ):
        """Initialize audit service.

        Args:
            application_state: Application state store instance
            write_queue: Optional `core.observability.write_queue
                .ObservabilityWriteQueue` to enqueue onto instead of
                writing synchronously -- see `set_queue()`. Usually
                left `None` here and wired in afterward via
                `set_queue()`, since the queue isn't running yet at the
                point `ApplicationServices` constructs this (see
                `core.app.lifecycle.application_state.ApplicationStateStep
                .startup_sync`).
        """
        self.application_state = application_state
        self._write_queue = write_queue

    def set_queue(self, write_queue: Any) -> None:
        """Wire in the background flush queue after it's started.

        Once set, an unqueued `log_audit_event()` call (the normal
        case -- see that method's `_adapter` parameter) enqueues the
        record and returns immediately instead of acquiring a pooled
        connection and committing on the caller's own thread. This is
        the same latency fix `core.observability.write_queue` already
        gives general request logging/tracing (batching many records'
        worth of writes into one background transaction instead of one
        round trip per request) -- audit logging just wasn't routed
        through it yet. Without a queue wired in (this method never
        called, e.g. a script/test building `AuditTrail` directly),
        `log_audit_event()` keeps writing synchronously as before.
        """
        self._write_queue = write_queue

    def log_audit_event(
        self,
        event_type: str,
        action: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        ip_address: str = "",
        user_agent: str = "",
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        old_values: Dict[str, Any] = None,
        new_values: Dict[str, Any] = None,
        success: bool = True,
        error_message: str = "",
        metadata: Dict[str, Any] = None,
        _adapter=None,
    ) -> None:
        """Log an audit event.

        Args:
            event_type: Type of event (e.g., 'auth', 'api_key', 'user')
            action: Action performed (e.g., 'login', 'create', 'delete')
            user_id: User ID performing the action
            session_id: Session ID
            ip_address: Client IP address
            user_agent: User agent string
            resource_type: Type of resource affected
            resource_id: ID of resource affected
            old_values: Previous values (for updates)
            new_values: New values (for updates/creates)
            success: Whether the action succeeded
            error_message: Error message if failed
            metadata: Additional metadata
            _adapter: see RequestLogger.log_event()'s docstring. When
                given, this call is already running inside the write
                queue's background flush transaction (see
                `core.observability.context.write_observability_record`)
                -- write directly against it rather than enqueuing
                again, or the record would never actually get written.
        """
        if _adapter is None and self._write_queue is not None:
            # Fast path: hand off to the background flush thread and
            # return immediately -- no pooled-connection acquire, no
            # commit, on the request path. Mirrors
            # core.observability.context.emit_request_observability's
            # fast path for the same reason; see set_queue()'s
            # docstring.
            self._write_queue.enqueue(
                {
                    "audit": dict(
                        event_type=event_type,
                        action=action,
                        user_id=user_id,
                        session_id=session_id,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        resource_type=resource_type,
                        resource_id=resource_id,
                        old_values=old_values,
                        new_values=new_values,
                        success=success,
                        error_message=error_message,
                        metadata=metadata,
                    )
                }
            )
            return

        timestamp = int(time.time())

        old_values_json = json.dumps(old_values) if old_values else None
        new_values_json = json.dumps(new_values) if new_values else None
        metadata_json = json.dumps(metadata) if metadata else None

        sql = """
        INSERT INTO audit_log (
            timestamp, event_type, user_id, session_id, ip_address, user_agent,
            resource_type, resource_id, action, old_values, new_values,
            success, error_message, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            timestamp,
            event_type,
            user_id,
            session_id,
            ip_address,
            user_agent,
            resource_type,
            resource_id,
            action,
            old_values_json,
            new_values_json,
            success,
            error_message,
            metadata_json,
        )

        try:
            if _adapter is not None:
                self.application_state.execute_on(_adapter, sql, params)
            else:
                self.application_state.execute(sql, params)
        except Exception:
            logger.exception("Failed to log audit event")

    def get_audit_events(
        self,
        limit: int = 100,
        offset: int = 0,
        event_type: str = "",
        user_id: str = "",
        resource_type: str = "",
        resource_id: str = "",
        action: str = "",
        start_time: int = 0,
        end_time: int = 0,
        success: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve audit events with filtering.

        Args:
            limit: Maximum number of events to return
            offset: Offset for pagination
            event_type: Filter by event type
            user_id: Filter by user ID
            resource_type: Filter by resource type
            resource_id: Filter by resource ID
            action: Filter by action
            start_time: Filter events after this timestamp
            end_time: Filter events before this timestamp
            success: Filter by success status

        Returns:
            List of audit events
        """
        conditions = []
        params = []

        if event_type:
            conditions.append("event_type = %s")
            params.append(event_type)

        if user_id:
            conditions.append("user_id = %s")
            params.append(user_id)

        if resource_type:
            conditions.append("resource_type = %s")
            params.append(resource_type)

        if resource_id:
            conditions.append("resource_id = %s")
            params.append(resource_id)

        if action:
            conditions.append("action = %s")
            params.append(action)

        if start_time:
            conditions.append("timestamp >= %s")
            params.append(start_time)

        if end_time:
            conditions.append("timestamp <= %s")
            params.append(end_time)

        if success is not None:
            conditions.append("success = %s")
            params.append(success)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
        SELECT * FROM audit_log
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """

        params.extend([limit, offset])

        try:
            rows = self.application_state.fetch_all(sql, tuple(params))
            events = []
            for row in rows:
                event_dict = dict(row)
                # Parse JSON fields
                for field in ["old_values", "new_values", "metadata"]:
                    if event_dict.get(field):
                        try:
                            event_dict[field] = json.loads(event_dict[field])
                        except (TypeError, ValueError):
                            # Malformed/non-JSON stored value -- see
                            # the matching comment in
                            # core.observability.request_tracer for why
                            # this is deliberately narrow, not bare.
                            event_dict[field] = None
                    else:
                        event_dict[field] = None
                events.append(event_dict)
            return events
        except Exception:
            logger.exception("Failed to retrieve audit events")
            return []
