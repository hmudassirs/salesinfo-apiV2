"""Distributed request tracing, persisted to the application state store. Renamed from the old `TracingService` to avoid colliding with core.db.tracing's generic TracingContext/@traced_operation, an unrelated in-process utility."""

import json
import time
from typing import Any, Dict, List, Optional

from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore

logger = get_logger(__name__)


class RequestTracer:
    """Service for managing distributed traces."""

    def __init__(self, application_state: ApplicationStateStore):
        """Initialize tracing service.

        Args:
            application_state: Application state store instance
        """
        self.application_state = application_state

    def start_trace(
        self,
        trace_id: str,
        span_id: str,
        operation_name: str,
        service_name: str,
        parent_span_id: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        request_id: Optional[str] = None,
        service_version: Optional[str] = None,
        _adapter=None,
    ) -> str:
        """Start a new trace span.

        Args:
            trace_id: Trace ID
            span_id: Span ID
            operation_name: Operation name
            service_name: Service name
            parent_span_id: Parent span ID
            user_id: User ID
            session_id: Session ID
            request_id: Request ID
            service_version: Service version
            _adapter: see RequestLogger.log_event()'s docstring.

        Returns:
            Span ID
        """
        start_time = int(time.time() * 1000000)  # Microseconds

        sql = """
        INSERT INTO traces (
            trace_id, span_id, parent_span_id, operation_name, start_time,
            service_name, service_version, user_id, session_id, request_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = (
            trace_id,
            span_id,
            parent_span_id,
            operation_name,
            start_time,
            service_name,
            service_version,
            user_id,
            session_id,
            request_id,
        )

        try:
            if _adapter is not None:
                self.application_state.execute_on(_adapter, sql, params)
            else:
                self.application_state.execute(sql, params)
            return span_id
        except Exception:
            logger.exception("Failed to start trace")
            return span_id

    def end_trace(
        self,
        trace_id: str,
        span_id: str,
        status: str = "OK",
        error_message: str = "",
        http_method: str = "",
        http_url: str = "",
        http_status_code: int = 0,
        db_query: str = "",
        db_duration_ms: int = 0,
        tags: Dict[str, Any] = None,
        _adapter=None,
    ) -> None:
        """End a trace span.

        Args:
            trace_id: Trace ID
            span_id: Span ID
            status: Span status
            error_message: Error message if any
            http_method: HTTP method
            http_url: HTTP URL
            http_status_code: HTTP status code
            db_query: Database query
            db_duration_ms: Database query duration
            tags: Additional tags
            _adapter: see RequestLogger.log_event()'s docstring. Used for
                both the read (start_time lookup) and the write here, so
                both happen against the same connection/transaction.
        """
        end_time = int(time.time() * 1000000)  # Microseconds

        # Calculate duration
        read_sql = "SELECT start_time FROM traces WHERE trace_id = %s AND span_id = %s"
        read_params = (trace_id, span_id)
        if _adapter is not None:
            start_result = self.application_state.fetch_one_on(
                _adapter, read_sql, read_params
            )
        else:
            start_result = self.application_state.fetch_one(read_sql, read_params)

        duration_ms = 0
        if start_result:
            duration_ms = (
                end_time - start_result[0]
            ) // 1000  # Convert to milliseconds

        tags_json = json.dumps(tags) if tags else ""

        sql = """
        UPDATE traces SET
            end_time = %s, duration_ms = %s, status = %s, error_message = %s,
            http_method = %s, http_url = %s, http_status_code = %s,
            db_query = %s, db_duration_ms = %s, tags = %s
        WHERE trace_id = %s AND span_id = %s
        """

        params = (
            end_time,
            duration_ms,
            status,
            error_message,
            http_method,
            http_url,
            http_status_code,
            db_query,
            db_duration_ms,
            tags_json,
            trace_id,
            span_id,
        )

        try:
            if _adapter is not None:
                self.application_state.execute_on(_adapter, sql, params)
            else:
                self.application_state.execute(sql, params)
        except Exception:
            logger.exception("Failed to end trace")

    def get_traces(
        self,
        limit: int = 100,
        offset: int = 0,
        service_name: str = "",
        operation_name: str = "",
        user_id: str = "",
        request_id: str = "",
        start_time: int = 0,
        end_time: int = 0,
        status: str = "",
    ) -> List[Dict[str, Any]]:
        """Retrieve traces with filtering.

        Args:
            limit: Maximum number of traces to return
            offset: Offset for pagination
            service_name: Filter by service name
            operation_name: Filter by operation name
            user_id: Filter by user ID
            request_id: Filter by request ID
            start_time: Filter traces after this timestamp
            end_time: Filter traces before this timestamp
            status: Filter by status

        Returns:
            List of trace entries
        """
        conditions = []
        params = []

        if service_name:
            conditions.append("service_name = %s")
            params.append(service_name)

        if operation_name:
            conditions.append("operation_name = %s")
            params.append(operation_name)

        if user_id:
            conditions.append("user_id = %s")
            params.append(user_id)

        if request_id:
            conditions.append("request_id = %s")
            params.append(request_id)

        if start_time:
            conditions.append("start_time >= %s")
            params.append(start_time * 1000000)  # Convert to microseconds

        if end_time:
            conditions.append("start_time <= %s")
            params.append(end_time * 1000000)  # Convert to microseconds

        if status:
            conditions.append("status = %s")
            params.append(status)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
        SELECT * FROM traces
        WHERE {where_clause}
        ORDER BY start_time DESC
        LIMIT %s OFFSET %s
        """

        params.extend([limit, offset])

        try:
            rows = self.application_state.fetch_all(sql, tuple(params))
            traces = []
            for row in rows:
                trace_dict = dict(row)
                # Parse tags JSON
                if trace_dict.get("tags"):
                    try:
                        trace_dict["tags"] = json.loads(trace_dict["tags"])
                    except (TypeError, ValueError):
                        # Malformed/non-JSON stored value -- don't let
                        # one bad row take down the whole listing.
                        # Deliberately narrow: a bare `except:` here
                        # would also swallow KeyboardInterrupt/
                        # SystemExit and mask real bugs elsewhere in
                        # this block.
                        trace_dict["tags"] = {}
                else:
                    trace_dict["tags"] = {}
                traces.append(trace_dict)
            return traces
        except Exception:
            logger.exception("Failed to retrieve traces")
            return []
