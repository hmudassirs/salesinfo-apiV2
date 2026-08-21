"""PostgreSQL database adapter."""

import json
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from core.db.logger import get_logger

if TYPE_CHECKING:
    import psycopg2  # type: ignore
else:
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except ImportError:
        psycopg2 = None  # type: ignore

logger = get_logger(__name__)
sql_logger = get_logger("core.db.adapters.sql")


class StatementTimeoutError(Exception):
    """A statement was cancelled server-side by PostgreSQL's own
    `statement_timeout`, not merely abandoned by the client.

    This is the *only* cancellation mechanism `PostgreSQLAdapter`
    treats as authoritative for a running query -- see `fetch_all`'s
    `statement_timeout_seconds` parameter docstring for why. A client
    that stops awaiting a query (e.g. `asyncio.wait_for()` timing out
    on the coroutine) does not, and cannot, raise this: the underlying
    blocking `cursor.execute()` call is running in a worker thread
    (`core.concurrency.executors`), and Python has no mechanism to
    forcibly interrupt a running thread. Only PostgreSQL itself, via
    `statement_timeout`, can actually stop the query and return
    control to this thread -- which is what makes this exception
    trustworthy as "the query is no longer running" rather than merely
    "something upstream of us got bored of waiting."
    """


def _apply_statement_timeout(cursor, statement_timeout_seconds: Optional[float]) -> None:
    """`SET statement_timeout` (a session-level GUC, not `SET LOCAL`)
    before running the caller's statement on `cursor`'s connection.

    Session-level, not `SET LOCAL`/transaction-scoped: `connect()`
    sets `autocommit = True`, so a plain SQL statement issued through
    `fetch_all`/`fetch_one`/`execute` is its own implicit transaction
    -- a `SET LOCAL` issued as a separate `cursor.execute()` call
    immediately before it would already have expired by the time that
    next statement's own implicit transaction begins, and would
    silently do nothing. Must be paired with
    `_reset_statement_timeout` before the connection goes back to the
    pool, or this caller's timeout would silently apply to whichever
    unrelated caller acquires this connection next.
    """
    if statement_timeout_seconds is not None:
        timeout_ms = max(1, int(statement_timeout_seconds * 1000))
        cursor.execute("SET statement_timeout = %s", (timeout_ms,))


def _reset_statement_timeout(cursor, statement_timeout_seconds: Optional[float]) -> None:
    """Undo `_apply_statement_timeout` -- see that function's docstring
    for why this must run even when the statement itself failed or was
    cancelled (hence always being called from a `finally`)."""
    if statement_timeout_seconds is not None:
        cursor.execute("SET statement_timeout = 0")


def _is_query_canceled(exc: BaseException) -> bool:
    """Was `exc` PostgreSQL's own `QueryCanceled` (SQLSTATE 57014)?

    Checked against `psycopg2.extensions.QueryCanceledError` (stable
    since early psycopg2, unlike the newer per-SQLSTATE
    `psycopg2.errors` classes) with a bare `pgcode` fallback so a fake
    connection/cursor in a test doesn't need to construct a real
    psycopg2 exception instance to exercise this path -- setting
    `.pgcode = "57014"` on any plain exception is enough.

    SQLSTATE 57014 also covers cancellation for reasons other than
    `statement_timeout` specifically (e.g. an administrator calling
    `pg_cancel_backend()`, or `lock_timeout`/`idle_in_transaction_
    session_timeout` on some PostgreSQL versions). Since this is only
    ever checked immediately after this adapter set its own
    `statement_timeout` for this one statement, attributing any 57014
    seen here to "exceeded the configured limit" is accurate enough
    for the caller-facing error message -- the query was cancelled
    server-side either way, which is the property that matters for
    connection-reuse safety.
    """
    if psycopg2 is not None and isinstance(
        exc, psycopg2.extensions.QueryCanceledError
    ):
        return True
    return getattr(exc, "pgcode", None) == "57014"


def translate_qmark_placeholders(sql: str) -> str:
    """Translate `?`-style positional placeholders to psycopg2's `%s`
    pyformat style.

    This exists for exactly one caller: `core.services.query_service
    .QueryService.run`, which executes SQL text submitted by a client
    through the `/api/query` endpoint (`core.app.api.schemas.QueryRequest.sql`).
    That's the one place in this codebase where the SQL text isn't
    something this codebase wrote itself -- a client may have been
    written against the `?` convention from an earlier DuckDB/SQLite
    era before PostgreSQL was the sole supported backend, and changing
    what placeholder syntax `/api/query` accepts would be a breaking
    API change for any such client. Every query this codebase authors
    internally (repositories, migrations, auth coordination state, the
    application state store) is written directly against native `%s`
    placeholders -- there is only one backend, so there is no
    portability reason for internal code to go through this
    translation. See `core.auth.shared_state.PersistentAuthState`'s
    docstring for the same reasoning spelled out at a call site that
    used to (incorrectly) go through this function.

    A plain `sql.replace("?", "%s")` would also mangle any literal
    `?` inside a quoted string (e.g. `WHERE note = 'really?'`), so
    this walks the string tracking single-quote state (with `''` as
    the standard SQL-escaped quote) and only replaces `?` outside of
    a string literal. It does not attempt to understand comments,
    dollar-quoting, or other Postgres-specific quoting -- for a
    console-style ad-hoc query endpoint that's an acceptable
    limitation, not a silent-corruption risk, since anything it can't
    handle correctly is a `?` that stays a literal `?` (a query error
    from Postgres), never a placeholder inserted somewhere wrong.
    """
    if "?" not in sql:
        return sql
    out = []
    in_string = False
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if in_string:
            if ch == "'":
                # `''` inside a string is an escaped quote, not the end
                # of the string -- consume both characters as literal.
                if i + 1 < n and sql[i + 1] == "'":
                    out.append("''")
                    i += 2
                    continue
                in_string = False
            out.append(ch)
        elif ch == "'":
            in_string = True
            out.append(ch)
        elif ch == "?":
            out.append("%s")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


class PostgreSQLAdapter:
    """PostgreSQL database adapter for direct connection management.

    Wired into `DatabaseSession._create_adapter()` via
    `DatabaseConfig.from_postgresql()` -- see that classmethod's
    docstring for why Postgres needs its own config path (discrete
    connection kwargs rather than a single file path).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "postgres",
        echo: bool = False,
        **extra_options: Any,
    ):
        """Initialize PostgreSQL adapter.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            echo: Log every statement (and its params) at INFO via the
                `core.db.adapters.sql` logger.
            **extra_options: Additional connection options, e.g.
                `sslmode`, `connect_timeout`, `application_name` --
                forwarded as-is to `psycopg2.connect()`.
        """
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 package is required. Install with: pip install psycopg2-binary"
            )

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.echo = echo
        self.extra_options = extra_options
        self.connection: Optional[Any] = None
        logger.debug(
            "PostgreSQL adapter initialized: %s@%s:%s/%s", user, host, port, database
        )

    def connect(self) -> None:
        """Establish database connection."""
        try:
            if psycopg2 is None:
                raise ImportError("psycopg2 is required")

            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                **self.extra_options,
            )
            # Without this, psycopg2 defaults to autocommit=False, which
            # left an implicit transaction open after every fetch_one()/
            # fetch_all() call (unlike execute(), below, which explicitly
            # commits). Harmless for plain SELECTs, but for a statement
            # that both writes and reads back via RETURNING -- issued
            # through fetch_one() specifically to capture that value,
            # e.g. core.auth.shared_state.PersistentAuthState
            # .check_and_record_attempt()'s rate-limit UPSERT -- the row
            # lock from that write was never released. It stayed held on
            # that pooled connection until something else unrelated
            # later happened to call execute() (which does commit) on
            # the same connection, so every other caller trying to touch
            # that row in the meantime queued up behind it -- observed
            # in production as login's rate-limit check occasionally
            # taking seconds. Matches the invariant
            # core.storage.application_state_store.ApplicationStateStore's
            # own pool already documents and relies on for the same
            # reason (see _ApplicationStateAdapter's docstring) -- this
            # brings the async/DatabaseSession adapter in line with it.
            # core.performance.adapters.transactions.instrumented_transaction()/
            # instrumented_async_transaction() (explicit BEGIN/COMMIT/
            # ROLLBACK via begin()/commit()/rollback(), not raw SQL)
            # also assume this is already true; it silently wasn't.
            self.connection.autocommit = True

            # Use RealDictCursor for dict-like rows
            self.connection.cursor_factory = psycopg2.extras.RealDictCursor
            logger.info(
                "Connected to PostgreSQL: %s@%s:%s/%s",
                self.user,
                self.host,
                self.port,
                self.database,
            )
        except Exception:
            logger.exception("Failed to connect to PostgreSQL")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL")

    def execute(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        *,
        statement_timeout_seconds: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Execute SQL and return rows when the statement produces a result set.

        Statements such as CREATE/ALTER/DROP/INSERT/UPDATE/DELETE return an
        empty list because PostgreSQL does not provide rows to fetch for them.
        SELECT and other row-producing statements return dictionaries.

        Args:
            statement_timeout_seconds: when set, bounds this one
                statement with PostgreSQL's own `statement_timeout` --
                see `_apply_statement_timeout`'s docstring. Raises
                `StatementTimeoutError` (not the raw psycopg2
                exception) if PostgreSQL cancels it. `None` (the
                default) leaves the connection's existing
                `statement_timeout` untouched -- unbounded unless the
                connection string itself set one.
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = translate_qmark_placeholders(sql)

        try:
            if self.echo:
                sql_logger.info(
                    "SQL EXECUTE: %s -- params=%s",
                    sql,
                    params,
                )

            with self.connection.cursor() as cursor:
                _apply_statement_timeout(cursor, statement_timeout_seconds)
                try:
                    cursor.execute(sql, params)
                    self.connection.commit()

                    if cursor.description is None:
                        return []

                    return [dict(row) for row in cursor.fetchall()]
                except Exception as exc:
                    if statement_timeout_seconds is not None and _is_query_canceled(
                        exc
                    ):
                        raise StatementTimeoutError(
                            f"Statement exceeded the {statement_timeout_seconds}s "
                            "server-side statement_timeout"
                        ) from exc
                    raise
                finally:
                    _reset_statement_timeout(cursor, statement_timeout_seconds)

        except Exception:
            self.connection.rollback()
            logger.exception("Query execution failed")
            raise

    def fetch_all(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        *,
        statement_timeout_seconds: Optional[float] = None,
    ) -> List[Dict]:
        """Fetch all rows from query.

        Args:
            sql: SQL query string (may use `?` placeholders; translated
                to psycopg2's `%s` style before execution)
            params: Query parameters
            statement_timeout_seconds: see `execute`'s docstring for
                this parameter -- same contract here.

        Returns:
            List of row dictionaries
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = translate_qmark_placeholders(sql)
        try:
            if self.echo:
                sql_logger.info("SQL FETCH_ALL: %s -- params=%s", sql, params)
            with self.connection.cursor() as cursor:
                _apply_statement_timeout(cursor, statement_timeout_seconds)
                try:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                    return [dict(row) for row in rows]
                except Exception as exc:
                    if statement_timeout_seconds is not None and _is_query_canceled(
                        exc
                    ):
                        raise StatementTimeoutError(
                            f"Statement exceeded the {statement_timeout_seconds}s "
                            "server-side statement_timeout"
                        ) from exc
                    raise
                finally:
                    _reset_statement_timeout(cursor, statement_timeout_seconds)
        except Exception:
            logger.exception("Query execution failed")
            raise

    def fetch_one(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        *,
        statement_timeout_seconds: Optional[float] = None,
    ) -> Optional[Dict]:
        """Fetch single row from query.

        Args:
            sql: SQL query string (may use `?` placeholders; translated
                to psycopg2's `%s` style before execution)
            params: Query parameters
            statement_timeout_seconds: see `execute`'s docstring for
                this parameter -- same contract here.

        Returns:
            Single row dictionary or None
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = translate_qmark_placeholders(sql)
        try:
            if self.echo:
                sql_logger.info("SQL FETCH_ONE: %s -- params=%s", sql, params)
            with self.connection.cursor() as cursor:
                _apply_statement_timeout(cursor, statement_timeout_seconds)
                try:
                    cursor.execute(sql, params)
                    row = cursor.fetchone()
                    return dict(row) if row else None
                except Exception as exc:
                    if statement_timeout_seconds is not None and _is_query_canceled(
                        exc
                    ):
                        raise StatementTimeoutError(
                            f"Statement exceeded the {statement_timeout_seconds}s "
                            "server-side statement_timeout"
                        ) from exc
                    raise
                finally:
                    _reset_statement_timeout(cursor, statement_timeout_seconds)
        except Exception:
            logger.exception("Query execution failed")
            raise

    def fetch_bounded(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        *,
        max_rows: int,
        max_bytes: int,
        batch_size: int = 500,
        statement_timeout_seconds: Optional[float] = None,
    ) -> Tuple[List[Dict], bool]:
        """Stream results via a server-side cursor, stopping as soon as
        `max_rows`/`max_bytes` is reached -- unlike `fetch_all`, this
        never materializes more of the result set than the caller
        actually wants.

        `fetch_all`'s `cursor.fetchall()` is not actually bounded by
        anything the caller passes it: psycopg2's default (unnamed,
        client-side) cursor has libpq buffer the *entire* result set
        into client memory as soon as `cursor.execute()` runs, before
        a single row is fetched Python-side. A query that happens to
        match far more rows than any caller wants (a missing WHERE
        clause, an unexpectedly broad join) fully materializes in this
        process's memory regardless of the `LIMIT`-like caps
        `core.services.query_service._apply_result_limits` applies
        afterward -- the truncation happens only after paying the full
        memory (and network) cost of the untruncated result.

        A *named* cursor (`self.connection.cursor(name=...)`) is a
        real server-side cursor: PostgreSQL holds the result set on
        the server and sends only `batch_size` rows per `FETCH`, so
        neither side buffers more than a couple of batches at once.
        This method stops issuing further `FETCH`s the moment
        `max_rows`/`max_bytes` is reached, so a query matching a
        billion rows costs this process roughly `2 * batch_size` rows
        of memory, not a billion.

        Named cursors need an explicit transaction -- `DECLARE ...
        CURSOR` is scoped to the transaction that created it, and
        `connect()` normally runs this connection in autocommit mode
        (see that method's docstring), which would auto-commit, and
        thus destroy, the cursor before the first `FETCH`. This method
        temporarily disables autocommit around the declare/fetch/close
        sequence and always restores it before returning -- including
        on error -- since every other method on this class (and the
        connection pool that hands this connection to the next caller)
        assumes autocommit is on.

        Returns:
            `(rows, truncated)` -- `rows` has at most `max_rows`
            entries and its cumulative JSON-serialized size is at most
            `max_bytes` (both checked incrementally per row, not by
            serializing the whole batch first, for the same reason
            `core.services.query_service._apply_result_limits` does
            this incrementally); `truncated` is `True` if either limit
            caused this to stop short of the query's actual result.
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = translate_qmark_placeholders(sql)
        cursor_name = f"salesinfo_stream_{uuid.uuid4().hex}"

        rows: List[Dict] = []
        total_bytes = 0
        truncated = False

        self.connection.autocommit = False
        try:
            if self.echo:
                sql_logger.info("SQL FETCH_BOUNDED: %s -- params=%s", sql, params)
            with self.connection.cursor(name=cursor_name) as cursor:
                cursor.itersize = batch_size
                _apply_statement_timeout(cursor, statement_timeout_seconds)
                try:
                    cursor.execute(sql, params)
                    while True:
                        batch = cursor.fetchmany(batch_size)
                        if not batch:
                            break
                        stop = False
                        for row in batch:
                            if len(rows) >= max_rows:
                                truncated = True
                                stop = True
                                break
                            row_dict = dict(row)
                            row_bytes = len(
                                json.dumps(row_dict, default=str).encode("utf-8")
                            )
                            if total_bytes + row_bytes > max_bytes:
                                truncated = True
                                stop = True
                                break
                            rows.append(row_dict)
                            total_bytes += row_bytes
                        if stop:
                            break
                except Exception as exc:
                    if statement_timeout_seconds is not None and _is_query_canceled(
                        exc
                    ):
                        raise StatementTimeoutError(
                            f"Statement exceeded the {statement_timeout_seconds}s "
                            "server-side statement_timeout"
                        ) from exc
                    raise
                finally:
                    _reset_statement_timeout(cursor, statement_timeout_seconds)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            logger.exception("Query execution failed")
            raise
        finally:
            self.connection.autocommit = True

        return rows, truncated

    def get_tables(self) -> List[str]:
        """Get list of tables in database.

        Returns:
            List of table names
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
        """

        try:
            rows = self.fetch_all(sql)
            return [row["table_name"] for row in rows]
        except Exception:
            logger.exception("Failed to get tables")
            raise

    def get_schema(self, table: str) -> List[Dict[str, Any]]:
        """Get table schema information.

        Args:
            table: Table name

        Returns:
            List of column information dictionaries
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = """
        SELECT 
            column_name as name, 
            data_type as type, 
            is_nullable,
            column_default as default_value
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
        """

        try:
            rows = self.fetch_all(sql, (table,))
            return [dict(row) for row in rows]
        except Exception as e:
            logger.exception("Failed to get schema for table %s: %s", table, e)
            raise

    def create_table(
        self,
        table: str,
        columns: Dict[str, str],
        if_not_exists: bool = True,
    ) -> None:
        """Create a table.

        Args:
            table: Table name
            columns: Column definitions {name: type}
            if_not_exists: Only create if table doesn't exist

        Raises:
            RuntimeError: If not connected
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        col_defs = ", ".join(
            [f"{name} {col_type}" for name, col_type in columns.items()]
        )
        sql = f"CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {table} ({col_defs})"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.connection.commit()
                logger.info("Created table: %s", table)
        except Exception as e:
            self.connection.rollback()
            logger.exception("Failed to create table %s: %s", table, e)
            raise

    def drop_table(self, table: str, if_exists: bool = True) -> None:
        """Drop a table.

        Args:
            table: Table name
            if_exists: Only drop if table exists

        Raises:
            RuntimeError: If not connected
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        sql = f"DROP TABLE {'IF EXISTS' if if_exists else ''} {table}"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.connection.commit()
                logger.info("Dropped table: %s", table)
        except Exception as e:
            self.connection.rollback()
            logger.exception("Failed to drop table %s: %s", table, e)
            raise


# Self-register with the AdapterRegistry on import -- see
# core/db/adapters/__init__.py for why this is eager. Constructed with
# discrete host/port/database/user/password keywords (unpacked from
# DatabaseConfig.extra_options by DatabaseSession._create_adapter() --
# see DatabaseConfig.from_postgresql()'s docstring), not a path string.
from core.db.registry import get_adapter_registry  # noqa: E402

if not get_adapter_registry().is_registered("postgresql"):
    get_adapter_registry().register("postgresql", PostgreSQLAdapter)
