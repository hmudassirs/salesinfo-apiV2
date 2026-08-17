"""PostgreSQL database adapter."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

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


def translate_qmark_placeholders(sql: str) -> str:
    """Translate `?`-style positional placeholders (what every existing
    `/api/query` client and the rest of this codebase's SQL was written
    against, a convention carried over from an earlier DuckDB/SQLite
    era -- PostgreSQL is the sole supported backend now) to
    psycopg2's `%s` pyformat style.

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

    Public (not `_`-prefixed) because `core.storage.application_state_store`'s
    Postgres-backed adapter reuses it too, so every `?`-placeholder
    query in this codebase is translated identically regardless of
    which adapter runs it -- see that module's docstring.
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
        logger.debug(f"PostgreSQL adapter initialized: {user}@{host}:{port}/{database}")

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
            # core.db.transactions.transaction()/async_transaction()
            # (explicit BEGIN/COMMIT/ROLLBACK statements) also assume
            # this is already true; it silently wasn't.
            self.connection.autocommit = True

            # Use RealDictCursor for dict-like rows
            self.connection.cursor_factory = psycopg2.extras.RealDictCursor
            logger.info(
                f"Connected to PostgreSQL: {self.user}@{self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
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
    ) -> List[Dict[str, Any]]:
        """Execute SQL and return rows when the statement produces a result set.

        Statements such as CREATE/ALTER/DROP/INSERT/UPDATE/DELETE return an
        empty list because PostgreSQL does not provide rows to fetch for them.
        SELECT and other row-producing statements return dictionaries.
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
                cursor.execute(sql, params)
                self.connection.commit()

                if cursor.description is None:
                    return []

                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Query execution failed: {e}")
            raise

    def fetch_all(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict]:
        """Fetch all rows from query.

        Args:
            sql: SQL query string (may use `?` placeholders; translated
                to psycopg2's `%s` style before execution)
            params: Query parameters

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
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def fetch_one(self, sql: str, params: Optional[List[Any]] = None) -> Optional[Dict]:
        """Fetch single row from query.

        Args:
            sql: SQL query string (may use `?` placeholders; translated
                to psycopg2's `%s` style before execution)
            params: Query parameters

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
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

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
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
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
            logger.error(f"Failed to get schema for table {table}: {e}")
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
                logger.info(f"Created table: {table}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to create table {table}: {e}")
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
                logger.info(f"Dropped table: {table}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to drop table {table}: {e}")
            raise


# Self-register with the AdapterRegistry on import -- see
# core/db/adapters/__init__.py for why this is eager. Constructed with
# discrete host/port/database/user/password keywords (unpacked from
# DatabaseConfig.extra_options by DatabaseSession._create_adapter() --
# see DatabaseConfig.from_postgresql()'s docstring), not a path string.
from core.db.registry import get_adapter_registry  # noqa: E402

if not get_adapter_registry().is_registered("postgresql"):
    get_adapter_registry().register("postgresql", PostgreSQLAdapter)
