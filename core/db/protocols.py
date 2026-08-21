"""Database adapter protocol and abstractions (SOLID - Interface Segregation Principle)."""

from typing import Any, Dict, List, Optional, Protocol, Sequence


class DatabaseAdapter(Protocol):
    """Minimal database adapter contract.

    Defines the interface that all adapters must implement.
    """

    connection: Any

    def connect(self) -> None:
        """Establish database connection."""
        ...

    def disconnect(self) -> None:
        """Close database connection."""
        ...

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> Any:
        """Execute SQL query.

        Args:
            sql: SQL query string
            params: Query parameters
            statement_timeout_seconds: bound this one statement with
                the backend's own server-side cancellation mechanism
                (PostgreSQL: `SET statement_timeout`), if it has one.
                `None` (the default) leaves any existing timeout
                untouched. See `core.db.adapters.postgresql
                ._apply_statement_timeout`'s docstring for why this
                needs to be a real, server-side mechanism rather than
                the caller simply giving up on awaiting the call --
                only the backend itself can guarantee the statement
                has actually stopped running before the connection is
                reused.

        Returns:
            Query result
        """
        ...

    def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> List[Dict]:
        """Fetch all rows from query.

        Args:
            sql: SQL query string
            params: Query parameters
            statement_timeout_seconds: see `execute`'s docstring for
                this parameter.

        Returns:
            List of row dictionaries
        """
        ...

    def fetch_one(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        statement_timeout_seconds: float | None = None,
    ) -> Optional[Dict]:
        """Fetch single row from query.

        Args:
            sql: SQL query string
            params: Query parameters
            statement_timeout_seconds: see `execute`'s docstring for
                this parameter.

        Returns:
            Single row dictionary or None
        """
        ...

    def get_tables(self) -> List[str]:
        """Get list of tables in database.

        Returns:
            List of table names
        """
        ...


# Note: this module previously also defined `QueryCache` and
# `TransactionHandler` Protocols here ("for future implementation").
# Neither had a single real caller anywhere in this codebase, and
# `QueryCache` duplicated the name of the actually-used
# `core.db.cache.QueryCache` (an ABC, not a Protocol, with a different
# module) -- a real implementation and a speculative placeholder
# sharing a name in different modules is exactly the kind of thing
# that makes "which cache abstraction do I use" a harder question than
# it needs to be. Removed rather than kept as unused scaffolding --
# see the framework review's cache-architecture section.
