"""Data access for the users table."""

import time
from typing import Any, Dict, List, Optional

from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore
from core.storage.exceptions import DatabaseUnavailableError, DuplicateRecordError

try:
    import psycopg2.errors as psycopg2_errors  # type: ignore
except ImportError:
    psycopg2_errors = None  # type: ignore

logger = get_logger(__name__)


class UserRepository:
    """Data-access layer over the users table -- owns its own SQL
    against ApplicationStateStore's execute/fetch_one/fetch_all
    primitives, rather than delegating to convenience methods that
    used to live on ApplicationStateStore itself.

    Failures raise `core.storage.exceptions.RepositoryError` subclasses
    rather than being swallowed to False/None/[] -- see that module's
    docstring for why. A "no such row" result is still a plain
    None/[]/False return; only a genuine execute/fetch failure raises.
    """

    def __init__(self, application_state: ApplicationStateStore):
        """Initialize user service.

        Args:
            application_state: Application state store instance
        """
        self.application_state = application_state

    def create(
        self,
        user_id: str,
        username: str,
        email: str,
        password_hash: str,
        role: str = "user",
        created_at: Optional[int] = None,
    ) -> bool:
        """Create a new user.

        Returns:
            True if created successfully

        Raises:
            DuplicateRecordError: if `username` (or `user_id`) already exists.
            DatabaseUnavailableError: on any other execute failure.
        """
        if created_at is None:
            created_at = int(time.time())

        sql = """
        INSERT INTO users (user_id, username, email, password_hash, roles, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, true, ?, ?)
        """
        try:
            self.application_state.execute(
                sql,
                (user_id, username, email, password_hash, role, created_at, created_at),
            )
            return True
        except Exception as e:
            if psycopg2_errors is not None and isinstance(e, psycopg2_errors.UniqueViolation):
                raise DuplicateRecordError(
                    f"User '{username}' already exists"
                ) from e
            logger.error(f"Failed to create user: {e}")
            raise DatabaseUnavailableError("Failed to create user") from e

    def get_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID.

        Args:
            user_id: User ID

        Returns:
            User data if found, None if no such user.

        Raises:
            DatabaseUnavailableError: on a query failure.
        """
        sql = "SELECT * FROM users WHERE user_id = ?"
        try:
            result = self.application_state.fetch_one(sql, (user_id,))
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get user by ID: {e}")
            raise DatabaseUnavailableError("Failed to get user by ID") from e

    def get_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user by username.

        Args:
            username: Username

        Returns:
            User data if found, None if no such user.

        Raises:
            DatabaseUnavailableError: on a query failure.
        """
        sql = "SELECT * FROM users WHERE username = ?"
        try:
            result = self.application_state.fetch_one(sql, (username,))
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get user by username: {e}")
            raise DatabaseUnavailableError("Failed to get user by username") from e

    def update_role(self, user_id: str, role: str) -> bool:
        """Update user role.

        Args:
            user_id: User ID
            role: New role

        Returns:
            True if the statement executed successfully.

        Raises:
            DatabaseUnavailableError: on an execute failure.
        """
        sql = "UPDATE users SET roles = ?, updated_at = ? WHERE user_id = ?"
        try:
            self.application_state.execute(sql, (role, int(time.time()), user_id))
            return True
        except Exception as e:
            logger.error(f"Failed to update user role: {e}")
            raise DatabaseUnavailableError("Failed to update user role") from e

    def delete(self, user_id: str) -> bool:
        """Delete a user.

        Args:
            user_id: User ID

        Returns:
            True if the statement executed successfully.

        Raises:
            DatabaseUnavailableError: on an execute failure.
        """
        sql = "DELETE FROM users WHERE user_id = ?"
        try:
            self.application_state.execute(sql, (user_id,))
            return True
        except Exception as e:
            logger.error(f"Failed to delete user: {e}")
            raise DatabaseUnavailableError("Failed to delete user") from e

    def update_last_login(self, user_id: str) -> bool:
        """Update user's last login timestamp.

        Args:
            user_id: User ID

        Returns:
            True if the statement executed successfully.

        Raises:
            DatabaseUnavailableError: on an execute failure.
        """
        sql = "UPDATE users SET last_login_at = ? WHERE user_id = ?"
        try:
            self.application_state.execute(sql, (int(time.time()), user_id))
            return True
        except Exception as e:
            logger.error(f"Failed to update last login: {e}")
            raise DatabaseUnavailableError("Failed to update last login") from e

    def list_all(self) -> List[Dict[str, Any]]:
        """List all users.

        Returns:
            List of all users (empty list if there are none).

        Raises:
            DatabaseUnavailableError: on a query failure.
        """
        sql = "SELECT * FROM users ORDER BY created_at DESC"
        try:
            results = self.application_state.fetch_all(sql)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Failed to list users: {e}")
            raise DatabaseUnavailableError("Failed to list users") from e
