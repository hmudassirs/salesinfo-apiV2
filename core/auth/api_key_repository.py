"""Data access for the api_keys table."""

from typing import Any, Dict, List, Optional

from core.db.logger import get_logger
from core.storage.application_state_store import ApplicationStateStore
from core.storage.exceptions import DatabaseUnavailableError, DuplicateRecordError

try:
    import psycopg2.errors as psycopg2_errors  # type: ignore
except ImportError:
    psycopg2_errors = None  # type: ignore

logger = get_logger(__name__)


class APIKeyRepository:
    """Data-access layer over the api_keys table -- owns its own SQL
    against ApplicationStateStore's execute/fetch_one/fetch_all
    primitives, rather than delegating to convenience methods that
    used to live on ApplicationStateStore itself.

    Renamed from the old `APIKeyService` — that name collided with
    core.auth.api_key_service.APIKeyService, a completely different class
    (business logic: generates/hashes keys, calls *this* repository for
    the actual reads/writes). Same problem this whole restructure exists
    to fix, just one level deeper.

    Failures raise `core.storage.exceptions.RepositoryError` subclasses
    rather than being swallowed to False/None/[] -- see that module's
    docstring for why. A "no such row" result is still a plain
    None/[]/False return; only a genuine execute/fetch failure raises.
    """

    def __init__(self, application_state: ApplicationStateStore):
        """Initialize API key service.

        Args:
            application_state: Application state store instance
        """
        self.application_state = application_state

    def create(
        self,
        key_id: str,
        api_key_hash: str,
        owner_id: str,
        created_at: int,
        expires_at: Optional[int] = None,
        scopes: Optional[str] = None,
        is_active: bool = True,
    ) -> bool:
        """Create a new API key.

        Returns:
            True if created successfully

        Raises:
            DuplicateRecordError: if `key_id`/`api_key_hash` already exists.
            DatabaseUnavailableError: on any other execute failure.
        """
        sql = """
        INSERT INTO api_keys (key_id, api_key_hash, owner_id, created_at, expires_at, scopes, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        try:
            self.application_state.execute(
                sql,
                (
                    key_id,
                    api_key_hash,
                    owner_id,
                    created_at,
                    expires_at,
                    scopes,
                    is_active,
                ),
            )
            return True
        except Exception as e:
            if psycopg2_errors is not None and isinstance(e, psycopg2_errors.UniqueViolation):
                raise DuplicateRecordError(f"API key '{key_id}' already exists") from e
            logger.error(f"Failed to create API key: {e}")
            raise DatabaseUnavailableError("Failed to create API key") from e

    def validate(
        self, api_key_hash: str, current_time: int
    ) -> Optional[Dict[str, Any]]:
        """Validate an API key hash.

        Args:
            api_key_hash: Hashed API key
            current_time: Current timestamp

        Returns:
            API key data if valid, None if not found/expired.

        Raises:
            DatabaseUnavailableError: on a query failure.
        """
        sql = """
        SELECT key_id, owner_id, created_at, expires_at, scopes, is_active
        FROM api_keys
        WHERE api_key_hash = ? AND is_active = true
        """
        try:
            result = self.application_state.fetch_one(sql, (api_key_hash,))
        except Exception as e:
            logger.error(f"Failed to validate API key: {e}")
            raise DatabaseUnavailableError("Failed to validate API key") from e

        if not result:
            return None

        key_data = dict(result)

        # Check if key is expired
        if key_data.get("expires_at") and key_data["expires_at"] < current_time:
            logger.warning(f"API key {key_data['key_id']} has expired")
            return None

        return key_data

    def list_by_owner(self, owner_id: str) -> List[Dict[str, Any]]:
        """List all API keys for an owner.

        Args:
            owner_id: Owner user ID

        Returns:
            List of API key data (empty list if there are none).

        Raises:
            DatabaseUnavailableError: on a query failure.
        """
        sql = """
        SELECT key_id, owner_id, created_at, expires_at, scopes, is_active
        FROM api_keys
        WHERE owner_id = ?
        ORDER BY created_at DESC
        """
        try:
            results = self.application_state.fetch_all(sql, (owner_id,))
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Failed to list API keys: {e}")
            raise DatabaseUnavailableError("Failed to list API keys") from e

    def revoke(self, key_id: str, owner_id: str) -> bool:
        """Revoke an API key.

        Args:
            key_id: API key ID
            owner_id: Owner user ID

        Returns:
            True if the statement executed successfully.

        Raises:
            DatabaseUnavailableError: on an execute failure.
        """
        sql = "UPDATE api_keys SET is_active = false WHERE key_id = ? AND owner_id = ?"
        try:
            self.application_state.execute(sql, (key_id, owner_id))
            return True
        except Exception as e:
            logger.error(f"Failed to revoke API key: {e}")
            raise DatabaseUnavailableError("Failed to revoke API key") from e

    def delete(self, key_id: str, owner_id: str) -> bool:
        """Delete an API key.

        Args:
            key_id: API key ID
            owner_id: Owner user ID

        Returns:
            True if the statement executed successfully.

        Raises:
            DatabaseUnavailableError: on an execute failure.
        """
        sql = "DELETE FROM api_keys WHERE key_id = ? AND owner_id = ?"
        try:
            self.application_state.execute(sql, (key_id, owner_id))
            return True
        except Exception as e:
            logger.error(f"Failed to delete API key: {e}")
            raise DatabaseUnavailableError("Failed to delete API key") from e
