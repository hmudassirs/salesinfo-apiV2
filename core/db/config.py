"""Database configuration module following SOLID principles."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

from core.db.settings import DatabaseSettings


class DatabaseType(str, Enum):
    """Supported database types.

    PostgreSQL only -- see `DatabaseConfig.from_postgresql`'s docstring
    and `core.storage.service_db`'s module docstring for why this isn't
    a choice between backends: one PostgreSQL database serves both the
    data warehouse and operational/service data.
    """

    POSTGRESQL = "postgresql"


@dataclass
class DatabaseConfig:
    """Database configuration with performance settings.

    Attributes:
        db_type: Type of database
        connection_string: Connection string
        pool_size: Connection pool size
        max_overflow: Maximum overflow connections
        echo: Enable SQL echo
        timeout: Connection timeout in seconds
        extra_options: Additional database options
        settings: Performance and caching settings
    """

    db_type: DatabaseType
    connection_string: str
    pool_size: int = 5
    max_overflow: int = 10
    echo: bool = False
    timeout: int = 30
    extra_options: dict[str, Any] = field(default_factory=dict)
    settings: DatabaseSettings = field(default_factory=DatabaseSettings)

    @classmethod
    def from_postgresql(
        cls,
        dsn: Optional[str] = None,
        *,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "",
        sslmode: Optional[str] = None,
        pool_size: int | None = None,
        max_overflow: int = 10,
        echo: bool = False,
        settings: DatabaseSettings | None = None,
        **kwargs: Any,
    ) -> "DatabaseConfig":
        """Create a PostgreSQL configuration.

        `PostgreSQLAdapter`'s constructor takes discrete host/port/
        database/user/password keywords rather than a single file path,
        so those go into `extra_options` -- that's what
        `DatabaseSession._create_adapter()` unpacks as `**kwargs` when
        building a `PostgreSQLAdapter`.

        Args:
            dsn: Full connection URL, e.g.
                `postgresql://user:pass@host:5432/dbname?sslmode=require`.
                Takes precedence over the discrete host/port/... args
                below when provided (the standard 12-factor
                `DATABASE_URL` shape). `sslmode` and any other query
                parameters on the URL are merged into the connect
                kwargs the same as if passed via **kwargs.
            host, port, database, user, password: discrete connection
                params, used when `dsn` is not given.
            sslmode: libpq sslmode (e.g. "require", "verify-full").
                Production deployments should set this -- psycopg2/libpq
                defaults to "prefer" (opportunistic, not enforced) when
                omitted entirely.
            pool_size, max_overflow, echo, settings: standard pool/
                logging/caching configuration.
            **kwargs: any other psycopg2.connect() keyword (e.g.
                `connect_timeout`, `application_name`), merged into
                `extra_options` alongside the connection params.

        Returns:
            DatabaseConfig instance
        """
        connect_kwargs: dict[str, Any] = dict(kwargs)

        if dsn:
            parsed = urlsplit(dsn)
            host = parsed.hostname or host
            port = parsed.port or port
            database = (parsed.path or "").lstrip("/") or database
            user = parsed.username or user
            password = parsed.password or password
            for key, values in parse_qs(parsed.query).items():
                if values:
                    connect_kwargs.setdefault(key, values[0])
            sslmode = connect_kwargs.pop("sslmode", sslmode)

        if sslmode:
            connect_kwargs["sslmode"] = sslmode

        connect_kwargs.update(
            {
                "host": host,
                "port": port,
                "database": database,
                "user": user,
                "password": password,
            }
        )

        db_settings = settings or DatabaseSettings()
        effective_pool_size = (
            (db_settings.pool.max_size if db_settings.pool else None) or pool_size or 5
        )

        return cls(
            db_type=DatabaseType.POSTGRESQL,
            # Display/logging only -- never includes the password.
            # The adapter itself connects using extra_options below,
            # not this string.
            connection_string=f"postgresql://{user}@{host}:{port}/{database}",
            pool_size=effective_pool_size,
            max_overflow=max_overflow,
            echo=echo,
            settings=db_settings,
            extra_options=connect_kwargs,
        )
