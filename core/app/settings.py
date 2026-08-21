# core/app/settings.py
"""Application settings and configuration."""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DatabaseRuntimeSettings:
    """Raw PostgreSQL connection parameters for the one shared database
    (application data + application state store -- see
    core.storage.application_state_store's module docstring for why
    this isn't a per-subsystem choice).

    Kept as its own dataclass, distinct from `PoolRuntimeSettings`
    below: this is about *where* to connect, not *how many*
    connections to hold open.
    """

    # Full connection URL, e.g.
    # `postgresql://user:pass@host:5432/dbname?sslmode=require`. Takes
    # precedence over the discrete host/port/... fields below when
    # set -- mirrors `DatabaseConfig.from_postgresql`'s own dsn
    # precedence.
    dsn: Optional[str] = None
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    # libpq sslmode (e.g. "require", "verify-full"). Production
    # deployments should set this explicitly -- psycopg2/libpq default
    # to "prefer" (opportunistic, not enforced) when omitted.
    sslmode: Optional[str] = None


@dataclass
class PoolRuntimeSettings:
    """Connection pool sizing for the two PostgreSQL pools this process
    opens (application data, application state store).

    Defaults are derived from the host's actual CPU count
    (`core.concurrency.cpu.recommended_sizing()`) rather than a
    hardcoded constant that either starves a bigger box or
    oversubscribes a smaller one -- see that module's docstring.
    `AppSettings.from_env()` only overrides a field when its matching
    env var is actually set.
    """

    application_data_min_size: int
    application_data_max_size: int
    application_state_min_size: int
    application_state_max_size: int
    timeout: int = 30


@dataclass
class ExecutorRuntimeSettings:
    """Thread pool sizing for the dedicated, per-workload executors in
    `core.concurrency.executors` -- see that module's docstring for why
    application data / application state / background / password work
    each get their own bounded pool instead of sharing one.

    Sized off the matching connection pool's max size (plus headroom)
    by default, the same core-count-derived defaults `PoolRuntimeSettings`
    above uses; `AppSettings.from_env()` only overrides a field when its
    matching env var is set.
    """

    application_data_workers: int
    application_state_workers: int
    background_workers: int
    password_workers: int


@dataclass
class AppSettings:
    """Application configuration settings."""

    log_level: str = "INFO"

    # --- Auth / JWT ---
    # No hardcoded default: fail loudly rather than silently signing
    # tokens with a well-known secret.
    jwt_secret_key: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_seconds: int = 3600

    # --- CORS ---
    # Comma-separated list of allowed origins. "*" is only acceptable
    # when allow_credentials is False; validated in from_env().
    cors_allow_origins: tuple = ("*",)
    cors_allow_credentials: bool = False

    # --- Query execution policy (roadmap Phase 13.1) ---
    # This deployment is an intentional "authenticated DB console", not
    # a read-only reporting API: callers are expected to run DDL/DML
    # against their own application data store, and there's no per-row/per-tenant
    # data to leak between callers (single shared application data store, no RLS
    # concept anywhere in this codebase -- see QueryCacheCoordinator's
    # docstring for the corresponding cache-isolation decision). Given
    # that, the policy chosen here is: SELECT/WITH require only the
    # "read" scope (or any authenticated caller, since read is always
    # granted); write statements (INSERT/UPDATE/DELETE/DDL/etc.)
    # require the "write" scope on the caller's API key, or an "admin"
    # role for JWT-authenticated sessions (which have no scope concept
    # of their own). See core/db/sql_policy.py.
    require_write_scope_for_mutations: bool = True

    # --- Query result limits (roadmap Phase 13.4) ---
    # Protects one caller's unbounded SELECT from exhausting memory or
    # the response pipeline. Rows beyond this are dropped and the
    # response is flagged `truncated=True` rather than silently
    # returning a partial result with no indication anything was cut.
    max_result_rows: int = 10_000
    # Same idea, but on serialized response size -- catches wide rows
    # (many/large columns) that a row-count cap alone wouldn't.
    max_result_bytes: int = 10 * 1024 * 1024  # 10 MiB
    # A query running longer than this is cancelled and reported as an
    # error rather than left to run indefinitely and hold a pool
    # connection open.
    max_query_duration_seconds: float = 30.0

    # --- Concurrency controls for expensive queries (roadmap Phase 14) ---
    # Separate semaphores per query class so a handful of expensive
    # aggregations/wide scans can't starve simple/fast queries of pool
    # capacity. See core/db/sql_policy.py for classification and
    # core/services/query_service.py for where these are applied.
    # Intentionally >= pool max_size: these gate *how many callers can
    # be mid-execution*, not how many DB connections exist -- the pool
    # itself is still the hard ceiling on actual DB concurrency.
    #
    # None (the default) means "derive from the host's actual CPU count
    # via core.concurrency.cpu.recommended_sizing()" instead of a
    # constant picked for one machine -- resolved in from_env(). Pass an
    # explicit int (or set the matching env var) to pin a fixed value
    # instead, e.g. for a deployment that's benchmarked its own optimum.
    fast_query_concurrency_limit: Optional[int] = None
    normal_query_concurrency_limit: Optional[int] = None
    expensive_query_concurrency_limit: Optional[int] = None

    # --- Cache invalidation ---
    # When True, a write statement invalidates only the cache entries
    # for tables it plausibly touched (core.db.sql_policy.extract_tables),
    # instead of clearing the entire query cache. Falls back to a full
    # clear automatically whenever no table can be resolved from the
    # statement. Flagged so a full clear-on-every-write can be restored
    # (the previous, maximally-conservative behavior) if the heuristic
    # ever misses a table reference in production.
    cache_invalidation_precise: bool = True

    # --- Auth rate limiting ---
    # Basic in-process, per-IP sliding-window limiter on
    # /api/auth/users/login and /api/auth/users/register (see
    # core.auth.rate_limiter) -- both are unauthenticated endpoints, so
    # they'd otherwise have no protection against credential stuffing
    # or registration spam. Flagged off for deployments that already
    # rate-limit at a reverse proxy/gateway layer. Per-process, like the
    # other in-memory caches in this codebase: not shared across
    # multiple workers/instances.
    auth_rate_limit_enabled: bool = True
    auth_rate_limit_max_attempts: int = 10
    auth_rate_limit_window_seconds: float = 60.0

    # --- Application state store maintenance (core.app.lifecycle
    # .application_state.ApplicationStateStep) ---
    # Periodic cleanup of unbounded generated data (expired cache
    # entries, old traces/audit rows) -- see that step's docstring for
    # why this needs to run at all.
    maintenance_enabled: bool = True
    maintenance_interval_seconds: float = 24 * 60 * 60  # 24 hours

    # --- First-admin bootstrap (core.auth.admin_bootstrap
    # .AdminBootstrapService) ---
    # No default password: a no-op unless INITIAL_ADMIN_PASSWORD is
    # explicitly set -- see that class's docstring for why (this
    # replaced a hardcoded admin/admin123! account created on every
    # fresh database). initial_admin_email left as None here rather
    # than defaulted, so AdminBootstrapService can apply its own
    # username-derived default only when this is genuinely unset.
    initial_admin_username: str = "admin"
    initial_admin_password: Optional[str] = None
    initial_admin_email: Optional[str] = None

    # --- Performance-subsystem application-level wiring
    # (core.app.lifecycle.performance.PerformanceStep) ---
    # Distinct from core.performance.config.PerformanceConfig's own
    # PERF_* settings (sample rate, collectors, etc.): those configure
    # the framework-independent performance package itself, while
    # these two configure *this application's* integration of it (how
    # often to bridge its registry onto OTel / publish cross-process
    # snapshots), so they live here rather than in that package.
    perf_otel_export_interval_seconds: float = 15.0
    perf_cross_process_publish_interval_seconds: float = 2.0

    # --- Runtime infrastructure (composition-root config) ---
    # Connection parameters and pool/executor sizing for the process's
    # database and thread pools. These used to be read directly out of
    # `os.environ` in `run_api.py` -- a second, parallel configuration
    # path alongside this class (see the P0 item in the refactor
    # review). `run_api.py` should now build `DatabaseConfig`,
    # `PoolSettings`, and `configure_executors(...)` entirely from these
    # three fields instead of calling `os.getenv()` itself.
    database: DatabaseRuntimeSettings = field(default_factory=DatabaseRuntimeSettings)
    pool: PoolRuntimeSettings = field(
        default_factory=lambda: AppSettings._default_pool_settings()
    )
    executors: ExecutorRuntimeSettings = field(
        default_factory=lambda: AppSettings._default_executor_settings()
    )

    @staticmethod
    def _default_pool_settings() -> "PoolRuntimeSettings":
        from core.concurrency.cpu import recommended_sizing

        sizing = recommended_sizing()
        return PoolRuntimeSettings(
            application_data_min_size=sizing.application_data_pool_min,
            application_data_max_size=sizing.application_data_pool_max,
            application_state_min_size=sizing.state_pool_min,
            application_state_max_size=sizing.state_pool_max,
        )

    @staticmethod
    def _default_executor_settings(
        pool: Optional["PoolRuntimeSettings"] = None,
    ) -> "ExecutorRuntimeSettings":
        from core.concurrency.cpu import recommended_sizing

        sizing = recommended_sizing()
        pool = pool or AppSettings._default_pool_settings()
        return ExecutorRuntimeSettings(
            application_data_workers=pool.application_data_max_size + 2,
            application_state_workers=pool.application_state_max_size + 2,
            background_workers=sizing.background_executor_workers,
            password_workers=sizing.password_executor_workers,
        )

    @classmethod
    def from_env(cls) -> "AppSettings":
        """Create settings from environment variables.

        Returns:
            AppSettings instance

        Raises:
            RuntimeError: If JWT_SECRET_KEY is not set, if CORS is
                configured with both a wildcard origin and credentials
                enabled (an insecure, browser-rejected combination), or
                if `validate()` finds any other invalid combination
                (bad pool/executor sizes, a query timeout that doesn't
                fit inside the pool acquire timeout, an unrecognized
                JWT algorithm, etc. -- see `validate()`'s docstring for
                the full set of checks).
        """
        import os

        from core.config_env import env_flag, env_float, env_int

        jwt_secret_key = os.getenv("JWT_SECRET_KEY")
        if not jwt_secret_key:
            raise RuntimeError(
                "JWT_SECRET_KEY environment variable must be set. "
                "Refusing to start with no signing secret or a hardcoded default."
            )
        if len(jwt_secret_key.encode()) < 32:
            logger.warning(
                "JWT_SECRET_KEY is only %d bytes; RFC 7518 recommends at least 32 "
                "for HS256. Generate a proper one, e.g.: "
                'python -c "import secrets; print(secrets.token_hex(32))"',
                len(jwt_secret_key.encode()),
            )

        cors_origins_raw = os.getenv("CORS_ALLOW_ORIGINS", "*")
        cors_allow_origins = tuple(
            o.strip() for o in cors_origins_raw.split(",") if o.strip()
        )
        cors_allow_credentials = (
            os.getenv("CORS_ALLOW_CREDENTIALS", "false").lower() == "true"
        )

        if cors_allow_credentials and "*" in cors_allow_origins:
            raise RuntimeError(
                "CORS_ALLOW_ORIGINS cannot be '*' when CORS_ALLOW_CREDENTIALS is true. "
                "List explicit origins instead."
            )

        # Resolve query-concurrency defaults from the host's actual CPU
        # count (core/concurrency/cpu.py) unless an env var pins an
        # explicit value -- see the field docstrings above.
        from core.concurrency.cpu import recommended_sizing

        sizing = recommended_sizing()
        fast_limit = os.getenv("FAST_QUERY_CONCURRENCY_LIMIT")
        normal_limit = os.getenv("NORMAL_QUERY_CONCURRENCY_LIMIT")
        expensive_limit = os.getenv("EXPENSIVE_QUERY_CONCURRENCY_LIMIT")

        database = DatabaseRuntimeSettings(
            dsn=os.getenv("DATABASE_URL"),
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            database=os.getenv("PGDATABASE", "postgres"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            sslmode=os.getenv("PGSSLMODE"),
        )

        pool_defaults = cls._default_pool_settings()
        pool = PoolRuntimeSettings(
            application_data_min_size=env_int(
                "APPLICATION_DATA_POOL_MIN_SIZE",
                pool_defaults.application_data_min_size,
            ),
            application_data_max_size=env_int(
                "APPLICATION_DATA_POOL_MAX_SIZE",
                pool_defaults.application_data_max_size,
            ),
            application_state_min_size=env_int(
                "APPLICATION_STATE_POOL_MIN_SIZE",
                pool_defaults.application_state_min_size,
            ),
            application_state_max_size=env_int(
                "APPLICATION_STATE_POOL_MAX_SIZE",
                pool_defaults.application_state_max_size,
            ),
        )

        executor_defaults = cls._default_executor_settings(pool)
        executors = ExecutorRuntimeSettings(
            application_data_workers=env_int(
                "APPLICATION_DATA_EXECUTOR_WORKERS",
                executor_defaults.application_data_workers,
            ),
            application_state_workers=env_int(
                "APPLICATION_STATE_EXECUTOR_WORKERS",
                executor_defaults.application_state_workers,
            ),
            background_workers=env_int(
                "BACKGROUND_EXECUTOR_WORKERS", executor_defaults.background_workers
            ),
            password_workers=env_int(
                "PASSWORD_EXECUTOR_WORKERS", executor_defaults.password_workers
            ),
        )

        settings = cls(
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            jwt_secret_key=jwt_secret_key,
            jwt_algorithm=os.getenv("JWT_ALGORITHM", "HS256"),
            jwt_expiry_seconds=int(os.getenv("JWT_EXPIRY_SECONDS", "3600")),
            cors_allow_origins=cors_allow_origins,
            cors_allow_credentials=cors_allow_credentials,
            require_write_scope_for_mutations=(
                os.getenv("REQUIRE_WRITE_SCOPE_FOR_MUTATIONS", "true").lower()
                == "true"
            ),
            max_result_rows=int(os.getenv("MAX_RESULT_ROWS", "10000")),
            max_result_bytes=int(os.getenv("MAX_RESULT_BYTES", str(10 * 1024 * 1024))),
            max_query_duration_seconds=float(
                os.getenv("MAX_QUERY_DURATION_SECONDS", "30.0")
            ),
            fast_query_concurrency_limit=int(
                fast_limit if fast_limit is not None else sizing.fast_query_concurrency
            ),
            normal_query_concurrency_limit=int(
                normal_limit
                if normal_limit is not None
                else sizing.normal_query_concurrency
            ),
            expensive_query_concurrency_limit=int(
                expensive_limit
                if expensive_limit is not None
                else sizing.expensive_query_concurrency
            ),
            cache_invalidation_precise=(
                os.getenv("CACHE_INVALIDATION_PRECISE", "true").lower() == "true"
            ),
            auth_rate_limit_enabled=(
                os.getenv("AUTH_RATE_LIMIT_ENABLED", "true").lower() == "true"
            ),
            auth_rate_limit_max_attempts=int(
                os.getenv("AUTH_RATE_LIMIT_MAX_ATTEMPTS", "10")
            ),
            auth_rate_limit_window_seconds=float(
                os.getenv("AUTH_RATE_LIMIT_WINDOW_SECONDS", "60.0")
            ),
            maintenance_enabled=env_flag("MAINTENANCE_ENABLED", default=True),
            maintenance_interval_seconds=env_float(
                "MAINTENANCE_INTERVAL_SECONDS", 24 * 60 * 60
            ),
            initial_admin_username=os.getenv("INITIAL_ADMIN_USERNAME", "admin"),
            initial_admin_password=os.getenv("INITIAL_ADMIN_PASSWORD"),
            initial_admin_email=os.getenv("INITIAL_ADMIN_EMAIL"),
            perf_otel_export_interval_seconds=env_float(
                "PERF_OTEL_EXPORT_INTERVAL_SECONDS", 15.0
            ),
            perf_cross_process_publish_interval_seconds=env_float(
                "PERF_CROSS_PROCESS_PUBLISH_INTERVAL_SECONDS", 2.0
            ),
            database=database,
            pool=pool,
            executors=executors,
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        """Fail fast on configuration that would otherwise surface as a
        confusing runtime error minutes or hours after startup (framework
        review item "configuration: needs production validation" /
        P0-5). Called automatically at the end of `from_env()`, so every
        real deployment gets these checks; tests that build
        `AppSettings(...)` directly can opt in by calling this
        themselves.

        Deliberately conservative: this only rejects combinations that
        are never correct (negative/zero sizes, a pool floor above its
        own ceiling, a query timeout that can't fit inside its own pool
        wait budget, etc.), not combinations that are merely unusual.
        """
        errors: list[str] = []

        def _positive(value: float, label: str) -> None:
            if value <= 0:
                errors.append(f"{label} must be > 0 (got {value})")

        def _non_negative(value: float, label: str) -> None:
            if value < 0:
                errors.append(f"{label} must be >= 0 (got {value})")

        def _min_max(min_value: int, max_value: int, label: str) -> None:
            _positive(min_value, f"{label}_min_size")
            _positive(max_value, f"{label}_max_size")
            if min_value > max_value:
                errors.append(
                    f"{label}_min_size ({min_value}) must be <= "
                    f"{label}_max_size ({max_value})"
                )

        # --- Pool sizes ---
        _min_max(
            self.pool.application_data_min_size,
            self.pool.application_data_max_size,
            "application_data_pool",
        )
        _min_max(
            self.pool.application_state_min_size,
            self.pool.application_state_max_size,
            "application_state_pool",
        )
        _positive(self.pool.timeout, "pool.timeout")

        # --- Executor sizes ---
        _positive(self.executors.application_data_workers, "executors.application_data_workers")
        _positive(self.executors.application_state_workers, "executors.application_state_workers")
        _positive(self.executors.background_workers, "executors.background_workers")
        _positive(self.executors.password_workers, "executors.password_workers")
        # An executor with fewer workers than its matching pool's
        # max_size can't ever drive every connection concurrently --
        # not fatal, but worth failing on since it's always a
        # misconfiguration rather than an intentional choice (there's
        # no scenario where fewer threads than connections helps).
        if self.executors.application_data_workers < self.pool.application_data_max_size:
            errors.append(
                "executors.application_data_workers "
                f"({self.executors.application_data_workers}) is less than "
                f"pool.application_data_max_size ({self.pool.application_data_max_size}); "
                "some pooled connections could never be used concurrently"
            )
        if self.executors.application_state_workers < self.pool.application_state_max_size:
            errors.append(
                "executors.application_state_workers "
                f"({self.executors.application_state_workers}) is less than "
                f"pool.application_state_max_size ({self.pool.application_state_max_size}); "
                "some pooled connections could never be used concurrently"
            )

        # --- Query limits ---
        _positive(self.max_result_rows, "max_result_rows")
        _positive(self.max_result_bytes, "max_result_bytes")
        _positive(self.max_query_duration_seconds, "max_query_duration_seconds")
        # The query timeout is enforced *after* a connection is already
        # acquired from the pool (see core.services.query_service); a
        # pool acquire timeout shorter than the query timeout means a
        # waiting caller can be timed out of the pool queue well before
        # the query it's waiting behind would itself be cancelled,
        # which just relabels ordinary load as a pool-timeout error.
        if self.max_query_duration_seconds > self.pool.timeout:
            errors.append(
                f"max_query_duration_seconds ({self.max_query_duration_seconds}) "
                f"exceeds pool.timeout ({self.pool.timeout}); a caller can be "
                "timed out waiting for a connection before a long-running query "
                "on another connection would even be cancelled"
            )

        # --- Query concurrency limits ---
        for label, value in (
            ("fast_query_concurrency_limit", self.fast_query_concurrency_limit),
            ("normal_query_concurrency_limit", self.normal_query_concurrency_limit),
            ("expensive_query_concurrency_limit", self.expensive_query_concurrency_limit),
        ):
            if value is not None:
                _positive(value, label)

        # --- JWT ---
        if not self.jwt_secret_key:
            errors.append("jwt_secret_key must be set")
        _positive(self.jwt_expiry_seconds, "jwt_expiry_seconds")
        if self.jwt_algorithm not in {"HS256", "HS384", "HS512", "RS256", "RS384", "RS512"}:
            errors.append(f"jwt_algorithm {self.jwt_algorithm!r} is not a recognized JWT algorithm")

        # --- CORS ---
        if not self.cors_allow_origins:
            errors.append("cors_allow_origins must not be empty")
        if self.cors_allow_credentials and "*" in self.cors_allow_origins:
            errors.append(
                "cors_allow_origins cannot include '*' when "
                "cors_allow_credentials is true"
            )

        # --- Database credentials / TLS ---
        # `postgres`/`postgres` is psycopg2's and this module's own
        # fallback default -- fine for local dev, a real production
        # footgun if it's still what's configured against a real
        # database (framework review item "SQL authorization... not a
        # true security boundary" -- least-privilege roles start with
        # not running as the superuser).
        if self.database.user == "postgres" and not self.database.dsn:
            logger.warning(
                "PGUSER is 'postgres' (the default superuser). Production "
                "deployments should connect as a dedicated least-privilege "
                "role instead -- see the framework review's least-privilege "
                "PostgreSQL item."
            )
        if not self.database.sslmode and not self.database.dsn:
            logger.warning(
                "PGSSLMODE is not set; libpq defaults to 'prefer' (opportunistic, "
                "not enforced). Production deployments should set it explicitly, "
                "e.g. 'require' or 'verify-full'."
            )

        # --- Auth rate limiting ---
        if self.auth_rate_limit_enabled:
            _positive(self.auth_rate_limit_max_attempts, "auth_rate_limit_max_attempts")
            _positive(self.auth_rate_limit_window_seconds, "auth_rate_limit_window_seconds")

        # --- Maintenance ---
        if self.maintenance_enabled:
            _positive(self.maintenance_interval_seconds, "maintenance_interval_seconds")

        # --- Performance/OTel intervals ---
        _positive(self.perf_otel_export_interval_seconds, "perf_otel_export_interval_seconds")
        _positive(
            self.perf_cross_process_publish_interval_seconds,
            "perf_cross_process_publish_interval_seconds",
        )

        if errors:
            raise RuntimeError(
                "Invalid configuration:\n" + "\n".join(f"  - {e}" for e in errors)
            )

    def configure_logging(self) -> None:
        """Configure logging based on settings."""
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logger.info("Logging configured with level: %s", self.log_level)
