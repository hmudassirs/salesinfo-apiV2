-- Auth coordination tables used by core.auth.shared_state.PersistentAuthState
-- when DATABASE_BACKEND=postgresql. Prefixed `_app_` and documented here
-- as internal control-plane state, not application data, since
-- they live in the same database as the customer's tables.

CREATE TABLE IF NOT EXISTS _app_jwt_revocations (
    user_id TEXT PRIMARY KEY,
    revoked_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS _app_rate_limit_windows (
    rate_key TEXT PRIMARY KEY,
    window_start DOUBLE PRECISION NOT NULL,
    attempt_count INTEGER NOT NULL
);
