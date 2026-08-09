-- Service database tables (api_keys, users, logs, traces, query_cache,
-- audit_log), applied here -- alongside 0001's auth coordination
-- tables -- when the service database shares this same Postgres
-- database instead of living in a separate SQLite file (see
-- core.storage.service_db's module docstring). Same schema as
-- migrations/service_db/0001_initial_schema.sql, translated to
-- Postgres's dialect:
--   - `INTEGER PRIMARY KEY AUTOINCREMENT` -> `SERIAL PRIMARY KEY`
--   - `BOOLEAN DEFAULT 1/0` -> `BOOLEAN DEFAULT TRUE/FALSE` (Postgres
--     does not implicitly cast an integer literal to boolean)
-- Column names, types, indexes, and foreign keys are otherwise
-- unchanged, so application code (core/storage/service_db.py and the
-- repositories built on it) runs unmodified against either backend.

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,
    email VARCHAR NOT NULL UNIQUE,
    password_hash VARCHAR NOT NULL,
    roles VARCHAR DEFAULT 'viewer',
    is_active BOOLEAN DEFAULT TRUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_login_at INTEGER,
    login_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active);

CREATE TABLE IF NOT EXISTS api_keys (
    key_id VARCHAR PRIMARY KEY,
    api_key_hash VARCHAR NOT NULL,
    owner_id VARCHAR NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    scopes VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    last_used_at INTEGER,
    usage_count INTEGER DEFAULT 0,
    FOREIGN KEY (owner_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_api_keys_owner ON api_keys(owner_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(api_key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active);

CREATE TABLE IF NOT EXISTS logs (
    log_id SERIAL PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    level VARCHAR NOT NULL,
    logger VARCHAR NOT NULL,
    message TEXT NOT NULL,
    module VARCHAR,
    function VARCHAR,
    line INTEGER,
    exception TEXT,
    user_id VARCHAR,
    session_id VARCHAR,
    request_id VARCHAR,
    ip_address VARCHAR,
    user_agent TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_logger ON logs(logger);
CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id);
CREATE INDEX IF NOT EXISTS idx_logs_request ON logs(request_id);

CREATE TABLE IF NOT EXISTS traces (
    trace_id VARCHAR NOT NULL,
    span_id VARCHAR NOT NULL,
    parent_span_id VARCHAR,
    operation_name VARCHAR NOT NULL,
    start_time BIGINT NOT NULL,
    end_time BIGINT,
    duration_ms BIGINT,
    status VARCHAR,
    error_message TEXT,
    service_name VARCHAR NOT NULL,
    service_version VARCHAR,
    user_id VARCHAR,
    session_id VARCHAR,
    request_id VARCHAR,
    http_method VARCHAR,
    http_url TEXT,
    http_status_code INTEGER,
    db_query TEXT,
    db_duration_ms BIGINT,
    tags TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS idx_traces_start_time ON traces(start_time);
CREATE INDEX IF NOT EXISTS idx_traces_operation ON traces(operation_name);
CREATE INDEX IF NOT EXISTS idx_traces_service ON traces(service_name);
CREATE INDEX IF NOT EXISTS idx_traces_user ON traces(user_id);
CREATE INDEX IF NOT EXISTS idx_traces_request ON traces(request_id);
CREATE INDEX IF NOT EXISTS idx_traces_parent ON traces(parent_span_id);

CREATE TABLE IF NOT EXISTS query_cache (
    cache_key VARCHAR PRIMARY KEY,
    query_hash VARCHAR NOT NULL,
    query_sql TEXT NOT NULL,
    result_data TEXT NOT NULL,
    result_count INTEGER DEFAULT 0,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    last_accessed_at INTEGER,
    access_count INTEGER DEFAULT 0,
    user_id VARCHAR,
    session_id VARCHAR,
    execution_time_ms INTEGER,
    result_size_bytes INTEGER,
    is_compressed BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_cache_query_hash ON query_cache(query_hash);
CREATE INDEX IF NOT EXISTS idx_cache_expires ON query_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_accessed ON query_cache(last_accessed_at);
CREATE INDEX IF NOT EXISTS idx_cache_user ON query_cache(user_id);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id SERIAL PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    event_type VARCHAR NOT NULL,
    user_id VARCHAR,
    session_id VARCHAR,
    ip_address VARCHAR,
    user_agent TEXT,
    resource_type VARCHAR,
    resource_id VARCHAR,
    action VARCHAR NOT NULL,
    old_values TEXT,
    new_values TEXT,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    metadata TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_log(resource_type, resource_id);
