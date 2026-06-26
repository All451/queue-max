"""SQL constants and configuration defaults for the database layer."""

import os

from queue_max.utils.helpers import get_env_int

# ------------------------------------------------------------------
# Environment-based configuration
# ------------------------------------------------------------------

NUM_SHARDS = get_env_int("NUM_SHARDS", 6)
DB_BUSY_TIMEOUT = get_env_int("DB_BUSY_TIMEOUT", 30000)
DATA_DIR = os.environ.get("DATA_DIR", "./data")
CACHE_SIZE = get_env_int("CACHE_SIZE", 10000)
MMAP_SIZE = get_env_int("MMAP_SIZE", 268435456)

# Schema version — bump when structural changes are made.
# Migrations are applied in _run_migrations() in repository.py.
SCHEMA_VERSION = 2

# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------

VALID_STATUSES = "'pending', 'processing', 'failed', 'completed', 'cancelled', 'scheduled'"

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS fila (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pagina_id INTEGER NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ({VALID_STATUSES})),
    priority INTEGER NOT NULL DEFAULT 0,
    tentativas INTEGER NOT NULL DEFAULT 0,
    max_tentativas INTEGER NOT NULL DEFAULT 3,
    retry_delay INTEGER NOT NULL DEFAULT 60,
    last_error TEXT NULL,
    error_type TEXT NULL,
    error_stack TEXT NULL,
    worker_id TEXT NULL,
    heartbeat TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT NULL,
    completed_at TEXT NULL,
    next_retry_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS shard_metadata (
    shard_id INTEGER PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_vacuum TEXT NULL,
    total_jobs_processed INTEGER NOT NULL DEFAULT 0,
    total_jobs_failed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_job_id INTEGER NULL,
    payload TEXT NOT NULL,
    error TEXT NOT NULL,
    error_type TEXT NOT NULL,
    failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    shard_id INTEGER NULL
);
"""

# ------------------------------------------------------------------
# Indexes
# ------------------------------------------------------------------

INDEXES_SQL = [
    # Covering index for POP_JOB: filters status + next_retry, orders by priority/id
    "CREATE INDEX IF NOT EXISTS idx_pop_job "
    "ON fila(status, next_retry_at, priority DESC, id);",
    # Index for orphan recovery (status='processing' AND heartbeat IS NULL OR heartbeat<?)
    "CREATE INDEX IF NOT EXISTS idx_heartbeat "
    "ON fila(heartbeat) WHERE status = 'processing';",
    # Index for periodic cleanup
    "CREATE INDEX IF NOT EXISTS idx_created_at ON fila(created_at);",
    # Index for status-based scans (get_failed_jobs, get_processing_jobs, etc.)
    "CREATE INDEX IF NOT EXISTS idx_status_created ON fila(status, created_at);",
    # DLQ ordering
    "CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_queue(failed_at);",
]

# Drop legacy index that is now superseded by idx_pop_job
DROP_LEGACY_INDEXES_SQL = [
    "DROP INDEX IF EXISTS idx_status_priority;",
    "DROP INDEX IF EXISTS idx_next_retry;",
]

# ------------------------------------------------------------------
# PRAGMAs
# ------------------------------------------------------------------

PRAGMAS_SQL = [
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    f"PRAGMA cache_size = {CACHE_SIZE};",
    f"PRAGMA mmap_size = {MMAP_SIZE};",
    "PRAGMA temp_store = MEMORY;",
    f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT};",
    "PRAGMA foreign_keys = ON;",
]

# ------------------------------------------------------------------
# Schema migrations
# ------------------------------------------------------------------

MIGRATIONS: dict[int, list[str]] = {
    # v2: add CHECK constraint on fila.status
    # SQLite supports ALTER TABLE ADD CONSTRAINT for CHECK since 3.25.2 (2018).
    # For older versions the constraint is silently ignored.
    2: [
        (
            "ALTER TABLE fila ADD CONSTRAINT "
            f"ck_fila_status CHECK(status IN ({VALID_STATUSES}));"
        ),
    ],
}


def get_migrations(from_version: int, to_version: int) -> list[str]:
    """Collect all migration statements from ``from_version`` up to ``to_version``."""
    statements: list[str] = []
    for v in range(from_version + 1, to_version + 1):
        statements.extend(MIGRATIONS.get(v, []))
    return statements


# ------------------------------------------------------------------
# Query constants
# ------------------------------------------------------------------

POP_JOB_SELECT_SQL = (
    "SELECT * FROM fila WHERE status='pending' "
    "AND (next_retry_at IS NULL OR next_retry_at<=?) "
    "ORDER BY priority DESC, id ASC LIMIT 1;"
)
CLAIM_JOB_SQL = (
    "UPDATE fila SET status='processing', worker_id=?, heartbeat=?, "
    "started_at=?, tentativas=tentativas+1 WHERE id=? AND status='pending';"
)
COMPLETE_JOB_SQL = "DELETE FROM fila WHERE id=?;"
FAIL_JOB_SQL = (
    "UPDATE fila SET status='failed', last_error=?, error_type=?, "
    "error_stack=?, worker_id=NULL, heartbeat=NULL, completed_at=? WHERE id=?;"
)
RETRY_SCHEDULE_SQL = (
    "UPDATE fila SET status='pending', next_retry_at=?, last_error=?, "
    "error_type=?, error_stack=? WHERE id=?;"
)
RETRY_FAILED_SQL = (
    "UPDATE fila SET status='pending', next_retry_at=?, worker_id=NULL, "
    "heartbeat=NULL, last_error=NULL, error_type=NULL, error_stack=NULL "
    "WHERE status='failed';"
)
HEARTBEAT_SQL = (
    "UPDATE fila SET heartbeat=? WHERE worker_id=? AND status='processing';"
)
RECOVER_ORPHAN_SQL = (
    "UPDATE fila SET status='pending', worker_id=NULL, heartbeat=NULL, "
    "next_retry_at=?, last_error='Recovered orphan', error_type='OrphanRecovery' "
    "WHERE status='processing' AND (heartbeat IS NULL OR heartbeat<?);"
)
MOVE_TO_DLQ_SQL = (
    "INSERT INTO dead_letter_queue "
    "(original_job_id, payload, error, error_type, shard_id) "
    "VALUES (?, ?, ?, ?, ?);"
)
UPDATE_META_PROCESSED_SQL = (
    "UPDATE shard_metadata SET total_jobs_processed=total_jobs_processed+1 "
    "WHERE shard_id=?;"
)
UPDATE_META_FAILED_SQL = (
    "UPDATE shard_metadata SET total_jobs_failed=total_jobs_failed+1 "
    "WHERE shard_id=?;"
)
INSERT_META_SQL = (
    "INSERT OR IGNORE INTO shard_metadata (shard_id, version) VALUES (?, ?);"
)
GET_METRICS_SQL = (
    "SELECT "
    "COUNT(CASE WHEN status='pending' THEN 1 END) as pending, "
    "COUNT(CASE WHEN status='processing' THEN 1 END) as processing, "
    "COUNT(CASE WHEN status='failed' THEN 1 END) as failed, "
    "AVG(CASE WHEN status='processing' AND started_at IS NOT NULL "
    "THEN (julianday('now')-julianday(started_at))*86400.0 END) "
    "as avg_processing_time "
    "FROM fila;"
)
GET_SCHEMA_VERSION_SQL = (
    "SELECT version FROM shard_metadata WHERE shard_id=0;"
)
UPDATE_SCHEMA_VERSION_SQL = (
    "UPDATE shard_metadata SET version=? WHERE shard_id=0;"
)
