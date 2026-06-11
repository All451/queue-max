"""Database and shard management for Queue Max.

Each shard is an independent SQLite database file with WAL mode.
Uses thread-local connections for thread safety.
"""

import json, os, sqlite3, threading, time, traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from queue_max.models.job import Job, JobStatus
from queue_max.utils.helpers import backoff_delay, get_env_int, now_iso

NUM_SHARDS = get_env_int("NUM_SHARDS", 6)
DB_BUSY_TIMEOUT = get_env_int("DB_BUSY_TIMEOUT", 30000)
DATA_DIR = os.environ.get("DATA_DIR", "./data")
CACHE_SIZE = get_env_int("CACHE_SIZE", 10000)
MMAP_SIZE = get_env_int("MMAP_SIZE", 268435456)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fila (
    id INTEGER PRIMARY KEY AUTOINCREMENT, pagina_id INTEGER NULL,
    payload TEXT NOT NULL, status TEXT DEFAULT 'pending',
    priority INTEGER DEFAULT 0, tentativas INTEGER DEFAULT 0,
    max_tentativas INTEGER DEFAULT 3, retry_delay INTEGER DEFAULT 60,
    last_error TEXT NULL, error_type TEXT NULL, error_stack TEXT NULL,
    worker_id TEXT NULL, heartbeat TEXT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    started_at TEXT NULL, completed_at TEXT NULL, next_retry_at TEXT NULL
);
CREATE TABLE IF NOT EXISTS shard_metadata (
    shard_id INTEGER PRIMARY KEY, version INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')), last_vacuum TEXT NULL,
    total_jobs_processed INTEGER DEFAULT 0, total_jobs_failed INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT, original_job_id INTEGER,
    payload TEXT NOT NULL, error TEXT NOT NULL, error_type TEXT NOT NULL,
    failed_at TEXT DEFAULT (datetime('now')), shard_id INTEGER
);
"""
INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_status_priority ON fila(status, priority DESC);",
    "CREATE INDEX IF NOT EXISTS idx_next_retry ON fila(next_retry_at) WHERE status = 'pending';",
    "CREATE INDEX IF NOT EXISTS idx_heartbeat ON fila(heartbeat) WHERE status = 'processing';",
    "CREATE INDEX IF NOT EXISTS idx_created_at ON fila(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_status_created ON fila(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_queue(failed_at);",
]
PRAGMAS_SQL = [
    "PRAGMA journal_mode = WAL;", "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = {};".format(CACHE_SIZE),
    "PRAGMA mmap_size = {};".format(MMAP_SIZE),
    "PRAGMA temp_store = MEMORY;",
    "PRAGMA busy_timeout = {};".format(DB_BUSY_TIMEOUT),
]
POP_JOB_SELECT_SQL = "SELECT * FROM fila WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at<=?) ORDER BY priority DESC, id ASC LIMIT 1;"
CLAIM_JOB_SQL = "UPDATE fila SET status='processing', worker_id=?, heartbeat=?, started_at=?, tentativas=tentativas+1 WHERE id=? AND status='pending';"
COMPLETE_JOB_SQL = "DELETE FROM fila WHERE id=?;"
FAIL_JOB_SQL = "UPDATE fila SET status='failed', last_error=?, error_type=?, error_stack=?, worker_id=NULL, heartbeat=NULL, completed_at=? WHERE id=?;"
RETRY_SCHEDULE_SQL = "UPDATE fila SET status='pending', next_retry_at=?, last_error=?, error_type=?, error_stack=? WHERE id=?;"
RETRY_FAILED_SQL = "UPDATE fila SET status='pending', next_retry_at=?, worker_id=NULL, heartbeat=NULL, last_error=NULL, error_type=NULL, error_stack=NULL WHERE status='failed';"
HEARTBEAT_SQL = "UPDATE fila SET heartbeat=? WHERE worker_id=? AND status='processing';"
RECOVER_ORPHAN_SQL = "UPDATE fila SET status='pending', worker_id=NULL, heartbeat=NULL, next_retry_at=?, last_error='Recovered orphan', error_type='OrphanRecovery' WHERE status='processing' AND (heartbeat IS NULL OR heartbeat<?);"
MOVE_TO_DLQ_SQL = "INSERT INTO dead_letter_queue (original_job_id, payload, error, error_type, shard_id) VALUES (?, ?, ?, ?, ?);"
UPDATE_META_PROCESSED_SQL = "UPDATE shard_metadata SET total_jobs_processed=total_jobs_processed+1 WHERE shard_id=?;"
UPDATE_META_FAILED_SQL = "UPDATE shard_metadata SET total_jobs_failed=total_jobs_failed+1 WHERE shard_id=?;"
INSERT_META_SQL = "INSERT OR IGNORE INTO shard_metadata (shard_id, version) VALUES (?, ?);"
GET_METRICS_SQL = "SELECT COUNT(CASE WHEN status='pending' THEN 1 END) as pending, COUNT(CASE WHEN status='processing' THEN 1 END) as processing, COUNT(CASE WHEN status='failed' THEN 1 END) as failed, AVG(CASE WHEN status='processing' AND started_at IS NOT NULL THEN (julianday('now')-julianday(started_at))*86400.0 END) as avg_processing_time FROM fila;"


@dataclass
class ShardMetrics:
    shard_id: int; pending: int = 0; processing: int = 0; failed: int = 0
    avg_processing_time: Optional[float] = None
    total_jobs_processed: int = 0; total_jobs_failed: int = 0
    is_healthy: bool = True; last_error: Optional[str] = None


class ShardManager:
    """Manages SQLite shards with thread-local connections."""

    def __init__(self, num_shards: int = NUM_SHARDS, data_dir: str = DATA_DIR):
        if num_shards < 1:
            raise ValueError(f"num_shards must be >= 1, got {num_shards}")
        self.num_shards = num_shards
        self.data_dir = data_dir
        self.vacuum_interval_hours = 24
        self._local = threading.local()
        self._last_vacuum_time: Dict[int, float] = {}
        os.makedirs(data_dir, exist_ok=True)
        self._init_all_shards()

    def _init_all_shards(self) -> None:
        for shard_id in range(self.num_shards):
            db_path = os.path.join(self.data_dir, f"shard_{shard_id}.db")
            is_new = not os.path.exists(db_path)
            conn = sqlite3.connect(db_path, timeout=DB_BUSY_TIMEOUT / 1000)
            try:
                for p in PRAGMAS_SQL: conn.execute(p)
                conn.executescript(SCHEMA_SQL)
                for i in INDEXES_SQL: conn.execute(i)
                if is_new: conn.execute(INSERT_META_SQL, (shard_id, 1))
                conn.commit()
            finally:
                conn.close()

    @contextmanager
    def get_connection(self, shard_id: int):
        conn = self._get_connection(shard_id)
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise

    def _get_connection(self, shard_id: int) -> sqlite3.Connection:
        if not hasattr(self._local, "connections"):
            self._local.connections = {}
        if shard_id not in self._local.connections:
            db_path = os.path.join(self.data_dir, f"shard_{shard_id}.db")
            conn = sqlite3.connect(db_path, timeout=DB_BUSY_TIMEOUT / 1000)
            conn.row_factory = sqlite3.Row
            for p in PRAGMAS_SQL: conn.execute(p)
            self._local.connections[shard_id] = conn
        return self._local.connections[shard_id]

    def insert_job(self, shard_id: int, payload: Dict[str, Any], pagina_id: Optional[int] = None, priority: int = 0, max_retries: Optional[int] = None) -> int:
        max_retries = max_retries or get_env_int("QUEUE_MAX_RETRIES", 3)
        conn = self._get_connection(shard_id)
        cur = conn.execute("INSERT INTO fila (pagina_id, payload, priority, max_tentativas) VALUES (?, ?, ?, ?)", (pagina_id, json.dumps(payload), priority, max_retries))
        conn.commit()
        return cur.lastrowid

    def pop_job(self, shard_id: int, worker_id: str) -> Optional[Job]:
        conn = self._get_connection(shard_id)
        now = now_iso()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(POP_JOB_SELECT_SQL, (now,)).fetchone()
            if row is None:
                conn.commit(); return None
            job_id = row["id"]
            cur = conn.execute(CLAIM_JOB_SQL, (worker_id, now, now, job_id))
            conn.commit()
            if cur.rowcount == 0: return None
            job = Job.from_row(dict(row), shard_id=shard_id)
            job.status = JobStatus.PROCESSING; job.worker_id = worker_id
            return job
        except sqlite3.OperationalError:
            conn.rollback(); return None

    def complete_job(self, shard_id: int, job_id: int) -> None:
        conn = self._get_connection(shard_id)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(COMPLETE_JOB_SQL, (job_id,))
        conn.execute(UPDATE_META_PROCESSED_SQL, (shard_id,))
        conn.commit()

    def fail_job(self, shard_id: int, job_id: int, error: Exception, permanent: bool = False) -> None:
        now = now_iso()
        et = type(error).__name__; em = str(error)
        es = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        with self.get_connection(shard_id) as conn:
            if permanent:
                row = conn.execute("SELECT payload FROM fila WHERE id=?", (job_id,)).fetchone()
                conn.execute(FAIL_JOB_SQL, (em, et, es, now, job_id))
                if row: conn.execute(MOVE_TO_DLQ_SQL, (job_id, row["payload"], em, et, shard_id))
                conn.execute(UPDATE_META_FAILED_SQL, (shard_id,))
            else:
                row = conn.execute("SELECT tentativas, max_tentativas FROM fila WHERE id=?", (job_id,)).fetchone()
                if row:
                    t = row["tentativas"] + 1
                    if t >= row["max_tentativas"]:
                        conn.commit()
                        return self.fail_job(shard_id, job_id, error, permanent=True)
                    d = backoff_delay(t)
                    nr = (datetime.now(timezone.utc) + timedelta(seconds=d)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                    conn.execute(RETRY_SCHEDULE_SQL, (nr, em, et, es, job_id))
        conn.commit()

    def retry_failed_jobs(self, shard_id: int) -> int:
        conn = self._get_connection(shard_id)
        cur = conn.execute(RETRY_FAILED_SQL, (now_iso(),))
        conn.commit(); return cur.rowcount

    def cleanup_old_jobs(self, shard_id: int, days: int = 7) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        conn = self._get_connection(shard_id)
        cur = conn.execute("DELETE FROM fila WHERE created_at<? AND status IN ('failed','completed')", (cutoff,))
        conn.execute("DELETE FROM dead_letter_queue WHERE failed_at<?", (cutoff,))
        conn.commit(); self._maybe_vacuum(shard_id); return cur.rowcount

    def _maybe_vacuum(self, shard_id: int) -> None:
        if self.vacuum_interval_hours <= 0: return
        now = time.time()
        if now - self._last_vacuum_time.get(shard_id, 0) >= self.vacuum_interval_hours * 3600:
            try:
                conn = self._get_connection(shard_id)
                conn.execute("VACUUM")
                conn.execute("UPDATE shard_metadata SET last_vacuum=? WHERE shard_id=?", (now_iso(), shard_id))
                conn.commit(); self._last_vacuum_time[shard_id] = now
            except Exception: pass

    def get_failed_jobs(self, shard_id: int, limit: int = 100) -> List[Job]:
        conn = self._get_connection(shard_id)
        return [Job.from_row(dict(r), shard_id=shard_id) for r in conn.execute("SELECT * FROM fila WHERE status='failed' ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]

    def get_dead_letter_queue(self, shard_id: int, limit: int = 100) -> List[Dict]:
        conn = self._get_connection(shard_id)
        return [dict(r) for r in conn.execute("SELECT * FROM dead_letter_queue ORDER BY failed_at DESC LIMIT ?", (limit,)).fetchall()]

    def get_processing_jobs(self, shard_id: int) -> List[Job]:
        conn = self._get_connection(shard_id)
        return [Job.from_row(dict(r), shard_id=shard_id) for r in conn.execute("SELECT * FROM fila WHERE status='processing' ORDER BY id ASC").fetchall()]

    def heartbeat(self, shard_id: int, worker_id: str) -> None:
        conn = self._get_connection(shard_id)
        conn.execute(HEARTBEAT_SQL, (now_iso(), worker_id)); conn.commit()

    def recover_orphans(self, shard_id: int, stuck_timeout: int = 30000) -> int:
        now = now_iso()
        ts = (datetime.now(timezone.utc) - timedelta(seconds=stuck_timeout / 1000)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        conn = self._get_connection(shard_id)
        cur = conn.execute(RECOVER_ORPHAN_SQL, (now, ts)); conn.commit(); return cur.rowcount

    def get_metrics(self, shard_id: int) -> ShardMetrics:
        try:
            conn = self._get_connection(shard_id)
            row = conn.execute(GET_METRICS_SQL).fetchone()
            meta = conn.execute("SELECT total_jobs_processed, total_jobs_failed FROM shard_metadata WHERE shard_id=?", (shard_id,)).fetchone()
            return ShardMetrics(shard_id, row["pending"] or 0, row["processing"] or 0, row["failed"] or 0, row["avg_processing_time"], meta["total_jobs_processed"] if meta else 0, meta["total_jobs_failed"] if meta else 0)
        except Exception as e:
            return ShardMetrics(shard_id=shard_id, is_healthy=False, last_error=str(e))

    def get_stats(self, shard_id: int) -> Dict[str, int]:
        m = self.get_metrics(shard_id); return {"pending": m.pending, "processing": m.processing, "failed": m.failed}

    def get_all_stats(self) -> Dict[str, int]:
        total: Dict[str, int] = {"pending": 0, "processing": 0, "failed": 0}
        for sid in range(self.num_shards):
            s = self.get_stats(sid)
            for k in total: total[k] += s[k]
        return total

    def close_all(self) -> None:
        if hasattr(self._local, "connections"):
            for c in self._local.connections.values():
                try: c.close()
                except Exception: pass
            self._local.connections.clear()
