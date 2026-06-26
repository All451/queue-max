"""Data access and business logic for sharded SQLite queues."""

import json
import logging
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from sqlite3 import OperationalError
from typing import Any, Optional

from queue_max.core.db.connection import ConnectionManager
from queue_max.core.db.constants import (
    CLAIM_JOB_SQL,
    COMPLETE_JOB_SQL,
    FAIL_JOB_SQL,
    GET_METRICS_SQL,
    HEARTBEAT_SQL,
    INSERT_META_SQL,
    MOVE_TO_DLQ_SQL,
    POP_JOB_SELECT_SQL,
    RECOVER_ORPHAN_SQL,
    RETRY_FAILED_SQL,
    RETRY_SCHEDULE_SQL,
    UPDATE_META_FAILED_SQL,
    UPDATE_META_PROCESSED_SQL,
)
from queue_max.models.job import Job, JobStatus
from queue_max.utils.helpers import backoff_delay, get_env_int, now_iso

logger = logging.getLogger("queue_max.database.repository")


@dataclass
class ShardMetrics:
    shard_id: int
    pending: int = 0
    processing: int = 0
    failed: int = 0
    avg_processing_time: Optional[float] = None
    total_jobs_processed: int = 0
    total_jobs_failed: int = 0
    is_healthy: bool = True
    last_error: Optional[str] = None


class ShardRepository:
    """Data access layer for sharded SQLite queues.

    Handles all CRUD operations for jobs, retry logic,
    metrics collection, and maintenance tasks.
    """

    def __init__(self, connection_manager: ConnectionManager):
        self.conn = connection_manager
        self.vacuum_interval_hours = 24
        self._last_vacuum_time: dict[int, float] = {}
        self._vacuum_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def insert_job(
        self,
        shard_id: int,
        payload: dict[str, Any],
        pagina_id: Optional[int] = None,
        priority: int = 0,
        max_retries: Optional[int] = None,
    ) -> int:
        max_retries = max_retries or get_env_int("QUEUE_MAX_RETRIES", 3)
        conn = self.conn.get_raw_connection(shard_id)
        cur = conn.execute(
            "INSERT INTO fila (pagina_id, payload, priority, max_tentativas) VALUES (?, ?, ?, ?)",
            (pagina_id, json.dumps(payload), priority, max_retries + 1),
        )
        conn.commit()
        return cur.lastrowid

    def insert_jobs_batch(self, shard_id: int, jobs: list[tuple]) -> int:
        conn = self.conn.get_raw_connection(shard_id)
        rows = [
            (json.dumps(p), pid, pri, (mr or get_env_int("QUEUE_MAX_RETRIES", 3)) + 1)
            for p, pid, pri, mr in jobs
        ]
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT INTO fila (payload, pagina_id, priority, max_tentativas) VALUES (?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        return len(rows)

    def pop_job(self, shard_id: int, worker_id: str) -> Optional[Job]:
        conn = self.conn.get_raw_connection(shard_id)
        now = now_iso()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(POP_JOB_SELECT_SQL, (now,)).fetchone()
            if row is None:
                conn.commit()
                return None
            job_id = row["id"]
            cur = conn.execute(CLAIM_JOB_SQL, (worker_id, now, now, job_id))
            conn.commit()
            if cur.rowcount == 0:
                return None
            job = Job.from_row(dict(row), shard_id=shard_id)
            job.status = JobStatus.PROCESSING
            job.worker_id = worker_id
            return job
        except OperationalError as e:
            logger.warning("pop_job shard %d: %s", shard_id, e)
            conn.rollback()
            return None

    def complete_job(self, shard_id: int, job_id: int) -> None:
        conn = self.conn.get_raw_connection(shard_id)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(COMPLETE_JOB_SQL, (job_id,))
        conn.execute(UPDATE_META_PROCESSED_SQL, (shard_id,))
        conn.commit()

    # ------------------------------------------------------------------
    # Fail / Retry
    # ------------------------------------------------------------------

    def fail_job(self, shard_id: int, job_id: int, error: Exception, permanent: bool = False) -> None:
        now = now_iso()
        em = str(error)
        et = type(error).__name__
        es = "".join(traceback.format_exception(type(error), error, error.__traceback__))

        with self.conn.get_connection(shard_id) as conn:
            if permanent:
                self._fail_permanent(conn, shard_id, job_id, em, et, es, now)
            else:
                self._fail_with_retry(conn, shard_id, job_id, em, et, es, now)
            conn.commit()

    def _fail_permanent(self, conn, shard_id: int, job_id: int, em: str, et: str, es: str, now: str) -> None:
        row = conn.execute("SELECT payload FROM fila WHERE id=?", (job_id,)).fetchone()
        conn.execute(FAIL_JOB_SQL, (em, et, es, now, job_id))
        if row:
            conn.execute(MOVE_TO_DLQ_SQL, (job_id, row["payload"], em, et, shard_id))
        conn.execute(UPDATE_META_FAILED_SQL, (shard_id,))

    def _fail_with_retry(self, conn, shard_id: int, job_id: int, em: str, et: str, es: str, now: str) -> None:
        row = conn.execute(
            "SELECT tentativas, max_tentativas FROM fila WHERE id=?", (job_id,)
        ).fetchone()
        if not row:
            return
        t = row["tentativas"] + 1
        if t > row["max_tentativas"]:
            self._fail_permanent(conn, shard_id, job_id, em, et, es, now)
        else:
            self._schedule_retry(conn, shard_id, job_id, t, em, et, es)

    def _schedule_retry(self, conn, shard_id: int, job_id: int, attempt: int, em: str, et: str, es: str) -> None:
        d = backoff_delay(attempt)
        nr = (datetime.now(timezone.utc) + timedelta(seconds=d)).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
        conn.execute(RETRY_SCHEDULE_SQL, (nr, em, et, es, job_id))

    def retry_failed_jobs(self, shard_id: int) -> int:
        conn = self.conn.get_raw_connection(shard_id)
        cur = conn.execute(RETRY_FAILED_SQL, (now_iso(),))
        conn.commit()
        return cur.rowcount

    def retry_job(self, shard_id: int, job_id: int) -> bool:
        conn = self.conn.get_raw_connection(shard_id)
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT id, status FROM fila WHERE id=? AND status='failed'",
            (job_id,),
        ).fetchone()
        if row is None:
            conn.commit()
            return False
        cur = conn.execute(
            "UPDATE fila SET status='pending', next_retry_at=?, "
            "worker_id=NULL, heartbeat=NULL, last_error=NULL, "
            "error_type=NULL, error_stack=NULL WHERE id=?",
            (now_iso(), job_id),
        )
        conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_failed_jobs(self, shard_id: int, limit: int = 100) -> list[Job]:
        conn = self.conn.get_raw_connection(shard_id)
        return [
            Job.from_row(dict(r), shard_id=shard_id)
            for r in conn.execute(
                "SELECT * FROM fila WHERE status='failed' ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        ]

    def get_processing_jobs(self, shard_id: int) -> list[Job]:
        conn = self.conn.get_raw_connection(shard_id)
        return [
            Job.from_row(dict(r), shard_id=shard_id)
            for r in conn.execute(
                "SELECT * FROM fila WHERE status='processing' ORDER BY id ASC"
            ).fetchall()
        ]

    def get_dead_letter_queue(self, shard_id: int, limit: int = 100) -> list[dict]:
        conn = self.conn.get_raw_connection(shard_id)
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM dead_letter_queue ORDER BY failed_at DESC LIMIT ?", (limit,)
            ).fetchall()
        ]

    def purge_jobs(self, shard_id: int, status: Optional[str] = None) -> int:
        conn = self.conn.get_raw_connection(shard_id)
        if status:
            cur = conn.execute("DELETE FROM fila WHERE status=?", (status,))
        else:
            cur = conn.execute("DELETE FROM fila")
        conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def get_metrics(self, shard_id: int) -> ShardMetrics:
        try:
            conn = self.conn.get_raw_connection(shard_id)
            row = conn.execute(GET_METRICS_SQL).fetchone()
            meta = conn.execute(
                "SELECT total_jobs_processed, total_jobs_failed FROM shard_metadata WHERE shard_id=?",
                (shard_id,),
            ).fetchone()
            return ShardMetrics(
                shard_id=shard_id,
                pending=row["pending"] or 0,
                processing=row["processing"] or 0,
                failed=row["failed"] or 0,
                avg_processing_time=row["avg_processing_time"],
                total_jobs_processed=meta["total_jobs_processed"] if meta else 0,
                total_jobs_failed=meta["total_jobs_failed"] if meta else 0,
            )
        except Exception as e:
            return ShardMetrics(shard_id=shard_id, is_healthy=False, last_error=str(e))

    def get_stats(self, shard_id: int) -> dict[str, int]:
        m = self.get_metrics(shard_id)
        return {"pending": m.pending, "processing": m.processing, "failed": m.failed}

    def get_all_stats(self) -> dict[str, int]:
        total: dict[str, int] = {"pending": 0, "processing": 0, "failed": 0}
        for sid in range(self.conn.num_shards):
            s = self.get_stats(sid)
            for k in total:
                total[k] += s[k]
        return total

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def heartbeat(self, shard_id: int, worker_id: str) -> None:
        conn = self.conn.get_raw_connection(shard_id)
        conn.execute(HEARTBEAT_SQL, (now_iso(), worker_id))
        conn.commit()

    def recover_orphans(self, shard_id: int, stuck_timeout: int = 30000) -> int:
        now = now_iso()
        ts = (
            datetime.now(timezone.utc) - timedelta(seconds=stuck_timeout / 1000)
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        conn = self.conn.get_raw_connection(shard_id)
        cur = conn.execute(RECOVER_ORPHAN_SQL, (now, ts))
        conn.commit()
        return cur.rowcount

    def cleanup_old_jobs(self, shard_id: int, days: int = 7) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        conn = self.conn.get_raw_connection(shard_id)
        cur = conn.execute(
            "DELETE FROM fila WHERE created_at<? AND status IN ('failed','completed')", (cutoff,)
        )
        conn.execute("DELETE FROM dead_letter_queue WHERE failed_at<?", (cutoff,))
        conn.commit()
        self._maybe_vacuum(shard_id)
        return cur.rowcount

    def _maybe_vacuum(self, shard_id: int) -> None:
        if self.vacuum_interval_hours <= 0:
            return
        with self._vacuum_lock:
            now_time = time.time()
            if now_time - self._last_vacuum_time.get(shard_id, 0) < self.vacuum_interval_hours * 3600:
                return
            try:
                conn = self.conn.get_raw_connection(shard_id)
                conn.execute("VACUUM")
                conn.execute(
                    "UPDATE shard_metadata SET last_vacuum=? WHERE shard_id=?",
                    (now_iso(), shard_id),
                )
                conn.commit()
                self._last_vacuum_time[shard_id] = now_time
            except Exception as e:
                logger.error("VACUUM failed for shard %d: %s", shard_id, e)
