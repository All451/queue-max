"""ShardManager facade — delegates to specialized components.

Keeps backward compatibility while the heavy lifting is done by
ConnectionManager and ShardRepository.
"""

from typing import Any, Optional

from queue_max.core.db.connection import ConnectionManager
from queue_max.core.db.constants import DATA_DIR, NUM_SHARDS
from queue_max.core.db.repository import ShardMetrics, ShardRepository
from queue_max.models.job import Job


class ShardManager:
    """Facade for sharded SQLite queue operations.

    Delegates to ConnectionManager (connection lifecycle) and
    ShardRepository (data access, metrics, maintenance).
    """

    def __init__(self, num_shards: int = NUM_SHARDS, data_dir: str = DATA_DIR):
        self.connections = ConnectionManager(num_shards, data_dir)
        self.repository = ShardRepository(self.connections)
        # Expose for backward compat
        self.num_shards = num_shards
        self.data_dir = data_dir

    # ---- Connection lifecycle ----
    def get_connection(self, shard_id: int):
        return self.connections.get_connection(shard_id)

    def close_all(self) -> None:
        self.connections.close_all()

    # ---- Job lifecycle ----
    def insert_job(self, shard_id: int, payload: dict, pagina_id=None, priority=0, max_retries=None) -> int:
        return self.repository.insert_job(shard_id, payload, pagina_id, priority, max_retries)

    def insert_jobs_batch(self, shard_id: int, jobs: list) -> int:
        return self.repository.insert_jobs_batch(shard_id, jobs)

    def pop_job(self, shard_id: int, worker_id: str):
        return self.repository.pop_job(shard_id, worker_id)

    def complete_job(self, shard_id: int, job_id: int) -> None:
        self.repository.complete_job(shard_id, job_id)

    def fail_job(self, shard_id: int, job_id: int, error: Exception, permanent: bool = False) -> None:
        self.repository.fail_job(shard_id, job_id, error, permanent)

    def retry_failed_jobs(self, shard_id: int) -> int:
        return self.repository.retry_failed_jobs(shard_id)

    def retry_job(self, shard_id: int, job_id: int) -> bool:
        return self.repository.retry_job(shard_id, job_id)

    # ---- Queries ----
    def get_failed_jobs(self, shard_id: int, limit: int = 100) -> list[Job]:
        return self.repository.get_failed_jobs(shard_id, limit)

    def get_processing_jobs(self, shard_id: int) -> list[Job]:
        return self.repository.get_processing_jobs(shard_id)

    def get_dead_letter_queue(self, shard_id: int, limit: int = 100) -> list[dict]:
        return self.repository.get_dead_letter_queue(shard_id, limit)

    def purge_jobs(self, shard_id: int, status: Optional[str] = None) -> int:
        return self.repository.purge_jobs(shard_id, status)

    # ---- Maintenance ----
    def heartbeat(self, shard_id: int, worker_id: str) -> None:
        self.repository.heartbeat(shard_id, worker_id)

    def recover_orphans(self, shard_id: int, stuck_timeout: int = 30000) -> int:
        return self.repository.recover_orphans(shard_id, stuck_timeout)

    def cleanup_old_jobs(self, shard_id: int, days: int = 7) -> int:
        return self.repository.cleanup_old_jobs(shard_id, days)

    # ---- Metrics ----
    def get_metrics(self, shard_id: int) -> ShardMetrics:
        return self.repository.get_metrics(shard_id)

    def get_stats(self, shard_id: int) -> dict[str, int]:
        return self.repository.get_stats(shard_id)

    def get_all_stats(self) -> dict[str, int]:
        return self.repository.get_all_stats()
