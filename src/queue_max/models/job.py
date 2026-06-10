"""Job data model for the Robusta Queue library.

Represents a task in the queue with full lifecycle tracking,
including retry logic, error handling, and metadata.
"""

import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

class JobStatus(Enum):
    """Possible job statuses."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"

class JobPriority(Enum):
    """Job priority levels."""
    LOW = 0
    MEDIUM = 1
    HIGH = 2

    @classmethod
    def from_int(cls, value: int) -> "JobPriority":
        """Convert integer to JobPriority."""
        for priority in cls:
            if priority.value == value:
                return priority
        return cls.MEDIUM

@dataclass
class JobResult:
    """Result of a job execution."""
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    execution_time: float = 0.0
    worker_id: Optional[str] = None
    completed_at: Optional[str] = None

def _now_iso() -> str:
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

@dataclass
class Job:
    """Represents a job in the queue.

    Attributes:
        id: Unique identifier for the job.
        pagina_id: Optional ID used for consistent sharding.
        payload: JSON-serializable dictionary with job data.
        status: Current status.
        priority: Job priority (0=low, 1=medium, 2=high).
        tentativas: Number of attempts made so far.
        max_tentativas: Maximum number of retry attempts.
        retry_delay: Base delay in seconds for exponential backoff.
        last_error: Last error message.
        error_type: Type/class of the last error.
        error_stack: Stack trace of the last error.
        worker_id: ID of the worker processing this job.
        heartbeat: Last activity timestamp (ISO UTC).
        created_at: Creation timestamp (ISO UTC).
        started_at: When processing started (ISO UTC).
        completed_at: When job completed/failed (ISO UTC).
        next_retry_at: Scheduled next retry timestamp (ISO UTC).
        shard_id: Shard this job belongs to.
        tags: List of tags for categorization.
        metadata: Additional metadata dictionary.
        parent_job_id: Optional parent job ID for dependency chains.
        timeout_seconds: Maximum execution time in seconds.
        progress: Progress percentage (0-100) for long-running jobs.
    """

    id: int
    payload: Dict[str, Any]
    pagina_id: Optional[int] = None
    status: Union[JobStatus, str] = JobStatus.PENDING
    priority: Union[JobPriority, int] = JobPriority.MEDIUM
    tentativas: int = 0
    max_tentativas: int = 3
    retry_delay: int = 60
    last_error: Optional[str] = None
    error_type: Optional[str] = None
    error_stack: Optional[str] = None
    worker_id: Optional[str] = None
    heartbeat: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    next_retry_at: Optional[str] = None
    shard_id: int = 0
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_job_id: Optional[int] = None
    timeout_seconds: Optional[int] = None
    progress: float = 0.0

    def __post_init__(self):
        """Initialize timestamps and normalize enums."""
        if isinstance(self.status, str):
            try:
                self.status = JobStatus(self.status)
            except ValueError:
                self.status = JobStatus.PENDING
        if isinstance(self.priority, int):
            self.priority = JobPriority.from_int(self.priority)
        if self.created_at is None:
            self.created_at = _now_iso()

    @property
    def status_str(self) -> str:
        return self.status.value if isinstance(self.status, JobStatus) else str(self.status)

    @property
    def priority_int(self) -> int:
        return self.priority.value if isinstance(self.priority, JobPriority) else int(self.priority)

    @property
    def is_pending(self) -> bool:
        return self.status == JobStatus.PENDING

    @property
    def is_processing(self) -> bool:
        return self.status == JobStatus.PROCESSING

    @property
    def is_completed(self) -> bool:
        return self.status == JobStatus.COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.status == JobStatus.FAILED

    @property
    def is_cancelled(self) -> bool:
        return self.status == JobStatus.CANCELLED

    @property
    def is_terminal(self) -> bool:
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

    @property
    def can_retry(self) -> bool:
        return self.tentativas < self.max_tentativas and self.status == JobStatus.FAILED

    @property
    def remaining_retries(self) -> int:
        return max(0, self.max_tentativas - self.tentativas)

    @property
    def age_seconds(self) -> Optional[float]:
        if self.created_at:
            try:
                created = datetime.fromisoformat(self.created_at.replace("Z", "+00:00"))
                return (datetime.now(timezone.utc) - created).total_seconds()
            except (ValueError, TypeError):
                return None
        return None

    @property
    def processing_time_seconds(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            try:
                start = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
                end = datetime.fromisoformat(self.completed_at.replace("Z", "+00:00"))
                return (end - start).total_seconds()
            except (ValueError, TypeError):
                return None
        return None

    def mark_processing(self, worker_id: str) -> None:
        """Mark job as being processed by a worker."""
        self.status = JobStatus.PROCESSING
        self.worker_id = worker_id
        self.started_at = _now_iso()
        self.heartbeat = self.started_at
        self.progress = 0.0

    def mark_completed(self, result: Any = None) -> None:
        """Mark job as completed with optional result."""
        self.status = JobStatus.COMPLETED
        self.completed_at = _now_iso()
        self.progress = 100.0
        self.last_error = None
        self.error_type = None
        self.error_stack = None
        if result is not None:
            self.metadata["result"] = result

    def mark_failed(self, error: Exception, permanent: bool = False) -> None:
        """Mark job as failed, scheduling retry if applicable."""
        self.status = JobStatus.FAILED
        self.completed_at = _now_iso()
        self.last_error = str(error)
        self.error_type = type(error).__name__
        self.error_stack = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        if not permanent and self.can_retry:
            self.status = JobStatus.PENDING
            self.tentativas += 1
            self._schedule_retry()

    def mark_cancelled(self) -> None:
        """Mark job as cancelled."""
        self.status = JobStatus.CANCELLED
        self.completed_at = _now_iso()

    def update_progress(self, progress: float) -> None:
        """Update job progress (0-100)."""
        self.progress = max(0, min(100, progress))
        self.heartbeat = _now_iso()

    def _schedule_retry(self) -> None:
        """Schedule retry with exponential backoff and jitter."""
        import random
        delay = float(self.retry_delay) * (2 ** (self.tentativas - 1))
        jitter = delay * 0.2
        delay = delay + (random.random() * jitter * 2 - jitter)
        delay = min(delay, 3600)
        next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)
        self.next_retry_at = next_retry.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to a dictionary for serialization."""
        return {
            "id": self.id,
            "pagina_id": self.pagina_id,
            "payload": self.payload,
            "status": self.status_str,
            "priority": self.priority_int,
            "tentativas": self.tentativas,
            "max_tentativas": self.max_tentativas,
            "retry_delay": self.retry_delay,
            "last_error": self.last_error,
            "error_type": self.error_type,
            "error_stack": self.error_stack,
            "worker_id": self.worker_id,
            "heartbeat": self.heartbeat,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "next_retry_at": self.next_retry_at,
            "shard_id": self.shard_id,
            "tags": self.tags,
            "metadata": self.metadata,
            "parent_job_id": self.parent_job_id,
            "timeout_seconds": self.timeout_seconds,
            "progress": self.progress,
        }

    @classmethod
    def from_row(cls, row: dict, shard_id: int = 0) -> "Job":
        """Create a Job from a database row dict."""
        raw = row.get("payload")
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                payload = {"raw": raw}
        else:
            payload = raw or {}

        tags = row.get("tags", [])
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except (json.JSONDecodeError, TypeError):
                tags = []

        metadata = row.get("metadata", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}

        return cls(
            id=row["id"],
            pagina_id=row.get("pagina_id"),
            payload=payload,
            status=row.get("status", "pending"),
            priority=row.get("priority", 1),
            tentativas=row.get("tentativas", 0),
            max_tentativas=row.get("max_tentativas", 3),
            retry_delay=row.get("retry_delay", 60),
            last_error=row.get("last_error"),
            error_type=row.get("error_type"),
            error_stack=row.get("error_stack"),
            worker_id=row.get("worker_id"),
            heartbeat=row.get("heartbeat"),
            created_at=row.get("created_at"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            next_retry_at=row.get("next_retry_at"),
            shard_id=shard_id,
            tags=tags,
            metadata=metadata,
            parent_job_id=row.get("parent_job_id"),
            timeout_seconds=row.get("timeout_seconds"),
            progress=float(row.get("progress", 0)),
        )

    @classmethod
    def create(
        cls,
        payload: Dict[str, Any],
        pagina_id: Optional[int] = None,
        priority: Union[JobPriority, int] = JobPriority.MEDIUM,
        max_retries: int = 3,
        retry_delay: int = 60,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        parent_job_id: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> "Job":
        """Create a new job with default values (id=0 until assigned by DB)."""
        return cls(
            id=0,
            payload=payload,
            pagina_id=pagina_id,
            priority=priority,
            max_tentativas=max_retries,
            retry_delay=retry_delay,
            tags=tags or [],
            metadata=metadata or {},
            parent_job_id=parent_job_id,
            timeout_seconds=timeout_seconds,
        )

    def __repr__(self) -> str:
        return f"Job(id={self.id}, status={self.status_str}, priority={self.priority_int})"
