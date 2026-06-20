"""FastAPI integration for Queue Max.

Provides background task processing via dependency injection
and middleware for automatic queue management.

Usage:
    from fastapi import FastAPI
    from queue_max.contrib.fastapi import QueueMiddleware, BackgroundQueue

    app = FastAPI()
    app.add_middleware(QueueMiddleware, max_workers=4)

    @app.post('/webhook')
    async def webhook(payload: dict, background: BackgroundQueue):
        background.enqueue('process_webhook', payload=payload)
        return {'status': 'accepted'}
"""

import logging
from typing import Any, Callable, Optional

from queue_max import Queue, Worker

logger = logging.getLogger("queue_max.fastapi")


class BackgroundQueue:
    """FastAPI dependency for background task enqueuing.

    Injected via FastAPI dependency resolution to allow
    endpoints to enqueue background tasks easily.
    """

    def __init__(self, queue: Queue):
        self._queue = queue

    def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any],
        pagina_id: Optional[int] = None,
        priority: int = 0,
    ) -> dict[str, Any]:
        """Enqueue a background task.

        Args:
            task_name: Name of the task to enqueue.
            payload: Task payload data.
            pagina_id: Optional ID for consistent sharding.
            priority: Task priority (0, 1, 2).

        Returns:
            dict with 'id' and 'shard_id'.
        """
        full_payload = {"task": task_name, **payload}
        return self._queue.enqueue(
            payload=full_payload,
            pagina_id=pagina_id,
            priority=priority,
        )


class QueueMiddleware:
    """FastAPI middleware for Queue Max lifecycle management.

    Automatically starts background workers when the app starts
    and gracefully shuts them down on app shutdown.
    """

    def __init__(
        self,
        app: Any,
        queue: Optional[Queue] = None,
        max_workers: int = 2,
        process_function: Optional[Callable] = None,
    ):
        self.app = app
        self.queue = queue or Queue()
        self.max_workers = max_workers
        self.process_function = process_function

        @app.on_event("startup")
        async def startup():
            if self.process_function:
                self._start_workers()

        @app.on_event("shutdown")
        async def shutdown():
            self._stop_workers()

    def _start_workers(self) -> None:
        """Start background worker pool."""
        if not self.process_function:
            return
        from queue_max import WorkerPool

        workers = [
            Worker(
                worker_id=f"fastapi-worker-{i + 1}",
                process_function=self.process_function,
                queue=self.queue,
            )
            for i in range(self.max_workers)
        ]
        self._pool = WorkerPool(workers)
        self._pool.start_all()
        logger.info("Started %d FastAPI worker(s)", self.max_workers)

    def _stop_workers(self) -> None:
        """Stop background workers gracefully."""
        if hasattr(self, "_pool"):
            self._pool.stop_all()
            logger.info("FastAPI workers stopped")

    async def __call__(self, scope, receive, send) -> None:
        """ASGI callable."""
        await self.app(scope, receive, send)
