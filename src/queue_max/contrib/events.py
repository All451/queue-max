"""bubus-powered typed events for Queue Max.

Provides Pydantic-typed event classes for all Queue lifecycle events
and a bridge that connects Queue's simple event system to a bubus EventBus.

Requires: pip install queue-max[events]  (installs bubus + pydantic)

Usage:
    from queue_max.contrib.events import QueueEventBus, JobCompleted

    queue = Queue()
    events = QueueEventBus(queue)

    @events.on(JobCompleted)
    def handle(event: JobCompleted) -> None:
        print(f"Job {event.job_id} completed")

    # Wait for a specific event
    result = events.expect(JobEnqueued, timeout=30)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Callable, Optional, Type, TypeVar

from queue_max import Queue as _Queue

try:
    from bubus import BaseEvent, EventBus
except ImportError:
    raise ImportError(
        "queue-max[events] requires bubus. Install it with: "
        "pip install queue-max[events]"
    ) from None

logger = logging.getLogger("queue_max.events")

# ── Typed Event Definitions ──────────────────────────────────────────────

T = TypeVar("T")


class JobEnqueued(BaseEvent[None]):
    """Emitted when a job is enqueued successfully."""

    job_id: int
    shard_id: int


class JobCompleted(BaseEvent[None]):
    """Emitted when a job completes successfully."""

    job_id: int
    shard_id: int


class JobFailed(BaseEvent[None]):
    """Emitted when a job fails permanently (retries exhausted or permanent=True)."""

    job_id: int
    shard_id: int
    error: str


class JobRetried(BaseEvent[None]):
    """Emitted when a job fails transiently and is scheduled for retry."""

    job_id: int
    shard_id: int
    error: str


class Alert(BaseEvent[None]):
    """Emitted when pending jobs exceed the alert threshold."""

    alert_type: str
    pending: int
    threshold: int


# ── Event Bus Bridge ─────────────────────────────────────────────────────


_EVENT_MAP: dict[str, Type[BaseEvent[Any]]] = {
    "job_enqueued": JobEnqueued,
    "job_completed": JobCompleted,
    "job_failed": JobFailed,
    "job_retried": JobRetried,
    "alert": Alert,
}


class QueueEventBus:
    """Bridges Queue lifecycle events to a typed bubus EventBus.

    Runs a background asyncio event loop to dispatch events
    asynchronously without blocking the main thread.

    Supports context manager usage:

    >>> with QueueEventBus(queue) as events:
    ...     @events.on(JobCompleted)
    ...     def handler(event): ...

    Example:
        queue = Queue(shards=3)
        events = QueueEventBus(queue)

        @events.on(JobCompleted)
        def on_completed(event: JobCompleted) -> None:
            print(f"Job {event.job_id} done")

        # Wildcard handler
        @events.on("job_*")
        def on_any(event: BaseEvent) -> None:
            print(f"Event: {type(event).__name__}")

        # Wait for an event
        result = events.expect(JobEnqueued, timeout=10)
    """

    def __init__(
        self,
        queue: _Queue,
        *,
        bus_name: str = "queue_max",
        wal_path: Optional[str] = None,
        max_history_size: int = 50,
    ):
        self.queue = queue
        self.bus = EventBus(
            name=bus_name,
            wal_path=wal_path,
            max_history_size=max_history_size,
        )

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"bubus-{bus_name}", daemon=True
        )
        self._thread_started = threading.Event()
        self._thread.start()
        self._thread_started.wait(timeout=5)

        # Metrics
        self._metrics = {
            "total_dispatched": 0,
            "total_errors": 0,
            "by_type": {},
            "last_event_time": None,
        }
        self._metrics_lock = threading.Lock()

        self._attach()

    def __enter__(self) -> QueueEventBus:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _run_loop(self) -> None:
        """Background thread that runs the asyncio event loop."""
        asyncio.set_event_loop(self._loop)
        self._thread_started.set()
        self._loop.run_forever()

    def _dispatch_async(self, event: BaseEvent) -> None:
        """Record metrics and dispatch event on the background event loop."""
        with self._metrics_lock:
            self._metrics["total_dispatched"] += 1
            event_type = type(event).__name__
            self._metrics["by_type"][event_type] = (
                self._metrics["by_type"].get(event_type, 0) + 1
            )
            self._metrics["last_event_time"] = time.time()

        asyncio.run_coroutine_threadsafe(
            self._dispatch_coro(event), self._loop
        )

    async def _dispatch_coro(self, event: BaseEvent) -> None:
        """Coroutine that dispatches the event (runs on background loop)."""
        try:
            self.bus.dispatch(event)
        except Exception:
            logger.exception("Error dispatching event %s", type(event).__name__)
            with self._metrics_lock:
                self._metrics["total_errors"] += 1

    def _attach(self) -> None:
        """Subscribe to all Queue events and forward to bubus."""

        def bridge(event_name: str) -> Callable:
            event_cls = _EVENT_MAP[event_name]

            def handler(**data: Any) -> None:
                event = event_cls(**data)
                self._dispatch_async(event)

            return handler

        for event_name in _EVENT_MAP:
            self.queue.on(event_name, bridge(event_name))

    def on(
        self,
        event_type: Type[BaseEvent[T]] | str,
        handler: Callable[[Any], Any] | None = None,
    ) -> Callable | None:
        """Register a handler for a typed event or pattern.

        Can be used as a decorator or called directly:

        @events.on(JobCompleted)
        def handle(event: JobCompleted): ...

        events.on("job_*", lambda e: print(e))
        """
        if handler is not None:
            self.bus.on(event_type, handler)
            return None

        def decorator(fn: Callable) -> Callable:
            self.bus.on(event_type, fn)
            return fn

        return decorator

    def expect(
        self,
        event_type: Type[BaseEvent[T]],
        *,
        timeout: float | None = None,
        predicate: Callable[[BaseEvent], bool] | None = None,
    ) -> BaseEvent[T]:
        """Wait for a specific event type to be dispatched.

        Runs on the background event loop. Blocks the calling thread
        until the event arrives or the timeout expires.

        Args:
            event_type: The event class to wait for.
            timeout: Max seconds to wait (None = 30s).
            predicate: Optional filter function.

        Returns:
            The matching event instance.

        Raises:
            TimeoutError: If no matching event within the timeout.
        """
        kwargs: dict[str, Any] = {}
        if predicate is not None:
            kwargs["predicate"] = predicate

        future = asyncio.run_coroutine_threadsafe(
            self.bus.expect(event_type, timeout=timeout, **kwargs),
            self._loop,
        )
        try:
            return future.result(timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"No {event_type.__name__} received within "
                f"{timeout or 30}s"
            )

    def get_metrics(self) -> dict[str, Any]:
        """Return event dispatch metrics.

        Returns:
            dict with total_dispatched, total_errors, by_type (count per event
            class name), and last_event_time (unix timestamp or None).
        """
        with self._metrics_lock:
            return dict(self._metrics)

    def log_tree(self) -> str:
        """Return a tree representation of all dispatched events."""
        return self.bus.log_tree()

    def close(self) -> None:
        """Stop the background event loop and clean up."""
        if not self._loop or self._loop.is_closed():
            return

        async def _shutdown() -> None:
            tasks = [t for t in asyncio.all_tasks(self._loop) if t is not asyncio.current_task()]
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.wait(tasks, timeout=3)

        try:
            if self._loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    _shutdown(), self._loop
                )
                future.result(timeout=5)
                self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception as e:
            logger.warning("Error shutting down event loop: %s", e)

        if self._thread.is_alive():
            self._thread.join(timeout=5)

        if not self._loop.is_closed():
            self._loop.close()
