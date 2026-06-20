"""@task decorator for Queue Max.

Converts regular functions into enqueuable tasks with support for:
- Synchronous execution (direct call)
- Background execution (.delay())
- Scheduled execution (.schedule_at(), .schedule_in())
- Parallel map (.map())
- Bulk operations (.bulk_delay())
- Argument validation
- Task versioning
- Timeout support
"""

import functools
import inspect
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from queue_max.core.queue import Queue

logger = logging.getLogger("queue_max.task")

def _is_serializable(obj: Any) -> bool:
    """Check if an object is JSON-serializable."""
    try:
        json.dumps(obj)
        return True
    except (TypeError, OverflowError):
        return False

def task(
    queue: Optional[Queue] = None,
    priority: int = 0,
    max_retries: Optional[int] = None,
    rate_limit: Optional[int] = None,
    timeout: Optional[int] = None,
    retry_delay: int = 60,
    on_success: Optional[Callable] = None,
    on_failure: Optional[Callable] = None,
    version: int = 1,
) -> Callable:
    """Decorator that converts a function into an enqueuable task.

    The decorated function can be called directly (synchronous execution)
    or via .delay() to enqueue it for background processing.

    Args:
        queue: Queue instance (creates a new one if None).
        priority: Default priority (0=low, 1=medium, 2=high).
        max_retries: Max retry attempts (default: queue default).
        rate_limit: Optional rate limit override for the queue.
        timeout: Max execution time in seconds (Unix only).
        retry_delay: Base delay in seconds for retry backoff.
        on_success: Callback on successful completion.
        on_failure: Callback on task failure.
        version: Task version for schema migrations.

    Returns:
        Decorated function with .delay(), .schedule_at(), .map(), etc.

    Example:
        @task(priority=2, timeout=30)
        def send_email(to: str, subject: str):
            return send(to, subject)

        # Enqueue for background processing
        send_email.delay("user@example.com", "Hello!")

        # Schedule for later
        from datetime import datetime, timedelta
        future = datetime.now(timezone.utc) + timedelta(minutes=5)
        send_email.schedule_at(future, "user@example.com", "Hello!")

        # Parallel processing
        send_email.map(["a@b.com", "c@d.com"], "Welcome!")
    """

    def decorator(func: Callable) -> Callable:
        _queue = queue or Queue(rate_limit=rate_limit)
        task_name = f"{func.__module__}.{func.__name__}"
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            """Execute the task synchronously."""
            try:
                sig.bind(*args, **kwargs)

                if timeout is not None:
                    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(func, *args, **kwargs)
                        try:
                            result = future.result(timeout=timeout)
                        except FuturesTimeoutError:
                            raise TimeoutError(
                                f"Task {task_name} exceeded {timeout}s timeout"
                            )
                else:
                    result = func(*args, **kwargs)

                if on_success:
                    try:
                        on_success(result, task_name=task_name)
                    except Exception as e:
                        logger.error("on_success callback failed: %s", e)

                return result

            except Exception as e:
                if on_failure:
                    try:
                        on_failure(e, task_name=task_name)
                    except Exception as cb_err:
                        logger.error("on_failure callback failed: %s", cb_err)
                raise

        def delay(*args, **kwargs) -> dict[str, Any]:
            """Enqueue the function for background processing.

            Returns:
                dict with 'id' (job ID) and 'shard_id' (assigned shard).
            """
            try:
                sig.bind(*args, **kwargs)
            except TypeError as e:
                raise TypeError(f"Invalid arguments for {task_name}: {e}")

            for key, value in kwargs.items():
                if not _is_serializable(value):
                    logger.warning(
                        "Non-serializable argument '%s' for task %s", key, task_name
                    )

            payload = {
                "task": task_name,
                "version": version,
                "args": args,
                "kwargs": kwargs,
                "timeout": timeout,
                "retry_delay": retry_delay,
            }
            return _queue.enqueue(
                payload=payload,
                priority=priority,
                max_retries=max_retries,
            )

        def schedule_at(when: Union[datetime, str], *args, **kwargs) -> dict[str, Any]:
            """Schedule task to run at a specific time.

            Args:
                when: datetime object or ISO 8601 string.
                *args: Positional arguments for the task.
                **kwargs: Keyword arguments for the task.

            Returns:
                dict with job id and shard id.
            """
            if isinstance(when, str):
                when = datetime.fromisoformat(when)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            if when <= now:
                raise ValueError(f"Scheduled time {when} is in the past")

            delay_seconds = (when - now).total_seconds()
            return schedule_in(delay_seconds, *args, **kwargs)

        def schedule_in(seconds: float, *args, **kwargs) -> dict[str, Any]:
            """Schedule task to run after N seconds.

            Args:
                seconds: Delay in seconds.
                *args: Positional arguments for the task.
                **kwargs: Keyword arguments for the task.

            Returns:
                dict with job id and shard id.
            """
            scheduled = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
            payload = {
                "task": task_name,
                "version": version,
                "args": args,
                "kwargs": kwargs,
                "scheduled_at": scheduled,
                "timeout": timeout,
                "retry_delay": retry_delay,
            }
            result = _queue.enqueue(
                payload=payload,
                priority=priority,
                max_retries=max_retries,
            )
            logger.info("Task %s scheduled at %s (job %s)", task_name, scheduled, result["id"])
            return result

        def map(items: list[Any], *args, **kwargs) -> list[dict[str, Any]]:
            """Enqueue multiple items for parallel processing.

            Each item in the list becomes the first positional argument.

            Args:
                items: list of items to process.
                *args: Additional positional arguments.
                **kwargs: Keyword arguments.

            Returns:
                list of job result dicts.
            """
            return [delay(item, *args, **kwargs) for item in items]

        def bulk_delay(arg_list: list[Tuple[tuple, dict]]) -> list[dict[str, Any]]:
            """Enqueue multiple calls with different arguments.

            Args:
                arg_list: list of (args, kwargs) tuples.

            Returns:
                list of job result dicts.
            """
            return [delay(*a, **kw) for a, kw in arg_list]

        def get_task_stats() -> dict[str, Any]:
            """Get statistics for this task."""
            return {
                "task_name": task_name,
                "version": version,
                "priority": priority,
                "max_retries": max_retries,
                "timeout": timeout,
                "queue_stats": _queue.get_stats(),
            }

        # Attach methods
        wrapper.delay = delay
        wrapper.schedule_at = schedule_at
        wrapper.schedule_in = schedule_in
        wrapper.map = map
        wrapper.bulk_delay = bulk_delay
        wrapper.get_queue = lambda: _queue
        wrapper.get_stats = get_task_stats

        # Attach metadata
        wrapper.task_name = task_name
        wrapper.version = version
        wrapper.queue = _queue
        wrapper.priority = priority
        wrapper.max_retries = max_retries

        return wrapper

    return decorator

def periodic_task(interval: int, **task_kwargs) -> Callable:
    """Decorator for tasks that run periodically.

    Args:
        interval: Seconds between automatic executions.
        **task_kwargs: Arguments passed through to @task.

    Returns:
        Decorated function with .start_scheduler() method.

    Example:
        @periodic_task(interval=3600, priority=1)
        def cleanup():
            print("Cleaning up...")

        cleanup.start_scheduler()
    """
    def decorator(func: Callable) -> Callable:
        decorated = task(**task_kwargs)(func)

        def start_scheduler() -> None:
            import threading
            import time as _time

            def _run():
                while True:
                    try:
                        decorated.delay()
                    except Exception as e:
                        logger.error("Periodic task %s failed: %s", func.__name__, e)
                    _time.sleep(interval)

            thread = threading.Thread(target=_run, daemon=True)
            thread.start()

        decorated.start_scheduler = start_scheduler
        return decorated

    return decorator

def retryable_task(
    max_retries: int = 5,
    retry_on: Optional[list[Exception]] = None,
    **task_kwargs,
) -> Callable:
    """Decorator for tasks that should retry synchronously on specific exceptions.

    Args:
        max_retries: Max retry attempts.
        retry_on: list of exception types to retry on (default: all).
        **task_kwargs: Arguments passed through to @task.

    Returns:
        Decorated function with .delay() and synchronous retry logic.
    """
    def decorator(func: Callable) -> Callable:
        task_kwargs.setdefault("max_retries", max_retries)
        decorated = task(**task_kwargs)(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_on_exceptions = retry_on or (Exception,)
            attempt = 0
            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)
                except retry_on_exceptions as e:
                    attempt += 1
                    if attempt > max_retries:
                        raise
                    import time as _time
                    delay = task_kwargs.get("retry_delay", 60) * (2 ** (attempt - 1))
                    logger.warning(
                        "Retry %d/%d for %s in %.1fs: %s",
                        attempt, max_retries, func.__name__, delay, e,
                    )
                    _time.sleep(delay)

        wrapper.delay = decorated.delay
        wrapper.schedule_at = decorated.schedule_at
        wrapper.schedule_in = decorated.schedule_in
        wrapper.map = decorated.map
        wrapper.task_name = decorated.task_name
        return wrapper

    return decorator
