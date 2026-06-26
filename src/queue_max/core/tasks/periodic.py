"""@periodic_task decorator for Queue Max."""

import logging
from typing import Callable, Optional

from queue_max.core.tasks.base import task

logger = logging.getLogger("queue_max.task")


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

    Note:
        The scheduler runs in a daemon thread with no stop mechanism.
        It stops when the process exits.
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
