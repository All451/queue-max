"""Flask integration for Robusta Queue.

Provides an extension pattern for easy integration with Flask apps.

Usage:
    from flask import Flask
    from queue_max.contrib.flask import QueueExtension

    app = Flask(__name__)
    queue = QueueExtension(app)

    @queue.task
    def send_notification(user_id):
        # send notification
        pass

    @app.route('/notify/<int:user_id>')
    def notify(user_id):
        send_notification.delay(user_id=user_id)
        return 'OK'
"""

import logging
from typing import Any, Callable, Dict, Optional

from queue_max import Queue as BaseQueue
from queue_max import task as base_task

logger = logging.getLogger("queue_max.flask")


class QueueExtension:
    """Flask extension for Robusta Queue.

    Provides queue access via app.extensions['queue'] and a @task decorator.

    Attributes:
        queue: The underlying Queue instance.
        app: The Flask application instance.
    """

    def __init__(
        self,
        app: Any = None,
        queue: Optional[BaseQueue] = None,
    ):
        """Initialize the extension.

        Args:
            app: Flask application instance (optional, can be init_app later).
            queue: Queue instance (creates default if None).
        """
        self.queue = queue or BaseQueue()
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Any) -> None:
        """Initialize the extension with a Flask app.

        Args:
            app: Flask application instance.
        """
        app.extensions = getattr(app, "extensions", {})
        app.extensions["queue_max"] = self

    def task(
        self,
        func: Optional[Callable] = None,
        priority: int = 0,
        max_retries: Optional[int] = None,
    ) -> Callable:
        """Decorator that registers a function as a queue task.

        Can be used with or without arguments:

        @queue.task
        def my_task():
            pass

        @queue.task(priority=2)
        def my_task():
            pass

        Args:
            func: The function to decorate (when used without arguments).
            priority: Task priority (0, 1, 2).
            max_retries: Maximum retry attempts.

        Returns:
            Decorated function with .delay() method.
        """
        if func is not None:
            return base_task(queue=self.queue, priority=priority, max_retries=max_retries)(func)
        return base_task(queue=self.queue, priority=priority, max_retries=max_retries)

    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return self.queue.get_stats()
