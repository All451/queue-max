"""Django management command to start a queue worker."""

import importlib

from django.core.management.base import BaseCommand, CommandError

from robusta_queue import Queue, Worker


class Command(BaseCommand):
    """Start a Robusta Queue worker."""

    help = "Start a Robusta Queue worker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--function",
            required=True,
            help="Function reference (MODULE:FUNCTION)",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=1,
            help="Number of worker threads (default: 1)",
        )

    def handle(self, *args, **options):
        module_path, func_name = options["function"].split(":", 1)
        try:
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)
        except (ImportError, AttributeError) as e:
            raise CommandError(f"Could not load function '{options['function']}': {e}")

        if not callable(func):
            raise CommandError(f"'{func_name}' is not callable")

        from robusta_queue.contrib.django import _get_django_queue

        queue = _get_django_queue()
        num_workers = options["workers"]

        workers = [
            Worker(worker_id=f"django-worker-{i + 1}", process_function=func, queue=queue)
            for i in range(num_workers)
        ]

        from robusta_queue import WorkerPool

        pool = WorkerPool(workers)
        pool.start_all()

        self.stdout.write(
            self.style.SUCCESS(
                f"Started {num_workers} worker(s) processing {options['function']}"
            )
        )

        try:
            import time

            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stdout.write("\nShutting down...")
        finally:
            pool.stop_all()
            self.stdout.write(self.style.SUCCESS("Workers stopped"))
