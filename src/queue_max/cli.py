"""Command-line interface for Robusta Queue.

Provides commands for managing the queue, running workers,
inspecting job status, and performing maintenance operations.
"""

import argparse
import importlib
import json
import logging
import os
import sys
from typing import Any, Dict, List

from queue_max import Queue, Worker, WorkerPool

logger = logging.getLogger("queue_max")


def _format_table(headers: List[str], rows: List[List[Any]]) -> str:
    """Format data as a simple ASCII table."""
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    separator = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    header_row = (
        "|"
        + "|".join(f" {h.center(col_widths[i])} " for i, h in enumerate(headers))
        + "|"
    )

    lines = [separator, header_row, separator]
    for row in rows:
        line = (
            "|"
            + "|".join(f" {str(cell).ljust(col_widths[i])} " for i, cell in enumerate(row))
            + "|"
        )
        lines.append(line)
    lines.append(separator)
    return "\n".join(lines)


def cmd_stats(args: argparse.Namespace) -> None:
    """Handle the 'stats' command."""
    queue = Queue(
        shards=args.shards,
        rate_limit=args.rate_limit,
        data_dir=args.data_dir,
    )

    if args.shard is not None:
        stats = queue.shard_manager.get_stats(args.shard)
    else:
        stats = queue.get_stats()

    if args.json:
        print(json.dumps(stats, indent=2))
        return

    print("\n  Robusta Queue Statistics")
    print("=" * 45)
    rows = [
        ["Pending", stats.get("pending", 0)],
        ["Processing", stats.get("processing", 0)],
        ["Failed", stats.get("failed", 0)],
        ["Shards", stats.get("num_shards", queue.num_shards)],
        ["Circuit State", stats.get("circuit_state", "closed")],
        ["Tokens Available", stats.get("tokens_available", 0)],
    ]
    print(_format_table(["Metric", "Value"], rows))


def cmd_worker(args: argparse.Namespace) -> None:
    """Handle the 'worker' command."""
    # Parse function reference (module:function)
    if ":" not in args.function:
        print("Error: --function must be in MODULE:FUNCTION format")
        sys.exit(2)

    module_path, func_name = args.function.split(":", 1)
    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        print(f"Error: Could not load function '{args.function}': {e}")
        sys.exit(1)

    if not callable(func):
        print(f"Error: '{func_name}' in '{module_path}' is not callable")
        sys.exit(1)

    queue = Queue(
        shards=args.shards,
        rate_limit=args.rate_limit,
        data_dir=args.data_dir,
    )

    num_workers = args.workers or 1
    workers = []
    for i in range(num_workers):
        worker_id = f"worker-{i + 1}"
        w = Worker(worker_id=worker_id, process_function=func, queue=queue)
        workers.append(w)

    pool = WorkerPool(workers)
    pool.start_all()

    print(f"\n  Started {num_workers} worker(s) processing {args.function}")
    print(f"  Queue: {queue.num_shards} shards, {queue.rate_limiter.rate_limit} req/min")
    print("  Press Ctrl+C to stop.\n")

    try:
        import time

        while True:
            time.sleep(60)
            stats = queue.get_stats()
            worker_stats = pool.get_stats()
            print(
                f"  [Stats] Pending: {stats['pending']} | "
                f"Processed: {worker_stats['total_processed']} | "
                f"Failed: {worker_stats['total_failed']} | "
                f"Circuit: {stats['circuit_state']}"
            )
    except KeyboardInterrupt:
        print("\n  Shutting down workers...")
    finally:
        pool.stop_all()
        print("  Workers stopped.")


def cmd_enqueue(args: argparse.Namespace) -> None:
    """Handle the 'enqueue' command."""
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON payload: {e}")
        sys.exit(2)

    if not isinstance(payload, dict):
        print("Error: Payload must be a JSON object (dict)")
        sys.exit(2)

    queue = Queue(
        shards=args.shards,
        data_dir=args.data_dir,
    )

    result = queue.enqueue(
        payload=payload,
        pagina_id=args.pagina_id,
        priority=args.priority,
    )

    if args.json:
        print(json.dumps(result))
    else:
        print(f"  Job enqueued: id={result['id']}, shard={result['shard_id']}")


def cmd_purge(args: argparse.Namespace) -> None:
    """Handle the 'purge' command."""
    queue = Queue(
        shards=args.shards,
        data_dir=args.data_dir,
    )
    removed = queue.cleanup_old_jobs(days=args.days)
    print(f"  Removed {removed} old job(s).")


def cmd_retry(args: argparse.Namespace) -> None:
    """Handle the 'retry' command."""
    queue = Queue(
        shards=args.shards,
        data_dir=args.data_dir,
    )

    if args.job_id is not None:
        # Retry a single job (requires shard)
        if args.shard is None:
            print("Error: --shard is required when using --job-id")
            sys.exit(2)
        # For a single job, we update it directly
        if args.shard is not None:
            retried = queue.shard_manager.retry_failed_jobs(args.shard)
            # Note: retry_failed_jobs retries all in shard since we don't have single-job retry
        print(f"  Retried jobs in shard {args.shard}.")
    else:
        retried = queue.retry_failed_jobs(shard_id=args.shard)
        if args.shard is not None:
            print(f"  Retried {retried} job(s) in shard {args.shard}.")
        else:
            print(f"  Retried {retried} job(s) across all shards.")


def cmd_list(args: argparse.Namespace) -> None:
    """Handle the 'list' command."""
    queue = Queue(
        shards=args.shards,
        data_dir=args.data_dir,
    )

    status = args.status
    jobs_list: List[Dict[str, Any]] = []

    if status in ("all", "failed"):
        for job in queue.get_failed_jobs(limit=args.limit):
            jd = job.to_dict()
            jd["payload"] = json.dumps(jd["payload"], ensure_ascii=False)[:80]
            jobs_list.append(jd)

    if status in ("all", "processing"):
        for job in queue.get_processing_jobs():
            jd = job.to_dict()
            jd["payload"] = json.dumps(jd["payload"], ensure_ascii=False)[:80]
            jobs_list.append(jd)

    if status == "all":
        # Also add pending from stats
        pass

    # Sort by id descending
    jobs_list.sort(key=lambda j: j.get("id", 0), reverse=True)
    jobs_list = jobs_list[: args.limit]

    if args.json:
        print(json.dumps(jobs_list, indent=2, ensure_ascii=False))
        return

    if not jobs_list:
        print(f"  No jobs with status '{status}'.")
        return

    headers = ["ID", "Shard", "Status", "Priority", "Payload"]
    rows = []
    for jd in jobs_list:
        rows.append([
            jd.get("id", ""),
            jd.get("shard_id", ""),
            jd.get("status", ""),
            jd.get("priority", ""),
            str(jd.get("payload", ""))[:60],
        ])
    print()
    print(_format_table(headers, rows[:20]))  # Limit display rows


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="queue-max",
        description="Robusta Queue - Super robust task queue with SQLite sharding",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=None,
        help="Number of shards (default: NUM_SHARDS env or 6)",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=None,
        help="Requests per minute (default: RATE_LIMIT_MAX env or 160)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Data directory (default: DATA_DIR env or ./data)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # stats
    stats_parser = subparsers.add_parser("stats", help="Show queue statistics")
    stats_parser.add_argument("--shard", type=int, default=None, help="Specific shard")
    stats_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # worker
    worker_parser = subparsers.add_parser("worker", help="Start worker(s)")
    worker_parser.add_argument(
        "--function",
        required=True,
        help="Function reference (MODULE:FUNCTION)",
    )
    worker_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker threads (default: 1)",
    )

    # enqueue
    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue a job")
    enqueue_parser.add_argument(
        "--payload",
        required=True,
        help="Job payload as JSON string",
    )
    enqueue_parser.add_argument("--priority", type=int, default=0, choices=[0, 1, 2])
    enqueue_parser.add_argument("--pagina-id", type=int, default=None)
    enqueue_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # purge
    purge_parser = subparsers.add_parser("purge", help="Remove old jobs")
    purge_parser.add_argument("--days", type=int, default=7, help="Age threshold (days)")

    # retry
    retry_parser = subparsers.add_parser("retry", help="Retry failed jobs")
    retry_parser.add_argument("--shard", type=int, default=None, help="Specific shard")
    retry_parser.add_argument("--job-id", type=int, default=None, help="Specific job ID")

    # list
    list_parser = subparsers.add_parser("list", help="List jobs")
    list_parser.add_argument(
        "--status",
        choices=["pending", "processing", "failed", "all"],
        default="failed",
    )
    list_parser.add_argument("--limit", type=int, default=50)
    list_parser.add_argument("--json", action="store_true", help="Output as JSON")

    return parser


def main(argv: List[str] | None = None) -> int:
    """Main entry point for the Robusta Queue CLI.

    Args:
        argv: Command-line arguments (default: sys.argv[1:]).

    Returns:
        Exit code (0=success, 1=error, 2=usage error).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # Set up logging
    logging.basicConfig(
        level=os.environ.get("ROBUSTA_QUEUE_LOG_LEVEL", "WARNING").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.command:
        parser.print_help()
        return 0

    command_map = {
        "stats": cmd_stats,
        "worker": cmd_worker,
        "enqueue": cmd_enqueue,
        "purge": cmd_purge,
        "retry": cmd_retry,
        "list": cmd_list,
    }

    try:
        command_map[args.command](args)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        logger.exception("Command failed")
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
