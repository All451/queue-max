# Changelog

All notable changes to Queue Max are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-06-26

### Added

- **Schema migrations** — `SCHEMA_VERSION` tracking per shard, automatic migration
  execution on init, legacy index cleanup (`DROP idx_status_priority`, `DROP idx_next_retry`)
- **CHECK constraints** — `fila.status` now has `CHECK(status IN ('pending','processing',…))`
- **ConnectionManager** — dedicated class for thread-local SQLite connection lifecycle,
  shard ID validation (`IndexError` on out-of-range), `_apply_pragmas()` helper,
  `close_all()` scoped to calling thread
- **ShardRepository** — extracted from ShardManager: CRUD, retry, metrics, maintenance
- **ShardGroup** — extracted from Queue into `core/db/shard_group.py` with its own module
- **db/constants.py** — all SQL queries, schema, indexes, PRAGMAs centralized
- **CircuitBreaker improvements:**
  - `failure_predicate` — configurable callable to decide which exceptions count as failures
  - HALF_OPEN single probe — `_probe_in_flight` flag ensures exactly one probe in half-open state
  - `rejected_count` — tracks total blocked calls
  - `total_time_open_seconds` — cumulative time in OPEN state
  - `state_change_count` — number of state transitions
  - `failure_threshold` validation (`>= 1`)
  - `recovery_timeout` validation (`> 0`)
  - Typed `Callable[..., T]` with `TypeVar` for `call()` return type
  - Callback `on_state_change` invoked **outside** the mutex
- **RateLimiter improvements:**
  - `threading.Condition`-based waiting (replaces busy-polling)
  - No jitter in token regeneration (jitter distorts the bucket algorithm)
  - `burst_capacity` independent from `rate_limit` in `update_rate_limit()`
  - `update_rate_limit()` calls `_refill()` before changing rate
  - `get_stats()` calls `_refill()` internally (consistent snapshot)
  - Validation: `rate_limit >= 1`, `burst_capacity >= 1`
  - Private `random.Random()` instance for deterministic tests
  - Cumulative metrics: `total_granted`, `total_denied`, `total_wait_seconds`
  - `_tokens_per_second()` helper extracted
- **WorkerPool improvements:**
  - `worker_factory` — configurable callable for creating workers (decoupling)
  - `threading.Lock` protecting `self.workers` list
  - `scale_cooldown` — prevents thrashing by limiting scale frequency
  - Proportional scaling: `target = current + max(1, pending // threshold)`
  - Monotonic worker ID counter (no collisions after add/remove cycles)
  - `time.monotonic()` replaces `time.time()`
- **AsyncWorker callbacks** — now supports `on_job_start`, `on_job_complete`, `on_job_error`
- **Job model** — renamed fields: `pagina_id` → `partition_key`,
  `tentativas` → `attempts`, `max_tentativas` → `max_attempts`
  (`from_row()` maps legacy column names for backward compatibility)
- **Lifecycle methods** on `Job`: `mark_processing()`, `mark_completed()`, `mark_failed()`,
  `mark_cancelled()`, `update_progress()`, `_schedule_retry()`
- **`JobPriority`** enum and `JobResult` dataclass
- **`@retryable_task`** now at 100% test coverage
- **Stress test script** (`tests/stress_test.py`) — validates exactly-once processing
  under load (100k, 500k jobs)

### Changed

- **Core reorganization** into subdirectories by domain:
  ```
  core/             →  flat namespace
  core/db/          ←  ConnectionManager, ShardRepository, ShardGroup, constants
  core/queue/       ←  Queue class
  core/workers/     ←  Worker, AsyncWorker, WorkerPool
  core/tasks/       ←  @task, @periodic_task, @retryable_task
  ```
- **ShardManager** reduced to thin facade (delegates to ConnectionManager + ShardRepository)
- **`@task`** — `schedule_in` defined before `schedule_at` (avoid name resolution fragility);
  `Union`/`Tuple` type hints properly imported
- **`@periodic_task`** — documented that scheduler has no stop mechanism
- **`batch()` context manager** — replaced monkey-patch `self._emit` with
  `_batch_active` flag (eliminates deadlock risk)
- **`is_retryable_error`** — now treats any 4xx (except 429) as permanent failure
- **`pop_job`** in multi-group mode — now properly handles exception per shard
- **Index optimization** — `idx_pop_job` replaces `idx_status_priority` + `idx_next_retry`;
  covers `WHERE status, next_retry_at` + `ORDER BY priority, id`
- **Test suite** grew from 78 to 268 tests
- **Coverage** from 67% to 97%

### Fixed

- `Union` and `Tuple` missing from `typing` imports in decorator (`NameError` at decoration time)
- `CircuitBreakerOpenError` lazy import moved to module top
- Semicolons in multi-statement lines (`database.py` → `repository.py`)
- `purge_queue()` accessing raw SQLite connections (now delegates to `ShardManager.purge_jobs()`)
- `import random` inside `_refill()` hot path (moved to module top)
- AsyncWorker missing `on_job_start`, `on_job_complete`, `on_job_error` callbacks
- WorkerPool `_scale_to` hardcoded worker IDs (now uses monotonic counter)
- WorkerPool `wait_for_idle()` using `time.time()` (now `time.monotonic()`)
- Job priority test — jobs were scattered across shards (now uses `pagina_id=0`)
- `test_worker_stop_during_processing` — job could complete before stop (fixed with `stop(timeout=1)`)
- Concurrent pop test — job IDs collide across shards (now uses `(id, shard_id)` tuples)
- `ValueError` treated as permanent failure — now correctly retryable by default
- Schema migration `ALTER TABLE ADD CONSTRAINT` failure logged as warning, not crash

### Removed

- `Worker.is_running` property (dead code — `get_stats()` accesses `_state` directly)

### Performance

| Test | Config | Result |
|------|--------|--------|
| Burst | 20 workers, 10 shards | ~3.300 jobs/s |
| 100k jobs | 16 workers, 8 shards | 1.400 jobs/s — **0 duplicatas** |
| 500k jobs | 16 workers, 8 shards | 355 jobs/s — **0 duplicatas** |

## [0.1.1] — 2026-05-20

### Changed

- Updated project dependencies in `pyproject.toml`
- Type annotations updated to use built-in lowercase collection types (`list[int]` instead of `List[int]`)

## [0.1.0] — 2026-05-15

### Added

- Initial release
- SQLite-backed queue with WAL mode and physical sharding
- `Queue` — enqueue, pop, complete, fail, batch, events
- `ShardManager` — thread-local connections, schema init, CRUD, metrics
- `ShardGroup` — adaptive grouping for distributed pop scans
- `Worker`, `AsyncWorker`, `WorkerPool` — processing with state machine, heartbeats
- `CircuitBreaker` — 3-state (CLOSED/OPEN/HALF_OPEN), callback on state change
- `RateLimiter` — token bucket, per-second/minute/hour, jitter, dynamic rate update
- `@task` decorator — `.delay()`, `.schedule_at()`, `.schedule_in()`, `.map()`, `.bulk_delay()`
- `@periodic_task` — scheduler with daemon thread
- `@retryable_task` — synchronous retry with backoff
- `Job`, `JobStatus`, `JobPriority` — data model with lifecycle tracking
- Retry with exponential backoff (±20% jitter)
- Dead Letter Queue
- Heartbeat and orphan recovery
- CLI (`queue-max stats`, `worker`, `enqueue`, `list`, `retry`, `purge`)
- Event system (built-in callbacks + optional typed events with bubus)
- Django management commands, FastAPI middleware, Flask extension
- Stress test benchmarks (5k–50k jobs)

[0.2.0]: https://github.com/All451/queue-max/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/All451/queue-max/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/All451/queue-max/releases/tag/v0.1.0
