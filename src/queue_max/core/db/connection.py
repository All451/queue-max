"""Thread-local SQLite connection management."""

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Dict, Iterator

from queue_max.core.db.constants import (
    DATA_DIR,
    DB_BUSY_TIMEOUT,
    DROP_LEGACY_INDEXES_SQL,
    GET_SCHEMA_VERSION_SQL,
    INDEXES_SQL,
    INSERT_META_SQL,
    PRAGMAS_SQL,
    SCHEMA_SQL,
    SCHEMA_VERSION,
    UPDATE_SCHEMA_VERSION_SQL,
    get_migrations,
)

logger = logging.getLogger("queue_max.database.connection")


class ConnectionManager:
    """Manages thread-local SQLite connections per shard.

    Each thread gets its own connection per shard, avoiding the
    'SQLite objects created in a thread can only be used in that same thread' error.

    Connection lifecycle is tied to the thread that created them —
    ``close_all()`` only closes connections belonging to the calling thread.
    """

    def __init__(self, num_shards: int, data_dir: str = DATA_DIR):
        if num_shards < 1:
            raise ValueError(f"num_shards must be >= 1, got {num_shards}")
        self.num_shards = num_shards
        self.data_dir = data_dir
        self._local = threading.local()
        self._all_connections: set[sqlite3.Connection] = set()
        self._connections_lock = threading.Lock()
        os.makedirs(data_dir, exist_ok=True)
        self._init_all_shards()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _apply_pragmas(self, conn: sqlite3.Connection) -> None:
        """Apply performance PRAGMAs to a connection."""
        for p in PRAGMAS_SQL:
            conn.execute(p)

    def _init_all_shards(self) -> None:
        """Create shard databases, schema, and apply pending migrations."""
        for shard_id in range(self.num_shards):
            db_path = os.path.join(self.data_dir, f"shard_{shard_id}.db")
            is_new = not os.path.exists(db_path)
            conn = sqlite3.connect(db_path, timeout=DB_BUSY_TIMEOUT / 1000)
            conn.row_factory = sqlite3.Row
            try:
                self._apply_pragmas(conn)
                conn.executescript(SCHEMA_SQL)
                for i in INDEXES_SQL:
                    conn.execute(i)
                if is_new:
                    conn.execute(INSERT_META_SQL, (shard_id, SCHEMA_VERSION))
                else:
                    self._run_migrations(conn, shard_id)
                conn.commit()
            finally:
                conn.close()

    def _run_migrations(self, conn: sqlite3.Connection, shard_id: int) -> None:
        """Apply pending schema migrations for ``shard_id``.

        Reads the current schema version from ``shard_metadata``, applies
        any missing migration steps, and updates the version.
        """
        try:
            row = conn.execute(GET_SCHEMA_VERSION_SQL).fetchone()
            current_version = row["version"] if row else 0
        except sqlite3.OperationalError:
            current_version = 0

        if current_version >= SCHEMA_VERSION:
            return

        logger.info(
            "Migrating shard %d schema v%d -> v%d",
            shard_id, current_version, SCHEMA_VERSION,
        )

        # Legacy cleanups (not version-specific)
        for statement in DROP_LEGACY_INDEXES_SQL:
            try:
                conn.execute(statement)
            except sqlite3.OperationalError as e:
                logger.debug("Migration skip (shard %d): %s", shard_id, e)

        # Version-specific migrations
        for statement in get_migrations(current_version, SCHEMA_VERSION):
            try:
                conn.execute(statement)
            except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
                logger.warning(
                    "Migration step failed (shard %d, v%s): %s",
                    shard_id, SCHEMA_VERSION, e,
                )

        conn.execute(UPDATE_SCHEMA_VERSION_SQL, (SCHEMA_VERSION,))

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def _validate_shard_id(self, shard_id: int) -> None:
        if not (0 <= shard_id < self.num_shards):
            raise IndexError(
                f"shard_id {shard_id} out of range [0, {self.num_shards})"
            )

    def get_raw_connection(self, shard_id: int) -> sqlite3.Connection:
        self._validate_shard_id(shard_id)
        return self._get_connection(shard_id)

    @contextmanager
    def get_connection(self, shard_id: int) -> Iterator[sqlite3.Connection]:
        self._validate_shard_id(shard_id)
        conn = self._get_connection(shard_id)
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise

    def _get_connection(self, shard_id: int) -> sqlite3.Connection:
        if not hasattr(self._local, "connections"):
            self._local.connections: Dict[int, sqlite3.Connection] = {}
        if shard_id not in self._local.connections:
            db_path = os.path.join(self.data_dir, f"shard_{shard_id}.db")
            conn = sqlite3.connect(db_path, timeout=DB_BUSY_TIMEOUT / 1000)
            conn.row_factory = sqlite3.Row
            self._apply_pragmas(conn)
            self._local.connections[shard_id] = conn
            with self._connections_lock:
                self._all_connections.add(conn)
        return self._local.connections[shard_id]

    def close_all(self) -> None:
        if hasattr(self._local, "connections"):
            for conn in self._local.connections.values():
                try:
                    conn.close()
                except Exception:
                    pass
            self._local.connections.clear()
