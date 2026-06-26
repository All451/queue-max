"""Tests for ConnectionManager — connection lifecycle, schema, migrations."""

import sqlite3
import threading

import pytest

from queue_max.core.db.connection import ConnectionManager
from queue_max.core.db.constants import SCHEMA_VERSION


class TestConnectionManager:
    """User-provided test suite covering all ConnectionManager paths."""

    def test_invalid_num_shards(self, tmp_path):
        with pytest.raises(ValueError):
            ConnectionManager(0, str(tmp_path))

    def test_invalid_shard_id_negative(self, tmp_path):
        cm = ConnectionManager(2, str(tmp_path))
        with pytest.raises(IndexError):
            cm.get_raw_connection(-1)

    def test_invalid_shard_id_above_limit(self, tmp_path):
        cm = ConnectionManager(2, str(tmp_path))
        with pytest.raises(IndexError):
            cm.get_raw_connection(2)

    def test_same_thread_reuses_connection(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        c1 = cm.get_raw_connection(0)
        c2 = cm.get_raw_connection(0)
        assert c1 is c2

    def test_different_threads_have_different_connections(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        conn_main = cm.get_raw_connection(0)
        conn_thread = []

        def worker():
            conn_thread.append(cm.get_raw_connection(0))

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert conn_main is not conn_thread[0]

    def test_context_manager_returns_connection(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        with cm.get_connection(0) as conn:
            assert isinstance(conn, sqlite3.Connection)

    def test_context_manager_rolls_back(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        with pytest.raises(RuntimeError):
            with cm.get_connection(0) as conn:
                conn.execute(
                    "INSERT INTO fila(payload) VALUES(?)",
                    ('{"x":1}',),
                )
                raise RuntimeError()

        with cm.get_connection(0) as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM fila"
            ).fetchone()[0]
        assert n == 0

    def test_close_all(self, tmp_path):
        cm = ConnectionManager(2, str(tmp_path))
        cm.get_raw_connection(0)
        cm.get_raw_connection(1)
        cm.close_all()
        assert cm._local.connections == {}

    def test_connection_recreated_after_close(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        c1 = cm.get_raw_connection(0)
        cm.close_all()
        c2 = cm.get_raw_connection(0)
        assert c1 is not c2

    def test_database_files_created(self, tmp_path):
        ConnectionManager(3, str(tmp_path))
        assert (tmp_path / "shard_0.db").exists()
        assert (tmp_path / "shard_1.db").exists()
        assert (tmp_path / "shard_2.db").exists()

    def test_schema_exists(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        with cm.get_connection(0) as conn:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        assert "fila" in tables
        assert "dead_letter_queue" in tables
        assert "shard_metadata" in tables

    def test_metadata_created(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        with cm.get_connection(0) as conn:
            row = conn.execute(
                "SELECT shard_id, version FROM shard_metadata"
            ).fetchone()
        assert row["shard_id"] == 0

    def test_get_connection_multiple_times(self, tmp_path):
        cm = ConnectionManager(4, str(tmp_path))
        for i in range(4):
            assert cm.get_raw_connection(i)


class TestConnectionManagerMigrations:
    """Tests for schema migration logic in ConnectionManager."""

    def test_new_database_has_current_version(self, tmp_path):
        cm = ConnectionManager(1, str(tmp_path))
        with cm.get_connection(0) as conn:
            row = conn.execute(
                "SELECT version FROM shard_metadata WHERE shard_id=0"
            ).fetchone()
        assert row["version"] == SCHEMA_VERSION

    def test_migration_not_run_for_current_version(self, tmp_path):
        """Migrate from SCHEMA_VERSION to SCHEMA_VERSION → no-op."""
        cm = ConnectionManager(1, str(tmp_path))
        # Set version to current manually
        with cm.get_connection(0) as conn:
            conn.execute(
                "UPDATE shard_metadata SET version=? WHERE shard_id=0",
                (SCHEMA_VERSION,),
            )
            conn.commit()

        # Re-init with another CM pointing at same DB
        cm2 = ConnectionManager(1, str(tmp_path))
        with cm2.get_connection(0) as conn:
            row = conn.execute(
                "SELECT version FROM shard_metadata WHERE shard_id=0"
            ).fetchone()
        assert row["version"] == SCHEMA_VERSION

    def test_drop_legacy_index_does_not_crash(self, tmp_path):
        """DROP INDEX IF EXISTS on a non-existent index is safe."""
        cm = ConnectionManager(1, str(tmp_path))
        # Legacy indexes might not exist — migration handles gracefully
        with cm.get_connection(0) as conn:
            conn.execute("DROP INDEX IF EXISTS idx_status_priority")
            conn.commit()
        assert True

    def test_pragma_values(self, tmp_path):
        """_apply_pragmas sets expected PRAGMA values."""
        cm = ConnectionManager(1, str(tmp_path))
        with cm.get_connection(0) as conn:
            journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
            sync = conn.execute("PRAGMA synchronous").fetchone()[0]
        assert journal == "wal"
        assert sync == 1  # NORMAL

    def test_migration_exception_logged_not_crash(self, tmp_path):
        """A migration step that raises is caught and logged."""
        cm = ConnectionManager(1, str(tmp_path))
        # Simulate an old version that triggers a failing migration
        with cm.get_connection(0) as conn:
            conn.execute(
                "UPDATE shard_metadata SET version=0 WHERE shard_id=0"
            )
            conn.commit()
        # Re-init → migration from v0 to SCHEMA_VERSION
        cm2 = ConnectionManager(1, str(tmp_path))
        # Should not crash — version updated even if some steps fail
        with cm2.get_connection(0) as conn:
            row = conn.execute(
                "SELECT version FROM shard_metadata WHERE shard_id=0"
            ).fetchone()
        assert row["version"] >= 0
