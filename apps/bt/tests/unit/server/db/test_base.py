"""
Tests for BaseDbAccess
"""

from __future__ import annotations

from pathlib import Path
from threading import Thread

from sqlalchemy import text
from sqlalchemy.pool import NullPool

from src.infrastructure.db.market.base import BaseDbAccess


class TestBaseDbAccess:
    def test_create_rw(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        assert db.engine is not None
        db.close()

    def test_create_readonly(self, tmp_path: Path) -> None:
        # Create DB first
        db_path = str(tmp_path / "test.db")
        rw = BaseDbAccess(db_path)
        with rw.engine.begin() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER PRIMARY KEY)"))
        rw.close()

        # Now open read-only
        ro = BaseDbAccess(db_path, read_only=True)
        with ro.engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in result]
            assert "test" in tables
        ro.close()

    def test_wal_pragma_set_on_rw(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        with db.engine.connect() as conn:
            result = conn.execute(text("PRAGMA journal_mode"))
            mode = result.scalar()
            assert mode == "wal"
        db.close()

    def test_fk_pragma_set_on_rw(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        with db.engine.connect() as conn:
            result = conn.execute(text("PRAGMA foreign_keys"))
            fk = result.scalar()
            assert fk == 1
        db.close()

    def test_pragmas_not_set_on_readonly(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        # Create DB first
        rw = BaseDbAccess(db_path)
        with rw.engine.begin() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER PRIMARY KEY)"))
        rw.close()

        # Read-only should not set WAL (already set from write)
        ro = BaseDbAccess(db_path, read_only=True)
        with ro.engine.connect() as conn:
            # FK pragma is per-connection, read-only should NOT set it
            # (though journal_mode persists from the write)
            result = conn.execute(text("PRAGMA foreign_keys"))
            fk = result.scalar()
            assert fk == 0  # Not set for read-only
        ro.close()

    def test_close_and_dispose(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        db.close()
        # After close, engine should be disposed (no error on second close)
        db.close()

    def test_engine_property(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        from sqlalchemy import Engine

        assert isinstance(db.engine, Engine)
        db.close()

    def test_uses_null_pool(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        assert isinstance(db.engine.pool, NullPool)
        db.close()

    def test_busy_timeout_set_on_connection(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        with db.engine.connect() as conn:
            timeout = conn.execute(text("PRAGMA busy_timeout")).scalar()
            assert timeout == 30000
        db.close()

    def test_concurrent_read_write_does_not_raise_transaction_errors(self, tmp_path: Path) -> None:
        db = BaseDbAccess(str(tmp_path / "test.db"))
        with db.engine.begin() as conn:
            conn.execute(text("CREATE TABLE sync_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"))

        errors: list[Exception] = []

        def _writer() -> None:
            try:
                for idx in range(300):
                    with db.engine.begin() as conn:
                        conn.execute(
                            text(
                                "INSERT OR REPLACE INTO sync_metadata (key, value) "
                                "VALUES ('k', :value)"
                            ),
                            {"value": str(idx)},
                        )
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        def _reader() -> None:
            try:
                for _ in range(300):
                    with db.engine.connect() as conn:
                        conn.execute(text("SELECT COUNT(*) FROM sync_metadata")).scalar()
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        writer = Thread(target=_writer)
        reader = Thread(target=_reader)
        writer.start()
        reader.start()
        writer.join(timeout=10)
        reader.join(timeout=10)

        assert not writer.is_alive()
        assert not reader.is_alive()
        assert not errors
        db.close()
