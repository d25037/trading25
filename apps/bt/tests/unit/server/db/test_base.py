"""
Tests for BaseDbAccess
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from src.server.db.base import BaseDbAccess


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
