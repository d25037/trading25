"""DatasetResolver のユニットテスト"""

from __future__ import annotations

import os
import sqlite3

import pytest

from src.server.services.dataset_resolver import DatasetResolver


@pytest.fixture
def resolver_dir(tmp_path):
    """テスト用のデータセットディレクトリ"""
    # テスト用 .db ファイルを作成
    for name in ["test-market", "prime_v2", "quickTesting"]:
        db_path = os.path.join(str(tmp_path), f"{name}.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS dataset_info (key TEXT PRIMARY KEY, value TEXT)")
        conn.close()
    return str(tmp_path)


class TestDatasetResolver:
    def test_list_datasets(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        names = resolver.list_datasets()
        assert sorted(names) == ["prime_v2", "quickTesting", "test-market"]

    def test_resolve_existing(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("test-market")
        assert db is not None

    def test_resolve_with_db_extension(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("test-market.db")
        assert db is not None

    def test_resolve_nonexistent(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("nonexistent")
        assert db is None

    def test_resolve_caches(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db1 = resolver.resolve("test-market")
        db2 = resolver.resolve("test-market")
        assert db1 is db2

    def test_validate_name_invalid(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            resolver.resolve("../etc/passwd")

    def test_validate_name_path_traversal(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            resolver.resolve("../../secret")

    def test_evict(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db1 = resolver.resolve("test-market")
        assert db1 is not None
        resolver.evict("test-market")
        # After evict, resolve should create a new instance
        db2 = resolver.resolve("test-market")
        assert db2 is not None
        assert db1 is not db2

    def test_close_all(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        resolver.resolve("test-market")
        resolver.resolve("prime_v2")
        resolver.close_all()
        # After close_all, cache should be empty
        assert len(resolver._cache) == 0

    def test_get_db_path(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_db_path("test-market")
        assert path.endswith("test-market.db")

    def test_empty_dir(self, tmp_path) -> None:
        resolver = DatasetResolver(str(tmp_path))
        assert resolver.list_datasets() == []

    def test_nonexistent_dir(self) -> None:
        resolver = DatasetResolver("/nonexistent/path")
        assert resolver.list_datasets() == []
