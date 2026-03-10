"""DatasetResolver のユニットテスト"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.application.services.dataset_resolver import DatasetResolver
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter


@pytest.fixture
def resolver_dir(tmp_path):
    """テスト用のデータセットディレクトリ"""
    for name in ["test-market", "prime_v2"]:
        writer = DatasetWriter(str(tmp_path / f"{name}.db"))
        writer.set_dataset_info("preset", "quickTesting")
        writer.close()
    legacy_db_path = tmp_path / "quickTesting.db"
    conn = sqlite3.connect(legacy_db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS dataset_info (key TEXT PRIMARY KEY, value TEXT)")
    conn.close()
    compat_snapshot_dir = tmp_path / "compat_only"
    compat_snapshot_dir.mkdir()
    compat_conn = sqlite3.connect(compat_snapshot_dir / "dataset.db")
    compat_conn.execute("CREATE TABLE IF NOT EXISTS dataset_info (key TEXT PRIMARY KEY, value TEXT)")
    compat_conn.close()
    return str(tmp_path)


@pytest.fixture(autouse=True)
def _patch_manifest_validation(monkeypatch) -> None:
    from src.infrastructure.db.market import dataset_snapshot_reader as reader_module

    def _fake_validate(snapshot_dir: str | Path):
        snapshot_path = Path(snapshot_dir)
        duckdb_path = snapshot_path / "dataset.duckdb"
        db_path = snapshot_path / "dataset.db"
        return reader_module.DatasetSnapshotManifest.model_validate(
            {
                "schemaVersion": 1,
                "generatedAt": "2026-03-09T00:00:00+00:00",
                "dataset": {
                    "name": snapshot_path.name,
                    "preset": "quickTesting",
                    "duckdbFile": "dataset.duckdb",
                    "compatibilityDbFile": "dataset.db",
                    "parquetDir": "parquet",
                },
                "source": {
                    "backend": "duckdb-parquet",
                    "compatibilityArtifact": "dataset.db",
                },
                "counts": {
                    "stocks": 0,
                    "stock_data": 0,
                    "topix_data": 0,
                    "indices_data": 0,
                    "margin_data": 0,
                    "statements": 0,
                    "dataset_info": 1,
                },
                "coverage": {
                    "totalStocks": 0,
                    "stocksWithQuotes": 0,
                    "stocksWithStatements": 0,
                    "stocksWithMargin": 0,
                },
                "checksums": {
                    "duckdbSha256": reader_module._sha256_of_file(duckdb_path),
                    "compatibilityDbSha256": reader_module._sha256_of_file(db_path),
                    "logicalSha256": "x",
                    "parquet": {},
                },
            }
        )

    monkeypatch.setattr(reader_module, "validate_dataset_snapshot", _fake_validate)


class TestDatasetResolver:
    def test_list_datasets(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        names = resolver.list_datasets()
        assert sorted(names) == ["compat_only", "prime_v2", "quickTesting", "test-market"]

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
        assert path.endswith("test-market/dataset.db")

    def test_get_dataset_path_prefers_snapshot_dir(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_dataset_path("test-market")
        assert path.endswith("test-market")

    def test_exists_checks_snapshot_and_legacy(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        assert resolver.exists("test-market") is True
        assert resolver.exists("compat_only") is True
        assert resolver.exists("quickTesting") is True
        assert resolver.exists("missing") is False

    def test_resolve_uses_snapshot_compatibility_db_when_duckdb_missing(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("compat_only")
        assert db is not None

    def test_get_dataset_path_prefers_snapshot_dir_when_only_compatibility_db_exists(
        self, resolver_dir: str
    ) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_dataset_path("compat_only")
        assert path.endswith("compat_only")

    def test_empty_dir(self, tmp_path) -> None:
        resolver = DatasetResolver(str(tmp_path))
        assert resolver.list_datasets() == []

    def test_nonexistent_dir(self) -> None:
        resolver = DatasetResolver("/nonexistent/path")
        assert resolver.list_datasets() == []
