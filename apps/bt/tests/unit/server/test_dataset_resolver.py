"""DatasetResolver のユニットテスト"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import src.infrastructure.db.market.dataset_snapshot_reader as snapshot_reader_module
from src.application.services.dataset_resolver import DatasetResolver
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)


def _write_manifest(snapshot_dir: Path, name: str, *, schema_version: int = 3) -> None:
    duckdb_path = snapshot_dir / "dataset.duckdb"
    parquet_dir = snapshot_dir / "parquet"
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    manifest = {
        "schemaVersion": schema_version,
        "generatedAt": "2026-03-14T00:00:00+00:00",
        "dataset": {
            "name": name,
            "preset": "quickTesting",
            "duckdbFile": "dataset.duckdb",
            "parquetDir": "parquet",
        },
        "source": {
            "backend": "duckdb-parquet",
            "marketSchemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        },
        "logicalCounts": inspection.counts.model_dump(),
        "coverage": inspection.coverage.model_dump(),
        "checksums": {
            "duckdbSha256": hashlib.sha256(duckdb_path.read_bytes()).hexdigest(),
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                counts=inspection.counts,
                coverage=inspection.coverage,
                date_range=inspection.date_range,
            ),
            "parquet": {
                parquet_file.name: hashlib.sha256(parquet_file.read_bytes()).hexdigest()
                for parquet_file in sorted(parquet_dir.glob("*.parquet"))
            },
        },
    }
    if inspection.date_range is not None:
        manifest["dateRange"] = inspection.date_range.model_dump()
    (snapshot_dir / "manifest.v2.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.fixture
def resolver_dir(tmp_path: Path) -> str:
    """テスト用のデータセットディレクトリ"""
    for name in ["test-market", "prime_v2"]:
        writer = DatasetWriter(str(tmp_path / name))
        writer.set_dataset_info("preset", "quickTesting")
        writer.close()
        _write_manifest(tmp_path / name, name)

    compat_snapshot_dir = tmp_path / "compat_only"
    compat_snapshot_dir.mkdir()
    (compat_snapshot_dir / "dataset.db").write_text("", encoding="utf-8")
    return str(tmp_path)


class TestDatasetResolver:
    def test_list_datasets(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        names = resolver.list_datasets()
        assert sorted(names) == ["prime_v2", "test-market"]

    def test_resolve_existing(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        db = resolver.resolve("test-market")
        assert db is not None

    def test_resolve_rejects_db_extension(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            resolver.resolve("test-market.db")

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
        db2 = resolver.resolve("test-market")
        assert db2 is not None
        assert db1 is not db2

    def test_close_all(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        resolver.resolve("test-market")
        resolver.resolve("prime_v2")
        resolver.close_all()
        assert len(resolver._cache) == 0

    def test_get_duckdb_path(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_duckdb_path("test-market")
        assert path.endswith("test-market/dataset.duckdb")

    def test_get_dataset_path_prefers_snapshot_dir(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_dataset_path("test-market")
        assert path.endswith("test-market")

    def test_exists_checks_supported_snapshots_only(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        assert resolver.exists("test-market") is True
        assert resolver.exists("prime_v2") is True
        assert resolver.exists("compat_only") is False
        assert resolver.exists("missing") is False

    def test_discovery_rejects_schema_version_two_without_opening_duckdb(
        self, resolver_dir: str
    ) -> None:
        snapshot_dir = Path(resolver_dir) / "test-market"
        _write_manifest(snapshot_dir, "test-market", schema_version=2)
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()
        assert resolver.resolve("test-market") is None

    def test_discovery_rejects_missing_event_time_mode(self, resolver_dir: str) -> None:
        manifest_path = Path(resolver_dir) / "test-market" / "manifest.v2.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        del manifest["source"]["stockPriceAdjustmentMode"]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()

    @pytest.mark.parametrize(
        ("section", "mutation"),
        [
            ("dataset", lambda manifest: manifest.pop("dataset")),
            ("logicalCounts", lambda manifest: manifest.pop("logicalCounts")),
            ("coverage", lambda manifest: manifest.pop("coverage")),
            ("checksums", lambda manifest: manifest.pop("checksums")),
            (
                "dataset.duckdbFile",
                lambda manifest: manifest["dataset"].__setitem__("duckdbFile", "dataset.db"),
            ),
            (
                "logicalCounts.stocks",
                lambda manifest: manifest["logicalCounts"].__setitem__("stocks", -1),
            ),
            (
                "coverage.totalStocks",
                lambda manifest: manifest["coverage"].__setitem__("totalStocks", -1),
            ),
            (
                "checksums.duckdbSha256",
                lambda manifest: manifest["checksums"].__setitem__(
                    "duckdbSha256", "not-a-sha256"
                ),
            ),
        ],
    )
    def test_discovery_rejects_partial_or_malformed_v3_manifest_without_opening_duckdb(
        self,
        resolver_dir: str,
        monkeypatch: pytest.MonkeyPatch,
        section: str,
        mutation,
    ) -> None:
        del section
        manifest_path = Path(resolver_dir) / "test-market" / "manifest.v2.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        mutation(manifest)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        monkeypatch.setattr(
            snapshot_reader_module,
            "_connect_duckdb",
            lambda *_args, **_kwargs: pytest.fail("discovery opened DuckDB"),
        )
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("test-market") is False
        assert "test-market" not in resolver.list_datasets()

    def test_discovery_rejects_partial_snapshot_without_manifest(self, resolver_dir: str) -> None:
        partial = Path(resolver_dir) / "cancelled"
        writer = DatasetWriter(str(partial))
        writer.close()
        resolver = DatasetResolver(resolver_dir)

        assert resolver.exists("cancelled") is False
        assert "cancelled" not in resolver.list_datasets()

    def test_resolve_unsupported_compatibility_snapshot_returns_none(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        assert resolver.resolve("compat_only") is None

    def test_get_dataset_path_for_unsupported_snapshot_dir(self, resolver_dir: str) -> None:
        resolver = DatasetResolver(resolver_dir)
        path = resolver.get_dataset_path("compat_only")
        assert path.endswith("compat_only")

    def test_empty_dir(self, tmp_path: Path) -> None:
        resolver = DatasetResolver(str(tmp_path))
        assert resolver.list_datasets() == []

    def test_nonexistent_dir(self) -> None:
        resolver = DatasetResolver("/nonexistent/path")
        assert resolver.list_datasets() == []
