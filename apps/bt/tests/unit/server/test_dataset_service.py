from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from src.application.services import dataset_service
from src.application.services.dataset_resolver import DatasetResolver


class DummyResolver:
    def __init__(
        self,
        base_dir: Path,
        names: list[str],
        db_by_name: dict[str, object | None],
        path_by_name: dict[str, str] | None = None,
    ) -> None:
        self._base_dir = base_dir
        self._names = names
        self._db_by_name = db_by_name
        self._path_by_name = path_by_name or {}

    def list_datasets(self) -> list[str]:
        return self._names

    def get_snapshot_dir(self, name: str) -> str:
        return str(self._base_dir / name)

    def get_duckdb_path(self, name: str) -> str:
        return str(Path(self.get_snapshot_dir(name)) / "dataset.duckdb")

    def get_manifest_path(self, name: str) -> str:
        return str(Path(self.get_snapshot_dir(name)) / "manifest.v2.json")

    def get_dataset_path(self, name: str) -> str:
        return self._path_by_name.get(name, self.get_snapshot_dir(name))

    def get_legacy_db_path(self, name: str) -> str:
        return str(self._base_dir / f"{name}.db")

    def resolve(self, name: str) -> object | None:
        return self._db_by_name.get(name)

    def evict(self, name: str) -> None:
        self._db_by_name.pop(name, None)

    def get_artifact_paths(self, name: str) -> list[str]:
        paths: list[str] = []
        dataset_path = self.get_dataset_path(name)
        if os.path.exists(dataset_path):
            paths.append(dataset_path)
        legacy_db_path = self.get_legacy_db_path(name)
        if legacy_db_path != dataset_path and os.path.exists(legacy_db_path):
            paths.append(legacy_db_path)
        return paths


class DummyDb:
    def __init__(
        self,
        info: dict[str, str],
        table_counts: dict[str, int],
        date_range: dict[str, str] | None,
        stock_count: int,
        stocks_with_quotes: int,
        stocks_with_margin: int,
        stocks_with_statements: int,
        fk_orphans: dict[str, int],
        stocks_without_quotes: int,
    ) -> None:
        self._info = info
        self._table_counts = table_counts
        self._date_range = date_range
        self._stock_count = stock_count
        self._stocks_with_quotes = stocks_with_quotes
        self._stocks_with_margin = stocks_with_margin
        self._stocks_with_statements = stocks_with_statements
        self._fk_orphans = fk_orphans
        self._stocks_without_quotes = stocks_without_quotes

    def get_dataset_info(self) -> dict[str, str]:
        return self._info

    def get_table_counts(self) -> dict[str, int]:
        return self._table_counts

    def get_date_range(self) -> dict[str, str] | None:
        return self._date_range

    def get_stock_count(self) -> int:
        return self._stock_count

    def get_stocks_with_quotes_count(self) -> int:
        return self._stocks_with_quotes

    def get_stocks_with_margin_count(self) -> int:
        return self._stocks_with_margin

    def get_stocks_with_statements_count(self) -> int:
        return self._stocks_with_statements

    def get_fk_orphan_counts(self) -> dict[str, int]:
        return self._fk_orphans

    def get_stocks_without_quotes_count(self) -> int:
        return self._stocks_without_quotes


def test_list_datasets_skips_missing_files_and_handles_optional_metadata(tmp_path: Path) -> None:
    for name in ["a", "b"]:
        snapshot_dir = tmp_path / name
        snapshot_dir.mkdir()
        (snapshot_dir / "dataset.duckdb").write_text("", encoding="utf-8")
        (snapshot_dir / "manifest.v2.json").write_text("{}", encoding="utf-8")

    class MetadataErrorDb:
        def get_dataset_info(self) -> dict[str, str]:
            raise RuntimeError("broken metadata")

    resolver = DummyResolver(
        base_dir=tmp_path,
        names=["a", "b", "missing"],
        db_by_name={
            "a": None,  # metadata is optional
            "b": MetadataErrorDb(),  # metadata read failure is optional
        },
    )

    items = dataset_service.list_datasets(cast(Any, resolver))

    assert [item.name for item in items] == ["a", "b"]
    assert items[0].preset is None and items[0].createdAt is None
    assert items[1].preset is None and items[1].createdAt is None
    assert items[0].backend == "duckdb-parquet"
    assert items[1].backend == "duckdb-parquet"


def test_list_datasets_uses_latest_file_mtime_for_snapshot_dir(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    latest_file = snapshot_dir / "dataset.duckdb"
    latest_file.write_text("", encoding="utf-8")
    (snapshot_dir / "manifest.v2.json").write_text("{}", encoding="utf-8")

    dir_mtime = 1_700_000_000
    file_mtime = 1_700_000_500
    os.utime(snapshot_dir, (dir_mtime, dir_mtime))
    os.utime(latest_file, (file_mtime, file_mtime))
    os.utime(snapshot_dir / "manifest.v2.json", (dir_mtime, dir_mtime))

    resolver = DummyResolver(
        base_dir=tmp_path,
        names=["snapshot"],
        db_by_name={"snapshot": None},
        path_by_name={"snapshot": str(snapshot_dir)},
    )

    items = dataset_service.list_datasets(cast(Any, resolver))

    assert items[0].lastModified == datetime.fromtimestamp(file_mtime).isoformat()
    assert items[0].backend == "duckdb-parquet"


def test_get_dataset_info_with_sparse_data_returns_errors_and_warnings(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "sparse"
    snapshot_dir.mkdir()
    (snapshot_dir / "dataset.duckdb").write_text("", encoding="utf-8")
    (snapshot_dir / "manifest.v2.json").write_text("{}", encoding="utf-8")

    db = DummyDb(
        info={"preset": "primeMarket", "stock_count": "oops", "created_at": "2026-01-01T00:00:00+00:00"},
        table_counts={
            "stock_data": 0,
            "topix_data": 0,
            "margin_data": 0,
            "statements": 0,
            "indices_data": 0,
        },
        date_range=None,
        stock_count=0,
        stocks_with_quotes=0,
        stocks_with_margin=0,
        stocks_with_statements=0,
        fk_orphans={"stockDataOrphans": 1, "marginDataOrphans": 0, "statementsOrphans": 2},
        stocks_without_quotes=3,
    )
    resolver = DummyResolver(tmp_path, ["sparse"], {"sparse": db})

    info = dataset_service.get_dataset_info(cast(Any, resolver), "sparse")
    assert info is not None
    assert info.validation.isValid is False
    assert "No stocks found" in info.validation.errors
    assert "No stock data found" in info.validation.errors
    assert "Foreign key integrity issues detected" in info.validation.errors
    assert "No stocks have OHLCV data" in info.validation.warnings
    assert "No TOPIX data" in info.validation.warnings
    assert "No margin data" in info.validation.warnings
    assert "No statements data" in info.validation.warnings
    assert "No sector index data" in info.validation.warnings
    assert "3 stocks have no OHLCV records" in info.validation.warnings
    assert info.validation.details is not None
    assert info.validation.details.stockCountValidation is not None
    assert info.validation.details.stockCountValidation.expected is None
    assert info.stats.dateRange.from_ == "-"
    assert info.stats.dateRange.to == "-"
    assert info.snapshot.createdAt == "2026-01-01T00:00:00+00:00"
    assert info.storage.backend == "duckdb-parquet"
    assert info.storage.primaryPath.endswith("/sparse/dataset.duckdb")


def test_get_dataset_info_uses_latest_file_mtime_for_snapshot_dir(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    duckdb_file = snapshot_dir / "dataset.duckdb"
    duckdb_file.write_text("", encoding="utf-8")
    latest_file = snapshot_dir / "manifest.v2.json"
    latest_file.write_text("{}", encoding="utf-8")

    dir_mtime = 1_700_001_000
    file_mtime = 1_700_001_500
    os.utime(snapshot_dir, (dir_mtime, dir_mtime))
    os.utime(duckdb_file, (dir_mtime, dir_mtime))
    os.utime(latest_file, (file_mtime, file_mtime))

    db = DummyDb(
        info={"preset": "primeMarket"},
        table_counts={
            "stock_data": 1,
            "topix_data": 1,
            "margin_data": 0,
            "statements": 0,
            "indices_data": 0,
        },
        date_range={"min": "2024-01-01", "max": "2024-01-01"},
        stock_count=1,
        stocks_with_quotes=1,
        stocks_with_margin=0,
        stocks_with_statements=0,
        fk_orphans={"stockDataOrphans": 0, "marginDataOrphans": 0, "statementsOrphans": 0},
        stocks_without_quotes=0,
    )
    resolver = DummyResolver(
        base_dir=tmp_path,
        names=["snapshot"],
        db_by_name={"snapshot": db},
        path_by_name={"snapshot": str(snapshot_dir)},
    )

    info = dataset_service.get_dataset_info(cast(Any, resolver), "snapshot")

    assert info is not None
    assert info.lastModified == datetime.fromtimestamp(file_mtime).isoformat()
    assert info.storage.backend == "duckdb-parquet"
    assert info.storage.duckdbPath is not None
    assert info.storage.duckdbPath.endswith("/snapshot/dataset.duckdb")


def test_get_dataset_info_with_healthy_data_has_no_warnings(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "healthy"
    snapshot_dir.mkdir()
    (snapshot_dir / "dataset.duckdb").write_text("", encoding="utf-8")
    (snapshot_dir / "manifest.v2.json").write_text("{}", encoding="utf-8")

    db = DummyDb(
        info={"preset": "primeMarket", "stock_count": "2"},
        table_counts={
            "stock_data": 20,
            "topix_data": 10,
            "margin_data": 10,
            "statements": 10,
            "indices_data": 10,
        },
        date_range={"min": "2024-01-01", "max": "2024-12-31"},
        stock_count=2,
        stocks_with_quotes=2,
        stocks_with_margin=2,
        stocks_with_statements=2,
        fk_orphans={"stockDataOrphans": 0, "marginDataOrphans": 0, "statementsOrphans": 0},
        stocks_without_quotes=0,
    )
    resolver = DummyResolver(tmp_path, ["healthy"], {"healthy": db})

    info = dataset_service.get_dataset_info(cast(Any, resolver), "healthy")
    assert info is not None
    assert info.validation.isValid is True
    assert info.validation.errors == []
    assert info.validation.warnings == []
    assert info.validation.details is not None
    assert info.validation.details.stockCountValidation is not None
    assert info.validation.details.stockCountValidation.isWithinRange is True
    assert info.stats.dateRange.from_ == "2024-01-01"
    assert info.stats.dateRange.to == "2024-12-31"
    assert info.stats.hasMarginData is True
    assert info.stats.hasTOPIXData is True
    assert info.stats.hasSectorData is True
    assert info.stats.hasStatementsData is True
    assert info.storage.backend == "duckdb-parquet"


def test_get_dataset_info_without_stock_count_metadata_uses_none_expected(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "nostockcount"
    snapshot_dir.mkdir()
    (snapshot_dir / "dataset.duckdb").write_text("", encoding="utf-8")
    (snapshot_dir / "manifest.v2.json").write_text("{}", encoding="utf-8")

    db = DummyDb(
        info={},
        table_counts={
            "stock_data": 1,
            "topix_data": 0,
            "margin_data": 0,
            "statements": 0,
            "indices_data": 0,
        },
        date_range={"min": "2024-01-01", "max": "2024-01-01"},
        stock_count=1,
        stocks_with_quotes=1,
        stocks_with_margin=0,
        stocks_with_statements=0,
        fk_orphans={"stockDataOrphans": 0, "marginDataOrphans": 0, "statementsOrphans": 0},
        stocks_without_quotes=0,
    )
    resolver = DummyResolver(tmp_path, ["nostockcount"], {"nostockcount": db})

    info = dataset_service.get_dataset_info(cast(Any, resolver), "nostockcount")
    assert info is not None
    assert info.validation.details is not None
    assert info.validation.details.stockCountValidation is not None
    assert info.validation.details.stockCountValidation.expected is None
    assert "No TOPIX data" in info.validation.warnings
    assert info.storage.backend == "duckdb-parquet"


def test_search_dataset_deduplicates_exact_and_partial_results() -> None:
    class SearchDb:
        def search_stocks(self, term: str, exact: bool, limit: int):
            if exact:
                return [
                    SimpleNamespace(code="7203", company_name="トヨタ"),
                    SimpleNamespace(code="7203", company_name="トヨタ"),
                ]
            return [
                SimpleNamespace(code="7203", company_name="トヨタ"),
                SimpleNamespace(code="6758", company_name="ソニー"),
            ]

    result = dataset_service.search_dataset(cast(Any, SearchDb()), q="ト", limit=50)
    assert [row.code for row in result.results] == ["7203", "6758"]
    assert result.results[0].match_type == "exact"
    assert result.results[1].match_type == "partial"


def test_delete_dataset_removes_snapshot_and_legacy_artifacts(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "sample"
    snapshot_dir.mkdir()
    (snapshot_dir / "dataset.duckdb").write_text("", encoding="utf-8")
    (snapshot_dir / "manifest.v2.json").write_text("{}", encoding="utf-8")
    legacy_db = tmp_path / "sample.db"
    conn = sqlite3.connect(legacy_db)
    conn.execute("CREATE TABLE IF NOT EXISTS dataset_info (key TEXT PRIMARY KEY, value TEXT)")
    conn.close()

    resolver = DatasetResolver(str(tmp_path))

    deleted = dataset_service.delete_dataset(resolver, "sample")

    assert deleted is True
    assert not snapshot_dir.exists()
    assert not legacy_db.exists()
