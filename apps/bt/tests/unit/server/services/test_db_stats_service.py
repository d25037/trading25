from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.application.services import db_stats_service
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyMarketDb:
    def __init__(
        self,
        fundamentals_target_codes: set[str] | None = None,
    ) -> None:
        self._fundamentals_target_codes = fundamentals_target_codes or set()

    def is_initialized(self) -> bool:
        return True

    def get_sync_metadata(self, key: str) -> str | None:
        del key
        return "2026-03-01T00:00:00+00:00"

    def get_stats(self) -> dict[str, int]:
        return {"stocks": 2, "index_master": 1}

    def get_stock_count_by_market(self) -> dict[str, int]:
        return {"プライム": 2}

    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]:
        return [
            {
                "code": code,
                "company_name": "",
                "market_code": "0111",
            }
            for code in sorted(self._fundamentals_target_codes)
        ]

    def get_index_master_category_counts(self) -> dict[str, int]:
        return {"sector33": 1}


class DummyStore:
    def __init__(
        self,
        inspection: TimeSeriesInspection,
        duckdb_path: Any = None,
        parquet_dir: Any = None,
        storage_stats: Any = None,
    ) -> None:
        self._inspection = inspection
        self.storage_stats_calls = 0
        if duckdb_path is not None:
            self._duckdb_path = duckdb_path
        if parquet_dir is not None:
            self._parquet_dir = parquet_dir
        self._storage_stats = storage_stats

    def inspect(self) -> TimeSeriesInspection:
        return self._inspection

    def get_storage_stats(self) -> Any:
        self.storage_stats_calls += 1
        return self._storage_stats


def test_resolve_duckdb_size_bytes_returns_zero_when_path_is_missing() -> None:
    size = db_stats_service._resolve_duckdb_size_bytes(
        DummyStore(TimeSeriesInspection(source="duckdb-parquet"))
    )
    assert size == 0


def test_resolve_duckdb_size_bytes_returns_file_size(tmp_path: Path) -> None:
    duckdb_file = tmp_path / "market.duckdb"
    duckdb_file.write_bytes(b"abcde")

    size = db_stats_service._resolve_duckdb_size_bytes(
        DummyStore(TimeSeriesInspection(source="duckdb-parquet"), duckdb_path=duckdb_file)
    )
    assert size == 5


def test_resolve_duckdb_size_bytes_handles_oserror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duckdb_file = tmp_path / "market.duckdb"
    duckdb_file.write_bytes(b"abc")

    def _raise_exists(self: Path) -> bool:  # noqa: ARG001
        raise OSError("stat failed")

    monkeypatch.setattr(Path, "exists", _raise_exists)

    size = db_stats_service._resolve_duckdb_size_bytes(
        DummyStore(TimeSeriesInspection(source="duckdb-parquet"), duckdb_path=duckdb_file)
    )
    assert size == 0


def test_resolve_parquet_size_bytes_returns_directory_total(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()
    (parquet_dir / "stock_data.parquet").write_bytes(b"abc")
    nested_dir = parquet_dir / "nested"
    nested_dir.mkdir()
    (nested_dir / "margin_data.parquet").write_bytes(b"12345")
    (nested_dir / "ignore.txt").write_text("ignored")

    size = db_stats_service._resolve_parquet_size_bytes(
        DummyStore(TimeSeriesInspection(source="duckdb-parquet"), parquet_dir=parquet_dir)
    )

    assert size == 8


def test_resolve_storage_stats_prefers_single_atomic_store_lookup() -> None:
    store = DummyStore(
        TimeSeriesInspection(source="duckdb-parquet"),
        storage_stats=SimpleNamespace(duckdb_bytes=7, parquet_bytes=11),
    )

    stats = db_stats_service._resolve_storage_stats(store)

    assert stats.duckdbBytes == 7
    assert stats.parquetBytes == 11
    assert stats.totalBytes == 18
    assert store.storage_stats_calls == 1


def test_get_market_stats_handles_empty_ranges_and_fundamentals_target_codes() -> None:
    market_db = DummyMarketDb(fundamentals_target_codes=set())
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=0,
        stock_count=0,
        stock_date_count=0,
        indices_count=0,
        indices_date_count=0,
        statements_count=0,
        statement_codes=set(),
    )
    store = DummyStore(inspection)

    result = db_stats_service.get_market_stats(
        market_db=market_db,
        time_series_store=store,
    )

    assert result.timeSeriesSource == "duckdb-parquet"
    assert result.databaseSize == 0
    assert result.storage.totalBytes == 0
    assert result.topix.dateRange is None
    assert result.stockData.dateRange is None
    assert result.stockData.averageStocksPerDay == 0
    assert result.indices.dateRange is None
    assert result.indices.byCategory == {"sector33": 1}
    assert result.options225.count == 0
    assert result.options225.dateRange is None
    assert result.margin.count == 0
    assert result.margin.dateRange is None
    assert result.fundamentals.listedMarketCoverage.coverageRatio == 0
    assert result.fundamentals.listedMarketCoverage.issuerAliasCoveredCount == 0


def test_get_market_stats_includes_options_225_ranges() -> None:
    market_db = DummyMarketDb()
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        options_225_count=3,
        options_225_min="2024-01-16",
        options_225_max="2024-01-17",
        options_225_date_count=2,
        statement_codes=set(),
    )

    result = db_stats_service.get_market_stats(
        market_db=market_db,
        time_series_store=DummyStore(inspection),
    )

    assert result.options225.count == 3
    assert result.options225.dateCount == 2
    assert result.options225.dateRange is not None
    assert result.options225.dateRange.min == "2024-01-16"
    assert result.options225.dateRange.max == "2024-01-17"
