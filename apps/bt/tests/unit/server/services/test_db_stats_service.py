from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.application.services import db_stats_service
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyMarketDb:
    def __init__(self, prime_codes: set[str] | None = None) -> None:
        self._prime_codes = prime_codes or set()

    def is_initialized(self) -> bool:
        return True

    def get_sync_metadata(self, key: str) -> str | None:
        del key
        return "2026-03-01T00:00:00+00:00"

    def get_stats(self) -> dict[str, int]:
        return {"stocks": 2, "index_master": 1}

    def get_stock_count_by_market(self) -> dict[str, int]:
        return {"プライム": 2}

    def get_prime_codes(self) -> set[str]:
        return set(self._prime_codes)


class DummyStore:
    def __init__(self, inspection: TimeSeriesInspection, duckdb_path: Any = None) -> None:
        self._inspection = inspection
        if duckdb_path is not None:
            self._duckdb_path = duckdb_path

    def inspect(self) -> TimeSeriesInspection:
        return self._inspection


def test_resolve_duckdb_size_bytes_returns_zero_when_path_is_missing() -> None:
    size = db_stats_service._resolve_duckdb_size_bytes(  # type: ignore[arg-type]
        DummyStore(TimeSeriesInspection(source="duckdb-parquet"))
    )
    assert size == 0


def test_resolve_duckdb_size_bytes_returns_file_size(tmp_path: Path) -> None:
    duckdb_file = tmp_path / "market.duckdb"
    duckdb_file.write_bytes(b"abcde")

    size = db_stats_service._resolve_duckdb_size_bytes(  # type: ignore[arg-type]
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

    size = db_stats_service._resolve_duckdb_size_bytes(  # type: ignore[arg-type]
        DummyStore(TimeSeriesInspection(source="duckdb-parquet"), duckdb_path=duckdb_file)
    )
    assert size == 0


def test_get_market_stats_handles_empty_ranges_and_prime_codes() -> None:
    market_db = DummyMarketDb(prime_codes=set())
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

    result = db_stats_service.get_market_stats(  # type: ignore[arg-type]
        market_db=market_db,
        time_series_store=store,  # type: ignore[arg-type]
    )

    assert result.timeSeriesSource == "duckdb-parquet"
    assert result.databaseSize == 0
    assert result.topix.dateRange is None
    assert result.stockData.dateRange is None
    assert result.stockData.averageStocksPerDay == 0
    assert result.indices.dateRange is None
    assert result.fundamentals.primeCoverage.coverageRatio == 0
