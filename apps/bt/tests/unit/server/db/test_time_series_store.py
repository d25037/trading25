from __future__ import annotations

from pathlib import Path

import pytest

from src.infrastructure.db.market.time_series_store import create_time_series_store


def _stock_row() -> dict[str, object]:
    return {
        "code": "7203",
        "date": "2026-02-10",
        "open": 1.0,
        "high": 2.0,
        "low": 1.0,
        "close": 2.0,
        "volume": 100,
        "adjustment_factor": None,
        "created_at": "2026-02-10T00:00:00+00:00",
    }


def _topix_rows() -> list[dict[str, object]]:
    return [
        {"date": "2026-02-10", "open": 1.0, "high": 2.0, "low": 1.0, "close": 2.0},
        {"date": "2026-02-11", "open": 2.0, "high": 3.0, "low": 2.0, "close": 3.0},
    ]


def test_create_time_series_store_returns_none_for_unsupported_backend(tmp_path: Path) -> None:
    store = create_time_series_store(
        backend="sqlite",
        duckdb_path=str(tmp_path / "market.duckdb"),
        parquet_dir=str(tmp_path / "parquet"),
    )
    assert store is None


def test_create_time_series_store_returns_none_when_duckdb_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnavailableDuckDbStore:
        def __init__(self, *, duckdb_path: str, parquet_dir: str) -> None:
            del duckdb_path, parquet_dir
            raise RuntimeError("duckdb unavailable")

    monkeypatch.setattr(
        "src.infrastructure.db.market.time_series_store.DuckDbParquetTimeSeriesStore",
        _UnavailableDuckDbStore,
    )

    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )

    assert store is None


def test_duckdb_store_inspect_reports_core_stats(tmp_path: Path) -> None:
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    store.publish_topix_data(_topix_rows())
    store.publish_stock_data([_stock_row()])
    store.publish_indices_data(
        [
            {
                "code": "0000",
                "date": "2026-02-10",
                "open": 1.0,
                "high": 2.0,
                "low": 1.0,
                "close": 2.0,
                "sector_name": "TOPIX",
            }
        ]
    )
    store.publish_statements(
        [
            {
                "code": "7203",
                "disclosed_date": "2026-02-10",
                "earnings_per_share": 120.0,
                "profit": 1000.0,
            },
            {
                "code": "7203",
                "disclosed_date": "2026-02-11",
                "earnings_per_share": 122.0,
            },
        ]
    )
    store.index_topix_data()
    store.index_stock_data()
    store.index_indices_data()
    store.index_statements()

    inspection = store.inspect(
        missing_stock_dates_limit=10,
        statement_non_null_columns=["earnings_per_share", "profit", "unknown_column"],
    )

    assert inspection.source == "duckdb-parquet"
    assert inspection.topix_count == 2
    assert inspection.stock_count == 1
    assert inspection.stock_date_count == 1
    assert inspection.indices_count == 1
    assert inspection.indices_min == "2026-02-10"
    assert inspection.indices_max == "2026-02-10"
    assert inspection.indices_date_count == 1
    assert inspection.missing_stock_dates == ["2026-02-11"]
    assert inspection.missing_stock_dates_count == 1
    assert inspection.statements_count == 2
    assert inspection.statement_codes == {"7203"}
    assert inspection.statement_non_null_counts["earnings_per_share"] == 2
    assert inspection.statement_non_null_counts["profit"] == 1
    assert inspection.statement_non_null_counts["unknown_column"] == 0

    store.close()
