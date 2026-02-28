from __future__ import annotations

from pathlib import Path

import pytest

from src.infrastructure.db.market.market_db import MarketDb
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


def test_create_time_series_store_falls_back_to_sqlite_mirror_when_duckdb_unavailable(tmp_path: Path) -> None:
    market_db = MarketDb(str(tmp_path / "market.db"), read_only=False)
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        sqlite_mirror=True,
        market_db=market_db,
    )

    assert store is not None

    published = store.publish_stock_data([_stock_row()])
    assert published == 1
    store.index_stock_data()

    stats = market_db.get_stats()
    assert stats["stock_data"] == 1

    store.close()
    market_db.close()


def test_create_time_series_store_returns_none_without_targets(tmp_path: Path) -> None:
    store = create_time_series_store(
        backend="none",
        duckdb_path=str(tmp_path / "market.duckdb"),
        parquet_dir=str(tmp_path / "parquet"),
        sqlite_mirror=False,
        market_db=None,
    )

    assert store is None


def test_create_time_series_store_can_disable_sqlite_fallback(
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

    market_db = MarketDb(str(tmp_path / "market.db"), read_only=False)
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        sqlite_mirror=False,
        market_db=market_db,
        allow_sqlite_fallback=False,
    )

    assert store is None
    market_db.close()


def test_sqlite_mirror_store_inspect_reports_core_stats(tmp_path: Path) -> None:
    market_db = MarketDb(str(tmp_path / "market.db"), read_only=False)
    market_db.upsert_topix_data(_topix_rows())
    market_db.upsert_stock_data([_stock_row()])
    market_db.upsert_statements(
        [
            {
                "code": "7203",
                "disclosed_date": "2026-02-10",
                "earnings_per_share": 120.0,
            },
            {
                "code": "7203",
                "disclosed_date": "2026-02-11",
                "earnings_per_share": 122.0,
            },
        ]
    )

    store = create_time_series_store(
        backend="sqlite",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        sqlite_mirror=True,
        market_db=market_db,
    )

    assert store is not None
    inspection = store.inspect(
        missing_stock_dates_limit=10,
        statement_non_null_columns=["earnings_per_share"],
    )

    assert inspection.source == "sqlite-mirror"
    assert inspection.topix_count == 2
    assert inspection.stock_count == 1
    assert inspection.stock_date_count == 1
    assert inspection.missing_stock_dates == ["2026-02-11"]
    assert inspection.missing_stock_dates_count == 1
    assert inspection.statements_count == 2
    assert inspection.statement_codes == {"7203"}
    assert inspection.statement_non_null_counts["earnings_per_share"] == 2

    store.close()
    market_db.close()


def test_duckdb_store_inspect_reports_core_stats(tmp_path: Path) -> None:
    market_db = MarketDb(str(tmp_path / "market.db"), read_only=False)
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        sqlite_mirror=False,
        market_db=market_db,
        allow_sqlite_fallback=False,
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
    assert inspection.missing_stock_dates == ["2026-02-11"]
    assert inspection.missing_stock_dates_count == 1
    assert inspection.statements_count == 2
    assert inspection.statement_non_null_counts["earnings_per_share"] == 2
    assert inspection.statement_non_null_counts["profit"] == 1
    assert inspection.statement_non_null_counts["unknown_column"] == 0

    store.close()
    market_db.close()


def test_composite_store_inspect_merges_statement_codes_from_duckdb_and_sqlite(tmp_path: Path) -> None:
    market_db = MarketDb(str(tmp_path / "market.db"), read_only=False)
    market_db.upsert_statements(
        [
            {
                "code": "1301",
                "disclosed_date": "2026-02-09",
                "earnings_per_share": 90.0,
            }
        ]
    )

    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        sqlite_mirror=True,
        market_db=market_db,
        allow_sqlite_fallback=False,
    )

    assert store is not None
    store.publish_topix_data([_topix_rows()[0]])
    store.publish_stock_data([_stock_row()])
    store.publish_indices_data(
        [
            {
                "code": "0040",
                "date": "2026-02-10",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "sector_name": "sector33",
            }
        ]
    )
    store.publish_statements(
        [
            {
                "code": "7203",
                "disclosed_date": "2026-02-10",
                "earnings_per_share": 120.0,
            }
        ]
    )
    store.index_topix_data()
    store.index_stock_data()
    store.index_indices_data()
    store.index_statements()

    inspection = store.inspect(
        missing_stock_dates_limit=10,
        statement_non_null_columns=["earnings_per_share"],
    )

    assert "duckdb" in inspection.source
    assert inspection.statement_codes == {"1301", "7203"}
    assert inspection.statements_count >= 2
    assert inspection.latest_indices_dates["0040"] == "2026-02-10"

    store.close()
    market_db.close()
