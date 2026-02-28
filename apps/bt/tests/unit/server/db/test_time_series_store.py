from __future__ import annotations

from pathlib import Path

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
