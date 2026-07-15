"""Test-only adapters for seeding Market tables through supported writers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.time_series_store import DuckDbParquetTimeSeriesStore


def _publish(
    db: MarketDb,
    rows: list[dict[str, Any]],
    operation: Callable[[DuckDbParquetTimeSeriesStore, list[dict[str, Any]]], int],
) -> int:
    store = DuckDbParquetTimeSeriesStore(
        duckdb_path=db.db_path,
        parquet_dir=str(db.db_path + ".test-parquet"),
    )
    try:
        return operation(store, rows)
    finally:
        store.close()


def publish_stock_data(db: MarketDb, rows: list[dict[str, Any]]) -> int:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_stock_data)


def publish_topix_data(db: MarketDb, rows: list[dict[str, Any]]) -> int:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_topix_data)


def publish_indices_data(db: MarketDb, rows: list[dict[str, Any]]) -> int:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_indices_data)


def publish_options_225_data(db: MarketDb, rows: list[dict[str, Any]]) -> int:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_options_225_data)


def publish_margin_data(db: MarketDb, rows: list[dict[str, Any]]) -> int:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_margin_data)


def publish_statements(db: MarketDb, rows: list[dict[str, Any]]) -> int:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_statements)


def seed_adjusted_statement_metrics(db: MarketDb, rows: list[dict[str, Any]]) -> None:
    placeholders = ", ".join("?" for _ in STATEMENT_METRICS_ADJUSTED_COLUMNS)
    db._executemany(
        "INSERT INTO statement_metrics_adjusted "
        f"({', '.join(STATEMENT_METRICS_ADJUSTED_COLUMNS)}) VALUES ({placeholders})",
        [
            tuple(row.get(column) for column in STATEMENT_METRICS_ADJUSTED_COLUMNS)
            for row in rows
        ],
    )


def seed_daily_valuation(db: MarketDb, rows: list[dict[str, Any]]) -> None:
    placeholders = ", ".join("?" for _ in DAILY_VALUATION_COLUMNS)
    db._executemany(
        "INSERT INTO daily_valuation "
        f"({', '.join(DAILY_VALUATION_COLUMNS)}) VALUES ({placeholders})",
        [tuple(row.get(column) for column in DAILY_VALUATION_COLUMNS) for row in rows],
    )
