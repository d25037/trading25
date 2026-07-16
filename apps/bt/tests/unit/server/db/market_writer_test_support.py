"""Test-only adapters for seeding Market tables through supported writers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.duckdb_connection import (
    issue_market_writer_token,
    connect_market_duckdb,
)
from src.infrastructure.db.market.market_schema import (
    DAILY_VALUATION_COLUMNS,
    STATEMENT_METRICS_ADJUSTED_COLUMNS,
)
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult
from src.infrastructure.db.market.time_series_store import (
    DuckDbParquetTimeSeriesStore,
    MarketTimeSeriesStore,
    create_time_series_store,
)


def open_market_db(db_path: str, *, read_only: bool = False) -> MarketDb:
    return MarketDb(
        db_path,
        read_only=read_only,
        writer_token=(None if read_only else issue_market_writer_token()),
    )


def open_time_series_store(
    *,
    duckdb_path: str,
    parquet_dir: str,
    read_only: bool = False,
) -> DuckDbParquetTimeSeriesStore:
    return DuckDbParquetTimeSeriesStore(
        duckdb_path=duckdb_path,
        parquet_dir=parquet_dir,
        read_only=read_only,
        writer_token=(None if read_only else issue_market_writer_token()),
    )


def connect_market_duckdb_for_test(
    db_path: str,
    *,
    read_only: bool = False,
    temp_directory: str | None = None,
) -> Any:
    return connect_market_duckdb(
        db_path,
        read_only=read_only,
        temp_directory=temp_directory,
        writer_token=(None if read_only else issue_market_writer_token()),
    )


def create_time_series_store_for_test(
    *,
    backend: str,
    duckdb_path: str,
    parquet_dir: str,
    read_only: bool = False,
) -> MarketTimeSeriesStore | None:
    return create_time_series_store(
        backend=backend,
        duckdb_path=duckdb_path,
        parquet_dir=parquet_dir,
        read_only=read_only,
        writer_token=(None if read_only else issue_market_writer_token()),
    )


def _publish(
    db: MarketDb,
    rows: list[dict[str, Any]],
    operation: Callable[
        [DuckDbParquetTimeSeriesStore, list[dict[str, Any]]], SemanticDeltaResult
    ],
) -> SemanticDeltaResult:
    store = open_time_series_store(
        duckdb_path=db.db_path,
        parquet_dir=str(db.db_path + ".test-parquet"),
    )
    try:
        return operation(store, rows)
    finally:
        store.close()


def publish_stock_data(db: MarketDb, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_stock_data)


def publish_topix_data(db: MarketDb, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_topix_data)


def publish_indices_data(db: MarketDb, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_indices_data)


def publish_options_225_data(db: MarketDb, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_options_225_data)


def publish_margin_data(db: MarketDb, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
    return _publish(db, rows, DuckDbParquetTimeSeriesStore.publish_margin_data)


def publish_statements(db: MarketDb, rows: list[dict[str, Any]]) -> SemanticDeltaResult:
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
