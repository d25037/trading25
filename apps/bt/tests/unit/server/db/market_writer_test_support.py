"""Test-only adapters for seeding Market tables through supported writers."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import threading
from typing import Any

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.duckdb_connection import (
    MarketWriterToken,
    _resolve_market_duckdb_temp_directory,
    connect_market_duckdb,
)
from src.infrastructure.db.market.market_operation_lease import MarketOperationLease
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


_TEST_WRITER_LEASES: dict[Path, tuple[MarketOperationLease, int]] = {}
_TEST_WRITER_LEASES_LOCK = threading.Lock()


def _test_writer_token(db_path: str) -> tuple[MarketWriterToken, Callable[[], None]]:
    path = Path(db_path).absolute()
    data_root = path.parent.parent
    data_root.mkdir(parents=True, exist_ok=True)
    with _TEST_WRITER_LEASES_LOCK:
        entry = _TEST_WRITER_LEASES.get(data_root)
        if entry is None:
            lease = MarketOperationLease.acquire(data_root, exclusive=True)
            count = 0
        else:
            lease, count = entry
        _TEST_WRITER_LEASES[data_root] = (lease, count + 1)

    def release() -> None:
        with _TEST_WRITER_LEASES_LOCK:
            current = _TEST_WRITER_LEASES.get(data_root)
            if current is None:
                return
            current_lease, current_count = current
            if current_count > 1:
                _TEST_WRITER_LEASES[data_root] = (current_lease, current_count - 1)
                return
            del _TEST_WRITER_LEASES[data_root]
            current_lease.release()

    return MarketWriterToken._from_writer_factory(lease, path), release


def _bind_release(resource: Any, release: Callable[[], None]) -> Any:
    original_close = resource.close
    released = False

    def close() -> None:
        nonlocal released
        try:
            original_close()
        finally:
            if not released:
                released = True
                release()

    resource.close = close
    return resource


class _LeaseBoundConnection:
    def __init__(self, connection: Any, release: Callable[[], None]) -> None:
        self._connection = connection
        self._release = release
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def __enter__(self) -> "_LeaseBoundConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._connection.close()
        finally:
            if not self._closed:
                self._closed = True
                self._release()


def open_market_db(db_path: str, *, read_only: bool = False) -> MarketDb:
    if read_only:
        return MarketDb(db_path, read_only=True)
    token, release = _test_writer_token(db_path)
    try:
        return _bind_release(
            MarketDb(db_path, read_only=False, writer_token=token),
            release,
        )
    except BaseException:
        release()
        raise


def open_time_series_store(
    *,
    duckdb_path: str,
    parquet_dir: str,
    read_only: bool = False,
) -> DuckDbParquetTimeSeriesStore:
    if read_only:
        return DuckDbParquetTimeSeriesStore(
            duckdb_path=duckdb_path,
            parquet_dir=parquet_dir,
            read_only=True,
        )
    token, release = _test_writer_token(duckdb_path)
    try:
        return _bind_release(
            DuckDbParquetTimeSeriesStore(
                duckdb_path=duckdb_path,
                parquet_dir=parquet_dir,
                read_only=False,
                writer_token=token,
            ),
            release,
        )
    except BaseException:
        release()
        raise


def connect_market_duckdb_for_test(
    db_path: str,
    *,
    read_only: bool = False,
    temp_directory: str | None = None,
) -> Any:
    if not read_only:
        _resolve_market_duckdb_temp_directory(Path(db_path), temp_directory)
    if read_only:
        return connect_market_duckdb(
            db_path,
            read_only=True,
            temp_directory=temp_directory,
        )
    token, release = _test_writer_token(db_path)
    try:
        connection = connect_market_duckdb(
            db_path,
            read_only=False,
            temp_directory=temp_directory,
            writer_token=token,
        )
    except BaseException:
        release()
        raise
    return _LeaseBoundConnection(connection, release)


def create_time_series_store_for_test(
    *,
    backend: str,
    duckdb_path: str,
    parquet_dir: str,
    read_only: bool = False,
) -> MarketTimeSeriesStore | None:
    if read_only:
        return create_time_series_store(
            backend=backend,
            duckdb_path=duckdb_path,
            parquet_dir=parquet_dir,
            read_only=True,
        )
    token, release = _test_writer_token(duckdb_path)
    try:
        store = create_time_series_store(
            backend=backend,
            duckdb_path=duckdb_path,
            parquet_dir=parquet_dir,
            read_only=False,
            writer_token=token,
        )
    except BaseException:
        release()
        raise
    if store is None:
        release()
        return None
    return _bind_release(store, release)


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


def publish_stock_data(
    db: MarketDb,
    rows: list[dict[str, Any]],
    *,
    provider_plan: str = "premium",
) -> SemanticDeltaResult:
    complete_rows = []
    for source in rows:
        row = dict(source)
        row.setdefault("adjustment_factor", 1.0)
        row.setdefault("adjusted_open", row.get("open"))
        row.setdefault("adjusted_high", row.get("high"))
        row.setdefault("adjusted_low", row.get("low"))
        row.setdefault("adjusted_close", row.get("close"))
        row.setdefault("adjusted_volume", row.get("volume"))
        complete_rows.append(row)
    return _publish(
        db,
        complete_rows,
        lambda store, values: store.publish_stock_data(
            values, provider_plan=provider_plan
        ),
    )


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
