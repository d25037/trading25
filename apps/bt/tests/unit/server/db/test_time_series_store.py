from __future__ import annotations

import builtins
import os
import hashlib
from pathlib import Path
from threading import Barrier, Lock, RLock, Thread
from time import sleep

import duckdb
import pytest

from src.infrastructure.db.market.time_series_store import DuckDbParquetTimeSeriesStore
from tests.unit.server.db.market_writer_test_support import (
    connect_market_duckdb_for_test,
    create_time_series_store_for_test,
    open_time_series_store,
)


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


def _stock_row_for(date: str) -> dict[str, object]:
    row = _stock_row()
    row["date"] = date
    row["created_at"] = f"{date}T00:00:00+00:00"
    return row


def _stock_minute_row(
    *,
    code: str = "7203",
    date: str = "2026-02-10",
    time: str = "09:00",
) -> dict[str, object]:
    return {
        "code": code,
        "date": date,
        "time": time,
        "open": 1.0,
        "high": 2.0,
        "low": 1.0,
        "close": 2.0,
        "volume": 100,
        "turnover_value": 200.0,
        "created_at": f"{date}T00:00:00+00:00",
    }


def _query_rows(db_path: Path, sql: str) -> list[tuple]:
    conn = duckdb.connect(str(db_path))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _topix_rows() -> list[dict[str, object]]:
    return [
        {"date": "2026-02-10", "open": 1.0, "high": 2.0, "low": 1.0, "close": 2.0},
        {"date": "2026-02-11", "open": 2.0, "high": 3.0, "low": 2.0, "close": 3.0},
    ]


def _indices_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "0000",
            "date": "2026-02-10",
            "open": 1.0,
            "high": 2.0,
            "low": 1.0,
            "close": 2.0,
            "sector_name": "TOPIX",
            "created_at": "2026-02-10T00:00:00+00:00",
        },
        {
            "code": "0000",
            "date": "2026-02-11",
            "open": 2.0,
            "high": 3.0,
            "low": 2.0,
            "close": 3.0,
            "sector_name": "TOPIX",
            "created_at": "2026-02-11T00:00:00+00:00",
        },
    ]


def _options_225_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "131040018",
            "date": "2026-02-10",
            "contract_month": "2026-04",
            "strike_price": 32000.0,
            "put_call_division": "1",
            "underlying_price": 39000.0,
            "created_at": "2026-02-10T00:00:00+00:00",
        },
        {
            "code": "141040018",
            "date": "2026-02-11",
            "contract_month": "2026-04",
            "strike_price": 36000.0,
            "put_call_division": "2",
            "underlying_price": 39200.0,
            "created_at": "2026-02-11T00:00:00+00:00",
        },
    ]


def _margin_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "7203",
            "date": "2026-02-07",
            "long_margin_volume": 900.0,
            "short_margin_volume": 120.0,
        },
        {
            "code": "7203",
            "date": "2026-02-10",
            "long_margin_volume": 1000.0,
            "short_margin_volume": 150.0,
        },
    ]


def _statement_rows() -> list[dict[str, object]]:
    return [
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


def test_create_time_series_store_returns_none_for_unsupported_backend(
    tmp_path: Path,
) -> None:
    store = create_time_series_store_for_test(
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

    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )

    assert store is None


def test_duckdb_connection_uses_managed_temp_directory(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    try:
        temp_directory = store._conn.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()[0]
    finally:
        store.close()

    assert temp_directory == str(tmp_path / "market-timeseries" / "duckdb-tmp")


def test_duckdb_connection_honors_isolated_temp_directory_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_root = tmp_path / "market-timeseries"
    isolated_relative = ".cutover-runtime-smoke/duckdb-tmp"
    isolated_temp = market_root / isolated_relative
    monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", "retained_market_smoke")
    monkeypatch.setenv("TRADING25_DUCKDB_TEMP_DIR", isolated_relative)

    store = open_time_series_store(
        duckdb_path=str(market_root / "market.duckdb"),
        parquet_dir=str(market_root / "parquet"),
    )
    try:
        temp_directory = store._conn.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()[0]
    finally:
        store.close()

    assert temp_directory == str(isolated_temp)
    assert isolated_temp.is_dir()
    assert not (market_root / "duckdb-tmp").exists()


@pytest.mark.parametrize(
    ("capability", "ambient"),
    [
        (None, "/tmp/hostile-duckdb-temp"),
        ("wrong", ".cutover-runtime-smoke/duckdb-tmp"),
        (None, "../../hostile-duckdb-temp"),
    ],
)
def test_duckdb_connection_ignores_ambient_temp_without_owned_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capability: str | None,
    ambient: str,
) -> None:
    market_root = tmp_path / "market-timeseries"
    market_root.mkdir()
    if capability is not None:
        monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", capability)
    monkeypatch.setenv("TRADING25_DUCKDB_TEMP_DIR", ambient)

    conn = connect_market_duckdb_for_test(market_root / "market.duckdb")
    try:
        configured = conn.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()[0]
    finally:
        conn.close()

    assert configured == str(market_root / "duckdb-tmp")
    assert not (tmp_path / "hostile-duckdb-temp").exists()


@pytest.mark.parametrize(
    "ambient",
    [
        "",
        ".",
        "..",
        "/tmp/duckdb-tmp",
        ".cutover-runtime-smoke",
        ".cutover-runtime-smoke/../duckdb-tmp",
        ".cutover-runtime-smoke//duckdb-tmp",
        ".cutover-runtime-/duckdb-tmp",
        ".cutover-runtime-smoke/duckdb-tmp/extra",
    ],
)
def test_owned_duckdb_temp_rejects_noncanonical_ambient_path_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ambient: str,
) -> None:
    market_root = tmp_path / "market-timeseries"
    market_root.mkdir()
    monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", "retained_market_smoke")
    monkeypatch.setenv("TRADING25_DUCKDB_TEMP_DIR", ambient)

    before = tuple(tmp_path.rglob("*"))
    with pytest.raises(ValueError, match="DuckDB temp"):
        connect_market_duckdb_for_test(market_root / "market.duckdb")

    assert tuple(tmp_path.rglob("*")) == before


@pytest.mark.parametrize("collision", ["ancestor_symlink", "leaf_symlink", "leaf_fifo"])
def test_owned_duckdb_temp_rejects_non_directory_path_components(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collision: str,
) -> None:
    market_root = tmp_path / "market-timeseries"
    market_root.mkdir()
    runtime = market_root / ".cutover-runtime-smoke"
    outside = tmp_path / "outside"
    outside.mkdir()
    if collision == "ancestor_symlink":
        runtime.symlink_to(outside, target_is_directory=True)
    else:
        runtime.mkdir()
        leaf = runtime / "duckdb-tmp"
        if collision == "leaf_symlink":
            leaf.symlink_to(outside, target_is_directory=True)
        else:
            os.mkfifo(leaf)
    monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", "retained_market_smoke")
    monkeypatch.setenv(
        "TRADING25_DUCKDB_TEMP_DIR",
        ".cutover-runtime-smoke/duckdb-tmp",
    )

    with pytest.raises(ValueError, match="real director"):
        connect_market_duckdb_for_test(market_root / "market.duckdb")

    assert not (outside / "duckdb-tmp").exists()


def test_explicit_duckdb_temp_is_authoritative_over_hostile_ambient(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_root = tmp_path / "market-timeseries"
    market_root.mkdir()
    explicit = tmp_path / "explicit" / "duckdb-tmp"
    monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", "retained_market_smoke")
    monkeypatch.setenv("TRADING25_DUCKDB_TEMP_DIR", "../../hostile")

    conn = connect_market_duckdb_for_test(
        market_root / "market.duckdb",
        temp_directory=explicit,
    )
    try:
        configured = conn.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()[0]
    finally:
        conn.close()

    assert configured == str(explicit)
    assert explicit.is_dir()
    assert not (tmp_path.parent / "hostile").exists()


def test_get_storage_stats_includes_duckdb_free_blocks(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    try:
        store.publish_topix_data(_topix_rows())
        store.index_topix_data()

        stats = store.get_storage_stats()
    finally:
        store.close()

    assert stats.duckdb_blocks_total >= stats.duckdb_blocks_used
    assert stats.duckdb_blocks_free >= 0
    assert stats.duckdb_bytes_free >= 0


def test_index_topix_data_emits_export_telemetry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, object]] = []

    def _capture_info(_message: str, **kwargs: object) -> None:
        events.append(kwargs)

    monkeypatch.setattr(
        "src.infrastructure.db.market.time_series_store.logger.info",
        _capture_info,
    )
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    try:
        store.publish_topix_data(_topix_rows())
        store.index_topix_data()
    finally:
        store.close()

    export_events = [
        event
        for event in events
        if event.get("event") == "market_store_phase_timing"
        and event.get("operation") == "parquet_export"
        and event.get("table") == "topix_data"
    ]
    assert export_events
    assert isinstance(export_events[0].get("elapsedMs"), float)
    assert export_events[0]["rows"] == 2


def test_duckdb_store_inspect_reports_core_stats(tmp_path: Path) -> None:
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    store.publish_topix_data(_topix_rows())
    store.publish_stock_data([_stock_row()])
    store.publish_stock_minute_data(
        [
            _stock_minute_row(date="2026-02-10", time="09:00"),
            _stock_minute_row(date="2026-02-10", time="09:01"),
            _stock_minute_row(date="2026-02-11", time="15:30"),
        ]
    )
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
    store.publish_options_225_data(_options_225_rows())
    store.publish_margin_data(
        [
            {
                "code": "7203",
                "date": "2026-02-07",
                "long_margin_volume": 900.0,
                "short_margin_volume": 120.0,
            },
            {
                "code": "7203",
                "date": "2026-02-10",
                "long_margin_volume": 1000.0,
                "short_margin_volume": 150.0,
            },
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
    store.index_stock_minute_data()
    store.index_indices_data()
    store.index_options_225_data()
    store.index_margin_data()
    store.index_statements()

    inspection = store.inspect(
        missing_stock_dates_limit=10,
        missing_options_225_dates_limit=10,
        statement_non_null_columns=["earnings_per_share", "profit", "unknown_column"],
    )

    assert inspection.source == "duckdb-parquet"
    assert inspection.topix_count == 2
    assert inspection.stock_count == 1
    assert inspection.stock_date_count == 1
    assert inspection.stock_minute_count == 3
    assert inspection.stock_minute_min == "2026-02-10"
    assert inspection.stock_minute_max == "2026-02-11"
    assert inspection.stock_minute_date_count == 2
    assert inspection.stock_minute_code_count == 1
    assert inspection.latest_stock_minute_time == "15:30"
    assert inspection.indices_count == 1
    assert inspection.indices_min == "2026-02-10"
    assert inspection.indices_max == "2026-02-10"
    assert inspection.indices_date_count == 1
    assert inspection.options_225_count == 2
    assert inspection.options_225_min == "2026-02-10"
    assert inspection.options_225_max == "2026-02-11"
    assert inspection.options_225_date_count == 2
    assert inspection.latest_options_225_date == "2026-02-11"
    assert inspection.missing_options_225_dates == []
    assert inspection.missing_options_225_dates_count == 0
    assert inspection.margin_count == 2
    assert inspection.margin_min == "2026-02-07"
    assert inspection.margin_max == "2026-02-10"
    assert inspection.margin_date_count == 2
    assert inspection.margin_codes == {"7203"}
    assert inspection.missing_stock_dates == ["2026-02-11"]
    assert inspection.missing_stock_dates_count == 1
    assert inspection.statements_count == 2
    assert inspection.statement_codes == {"7203"}
    assert inspection.statement_non_null_counts["earnings_per_share"] == 2
    assert inspection.statement_non_null_counts["profit"] == 1
    assert inspection.statement_non_null_counts["unknown_column"] == 0

    store.close()


def test_publish_statements_persists_forecast_sales_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "market-timeseries" / "market.duckdb"
    store = open_time_series_store(
        duckdb_path=str(db_path),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    try:
        assert store.publish_statements(
            [
                {
                    "code": "7203",
                    "disclosed_date": "2026-05-08",
                    "sales": 50_684_952_000_000.0,
                    "forecast_sales": None,
                    "next_year_forecast_sales": 51_000_000_000_000.0,
                },
                {
                    "code": "7203",
                    "disclosed_date": "2026-08-07",
                    "sales": 12_253_326_000_000.0,
                    "forecast_sales": 48_500_000_000_000.0,
                    "next_year_forecast_sales": None,
                },
            ]
        ).stats.inserted == 2
    finally:
        store.close()

    rows = _query_rows(
        db_path,
        """
        SELECT code, disclosed_date, sales, forecast_sales, next_year_forecast_sales
        FROM statements
        ORDER BY disclosed_date
        """,
    )

    assert rows == [
        ("7203", "2026-05-08", 50_684_952_000_000.0, None, 51_000_000_000_000.0),
        ("7203", "2026-08-07", 12_253_326_000_000.0, 48_500_000_000_000.0, None),
    ]


def test_index_options_225_data_exports_partitioned_parquet(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "market-timeseries" / "parquet"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(parquet_dir),
    )
    assert store is not None

    store.publish_options_225_data(_options_225_rows())
    store.index_options_225_data()

    assert (parquet_dir / "options_225_data" / "date=2026-02-10" / "data.parquet").exists()
    assert (parquet_dir / "options_225_data" / "date=2026-02-11" / "data.parquet").exists()
    assert not (parquet_dir / "options_225_data.parquet").exists()

    store.close()


def test_index_stock_data_exports_large_tables_by_dirty_date(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "market-timeseries" / "parquet"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(parquet_dir),
    )
    assert store is not None

    store.publish_stock_data([_stock_row_for("2026-02-10"), _stock_row_for("2026-02-11")])
    store.index_stock_data()

    assert (parquet_dir / "stock_data_raw" / "date=2026-02-10" / "data.parquet").exists()
    assert (parquet_dir / "stock_data_raw" / "date=2026-02-11" / "data.parquet").exists()
    assert (parquet_dir / "stock_data" / "date=2026-02-10" / "data.parquet").exists()
    assert (parquet_dir / "stock_data" / "date=2026-02-11" / "data.parquet").exists()
    assert not (parquet_dir / "stock_data_raw.parquet").exists()
    assert not (parquet_dir / "stock_data.parquet").exists()

    store.close()


def test_staged_stock_data_flushes_once_and_projects_rows(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    try:
        assert store.stage_stock_data_rows(
            [_stock_row_for("2026-02-10")]
        ).stats.input == 1
        assert store.stage_stock_data_rows(
            [_stock_row_for("2026-02-11")]
        ).stats.input == 1
        assert _query_rows(
            tmp_path / "market-timeseries" / "market.duckdb",
            "SELECT COUNT(*) FROM stock_data_raw",
        ) == [(0,)]

        assert store.flush_staged_stock_data().stats.inserted == 2

        assert _query_rows(
            tmp_path / "market-timeseries" / "market.duckdb",
            "SELECT COUNT(*), MIN(date), MAX(date) FROM stock_data_raw",
        ) == [(2, "2026-02-10", "2026-02-11")]
        assert _query_rows(
            tmp_path / "market-timeseries" / "market.duckdb",
            "SELECT COUNT(*), MIN(date), MAX(date) FROM stock_data",
        ) == [(2, "2026-02-10", "2026-02-11")]
    finally:
        store.close()


def test_index_stock_minute_data_exports_partitioned_parquet(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "market-timeseries" / "parquet"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(parquet_dir),
    )
    assert store is not None

    store.publish_stock_minute_data(
        [
            _stock_minute_row(date="2026-02-10", time="09:00"),
            _stock_minute_row(date="2026-02-11", time="15:30"),
        ]
    )
    store.index_stock_minute_data()

    assert (parquet_dir / "stock_data_minute_raw" / "date=2026-02-10" / "data.parquet").exists()
    assert (parquet_dir / "stock_data_minute_raw" / "date=2026-02-11" / "data.parquet").exists()

    store.close()


def test_publish_stock_data_batch_uses_semantic_delta_kernel(tmp_path: Path) -> None:
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    mutation = store.publish_stock_data(
        [_stock_row_for("2026-02-10"), _stock_row_for("2026-02-11")]
    )

    inspection = store.inspect()

    assert mutation.stats.inserted == 2
    assert inspection.stock_count == 2
    assert inspection.stock_min == "2026-02-10"
    assert inspection.stock_max == "2026-02-11"

    store.close()


def test_publish_stock_data_directly_projects_append_rows_without_future_adjustments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "market-timeseries" / "market.duckdb"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(db_path),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    projection_calls = 0
    original_project = DuckDbParquetTimeSeriesStore._project_stock_rows

    def _spy_project(self: DuckDbParquetTimeSeriesStore, rows: list[dict[str, object]]) -> None:
        nonlocal projection_calls
        projection_calls += 1
        original_project(self, rows)

    monkeypatch.setattr(DuckDbParquetTimeSeriesStore, "_project_stock_rows", _spy_project)

    store.publish_stock_data([_stock_row_for("2026-02-10")])

    rows = _query_rows(
        db_path,
        """
        SELECT code, date, open, high, low, close, volume, adjustment_factor
        FROM stock_data
        ORDER BY code, date
        """,
    )

    assert projection_calls == 0
    assert rows == [("7203", "2026-02-10", 1.0, 2.0, 1.0, 2.0, 100, None)]

    store.close()


def test_index_stock_data_projects_split_adjustments_from_raw_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "market-timeseries" / "market.duckdb"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(db_path),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    store.publish_stock_data(
        [
            {
                "code": "7203",
                "date": "2026-02-05",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.0,
                "volume": 1000,
                "adjustment_factor": 1.0,
                "created_at": "2026-02-05T00:00:00+00:00",
            },
            {
                "code": "7203",
                "date": "2026-02-06",
                "open": 5.0,
                "high": 6.0,
                "low": 4.0,
                "close": 5.0,
                "volume": 1000,
                "adjustment_factor": 0.5,
                "created_at": "2026-02-06T00:00:00+00:00",
            },
        ]
    )
    store.index_stock_data()

    rows = _query_rows(
        db_path,
        """
        SELECT date, open, high, low, close, volume, adjustment_factor
        FROM stock_data
        WHERE code = '7203'
        ORDER BY date
        """,
    )

    assert rows == [
        ("2026-02-05", 5.0, 5.5, 4.5, 5.0, 2000, 1.0),
        ("2026-02-06", 5.0, 6.0, 4.0, 5.0, 1000, 0.5),
    ]

    raw_rows = _query_rows(
        db_path,
        """
        SELECT date, open, close, volume, adjustment_factor
        FROM stock_data_raw
        WHERE code = '7203'
        ORDER BY date
        """,
    )
    assert raw_rows == [
        ("2026-02-05", 10.0, 10.0, 1000, 1.0),
        ("2026-02-06", 5.0, 5.0, 1000, 0.5),
    ]

    store.close()


def test_index_stock_data_projects_chained_adjustments_without_boundary_gap(tmp_path: Path) -> None:
    db_path = tmp_path / "market-timeseries" / "market.duckdb"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(db_path),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    store.publish_stock_data(
        [
            {
                "code": "9984",
                "date": "2016-03-23",
                "open": 5600.0,
                "high": 5650.0,
                "low": 5550.0,
                "close": 5604.0,
                "volume": 1000,
                "adjustment_factor": 1.0,
                "created_at": "2016-03-23T00:00:00+00:00",
            },
            {
                "code": "9984",
                "date": "2016-03-24",
                "open": 5600.0,
                "high": 5680.0,
                "low": 5520.0,
                "close": 5590.4,
                "volume": 2000,
                "adjustment_factor": 1.0,
                "created_at": "2016-03-24T00:00:00+00:00",
            },
            {
                "code": "9984",
                "date": "2019-06-25",
                "open": 600.0,
                "high": 610.0,
                "low": 590.0,
                "close": 605.0,
                "volume": 3000,
                "adjustment_factor": 0.5,
                "created_at": "2019-06-25T00:00:00+00:00",
            },
            {
                "code": "9984",
                "date": "2025-12-29",
                "open": 150.0,
                "high": 160.0,
                "low": 140.0,
                "close": 155.0,
                "volume": 4000,
                "adjustment_factor": 0.25,
                "created_at": "2025-12-29T00:00:00+00:00",
            },
        ]
    )
    store.index_stock_data()

    rows = _query_rows(
        db_path,
        """
        SELECT date, ROUND(close, 1), volume
        FROM stock_data
        WHERE code = '9984'
          AND date IN ('2016-03-23', '2016-03-24')
        ORDER BY date
        """,
    )

    assert rows == [
        ("2016-03-23", 700.5, 8000),
        ("2016-03-24", 698.8, 16000),
    ]

    store.close()


def test_publish_indices_data_batch_uses_semantic_delta_kernel(tmp_path: Path) -> None:
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    mutation = store.publish_indices_data(_indices_rows())

    inspection = store.inspect()

    assert mutation.stats.inserted == 2
    assert inspection.indices_count == 2
    assert inspection.indices_min == "2026-02-10"
    assert inspection.indices_max == "2026-02-11"

    store.close()


def test_publish_options_225_data_batch_uses_semantic_delta_kernel(tmp_path: Path) -> None:
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    mutation = store.publish_options_225_data(_options_225_rows())

    inspection = store.inspect()

    assert mutation.stats.inserted == 2
    assert inspection.options_225_count == 2
    assert inspection.options_225_min == "2026-02-10"
    assert inspection.options_225_max == "2026-02-11"

    store.close()


def test_publish_margin_data_batch_uses_semantic_delta_kernel(tmp_path: Path) -> None:
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    mutation = store.publish_margin_data(_margin_rows())

    inspection = store.inspect()

    assert mutation.stats.inserted == 2
    assert inspection.margin_count == 2
    assert inspection.margin_min == "2026-02-07"
    assert inspection.margin_max == "2026-02-10"
    assert inspection.margin_codes == {"7203"}

    store.close()


def test_publish_statements_batch_preserves_non_null_merge(tmp_path: Path) -> None:
    db_path = tmp_path / "market-timeseries" / "market.duckdb"
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(db_path),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    store.publish_statements(_statement_rows())
    store.publish_statements(
        [
            {
                "code": "7203",
                "disclosed_date": "2026-02-10",
                "earnings_per_share": None,
                "profit": 1100.0,
            }
        ]
    )

    rows = _query_rows(
        db_path,
        """
        SELECT disclosed_date, earnings_per_share, profit
        FROM statements
        ORDER BY disclosed_date
        """,
    )

    assert rows == [
        ("2026-02-10", 120.0, 1100.0),
        ("2026-02-11", 122.0, None),
    ]

    store.close()


def test_publish_topix_data_excludes_flat_row_equal_to_previous_close(
    tmp_path: Path,
) -> None:
    store = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    published = store.publish_topix_data(
        [
            {
                "date": "2020-09-30",
                "open": 1650.32,
                "high": 1654.18,
                "low": 1625.49,
                "close": 1625.49,
                "created_at": "2026-03-05T00:00:00+00:00",
            },
            {
                "date": "2020-10-01",
                "open": 1625.49,
                "high": 1625.49,
                "low": 1625.49,
                "close": 1625.49,
                "created_at": "2026-03-05T00:00:00+00:00",
            },
            {
                "date": "2020-10-02",
                "open": 1633.02,
                "high": 1638.80,
                "low": 1603.32,
                "close": 1609.22,
                "created_at": "2026-03-05T00:00:00+00:00",
            },
        ]
    )
    assert published.stats.input == 3
    assert published.stats.inserted == 2

    store.publish_stock_data(
        [
            _stock_row_for("2020-09-30"),
            _stock_row_for("2020-10-02"),
        ]
    )
    inspection = store.inspect(missing_stock_dates_limit=10)

    assert inspection.topix_count == 2
    assert inspection.topix_min == "2020-09-30"
    assert inspection.topix_max == "2020-10-02"
    assert inspection.missing_stock_dates_count == 0
    assert inspection.missing_stock_dates == []

    store.close()


def test_store_startup_cleans_existing_invalid_topix_rows(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "market-timeseries" / "market.duckdb"
    parquet_dir = tmp_path / "market-timeseries" / "parquet"

    first = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(duckdb_path),
        parquet_dir=str(parquet_dir),
    )
    assert isinstance(first, DuckDbParquetTimeSeriesStore)

    first._conn.executemany(
        """
        INSERT INTO topix_data (date, open, high, low, close, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "2020-09-30",
                1650.32,
                1654.18,
                1625.49,
                1625.49,
                "2026-03-05T00:00:00+00:00",
            ),
            (
                "2020-10-01",
                1625.49,
                1625.49,
                1625.49,
                1625.49,
                "2026-03-05T00:00:00+00:00",
            ),
            (
                "2020-10-02",
                1633.02,
                1638.80,
                1603.32,
                1609.22,
                "2026-03-05T00:00:00+00:00",
            ),
        ],
    )
    first.close()

    second = create_time_series_store_for_test(
        backend="duckdb-parquet",
        duckdb_path=str(duckdb_path),
        parquet_dir=str(parquet_dir),
    )
    assert second is not None
    inspection = second.inspect(missing_stock_dates_limit=10)

    assert inspection.topix_count == 2
    assert inspection.topix_min == "2020-09-30"
    assert inspection.topix_max == "2020-10-02"

    second.close()


class _ResultCursor:
    def __init__(
        self,
        *,
        one: tuple[object, ...] | None = None,
        many: list[tuple[object, ...]] | None = None,
    ) -> None:
        self._one = one
        self._many = many or []

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._many)


class _ConcurrentAccessDetectingConnection:
    """同時アクセスを検知して例外化するテスト用接続。"""

    def __init__(self) -> None:
        self._guard = Lock()
        self.closed = False

    def execute(self, sql: str, params: list[int] | None = None) -> _ResultCursor:
        del params
        self._enter_critical()
        try:
            return self._cursor_for(sql)
        finally:
            self._exit_critical()

    def executemany(self, sql: str, _values: list[tuple[object, ...]]) -> None:
        del sql
        self._enter_critical()
        try:
            return
        finally:
            self._exit_critical()

    def register(self, _name: str, _value: object) -> None:
        self._enter_critical()
        try:
            return
        finally:
            self._exit_critical()

    def unregister(self, _name: str) -> None:
        self._enter_critical()
        try:
            return
        finally:
            self._exit_critical()

    def close(self) -> None:
        self.closed = True

    def _enter_critical(self) -> None:
        if not self._guard.acquire(blocking=False):
            raise RuntimeError("concurrent access detected")
        sleep(0.001)

    def _exit_critical(self) -> None:
        self._guard.release()

    @staticmethod
    def _cursor_for(sql: str) -> _ResultCursor:
        if "END AS delta_kind" in sql:
            return _ResultCursor(many=[("7203", "2026-01-01", "inserted")])
        if "FROM topix_data" in sql and "MAX(date)" in sql:
            return _ResultCursor(one=(0, None, None))
        if "FROM stock_data_minute_raw" in sql and "COUNT(DISTINCT date)" in sql:
            return _ResultCursor(one=(0, None, None, 0, 0))
        if "FROM stock_data_minute_raw" in sql and "ORDER BY date DESC, time DESC" in sql:
            return _ResultCursor(one=None)
        if "FROM stock_data" in sql and "COUNT(DISTINCT date)" in sql:
            return _ResultCursor(one=(0, None, None, 0))
        if "FROM indices_data" in sql and "COUNT(DISTINCT date)" in sql:
            return _ResultCursor(one=(0, None, None, 0))
        if "FROM indices_data" in sql and "GROUP BY code" in sql:
            return _ResultCursor(many=[])
        if "FROM statements" in sql and "MAX(disclosed_date)" in sql:
            return _ResultCursor(one=(0, None))
        if "LEFT JOIN (SELECT DISTINCT date FROM stock_data)" in sql:
            return _ResultCursor(one=(0,))
        if "SELECT DISTINCT code FROM statements" in sql:
            return _ResultCursor(many=[])
        if "PRAGMA table_info('statements')" in sql:
            return _ResultCursor(many=[])
        if "SELECT DISTINCT date FROM stock_data_minute_raw" in sql:
            return _ResultCursor(many=[])
        if "COPY (SELECT * FROM" in sql:
            return _ResultCursor()
        return _ResultCursor()


class _FailingCopyConnection(_ConcurrentAccessDetectingConnection):
    @staticmethod
    def _cursor_for(sql: str) -> _ResultCursor:
        if sql.startswith("COPY "):
            raise RuntimeError("copy failed")
        return _ConcurrentAccessDetectingConnection._cursor_for(sql)


def _build_lock_test_store(tmp_path: Path) -> DuckDbParquetTimeSeriesStore:
    store = DuckDbParquetTimeSeriesStore.__new__(DuckDbParquetTimeSeriesStore)
    store._duckdb_path = tmp_path / "market.duckdb"
    store._parquet_dir = tmp_path / "parquet"
    store._parquet_dir.mkdir(parents=True, exist_ok=True)
    store._conn = _ConcurrentAccessDetectingConnection()
    store._dirty_tables = set()
    store._dirty_partition_dates = {}
    store._dirty_stock_minute_dates = set()
    store._stock_projection_full_rebuild_codes = set()
    store._lock = RLock()
    return store


@pytest.mark.slow
def test_duckdb_store_serializes_publish_and_inspect(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    barrier = Barrier(3)
    errors: list[Exception] = []
    iterations = 100
    join_timeout_seconds = 15

    row = _stock_row()

    def _publish_worker() -> None:
        try:
            barrier.wait(timeout=2)
            for _ in range(iterations):
                store.publish_stock_data([row])
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    def _inspect_worker() -> None:
        try:
            barrier.wait(timeout=2)
            for _ in range(iterations):
                store.inspect()
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    publisher = Thread(target=_publish_worker)
    inspector = Thread(target=_inspect_worker)
    publisher.start()
    inspector.start()
    barrier.wait(timeout=2)
    publisher.join(timeout=join_timeout_seconds)
    inspector.join(timeout=join_timeout_seconds)

    assert not publisher.is_alive()
    assert not inspector.is_alive()
    assert not errors


def test_duckdb_store_init_raises_when_duckdb_import_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def _patched_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _patched_import)

    with pytest.raises(RuntimeError, match="duckdb"):
        open_time_series_store(
            duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
            parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        )


def test_publish_methods_return_empty_delta_for_empty_rows(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    assert store.publish_topix_data([]).mutated_rows == 0
    assert store.publish_stock_data([]).mutated_rows == 0
    assert store.publish_stock_minute_data([]).mutated_rows == 0
    assert store.publish_indices_data([]).mutated_rows == 0
    assert store.publish_margin_data([]).mutated_rows == 0
    assert store.publish_statements([]).mutated_rows == 0


def test_export_if_dirty_handles_absent_and_existing_parquet(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)

    # dirty でない場合は no-op
    store._export_if_dirty("topix_data")

    output = store._parquet_dir / "topix_data.parquet"
    output.write_text("stale")
    store._dirty_tables.add("topix_data")

    store._export_if_dirty("topix_data")

    assert not output.exists()
    assert "topix_data" not in store._dirty_tables


def test_export_if_dirty_preserves_existing_parquet_when_copy_fails(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    store._conn = _FailingCopyConnection()
    output = store._parquet_dir / "topix_data.parquet"
    output.write_text("stale")
    store._dirty_tables.add("topix_data")

    with pytest.raises(RuntimeError, match="copy failed"):
        store._export_if_dirty("topix_data")

    assert output.read_text() == "stale"
    assert "topix_data" in store._dirty_tables


def test_close_flushes_dirty_tables_and_closes_connection(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    (store._parquet_dir / "topix_data.parquet").write_text("stale")
    store._dirty_tables.add("topix_data")

    store.close()

    assert store._dirty_tables == set()
    assert store._conn.closed is True


def test_identical_nullable_publish_is_zero_delta_and_preserves_parquet_identity(
    tmp_path: Path,
) -> None:
    parquet_dir = tmp_path / "market-timeseries" / "parquet"
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(parquet_dir),
    )
    row = _indices_rows()[0]
    row["sector_name"] = None
    first = store.publish_indices_data([row])
    store.index_indices_data()
    output = parquet_dir / "indices_data.parquet"
    before = (output.stat().st_ino, output.stat().st_mtime_ns, hashlib.sha256(output.read_bytes()).hexdigest())

    repeated = dict(row)
    repeated["created_at"] = "2099-01-01T00:00:00+00:00"
    second = store.publish_indices_data([repeated])
    store.index_indices_data()
    after = (output.stat().st_ino, output.stat().st_mtime_ns, hashlib.sha256(output.read_bytes()).hexdigest())

    assert first.stats.inserted == 1
    assert second.stats.unchanged == 1
    assert second.mutated_rows == 0
    assert after == before
    assert store._conn.execute(
        "SELECT created_at FROM indices_data WHERE code = '0000' AND date = '2026-02-10'"
    ).fetchone() == ("2026-02-10T00:00:00+00:00",)
    store.close()


def test_identical_stock_publish_preserves_partitioned_parquet_identity(
    tmp_path: Path,
) -> None:
    parquet_dir = tmp_path / "market-timeseries" / "parquet"
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(parquet_dir),
    )
    rows = [_stock_row()]
    store.publish_stock_data(rows)
    store.index_stock_data()
    outputs = [
        parquet_dir / "stock_data_raw" / "date=2026-02-10" / "data.parquet",
        parquet_dir / "stock_data" / "date=2026-02-10" / "data.parquet",
    ]
    before = {
        path: (path.stat().st_ino, path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in outputs
    }

    assert store.publish_stock_data(rows).mutated_rows == 0
    store.index_stock_data()
    after = {
        path: (path.stat().st_ino, path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in outputs
    }

    assert after == before
    store.close()


def test_duplicate_publish_is_last_wins_and_reports_one_insert(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    first, second = _indices_rows()[0], dict(_indices_rows()[0])
    second["close"] = 99.0
    result = store.publish_indices_data([first, second])

    assert result.stats.input == 2
    assert result.stats.inserted == 1
    assert result.stats.unchanged == 1
    assert result.stats.input == (
        result.stats.inserted + result.stats.updated + result.stats.unchanged
    )
    assert store._conn.execute("SELECT close FROM indices_data").fetchone() == (99.0,)
    store.close()


def test_nullable_value_transition_is_reported_as_true_update(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    row = _indices_rows()[0]
    row["sector_name"] = None
    store.publish_indices_data([row])
    changed = dict(row)
    changed["sector_name"] = "TOPIX"
    result = store.publish_indices_data([changed])

    assert result.stats.updated == 1
    assert result.updated_keys == (("0000", "2026-02-10"),)
    store.close()


def test_adjustment_semantic_change_reprojects_affected_code_via_anti_diff(
    tmp_path: Path,
) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    prior = _stock_row_for("2026-01-01")
    boundary = _stock_row_for("2026-01-02")
    boundary["adjustment_factor"] = 2.0
    store.publish_stock_data([prior, boundary])
    store.index_stock_data()
    assert store._conn.execute(
        "SELECT close FROM stock_data WHERE date = '2026-01-01'"
    ).fetchone() == (4.0,)

    corrected = dict(boundary)
    corrected["adjustment_factor"] = 1.0
    corrected["created_at"] = "2099-01-01T00:00:00+00:00"
    result = store.publish_stock_data([corrected])
    store.index_stock_data()

    assert result.stats.updated == 1
    assert store._conn.execute(
        "SELECT close FROM stock_data WHERE date = '2026-01-01'"
    ).fetchone() == (2.0,)
    store.close()


def test_full_code_projection_removes_only_target_only_stale_keys(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    raw = _stock_row_for("2026-01-02")
    raw["adjustment_factor"] = 2.0
    store.publish_stock_data([raw])
    store._conn.execute(
        """
        INSERT INTO stock_data
        (code, date, open, high, low, close, volume, adjustment_factor, created_at)
        VALUES ('7203', '1900-01-01', 1, 1, 1, 1, 1, 1, '1900-01-01')
        """
    )

    result = store._reproject_pending_stock_codes()

    assert result.stats.deleted == 1
    assert result.deleted_keys == (("7203", "1900-01-01"),)
    assert result.affected_dates == frozenset({"1900-01-01", "2026-01-02"})
    assert store._conn.execute(
        "SELECT date FROM stock_data WHERE code = '7203' ORDER BY date"
    ).fetchall() == [("2026-01-02",)]
    store.close()


def test_every_high_volume_writer_reports_zero_delta_for_identical_input(
    tmp_path: Path,
) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    cases = [
        (store.publish_topix_data, _topix_rows()),
        (store.publish_stock_data, [_stock_row()]),
        (store.publish_stock_minute_data, [_stock_minute_row()]),
        (store.publish_indices_data, _indices_rows()),
        (store.publish_options_225_data, _options_225_rows()),
        (store.publish_margin_data, _margin_rows()),
        (store.publish_statements, _statement_rows()),
    ]

    for publish, rows in cases:
        first = publish(rows)
        repeated = publish(rows)
        assert first.mutated_rows > 0
        assert repeated.mutated_rows == 0
        assert repeated.stats.unchanged == len(rows)
    store.close()


def test_zero_delta_does_not_issue_persistent_dml(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    store.publish_margin_data(_margin_rows())

    class RecordingConnection:
        def __init__(self, connection: object) -> None:
            self.connection = connection
            self.sql: list[str] = []

        def execute(self, sql: str, *args: object, **kwargs: object) -> object:
            self.sql.append(sql)
            return self.connection.execute(sql, *args, **kwargs)  # type: ignore[attr-defined]

        def __getattr__(self, name: str) -> object:
            return getattr(self.connection, name)

    recording = RecordingConnection(store._conn)
    store._conn = recording
    result = store.publish_margin_data(_margin_rows())

    assert result.mutated_rows == 0
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "BEGIN"))
        for sql in recording.sql
    )
    store.close()


def test_statement_null_input_preserves_existing_non_null_as_zero_delta(
    tmp_path: Path,
) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    store.publish_statements([{"code": "7203", "disclosed_date": "2026-01-01", "profit": 10.0}])
    result = store.publish_statements([
        {"code": "7203", "disclosed_date": "2026-01-01", "profit": None}
    ])

    assert result.mutated_rows == 0
    assert store._conn.execute("SELECT profit FROM statements").fetchone() == (10.0,)
    store.close()


def test_staged_stock_flush_uses_same_last_wins_semantic_kernel(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    first = _stock_row()
    last = dict(first)
    last["close"] = 7.0
    store.stage_stock_data_rows([first])
    store.stage_stock_data_rows([last])
    result = store.flush_staged_stock_data()

    assert result.stats.input == 2
    assert result.stats.inserted == 1
    assert store._conn.execute("SELECT close FROM stock_data_raw").fetchone() == (7.0,)
    store.stage_stock_data_rows([last])
    assert store.flush_staged_stock_data().mutated_rows == 0
    store.close()


def test_discard_staged_stock_data_clears_temp_rows_without_publishing(tmp_path: Path) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    store.stage_stock_data_rows([_stock_row()])

    store.discard_staged_stock_data()

    assert store.flush_staged_stock_data().mutated_rows == 0
    assert store._conn.execute("SELECT COUNT(*) FROM stock_data_raw").fetchone() == (0,)
    store.close()


def test_topix_valid_to_invalid_transition_deletes_only_the_affected_key(
    tmp_path: Path,
) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    store.publish_topix_data([
        {"date": "2026-01-01", "open": 9.0, "high": 11.0, "low": 9.0, "close": 10.0},
        {"date": "2026-01-02", "open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0},
    ])

    result = store.publish_topix_data([
        {"date": "2026-01-02", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0},
    ])

    assert result.stats.deleted == 1
    assert result.deleted_keys == (("2026-01-02",),)
    assert store._conn.execute("SELECT date FROM topix_data ORDER BY date").fetchall() == [("2026-01-01",)]
    store.close()


def test_topix_preflight_deletes_existing_next_row_invalidated_by_prior_update(
    tmp_path: Path,
) -> None:
    store = open_time_series_store(
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    store.publish_topix_data([
        {"date": "2026-01-01", "open": 8.0, "high": 10.0, "low": 8.0, "close": 9.0},
        {"date": "2026-01-02", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0},
    ])

    result = store.publish_topix_data([
        {"date": "2026-01-01", "open": 8.0, "high": 11.0, "low": 8.0, "close": 10.0}
    ])

    assert result.stats.updated == 1
    assert result.stats.deleted == 1
    assert result.deleted_keys == (("2026-01-02",),)
    store.close()
