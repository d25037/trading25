from __future__ import annotations

import builtins
from pathlib import Path
from threading import Barrier, Lock, RLock, Thread
from time import sleep

import pytest

from src.infrastructure.db.market.time_series_store import (
    DuckDbParquetTimeSeriesStore,
    create_time_series_store,
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


def test_create_time_series_store_returns_none_for_unsupported_backend(
    tmp_path: Path,
) -> None:
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


def test_index_options_225_data_exports_parquet(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "market-timeseries" / "parquet"
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(parquet_dir),
    )
    assert store is not None

    store.publish_options_225_data(_options_225_rows())
    store.index_options_225_data()

    assert (parquet_dir / "options_225_data.parquet").exists()

    store.close()


def test_publish_stock_data_large_batch_uses_relation_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    called = {"relation": False}
    original = DuckDbParquetTimeSeriesStore._publish_stock_data_via_relation

    def _spy(self: DuckDbParquetTimeSeriesStore, rows: list[dict[str, object]]) -> int:
        called["relation"] = True
        return original(self, rows)

    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_STOCK_DATA_RELATION_INSERT_THRESHOLD", 1
    )
    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_publish_stock_data_via_relation", _spy
    )

    store.publish_stock_data(
        [_stock_row_for("2026-02-10"), _stock_row_for("2026-02-11")]
    )

    inspection = store.inspect()

    assert called["relation"] is True
    assert inspection.stock_count == 2
    assert inspection.stock_min == "2026-02-10"
    assert inspection.stock_max == "2026-02-11"

    store.close()


def test_publish_indices_data_large_batch_uses_relation_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    called = {"relation": False}
    original = DuckDbParquetTimeSeriesStore._publish_indices_data_via_relation

    def _spy(self: DuckDbParquetTimeSeriesStore, rows: list[dict[str, object]]) -> int:
        called["relation"] = True
        return original(self, rows)

    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_INDICES_DATA_RELATION_INSERT_THRESHOLD", 1
    )
    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_publish_indices_data_via_relation", _spy
    )

    store.publish_indices_data(_indices_rows())

    inspection = store.inspect()

    assert called["relation"] is True
    assert inspection.indices_count == 2
    assert inspection.indices_min == "2026-02-10"
    assert inspection.indices_max == "2026-02-11"

    store.close()


def test_publish_options_225_data_large_batch_uses_relation_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    called = {"relation": False}
    original = DuckDbParquetTimeSeriesStore._publish_options_225_data_via_relation

    def _spy(self: DuckDbParquetTimeSeriesStore, rows: list[dict[str, object]]) -> int:
        called["relation"] = True
        return original(self, rows)

    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_OPTIONS_225_RELATION_INSERT_THRESHOLD", 1
    )
    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_publish_options_225_data_via_relation", _spy
    )

    store.publish_options_225_data(_options_225_rows())

    inspection = store.inspect()

    assert called["relation"] is True
    assert inspection.options_225_count == 2
    assert inspection.options_225_min == "2026-02-10"
    assert inspection.options_225_max == "2026-02-11"

    store.close()


def test_publish_margin_data_large_batch_uses_relation_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
        parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
    )
    assert store is not None

    called = {"relation": False}
    original = DuckDbParquetTimeSeriesStore._publish_margin_data_via_relation

    def _spy(self: DuckDbParquetTimeSeriesStore, rows: list[dict[str, object]]) -> int:
        called["relation"] = True
        return original(self, rows)

    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_MARGIN_DATA_RELATION_INSERT_THRESHOLD", 1
    )
    monkeypatch.setattr(
        DuckDbParquetTimeSeriesStore, "_publish_margin_data_via_relation", _spy
    )

    store.publish_margin_data(_margin_rows())

    inspection = store.inspect()

    assert called["relation"] is True
    assert inspection.margin_count == 2
    assert inspection.margin_min == "2026-02-07"
    assert inspection.margin_max == "2026-02-10"
    assert inspection.margin_codes == {"7203"}

    store.close()


def test_publish_topix_data_excludes_flat_row_equal_to_previous_close(
    tmp_path: Path,
) -> None:
    store = create_time_series_store(
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
    assert published == 3

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

    first = create_time_series_store(
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

    second = create_time_series_store(
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
        if "FROM topix_data" in sql and "MAX(date)" in sql:
            return _ResultCursor(one=(0, None, None))
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
        if "COPY (SELECT * FROM" in sql:
            return _ResultCursor()
        return _ResultCursor()


def _build_lock_test_store(tmp_path: Path) -> DuckDbParquetTimeSeriesStore:
    store = DuckDbParquetTimeSeriesStore.__new__(DuckDbParquetTimeSeriesStore)
    store._duckdb_path = tmp_path / "market.duckdb"
    store._parquet_dir = tmp_path / "parquet"
    store._parquet_dir.mkdir(parents=True, exist_ok=True)
    store._conn = _ConcurrentAccessDetectingConnection()
    store._dirty_tables = set()
    store._lock = RLock()
    return store


def test_duckdb_store_serializes_publish_and_inspect(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    barrier = Barrier(3)
    errors: list[Exception] = []

    row = _stock_row()

    def _publish_worker() -> None:
        try:
            barrier.wait(timeout=2)
            for _ in range(200):
                store.publish_stock_data([row])
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    def _inspect_worker() -> None:
        try:
            barrier.wait(timeout=2)
            for _ in range(200):
                store.inspect()
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    publisher = Thread(target=_publish_worker)
    inspector = Thread(target=_inspect_worker)
    publisher.start()
    inspector.start()
    barrier.wait(timeout=2)
    publisher.join(timeout=5)
    inspector.join(timeout=5)

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
        DuckDbParquetTimeSeriesStore(
            duckdb_path=str(tmp_path / "market-timeseries" / "market.duckdb"),
            parquet_dir=str(tmp_path / "market-timeseries" / "parquet"),
        )


def test_publish_methods_return_zero_for_empty_rows(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    assert store.publish_topix_data([]) == 0
    assert store.publish_stock_data([]) == 0
    assert store.publish_indices_data([]) == 0
    assert store.publish_margin_data([]) == 0
    assert store.publish_statements([]) == 0


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


def test_close_flushes_dirty_tables_and_closes_connection(tmp_path: Path) -> None:
    store = _build_lock_test_store(tmp_path)
    (store._parquet_dir / "topix_data.parquet").write_text("stale")
    store._dirty_tables.add("topix_data")

    store.close()

    assert store._dirty_tables == set()
    assert store._conn.closed is True
