from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
    EventTimePitCopyResult,
)
from src.infrastructure.db.market.market_db import MarketDb


_BASIS_COLUMNS = """
    code, basis_id, valid_from, valid_to_exclusive,
    adjustment_through_date, source_fingerprint,
    materialized_through_date, status, created_at, updated_at
"""


def _build_v4_market_with_two_regimes(tmp_path: Path) -> Path:
    source = tmp_path / "market-v4.duckdb"
    db = MarketDb(str(source))
    db.close()
    conn = duckdb.connect(str(source))
    try:
        conn.executemany(
            "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("72030", "2024-01-04", 100.0, 110.0, 90.0, 105.0, 1000, 1.0, None),
                ("7203", "2024-06-28", 200.0, 210.0, 190.0, 205.0, 2000, 0.5, None),
                ("7203", "2024-12-30", 220.0, 230.0, 210.0, 225.0, 2200, 1.0, None),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_master_daily VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                (date, "72030", "Toyota", None, "0111", "Prime", "6", "Auto", "3700", "Transport", None, "1949-05-16", None)
                for date in ("2024-01-04", "2024-06-28", "2024-12-30")
            ],
        )
        conn.executemany(
            f"INSERT INTO stock_adjustment_bases ({_BASIS_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("7203", "event-pit-v1:7203:2024-01-04", "2024-01-04", "2024-06-28", "2024-01-04", "fp-origin", "2024-06-27", "ready", None, None),
                ("72030", "event-pit-v1:7203:2024-06-28", "2024-06-28", None, "2024-06-28", "fp-split", "2024-12-30", "ready", None, None),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
            [
                ("7203", "event-pit-v1:7203:2024-01-04", "2024-01-04", None, 1.0),
                ("72030", "event-pit-v1:7203:2024-06-28", "2024-01-04", "2024-06-28", 0.5),
                ("7203", "event-pit-v1:7203:2024-06-28", "2024-06-28", None, 1.0),
            ],
        )
        conn.executemany(
            """
            INSERT INTO statement_metrics_adjusted (
                code, disclosed_date, period_end, period_type, price_basis_date,
                adjusted_eps, basis_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("72030", "2024-05-10", "2024-03-31", "FY", "2024-01-04", 100.0, "event-pit-v1:7203:2024-01-04"),
                ("7203", "2024-05-10", "2024-03-31", "FY", "2024-06-28", 50.0, "event-pit-v1:7203:2024-06-28"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO daily_valuation (
                code, date, price_basis_date, close, eps, basis_version,
                statement_disclosed_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("7203", "2024-01-04", "2024-01-04", 105.0, 100.0, "event-pit-v1:7203:2024-01-04", None),
                ("72030", "2024-06-27", "2024-01-04", 102.5, 100.0, "event-pit-v1:7203:2024-01-04", "2024-05-10"),
                ("7203", "2024-06-28", "2024-06-28", 205.0, 50.0, "event-pit-v1:7203:2024-06-28", "2024-05-10"),
                ("7203", "2024-12-30", "2024-06-28", 225.0, 50.0, "event-pit-v1:7203:2024-06-28", "2024-05-10"),
            ],
        )
    finally:
        conn.close()
    return source


def _basis_versions(path: Path) -> list[str]:
    conn = duckdb.connect(str(path))
    try:
        return [row[0] for row in conn.execute("SELECT basis_id FROM stock_adjustment_bases ORDER BY valid_from").fetchall()]
    finally:
        conn.close()


def test_copy_event_time_pit_retains_origin_and_split_bases(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot"))

    result = writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    assert result == EventTimePitCopyResult(
        raw_price_rows=3,
        stock_master_rows=3,
        basis_rows=2,
        segment_rows=3,
        statement_metric_rows=2,
        daily_valuation_rows=4,
    )
    assert writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["72030"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    ) == result
    assert set(_basis_versions(writer.duckdb_path)) == {
        "event-pit-v1:7203:2024-01-04",
        "event-pit-v1:7203:2024-06-28",
    }
    conn = duckdb.connect(str(writer.duckdb_path))
    try:
        assert conn.execute("SELECT DISTINCT code FROM stock_data_raw").fetchall() == [("7203",)]
        assert conn.execute("SELECT DISTINCT code FROM stock_master_daily").fetchall() == [("7203",)]
        assert conn.execute("SELECT COUNT(*) FROM daily_valuation WHERE date > '2024-12-31'").fetchone() == (0,)
    finally:
        conn.close()


@pytest.mark.parametrize("fault", ["market_v3", "missing_segments", "building_basis"])
def test_copy_preflight_fails_before_partial_insert(tmp_path: Path, fault: str) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        if fault == "market_v3":
            conn.execute("DELETE FROM market_schema_version")
            conn.execute("INSERT INTO market_schema_version VALUES (3, '2026-07-14', 'fault')")
        elif fault == "missing_segments":
            conn.execute("DROP TABLE stock_adjustment_basis_segments")
        else:
            conn.execute("UPDATE stock_adjustment_bases SET status = 'building' WHERE valid_from = '2024-06-28'")
    finally:
        conn.close()

    writer = DatasetWriter(str(tmp_path / "snapshot"))
    sentinel = {
        "code": "9999", "date": "2000-01-01", "open": 1.0, "high": 1.0,
        "low": 1.0, "close": 1.0, "volume": 1, "adjustment_factor": 1.0,
        "created_at": None,
    }
    writer._duckdb_store._conn.execute(  # noqa: SLF001 - atomicity sentinel
        "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        list(sentinel.values()),
    )

    with pytest.raises(DatasetSnapshotError):
        writer.copy_event_time_pit_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from="2024-01-01",
            date_to="2024-12-31",
        )

    assert writer._duckdb_store._conn.execute(  # noqa: SLF001
        "SELECT code, date FROM stock_data_raw"
    ).fetchall() == [("9999", "2000-01-01")]


def test_copy_preflight_rejects_incomplete_materialized_coverage(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_adjustment_bases SET materialized_through_date = '2024-06-28' WHERE valid_from = '2024-06-28'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot"))

    with pytest.raises(DatasetSnapshotError, match="coverage"):
        writer.copy_event_time_pit_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from="2024-01-01",
            date_to="2024-12-31",
        )
