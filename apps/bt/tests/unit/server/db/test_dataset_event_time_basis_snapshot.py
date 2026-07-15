from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
    EventTimePitCopyResult,
)
from src.infrastructure.db.market.dataset_snapshot_reader import DatasetSnapshotReader
from src.infrastructure.db.market.market_db import MarketDb
from tests.unit.server.test_dataset_snapshot_reader import _write_manifest


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
                ("72030", "2024-01-04", "2024-06-28", 52.5, 50.0, "event-pit-v1:7203:2024-06-28", None),
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


_PIT_TABLES = (
    "stock_data_raw",
    "stock_master_daily",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
    "statement_metrics_adjusted",
    "daily_valuation",
)


def _target_graph(writer: DatasetWriter) -> dict[str, list[tuple[object, ...]]]:
    conn = writer._duckdb_store._conn  # noqa: SLF001 - immutable snapshot assertion
    return {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
        for table in _PIT_TABLES
    }


def _copy(writer: DatasetWriter, source: Path) -> EventTimePitCopyResult:
    return writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )


def _build_readable_two_regime_snapshot(tmp_path: Path) -> Path:
    source = _build_v4_market_with_two_regimes(tmp_path)
    snapshot_dir = tmp_path / "readable-snapshot"
    writer = DatasetWriter(str(snapshot_dir))
    _copy(writer, source)
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()
    _write_manifest(snapshot_dir)
    return snapshot_dir


def test_reader_selects_containing_basis_and_never_mixes_versions(
    tmp_path: Path,
) -> None:
    snapshot_dir = _build_readable_two_regime_snapshot(tmp_path)
    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        origin = reader.resolve_adjustment_basis("72030", "2024-06-27")
        post_split = reader.resolve_adjustment_basis("7203", "2024-06-28")

        assert origin.basis_id != post_split.basis_id
        assert origin.valid_from == "2024-01-04"
        assert post_split.valid_from == "2024-06-28"
        assert {
            row["basis_version"]
            for row in reader.get_daily_valuation(
                "7203", basis_id=origin.basis_id
            )
        } == {origin.basis_id}
        assert {
            row["basis_version"]
            for row in reader.get_adjusted_statement_metrics(
                "72030", basis_id=post_split.basis_id
            )
        } == {post_split.basis_id}
    finally:
        reader.close()


def test_reader_projects_ohlcv_from_raw_rows_and_selected_basis_segments(
    tmp_path: Path,
) -> None:
    snapshot_dir = _build_readable_two_regime_snapshot(tmp_path)
    reader = DatasetSnapshotReader(str(snapshot_dir))
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(
            "INSERT INTO stock_data VALUES "
            "('7203', '2024-01-04', 9, 9, 9, 9, 9999, 1, NULL)"
        )
    finally:
        conn.close()

    try:
        projected = reader.get_basis_adjusted_stock_ohlcv(
            "72030",
            basis_id="event-pit-v1:7203:2024-06-28",
            end="2024-06-28",
        )

        assert list(projected["date"]) == ["2024-01-04", "2024-06-28"]
        assert list(projected["close"]) == [52.5, 205.0]
        assert list(projected["volume"]) == [2000, 2000]
        assert set(projected["basis_id"]) == {
            "event-pit-v1:7203:2024-06-28"
        }
    finally:
        reader.close()


def _add_unrelated_sentinel(writer: DatasetWriter) -> None:
    writer._duckdb_store._conn.execute(  # noqa: SLF001 - atomicity sentinel
        "INSERT INTO stock_data_raw VALUES "
        "('9999', '2000-01-01', 1, 1, 1, 1, 1, 1, NULL)"
    )


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


def test_copy_event_time_pit_accepts_and_retains_master_only_dates(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_master_daily (
                date, code, company_name, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name
            ) VALUES (
                '2024-02-01', '72030', 'Toyota', '0111', 'Prime',
                '6', 'Auto', '3700', 'Transport'
            )
            """
        )
    finally:
        conn.close()

    writer = DatasetWriter(str(tmp_path / "snapshot-master-only"))
    try:
        result = _copy(writer, source)

        assert result.raw_price_rows == 3
        assert result.stock_master_rows == 4
        assert writer._duckdb_store._conn.execute(  # noqa: SLF001
            "SELECT code, date FROM stock_master_daily ORDER BY date"
        ).fetchall() == [
            ("7203", "2024-01-04"),
            ("7203", "2024-02-01"),
            ("7203", "2024-06-28"),
            ("7203", "2024-12-30"),
        ]
    finally:
        writer.close()


def test_copy_event_time_pit_does_not_require_valuation_for_incomplete_raw_quote(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_data_raw (
                code, date, open, high, low, close, volume, adjustment_factor
            ) VALUES ('7203', '2024-12-31', NULL, NULL, NULL, NULL, NULL, 1.0)
            """
        )
        conn.execute(
            """
            UPDATE stock_adjustment_bases
            SET materialized_through_date = '2024-12-31'
            WHERE basis_id = 'event-pit-v1:7203:2024-06-28'
            """
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-incomplete-raw"))
    try:
        result = _copy(writer, source)
        assert result.raw_price_rows == 3
        assert result.daily_valuation_rows == 4
    finally:
        writer.close()


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


@pytest.mark.parametrize(
    "correction_sql",
    [
        "UPDATE market_source.stock_data_raw SET close = 999 WHERE date = '2024-12-30'",
        "UPDATE market_source.stock_master_daily SET company_name = 'Corrected' WHERE date = '2024-12-30'",
        "UPDATE market_source.stock_adjustment_bases SET source_fingerprint = 'corrected' WHERE valid_from = '2024-06-28'",
        "UPDATE market_source.stock_adjustment_basis_segments SET cumulative_factor = 0.75 WHERE basis_id = 'event-pit-v1:7203:2024-06-28' AND source_date_from = '2024-01-04'",
        "UPDATE market_source.statement_metrics_adjusted SET adjusted_eps = 49 WHERE basis_version = 'event-pit-v1:7203:2024-06-28'",
        "UPDATE market_source.daily_valuation SET close = 999 WHERE basis_version = 'event-pit-v1:7203:2024-06-28' AND date = '2024-12-30'",
    ],
)
def test_recopy_rejects_source_correction_and_preserves_full_target(
    tmp_path: Path,
    correction_sql: str,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _copy(writer, source)
    before = _target_graph(writer)
    writer._duckdb_store._conn.execute(correction_sql)  # noqa: SLF001

    with pytest.raises(DatasetSnapshotError, match="immutable"):
        _copy(writer, source)

    assert _target_graph(writer) == before


def test_recopy_rejects_source_deletion_and_preserves_full_target(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _copy(writer, source)
    before = _target_graph(writer)
    writer._duckdb_store._conn.execute(  # noqa: SLF001
        "DELETE FROM market_source.statement_metrics_adjusted "
        "WHERE basis_version = 'event-pit-v1:7203:2024-06-28'"
    )

    with pytest.raises(DatasetSnapshotError):
        _copy(writer, source)

    assert _target_graph(writer) == before


def test_recopy_rejects_stale_target_row_without_mutating_it(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _copy(writer, source)
    writer._duckdb_store._conn.execute(  # noqa: SLF001
        "INSERT INTO stock_data_raw VALUES "
        "('7203', '2024-02-01', 1, 1, 1, 1, 1, 1, NULL)"
    )
    before = _target_graph(writer)

    with pytest.raises(DatasetSnapshotError, match="immutable"):
        _copy(writer, source)

    assert _target_graph(writer) == before


def test_preflight_rejects_orphan_adjusted_metric_basis(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, disclosed_date, period_end, period_type, price_basis_date,
                adjusted_eps, basis_version
            ) VALUES ('7203', '2024-07-01', '2024-06-30', '1Q', '1900-01-01', 1,
                      'ghost-basis')
            """
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _add_unrelated_sentinel(writer)
    before = _target_graph(writer)

    with pytest.raises(DatasetSnapshotError, match="provenance"):
        _copy(writer, source)

    assert _target_graph(writer) == before


def test_preflight_rejects_in_range_orphan_basis_segment_and_preserves_target(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_adjustment_basis_segments
            VALUES ('72030', 'ghost-basis', '2024-01-04', NULL, 1.0)
            """
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _add_unrelated_sentinel(writer)
    before = _target_graph(writer)

    with pytest.raises(DatasetSnapshotError, match="segment provenance"):
        _copy(writer, source)

    assert _target_graph(writer) == before


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE statement_metrics_adjusted SET price_basis_date = '1900-01-01' WHERE basis_version = 'event-pit-v1:7203:2024-06-28'",
        "UPDATE daily_valuation SET price_basis_date = '1900-01-01' WHERE basis_version = 'event-pit-v1:7203:2024-06-28'",
    ],
)
def test_preflight_rejects_basis_provenance_mismatch(tmp_path: Path, sql: str) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(sql)
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _add_unrelated_sentinel(writer)
    before = _target_graph(writer)

    with pytest.raises(DatasetSnapshotError, match="provenance"):
        _copy(writer, source)

    assert _target_graph(writer) == before


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("DELETE FROM daily_valuation", "valuation coverage"),
        (
            "DELETE FROM daily_valuation WHERE date = '2024-01-04' "
            "AND basis_version = 'event-pit-v1:7203:2024-06-28'",
            "valuation coverage",
        ),
        (
            "UPDATE stock_adjustment_basis_segments SET source_date_to_exclusive = '2024-01-05' "
            "WHERE basis_id = 'event-pit-v1:7203:2024-06-28' AND source_date_from = '2024-01-04'",
            "segment",
        ),
        (
            "DELETE FROM statement_metrics_adjusted "
            "WHERE basis_version = 'event-pit-v1:7203:2024-06-28'",
            "adjusted metric coverage",
        ),
        (
            "DELETE FROM stock_master_daily WHERE date = '2024-06-28'",
            "raw price coverage",
        ),
    ],
)
def test_preflight_rejects_empty_or_gapped_physical_coverage(
    tmp_path: Path,
    sql: str,
    message: str,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(sql)
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot"))
    _add_unrelated_sentinel(writer)
    before = _target_graph(writer)

    with pytest.raises(DatasetSnapshotError, match=message):
        _copy(writer, source)

    assert _target_graph(writer) == before
