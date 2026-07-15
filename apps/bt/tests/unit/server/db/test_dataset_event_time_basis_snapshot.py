from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.fundamentals.adjustment_basis import (
    RawAdjustmentPoint,
    build_stock_adjustment_lineage,
)
from src.infrastructure.db.dataset_io.dataset_pit_lineage import iter_cutoff_lineages
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
    EventTimePitCopyResult,
)
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetManifestValidationError,
    DatasetSnapshotReader,
)
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
            "INSERT INTO topix_data VALUES (?, 1, 1, 1, 1, NULL)",
            [
                (session,)
                for session in (
                    "2024-01-04",
                    "2024-06-27",
                    "2024-06-28",
                    "2024-12-30",
                    "2024-12-31",
                )
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
                for date in (
                    "2024-01-04",
                    "2024-06-28",
                    "2024-12-30",
                    "2024-12-31",
                )
            ],
        )
        conn.executemany(
            f"INSERT INTO stock_adjustment_bases ({_BASIS_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("7203", "event-pit-v1:7203:2024-01-04", "2024-01-04", "2024-06-28", "2024-01-04", "b7bc1d3ff29388d3ee4a54ca325c8100dc9b503a711de0ebe1735ea103b020c4", "2024-06-27", "ready", None, None),
                ("72030", "event-pit-v1:7203:2024-06-28", "2024-06-28", None, "2024-06-28", "fp-split", "2024-12-31", "ready", None, None),
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
                ("72030", "2024-05-10", "2024-05-10", "FY", "2024-01-04", 100.0, "event-pit-v1:7203:2024-01-04"),
                ("7203", "2024-05-10", "2024-05-10", "FY", "2024-06-28", 50.0, "event-pit-v1:7203:2024-06-28"),
            ],
        )
        conn.execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period
            ) VALUES ('7203', '2024-05-10', 100.0, 'FY')
            """
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


def _replace_source_lineage_from_raw(source: Path, code: str = "7203") -> None:
    conn = duckdb.connect(str(source))
    try:
        raw_rows = conn.execute(
            "SELECT code, date, adjustment_factor FROM stock_data_raw "
            "WHERE code IN ('7203', '72030') ORDER BY date, code"
        ).fetchall()
        sessions = [
            str(row[0])
            for row in conn.execute("SELECT date FROM topix_data ORDER BY date").fetchall()
        ]
        lineage = build_stock_adjustment_lineage(
            code,
            [
                RawAdjustmentPoint(str(row[0]), str(row[1]), row[2])
                for row in raw_rows
            ],
            market_sessions=sessions,
        )
        conn.execute(
            "DELETE FROM stock_adjustment_basis_segments WHERE code IN ('7203', '72030')"
        )
        conn.execute("DELETE FROM stock_adjustment_bases WHERE code IN ('7203', '72030')")
        conn.executemany(
            f"INSERT INTO stock_adjustment_bases ({_BASIS_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
            [
                (
                    basis.code,
                    basis.basis_id,
                    basis.valid_from,
                    basis.valid_to_exclusive,
                    basis.adjustment_through_date,
                    basis.source_fingerprint,
                    basis.materialized_through_date,
                    basis.status,
                )
                for basis in lineage.bases
            ],
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
            [
                (
                    segment.code,
                    segment.basis_id,
                    segment.source_date_from,
                    segment.source_date_to_exclusive,
                    segment.cumulative_factor,
                )
                for segment in lineage.segments
            ],
        )
    finally:
        conn.close()


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


def _upsert_cutoff_stock(writer: DatasetWriter) -> None:
    writer.upsert_stocks(
        [
            {
                "code": "7203",
                "company_name": "Toyota",
                "company_name_english": None,
                "market_code": "0111",
                "market_name": "Prime",
                "sector_17_code": "6",
                "sector_17_name": "Auto",
                "sector_33_code": "3700",
                "sector_33_name": "Transport",
                "scale_category": None,
                "listed_date": "1949-05-16",
            }
        ]
    )


def _copy(writer: DatasetWriter, source: Path) -> EventTimePitCopyResult:
    _upsert_cutoff_stock(writer)
    return writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )


def test_writer_requires_stocks_to_equal_cutoff_day_master(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot-master-invariant"))
    writer.upsert_stocks(
        [
            {
                "code": "7203",
                "company_name": "Current Wrong",
                "company_name_english": None,
                "market_code": "0113",
                "market_name": "Growth",
                "sector_17_code": "99",
                "sector_17_name": "Wrong",
                "sector_33_code": "9999",
                "sector_33_name": "Wrong",
                "scale_category": "Wrong",
                "listed_date": "2099-01-01",
            }
        ]
    )

    with pytest.raises(DatasetSnapshotError, match="cutoff-day stock master"):
        writer.copy_event_time_pit_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from="2024-01-01",
            date_to="2024-12-31",
        )


def test_writer_rejects_extra_stale_destination_stock(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot-extra-stock"))
    _upsert_cutoff_stock(writer)
    writer.upsert_stocks(
        [
            {
                "code": "9999",
                "company_name": "Stale",
                "company_name_english": None,
                "market_code": "0111",
                "market_name": "Prime",
                "sector_17_code": "",
                "sector_17_name": "",
                "sector_33_code": "",
                "sector_33_name": "",
                "scale_category": None,
                "listed_date": "",
            }
        ]
    )

    with pytest.raises(DatasetSnapshotError, match="cutoff-day stock master"):
        writer.copy_event_time_pit_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from="2024-01-01",
            date_to="2024-12-31",
        )


def test_writer_dedupes_cutoff_master_alias_with_whole_canonical_preference(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_master_daily VALUES (
                '2024-12-31', '7203', 'Canonical Toyota', 'CANONICAL',
                '0112', 'Standard', '10', 'Canonical 17', '6050',
                'Canonical 33', 'TOPIX Mid400', '1950-01-01', NULL
            )
            """
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-master-alias"))
    writer.upsert_stocks(
        [
            {
                "code": "7203",
                "company_name": "Canonical Toyota",
                "company_name_english": "CANONICAL",
                "market_code": "0112",
                "market_name": "Standard",
                "sector_17_code": "10",
                "sector_17_name": "Canonical 17",
                "sector_33_code": "6050",
                "sector_33_name": "Canonical 33",
                "scale_category": "TOPIX Mid400",
                "listed_date": "1950-01-01",
            }
        ]
    )

    writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )
    assert writer._duckdb_store._conn.execute(  # noqa: SLF001
        "SELECT company_name, market_code FROM stock_master_daily "
        "WHERE date = '2024-12-31'"
    ).fetchall() == [("Canonical Toyota", "0112")]


def test_writer_normalizes_null_cutoff_listed_date_to_empty_string(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_master_daily SET listed_date = NULL "
            "WHERE date = '2024-12-31'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-null-listed"))
    _upsert_cutoff_stock(writer)
    writer._duckdb_store._conn.execute(  # noqa: SLF001
        "UPDATE stocks SET listed_date = '' WHERE code = '7203'"
    )

    writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    assert writer._duckdb_store._conn.execute(  # noqa: SLF001
        "SELECT listed_date FROM stock_master_daily WHERE date = '2024-12-31'"
    ).fetchone() == ("",)


def _build_readable_two_regime_snapshot(tmp_path: Path) -> Path:
    source = _build_v4_market_with_two_regimes(tmp_path)
    snapshot_dir = tmp_path / "readable-snapshot"
    writer = DatasetWriter(str(snapshot_dir))
    _copy(writer, source)
    writer.copy_statements_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_to="2024-12-31",
    )
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
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(
            "INSERT INTO stock_data VALUES "
            "('7203', '2024-01-04', 9, 9, 9, 9, 9999, 1, NULL)"
        )
    finally:
        conn.close()
    _write_manifest(snapshot_dir)
    reader = DatasetSnapshotReader(str(snapshot_dir))

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
    _upsert_cutoff_stock(writer)

    result = writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    assert result == EventTimePitCopyResult(
        raw_price_rows=3,
        stock_master_rows=4,
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


def test_cutoff_reopens_active_basis_and_excludes_future_split(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "INSERT INTO stock_data_raw VALUES "
            "('7203', '2025-01-06', 300, 301, 299, 300, 3000, 0.5, NULL)"
        )
        conn.execute(
            "INSERT INTO topix_data VALUES ('2025-01-06', 1, 1, 1, 1, NULL)"
        )
    finally:
        conn.close()
    _replace_source_lineage_from_raw(source)
    writer = DatasetWriter(str(tmp_path / "snapshot-cutoff"))
    _upsert_cutoff_stock(writer)

    result = writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    assert result.basis_rows == 2
    conn = duckdb.connect(writer.duckdb_path)
    try:
        rows = conn.execute(
            "SELECT basis_id, valid_to_exclusive, materialized_through_date "
            "FROM stock_adjustment_bases ORDER BY valid_from"
        ).fetchall()
        assert rows[-1] == (
            "event-pit-v1:7203:2024-06-28",
            None,
            "2024-12-31",
        )
        assert all("2025-01-06" not in str(row) for row in rows)
    finally:
        conn.close()
    assert writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to="2024-12-31",
    ) == result


def test_late_date_from_still_preserves_all_pre_cutoff_basis_ids(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    writer = DatasetWriter(str(tmp_path / "snapshot-late-from"))
    _upsert_cutoff_stock(writer)

    writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-12-01",
        date_to="2024-12-31",
    )

    assert set(_basis_versions(writer.duckdb_path)) == {
        "event-pit-v1:7203:2024-01-04",
        "event-pit-v1:7203:2024-06-28",
    }


def test_raw_adjustment_aliases_dedupe_and_conflicts_fail_atomically(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "INSERT INTO stock_data_raw VALUES "
            "('7203', '2024-01-04', 100, 110, 90, 105, 1000, 1.0, NULL)"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-alias"))

    assert _copy(writer, source).raw_price_rows == 3
    before = _target_graph(writer)
    writer._duckdb_store._conn.execute(  # noqa: SLF001 - attached source mutation
        "UPDATE market_source.stock_data_raw SET adjustment_factor = 0.5 "
        "WHERE code = '7203' AND date = '2024-01-04'"
    )

    with pytest.raises(DatasetSnapshotError, match="PIT preflight"):
        _copy(writer, source)
    assert _target_graph(writer) == before


def test_closed_source_basis_boundary_mismatch_is_rejected(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_adjustment_bases SET valid_to_exclusive = '2024-06-27' "
            "WHERE valid_from = '2024-01-04'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-closed-mismatch"))

    with pytest.raises(DatasetSnapshotError, match="closed source basis mismatch"):
        _copy(writer, source)


def test_source_basis_set_must_exactly_match_rebuilt_cutoff_graph(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            f"INSERT INTO stock_adjustment_bases ({_BASIS_COLUMNS}) VALUES "
            "('7203', 'event-pit-v1:7203:2024-02-01', '2024-02-01', NULL, "
            "'2024-02-01', 'extra', '2024-12-30', 'ready', NULL, NULL)"
        )
    finally:
        conn.close()

    with pytest.raises(DatasetSnapshotError, match="source basis set mismatch"):
        _copy(DatasetWriter(str(tmp_path / "snapshot-extra-basis")), source)


def test_closed_source_basis_fingerprint_must_match_rebuilt_graph(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_adjustment_bases SET source_fingerprint = 'mutated' "
            "WHERE valid_from = '2024-01-04'"
        )
    finally:
        conn.close()

    with pytest.raises(DatasetSnapshotError, match="closed source basis mismatch"):
        _copy(DatasetWriter(str(tmp_path / "snapshot-fingerprint")), source)


@pytest.mark.parametrize(
    "field,value",
    [
        ("valid_to_exclusive", "2025-1-06"),
        ("valid_to_exclusive", "not-a-date"),
        ("materialized_through_date", "2025-1-06"),
    ],
)
def test_active_source_basis_dates_must_be_canonical(
    tmp_path: Path, field: str, value: str
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            f"UPDATE stock_adjustment_bases SET {field} = ? "
            "WHERE valid_from = '2024-06-28'",
            [value],
        )
    finally:
        conn.close()

    with pytest.raises(DatasetSnapshotError, match="canonical ISO"):
        _copy(DatasetWriter(str(tmp_path / f"snapshot-date-{field}")), source)


def test_identical_basis_and_segment_aliases_dedupe_but_conflicts_fail(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            f"INSERT INTO stock_adjustment_bases ({_BASIS_COLUMNS}) "
            "SELECT '72030', basis_id, valid_from, valid_to_exclusive, "
            "adjustment_through_date, source_fingerprint, materialized_through_date, "
            "status, created_at, updated_at FROM stock_adjustment_bases "
            "WHERE code = '7203' AND valid_from = '2024-01-04'"
        )
        conn.execute(
            "INSERT INTO stock_adjustment_basis_segments "
            "SELECT '72030', basis_id, source_date_from, source_date_to_exclusive, "
            "cumulative_factor FROM stock_adjustment_basis_segments "
            "WHERE code = '7203' AND basis_id = 'event-pit-v1:7203:2024-01-04'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-identical-alias"))
    assert _copy(writer, source).basis_rows == 2

    writer._duckdb_store._conn.execute(  # noqa: SLF001
        "UPDATE market_source.stock_adjustment_bases "
        "SET source_fingerprint = 'alias-conflict' WHERE code = '72030' "
        "AND basis_id = 'event-pit-v1:7203:2024-01-04'"
    )
    with pytest.raises(DatasetSnapshotError, match="conflicting source basis aliases"):
        _copy(writer, source)
    writer._duckdb_store._conn.execute(  # noqa: SLF001
        "UPDATE market_source.stock_adjustment_bases "
        "SET source_fingerprint = 'b7bc1d3ff29388d3ee4a54ca325c8100dc9b503a711de0ebe1735ea103b020c4' "
        "WHERE code = '72030' "
        "AND basis_id = 'event-pit-v1:7203:2024-01-04'"
    )
    writer._duckdb_store._conn.execute(  # noqa: SLF001
        "UPDATE market_source.stock_adjustment_basis_segments "
        "SET cumulative_factor = 0.75 WHERE code = '72030' "
        "AND basis_id = 'event-pit-v1:7203:2024-01-04'"
    )
    with pytest.raises(DatasetSnapshotError, match="conflicting source segment aliases"):
        _copy(writer, source)


@pytest.mark.parametrize(
    "fault", ["empty", "malformed", "before_first", "non_session"]
)
def test_lineage_requires_canonical_covering_topix_sessions(
    tmp_path: Path, fault: str
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        if fault == "empty":
            conn.execute("DELETE FROM topix_data")
        elif fault == "malformed":
            conn.execute(
                "INSERT INTO topix_data VALUES ('2024-1-04', 1, 1, 1, 1, NULL)"
            )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / f"snapshot-topix-{fault}"))

    with pytest.raises(DatasetSnapshotError, match="TOPIX|canonical ISO"):
        writer.copy_event_time_pit_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=None if fault == "before_first" else "2024-01-01",
            date_to=(
                "2023-12-31"
                if fault == "before_first"
                else "2024-12-29"
                if fault == "non_session"
                else "2024-12-31"
            ),
        )


def test_implicit_cutoff_uses_latest_topix_session_not_selected_raw_max(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        assert conn.execute("SELECT max(date) FROM stock_data_raw").fetchone() == (
            "2024-12-30",
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot-implicit-cutoff"))
    _upsert_cutoff_stock(writer)

    writer.copy_event_time_pit_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from="2024-01-01",
        date_to=None,
    )

    conn = duckdb.connect(writer.duckdb_path)
    try:
        assert conn.execute(
            "SELECT materialized_through_date FROM stock_adjustment_bases "
            "WHERE valid_to_exclusive IS NULL"
        ).fetchone() == ("2024-12-31",)
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
        assert result.stock_master_rows == 5
        assert writer._duckdb_store._conn.execute(  # noqa: SLF001
            "SELECT code, date FROM stock_master_daily ORDER BY date"
        ).fetchall() == [
            ("7203", "2024-01-04"),
            ("7203", "2024-02-01"),
            ("7203", "2024-06-28"),
            ("7203", "2024-12-30"),
            ("7203", "2024-12-31"),
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


def test_lineage_retains_incomplete_ohlc_adjustment_fact(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_data_raw (
                code, date, open, high, low, close, volume, adjustment_factor
            ) VALUES ('7203', '2024-12-31', NULL, NULL, NULL, NULL, NULL, 0.25)
            """
        )
        conn.execute("CREATE TEMP TABLE target_codes (code TEXT)")
        conn.execute("INSERT INTO target_codes VALUES ('7203')")

        lineages = list(
            iter_cutoff_lineages(
                conn,
                source_alias="main",
                target_code_table="target_codes",
                cutoff="2024-12-31",
            )
        )

        assert [basis.basis_id for basis in lineages[0].bases] == [
            "event-pit-v1:7203:2024-01-04",
            "event-pit-v1:7203:2024-06-28",
            "event-pit-v1:7203:2024-12-31",
        ]
    finally:
        conn.close()


def test_copy_event_time_pit_accepts_basis_without_source_statements(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute("DELETE FROM statements")
        conn.execute("DELETE FROM statement_metrics_adjusted")
        conn.execute(
            """
            UPDATE daily_valuation
            SET eps = NULL, statement_disclosed_date = NULL
            """
        )
    finally:
        conn.close()

    snapshot_dir = tmp_path / "snapshot-without-statements"
    writer = DatasetWriter(str(snapshot_dir))
    result = _copy(writer, source)
    assert result.statement_metric_rows == 0
    assert result.basis_rows == 2
    assert result.daily_valuation_rows == 4
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()

    _write_manifest(snapshot_dir)
    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        assert reader.resolve_adjustment_basis(
            "7203", "2024-06-28"
        ).basis_id == "event-pit-v1:7203:2024-06-28"
        assert len(
            reader.get_daily_valuation(
                "7203", basis_id="event-pit-v1:7203:2024-06-28"
            )
        ) == 3
    finally:
        reader.close()


def test_copy_event_time_pit_rejects_missing_metric_for_source_statement(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "DELETE FROM statement_metrics_adjusted "
            "WHERE basis_version = 'event-pit-v1:7203:2024-06-28'"
        )
    finally:
        conn.close()

    writer = DatasetWriter(str(tmp_path / "snapshot-missing-expected-metric"))
    with pytest.raises(DatasetSnapshotError, match="adjusted metric coverage"):
        _copy(writer, source)


def test_copy_accepts_column_merged_statement_identity_aliases(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE statements SET type_of_current_period = NULL "
            "WHERE code = '7203' AND disclosed_date = '2024-05-10'"
        )
        conn.execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period
            ) VALUES ('72030', '2024-05-10', 100.0, 'FY')
            """
        )
    finally:
        conn.close()

    writer = DatasetWriter(str(tmp_path / "snapshot-canonical-statement-alias"))

    result = _copy(writer, source)

    assert result.statement_metric_rows == 2
    assert writer.copy_statements_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_to="2024-12-31",
    ) == 1


def _add_weekend_statement(source: Path, *, include_metric: bool) -> None:
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period
            ) VALUES ('7203', '2024-12-31', 110.0, 'FY')
            """
        )
        if include_metric:
            conn.execute(
                """
                INSERT INTO statement_metrics_adjusted (
                    code, disclosed_date, period_end, period_type,
                    price_basis_date, adjusted_eps, basis_version
                ) VALUES (
                    '7203', '2024-12-31', '2024-12-31', 'FY',
                    '2024-06-28', 110.0,
                    'event-pit-v1:7203:2024-06-28'
                )
                """
            )
    finally:
        conn.close()


def test_weekend_cutoff_rejects_missing_source_statement_metric(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    _add_weekend_statement(source, include_metric=False)
    writer = DatasetWriter(str(tmp_path / "snapshot-weekend-writer-reject"))

    with pytest.raises(DatasetSnapshotError, match="adjusted metric coverage"):
        _copy(writer, source)


def test_reader_uses_snapshot_cutoff_after_last_raw_trading_date(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    _add_weekend_statement(source, include_metric=True)
    snapshot_dir = tmp_path / "snapshot-weekend-reader"
    writer = DatasetWriter(str(snapshot_dir))
    _copy(writer, source)
    writer.copy_statements_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_to="2024-12-31",
    )
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()

    _write_manifest(snapshot_dir)
    reader = DatasetSnapshotReader(str(snapshot_dir))
    reader.close()

    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(
            "DELETE FROM statement_metrics_adjusted "
            "WHERE disclosed_date = '2024-12-31'"
        )
    finally:
        conn.close()

    with pytest.raises(
        DatasetManifestValidationError,
        match="adjusted metric coverage is insufficient",
    ):
        _write_manifest(snapshot_dir)


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

    with pytest.raises(DatasetSnapshotError, match="coverage|source basis proof"):
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

    with pytest.raises(DatasetSnapshotError, match="immutable|adjusted_metrics_pit"):
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

    with pytest.raises(DatasetSnapshotError, match="price basis|provenance"):
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

    with pytest.raises(DatasetSnapshotError, match="price basis|provenance"):
        _copy(writer, source)

    assert _target_graph(writer) == before


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "UPDATE stock_data_raw SET date = '2024-01-4' WHERE date = '2024-01-04'",
            "PIT preflight",
        ),
        (
            "UPDATE stock_adjustment_bases SET adjustment_through_date = '2024-01-05' "
            "WHERE valid_from = '2024-01-04'",
            "source basis proof",
        ),
        (
            "UPDATE stock_adjustment_bases SET basis_id = 'wrong-basis' "
            "WHERE valid_from = '2024-01-04'",
            "source basis set mismatch",
        ),
        (
            "UPDATE stock_adjustment_basis_segments SET source_date_from = '2024-1-04' "
            "WHERE source_date_from = '2024-01-04'",
            "canonical ISO",
        ),
        (
            "INSERT INTO statement_metrics_adjusted ("
            "code, disclosed_date, period_end, period_type, price_basis_date, basis_version"
            ") VALUES ('7203', '2024-05-10', '2024-05-09', 'FY', '2024-01-04', "
            "'event-pit-v1:7203:2024-01-04')",
            "exact raw statement identity",
        ),
    ],
)
def test_writer_rejects_noncanonical_or_incoherent_staged_pit_graph(
    tmp_path: Path, sql: str, message: str
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(sql)
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "snapshot"))

    with pytest.raises(DatasetSnapshotError, match=message):
        _copy(writer, source)


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
