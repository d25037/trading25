from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.daily_ranking_event_time_prices import (
    EventTimeSignalRequest,
    build_event_time_signal_sql,
)


def _build_market_v4_fixture(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE stock_data_raw (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, adjustment_factor DOUBLE
        );
        CREATE TABLE stock_master_daily (
            date TEXT, code TEXT, market_code TEXT
        );
        CREATE TABLE stock_adjustment_bases (
            code TEXT, basis_id TEXT, valid_from TEXT,
            valid_to_exclusive TEXT, adjustment_through_date TEXT,
            source_fingerprint TEXT, materialized_through_date TEXT,
            status TEXT
        );
        CREATE TABLE stock_adjustment_basis_segments (
            code TEXT, basis_id TEXT, source_date_from TEXT,
            source_date_to_exclusive TEXT, cumulative_factor DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-04", 100.0, 102.0, 98.0, 100.0, 1_000, 1.0),
            ("11110", "2024-01-04", 100.0, 102.0, 98.0, 100.0, 1_000, 1.0),
            ("1111", "2024-01-05", 55.0, 56.0, 54.0, 55.0, 2_000, 1.0),
        ],
    )
    conn.execute(
        "INSERT INTO stock_master_daily VALUES ('2024-01-05', '11110', '0111')"
    )
    basis_id = "event-pit-v1:1111:2024-01-05"
    conn.execute(
        """
        INSERT INTO stock_adjustment_bases VALUES (
            '1111', ?, '2024-01-05', NULL, '2024-01-05',
            'fingerprint', '2024-01-05', 'ready'
        )
        """,
        (basis_id,),
    )
    conn.executemany(
        "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
        [
            ("1111", basis_id, "2024-01-01", "2024-01-05", 0.5),
            ("1111", basis_id, "2024-01-05", None, 1.0),
        ],
    )
    return conn


def test_event_time_signal_sql_projects_one_signal_basis_without_outcomes(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_fixture(tmp_path / "market.duckdb")
    try:
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-05",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        issues = conn.execute(built.validation_sql, built.validation_params).fetchall()
        rows = conn.execute(
            f"WITH {built.cte_sql} SELECT * FROM {built.relation_name} ORDER BY date",
            built.params,
        ).fetchall()
    finally:
        conn.close()

    assert issues == []
    assert all(not column.startswith("forward_") for column in built.columns)
    assert "outcome" not in built.cte_sql.lower()
    assert rows[0][0:2] == ("1111", "2024-01-04")
    assert rows[0][2:7] == pytest.approx((50.0, 51.0, 49.0, 50.0, 2000))
    assert rows[0][7] is None
    assert rows[1][0] == "1111"
    assert rows[1][1] == "2024-01-05"
    assert rows[1][2:7] == pytest.approx((55.0, 56.0, 54.0, 55.0, 2000))
    assert rows[1][7] == pytest.approx(50.0)
    assert rows[1][10] == pytest.approx(10.0)
    assert rows[1][-1] == "event-pit-v1:1111:2024-01-05"


def test_event_time_signal_sql_reports_conflicting_normalized_aliases(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            "UPDATE stock_data_raw SET close = 999 WHERE code = '11110' AND date = '2024-01-04'"
        )
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-05",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        issues = conn.execute(built.validation_sql, built.validation_params).fetchall()
    finally:
        conn.close()

    assert issues == [("raw_alias_conflict", "1111", "2024-01-04")]


def test_event_time_signal_sql_reports_aliases_with_different_adjustment_factors(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            """
            UPDATE stock_data_raw
            SET adjustment_factor = 0.5
            WHERE code = '11110' AND date = '2024-01-04'
            """
        )
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-05",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        issues = conn.execute(built.validation_sql, built.validation_params).fetchall()
    finally:
        conn.close()

    assert issues == [("raw_alias_conflict", "1111", "2024-01-04")]


@pytest.mark.parametrize(
    ("mutation", "expected_issue"),
    [
        (
            "UPDATE stock_adjustment_bases SET status = 'building'",
            "signal_basis_not_ready",
        ),
        (
            """
            INSERT INTO stock_adjustment_bases VALUES (
                '1111', 'event-pit-v1:1111:2024-01-01', '2024-01-01', NULL,
                '2024-01-01', 'other', '2024-01-05', 'ready'
            )
            """,
            "signal_basis_ambiguous",
        ),
        (
            """
            DELETE FROM stock_adjustment_basis_segments
            WHERE source_date_from = '2024-01-05'
            """,
            "signal_segment_cardinality",
        ),
        (
            """
            UPDATE stock_adjustment_basis_segments
            SET cumulative_factor = 0
            WHERE source_date_from = '2024-01-05'
            """,
            "signal_segment_factor",
        ),
    ],
)
def test_event_time_signal_sql_fails_closed_for_incomplete_lineage(
    tmp_path: Path,
    mutation: str,
    expected_issue: str,
) -> None:
    conn = _build_market_v4_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(mutation)
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-05",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        issues = conn.execute(built.validation_sql, built.validation_params).fetchall()
    finally:
        conn.close()

    assert expected_issue in {str(row[0]) for row in issues}
