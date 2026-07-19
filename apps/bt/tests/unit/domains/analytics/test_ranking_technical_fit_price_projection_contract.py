from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.ranking_technical_fit_price_projection import (
    create_event_time_price_relations,
)


def _build_price_projection_db(
    db_path: Path,
    *,
    segment_failure: str | None = None,
    include_invalid_intermediate_bar: bool = False,
) -> Path:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_data_raw (
                code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume BIGINT, adjustment_factor DOUBLE
            );
            CREATE TABLE stock_data (
                code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume BIGINT
            );
            CREATE TABLE stock_master_daily (
                date TEXT, code TEXT, company_name TEXT, market_code TEXT
            );
            CREATE TABLE daily_valuation (
                code TEXT, date TEXT, basis_version TEXT
            );
            CREATE TABLE stock_adjustment_bases (
                code TEXT, basis_id TEXT, valid_from DATE,
                valid_to_exclusive DATE, adjustment_through_date DATE,
                source_fingerprint TEXT, materialized_through_date DATE,
                status TEXT
            );
            CREATE TABLE stock_adjustment_basis_segments (
                code TEXT, basis_id TEXT, source_date_from TEXT,
                source_date_to_exclusive TEXT, cumulative_factor DOUBLE
            );
            CREATE TABLE topix_data (
                date TEXT, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE
            )
            """
        )
        raw_rows = (
            [
                ("1111", "2024-01-04", 100.0, 101.0, 99.0, 100.0, 1_000, 1.0),
                ("1111", "2024-01-05", 0.0, 0.0, 0.0, 0.0, -1, 1.0),
                ("1111", "2024-01-08", 110.0, 111.0, 109.0, 110.0, 1_100, 1.0),
            ]
            if include_invalid_intermediate_bar
            else [
                ("1111", "2024-01-04", 100.0, 101.0, 99.0, 100.0, 1_000, 1.0),
                ("1111", "2024-01-05", 50.0, 51.0, 49.0, 50.0, 2_000, 1.0),
            ]
        )
        conn.executemany("INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)", raw_rows)
        conn.executemany(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("1111", "2024-01-04", 999.0, 999.0, 999.0, 999.0, 9),
                ("1111", "2024-01-05", 1.0, 1.0, 1.0, 1.0, 9),
            ],
        )
        stock_master_rows = [
            ("2024-01-04", "1111", "Alpha", "0111"),
            ("2024-01-05", "1111", "Alpha", "0111"),
        ]
        if include_invalid_intermediate_bar:
            stock_master_rows.append(("2024-01-08", "1111", "Alpha", "0111"))
        conn.executemany("INSERT INTO stock_master_daily VALUES (?, ?, ?, ?)", stock_master_rows)
        signal_basis = "event-pit-v1:1111:2024-01-04"
        completion_date = "2024-01-08" if include_invalid_intermediate_bar else "2024-01-05"
        completion_basis = f"event-pit-v1:1111:{completion_date}"
        conn.executemany(
            "INSERT INTO daily_valuation VALUES (?, ?, ?)",
            [
                ("1111", "2024-01-04", signal_basis),
                ("1111", completion_date, completion_basis),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "1111", signal_basis, "2024-01-04", completion_date,
                    "2024-01-04", "signal", "2024-01-04", "ready",
                ),
                (
                    "1111", completion_basis, completion_date, None,
                    completion_date, "completion", completion_date, "ready",
                ),
            ],
        )
        segments = [
            ("1111", signal_basis, "2024-01-04", None, 1.0),
            (
                "1111",
                completion_basis,
                "2024-01-04",
                completion_date,
                1.0 if include_invalid_intermediate_bar else 0.5,
            ),
            ("1111", completion_basis, completion_date, None, 1.0),
        ]
        if segment_failure == "missing":
            segments = [row for row in segments if row[2] != "2024-01-04" or row[1] == signal_basis]
        elif segment_failure == "overlap":
            segments.append(("1111", completion_basis, "2024-01-04", None, 0.5))
        elif segment_failure == "invalid":
            segments[1] = (*segments[1][:-1], 0.0)
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
            segments,
        )
        topix_rows = [
            ("2024-01-04", 100.0, 100.0, 100.0, 100.0),
            ("2024-01-05", 110.0, 110.0, 110.0, 110.0),
        ]
        if include_invalid_intermediate_bar:
            topix_rows.append(("2024-01-08", 120.0, 120.0, 120.0, 120.0))
        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    finally:
        conn.close()
    return db_path


def _create_price_relations(conn: duckdb.DuckDBPyConnection) -> None:
    create_event_time_price_relations(
        conn,
        query_start="2024-01-04",
        query_end="2024-01-05",
        analysis_start_date="2024-01-04",
        analysis_end_date="2024-01-04",
        horizons=(1,),
    )


def test_event_time_price_projection_ignores_poisoned_stock_data_and_uses_completion_basis(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        relations, audit = create_event_time_price_relations(
            conn,
            query_start="2024-01-04",
            query_end="2024-01-05",
            analysis_start_date="2024-01-04",
            analysis_end_date="2024-01-04",
            horizons=(1,),
        )
        signal = conn.execute(
            f"SELECT close FROM {relations.signal_features} WHERE date = '2024-01-04'"
        ).fetchone()
        outcome = conn.execute(
            f"SELECT forward_close_return_1d_pct FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert signal == (100.0,)
    assert outcome == pytest.approx((0.0,))
    assert audit.no_stock_data_fallback is True
    assert audit.signal_basis_row_count == 1
    assert audit.completion_basis_row_count == 1
    assert audit.signal_basis_sha256 != audit.completion_basis_sha256


def test_event_time_price_projection_skips_invalid_raw_bars_when_counting_horizon(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(
        tmp_path / "market.duckdb",
        include_invalid_intermediate_bar=True,
    )
    conn = duckdb.connect(str(db_path))
    try:
        relations, audit = create_event_time_price_relations(
            conn,
            query_start="2024-01-04",
            query_end="2024-01-08",
            analysis_start_date="2024-01-04",
            analysis_end_date="2024-01-04",
            horizons=(1,),
        )
        outcome = conn.execute(
            f"SELECT forward_outcome_completion_date_1d, "
            f"forward_close_return_1d_pct, "
            f"forward_close_excess_return_1d_pct "
            f"FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert str(outcome[0]) == "2024-01-08"
    assert outcome[1] == pytest.approx(10.0)
    assert outcome[2] == pytest.approx(-10.0)
    assert outcome[2] != pytest.approx(0.0)
    assert audit.canonical_raw_row_count == 3
    assert audit.completed_outcome_row_count == 1


@pytest.mark.parametrize("segment_failure", ["missing", "overlap", "invalid"])
def test_event_time_price_projection_fails_closed_on_segment_integrity(
    tmp_path: Path,
    segment_failure: str,
) -> None:
    db_path = _build_price_projection_db(
        tmp_path / "market.duckdb", segment_failure=segment_failure
    )
    conn = duckdb.connect(str(db_path))
    try:
        with pytest.raises(RuntimeError, match="segment"):
            _create_price_relations(conn)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("basis_id", "error"),
    [
        (
            "event-pit-v1:1111:2024-01-04",
            "price projection signal basis is not ready/materialized",
        ),
        (
            "event-pit-v1:1111:2024-01-05",
            "price projection completion basis is not ready/materialized through completion",
        ),
    ],
)
def test_event_time_price_projection_rejects_adjustment_frontier_mismatch(
    tmp_path: Path,
    basis_id: str,
    error: str,
) -> None:
    db_path = _build_price_projection_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE stock_adjustment_bases
            SET adjustment_through_date = valid_from + INTERVAL 1 DAY
            WHERE basis_id = ?
            """,
            [basis_id],
        )

        with pytest.raises(RuntimeError, match=error):
            _create_price_relations(conn)
    finally:
        conn.close()
