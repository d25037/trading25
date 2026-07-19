from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.ranking_technical_fit_price_projection import (
    FORWARD_OUTCOME_RELATION,
    SIGNAL_FEATURE_RELATION,
    create_event_time_price_relations,
)


_LEGACY_SIGNAL_SCHEMA = (
    ("code", "VARCHAR"),
    ("date", "DATE"),
    ("price_basis_id", "VARCHAR"),
    ("open", "DOUBLE"),
    ("high", "DOUBLE"),
    ("low", "DOUBLE"),
    ("close", "DOUBLE"),
    ("volume", "BIGINT"),
    ("med_adv60_jpy", "DOUBLE"),
    ("med_adv60_sessions", "BIGINT"),
    ("close_lag_20d", "DOUBLE"),
    ("close_lag_60d", "DOUBLE"),
    ("close_lag_120d", "DOUBLE"),
    ("close_lag_150d", "DOUBLE"),
    ("close_lag_252d", "DOUBLE"),
    ("close_lag_504d", "DOUBLE"),
    ("atr20", "DOUBLE"),
    ("atr20_sessions", "BIGINT"),
    ("atr60", "DOUBLE"),
    ("atr60_sessions", "BIGINT"),
    ("atr20_pct", "DOUBLE"),
    ("atr60_pct", "DOUBLE"),
    ("atr20_to_atr60", "DOUBLE"),
    ("atr20_change_20d_pct", "DOUBLE"),
    ("recent_return_20d_pct", "DOUBLE"),
    ("recent_return_60d_pct", "DOUBLE"),
    ("recent_return_120d_pct", "DOUBLE"),
    ("recent_return_150d_pct", "DOUBLE"),
    ("recent_return_252d_pct", "DOUBLE"),
    ("recent_return_504d_pct", "DOUBLE"),
    ("ols_move_20d_pct", "DOUBLE"),
    ("ols_r2_20", "DOUBLE"),
    ("ols_move_60d_pct", "DOUBLE"),
    ("ols_r2_60", "DOUBLE"),
)
_LEGACY_OUTCOME_SCHEMA = (
    ("code", "VARCHAR"),
    ("date", "DATE"),
    ("forward_outcome_completion_date_1d", "DATE"),
    ("forward_close_return_1d_pct", "DOUBLE"),
    ("forward_close_excess_return_1d_pct", "DOUBLE"),
    ("completion_basis_id_1d", "VARCHAR"),
)
_LEGACY_NORMAL_HASHES = {
    "signal_basis_sha256": "7cfe84ab9c4eb275ef6cb0b71749467133d89d1f4539345d0690a78663b08c76",
    "signal_segment_sha256": "1dbc77c37f7a24625c00235449a28df7b124150c5e5806f86c022318eff0c362",
    "completion_basis_sha256": "f76499adce7796dfbd32e235a672a7f7639c5d18965cc8a4ebb859ebfbc647c1",
    "completion_segment_sha256": "dcff49aca689b6ca68c28ee3379eaacb1d5142d2d9c118e5d3c5e3adbd06ae4a",
    "forward_outcome_sha256": "e7e6d2d85c190d47931348f4de92722c4ee9af3c902881909bc202e2e9ab90ec",
    "price_projection_sha256": "24bc681c4a5da044499a070cae93d758492d44c3d112ed5e53de761070d235a8",
}
_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


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
            CREATE TABLE market_schema_version (
                version INTEGER, applied_at TEXT, notes TEXT
            );
            INSERT INTO market_schema_version VALUES (4, NULL, NULL);
            CREATE TABLE sync_metadata (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
            );
            INSERT INTO sync_metadata VALUES (
                'stock_price_adjustment_mode',
                'local_projection_v2_event_time',
                NULL
            );
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
            );
            CREATE TABLE indices_data (
                code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
                close DOUBLE, volume BIGINT
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
        conn.executemany(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "N225_UNDERPX",
                    date,
                    close,
                    close,
                    close,
                    close,
                    0,
                )
                for date, _open, _high, _low, close in topix_rows
            ],
        )
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


def _describe_relation(
    conn: duckdb.DuckDBPyConnection,
    relation: str,
) -> tuple[tuple[str, str], ...]:
    return tuple(
        (str(row[0]), str(row[1]))
        for row in conn.execute(f"DESCRIBE {relation}").fetchall()
    )


def _legacy_projection(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[object, object]:
    return create_event_time_price_relations(
        conn,
        query_start="2024-01-04",
        query_end="2024-01-05",
        analysis_start_date="2024-01-04",
        analysis_end_date="2024-01-04",
        horizons=(1,),
    )


def test_technical_fit_compatibility_preserves_exact_schema_and_hashes(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        relations, audit = _legacy_projection(conn)
        signal_schema = _describe_relation(conn, relations.signal_features)
        outcome_schema = _describe_relation(conn, relations.forward_outcomes)
        outcome_row = conn.execute(
            f"SELECT * FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert signal_schema == _LEGACY_SIGNAL_SCHEMA
    assert outcome_schema == _LEGACY_OUTCOME_SCHEMA
    assert len(outcome_row) == len(_LEGACY_OUTCOME_SCHEMA)
    assert relations.signal_features == SIGNAL_FEATURE_RELATION
    assert relations.forward_outcomes == FORWARD_OUTCOME_RELATION
    for field, expected in _LEGACY_NORMAL_HASHES.items():
        assert getattr(audit, field) == expected


def test_technical_fit_compatibility_preserves_invalid_signal_bar_audit(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE stock_data_raw
            SET open = 0, high = 0, low = 0, close = 0, volume = -1
            WHERE date = '2024-01-04'
            """
        )
        relations, audit = _legacy_projection(conn)
        signal_rows = conn.execute(
            f"SELECT count(*) FROM {relations.signal_features}"
        ).fetchone()[0]
        outcome_rows = conn.execute(
            f"SELECT count(*) FROM {relations.forward_outcomes}"
        ).fetchone()[0]
    finally:
        conn.close()

    assert signal_rows == 0
    assert outcome_rows == 0
    assert audit.canonical_raw_row_count == 2
    assert audit.signal_feature_row_count == 0
    assert audit.outcome_request_row_count == 0
    assert audit.completed_outcome_row_count == 0
    assert audit.signal_basis_row_count == 1
    assert audit.signal_segment_row_count == 1
    assert audit.completion_basis_row_count == 0
    assert audit.completion_segment_row_count == 0
    assert audit.signal_basis_sha256 == _LEGACY_NORMAL_HASHES["signal_basis_sha256"]
    assert audit.signal_segment_sha256 == _LEGACY_NORMAL_HASHES["signal_segment_sha256"]
    assert audit.completion_basis_sha256 == _EMPTY_SHA256
    assert audit.completion_segment_sha256 == _EMPTY_SHA256
    assert audit.forward_outcome_sha256 == _EMPTY_SHA256
    assert (
        audit.price_projection_sha256
        == "42229397d2a02d65a7cf87fd6f7a2e8fca4405973433bda0a715d424f664bd10"
    )


def test_first_failed_compatibility_build_publishes_no_legacy_relation(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(
        tmp_path / "market.duckdb",
        segment_failure="missing",
    )
    conn = duckdb.connect(str(db_path))
    try:
        with pytest.raises(RuntimeError, match="segment"):
            _legacy_projection(conn)
        published = conn.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_name IN (?, ?)
            ORDER BY table_name
            """,
            [SIGNAL_FEATURE_RELATION, FORWARD_OUTCOME_RELATION],
        ).fetchall()
    finally:
        conn.close()

    assert published == []


def test_failed_compatibility_rebuild_keeps_prior_signal_and_outcome_generation(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        _legacy_projection(conn)
        prior_signal = conn.execute(f"SELECT * FROM {SIGNAL_FEATURE_RELATION}").fetchall()
        prior_outcome = conn.execute(f"SELECT * FROM {FORWARD_OUTCOME_RELATION}").fetchall()
        conn.execute(
            """
            UPDATE stock_data_raw
            SET open = 120, high = 121, low = 119, close = 120
            WHERE date = '2024-01-04'
            """
        )
        conn.execute(
            """
            UPDATE stock_adjustment_basis_segments
            SET cumulative_factor = 0
            WHERE basis_id = 'event-pit-v1:1111:2024-01-05'
              AND source_date_from = '2024-01-04'
            """
        )
        with pytest.raises(RuntimeError, match="completion segment factor"):
            _legacy_projection(conn)
        after_signal = conn.execute(f"SELECT * FROM {SIGNAL_FEATURE_RELATION}").fetchall()
        after_outcome = conn.execute(f"SELECT * FROM {FORWARD_OUTCOME_RELATION}").fetchall()
    finally:
        conn.close()

    assert after_signal == prior_signal
    assert after_outcome == prior_outcome
