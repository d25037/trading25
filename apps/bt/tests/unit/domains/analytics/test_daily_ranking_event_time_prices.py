from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.daily_ranking_event_time_prices import (
    DailyRankingPriceRequest,
    EventTimeSignalRequest,
    build_daily_ranking_event_time_prices,
    build_event_time_signal_sql,
    daily_ranking_valid_raw_bar_sql,
)


def test_valid_raw_bar_predicate_is_exact_and_qualifiable() -> None:
    assert daily_ranking_valid_raw_bar_sql() == (
        "open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0"
    )
    assert daily_ranking_valid_raw_bar_sql("raw") == (
        "raw.open > 0 AND raw.high > 0 AND raw.low > 0 AND raw.close > 0 "
        "AND raw.volume >= 0"
    )
    with pytest.raises(ValueError, match="invalid raw-bar qualifier"):
        daily_ranking_valid_raw_bar_sql("raw;drop")


def _install_market_v4_metadata(conn: duckdb.DuckDBPyConnection) -> None:
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
        )
        """
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


def _build_sparse_forward_outcome_fixture(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
    _install_market_v4_metadata(conn)
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
        );
        CREATE TABLE indices_data (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        )
        """
    )
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-04", 100.0, 101.0, 99.0, 100.0, 1_000, 1.0),
            ("1111", "2024-01-08", 60.0, 61.0, 59.0, 60.0, 2_000, 1.0),
        ],
    )
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-04", 999.0, 999.0, 999.0, 999.0, 9),
            ("1111", "2024-01-08", 1.0, 1.0, 1.0, 1.0, 9),
        ],
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?)",
        [
            ("2024-01-04", "1111", "Alpha", "0111"),
            ("2024-01-08", "1111", "Alpha", "0111"),
        ],
    )
    signal_basis = "event-pit-v1:1111:2024-01-04"
    completion_basis = "event-pit-v1:1111:2024-01-08"
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?)",
        [
            ("1111", "2024-01-04", signal_basis),
            ("1111", "2024-01-08", completion_basis),
        ],
    )
    conn.executemany(
        "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                signal_basis,
                "2024-01-04",
                "2024-01-08",
                "2024-01-04",
                "signal",
                "2024-01-04",
                "ready",
            ),
            (
                "1111",
                completion_basis,
                "2024-01-08",
                None,
                "2024-01-08",
                "completion",
                "2024-01-08",
                "ready",
            ),
        ],
    )
    conn.executemany(
        "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
        [
            ("1111", signal_basis, "2024-01-04", None, 1.0),
            ("1111", completion_basis, "2024-01-04", "2024-01-08", 0.5),
            ("1111", completion_basis, "2024-01-08", None, 1.0),
        ],
    )
    conn.executemany(
        "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
        [
            ("2024-01-04", 100.0, 100.0, 100.0, 100.0),
            ("2024-01-05", 110.0, 110.0, 110.0, 110.0),
            ("2024-01-08", 120.0, 120.0, 120.0, 120.0),
        ],
    )
    conn.executemany(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("N225_UNDERPX", "2024-01-04", 1_000.0, 1_000.0, 1_000.0, 1_000.0, 0),
            ("N225_UNDERPX", "2024-01-05", 1_500.0, 1_500.0, 1_500.0, 1_500.0, 0),
            ("N225_UNDERPX", "2024-01-08", 1_100.0, 1_100.0, 1_100.0, 1_100.0, 0),
        ],
    )
    return conn


def test_forward_outcomes_align_all_endpoints_to_stock_completion_date(
    tmp_path: Path,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_event_time_prices(
            conn,
            DailyRankingPriceRequest(
                namespace="sparse_projection",
                query_start="2024-01-04",
                query_end="2024-01-08",
                analysis_start_date="2024-01-04",
                analysis_end_date="2024-01-04",
                horizons=(1,),
            ),
        )
        signal = conn.execute(
            f"SELECT price_basis_id, close FROM {relations.signal_features}"
        ).fetchone()
        history_bounds = conn.execute(
            f"SELECT min(date), max(date), count(*), min(close), max(close) "
            f"FROM {relations.price_history}"
        ).fetchone()
        outcome = conn.execute(
            f"""
            SELECT forward_outcome_completion_date_1d,
                   completion_basis_id_1d,
                   forward_close_return_1d_pct,
                   forward_close_excess_return_1d_pct,
                   forward_close_n225_excess_return_1d_pct,
                   forward_next_open_return_1d_pct,
                   forward_next_open_excess_return_1d_pct
            FROM {relations.forward_outcomes}
            """
        ).fetchone()
    finally:
        conn.close()

    assert signal == ("event-pit-v1:1111:2024-01-04", 100.0)
    assert tuple(str(value) for value in history_bounds[:2]) == (
        "2024-01-04",
        "2024-01-04",
    )
    assert history_bounds[2] == 1
    assert history_bounds[3:] == pytest.approx((100.0, 100.0))
    assert str(outcome[0]) == "2024-01-08"
    assert outcome[1] == "event-pit-v1:1111:2024-01-08"
    assert outcome[2:] == pytest.approx((20.0, 0.0, 10.0, 0.0, 0.0))
    assert relations.signal_features.startswith("sparse_projection_g_")
    assert relations.signal_features.endswith("_signal_price_features")
    assert relations.forward_outcomes.startswith("sparse_projection_g_")
    assert relations.forward_outcomes.endswith("_forward_price_outcomes")
    assert relations.price_history.startswith("sparse_projection_g_")
    assert relations.price_history.endswith("_price_history")
    assert relations.lineage.no_stock_data_fallback is True


def test_forward_projection_exposes_cardinality_diagnostics(tmp_path: Path) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        request = DailyRankingPriceRequest(
            namespace="diagnostic_projection",
            query_start="2024-01-04",
            query_end="2024-01-08",
            analysis_start_date="2024-01-04",
            analysis_end_date="2024-01-04",
            horizons=(1,),
        )
        relations = build_daily_ranking_event_time_prices(conn, request)
    finally:
        conn.close()

    diagnostics = relations.diagnostics
    assert diagnostics.signal_feature_rows == diagnostics.signal_request_rows
    assert diagnostics.outcome_request_rows == diagnostics.signal_request_rows * len(
        request.horizons
    )
    assert diagnostics.endpoint_rows == 3 * diagnostics.completed_request_rows
    assert diagnostics.forward_outcome_rows <= diagnostics.signal_request_rows
    assert diagnostics.signal_feature_schema[0:3] == ("code", "date", "price_basis_id")
    assert diagnostics.forward_outcome_schema[0:2] == ("code", "date")


def test_forward_projection_rejects_unvalidated_relation_namespace() -> None:
    with pytest.raises(ValueError, match="namespace"):
        DailyRankingPriceRequest(
            namespace="unsafe;drop_table",
            query_start=None,
            query_end=None,
            analysis_start_date=None,
            analysis_end_date=None,
            horizons=(1,),
        )


def _generic_price_request(
    namespace: str,
    *,
    horizons: tuple[int, ...] = (1,),
) -> DailyRankingPriceRequest:
    return DailyRankingPriceRequest(
        namespace=namespace,
        query_start="2024-01-04",
        query_end="2024-01-08",
        analysis_start_date="2024-01-04",
        analysis_end_date="2024-01-04",
        horizons=horizons,
    )


def _temporary_relations_with_prefix(
    conn: duckdb.DuckDBPyConnection,
    prefix: str,
) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE ?
            ORDER BY table_name
            """,
            [f"{prefix}%"],
        ).fetchall()
    )


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (
            "UPDATE market_schema_version SET version = 3",
            "required schema version 4",
        ),
        (
            """
            UPDATE sync_metadata
            SET value = 'local_projection_v1'
            WHERE key = 'stock_price_adjustment_mode'
            """,
            "local_projection_v2_event_time",
        ),
        ("DROP TABLE market_schema_version", "market_schema_version"),
    ],
)
def test_forward_projection_requires_market_v4_metadata(
    tmp_path: Path,
    mutation: str,
    expected: str,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(mutation)
        with pytest.raises(RuntimeError, match=expected):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("invalid_metadata"),
            )
    finally:
        conn.close()


def test_forward_projection_requires_market_v4_columns(tmp_path: Path) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute("ALTER TABLE daily_valuation DROP COLUMN basis_version")
        with pytest.raises(RuntimeError, match="missing required Market v4 columns"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("invalid_columns"),
            )
    finally:
        conn.close()


def test_forward_projection_namespaces_coexist_and_rebuilds_are_generation_unique(
    tmp_path: Path,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        alpha_v1 = build_daily_ranking_event_time_prices(
            conn, _generic_price_request("alpha_projection")
        )
        beta = build_daily_ranking_event_time_prices(
            conn, _generic_price_request("beta_projection")
        )
        alpha_v2 = build_daily_ranking_event_time_prices(
            conn, _generic_price_request("alpha_projection")
        )
        alpha_v1_rows = conn.execute(
            f"SELECT * FROM {alpha_v1.forward_outcomes}"
        ).fetchall()
        alpha_v2_rows = conn.execute(
            f"SELECT * FROM {alpha_v2.forward_outcomes}"
        ).fetchall()
    finally:
        conn.close()

    assert alpha_v1.signal_features != alpha_v2.signal_features
    assert alpha_v1.forward_outcomes != alpha_v2.forward_outcomes
    assert alpha_v1.signal_features.startswith("alpha_projection_g_")
    assert alpha_v2.signal_features.startswith("alpha_projection_g_")
    assert beta.signal_features.startswith("beta_projection_g_")
    assert alpha_v1_rows == alpha_v2_rows


def test_first_failed_projection_build_leaves_no_partial_relations(
    tmp_path: Path,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            """
            DELETE FROM stock_adjustment_basis_segments
            WHERE basis_id = 'event-pit-v1:1111:2024-01-08'
              AND source_date_from = '2024-01-04'
            """
        )
        with pytest.raises(RuntimeError, match="completion segment"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("first_failure"),
            )
        remaining = _temporary_relations_with_prefix(conn, "first_failure")
    finally:
        conn.close()

    assert remaining == ()


def test_failed_rebuild_preserves_prior_complete_generation(tmp_path: Path) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        prior = build_daily_ranking_event_time_prices(
            conn,
            _generic_price_request("stable_projection"),
        )
        prior_signal = conn.execute(f"SELECT * FROM {prior.signal_features}").fetchall()
        prior_outcome = conn.execute(
            f"SELECT * FROM {prior.forward_outcomes}"
        ).fetchall()
        prior_relations = _temporary_relations_with_prefix(conn, "stable_projection")
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
            WHERE basis_id = 'event-pit-v1:1111:2024-01-08'
              AND source_date_from = '2024-01-04'
            """
        )
        with pytest.raises(RuntimeError, match="completion segment factor"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("stable_projection"),
            )
        after_relations = _temporary_relations_with_prefix(conn, "stable_projection")
        after_signal = conn.execute(f"SELECT * FROM {prior.signal_features}").fetchall()
        after_outcome = conn.execute(
            f"SELECT * FROM {prior.forward_outcomes}"
        ).fetchall()
    finally:
        conn.close()

    assert after_relations == prior_relations
    assert after_signal == prior_signal
    assert after_outcome == prior_outcome


@pytest.mark.parametrize(
    "duplicate_sql",
    [
        """
        INSERT INTO topix_data
        VALUES ('2024-01-08', 999, 999, 999, 999)
        """,
        """
        INSERT INTO indices_data
        VALUES ('N225_UNDERPX', '2024-01-08', 999, 999, 999, 999, 0)
        """,
    ],
)
def test_forward_projection_rejects_duplicate_benchmark_endpoints(
    tmp_path: Path,
    duplicate_sql: str,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(duplicate_sql)
        with pytest.raises(RuntimeError, match="benchmark.*duplicate"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("duplicate_benchmark"),
            )
    finally:
        conn.close()


def test_forward_projection_allows_missing_n225_rows_with_null_outcome(
    tmp_path: Path,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute("DELETE FROM indices_data")
        relations = build_daily_ranking_event_time_prices(
            conn,
            _generic_price_request("missing_n225"),
        )
        outcome = conn.execute(
            f"SELECT forward_close_n225_excess_return_1d_pct "
            f"FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert outcome == (None,)
    assert relations.diagnostics.n225_benchmark_rows == 0
    assert relations.diagnostics.topix_benchmark_rows == 3


def test_next_open_outcome_uses_stock_entry_date_and_nulls_missing_topix_endpoint(
    tmp_path: Path,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    completion_date = "2024-01-10"
    completion_basis = "event-pit-v1:1111:2024-01-08"
    try:
        conn.execute(
            "INSERT INTO stock_data_raw VALUES ('1111', ?, 60, 61, 59, 60, 2000, 1.0)",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO stock_master_daily VALUES (?, '1111', 'Alpha', '0111')",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO daily_valuation VALUES ('1111', ?, ?)",
            [completion_date, completion_basis],
        )
        conn.execute(
            "UPDATE stock_adjustment_bases "
            "SET materialized_through_date = ? "
            "WHERE basis_id = ?",
            [completion_date, completion_basis],
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, 130, 130, 130, 130)",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO indices_data VALUES "
            "('N225_UNDERPX', ?, 1300, 1300, 1300, 1300, 0)",
            [completion_date],
        )
        request = DailyRankingPriceRequest(
            namespace="complete_topix_next_open",
            query_start="2024-01-04",
            query_end=completion_date,
            analysis_start_date="2024-01-04",
            analysis_end_date="2024-01-04",
            horizons=(2,),
        )
        complete_relations = build_daily_ranking_event_time_prices(conn, request)
        conn.execute("DELETE FROM topix_data WHERE date = '2024-01-08'")
        relations = build_daily_ranking_event_time_prices(
            conn,
            DailyRankingPriceRequest(
                namespace="missing_topix_next_open",
                query_start=request.query_start,
                query_end=request.query_end,
                analysis_start_date=request.analysis_start_date,
                analysis_end_date=request.analysis_end_date,
                horizons=request.horizons,
            ),
        )
        outcome = conn.execute(
            f"SELECT forward_next_open_return_2d_pct, "
            f"forward_next_open_excess_return_2d_pct "
            f"FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert outcome == (pytest.approx(0.0), None)
    assert (
        relations.lineage.forward_outcome_sha256
        == complete_relations.lineage.forward_outcome_sha256
    )
    assert (
        relations.lineage.next_open_outcome_sha256
        != complete_relations.lineage.next_open_outcome_sha256
    )
    assert relations.lineage.completion_basis_policy == (
        "exact_completion_date_basis_applied_to_signal_entry_and_completion_endpoints"
    )
    assert relations.lineage.next_open_integrity_policy == (
        "exact_stock_entry_session_and_topix_entry_endpoint_no_backfill"
    )
    manifest = relations.lineage.to_manifest_payload()
    assert manifest["next_open_outcome_sha256"] == (
        relations.lineage.next_open_outcome_sha256
    )
    assert manifest["next_open_integrity_policy"] == (
        relations.lineage.next_open_integrity_policy
    )


@pytest.mark.parametrize(
    ("pre_completion_factor", "expected_stock", "expected_excess"),
    ((0.5, 200.0, 175.0), (2.0, -25.0, -50.0)),
)
def test_next_open_outcome_projects_split_or_reverse_split_with_completion_basis(
    tmp_path: Path,
    pre_completion_factor: float,
    expected_stock: float,
    expected_excess: float,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    completion_date = "2024-01-10"
    completion_basis = "event-pit-v1:1111:2024-01-10"
    try:
        conn.execute(
            "UPDATE stock_adjustment_bases SET valid_to_exclusive = ? "
            "WHERE basis_id = 'event-pit-v1:1111:2024-01-08'",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO stock_data_raw VALUES ('1111', ?, 90, 91, 89, 90, 3000, 1.0)",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO stock_data VALUES ('1111', ?, 999, 999, 999, 999, 9)",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO stock_master_daily VALUES (?, '1111', 'Alpha', '0111')",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO daily_valuation VALUES ('1111', ?, ?)",
            [completion_date, completion_basis],
        )
        conn.execute(
            "INSERT INTO stock_adjustment_bases VALUES "
            "('1111', ?, ?, NULL, ?, 'completion-2', ?, 'ready')",
            [completion_basis, completion_date, completion_date, completion_date],
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
            [
                ("1111", completion_basis, "2024-01-04", "2024-01-08", 0.5),
                (
                    "1111",
                    completion_basis,
                    "2024-01-08",
                    completion_date,
                    pre_completion_factor,
                ),
                ("1111", completion_basis, completion_date, None, 1.0),
            ],
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, 145, 151, 144, 150)",
            [completion_date],
        )
        conn.execute(
            "INSERT INTO indices_data VALUES "
            "('N225_UNDERPX', ?, 1200, 1200, 1200, 1200, 0)",
            [completion_date],
        )

        relations = build_daily_ranking_event_time_prices(
            conn,
            DailyRankingPriceRequest(
                namespace=f"next_open_factor_{str(pre_completion_factor).replace('.', '_')}",
                query_start="2024-01-04",
                query_end=completion_date,
                analysis_start_date="2024-01-04",
                analysis_end_date="2024-01-04",
                horizons=(2,),
            ),
        )
        outcome = conn.execute(
            f"SELECT forward_outcome_completion_date_2d, "
            f"forward_next_open_return_2d_pct, "
            f"forward_next_open_excess_return_2d_pct "
            f"FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert str(outcome[0]) == completion_date
    assert outcome[1:] == pytest.approx((expected_stock, expected_excess))


@pytest.mark.parametrize("conflicting", [False, True])
def test_forward_projection_validates_normalized_raw_aliases(
    tmp_path: Path,
    conflicting: bool,
) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        alias_rows = conn.execute(
            """
            SELECT '11110', date, open, high, low, close, volume, adjustment_factor
            FROM stock_data_raw
            ORDER BY date
            """
        ).fetchall()
        conn.executemany(
            "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            alias_rows,
        )
        if conflicting:
            conn.execute(
                """
                UPDATE stock_data_raw SET close = 999
                WHERE code = '11110' AND date = '2024-01-04'
                """
            )
            with pytest.raises(RuntimeError, match="alias conflict"):
                build_daily_ranking_event_time_prices(
                    conn,
                    _generic_price_request("raw_aliases"),
                )
        else:
            relations = build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("raw_aliases"),
            )
            assert relations.diagnostics.canonical_raw_rows == 2
    finally:
        conn.close()


def test_forward_projection_keeps_incomplete_horizons_null(tmp_path: Path) -> None:
    conn = _build_sparse_forward_outcome_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_event_time_prices(
            conn,
            _generic_price_request("incomplete_horizon", horizons=(1, 2)),
        )
        outcome = conn.execute(
            f"""
            SELECT forward_outcome_completion_date_1d,
                   forward_close_return_1d_pct,
                   forward_outcome_completion_date_2d,
                   forward_close_return_2d_pct
            FROM {relations.forward_outcomes}
            """
        ).fetchone()
    finally:
        conn.close()

    assert str(outcome[0]) == "2024-01-08"
    assert outcome[1] == pytest.approx(20.0)
    assert outcome[2:] == (None, None)
    assert relations.diagnostics.outcome_request_rows == 2
    assert relations.diagnostics.completed_request_rows == 1
    assert relations.diagnostics.endpoint_rows == 3
