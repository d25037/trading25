"""Market v5 provider-vintage tests for Daily Ranking prices."""

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
from src.shared.provider_stock_window import provider_stock_source_fingerprint


def _provider_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "1111",
            "date": "2024-01-04",
            "open": 100.0,
            "high": 102.0,
            "low": 98.0,
            "close": 100.0,
            "volume": 1_000,
            "turnover_value": 100_000.0,
            "adjustment_factor": 1.0,
            "adjusted_open": 50.0,
            "adjusted_high": 51.0,
            "adjusted_low": 49.0,
            "adjusted_close": 50.0,
            "adjusted_volume": 2_000,
        },
        {
            "code": "1111",
            "date": "2024-01-08",
            "open": 60.0,
            "high": 61.0,
            "low": 59.0,
            "close": 60.0,
            "volume": 2_000,
            "turnover_value": 120_000.0,
            "adjustment_factor": 0.5,
            "adjusted_open": 60.0,
            "adjusted_high": 61.0,
            "adjusted_low": 59.0,
            "adjusted_close": 60.0,
            "adjusted_volume": 2_000,
        },
    ]


def _build_market_v5_fixture(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE market_schema_version (version INTEGER);
        INSERT INTO market_schema_version VALUES (5);
        CREATE TABLE sync_metadata (key TEXT, value TEXT);
        INSERT INTO sync_metadata VALUES (
            'stock_price_adjustment_mode', 'provider_adjusted_v1'
        );
        CREATE TABLE stock_data_raw (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, turnover_value DOUBLE,
            adjustment_factor DOUBLE, adjusted_open DOUBLE,
            adjusted_high DOUBLE, adjusted_low DOUBLE, adjusted_close DOUBLE,
            adjusted_volume BIGINT
        );
        CREATE TABLE stock_data (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        );
        CREATE TABLE stock_master_daily (
            date TEXT, code TEXT, company_name TEXT, market_code TEXT
        );
        CREATE TABLE stock_provider_windows (
            code TEXT, coverage_start TEXT, coverage_end TEXT,
            provider_as_of TEXT, source_fingerprint TEXT, updated_at TEXT
        );
        CREATE TABLE stock_adjustment_events (
            code TEXT, date TEXT, adjustment_factor DOUBLE,
            source_fingerprint TEXT
        );
        CREATE TABLE current_basis_recompute_pending (code TEXT);
        CREATE TABLE current_basis_fundamentals_state (
            code TEXT, fundamentals_adjustment_basis_date TEXT,
            source_fingerprint TEXT, statement_count BIGINT,
            materialized_at TEXT
        );
        CREATE TABLE statements (
            code TEXT, statement_id TEXT, disclosed_date TEXT,
            disclosed_at TEXT, period_end TEXT, type_of_current_period TEXT
        );
        CREATE TABLE statement_metrics_adjusted (
            code TEXT, statement_id TEXT, disclosed_date TEXT,
            disclosed_at TEXT, period_end TEXT, period_type TEXT,
            fundamentals_adjustment_basis_date TEXT,
            source_fingerprint TEXT
        );
        CREATE TABLE daily_valuation (
            code TEXT, date TEXT, price_basis_date TEXT,
            fundamentals_adjustment_basis_date TEXT, source_fingerprint TEXT
        );
        CREATE TABLE topix_data (
            date TEXT, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE
        );
        CREATE TABLE indices_data (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        );
        """
    )
    rows = _provider_rows()
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [tuple(row.values()) for row in rows],
    )
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                row["code"],
                row["date"],
                row["adjusted_open"],
                row["adjusted_high"],
                row["adjusted_low"],
                row["adjusted_close"],
                row["adjusted_volume"],
            )
            for row in rows
        ],
    )
    fingerprint = provider_stock_source_fingerprint(rows)
    conn.execute(
        "INSERT INTO stock_provider_windows VALUES (?, ?, ?, ?, ?, ?)",
        ("1111", "2024-01-04", "2024-01-08", "2024-01-08", fingerprint, "now"),
    )
    conn.execute(
        "INSERT INTO stock_adjustment_events VALUES (?, ?, ?, ?)",
        ("1111", "2024-01-08", 0.5, fingerprint),
    )
    conn.execute(
        "INSERT INTO current_basis_fundamentals_state VALUES "
        "('1111', '2024-01-08', 'fundamentals-1111', 0, 'now')"
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?)",
        [
            ("2024-01-04", "1111", "Alpha", "0111"),
            ("2024-01-08", "1111", "Alpha", "0111"),
        ],
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-04", "2024-01-04", "2024-01-08", "fundamentals-1111"),
            ("1111", "2024-01-08", "2024-01-08", "2024-01-08", "fundamentals-1111"),
        ],
    )
    conn.executemany(
        "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
        [
            ("2024-01-04", 100.0, 100.0, 100.0, 100.0),
            ("2024-01-08", 120.0, 120.0, 120.0, 120.0),
        ],
    )
    conn.executemany(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("N225_UNDERPX", "2024-01-04", 1_000.0, 1_000.0, 1_000.0, 1_000.0, 0),
            ("N225_UNDERPX", "2024-01-08", 1_100.0, 1_100.0, 1_100.0, 1_100.0, 0),
        ],
    )
    return conn


def _refresh_provider_window(conn: duckdb.DuckDBPyConnection) -> None:
    columns = tuple(_provider_rows()[0])
    raw_rows = conn.execute(
        f"SELECT {', '.join(columns)} FROM stock_data_raw "
        "QUALIFY row_number() OVER ("
        "PARTITION BY CASE WHEN length(code) IN (5, 6) AND right(code, 1) = '0' "
        "THEN left(code, length(code) - 1) ELSE code END, date "
        "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, length(code), code"
        ") = 1 ORDER BY date"
    ).fetchall()
    rows = [dict(zip(columns, row, strict=True)) for row in raw_rows]
    conn.execute(
        "UPDATE stock_provider_windows SET source_fingerprint = ?",
        [provider_stock_source_fingerprint(rows)],
    )


def test_event_time_signal_uses_provider_adjusted_rows_and_vintage(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-08",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        assert conn.execute(built.validation_sql, built.validation_params).fetchall() == []
        rows = conn.execute(
            f"WITH {built.cte_sql} SELECT * FROM {built.relation_name} ORDER BY date",
            built.params,
        ).fetchall()
    finally:
        conn.close()

    assert rows[0][2:7] == pytest.approx((50.0, 51.0, 49.0, 50.0, 2_000))
    assert rows[1][2:7] == pytest.approx((60.0, 61.0, 59.0, 60.0, 2_000))
    assert rows[1][-1].startswith("provider-v1:1111:2024-01-08:")


@pytest.mark.parametrize(
    ("column", "value", "expected_issue"),
    [
        (None, None, None),
        ("close", 999.0, "raw_alias_conflict"),
        ("adjustment_factor", 2.0, "raw_alias_conflict"),
    ],
)
def test_event_time_signal_normalized_raw_aliases_fail_only_on_conflict(
    tmp_path: Path,
    column: str | None,
    value: float | None,
    expected_issue: str | None,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            "INSERT INTO stock_data_raw SELECT '11110', date, open, high, low, close, "
            "volume, turnover_value, adjustment_factor, adjusted_open, adjusted_high, "
            "adjusted_low, adjusted_close, adjusted_volume FROM stock_data_raw"
        )
        if column is not None:
            conn.execute(
                f"UPDATE stock_data_raw SET {column} = ? "
                "WHERE code = '11110' AND date = '2024-01-08'",
                [value],
            )
        _refresh_provider_window(conn)
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-08",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        issues = conn.execute(built.validation_sql, built.validation_params).fetchall()
    finally:
        conn.close()

    if expected_issue is None:
        assert issues == []
    else:
        assert expected_issue in {str(row[0]) for row in issues}


@pytest.mark.parametrize(
    ("mutation", "expected_issue"),
    [
        (
            "UPDATE stock_data SET close = 999 WHERE date = '2024-01-04'",
            "provider_projection_mismatch",
        ),
        (
            "UPDATE stock_adjustment_events SET source_fingerprint = repeat('0', 64)",
            "provider_event_ledger_mismatch",
        ),
        (
            "UPDATE stock_provider_windows SET provider_as_of = '2024-01-05'",
            "provider_window_invalid",
        ),
    ],
)
def test_event_time_signal_fails_closed_for_provider_lineage(
    tmp_path: Path, mutation: str, expected_issue: str
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(mutation)
        built = build_event_time_signal_sql(
            EventTimeSignalRequest(
                signal_date="2024-01-08",
                start_date="2024-01-04",
                market_codes=("0111",),
            )
        )
        issues = conn.execute(built.validation_sql, built.validation_params).fetchall()
    finally:
        conn.close()

    assert expected_issue in {str(row[0]) for row in issues}


def test_research_prices_preserve_signal_universe_and_provider_lineage(
    tmp_path: Path,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_event_time_prices(
            conn,
            DailyRankingPriceRequest(
                namespace="provider_projection",
                query_start="2024-01-04",
                query_end="2024-01-08",
                analysis_start_date="2024-01-04",
                analysis_end_date="2024-01-04",
                horizons=(1,),
                market_codes=("0111",),
            ),
        )
        signal = conn.execute(
            f"SELECT price_basis_id, close FROM {relations.signal_features}"
        ).fetchone()
        outcome = conn.execute(
            f"SELECT completion_basis_id_1d, forward_close_return_1d_pct "
            f"FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert signal[0].startswith("provider-v1:1111:2024-01-08:")
    assert signal[1] == pytest.approx(50.0)
    assert outcome[0] == signal[0]
    assert outcome[1] == pytest.approx(20.0)
    assert relations.lineage.signal_basis_policy == (
        "exact_provider_window_adjusted_prices_across_full_lookback"
    )
    assert relations.lineage.adjustment_formula == "provider_adjusted_ohlcv_direct"


def test_research_prices_allow_coverage_lagging_provider_frontier(
    tmp_path: Path,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            "UPDATE stock_provider_windows SET provider_as_of = '2024-01-31'"
        )
        relations = build_daily_ranking_event_time_prices(
            conn,
            _generic_price_request("suspended_frontier"),
        )
        vintage = conn.execute(
            f"SELECT price_basis_id FROM {relations.signal_features}"
        ).fetchone()[0]
    finally:
        conn.close()

    assert vintage.startswith("provider-v1:1111:2024-01-31:")


@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM current_basis_fundamentals_state",
        "UPDATE current_basis_fundamentals_state SET source_fingerprint = ''",
        "INSERT INTO current_basis_recompute_pending VALUES ('1111')",
        "UPDATE current_basis_fundamentals_state SET statement_count = 1",
        "UPDATE current_basis_fundamentals_state SET materialized_at = ''",
        "UPDATE daily_valuation SET source_fingerprint = 'mismatch'",
        "INSERT INTO current_basis_fundamentals_state SELECT * "
        "FROM current_basis_fundamentals_state LIMIT 1",
    ],
)
def test_research_prices_fail_closed_for_current_fundamentals_lineage(
    tmp_path: Path,
    mutation: str,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(mutation)
        with pytest.raises(RuntimeError, match="current fundamentals lineage"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("bad_fundamentals_lineage"),
            )
    finally:
        conn.close()


def test_research_prices_reject_market_v4_without_dual_read(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute("UPDATE market_schema_version SET version = 4")
        with pytest.raises(RuntimeError, match="required schema version 5"):
            build_daily_ranking_event_time_prices(
                conn,
                DailyRankingPriceRequest(
                    namespace="reject_v4",
                    query_start="2024-01-04",
                    query_end="2024-01-08",
                    analysis_start_date="2024-01-04",
                    analysis_end_date="2024-01-04",
                    horizons=(1,),
                ),
            )
    finally:
        conn.close()


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
        market_codes=("0111",),
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


def test_forward_projection_exposes_cardinality_diagnostics(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        request = _generic_price_request("diagnostic_projection")
        relations = build_daily_ranking_event_time_prices(conn, request)
    finally:
        conn.close()

    diagnostics = relations.diagnostics
    assert diagnostics.signal_feature_rows == diagnostics.signal_request_rows
    assert diagnostics.outcome_request_rows == diagnostics.signal_request_rows
    assert diagnostics.endpoint_rows == 3 * diagnostics.completed_request_rows
    assert diagnostics.signal_feature_schema[:3] == ("code", "date", "price_basis_id")
    assert diagnostics.forward_outcome_schema[:2] == ("code", "date")


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


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("UPDATE market_schema_version SET version = 4", "required schema version 5"),
        (
            "UPDATE sync_metadata SET value = 'local_projection_v2_event_time'",
            "provider_adjusted_v1",
        ),
        ("DROP TABLE market_schema_version", "market_schema_version"),
    ],
)
def test_forward_projection_requires_market_v5_metadata(
    tmp_path: Path,
    mutation: str,
    expected: str,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(mutation)
        with pytest.raises(RuntimeError, match=expected):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("invalid_metadata"),
            )
    finally:
        conn.close()


def test_forward_projection_requires_market_v5_columns(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute("ALTER TABLE daily_valuation DROP COLUMN price_basis_date")
        with pytest.raises(RuntimeError, match="missing required Market v5 columns"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("invalid_columns"),
            )
    finally:
        conn.close()


def test_forward_projection_namespaces_are_generation_unique(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
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
    assert beta.signal_features.startswith("beta_projection_g_")
    assert alpha_v1_rows == alpha_v2_rows


def test_first_failed_projection_build_leaves_no_partial_relations(
    tmp_path: Path,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute("DELETE FROM stock_adjustment_events")
        with pytest.raises(RuntimeError, match="provider vintage lineage"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("first_failure"),
            )
        remaining = _temporary_relations_with_prefix(conn, "first_failure")
    finally:
        conn.close()

    assert remaining == ()


def test_failed_rebuild_preserves_prior_complete_generation(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
    try:
        prior = build_daily_ranking_event_time_prices(
            conn,
            _generic_price_request("stable_projection"),
        )
        prior_rows = conn.execute(f"SELECT * FROM {prior.forward_outcomes}").fetchall()
        prior_relations = _temporary_relations_with_prefix(conn, "stable_projection")
        conn.execute("UPDATE stock_data SET close = 999 WHERE date = '2024-01-04'")
        with pytest.raises(RuntimeError, match="provider vintage lineage"):
            build_daily_ranking_event_time_prices(
                conn,
                _generic_price_request("stable_projection"),
            )
        after_relations = _temporary_relations_with_prefix(conn, "stable_projection")
        after_rows = conn.execute(f"SELECT * FROM {prior.forward_outcomes}").fetchall()
    finally:
        conn.close()

    assert after_relations == prior_relations
    assert after_rows == prior_rows


@pytest.mark.parametrize(
    "duplicate_sql",
    [
        "INSERT INTO topix_data VALUES ('2024-01-08', 999, 999, 999, 999)",
        "INSERT INTO indices_data VALUES "
        "('N225_UNDERPX', '2024-01-08', 999, 999, 999, 999, 0)",
    ],
)
def test_forward_projection_rejects_duplicate_benchmark_endpoints(
    tmp_path: Path,
    duplicate_sql: str,
) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
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
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
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


def test_forward_projection_keeps_incomplete_horizons_null(tmp_path: Path) -> None:
    conn = _build_market_v5_fixture(tmp_path / "market.duckdb")
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
    assert relations.diagnostics.completed_request_rows == 1
