from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_BRIDGE_DEPRECATED,
    DEPRECATED_DAILY_RANKING_RESEARCH_BRIDGE_CALLERS,
    DailyRankingPanelRequest,
    RelationRef,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
)
from src.domains.analytics import daily_ranking_research_base


def _build_market_v4_research_fixture(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
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
            code TEXT, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, adjustment_factor DOUBLE
        );
        CREATE TABLE stock_master_daily (
            date DATE, code TEXT, company_name TEXT, market_code TEXT,
            market_name TEXT, scale_category TEXT
        );
        CREATE TABLE stock_adjustment_bases (
            code TEXT, basis_id TEXT, valid_from DATE,
            valid_to_exclusive DATE, adjustment_through_date DATE,
            source_fingerprint TEXT, materialized_through_date DATE,
            status TEXT
        );
        CREATE TABLE stock_adjustment_basis_segments (
            code TEXT, basis_id TEXT, source_date_from DATE,
            source_date_to_exclusive DATE, cumulative_factor DOUBLE
        );
        CREATE TABLE daily_valuation (
            code TEXT, date DATE, price_basis_date DATE, per DOUBLE,
            forward_per DOUBLE, pbr DOUBLE, p_op DOUBLE, forward_p_op DOUBLE,
            market_cap DOUBLE, free_float_market_cap DOUBLE, basis_version TEXT
        );
        CREATE TABLE topix_data (
            date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE
        );
        CREATE TABLE indices_data (
            code TEXT, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        );
        """
    )
    dates = tuple(pd.bdate_range("2024-01-04", periods=90).date)
    securities = (
        ("1111", "Alpha", "0111", "Prime", 100.0, 0.25),
        ("2222", "Beta", "0111", "Prime", 180.0, -0.05),
        ("3333", "Gamma", "0112", "Standard", 80.0, 0.15),
    )
    raw_rows: list[tuple[object, ...]] = []
    master_rows: list[tuple[object, ...]] = []
    valuation_rows: list[tuple[object, ...]] = []
    for index, session_date in enumerate(dates):
        for security_index, (
            code,
            company_name,
            market_code,
            market_name,
            base,
            slope,
        ) in enumerate(securities):
            close = base + index * slope
            basis_id = f"event-pit-v1:{code}:{dates[0]}"
            raw_rows.append(
                (
                    code,
                    session_date,
                    close * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    10_000 + index * 10 + security_index * 1_000,
                    1.0,
                )
            )
            master_rows.append(
                (
                    session_date,
                    code,
                    company_name,
                    market_code,
                    market_name,
                    None,
                )
            )
            valuation_rows.append(
                (
                    code,
                    session_date,
                    session_date,
                    10.0 + security_index * 5.0,
                    8.0 + security_index * 4.0,
                    0.8 + security_index * 0.7,
                    7.0 + security_index * 2.0,
                    6.0 + security_index * 2.0,
                    100_000_000.0 + security_index * 50_000_000.0,
                    80_000_000.0 + security_index * 40_000_000.0,
                    basis_id,
                )
            )
    conn.executemany("INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?)", raw_rows)
    conn.executemany("INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows)
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    basis_rows = [
        (
            code,
            f"event-pit-v1:{code}:{dates[0]}",
            dates[0],
            None,
            dates[0],
            f"fixture-{code}",
            dates[-1],
            "ready",
        )
        for code, *_ in securities
    ]
    segment_rows = [
        (code, f"event-pit-v1:{code}:{dates[0]}", dates[0], None, 1.0)
        for code, *_ in securities
    ]
    conn.executemany(
        "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        basis_rows,
    )
    conn.executemany(
        "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
        segment_rows,
    )
    topix_rows = []
    n225_rows = []
    for index, session_date in enumerate(dates):
        topix_close = 2_000.0 + index * 2.0
        n225_close = 30_000.0 + index * 20.0
        topix_rows.append(
            (
                session_date,
                topix_close * 0.995,
                topix_close * 1.005,
                topix_close * 0.99,
                topix_close,
            )
        )
        n225_rows.append(
            (
                "N225_UNDERPX",
                session_date,
                n225_close * 0.995,
                n225_close * 1.005,
                n225_close * 0.99,
                n225_close,
                0,
            )
        )
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)", n225_rows)
    return conn


def _request(
    namespace: str,
    *,
    include_liquidity: bool = True,
    percentile_features: tuple[str, ...] = (),
) -> DailyRankingPanelRequest:
    return DailyRankingPanelRequest(
        namespace=namespace,
        analysis_start_date=date(2024, 4, 4),
        analysis_end_date=date(2024, 4, 12),
        horizons=(2,),
        market_scopes=("prime",),
        include_liquidity=include_liquidity,
        percentile_features=percentile_features,
    )


def _relation_type_map(
    conn: duckdb.DuckDBPyConnection,
    relation: RelationRef,
) -> dict[str, str]:
    return {
        str(row[1]): str(row[2]).upper()
        for row in conn.execute(f"PRAGMA table_info('{relation.name}')").fetchall()
    }


def _assert_unique_keys(
    conn: duckdb.DuckDBPyConnection,
    relation: RelationRef,
) -> None:
    keys = ", ".join(relation.key_columns)
    duplicate_count = conn.execute(
        f"SELECT count(*) FROM ("
        f"SELECT {keys} FROM {relation.name} GROUP BY {keys} HAVING count(*) > 1)"
    ).fetchone()[0]
    assert duplicate_count == 0


def test_relation_and_request_identifiers_are_validated() -> None:
    with pytest.raises(ValueError, match="invalid DuckDB relation name"):
        RelationRef("unsafe;drop", ("code",), ("code",), 0)
    with pytest.raises(ValueError, match="namespace"):
        _request("Unsafe-Namespace")


def test_deprecated_bridge_is_explicit_and_does_not_import_ranking_color() -> None:
    assert DAILY_RANKING_RESEARCH_BRIDGE_DEPRECATED is True
    assert set(DEPRECATED_DAILY_RANKING_RESEARCH_BRIDGE_CALLERS) == {
        "ranking_crowded_long_tail_evidence",
        "ranking_daily_triage_lens",
        "ranking_fixed_return_priority_evidence",
        "ranking_forecast_operating_profit_growth_evidence",
        "ranking_liquidity_price_action_recomposition",
        "ranking_liquidity_z_long_evidence",
        "ranking_long_scaffold_factor_cross_evidence",
        "ranking_long_scaffold_value_composite_evidence",
        "ranking_moving_average_replacement_evidence",
        "ranking_n225_neutral_rerating_benchmark",
        "ranking_psr_valuation_evidence",
        "ranking_roe_quality_evidence",
        "ranking_sector_strength_evidence",
        "ranking_short_red_evidence",
        "ranking_short_sector_strength_evidence",
        "ranking_short_value_composite_evidence",
        "ranking_sma5_atr_deviation_evidence",
        "ranking_sma5_below_streak_evidence",
        "ranking_sma5_count_long_evidence",
        "ranking_sma5_count_short_evidence",
        "ranking_sma5_deviation_evidence",
        "ranking_sma5_position_state_evidence",
        "ranking_technical_fit_score_shape_evidence",
        "ranking_trend_acceleration_conditional_lift",
        "ranking_trend_slope_evidence",
    }
    assert "ranking_color_evidence" not in Path(
        daily_ranking_research_base.__file__
    ).read_text()


def test_namespaced_builds_coexist_with_explicit_unique_date_schemas(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        first = build_daily_ranking_research_base(conn, _request("alpha"))
        second = build_daily_ranking_research_base(conn, _request("beta"))

        first_refs = (
            first.signal_prices,
            first.forward_outcomes,
            first.signal_panel,
            first.ranked_signals,
            first.liquidity_ranked_signals,
        )
        second_refs = (
            second.signal_prices,
            second.forward_outcomes,
            second.signal_panel,
            second.ranked_signals,
            second.liquidity_ranked_signals,
        )
        assert None not in first_refs
        assert None not in second_refs
        assert {ref.name for ref in first_refs if ref is not None}.isdisjoint(
            ref.name for ref in second_refs if ref is not None
        )
        for relation in (*first_refs, *second_refs):
            assert relation is not None
            assert relation.columns == tuple(
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info('{relation.name}')"
                ).fetchall()
            )
            assert relation.row_count == conn.execute(
                f"SELECT count(*) FROM {relation.name}"
            ).fetchone()[0]
            _assert_unique_keys(conn, relation)
            date_types = {
                column: sql_type
                for column, sql_type in _relation_type_map(conn, relation).items()
                if column == "date" or "_date_" in column
            }
            assert date_types
            assert set(date_types.values()) == {"DATE"}

        for relation in (
            first.signal_prices,
            first.signal_panel,
            first.ranked_signals,
            first.liquidity_ranked_signals,
        ):
            assert relation is not None
            assert not any(column.startswith("forward_") for column in relation.columns)
        assert first.lineage.no_stock_data_fallback
        assert first.lineage.verification_status == "verified"
    finally:
        conn.close()


def test_disabled_optional_stages_return_none_and_do_not_expose_stale_relations(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        enabled = build_daily_ranking_research_base(
            conn,
            _request(
                "repeatable",
                percentile_features=("forecast_per_to_per_ratio",),
            ),
        )
        disabled = build_daily_ranking_research_base(
            conn,
            _request("repeatable", include_liquidity=False),
        )

        assert enabled.liquidity_ranked_signals is not None
        assert disabled.liquidity_ranked_signals is None
        assert enabled.ranked_signals.name != disabled.ranked_signals.name
        assert "forecast_per_to_per_ratio_percentile" in enabled.ranked_signals.columns
        assert "forecast_per_to_per_ratio_percentile" not in disabled.ranked_signals.columns
        assert disabled.diagnostics.liquidity_stage_executed is False
        assert disabled.diagnostics.percentile_features == ()
        disabled_generation = disabled.ranked_signals.name.removesuffix(
            "_ranked_signals"
        )
        assert conn.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = ?",
            [f"{disabled_generation}_liquidity_ranked_signals"],
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_unknown_scope_is_filtered_by_exact_date_market_membership(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        conn.execute(
            "UPDATE stock_master_daily "
            "SET market_code = '9999', market_name = 'Unclassified' "
            "WHERE code = '3333'"
        )
        request = DailyRankingPanelRequest(
            namespace="unknown_scope",
            analysis_start_date=date(2024, 4, 4),
            analysis_end_date=date(2024, 4, 12),
            horizons=(2,),
            market_scopes=("unknown",),
            include_liquidity=False,
        )

        relations = build_daily_ranking_research_base(conn, request)

        assert conn.execute(
            f"SELECT DISTINCT code, market_scope FROM {relations.signal_panel.name}"
        ).fetchall() == [("3333", "unknown")]
    finally:
        conn.close()


def test_future_source_append_does_not_change_prior_signal_relations(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        baseline = build_daily_ranking_research_base(conn, _request("stable_before"))
        baseline_rows = conn.execute(
            f"SELECT * FROM {baseline.ranked_signals.name} ORDER BY date, code"
        ).fetchall()
        future_date = date(2024, 5, 17)
        basis_id = "event-pit-v1:1111:2024-01-04"
        conn.execute(
            "INSERT INTO stock_data_raw VALUES "
            "('1111', ?, 999.0, 1000.0, 998.0, 999.0, 999999, 1.0)",
            [future_date],
        )
        conn.execute(
            "INSERT INTO stock_master_daily VALUES "
            "(?, '1111', 'Future Alpha', '0111', 'Prime', NULL)",
            [future_date],
        )
        conn.execute(
            "INSERT INTO daily_valuation VALUES "
            "('1111', ?, ?, 999.0, 999.0, 999.0, 999.0, 999.0, "
            "999999999.0, 999999999.0, ?)",
            [future_date, future_date, basis_id],
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, 999.0, 999.0, 999.0, 999.0)",
            [future_date],
        )
        conn.execute(
            "INSERT INTO indices_data VALUES "
            "('N225_UNDERPX', ?, 999.0, 999.0, 999.0, 999.0, 0)",
            [future_date],
        )
        conn.execute(
            "UPDATE stock_adjustment_bases SET materialized_through_date = ?",
            [future_date],
        )

        appended = build_daily_ranking_research_base(conn, _request("stable_after"))
        appended_rows = conn.execute(
            f"SELECT * FROM {appended.ranked_signals.name} ORDER BY date, code"
        ).fetchall()

        assert appended_rows == baseline_rows
    finally:
        conn.close()


def test_outcomes_attach_only_after_signal_membership_is_materialized(
    tmp_path: Path,
) -> None:
    conn = _build_market_v4_research_fixture(tmp_path / "market.duckdb")
    try:
        relations = build_daily_ranking_research_base(conn, _request("selection"))
        cohort = materialize_daily_ranking_signal_cohort(
            conn,
            relations,
            name="ranking_color_cohort",
            select_sql=f"""
                SELECT
                    code, date, market_scope, per_percentile,
                    CASE WHEN per_percentile <= 0.2
                        THEN 'cheapest' ELSE 'other' END AS valuation_bucket
                FROM {relations.ranked_signals.name}
                WHERE market_scope = 'prime'
            """,
        )
        evaluated = attach_daily_ranking_outcomes(
            conn,
            cohort,
            relations,
            name="ranking_color_evaluated",
        )

        assert cohort.row_count == evaluated.row_count
        assert not any(column.startswith("forward_") for column in cohort.columns)
        assert "forward_close_excess_return_2d_pct" in evaluated.columns
        assert conn.execute(
            f"SELECT code, date, valuation_bucket FROM {cohort.name} ORDER BY date, code"
        ).fetchall() == conn.execute(
            f"SELECT code, date, valuation_bucket FROM {evaluated.name} "
            "ORDER BY date, code"
        ).fetchall()
        with pytest.raises(ValueError, match="forward outcome"):
            materialize_daily_ranking_signal_cohort(
                conn,
                relations,
                name="unsafe_cohort",
                select_sql=f"""
                    SELECT signal.code, signal.date
                    FROM {relations.ranked_signals.name} signal
                    JOIN {relations.forward_outcomes.name} outcome USING (code, date)
                    WHERE outcome.forward_close_return_2d_pct IS NOT NULL
                """,
            )
    finally:
        conn.close()
