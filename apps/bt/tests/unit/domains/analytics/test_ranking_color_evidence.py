from __future__ import annotations

import datetime as dt
from pathlib import Path
import inspect

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
    DAILY_RANKING_RESEARCH_PANEL_TABLE,
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    DailyRankingPanelRequest,
    _materialize_ranked_signals,
    _materialize_signal_panel,
    _resolve_query_bounds,
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.ranking_color_evidence import (
    RankingColorEvidenceResult,
    _LIQUIDITY_REGIMES,
    _TOPIX_REGIMES,
    _VALUATION_BUCKETS,
    _VALUATION_FEATURES,
    _ranking_color_output_schema,
    build_summary_markdown,
    run_ranking_color_evidence_research,
    write_ranking_color_evidence_bundle,
)
from src.domains.analytics import ranking_color_evidence

_OBSERVATION_SAMPLE_COLUMNS = (
    "date",
    "code",
    "company_name",
    "market",
    "market_code",
    "scale_category",
    "close",
    "recent_return_20d_pct",
    "recent_return_60d_pct",
    "recent_return_120d_pct",
    "recent_return_150d_pct",
    "topix_recent_return_20d_pct",
    "topix_recent_return_60d_pct",
    "n225_recent_return_20d_pct",
    "n225_recent_return_60d_pct",
    "n225_close_return_20d_pct",
    "med_adv60_mil_jpy",
    "free_float_market_cap_bil_jpy",
    "liquidity_residual_z",
    "liquidity_regime",
    "per",
    "per_percentile",
    "forward_per",
    "forward_per_percentile",
    "forward_per_to_per_ratio",
    "forward_per_to_per_ratio_percentile",
    "pbr",
    "pbr_percentile",
    "p_op",
    "forward_p_op",
    "forward_p_op_percentile",
    "forward_p_op_to_per_ratio",
    "forward_p_op_to_per_ratio_percentile",
    "forecast_operating_profit_growth_ratio",
    "forecast_operating_profit_growth_ratio_percentile",
    "forecast_operating_profit_growth_pct",
    "per_to_fop_growth_ratio",
    "per_to_fop_growth_ratio_percentile",
    "forward_per_to_fop_growth_ratio",
    "forward_per_to_fop_growth_ratio_percentile",
    "market_cap_bil_jpy",
    "forward_close_excess_return_20d_pct",
    "forward_close_n225_excess_return_20d_pct",
)
_COVERAGE_COLUMNS = (
    "market",
    "observation_count",
    "code_count",
    "date_count",
    "per_coverage_pct",
    "forward_per_coverage_pct",
    "forward_p_op_coverage_pct",
    "pbr_coverage_pct",
    "liquidity_residual_z_coverage_pct",
)
_EVIDENCE_METRIC_COLUMNS = (
    "horizon",
    "market_scope",
    "observation_count",
    "code_count",
    "date_count",
    "mean_forward_excess_return_pct",
    "median_forward_excess_return_pct",
    "p10_forward_excess_return_pct",
    "p25_forward_excess_return_pct",
    "p75_forward_excess_return_pct",
    "p90_forward_excess_return_pct",
    "win_rate_pct",
    "severe_loss_rate_pct",
    "median_recent_return_20d_pct",
    "median_recent_return_60d_pct",
    "median_recent_return_120d_pct",
    "median_recent_return_150d_pct",
    "median_topix_recent_return_20d_pct",
    "median_topix_recent_return_60d_pct",
    "median_med_adv60_mil_jpy",
    "median_market_cap_bil_jpy",
    "median_free_float_market_cap_bil_jpy",
    "median_liquidity_residual_z",
    "median_per",
    "median_forward_per",
    "median_pbr",
    "median_p_op",
    "median_forward_p_op",
    "median_forward_per_to_per_ratio",
    "median_forward_p_op_to_per_ratio",
    "median_per_percentile",
    "median_forward_per_percentile",
    "median_forward_p_op_percentile",
    "median_pbr_percentile",
    "median_forward_per_to_per_ratio_percentile",
    "median_forward_p_op_to_per_ratio_percentile",
)
_RESULT_TABLE_COLUMNS = {
    "observation_sample_df": _OBSERVATION_SAMPLE_COLUMNS,
    "coverage_diagnostics_df": _COVERAGE_COLUMNS,
    "ranking_color_evidence_df": (
        "condition_family",
        "valuation_feature",
        "ranking_color_bucket",
        "ranking_color_bucket_order",
        "evidence_tier",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "per_relation_evidence_df": (
        "condition_family",
        "relation_feature",
        "relation_bucket",
        "relation_bucket_order",
        "evidence_tier",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "low_per_relation_evidence_df": (
        "condition_family",
        "per_scope",
        "relation_feature",
        "relation_bucket",
        "relation_bucket_order",
        "evidence_tier",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "low_per_relation_level_evidence_df": (
        "condition_family",
        "per_scope",
        "relation_feature",
        "relation_level_bucket",
        "relation_level_bucket_order",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "forward_per_pop_interaction_df": (
        "condition_family",
        "interaction_bucket",
        "interaction_bucket_order",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "liquidity_regime_evidence_df": (
        "condition_family",
        "liquidity_regime",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "topix_regime_liquidity_value_evidence_df": (
        "condition_family",
        "topix_regime",
        "topix_regime_order",
        "liquidity_regime",
        "value_condition",
        "value_condition_order",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "rerating_good_valuation_chain_df": (
        "condition_family",
        "good_scope",
        "good_scope_order",
        "chain_condition",
        "chain_condition_order",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "liquidity_color_long_trend_evidence_df": (
        "condition_family",
        "liquidity_regime",
        "liquidity_regime_order",
        "ui_color",
        "ui_color_order",
        "trend_window",
        "trend_condition",
        "trend_condition_order",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
    "overvalued_size_liquidity_interaction_df": (
        "condition_family",
        "valuation_condition",
        "valuation_condition_order",
        "market_cap_abs_bucket",
        "market_cap_abs_bucket_order",
        "adv60_abs_bucket",
        "adv60_abs_bucket_order",
        *_EVIDENCE_METRIC_COLUMNS,
    ),
}


def _assert_exact_result_schemas(result: RankingColorEvidenceResult) -> None:
    for field_name, expected_columns in _RESULT_TABLE_COLUMNS.items():
        assert tuple(getattr(result, field_name).columns) == expected_columns


def test_ranking_color_is_a_typed_research_base_consumer() -> None:
    source = Path(ranking_color_evidence.__file__).read_text()

    assert "build_daily_ranking_research_base" in source
    assert "materialize_daily_ranking_signal_cohort" in source
    assert "attach_daily_ranking_outcomes" in source
    assert "def _create_observation_panel(" not in source
    assert "def _create_percentile_view(" not in source


def test_ranking_color_evidence_uses_daily_valuation_fast_path(tmp_path: Path) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    _assert_exact_result_schemas(result)
    assert result.observation_count > 0
    assert not result.ranking_color_evidence_df.empty
    assert not result.per_relation_evidence_df.empty
    assert not result.low_per_relation_evidence_df.empty
    assert not result.low_per_relation_level_evidence_df.empty
    assert not result.forward_per_pop_interaction_df.empty
    assert not result.topix_regime_liquidity_value_evidence_df.empty
    assert not result.rerating_good_valuation_chain_df.empty
    assert not result.liquidity_color_long_trend_evidence_df.empty
    assert not result.overvalued_size_liquidity_interaction_df.empty
    assert {
        "valuation_feature",
        "ranking_color_bucket",
        "evidence_tier",
        "median_forward_excess_return_pct",
    }.issubset(result.ranking_color_evidence_df.columns)
    assert {
        "low_forward_per_low_forward_p_op",
        "low_forward_per_high_forward_p_op",
    }.issubset(
        set(result.forward_per_pop_interaction_df["interaction_bucket"].astype(str))
    )
    assert {
        "per_percentile",
        "forward_per_percentile",
        "forward_p_op_percentile",
        "forward_per_to_per_ratio",
        "forward_per_to_per_ratio_percentile",
        "forward_p_op_to_per_ratio",
        "forward_p_op_to_per_ratio_percentile",
        "forecast_operating_profit_growth_ratio",
        "forecast_operating_profit_growth_ratio_percentile",
        "forecast_operating_profit_growth_pct",
        "per_to_fop_growth_ratio",
        "per_to_fop_growth_ratio_percentile",
        "forward_per_to_fop_growth_ratio",
        "forward_per_to_fop_growth_ratio_percentile",
        "pbr_percentile",
        "topix_recent_return_20d_pct",
        "topix_recent_return_60d_pct",
        "n225_recent_return_20d_pct",
        "n225_recent_return_60d_pct",
        "n225_close_return_20d_pct",
        "forward_close_n225_excess_return_20d_pct",
        "recent_return_120d_pct",
        "recent_return_150d_pct",
        "liquidity_residual_z",
        "liquidity_regime",
    }.issubset(result.observation_sample_df.columns)
    assert set(
        result.liquidity_color_long_trend_evidence_df["liquidity_regime"].astype(str)
    ).issubset({"crowded_rerating", "neutral_rerating"})
    assert {
        "green",
        "blue",
    }.issubset(
        set(result.liquidity_color_long_trend_evidence_df["ui_color"].astype(str))
    )
    assert {120, 150}.issubset(
        set(result.liquidity_color_long_trend_evidence_df["trend_window"].astype(int))
    )
    assert set(
        result.liquidity_color_long_trend_evidence_df["trend_condition"].astype(str)
    ).issubset({"trend_positive", "trend_non_positive"})
    assert {
        "topix_20d_lt_0_60d_gt_0",
        "topix_60d_lt_0",
    }.issubset(
        set(result.topix_regime_liquidity_value_evidence_df["topix_regime"].astype(str))
    )
    assert {
        "high_per20_high_pbr20",
        "high_forward_per20_high_pbr20",
    }.issubset(
        set(
            result.overvalued_size_liquidity_interaction_df[
                "valuation_condition"
            ].astype(str)
        )
    )
    assert {
        "market_cap_abs_bucket",
        "adv60_abs_bucket",
        "median_market_cap_bil_jpy",
        "median_med_adv60_mil_jpy",
    }.issubset(result.overvalued_size_liquidity_interaction_df.columns)
    assert {
        "all_value",
        "strong_value_confirmation",
    }.issubset(
        set(
            result.topix_regime_liquidity_value_evidence_df["value_condition"].astype(
                str
            )
        )
    )
    assert {
        "all_good",
        "per_gt_fwdper_gt_fwdpop",
        "good_without_chain",
    }.issubset(
        set(result.rerating_good_valuation_chain_df["chain_condition"].astype(str))
    )
    assert {
        "all_rerating_good",
        "neutral_rerating_good",
    }.issubset(set(result.rerating_good_valuation_chain_df["good_scope"].astype(str)))
    assert {
        "forward_per_to_per_ratio",
        "forward_p_op_to_per_ratio",
    }.issubset(set(result.per_relation_evidence_df["relation_feature"].astype(str)))
    assert {
        "low_per_10pct",
        "low_per_20pct",
    }.issubset(set(result.low_per_relation_evidence_df["per_scope"].astype(str)))
    assert "ratio_lte_0_8" in set(
        result.low_per_relation_level_evidence_df["relation_level_bucket"].astype(str)
    )
    assert "crowded_rerating" in _LIQUIDITY_REGIMES
    assert "neutral_rerating" in _LIQUIDITY_REGIMES
    assert "topix_20d_lt_0_60d_gt_0" in {regime for regime, _ in _TOPIX_REGIMES}
    assert "rerating_participation" not in _LIQUIDITY_REGIMES
    assert "statements" not in set(result.required_tables)
    sample = result.observation_sample_df
    high_forward_p_op = sample.loc[sample["code"] == "3333"].iloc[0]
    invalid_forward_p_op = sample.loc[sample["code"] == "6666"].iloc[0]
    assert high_forward_p_op["forward_p_op_percentile"] == 1.0
    assert high_forward_p_op["forward_p_op_to_per_ratio_percentile"] == 1.0
    assert pd.isna(invalid_forward_p_op["forward_p_op_percentile"])
    assert pd.isna(invalid_forward_p_op["forward_p_op_to_per_ratio_percentile"])


def test_ranking_color_freezes_each_valuation_bucket_before_outcome_attach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    events: list[tuple[str, str, tuple[str, ...]]] = []
    original_materialize = (
        ranking_color_evidence.materialize_daily_ranking_signal_cohort
    )
    original_attach = ranking_color_evidence.attach_daily_ranking_outcomes

    def record_materialize(*args, **kwargs):
        relation = original_materialize(*args, **kwargs)
        events.append(("cohort", relation.name, relation.columns))
        return relation

    def record_attach(connection, cohort, relations, *, name):
        events.append(("attach", cohort.name, cohort.columns))
        return original_attach(connection, cohort, relations, name=name)

    monkeypatch.setattr(
        ranking_color_evidence,
        "materialize_daily_ranking_signal_cohort",
        record_materialize,
    )
    monkeypatch.setattr(
        ranking_color_evidence,
        "attach_daily_ranking_outcomes",
        record_attach,
    )

    _run_test_research(db_path)

    valuation_cohorts = [
        event
        for event in events
        if event[0] == "cohort"
        and {"valuation_feature", "ranking_color_bucket"}.issubset(event[2])
    ]
    valuation_attaches = [
        event
        for event in events
        if event[0] == "attach"
        and {"valuation_feature", "ranking_color_bucket"}.issubset(event[2])
    ]
    assert len(valuation_cohorts) == len(_VALUATION_FEATURES) * len(_VALUATION_BUCKETS)
    assert {event[1] for event in valuation_attaches} == {
        event[1] for event in valuation_cohorts
    }
    for cohort_event in valuation_cohorts:
        assert events.index(cohort_event) < next(
            index
            for index, event in enumerate(events)
            if event[0] == "attach" and event[1] == cohort_event[1]
        )


def test_daily_ranking_research_base_creates_public_panel_aliases(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))

    assert_daily_ranking_research_tables(conn)
    spec = create_daily_ranking_research_panel(
        conn,
        query_start=daily_ranking_query_start_date(
            "2024-03-01",
            warmup_calendar_days=150,
        ),
        query_end=daily_ranking_query_end_date("2024-04-30", max_horizon=20),
        analysis_start_date="2024-03-01",
        analysis_end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
    )

    assert spec.panel_table != DAILY_RANKING_RESEARCH_PANEL_TABLE
    assert spec.ranked_table != DAILY_RANKING_RESEARCH_RANKED_TABLE
    assert spec.liquidity_ranked_table != DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE
    assert "_g_" in spec.panel_table
    assert "_g_" in spec.ranked_table
    assert spec.liquidity_ranked_table is not None
    assert "_g_" in spec.liquidity_ranked_table
    assert spec.market_scopes == ("prime",)
    assert spec.horizons == (20,)
    assert normalize_daily_ranking_market_scopes(("0101",)) == ("prime",)
    public_count_row = conn.execute(
        f"SELECT count(*) FROM {DAILY_RANKING_RESEARCH_PANEL_TABLE}"
    ).fetchone()
    legacy_count_row = conn.execute(
        "SELECT count(*) FROM ranking_color_panel"
    ).fetchone()
    assert public_count_row is not None
    assert legacy_count_row is not None
    public_count = public_count_row[0]
    legacy_count = legacy_count_row[0]
    assert public_count == legacy_count
    ranked_count = conn.execute(
        f"SELECT count(*) FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}"
    ).fetchone()[0]
    scoped_count = conn.execute("SELECT count(*) FROM ranking_color_scoped").fetchone()[
        0
    ]
    assert scoped_count == ranked_count * 2
    assert "all_liquidity" in {
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT liquidity_scope FROM ranking_color_scoped"
        ).fetchall()
    }
    ranked_columns = {
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info('{DAILY_RANKING_RESEARCH_RANKED_TABLE}')"
        ).fetchall()
    }
    assert {
        "per_percentile",
        "forward_per_percentile",
        "forward_p_op_percentile",
        "pbr_percentile",
        "forward_close_excess_return_20d_pct",
        "n225_close_return_20d_pct",
        "forward_close_n225_excess_return_20d_pct",
        "forecast_operating_profit_growth_ratio",
        "forecast_operating_profit_growth_ratio_percentile",
        "per_to_fop_growth_ratio",
        "per_to_fop_growth_ratio_percentile",
        "forward_per_to_fop_growth_ratio",
        "forward_per_to_fop_growth_ratio_percentile",
        "valuation_signal",
        "strong_value_confirmation",
        "medium_value_confirmation",
        "overvalued_warning",
        "very_overvalued_warning",
        "no_positive_earnings_valuation",
        "no_value_confirmation",
    }.issubset(ranked_columns)
    n225_row = conn.execute(
        f"""
        SELECT
            n225_close_return_20d_pct,
            forward_close_n225_excess_return_20d_pct
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}
        WHERE n225_close_return_20d_pct IS NOT NULL
          AND forward_close_n225_excess_return_20d_pct IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    assert n225_row is not None
    conn.close()


def test_ranking_color_ignores_poisoned_stock_data_convenience_rows(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    baseline = _run_test_research(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("UPDATE stock_data SET close = close * 99")
    conn.close()

    poisoned = _run_test_research(db_path)

    pd.testing.assert_frame_equal(
        baseline.observation_sample_df,
        poisoned.observation_sample_df,
    )
    pd.testing.assert_frame_equal(
        baseline.ranking_color_evidence_df,
        poisoned.ranking_color_evidence_df,
    )


def test_daily_ranking_research_base_skips_liquidity_ranked_work_when_disabled(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))

    create_daily_ranking_research_panel(
        conn,
        query_start=daily_ranking_query_start_date(
            "2024-03-01",
            warmup_calendar_days=150,
        ),
        query_end=daily_ranking_query_end_date("2024-04-30", max_horizon=20),
        analysis_start_date="2024-03-01",
        analysis_end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        include_liquidity_ranked=False,
    )

    liquidity_scopes = {
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT liquidity_scope FROM ranking_color_scoped"
        ).fetchall()
    }
    liquidity_table_exists = bool(
        conn.execute(
            """
            SELECT count(*)
            FROM duckdb_tables()
            WHERE table_name = 'ranking_color_liquidity_ranked'
            """
        ).fetchone()[0]
    )

    assert liquidity_scopes == {"all_liquidity"}
    assert not liquidity_table_exists
    conn.close()


def test_daily_ranking_research_base_keeps_n225_columns_when_index_rows_are_missing(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute("DELETE FROM indices_data")

    create_daily_ranking_research_panel(
        conn,
        query_start=daily_ranking_query_start_date(
            "2024-03-01",
            warmup_calendar_days=150,
        ),
        query_end=daily_ranking_query_end_date("2024-04-30", max_horizon=20),
        analysis_start_date="2024-03-01",
        analysis_end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        include_liquidity_ranked=False,
    )

    null_count, row_count = conn.execute(
        f"""
        SELECT
            count(*) FILTER (
                WHERE n225_close_return_20d_pct IS NULL
                  AND forward_close_n225_excess_return_20d_pct IS NULL
            ),
            count(*)
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}
        """
    ).fetchone()

    assert row_count > 0
    assert null_count == row_count
    conn.close()


def test_daily_ranking_research_base_can_skip_relation_percentiles(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))

    create_daily_ranking_research_panel(
        conn,
        query_start=daily_ranking_query_start_date(
            "2024-03-01",
            warmup_calendar_days=150,
        ),
        query_end=daily_ranking_query_end_date("2024-04-30", max_horizon=20),
        analysis_start_date="2024-03-01",
        analysis_end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        include_relation_percentiles=False,
    )

    ranked_columns = {
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info('{DAILY_RANKING_RESEARCH_RANKED_TABLE}')"
        ).fetchall()
    }
    assert "forward_per_to_per_ratio" in ranked_columns
    assert "forward_per_to_per_ratio_percentile" not in ranked_columns
    assert "valuation_signal" in ranked_columns
    assert (
        conn.execute(
            f"""
        SELECT count(*)
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE}
        WHERE valuation_signal IS NOT NULL
        """
        ).fetchone()[0]
        > 0
    )
    conn.close()


def test_ranking_color_percentiles_use_single_window_per_metric() -> None:
    source = inspect.getsource(_materialize_ranked_signals)

    assert "percent_rank_sql" in source
    assert "percentile_window AS" in source
    assert "_valid_count" not in source


def test_ranking_color_panel_filters_expensive_inputs_before_residuals() -> None:
    source = inspect.getsource(_materialize_signal_panel)

    assert "benchmark_conditions" in source
    assert "panel_source AS" in source
    assert "_liquidity_sql" in source
    assert source.index("panel_source AS") < source.index("panel_with_relations AS")


def test_ranking_color_panel_uses_exact_date_market_membership() -> None:
    source = inspect.getsource(_materialize_signal_panel)

    assert "CAST(smd.date AS DATE)" in source
    assert "JOIN market_master market USING (code, date)" in source
    assert "stock_data " not in source


def test_ranking_color_panel_resolves_session_guaranteed_feature_history(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    earlier_dates = tuple(
        value.date() for value in pd.bdate_range(end="2023-06-30", periods=245)
    )
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES "
        "('1111', ?, 99.0, 101.0, 98.0, 100.0, 1000, 1.0)",
        [(value,) for value in earlier_dates],
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES "
        "(?, '1111', 'Alpha', '0111', 'Market', NULL)",
        [(value,) for value in earlier_dates],
    )
    request = DailyRankingPanelRequest(
        namespace="ranking_color_bounds",
        analysis_start_date=dt.date(2024, 6, 28),
        analysis_end_date=dt.date(2024, 6, 28),
        horizons=(20,),
        market_scopes=("prime",),
    )

    try:
        query_start, query_end = _resolve_query_bounds(
            conn,
            request,
            market_codes=("0111",),
        )
        valid_session_count = conn.execute(
            """
            SELECT count(DISTINCT CAST(raw.date AS DATE))
            FROM stock_data_raw raw
            JOIN stock_master_daily smd
              ON raw.code = smd.code
             AND CAST(raw.date AS DATE) = CAST(smd.date AS DATE)
            WHERE CAST(raw.date AS DATE) <= DATE '2024-06-28'
              AND raw.open > 0 AND raw.high > 0 AND raw.low > 0
              AND raw.close > 0 AND raw.volume >= 0
              AND smd.market_code = '0111'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    assert valid_session_count == 505
    assert query_start == earlier_dates[0]
    assert query_end == dt.date(2024, 10, 16)


def test_ranking_color_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Color Evidence" in summary
    assert "Forward Valuation vs PER Relation Evidence" in summary
    assert "Low PER x Forward Valuation Relation Evidence" in summary
    assert "Low PER x Forward Valuation Relation Level Evidence" in summary
    assert "Forward PER x Forward P/OP Interaction" in summary
    assert "TOPIX Regime x Liquidity x Value Evidence" in summary
    assert "Rerating Good x PER > Fwd PER > Fwd P/OP" in summary
    assert "Liquidity Color x Long Trend Evidence" in summary
    assert "Overvalued x Size x Liquidity Interaction" in summary

    bundle = write_ranking_color_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    _assert_exact_result_schemas(result)
    conn = duckdb.connect(str(bundle.results_db_path), read_only=True)
    try:
        assert tuple(bundle.output_tables) == tuple(_RESULT_TABLE_COLUMNS)
        for table_name, expected_columns in _RESULT_TABLE_COLUMNS.items():
            table_schema = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            assert tuple(row[1] for row in table_schema) == expected_columns
            frame = getattr(result, table_name)
            expected_types = tuple(
                sql_type for _, sql_type in _ranking_color_output_schema(frame)
            )
            assert tuple(str(row[2]) for row in table_schema) == expected_types
            pd.testing.assert_frame_equal(
                frame.reset_index(drop=True),
                conn.execute(f"SELECT {', '.join(expected_columns)} FROM {table_name}")
                .fetchdf()
                .reset_index(drop=True),
                check_dtype=False,
            )
    finally:
        conn.close()


def test_ranking_color_evidence_treats_tse_first_section_as_prime(
    tmp_path: Path,
) -> None:
    db_path = _build_ranking_color_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "UPDATE stock_master_daily SET market_code = '0101', market_name = '東証一部'"
    )
    conn.close()

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert set(result.observation_sample_df["market"].astype(str)) == {"prime"}


def _run_test_research(db_path: Path) -> RankingColorEvidenceResult:
    return run_ranking_color_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_ranking_color_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-07-03", "2024-06-28").strftime("%Y-%m-%d").tolist()
    conn = duckdb.connect(str(db_path))
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
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        );
        CREATE TABLE stock_data_raw (
            code TEXT,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjustment_factor DOUBLE
        );
        CREATE TABLE stock_adjustment_bases (
            code TEXT,
            basis_id TEXT,
            valid_from DATE,
            valid_to_exclusive DATE,
            adjustment_through_date DATE,
            source_fingerprint TEXT,
            materialized_through_date DATE,
            status TEXT
        );
        CREATE TABLE stock_adjustment_basis_segments (
            code TEXT,
            basis_id TEXT,
            source_date_from DATE,
            source_date_to_exclusive DATE,
            cumulative_factor DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE topix_data (
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE indices_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            sector_name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            date TEXT,
            code TEXT,
            company_name TEXT,
            market_code TEXT,
            market_name TEXT,
            scale_category TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_valuation (
            code TEXT,
            date TEXT,
            price_basis_date TEXT,
            per DOUBLE,
            forward_per DOUBLE,
            pbr DOUBLE,
            p_op DOUBLE,
            forward_p_op DOUBLE,
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            basis_version TEXT
        )
        """
    )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    codes = [
        ("1111", "Alpha", "0111", 100.0, 0.35),
        ("2222", "Beta", "0111", 180.0, -0.15),
        ("3333", "Gamma", "0111", 90.0, 0.05),
        ("4444", "Delta", "0111", 120.0, 0.12),
        ("5555", "Epsilon", "0111", 150.0, -0.03),
        ("6666", "Zeta", "0111", 75.0, 0.18),
    ]
    codes.extend(
        (
            str(7000 + extra_index),
            f"Filler {extra_index}",
            "0111",
            80.0 + extra_index,
            -0.08 + extra_index * 0.003,
        )
        for extra_index in range(120)
    )
    for index, date in enumerate(dates):
        for code, name, market_code, base, slope in codes:
            close = base + index * slope
            open_price = close * 0.995
            volume = 5_000 + index * 10 + int(code)
            stock_rows.append(
                (code, date, open_price, close * 1.01, close * 0.99, close, volume)
            )
            master_rows.append((date, code, name, market_code, "Market", None))
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, 1.0)",
        stock_rows,
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    topix_rows: list[tuple[str, float, float, float, float]] = []
    for index, date in enumerate(dates):
        if index < 150:
            topix_close = 1000.0 + index
        elif index < 190:
            topix_close = 1150.0 - (index - 150) * 1.2
        else:
            topix_close = 1102.0 + (index - 190) * 0.5
        topix_rows.append(
            (
                date,
                topix_close * 0.998,
                topix_close * 1.002,
                topix_close * 0.996,
                topix_close,
            )
        )
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    n225_rows: list[tuple[str, str, float, float, float, float, str]] = []
    for index, date in enumerate(dates):
        if index < 150:
            n225_close = 30000.0 + index * 25.0
        elif index < 190:
            n225_close = 33750.0 - (index - 150) * 42.0
        else:
            n225_close = 32070.0 + (index - 190) * 18.0
        n225_rows.append(
            (
                "N225_UNDERPX",
                date,
                n225_close * 0.998,
                n225_close * 1.002,
                n225_close * 0.996,
                n225_close,
                "synthetic",
            )
        )
    conn.executemany(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        n225_rows,
    )
    valuation_rows: list[
        tuple[
            str,
            str,
            str,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            str,
        ]
    ] = []
    for date in dates:
        valuation_rows.extend(
            [
                (
                    "1111",
                    date,
                    date,
                    12.0,
                    8.0,
                    0.5,
                    7.0,
                    6.0,
                    110_000_000.0,
                    90_000_000.0,
                    f"event-pit-v1:1111:{dates[0]}",
                ),
                (
                    "2222",
                    date,
                    date,
                    18.0,
                    30.0,
                    0.7,
                    8.0,
                    9.0,
                    220_000_000.0,
                    180_000_000.0,
                    f"event-pit-v1:2222:{dates[0]}",
                ),
                (
                    "3333",
                    date,
                    date,
                    14.0,
                    10.0,
                    2.0,
                    11.0,
                    80.0,
                    90_000_000.0,
                    70_000_000.0,
                    f"event-pit-v1:3333:{dates[0]}",
                ),
                (
                    "4444",
                    date,
                    date,
                    16.0,
                    14.0,
                    1.1,
                    9.0,
                    10.0,
                    120_000_000.0,
                    110_000_000.0,
                    f"event-pit-v1:4444:{dates[0]}",
                ),
                (
                    "5555",
                    date,
                    date,
                    20.0,
                    18.0,
                    1.6,
                    14.0,
                    15.0,
                    150_000_000.0,
                    140_000_000.0,
                    f"event-pit-v1:5555:{dates[0]}",
                ),
                (
                    "6666",
                    date,
                    date,
                    22.0,
                    22.0,
                    2.5,
                    20.0,
                    0.0,
                    75_000_000.0,
                    60_000_000.0,
                    f"event-pit-v1:6666:{dates[0]}",
                ),
            ]
        )
        valuation_rows.extend(
            (
                str(7000 + extra_index),
                date,
                date,
                13.0 + extra_index * 0.1,
                11.0 + extra_index * 0.1,
                0.9 + extra_index * 0.01,
                8.0 + extra_index * 0.1,
                12.0 + extra_index * 0.1,
                100_000_000.0 + extra_index * 1_000_000.0,
                80_000_000.0 + extra_index * 1_000_000.0,
                f"event-pit-v1:{7000 + extra_index}:{dates[0]}",
            )
            for extra_index in range(120)
        )
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
        for code, *_ in codes
    ]
    segment_rows = [
        (code, f"event-pit-v1:{code}:{dates[0]}", dates[0], None, 1.0)
        for code, *_ in codes
    ]
    conn.executemany(
        "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        basis_rows,
    )
    conn.executemany(
        "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
        segment_rows,
    )
    conn.close()
    return db_path
