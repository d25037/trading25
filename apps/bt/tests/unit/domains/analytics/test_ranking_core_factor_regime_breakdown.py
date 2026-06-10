from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_core_factor_regime_breakdown import (
    RankingCoreFactorRegimeBreakdownResult,
    build_summary_markdown,
    run_ranking_core_factor_regime_breakdown_research,
    write_ranking_core_factor_regime_breakdown_bundle,
)

from test_ranking_core_sector_relative_value_evidence import _build_core_value_db


def test_ranking_core_factor_regime_breakdown_builds_factor_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.market_source == "stock_master_daily_exact_date"
    assert not result.year_factor_spread_df.empty
    assert not result.year_breadth_summary_df.empty
    assert not result.annual_factor_breadth_df.empty
    assert not result.nt_ratio_regime_summary_df.empty
    assert not result.factor_nt_regime_df.empty
    assert not result.bank_exclusion_df.empty
    assert not result.regime_comparison_df.empty
    assert "core_slice" in result.core_failure_decomposition_df.columns
    assert "factor_display_name" in result.core_failure_decomposition_df.columns
    assert "sector_33_name" in result.sector_year_contribution_df.columns
    assert "breadth_bucket" in result.annual_factor_breadth_df.columns
    assert "factor_family" in result.annual_factor_breadth_df.columns
    assert set(result.year_breadth_summary_df["breadth_label"].astype(str)).issubset(
        {"Low Breadth", "Mid Breadth", "High Breadth"}
    )
    assert {
        "high_breadth_median_forward_topix_excess_return_pct",
        "low_breadth_median_forward_topix_excess_return_pct",
        "low_minus_high_median_forward_topix_excess_return_pct",
    }.issubset(result.factor_resilience_df.columns)
    assert {
        "nt_regime_60d_label",
        "factor_median_forward_topix_excess_return_pct",
        "baseline_median_forward_topix_excess_return_pct",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
    }.issubset(result.factor_nt_regime_df.columns)
    assert {
        "analysis_scope",
        "sector_scope",
        "sector_scope_label",
        "baseline_median_forward_topix_excess_return_pct",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
    }.issubset(result.bank_exclusion_df.columns)
    assert "ex Banks" in set(result.bank_exclusion_df["sector_scope_label"].astype(str))
    assert {
        "factor_signal",
        "factor_family",
        "factor_display_name",
    }.issubset(result.current_term_mapping_df.columns)
    display_names = set(
        result.current_term_mapping_df["factor_display_name"].astype(str)
    )
    assert "Undervalued" in display_names
    assert "Overvalued + 20/60D Momentum" in display_names
    assert "Cheap Valuation" not in display_names
    assert "Low Value" not in display_names
    assert "Expensive Momentum" not in display_names
    assert {
        "low_value",
        "momentum_20_60_top20",
    }.issubset(set(result.year_factor_spread_df["factor_signal"].astype(str)))
    assert "20/60D Momentum" in set(
        result.year_factor_spread_df["factor_display_name"].astype(str)
    )
    assert "Momentum Value + Balanced Sector Strength: Strong" in set(
        result.year_factor_spread_df["factor_display_name"].astype(str)
    )
    assert {
        "year_group",
        "factor_signal",
        "factor_display_name",
        "median_forward_topix_excess_return_pct",
        "severe_loss_rate_pct",
    }.issubset(result.regime_comparison_df.columns)
    expected_sample_columns = {
        "year",
        "core_slice",
        "factor_signal",
        "atr_state",
        "nt_regime_60d_label",
        "forward_close_excess_return_20d_pct",
    }
    assert expected_sample_columns.issubset(result.observation_sample_df.columns)


def test_ranking_core_factor_regime_breakdown_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Core Factor Regime Breakdown" in summary
    assert "Year Factor Spread" in summary
    assert "Year Breadth Summary" in summary
    assert "Annual Factor x Breadth" in summary
    assert "NT Ratio Regime Summary" in summary
    assert "Factor x NT 60D Regime" in summary
    assert "Bank Exclusion" in summary
    assert "Factor Resilience" in summary
    assert "Core Failure Decomposition" in summary
    assert "20/60D Momentum" in summary

    bundle = write_ranking_core_factor_regime_breakdown_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingCoreFactorRegimeBreakdownResult:
    return run_ranking_core_factor_regime_breakdown_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 10, 20, 60),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )
