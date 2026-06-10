from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_core_sector_neutral_value_regime_breakdown import (
    RankingCoreSectorNeutralValueRegimeBreakdownResult,
    build_summary_markdown,
    run_ranking_core_sector_neutral_value_regime_breakdown_research,
    write_ranking_core_sector_neutral_value_regime_breakdown_bundle,
)

from test_ranking_core_sector_relative_value_evidence import _build_core_value_db


def test_ranking_core_sector_neutral_value_regime_breakdown_builds_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.market_source == "stock_master_daily_exact_date"
    assert not result.coverage_diagnostics_df.empty
    assert not result.annual_strategy_summary_df.empty
    assert not result.bank_displacement_df.empty
    assert not result.sector_breadth_df.empty
    assert not result.sector_year_contribution_df.empty
    assert not result.strategy_breadth_regime_df.empty
    assert not result.nt_regime_strategy_df.empty
    assert not result.strategy_comparison_df.empty
    assert {
        "factor_signal",
        "factor_display_name",
        "sector_scope_label",
        "factor_minus_baseline_median_forward_topix_excess_return_pct",
    }.issubset(result.annual_strategy_summary_df.columns)
    signals = set(result.annual_strategy_summary_df["factor_signal"].astype(str))
    assert "raw_momentum_value_sector_strong" in signals
    assert "sector_neutral_momentum_value_sector_strong" in signals
    display_names = set(result.current_term_mapping_df["factor_display_name"].astype(str))
    assert "Sector-Neutral Momentum Value + Balanced Sector Strength: Strong" in display_names
    assert "Low Value" not in display_names
    assert {
        "ex_banks_median_forward_topix_excess_return_pct",
        "banks_only_median_forward_topix_excess_return_pct",
    }.issubset(result.bank_displacement_df.columns)
    assert {
        "positive_median_sector_share_pct",
        "bank_observation_share_pct",
        "max_sector_observation_share_pct",
    }.issubset(result.sector_breadth_df.columns)
    assert {
        "sector_pbr_percentile",
        "sector_forward_per_percentile",
        "bank_sector_flag",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)


def test_ranking_core_sector_neutral_value_regime_breakdown_writes_bundle(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Core Sector-Neutral Value Regime Breakdown" in summary
    assert "Annual Strategy Summary" in summary
    assert "Bank Displacement" in summary
    assert "Sector Breadth" in summary

    bundle = write_ranking_core_sector_neutral_value_regime_breakdown_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(
    db_path: Path,
) -> RankingCoreSectorNeutralValueRegimeBreakdownResult:
    return run_ranking_core_sector_neutral_value_regime_breakdown_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        min_sector_observations=2,
        observation_sample_limit=100,
    )
