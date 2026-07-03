from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_sma5_below_streak_evidence import (
    RankingSma5BelowStreakEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_below_streak_evidence_research,
    write_ranking_sma5_below_streak_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_sma5_below_streak_evidence_builds_tables(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.sma5_below_streak_evidence_df.empty
    assert not result.long_scaffold_sma5_below_streak_evidence_df.empty
    assert not result.long_scaffold_sma5_below_streak_count_cross_df.empty
    assert not result.same_day_sma5_below_streak_spread_df.empty
    assert not result.long_scaffold_same_day_sma5_below_streak_spread_df.empty
    assert not (
        result.long_scaffold_same_day_sma5_below_streak_count_cross_spread_df.empty
    )
    assert {
        "close_below_sma5_count_3d",
        "sma5_above_count_5d",
        "below_sma5_streak_ge3_flag",
        "sma5_below_streak_bucket",
        "sma5_count_group",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert set(
        result.sma5_below_streak_evidence_df["sma5_below_streak_bucket"].astype(str)
    ).issubset(
        {
            "below_sma5_streak_other",
            "below_sma5_streak_ge3",
        }
    )
    assert {
        "base_sma5_below_streak_bucket",
        "comparison_sma5_below_streak_bucket",
        "matched_date_count",
        "median_daily_median_excess_spread_pct",
        "comparison_outperform_date_rate_pct",
    }.issubset(result.same_day_sma5_below_streak_spread_df.columns)
    assert (
        result.same_day_sma5_below_streak_spread_df["matched_date_count"].astype(int)
        > 0
    ).all()
    assert {
        "weak_sma5_below_streak_bucket",
        "weak_sma5_count_group",
        "comparison_sma5_below_streak_bucket",
        "comparison_sma5_count_group",
        "median_daily_median_excess_weak_minus_comparison_pct",
        "weak_underperform_date_rate_pct",
    }.issubset(
        result.long_scaffold_same_day_sma5_below_streak_count_cross_spread_df.columns
    )


def test_sma5_below_streak_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 Below-Streak Evidence" in summary
    assert "SMA5 Below-Streak Evidence" in summary
    assert "Long Scaffold x SMA5 Below-Streak Evidence" in summary
    assert "Long Scaffold x SMA5 Below-Streak x SMA5 Count Cross Evidence" in summary
    assert "Same-Day SMA5 Below-Streak Spread" in summary
    assert "Long Scaffold Same-Day SMA5 Below-Streak x SMA5 Count Cross Spread" in summary

    bundle = write_ranking_sma5_below_streak_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSma5BelowStreakEvidenceResult:
    return run_ranking_sma5_below_streak_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )
