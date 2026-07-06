from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_trend_slope_evidence import (
    RankingTrendSlopeEvidenceResult,
    build_summary_markdown,
    run_ranking_trend_slope_evidence_research,
    write_ranking_trend_slope_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_trend_slope_evidence_builds_slope_and_conflict_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.technical_condition_evidence_df.empty
    assert not result.fixed_vs_slope_conflict_df.empty
    assert not result.long_candidate_trend_slope_evidence_df.empty
    assert {
        "price_lr_slope_20_pct",
        "price_lr_slope_60_pct",
        "price_lr_r2_20",
        "price_lr_r2_60",
        "sma20_slope_5d_pct",
        "sma60_slope_20d_pct",
        "ema20_slope_5d_pct",
        "ema60_slope_20d_pct",
        "fixed_20d_sign_bucket",
        "lr20_sign_bucket",
        "fixed20_lr20_conflict_bucket",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "lr20_pos_lr60_pos",
        "sma20_slope_pos_sma60_slope_pos",
        "ema20_slope_pos_ema60_slope_pos",
    }.issubset(set(result.technical_condition_evidence_df["technical_condition"]))
    assert {
        "fixed20_pos_lr20_pos",
        "fixed20_pos_lr20_neg",
    }.intersection(set(result.fixed_vs_slope_conflict_df["conflict_bucket"]))
    assert {
        "neutral_deep_value",
        "neutral_deep_value_long_hybrid_atr20_accel",
    }.intersection(set(result.long_candidate_trend_slope_evidence_df["long_scaffold"]))


def test_trend_slope_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Trend Slope Evidence" in summary
    assert "Technical Condition Evidence" in summary
    assert "Fixed vs Slope Conflict" in summary
    assert "Long Candidate Trend Slope Evidence" in summary

    bundle = write_ranking_trend_slope_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingTrendSlopeEvidenceResult:
    return run_ranking_trend_slope_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )
