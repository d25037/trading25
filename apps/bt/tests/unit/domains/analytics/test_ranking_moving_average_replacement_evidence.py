from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_moving_average_replacement_evidence import (
    RankingMovingAverageReplacementEvidenceResult,
    build_summary_markdown,
    run_ranking_moving_average_replacement_evidence_research,
    write_ranking_moving_average_replacement_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_moving_average_replacement_evidence_builds_comparison_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.technical_condition_evidence_df.empty
    assert not result.replacement_delta_df.empty
    assert not result.long_candidate_moving_average_evidence_df.empty
    assert not result.price_action_migration_df.empty
    assert not result.overheat_overlap_df.empty
    assert {
        "sma20_deviation_pct",
        "sma60_deviation_pct",
        "ema20_deviation_pct",
        "ema60_deviation_pct",
        "fixed_price_action_bucket",
        "sma_price_action_bucket",
        "fixed_overheat_flag",
        "sma20_qmatched_overheat_flag",
        "ema20_qmatched_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "fixed_20d_pos_60d_pos",
        "sma20_pos_sma60_pos",
        "ema20_pos_ema60_pos",
    }.issubset(set(result.technical_condition_evidence_df["technical_condition"]))
    assert {"sma_dual_positive", "ema_dual_positive"}.issubset(
        set(result.replacement_delta_df["replacement_pair"])
    )
    assert {
        "neutral_deep_value",
        "neutral_deep_value_long_hybrid_atr20_accel",
    }.intersection(set(result.long_candidate_moving_average_evidence_df["long_scaffold"]))


def test_moving_average_replacement_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Moving Average Replacement Evidence" in summary
    assert "Technical Condition Evidence" in summary
    assert "Replacement Delta" in summary
    assert "Long Candidate Moving Average Evidence" in summary

    bundle = write_ranking_moving_average_replacement_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(
    db_path: Path,
) -> RankingMovingAverageReplacementEvidenceResult:
    return run_ranking_moving_average_replacement_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )
