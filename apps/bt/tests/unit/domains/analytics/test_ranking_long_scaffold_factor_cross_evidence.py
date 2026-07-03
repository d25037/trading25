from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_long_scaffold_factor_cross_evidence import (
    RankingLongScaffoldFactorCrossEvidenceResult,
    _FACTOR_CONDITIONS,
    _LONG_SCAFFOLDS,
    build_summary_markdown,
    run_ranking_long_scaffold_factor_cross_evidence_research,
    write_ranking_long_scaffold_factor_cross_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_long_scaffold_factor_cross_evidence_builds_factor_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_factor_cross_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.long_scaffold_evidence_df.empty
    assert not result.factor_condition_evidence_df.empty
    assert not result.long_scaffold_factor_evidence_df.empty
    assert not result.long_scaffold_factor_combo_evidence_df.empty
    assert {
        "liquidity_residual_z",
        "liquidity_z_0_to_2_rerating_flag",
        "liquidity_z_minus1_to_2_rerating_flag",
        "forecast_operating_profit_growth_ratio",
        "fwd_op_op_gt_1_2_flag",
        "forward_per_to_per_ratio",
        "good_fwd_per_flag",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "liquidity_z_0_to_2_rerating",
        "liquidity_z_minus1_to_2_rerating",
        "fwd_op_op_gt_1_2",
        "good_fwd_per",
    }.issubset(set(result.factor_condition_evidence_df["factor_condition"].astype(str)))
    assert {
        "neutral_deep_value",
        "z_0_to_2_deep_value",
        "z_minus1_to_2_deep_value",
    }.issubset(set(result.long_scaffold_evidence_df["long_scaffold"].astype(str)))
    assert {
        "liquidity_z_0_to_2_rerating__fwd_op_op_gt_1_2",
        "liquidity_z_minus1_to_2_rerating__fwd_op_op_gt_1_2",
        "liquidity_z_0_to_2_rerating__good_fwd_per",
        "liquidity_z_minus1_to_2_rerating__good_fwd_per",
        "fwd_op_op_gt_1_2__good_fwd_per",
        "liquidity_z_0_to_2_rerating__fwd_op_op_gt_1_2__good_fwd_per",
        "liquidity_z_minus1_to_2_rerating__fwd_op_op_gt_1_2__good_fwd_per",
    }.issubset(set(result.long_scaffold_factor_combo_evidence_df["factor_combo"].astype(str)))
    assert not any(
        "liquidity_z_0_to_2_rerating__liquidity_z_minus1_to_2_rerating"
        in combo
        for combo in result.long_scaffold_factor_combo_evidence_df[
            "factor_combo"
        ].astype(str)
    )


def test_factor_condition_config_defines_requested_replacement_axes() -> None:
    conditions = {name: condition for name, condition in _FACTOR_CONDITIONS}
    scaffolds = {name: condition for name, condition in _LONG_SCAFFOLDS}

    assert (
        conditions["liquidity_z_0_to_2_rerating"]
        == "liquidity_residual_z > 0.0 AND liquidity_residual_z < 2.0 "
        "AND recent_return_20d_pct >= 0.0 AND recent_return_60d_pct >= 0.0"
    )
    assert (
        conditions["liquidity_z_minus1_to_2_rerating"]
        == "liquidity_residual_z > -1.0 AND liquidity_residual_z < 2.0 "
        "AND recent_return_20d_pct >= 0.0 AND recent_return_60d_pct >= 0.0"
    )
    assert (
        conditions["fwd_op_op_gt_1_2"]
        == "forecast_operating_profit_growth_ratio > 1.2"
    )
    assert conditions["good_fwd_per"] == "forward_per_to_per_ratio <= 0.8"
    assert "neutral_deep_value_long_hybrid_atr20_accel" in scaffolds
    assert "z_0_to_2_deep_value_long_hybrid_atr20_accel" in scaffolds
    assert "z_minus1_to_2_deep_value_long_hybrid_atr20_accel" in scaffolds


def test_long_scaffold_factor_cross_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_factor_cross_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Long Scaffold Factor Cross Evidence" in summary
    assert "Long Scaffold Evidence" in summary
    assert "Factor Condition Evidence" in summary
    assert "Long Scaffold x Factor Condition Evidence" in summary
    assert "Long Scaffold x Factor Combo Evidence" in summary

    bundle = write_ranking_long_scaffold_factor_cross_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingLongScaffoldFactorCrossEvidenceResult:
    return run_ranking_long_scaffold_factor_cross_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_factor_cross_db(db_path: Path) -> Path:
    db_path = _build_sma5_count_long_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        UPDATE daily_valuation
        SET forward_p_op = 5.0
        WHERE code = '1111'
        """
    )
    conn.close()
    return db_path
