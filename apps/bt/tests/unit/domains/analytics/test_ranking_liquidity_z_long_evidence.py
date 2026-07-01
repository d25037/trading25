from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_liquidity_z_long_evidence import (
    RankingLiquidityZLongEvidenceResult,
    _LIQUIDITY_Z_BUCKETS,
    build_summary_markdown,
    run_ranking_liquidity_z_long_evidence_research,
    write_ranking_liquidity_z_long_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_liquidity_z_long_evidence_builds_bucket_and_cap_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.z_bucket_evidence_df.empty
    assert not result.long_scaffold_z_bucket_evidence_df.empty
    assert not result.long_scaffold_z_cap_evidence_df.empty
    assert {
        "liquidity_residual_z",
        "liquidity_z_bucket",
        "liquidity_z_cap",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "z_minus1_to_0",
        "z_0_to_1",
        "z_1_to_2",
    }.intersection(set(result.z_bucket_evidence_df["liquidity_z_bucket"].astype(str)))
    assert {
        "z_cap_minus1_to_1",
        "z_cap_minus1_to_2",
        "z_cap_minus1_to_3",
    }.issubset(set(result.long_scaffold_z_cap_evidence_df["liquidity_z_cap"].astype(str)))
    assert {
        "all_rerating_price_action",
        "deep_value_long_hybrid_atr20_accel",
    }.intersection(set(result.long_scaffold_z_bucket_evidence_df["long_scaffold"].astype(str)))


def test_liquidity_z_bucket_config_splits_minus3_to_plus3_by_one() -> None:
    assert tuple(name for name, _condition in _LIQUIDITY_Z_BUCKETS) == (
        "z_lt_minus3",
        "z_minus3_to_minus2",
        "z_minus2_to_minus1",
        "z_minus1_to_0",
        "z_0_to_1",
        "z_1_to_2",
        "z_2_to_3",
        "z_ge_3",
    )


def test_liquidity_z_long_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Liquidity Z Long Evidence" in summary
    assert "Liquidity Z Bucket Evidence" in summary
    assert "Long Scaffold x Liquidity Z Cap Evidence" in summary

    bundle = write_ranking_liquidity_z_long_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingLiquidityZLongEvidenceResult:
    return run_ranking_liquidity_z_long_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )
