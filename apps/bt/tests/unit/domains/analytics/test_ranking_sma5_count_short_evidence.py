from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_sma5_count_short_evidence import (
    RankingSma5CountShortEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_count_short_evidence_research,
    write_ranking_sma5_count_short_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_liquidity_price_action_recomposition import (
    _build_recomposition_db,
)


def test_sma5_count_short_evidence_builds_5d_target_and_count_buckets(
    tmp_path: Path,
) -> None:
    db_path = _build_recomposition_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.short_overlay_evidence_df.empty
    assert not result.sma5_count_evidence_df.empty
    assert not result.sma5_count_group_evidence_df.empty
    assert not result.short_overlay_sma5_count_evidence_df.empty
    assert not result.short_overlay_sma5_count_group_evidence_df.empty
    assert {
        "sma5_above_count_5d",
        "forward_close_return_5d_pct",
        "topix_close_return_5d_pct",
        "forward_close_excess_return_5d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert set(result.observation_sample_df["sma5_above_count_5d"].dropna()).issubset(
        {0, 1, 2, 3, 4, 5}
    )
    assert set(result.sma5_count_evidence_df["sma5_above_count_5d"].dropna()).issubset(
        {0, 1, 2, 3, 4, 5}
    )
    assert set(
        result.short_overlay_sma5_count_evidence_df["sma5_above_count_5d"].dropna()
    ).issubset({0, 1, 2, 3, 4, 5})
    assert {
        "sma5_above_count_0_1",
        "sma5_above_count_2_3",
        "sma5_above_count_4_5",
    }.issubset(set(result.sma5_count_group_evidence_df["sma5_count_group"].astype(str)))
    assert {
        "sma5_above_count_0_1",
        "sma5_above_count_2_3",
        "sma5_above_count_4_5",
    }.issubset(
        set(
            result.short_overlay_sma5_count_group_evidence_df[
                "sma5_count_group"
            ].astype(str)
        )
    )
    assert 5 in set(result.sma5_count_evidence_df["horizon"].astype(int))
    assert {
        "high_psr",
        "sector_weak",
        "high_psr_sector_weak",
    }.issubset(
        set(result.short_overlay_sma5_count_evidence_df["short_overlay"].astype(str))
    )


def test_sma5_count_short_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_recomposition_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 Count Short Evidence" in summary
    assert "SMA5 Count Evidence" in summary
    assert "SMA5 Count Group Evidence" in summary
    assert "Short Overlay x SMA5 Count Evidence" in summary
    assert "Short Overlay x SMA5 Count Group Evidence" in summary

    bundle = write_ranking_sma5_count_short_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSma5CountShortEvidenceResult:
    return run_ranking_sma5_count_short_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        liquidity_bands=("high", "mid", "low"),
        min_observations=1,
        observation_sample_limit=100,
    )
