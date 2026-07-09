from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_long_sector_leadership_horizon_decomposition import (
    RankingLongSectorLeadershipHorizonDecompositionResult,
    build_summary_markdown,
    run_ranking_long_sector_leadership_horizon_decomposition_research,
    write_ranking_long_sector_leadership_horizon_decomposition_bundle,
)

from test_ranking_core_sector_relative_value_evidence import _build_core_value_db


def test_ranking_long_sector_leadership_horizon_decomposition_builds_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.market_source == "stock_master_daily_exact_date"
    assert not result.coverage_diagnostics_df.empty
    assert not result.annual_overlay_summary_df.empty
    assert not result.selected_sector_strength_summary_df.empty
    assert not result.bank_concentration_df.empty
    assert not result.sector_contribution_df.empty
    assert not result.leadership_horizon_df.empty
    assert not result.balanced_vs_long_matrix_df.empty
    assert not result.balanced_long_switch_attribution_df.empty
    assert not result.long_hybrid_balanced_tolerance_df.empty
    assert not result.future_top5_diagnostic_df.empty
    assert not result.overlay_comparison_df.empty
    assert {
        "overlay_signal",
        "overlay_display_name",
        "sector_scope_label",
        "median_forward_topix_excess_return_pct",
        "bank_observation_share_pct",
        "future_top5_sector_share_pct",
    }.issubset(result.annual_overlay_summary_df.columns)
    signals = set(result.annual_overlay_summary_df["overlay_signal"].astype(str))
    assert "balanced_sector_strength_strong" in signals
    assert "long_hybrid_leadership_strong" in signals
    selected_signals = set(
        result.selected_sector_strength_summary_df["overlay_signal"].astype(str)
    )
    assert selected_signals == {
        "balanced_sector_strength_strong",
        "long_hybrid_leadership_strong",
    }
    display_names = set(
        result.overlay_term_mapping_df["overlay_display_name"].astype(str)
    )
    assert "Momentum Value + Balanced Sector Strength: Strong" in display_names
    assert "Low Value" not in display_names
    assert "High Value" not in display_names
    assert {
        "long_index_leadership_score",
        "long_constituent_breadth_leadership_score",
        "long_hybrid_leadership_score",
        "future_top5_sector_flag",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "switch_group",
        "period_label",
        "date_level_median_forward_topix_excess_return_pct",
        "date_level_ir",
    }.issubset(result.balanced_long_switch_attribution_df.columns)
    assert "common_both_strong" in set(
        result.balanced_long_switch_attribution_df["switch_group"].astype(str)
    )
    assert {
        "balanced_score_band",
        "balanced_score_band_label",
        "date_level_median_forward_topix_excess_return_pct",
        "median_balanced_sector_strength_score",
    }.issubset(result.long_hybrid_balanced_tolerance_df.columns)


def test_ranking_long_sector_leadership_horizon_decomposition_writes_bundle(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Long Sector Leadership Horizon Decomposition" in summary
    assert "Annual Overlay Summary" in summary
    assert "Selected Sector Strength Summary" in summary
    assert "Bank Concentration" in summary
    assert "Balanced Long Switch Attribution" in summary
    assert "Long Hybrid Balanced Tolerance" in summary
    assert "Future Top 5 Diagnostic" in summary

    bundle = write_ranking_long_sector_leadership_horizon_decomposition_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_ranking_long_sector_leadership_horizon_decomposition_selects_score_family(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")
    result = _run_test_research(
        db_path, sector_strength_family="long_hybrid_leadership"
    )

    selected_signals = set(
        result.selected_sector_strength_summary_df["overlay_signal"].astype(str)
    )
    assert selected_signals == {"long_hybrid_leadership_strong"}


def _run_test_research(
    db_path: Path,
    *,
    sector_strength_family: str = "both",
) -> RankingLongSectorLeadershipHorizonDecompositionResult:
    _boost_low_value_momentum_fixture(db_path)
    return run_ranking_long_sector_leadership_horizon_decomposition_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        leadership_windows=(1, 2),
        sector_strength_family=sector_strength_family,
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _boost_low_value_momentum_fixture(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        UPDATE stock_data AS s
        SET
            close = 80.0 + d.date_index * 1.20 + CAST(substr(s.code, 4, 1) AS INTEGER),
            open = 80.0 + d.date_index * 1.20 + CAST(substr(s.code, 4, 1) AS INTEGER),
            high = 81.0 + d.date_index * 1.20 + CAST(substr(s.code, 4, 1) AS INTEGER),
            low = 79.0 + d.date_index * 1.20 + CAST(substr(s.code, 4, 1) AS INTEGER)
        FROM (
            SELECT
                date,
                row_number() OVER (ORDER BY date) - 1 AS date_index
            FROM topix_data
        ) d
        WHERE s.date = d.date
          AND s.code IN ('1100', '1101', '1102', '1103', '1104')
        """
    )
    conn.close()
