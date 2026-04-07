from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests.unit.analytics_market_research_db import build_topix100_research_market_db
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    RANKING_FEATURE_ORDER,
    TOPIX100_SMA_RATIO_RESEARCH_EXPERIMENT_ID,
    get_topix100_sma_ratio_rank_future_close_bundle_path_for_run_id,
    get_topix100_sma_ratio_rank_future_close_available_date_range,
    get_topix100_sma_ratio_rank_future_close_latest_bundle_path,
    load_topix100_sma_ratio_rank_future_close_research_bundle,
    run_topix100_sma_ratio_rank_future_close_research,
    write_topix100_sma_ratio_rank_future_close_research_bundle,
)


@pytest.fixture(scope="module")
def analytics_db_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    tmp_dir = tmp_path_factory.mktemp("topix100-sma-ratio-rank-future-close")
    return build_topix100_research_market_db(tmp_dir / "market.duckdb")


@pytest.fixture(scope="module")
def min4_result(analytics_db_path: str):
    return run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=4,
    )


@pytest.fixture(scope="module")
def min10_result(analytics_db_path: str):
    return run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )


def test_available_date_range_and_default_start_are_returned(
    analytics_db_path: str,
    min4_result,
) -> None:
    available_start, available_end = (
        get_topix100_sma_ratio_rank_future_close_available_date_range(analytics_db_path)
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = min4_result

    assert result.available_start_date == "2023-01-02"
    assert result.available_end_date == "2023-11-03"
    assert result.default_start_date == "2023-01-02"
    assert result.analysis_start_date == "2023-07-28"
    assert result.analysis_end_date == "2023-11-03"
    assert result.topix100_constituent_count == 10
    assert result.valid_date_count == 71
    assert result.stock_day_count == 710
    assert result.ranked_event_count == 4260


def test_ranked_panel_uses_all_sma_ratio_features_and_4digit_preference(
    min10_result,
) -> None:
    result = min10_result

    assert sorted(result.event_panel_df["code"].unique().tolist()) == [
        "1111",
        "1234",
        "2222",
        "3333",
        "4444",
        "5555",
        "6666",
        "7777",
        "8888",
        "9999",
    ]
    assert tuple(result.ranked_panel_df["ranking_feature"].unique()) == RANKING_FEATURE_ORDER

    feature_summary = result.ranking_feature_summary_df[
        result.ranking_feature_summary_df["ranking_feature"] == "price_sma_20_80"
    ].set_index("feature_decile")
    assert list(feature_summary.index) == [f"Q{i}" for i in range(1, 11)]
    assert feature_summary["mean_ranking_value"].is_monotonic_decreasing

    volume_summary = result.ranking_feature_summary_df[
        result.ranking_feature_summary_df["ranking_feature"] == "volume_sma_20_80"
    ].set_index("feature_decile")
    assert volume_summary["mean_ranking_value"].is_monotonic_decreasing


def test_feature_selection_and_composite_scores_are_returned(
    min10_result,
) -> None:
    result = min10_result

    assert result.discovery_end_date == "2021-12-31"
    assert result.validation_start_date == "2022-01-01"
    assert set(result.selected_feature_df["feature_family"]) == {"price", "volume"}
    assert set(result.selected_feature_df["horizon_key"]) == {
        "t_plus_1",
        "t_plus_5",
        "t_plus_10",
    }
    assert len(result.selected_feature_df) == 6

    assert set(result.composite_candidate_df["selected_horizon_key"]) == {
        "t_plus_1",
        "t_plus_5",
        "t_plus_10",
    }
    assert set(result.composite_candidate_df["score_method"]) == {
        "rank_mean",
        "rank_product",
    }
    assert len(result.composite_candidate_df) == 6
    assert len(result.selected_composite_df) == 3

    selected_features = set(result.selected_composite_df["ranking_feature"])
    assert selected_features
    assert selected_features.issubset(
        set(result.selected_composite_ranking_summary_df["ranking_feature"])
    )
    assert selected_features.issubset(
        set(result.selected_composite_future_summary_df["ranking_feature"])
    )
    assert selected_features.issubset(
        set(result.selected_composite_global_significance_df["ranking_feature"])
    )
    assert selected_features.issubset(
        set(result.selected_composite_pairwise_significance_df["ranking_feature"])
    )
    assert not result.extreme_vs_middle_summary_df.empty
    assert not result.extreme_vs_middle_significance_df.empty


def test_extreme_vs_middle_tables_are_returned(min10_result) -> None:
    result = min10_result

    row = result.extreme_vs_middle_significance_df[
        (result.extreme_vs_middle_significance_df["ranking_feature"] == "price_sma_20_80")
        & (result.extreme_vs_middle_significance_df["horizon_key"] == "t_plus_10")
        & (result.extreme_vs_middle_significance_df["metric_key"] == "future_return")
    ].iloc[0]

    assert row["n_dates"] > 0
    assert row["extreme_group_label"] == "Q1 + Q10"
    assert row["middle_group_label"] == "Q4 + Q5 + Q6"
    assert pd.notna(row["extreme_minus_middle_mean"])


def test_nested_volume_split_tables_are_returned(min10_result) -> None:
    result = min10_result

    summary = result.nested_volume_split_summary_df[
        result.nested_volume_split_summary_df["horizon_key"] == "t_plus_10"
    ]
    assert set(summary["nested_combined_bucket"]) == {
        "extreme_volume_high",
        "extreme_volume_low",
        "middle_volume_high",
        "middle_volume_low",
    }

    global_row = result.nested_volume_split_global_significance_df[
        (result.nested_volume_split_global_significance_df["horizon_key"] == "t_plus_10")
        & (
            result.nested_volume_split_global_significance_df["metric_key"]
            == "future_return"
        )
    ].iloc[0]
    assert global_row["n_dates"] > 0

    interaction_row = result.nested_volume_split_interaction_df[
        (result.nested_volume_split_interaction_df["horizon_key"] == "t_plus_10")
        & (result.nested_volume_split_interaction_df["metric_key"] == "future_return")
    ].iloc[0]
    assert pd.notna(interaction_row["interaction_difference"])


def test_q1_q10_volume_split_tables_are_returned(min10_result) -> None:
    result = min10_result

    summary = result.q1_q10_volume_split_summary_df[
        result.q1_q10_volume_split_summary_df["horizon_key"] == "t_plus_10"
    ]
    assert not result.q1_q10_volume_split_panel_df.empty
    assert {"q1_volume_high", "q10_volume_high"}.issubset(
        set(summary["q1_q10_combined_bucket"])
    )

    global_row = result.q1_q10_volume_split_global_significance_df[
        (result.q1_q10_volume_split_global_significance_df["horizon_key"] == "t_plus_10")
        & (
            result.q1_q10_volume_split_global_significance_df["metric_key"]
            == "future_return"
        )
    ].iloc[0]
    assert global_row["n_dates"] >= 0

    interaction_row = result.q1_q10_volume_split_interaction_df[
        (result.q1_q10_volume_split_interaction_df["horizon_key"] == "t_plus_10")
        & (result.q1_q10_volume_split_interaction_df["metric_key"] == "future_return")
    ].iloc[0]
    assert interaction_row["n_dates"] >= 0


def test_q10_low_hypothesis_tables_are_returned(min10_result) -> None:
    result = min10_result

    assert not result.q10_middle_volume_split_panel_df.empty
    summary = result.q10_middle_volume_split_summary_df[
        result.q10_middle_volume_split_summary_df["horizon_key"] == "t_plus_10"
    ]
    assert {"q10_volume_high", "middle_volume_high"}.issubset(
        set(summary["q10_middle_combined_bucket"])
    )

    hypothesis = result.q10_low_hypothesis_df[
        (result.q10_low_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.q10_low_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(hypothesis["hypothesis_label"]) == {
        "Q10 Low vs Q10 High",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }


def test_significance_tables_detect_ratio_rank_difference(min10_result) -> None:
    result = min10_result

    global_row = result.global_significance_df[
        (result.global_significance_df["ranking_feature"] == "price_sma_20_80")
        & (result.global_significance_df["horizon_key"] == "t_plus_10")
        & (result.global_significance_df["metric_key"] == "future_return")
    ].iloc[0]

    assert global_row["n_dates"] == 61
    assert global_row["q1_mean"] > global_row["q10_mean"]
    assert global_row["friedman_p_value"] < 0.05
    assert global_row["kruskal_p_value"] < 0.05

    pairwise_row = result.pairwise_significance_df[
        (result.pairwise_significance_df["ranking_feature"] == "price_sma_20_80")
        & (result.pairwise_significance_df["horizon_key"] == "t_plus_10")
        & (result.pairwise_significance_df["metric_key"] == "future_return")
        & (result.pairwise_significance_df["left_decile"] == "Q1")
        & (result.pairwise_significance_df["right_decile"] == "Q10")
    ].iloc[0]

    assert pairwise_row["mean_difference"] > 0
    assert pairwise_row["paired_t_p_value_holm"] < 0.05
    assert pairwise_row["wilcoxon_p_value_holm"] < 0.05


def test_sma_ratio_research_bundle_roundtrip(
    min10_result,
    tmp_path: Path,
) -> None:
    result = min10_result

    bundle = write_topix100_sma_ratio_rank_future_close_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_181000_testabcd",
    )
    reloaded = load_topix100_sma_ratio_rank_future_close_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX100_SMA_RATIO_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_topix100_sma_ratio_rank_future_close_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_sma_ratio_rank_future_close_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.analysis_start_date == result.analysis_start_date
    assert reloaded.analysis_end_date == result.analysis_end_date
    pd.testing.assert_frame_equal(
        reloaded.decile_future_summary_df,
        result.decile_future_summary_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.selected_composite_df,
        result.selected_composite_df,
        check_dtype=False,
    )
