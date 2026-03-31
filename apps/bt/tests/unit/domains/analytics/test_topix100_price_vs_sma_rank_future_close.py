from __future__ import annotations

from pathlib import Path

import pytest

from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRICE_FEATURE_ORDER,
    PRICE_SMA_WINDOW_ORDER,
    get_topix100_price_vs_sma_rank_future_close_available_date_range,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(tmp_path / "market-multi-sma.duckdb")


def test_available_date_range_and_price_feature_family_are_returned(
    analytics_db_path: str,
) -> None:
    available_start, available_end = (
        get_topix100_price_vs_sma_rank_future_close_available_date_range(
            analytics_db_path
        )
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = run_topix100_price_vs_sma_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.available_start_date == "2023-01-02"
    assert result.available_end_date == "2023-11-03"
    assert result.default_start_date == "2023-01-02"
    assert result.analysis_end_date == "2023-11-03"
    assert result.topix100_constituent_count == 10
    assert result.valid_date_count > 0
    assert result.stock_day_count > 0
    assert result.price_sma_windows == PRICE_SMA_WINDOW_ORDER
    assert result.price_feature_order == PRICE_FEATURE_ORDER
    assert set(result.ranked_panel_df["ranking_feature"].unique().tolist()) == set(
        PRICE_FEATURE_ORDER
    )


def test_deciles_and_price_volume_split_tables_are_built_for_all_price_features(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

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

    for price_feature in PRICE_FEATURE_ORDER:
        feature_summary = result.ranking_feature_summary_df[
            result.ranking_feature_summary_df["ranking_feature"] == price_feature
        ].set_index("feature_decile")
        assert list(feature_summary.index) == [f"Q{i}" for i in range(1, 11)]
        assert feature_summary["mean_ranking_value"].is_monotonic_decreasing

        bucket_features = set(
            result.price_bucket_summary_df["ranking_feature"].unique().tolist()
        )
        split_features = set(
            result.price_volume_split_summary_df["price_feature"].unique().tolist()
        )
        assert price_feature in bucket_features
        assert price_feature in split_features

    assert {
        "q1_volume_high",
        "middle_volume_low",
        "q10_volume_high",
    }.issubset(set(result.price_volume_split_summary_df["combined_bucket"]))


def test_hypothesis_tables_contain_expected_labels_for_selected_feature(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    target_feature = "price_vs_sma_100_gap"
    group_hypothesis = result.group_hypothesis_df[
        (result.group_hypothesis_df["ranking_feature"] == target_feature)
        & (result.group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.group_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(group_hypothesis["hypothesis_label"]) == {
        "Q1 vs Middle",
        "Q10 vs Middle",
        "Q1 vs Q10",
    }

    split_hypothesis = result.split_hypothesis_df[
        (result.split_hypothesis_df["price_feature"] == target_feature)
        & (result.split_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.split_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(split_hypothesis["hypothesis_label"]) == {
        "Q1 High vs Q1 Low",
        "Q10 Low vs Q10 High",
        "Q1 High vs Middle High",
        "Q1 Low vs Middle Low",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }
