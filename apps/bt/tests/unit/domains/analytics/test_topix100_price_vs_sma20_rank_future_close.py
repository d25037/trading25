from __future__ import annotations

from pathlib import Path

import pytest

from tests.unit.analytics_market_research_db import build_topix100_research_market_db
from src.domains.analytics.topix100_price_vs_sma20_rank_future_close import (
    PRIMARY_PRICE_FEATURE,
    get_topix100_price_vs_sma20_rank_future_close_available_date_range,
    run_topix100_price_vs_sma20_rank_future_close_research,
)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(tmp_path / "market.duckdb")


def test_available_date_range_and_primary_feature_are_returned(
    analytics_db_path: str,
) -> None:
    available_start, available_end = (
        get_topix100_price_vs_sma20_rank_future_close_available_date_range(
            analytics_db_path
        )
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = run_topix100_price_vs_sma20_rank_future_close_research(
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
    assert result.ranked_panel_df["ranking_feature"].unique().tolist() == [
        PRIMARY_PRICE_FEATURE
    ]


def test_deciles_and_price_volume_split_tables_are_built(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma20_rank_future_close_research(
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

    feature_summary = result.ranking_feature_summary_df[
        result.ranking_feature_summary_df["ranking_feature"] == PRIMARY_PRICE_FEATURE
    ].set_index("feature_quartile")
    assert list(feature_summary.index) == [f"Q{i}" for i in range(1, 11)]
    assert feature_summary["mean_ranking_value"].is_monotonic_decreasing

    assert set(result.price_bucket_summary_df["price_bucket"]) == {
        "q1",
        "middle",
        "q10",
    }
    assert {
        "q1_volume_high",
        "middle_volume_low",
        "q10_volume_high",
    }.issubset(set(result.price_volume_split_summary_df["combined_bucket"]))


def test_hypothesis_tables_contain_expected_labels(analytics_db_path: str) -> None:
    result = run_topix100_price_vs_sma20_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    group_hypothesis = result.group_hypothesis_df[
        (result.group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.group_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(group_hypothesis["hypothesis_label"]) == {
        "Q1 vs Middle",
        "Q10 vs Middle",
        "Q1 vs Q10",
    }

    split_hypothesis = result.split_hypothesis_df[
        (result.split_hypothesis_df["horizon_key"] == "t_plus_10")
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
