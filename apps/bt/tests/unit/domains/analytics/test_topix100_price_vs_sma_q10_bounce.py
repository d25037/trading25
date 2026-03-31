from __future__ import annotations

from pathlib import Path

import pytest

from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    run_topix100_price_vs_sma_q10_bounce_research,
)
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(
        tmp_path / "market-q10-bounce.duckdb",
        extra_topix100_constituents=10,
    )


def test_q10_bounce_tables_are_built_for_all_price_features(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.base_result.analysis_end_date == "2023-11-03"
    assert result.price_feature_order == (
        "price_vs_sma_20_gap",
        "price_vs_sma_50_gap",
        "price_vs_sma_100_gap",
    )
    assert set(Q10_MIDDLE_COMBINED_BUCKET_ORDER).issubset(
        set(result.q10_middle_volume_split_summary_df["combined_bucket"])
    )
    assert set(result.q10_middle_volume_split_summary_df["price_feature"]) == set(
        result.price_feature_order
    )
    assert not result.q10_low_scorecard_df.empty


def test_q10_low_hypothesis_labels_are_returned_for_selected_feature(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    target = result.q10_low_hypothesis_df[
        (result.q10_low_hypothesis_df["price_feature"] == "price_vs_sma_100_gap")
        & (result.q10_low_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.q10_low_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(target["hypothesis_label"]) == {
        "Q10 Low vs Q10 High",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }

    scorecard = result.q10_low_scorecard_df[
        (result.q10_low_scorecard_df["price_feature"] == "price_vs_sma_100_gap")
        & (result.q10_low_scorecard_df["horizon_key"] == "t_plus_10")
        & (result.q10_low_scorecard_df["metric_key"] == "future_return")
    ]
    assert set(scorecard["hypothesis_label"]) == set(target["hypothesis_label"])
    assert scorecard["positive_date_share"].between(0, 1).all()


def test_q10_bounce_can_be_filtered_to_long_sma_features(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
        price_features=["price_vs_sma_50_gap", "price_vs_sma_100_gap"],
    )

    assert result.price_feature_order == (
        "price_vs_sma_50_gap",
        "price_vs_sma_100_gap",
    )
    assert set(result.q10_middle_volume_split_summary_df["price_feature"]) == {
        "price_vs_sma_50_gap",
        "price_vs_sma_100_gap",
    }
    assert set(result.q10_low_spread_daily_df["price_feature"]) == {
        "price_vs_sma_50_gap",
        "price_vs_sma_100_gap",
    }
