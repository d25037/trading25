from __future__ import annotations

from pathlib import Path

import pytest

from src.domains.analytics.topix100_vi_change_regime_conditioning import (
    get_topix100_vi_change_available_date_range,
    run_topix100_vi_change_regime_conditioning_research,
)
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(
        tmp_path / "market.duckdb",
        include_vi=True,
    )


def test_vi_available_date_range_is_returned(analytics_db_path: str) -> None:
    available_start, available_end = get_topix100_vi_change_available_date_range(
        analytics_db_path
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"


def test_vi_regime_conditioning_tables_are_returned(analytics_db_path: str) -> None:
    result = run_topix100_vi_change_regime_conditioning_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.analysis_start_date == "2023-04-21"
    assert result.analysis_end_date == "2023-11-03"
    assert result.available_start_date == "2023-01-02"
    assert result.available_end_date == "2023-11-03"
    assert result.universe_constituent_count == 10
    assert result.valid_date_count == 141
    assert result.vi_change_stats is not None
    assert set(result.regime_day_counts_df["regime_type"]) == {"vi_change"}
    assert set(result.regime_group_day_counts_df["regime_group_key"]) == {
        "weak",
        "neutral",
        "strong",
    }
    assert not result.regime_summary_df.empty
    assert not result.regime_group_summary_df.empty
    assert {
        "q1_volume_high",
        "middle_volume_high",
    }.issubset(set(result.regime_summary_df["combined_bucket"]))
    assert set(result.split_panel_df["price_feature"]) == {"price_vs_sma_20_gap"}
    assert set(result.split_panel_df["volume_feature"]) == {"volume_sma_20_80"}


def test_vi_regime_conditioning_hypothesis_table_contains_expected_labels(
    analytics_db_path: str,
) -> None:
    result = run_topix100_vi_change_regime_conditioning_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    hypothesis = result.regime_hypothesis_df[
        (result.regime_hypothesis_df["regime_type"] == "vi_change")
        & (result.regime_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.regime_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(hypothesis["hypothesis_label"]) == {
        "Q1 High vs Q1 Low",
        "Q10 Low vs Q10 High",
        "Q1 High vs Middle High",
        "Q1 Low vs Middle Low",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }
    collapsed = result.regime_group_hypothesis_df[
        (result.regime_group_hypothesis_df["regime_group_key"] == "neutral")
        & (result.regime_group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.regime_group_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(collapsed["hypothesis_label"]) == set(hypothesis["hypothesis_label"])
