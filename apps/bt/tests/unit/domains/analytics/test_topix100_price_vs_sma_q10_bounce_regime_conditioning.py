from __future__ import annotations

from pathlib import Path

import pytest

from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research,
)
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(
        tmp_path / "market-q10-bounce-regime.duckdb",
        include_regimes=True,
        extra_topix100_constituents=10,
    )


def test_q10_bounce_regime_conditioning_tables_are_returned(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.analysis_end_date == "2023-11-03"
    assert result.price_feature == DEFAULT_PRICE_FEATURE
    assert result.topix_close_stats is not None
    assert result.nt_ratio_stats is not None
    assert set(result.regime_day_counts_df["regime_type"]) == {
        "topix_close",
        "nt_ratio",
    }
    assert set(result.regime_group_day_counts_df["regime_group_key"]) == {
        "weak",
        "neutral",
        "strong",
    }
    assert {
        "middle_volume_high",
        "middle_volume_low",
        "q10_volume_high",
        "q10_volume_low",
    }.issubset(set(result.regime_summary_df["combined_bucket"]))


def test_q10_bounce_regime_conditioning_hypothesis_labels_are_expected(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    hypothesis = result.regime_hypothesis_df[
        (result.regime_hypothesis_df["regime_type"] == "topix_close")
        & (result.regime_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.regime_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(hypothesis["hypothesis_label"]) == {
        "Q10 Low vs Q10 High",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }

    collapsed = result.regime_group_hypothesis_df[
        (result.regime_group_hypothesis_df["regime_type"] == "topix_close")
        & (result.regime_group_hypothesis_df["regime_group_key"] == "neutral")
        & (result.regime_group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.regime_group_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(collapsed["hypothesis_label"]) == set(hypothesis["hypothesis_label"])


def test_q10_bounce_regime_conditioning_can_target_sma100(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research(
        analytics_db_path,
        min_constituents_per_day=10,
        price_feature="price_vs_sma_100_gap",
    )

    assert result.price_feature == "price_vs_sma_100_gap"
    assert set(result.split_panel_df["price_feature"]) == {"price_vs_sma_100_gap"}
