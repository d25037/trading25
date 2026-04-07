from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests.unit.analytics_market_research_db import build_topix100_research_market_db
from src.domains.analytics.topix100_sma_ratio_regime_conditioning import (
    TOPIX100_SMA_RATIO_REGIME_RESEARCH_EXPERIMENT_ID,
    get_topix100_sma_ratio_regime_conditioning_bundle_path_for_run_id,
    get_topix100_sma_ratio_regime_conditioning_latest_bundle_path,
    load_topix100_sma_ratio_regime_conditioning_research_bundle,
    run_topix100_sma_ratio_regime_conditioning_research,
    write_topix100_sma_ratio_regime_conditioning_research_bundle,
)


@pytest.fixture(scope="module")
def analytics_db_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    tmp_dir = tmp_path_factory.mktemp("topix100-sma-ratio-regime-conditioning")
    return build_topix100_research_market_db(
        tmp_dir / "market.duckdb",
        include_regimes=True,
    )


@pytest.fixture(scope="module")
def research_result(analytics_db_path: str):
    return run_topix100_sma_ratio_regime_conditioning_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )


def test_regime_conditioning_tables_are_returned(research_result) -> None:
    result = research_result

    assert result.analysis_start_date == "2023-07-28"
    assert result.analysis_end_date == "2023-11-03"
    assert result.universe_constituent_count == 10
    assert result.valid_date_count == 71
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
    assert not result.regime_summary_df.empty
    assert not result.regime_group_summary_df.empty
    assert {
        "q1_volume_high",
        "middle_volume_high",
    }.issubset(set(result.regime_summary_df["combined_bucket"]))


def test_regime_conditioning_hypothesis_table_contains_expected_labels(
    research_result,
) -> None:
    result = research_result

    hypothesis = result.regime_hypothesis_df[
        (result.regime_hypothesis_df["regime_type"] == "topix_close")
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
        (result.regime_group_hypothesis_df["regime_type"] == "topix_close")
        & (result.regime_group_hypothesis_df["regime_group_key"] == "neutral")
        & (result.regime_group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.regime_group_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(collapsed["hypothesis_label"]) == set(hypothesis["hypothesis_label"])


def test_regime_conditioning_bundle_roundtrip(
    research_result,
    tmp_path: Path,
) -> None:
    result = research_result

    bundle = write_topix100_sma_ratio_regime_conditioning_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_182000_testabcd",
    )
    reloaded = load_topix100_sma_ratio_regime_conditioning_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX100_SMA_RATIO_REGIME_RESEARCH_EXPERIMENT_ID
    assert (
        get_topix100_sma_ratio_regime_conditioning_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_sma_ratio_regime_conditioning_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        reloaded.regime_summary_df,
        result.regime_summary_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.regime_group_hypothesis_df,
        result.regime_group_hypothesis_df,
        check_dtype=False,
    )
