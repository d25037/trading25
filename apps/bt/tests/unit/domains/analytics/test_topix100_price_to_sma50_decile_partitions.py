from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.topix100_price_to_sma50_decile_partitions import (
    PRICE_FEATURE,
    TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
    VOLUME_FEATURE,
    get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id,
    get_topix100_price_to_sma50_decile_partitions_latest_bundle_path,
    load_topix100_price_to_sma50_decile_partitions_research_bundle,
    run_topix100_price_to_sma50_decile_partitions_research,
    write_topix100_price_to_sma50_decile_partitions_research_bundle,
)
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


@pytest.fixture(scope="module")
def analytics_db_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    tmp_dir = tmp_path_factory.mktemp("topix100-price-to-sma50-decile-partitions")
    return build_topix100_research_market_db(
        tmp_dir / "market-price-to-sma50-partitions.duckdb",
        extra_topix100_constituents=10,
    )


@pytest.fixture(scope="module")
def research_result(analytics_db_path: str):
    return run_topix100_price_to_sma50_decile_partitions_research(
        analytics_db_path,
        min_constituents_per_day=20,
    )


def test_price_to_sma50_partition_research_builds_zero_base_candidate_tables(
    research_result,
) -> None:
    result = research_result

    assert result.price_feature == PRICE_FEATURE
    assert result.volume_feature == VOLUME_FEATURE
    assert result.analysis_end_date == "2023-11-03"
    assert result.candidate_count == 36
    assert len(result.candidate_definition_df) == 36
    assert result.candidate_definition_df["total_decile_count"].eq(10).all()
    assert set(result.decile_threshold_summary_df["feature_decile"]) == {
        f"Q{index}" for index in range(1, 11)
    }
    assert set(result.candidate_price_group_summary_df["price_group"]) == {
        "high",
        "middle",
        "low",
    }
    assert {
        "high_vs_middle",
        "low_vs_middle",
        "high_vs_low",
    } == set(result.candidate_price_hypothesis_df["hypothesis_key"])
    assert {
        "low_volume_low_vs_low_volume_high",
        "low_volume_low_vs_middle_volume_low",
        "low_volume_low_vs_middle_volume_high",
    } == set(result.candidate_low_volume_hypothesis_df["hypothesis_key"])


def test_overall_scorecard_exposes_price_and_volume_lenses_for_each_candidate(
    research_result,
) -> None:
    result = research_result

    scoped = result.candidate_overall_scorecard_df[
        (result.candidate_overall_scorecard_df["horizon_key"] == "t_plus_10")
        & (result.candidate_overall_scorecard_df["metric_key"] == "future_return")
    ].copy()

    assert len(scoped) == result.candidate_count
    assert "abs_extreme_vs_middle_mean_difference" in scoped.columns
    assert "price_wilcoxon_significant_pair_count" in scoped.columns
    assert "low_volume_low_vs_middle_volume_low" in scoped.columns
    assert "volume_wilcoxon_significant_pair_count" in scoped.columns
    assert scoped["min_mean_group_size"].notna().all()

    strongest = scoped.sort_values(
        [
            "price_wilcoxon_significant_pair_count",
            "abs_extreme_vs_middle_mean_difference",
            "min_mean_group_size",
        ],
        ascending=[False, False, False],
    ).iloc[0]
    assert strongest["candidate_label"]
    assert strongest["high_deciles_label"]
    assert strongest["middle_deciles_label"]
    assert strongest["low_deciles_label"]


def test_price_to_sma50_partition_research_bundle_roundtrip(
    research_result,
    tmp_path: Path,
) -> None:
    result = research_result

    bundle = write_topix100_price_to_sma50_decile_partitions_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260403_120000_testabcd",
    )
    reloaded = load_topix100_price_to_sma50_decile_partitions_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID
    )
    assert bundle.summary_path.exists()
    assert (
        get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_price_to_sma50_decile_partitions_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.candidate_count == result.candidate_count
    assert reloaded.price_feature == result.price_feature
    assert reloaded.volume_feature == result.volume_feature
    pd.testing.assert_frame_equal(
        reloaded.candidate_overall_scorecard_df,
        result.candidate_overall_scorecard_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.candidate_low_volume_hypothesis_df,
        result.candidate_low_volume_hypothesis_df,
        check_dtype=False,
    )
