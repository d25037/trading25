from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest

from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
    TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    _aligned_q10_middle_combined_pivot,
    _build_q10_low_hypothesis,
    _build_q10_low_scorecard,
    _build_q10_low_spread_daily,
    _build_q10_middle_volume_pairwise_significance,
    _filter_q10_middle_volume_daily_means,
    _filter_q10_middle_volume_split_panel,
    get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id,
    get_topix100_price_vs_sma_q10_bounce_latest_bundle_path,
    load_topix100_price_vs_sma_q10_bounce_research_bundle,
    _normalize_price_features,
    _normalize_volume_features,
    _summarize_q10_middle_volume_split,
    run_topix100_price_vs_sma_q10_bounce_research,
    write_topix100_price_vs_sma_q10_bounce_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRIMARY_VOLUME_FEATURE,
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
    assert set(result.q10_middle_volume_split_summary_df["volume_feature"]) == {
        PRIMARY_VOLUME_FEATURE
    }
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


def test_q10_bounce_can_target_volume_sma_5_20(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
        price_features=["price_vs_sma_50_gap"],
        volume_features=["volume_sma_5_20"],
    )

    assert result.price_feature_order == ("price_vs_sma_50_gap",)
    assert result.volume_feature_order == ("volume_sma_5_20",)
    assert set(result.q10_middle_volume_split_summary_df["volume_feature"]) == {
        "volume_sma_5_20"
    }
    assert set(result.q10_low_scorecard_df["volume_feature"]) == {"volume_sma_5_20"}


def test_q10_bounce_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_price_vs_sma_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
        price_features=["price_vs_sma_50_gap"],
        volume_features=["volume_sma_5_20"],
    )

    bundle = write_topix100_price_vs_sma_q10_bounce_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_173000_testabcd",
    )
    reloaded = load_topix100_price_vs_sma_q10_bounce_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_price_vs_sma_q10_bounce_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.price_feature_order == result.price_feature_order
    assert reloaded.volume_feature_order == result.volume_feature_order
    assert reloaded.base_result.analysis_start_date == result.base_result.analysis_start_date
    pd.testing.assert_frame_equal(
        reloaded.q10_low_hypothesis_df,
        result.q10_low_hypothesis_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.q10_low_scorecard_df,
        result.q10_low_scorecard_df,
        check_dtype=False,
    )


@pytest.mark.parametrize(
    ("func", "value", "message"),
    [
        (_normalize_price_features, ["price_vs_sma_999_gap"], "Unsupported price_features"),
        (_normalize_price_features, [], "price_features must include at least one feature"),
        (_normalize_volume_features, ["volume_sma_1_999"], "Unsupported volume_features"),
        (_normalize_volume_features, [], "volume_features must include at least one feature"),
    ],
)
def test_q10_bounce_rejects_invalid_feature_filters(
    func: object,
    value: list[str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        func(value)  # type: ignore[misc]


def test_q10_bounce_helpers_return_empty_frames_for_empty_inputs() -> None:
    empty_base_result = cast(
        Any,
        SimpleNamespace(
            price_volume_split_panel_df=pd.DataFrame(),
            price_volume_split_daily_means_df=pd.DataFrame(),
        ),
    )
    empty_daily_means = pd.DataFrame(
        columns=[
            "price_feature",
            "volume_feature",
            "horizon_key",
            "combined_bucket",
            "group_mean_future_close",
            "group_mean_future_return",
            "date",
        ]
    )

    empty_panel = _filter_q10_middle_volume_split_panel(
        empty_base_result,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    )
    empty_daily_means_from_base = _filter_q10_middle_volume_daily_means(
        empty_base_result,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    )

    assert empty_panel.empty
    assert empty_daily_means_from_base.empty
    assert _summarize_q10_middle_volume_split(
        empty_daily_means_from_base,
        price_feature_order=("price_vs_sma_50_gap",),
    ).empty
    assert _aligned_q10_middle_combined_pivot(
        empty_daily_means,
        price_feature="price_vs_sma_50_gap",
        volume_feature="volume_sma_5_20",
        horizon_key="t_plus_10",
        value_column="group_mean_future_return",
    ).empty
    assert _build_q10_middle_volume_pairwise_significance(
        empty_daily_means,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    ).empty
    assert _build_q10_low_hypothesis(
        pd.DataFrame(),
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    ).empty
    assert _build_q10_low_spread_daily(
        empty_daily_means,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    ).empty
    assert _build_q10_low_scorecard(
        pd.DataFrame(),
        pd.DataFrame(),
        price_feature_order=("price_vs_sma_50_gap",),
    ).empty


def test_q10_low_hypothesis_flips_reversed_pair_and_keeps_missing_pairs_empty() -> None:
    pairwise_df = pd.DataFrame.from_records(
        [
            {
                "price_feature": "price_vs_sma_50_gap",
                "price_feature_label": "Price vs SMA 50 Gap",
                "volume_feature": "volume_sma_5_20",
                "volume_feature_label": "Volume SMA 5 / 20",
                "horizon_key": "t_plus_10",
                "metric_key": "future_return",
                "left_combined_bucket": "middle_volume_high",
                "right_combined_bucket": "q10_volume_low",
                "mean_difference": -0.42,
                "paired_t_p_value_holm": 0.03,
                "wilcoxon_p_value_holm": 0.04,
            }
        ]
    )

    hypothesis_df = _build_q10_low_hypothesis(
        pairwise_df,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    )

    flipped = hypothesis_df.loc[
        (hypothesis_df["horizon_key"] == "t_plus_10")
        & (hypothesis_df["metric_key"] == "future_return")
        & (hypothesis_df["hypothesis_label"] == "Q10 Low vs Middle High")
    ].iloc[0]
    assert flipped["mean_difference"] == pytest.approx(0.42)
    assert flipped["paired_t_p_value_holm"] == pytest.approx(0.03)
    assert flipped["wilcoxon_p_value_holm"] == pytest.approx(0.04)

    missing = hypothesis_df.loc[
        (hypothesis_df["horizon_key"] == "t_plus_10")
        & (hypothesis_df["metric_key"] == "future_return")
        & (hypothesis_df["hypothesis_label"] == "Q10 Low vs Middle Low")
    ].iloc[0]
    assert pd.isna(missing["mean_difference"])
    assert pd.isna(missing["paired_t_p_value_holm"])
    assert pd.isna(missing["wilcoxon_p_value_holm"])


def test_q10_bounce_pairwise_and_spread_handle_incomplete_bucket_panels() -> None:
    daily_means_df = pd.DataFrame.from_records(
        [
            {
                "price_feature": "price_vs_sma_50_gap",
                "price_feature_label": "Price vs SMA 50 Gap",
                "volume_feature": "volume_sma_5_20",
                "volume_feature_label": "Volume SMA 5 / 20",
                "horizon_key": "t_plus_10",
                "horizon_days": 10,
                "combined_bucket": "middle_volume_high",
                "combined_bucket_label": "Middle x Volume High",
                "group_mean_future_close": 101.0,
                "group_mean_future_return": 0.01,
                "date": "2023-11-01",
            },
            {
                "price_feature": "price_vs_sma_50_gap",
                "price_feature_label": "Price vs SMA 50 Gap",
                "volume_feature": "volume_sma_5_20",
                "volume_feature_label": "Volume SMA 5 / 20",
                "horizon_key": "t_plus_10",
                "horizon_days": 10,
                "combined_bucket": "middle_volume_low",
                "combined_bucket_label": "Middle x Volume Low",
                "group_mean_future_close": 100.5,
                "group_mean_future_return": 0.02,
                "date": "2023-11-01",
            },
        ]
    )

    pairwise_df = _build_q10_middle_volume_pairwise_significance(
        daily_means_df,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    )
    spread_daily_df = _build_q10_low_spread_daily(
        daily_means_df,
        price_feature_order=("price_vs_sma_50_gap",),
        volume_feature_order=("volume_sma_5_20",),
    )

    target = pairwise_df.loc[
        (pairwise_df["price_feature"] == "price_vs_sma_50_gap")
        & (pairwise_df["volume_feature"] == "volume_sma_5_20")
        & (pairwise_df["horizon_key"] == "t_plus_10")
        & (pairwise_df["metric_key"] == "future_return")
        & (pairwise_df["left_combined_bucket"] == "middle_volume_high")
        & (pairwise_df["right_combined_bucket"] == "middle_volume_low")
    ].iloc[0]
    assert target["n_dates"] == 0
    assert pd.isna(target["mean_difference"])
    assert pd.isna(target["paired_t_p_value"])
    assert spread_daily_df.empty
