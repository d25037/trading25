# pyright: reportUnusedFunction=false
"""
Shared definitions for TOPIX SMA-ratio rank / future-close research modules.

These values are internal support for the TOPIX100 / PRIME ex TOPIX500 leaf
research workflow and keep bucket naming, feature naming, and local sort
behavior aligned across split helper modules.
"""

from __future__ import annotations

from typing import Literal, cast

import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import (
    _ordered_feature_values as _core_ordered_feature_values,
    _ranking_feature_label_lookup as _core_ranking_feature_label_lookup,
    _sort_frame as _core_sort_frame,
)
from src.shared.utils.market_code_alias import expand_market_codes

DecileKey = Literal[
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5",
    "Q6",
    "Q7",
    "Q8",
    "Q9",
    "Q10",
]
HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
RankingFeatureKey = Literal[
    "price_sma_5_20",
    "price_sma_20_80",
    "price_sma_50_150",
    "volume_sma_5_20",
    "volume_sma_20_80",
    "volume_sma_50_150",
]
UniverseKey = Literal["topix100", "prime_ex_topix500"]
BucketGroupKey = Literal["q1_q10_extreme", "q4_q5_q6_middle"]
NestedPriceBucketKey = Literal["extreme", "middle"]
NestedVolumeBucketKey = Literal["volume_high", "volume_low"]
NestedCombinedBucketKey = Literal[
    "extreme_volume_high",
    "extreme_volume_low",
    "middle_volume_high",
    "middle_volume_low",
]
Q1Q10PriceBucketKey = Literal["q1", "q10"]
Q1Q10CombinedBucketKey = Literal[
    "q1_volume_high",
    "q1_volume_low",
    "q10_volume_high",
    "q10_volume_low",
]
Q10MiddlePriceBucketKey = Literal["q10", "middle"]
Q10MiddleCombinedBucketKey = Literal[
    "q10_volume_low",
    "q10_volume_high",
    "middle_volume_low",
    "middle_volume_high",
]

DECILE_ORDER: tuple[DecileKey, ...] = (
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5",
    "Q6",
    "Q7",
    "Q8",
    "Q9",
    "Q10",
)
HORIZON_ORDER: tuple[HorizonKey, ...] = ("t_plus_1", "t_plus_5", "t_plus_10")
METRIC_ORDER: tuple[MetricKey, ...] = ("future_close", "future_return")
RANKING_FEATURE_ORDER: tuple[RankingFeatureKey, ...] = (
    "price_sma_5_20",
    "price_sma_20_80",
    "price_sma_50_150",
    "volume_sma_5_20",
    "volume_sma_20_80",
    "volume_sma_50_150",
)
PRICE_FEATURE_ORDER: tuple[RankingFeatureKey, ...] = (
    "price_sma_5_20",
    "price_sma_20_80",
    "price_sma_50_150",
)
VOLUME_FEATURE_ORDER: tuple[RankingFeatureKey, ...] = (
    "volume_sma_5_20",
    "volume_sma_20_80",
    "volume_sma_50_150",
)
COMPOSITE_METHOD_ORDER: tuple[str, ...] = ("rank_mean", "rank_product")
DISCOVERY_END_DATE = "2021-12-31"
VALIDATION_START_DATE = "2022-01-01"

TOPIX100_SCALE_CATEGORIES: tuple[str, ...] = ("TOPIX Core30", "TOPIX Large70")
TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
PRIME_MARKET_CODES: tuple[str, ...] = tuple(expand_market_codes(["prime"]))
UNIVERSE_LABEL_MAP: dict[UniverseKey, str] = {
    "topix100": "TOPIX100",
    "prime_ex_topix500": "PRIME ex TOPIX500",
}
PRICE_SMA_WINDOWS: tuple[tuple[int, int], ...] = ((5, 20), (20, 80), (50, 150))
VOLUME_SMA_WINDOWS: tuple[tuple[int, int], ...] = ((5, 20), (20, 80), (50, 150))
HORIZON_DAY_MAP: dict[HorizonKey, int] = {
    "t_plus_1": 1,
    "t_plus_5": 5,
    "t_plus_10": 10,
}
DECILE_LABEL_MAP: dict[DecileKey, str] = {
    "Q1": "Q1 Highest Ratio",
    "Q2": "Q2",
    "Q3": "Q3",
    "Q4": "Q4",
    "Q5": "Q5",
    "Q6": "Q6",
    "Q7": "Q7",
    "Q8": "Q8",
    "Q9": "Q9",
    "Q10": "Q10 Lowest Ratio",
}
RANKING_FEATURE_LABEL_MAP: dict[RankingFeatureKey, str] = {
    "price_sma_5_20": "Price SMA 5 / 20",
    "price_sma_20_80": "Price SMA 20 / 80",
    "price_sma_50_150": "Price SMA 50 / 150",
    "volume_sma_5_20": "Volume SMA 5 / 20",
    "volume_sma_20_80": "Volume SMA 20 / 80",
    "volume_sma_50_150": "Volume SMA 50 / 150",
}
BUCKET_GROUP_ORDER: tuple[BucketGroupKey, ...] = ("q1_q10_extreme", "q4_q5_q6_middle")
BUCKET_GROUP_DECILES: dict[BucketGroupKey, tuple[DecileKey, ...]] = {
    "q1_q10_extreme": ("Q1", "Q10"),
    "q4_q5_q6_middle": ("Q4", "Q5", "Q6"),
}
BUCKET_GROUP_LABEL_MAP: dict[BucketGroupKey, str] = {
    "q1_q10_extreme": "Q1 + Q10",
    "q4_q5_q6_middle": "Q4 + Q5 + Q6",
}
NESTED_PRICE_BUCKET_ORDER: tuple[NestedPriceBucketKey, ...] = ("extreme", "middle")
NESTED_PRICE_BUCKET_LABEL_MAP: dict[NestedPriceBucketKey, str] = {
    "extreme": "Q1 + Q10",
    "middle": "Q4 + Q5 + Q6",
}
NESTED_PRICE_BUCKET_DECILES: dict[NestedPriceBucketKey, tuple[DecileKey, ...]] = {
    "extreme": ("Q1", "Q10"),
    "middle": ("Q4", "Q5", "Q6"),
}
NESTED_VOLUME_BUCKET_ORDER: tuple[NestedVolumeBucketKey, ...] = (
    "volume_high",
    "volume_low",
)
NESTED_VOLUME_BUCKET_LABEL_MAP: dict[NestedVolumeBucketKey, str] = {
    "volume_high": "Volume 20 / 80 High Half",
    "volume_low": "Volume 20 / 80 Low Half",
}
NESTED_COMBINED_BUCKET_ORDER: tuple[NestedCombinedBucketKey, ...] = (
    "extreme_volume_high",
    "extreme_volume_low",
    "middle_volume_high",
    "middle_volume_low",
)
NESTED_COMBINED_BUCKET_LABEL_MAP: dict[NestedCombinedBucketKey, str] = {
    "extreme_volume_high": "Extreme x Volume High",
    "extreme_volume_low": "Extreme x Volume Low",
    "middle_volume_high": "Middle x Volume High",
    "middle_volume_low": "Middle x Volume Low",
}
Q1_Q10_PRICE_BUCKET_ORDER: tuple[Q1Q10PriceBucketKey, ...] = ("q1", "q10")
Q1_Q10_PRICE_BUCKET_LABEL_MAP: dict[Q1Q10PriceBucketKey, str] = {
    "q1": "Q1",
    "q10": "Q10",
}
Q1_Q10_PRICE_BUCKET_DECILES: dict[Q1Q10PriceBucketKey, tuple[DecileKey, ...]] = {
    "q1": ("Q1",),
    "q10": ("Q10",),
}
Q1_Q10_COMBINED_BUCKET_ORDER: tuple[Q1Q10CombinedBucketKey, ...] = (
    "q1_volume_high",
    "q1_volume_low",
    "q10_volume_high",
    "q10_volume_low",
)
Q1_Q10_COMBINED_BUCKET_LABEL_MAP: dict[Q1Q10CombinedBucketKey, str] = {
    "q1_volume_high": "Q1 x Volume High",
    "q1_volume_low": "Q1 x Volume Low",
    "q10_volume_high": "Q10 x Volume High",
    "q10_volume_low": "Q10 x Volume Low",
}
Q10_MIDDLE_PRICE_BUCKET_ORDER: tuple[Q10MiddlePriceBucketKey, ...] = (
    "q10",
    "middle",
)
Q10_MIDDLE_PRICE_BUCKET_LABEL_MAP: dict[Q10MiddlePriceBucketKey, str] = {
    "q10": "Q10",
    "middle": "Q4 + Q5 + Q6",
}
Q10_MIDDLE_PRICE_BUCKET_DECILES: dict[Q10MiddlePriceBucketKey, tuple[DecileKey, ...]] = {
    "q10": ("Q10",),
    "middle": ("Q4", "Q5", "Q6"),
}
Q10_MIDDLE_COMBINED_BUCKET_ORDER: tuple[Q10MiddleCombinedBucketKey, ...] = (
    "q10_volume_low",
    "q10_volume_high",
    "middle_volume_low",
    "middle_volume_high",
)
Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP: dict[Q10MiddleCombinedBucketKey, str] = {
    "q10_volume_low": "Q10 x Volume Low",
    "q10_volume_high": "Q10 x Volume High",
    "middle_volume_low": "Middle x Volume Low",
    "middle_volume_high": "Middle x Volume High",
}
PRIMARY_PRICE_FEATURE: RankingFeatureKey = "price_sma_20_80"
PRIMARY_VOLUME_FEATURE: RankingFeatureKey = "volume_sma_20_80"
DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY = 80
DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY = 400


def _ordered_feature_values(values: list[str] | pd.Series) -> list[str]:
    return _core_ordered_feature_values(
        values,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = _core_sort_frame(df, known_feature_order=RANKING_FEATURE_ORDER)
    if "bucket_group" in sorted_df.columns:
        sorted_df["_bucket_group_order"] = sorted_df["bucket_group"].map(
            {key: index for index, key in enumerate(BUCKET_GROUP_ORDER, start=1)}
        )
    if "nested_price_bucket" in sorted_df.columns:
        sorted_df["_nested_price_bucket_order"] = sorted_df["nested_price_bucket"].map(
            {key: index for index, key in enumerate(NESTED_PRICE_BUCKET_ORDER, start=1)}
        )
    if "nested_volume_bucket" in sorted_df.columns:
        sorted_df["_nested_volume_bucket_order"] = sorted_df[
            "nested_volume_bucket"
        ].map(
            {key: index for index, key in enumerate(NESTED_VOLUME_BUCKET_ORDER, start=1)}
        )
    if "nested_combined_bucket" in sorted_df.columns:
        sorted_df["_nested_combined_bucket_order"] = sorted_df[
            "nested_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(NESTED_COMBINED_BUCKET_ORDER, start=1)
            }
        )
    if "q1_q10_price_bucket" in sorted_df.columns:
        sorted_df["_q1_q10_price_bucket_order"] = sorted_df["q1_q10_price_bucket"].map(
            {key: index for index, key in enumerate(Q1_Q10_PRICE_BUCKET_ORDER, start=1)}
        )
    if "q1_q10_combined_bucket" in sorted_df.columns:
        sorted_df["_q1_q10_combined_bucket_order"] = sorted_df[
            "q1_q10_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(Q1_Q10_COMBINED_BUCKET_ORDER, start=1)
            }
        )
    if "q10_middle_price_bucket" in sorted_df.columns:
        sorted_df["_q10_middle_price_bucket_order"] = sorted_df[
            "q10_middle_price_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(Q10_MIDDLE_PRICE_BUCKET_ORDER, start=1)
            }
        )
    if "q10_middle_combined_bucket" in sorted_df.columns:
        sorted_df["_q10_middle_combined_bucket_order"] = sorted_df[
            "q10_middle_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(Q10_MIDDLE_COMBINED_BUCKET_ORDER, start=1)
            }
        )

    sort_columns = [
        column
        for column in [
            "_bucket_group_order",
            "_nested_price_bucket_order",
            "_nested_volume_bucket_order",
            "_nested_combined_bucket_order",
            "_q1_q10_price_bucket_order",
            "_q1_q10_combined_bucket_order",
            "_q10_middle_price_bucket_order",
            "_q10_middle_combined_bucket_order",
            "date",
            "metric_key",
            "left_decile",
            "right_decile",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns).reset_index(drop=True)

    return sorted_df.drop(
        columns=[
            column
            for column in [
                "_bucket_group_order",
                "_nested_price_bucket_order",
                "_nested_volume_bucket_order",
                "_nested_combined_bucket_order",
                "_q1_q10_price_bucket_order",
                "_q1_q10_combined_bucket_order",
                "_q10_middle_price_bucket_order",
                "_q10_middle_combined_bucket_order",
            ]
            if column in sorted_df.columns
        ]
    )


def _ranking_feature_label_lookup(df: pd.DataFrame) -> dict[str, str]:
    label_lookup = _core_ranking_feature_label_lookup(df)
    for feature in _ordered_feature_values(
        df["ranking_feature"] if "ranking_feature" in df.columns else []
    ):
        feature_key = cast(RankingFeatureKey, feature)
        label_lookup.setdefault(feature, RANKING_FEATURE_LABEL_MAP.get(feature_key, feature))
    return label_lookup
