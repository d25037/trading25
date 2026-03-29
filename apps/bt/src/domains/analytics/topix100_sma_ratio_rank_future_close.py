"""
TOPIX100 SMA-ratio rank / future close research analytics.

The market.duckdb file is the source of truth. This module reads the latest
TOPIX100 membership from `stocks`, aligns adjusted `stock_data`, derives
price/volume SMA ratio features, ranks each feature independently within the
daily TOPIX100 universe, and tests whether future closes differ across the
resulting deciles.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any, Literal

import numpy as np
import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import (
    _assign_feature_deciles as _core_assign_feature_deciles,
    _build_daily_group_means as _core_build_daily_group_means,
    _build_global_significance as _core_build_global_significance,
    _build_horizon_panel as _core_build_horizon_panel,
    _build_pairwise_significance as _core_build_pairwise_significance,
    _default_start_date as _core_default_start_date,
    _holm_adjust as _core_holm_adjust,
    _kendalls_w as _core_kendalls_w,
    _ordered_feature_values as _core_ordered_feature_values,
    _query_universe_date_range as _core_query_universe_date_range,
    _query_universe_stock_history as _core_query_universe_stock_history,
    _ranking_feature_label_lookup as _core_ranking_feature_label_lookup,
    _rolling_mean as _core_rolling_mean,
    _safe_friedman as _core_safe_friedman,
    _safe_kruskal as _core_safe_kruskal,
    _safe_paired_t_test as _core_safe_paired_t_test,
    _safe_ratio as _core_safe_ratio,
    _safe_wilcoxon as _core_safe_wilcoxon,
    _sort_frame as _base_sort_frame,
    _summarize_future_targets as _core_summarize_future_targets,
    _summarize_ranking_features as _core_summarize_ranking_features,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)

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
_DISCOVERY_END_DATE = "2021-12-31"
_VALIDATION_START_DATE = "2022-01-01"

_TOPIX100_SCALE_CATEGORIES: tuple[str, ...] = ("TOPIX Core30", "TOPIX Large70")
_TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
_PRIME_MARKET_CODES: tuple[str, ...] = ("0111", "prime")
_UNIVERSE_LABEL_MAP: dict[UniverseKey, str] = {
    "topix100": "TOPIX100",
    "prime_ex_topix500": "PRIME ex TOPIX500",
}
_PRICE_SMA_WINDOWS: tuple[tuple[int, int], ...] = ((5, 20), (20, 80), (50, 150))
_VOLUME_SMA_WINDOWS: tuple[tuple[int, int], ...] = ((5, 20), (20, 80), (50, 150))
_HORIZON_DAY_MAP: dict[HorizonKey, int] = {
    "t_plus_1": 1,
    "t_plus_5": 5,
    "t_plus_10": 10,
}
_DECILE_LABEL_MAP: dict[DecileKey, str] = {
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
_RANKING_FEATURE_LABEL_MAP: dict[RankingFeatureKey, str] = {
    "price_sma_5_20": "Price SMA 5 / 20",
    "price_sma_20_80": "Price SMA 20 / 80",
    "price_sma_50_150": "Price SMA 50 / 150",
    "volume_sma_5_20": "Volume SMA 5 / 20",
    "volume_sma_20_80": "Volume SMA 20 / 80",
    "volume_sma_50_150": "Volume SMA 50 / 150",
}
BUCKET_GROUP_ORDER: tuple[BucketGroupKey, ...] = ("q1_q10_extreme", "q4_q5_q6_middle")
_BUCKET_GROUP_DECILES: dict[BucketGroupKey, tuple[DecileKey, ...]] = {
    "q1_q10_extreme": ("Q1", "Q10"),
    "q4_q5_q6_middle": ("Q4", "Q5", "Q6"),
}
_BUCKET_GROUP_LABEL_MAP: dict[BucketGroupKey, str] = {
    "q1_q10_extreme": "Q1 + Q10",
    "q4_q5_q6_middle": "Q4 + Q5 + Q6",
}
_NESTED_PRICE_BUCKET_ORDER: tuple[NestedPriceBucketKey, ...] = ("extreme", "middle")
_NESTED_PRICE_BUCKET_LABEL_MAP: dict[NestedPriceBucketKey, str] = {
    "extreme": "Q1 + Q10",
    "middle": "Q4 + Q5 + Q6",
}
_NESTED_PRICE_BUCKET_DECILES: dict[NestedPriceBucketKey, tuple[DecileKey, ...]] = {
    "extreme": ("Q1", "Q10"),
    "middle": ("Q4", "Q5", "Q6"),
}
_NESTED_VOLUME_BUCKET_ORDER: tuple[NestedVolumeBucketKey, ...] = (
    "volume_high",
    "volume_low",
)
_NESTED_VOLUME_BUCKET_LABEL_MAP: dict[NestedVolumeBucketKey, str] = {
    "volume_high": "Volume 20 / 80 High Half",
    "volume_low": "Volume 20 / 80 Low Half",
}
_NESTED_COMBINED_BUCKET_ORDER: tuple[NestedCombinedBucketKey, ...] = (
    "extreme_volume_high",
    "extreme_volume_low",
    "middle_volume_high",
    "middle_volume_low",
)
_NESTED_COMBINED_BUCKET_LABEL_MAP: dict[NestedCombinedBucketKey, str] = {
    "extreme_volume_high": "Extreme x Volume High",
    "extreme_volume_low": "Extreme x Volume Low",
    "middle_volume_high": "Middle x Volume High",
    "middle_volume_low": "Middle x Volume Low",
}
_Q1_Q10_PRICE_BUCKET_ORDER: tuple[Q1Q10PriceBucketKey, ...] = ("q1", "q10")
_Q1_Q10_PRICE_BUCKET_LABEL_MAP: dict[Q1Q10PriceBucketKey, str] = {
    "q1": "Q1",
    "q10": "Q10",
}
_Q1_Q10_PRICE_BUCKET_DECILES: dict[Q1Q10PriceBucketKey, tuple[DecileKey, ...]] = {
    "q1": ("Q1",),
    "q10": ("Q10",),
}
_Q1_Q10_COMBINED_BUCKET_ORDER: tuple[Q1Q10CombinedBucketKey, ...] = (
    "q1_volume_high",
    "q1_volume_low",
    "q10_volume_high",
    "q10_volume_low",
)
_Q1_Q10_COMBINED_BUCKET_LABEL_MAP: dict[Q1Q10CombinedBucketKey, str] = {
    "q1_volume_high": "Q1 x Volume High",
    "q1_volume_low": "Q1 x Volume Low",
    "q10_volume_high": "Q10 x Volume High",
    "q10_volume_low": "Q10 x Volume Low",
}
_Q10_MIDDLE_PRICE_BUCKET_ORDER: tuple[Q10MiddlePriceBucketKey, ...] = (
    "q10",
    "middle",
)
_Q10_MIDDLE_PRICE_BUCKET_LABEL_MAP: dict[Q10MiddlePriceBucketKey, str] = {
    "q10": "Q10",
    "middle": "Q4 + Q5 + Q6",
}
_Q10_MIDDLE_PRICE_BUCKET_DECILES: dict[Q10MiddlePriceBucketKey, tuple[DecileKey, ...]] = {
    "q10": ("Q10",),
    "middle": ("Q4", "Q5", "Q6"),
}
_Q10_MIDDLE_COMBINED_BUCKET_ORDER: tuple[Q10MiddleCombinedBucketKey, ...] = (
    "q10_volume_low",
    "q10_volume_high",
    "middle_volume_low",
    "middle_volume_high",
)
_Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP: dict[Q10MiddleCombinedBucketKey, str] = {
    "q10_volume_low": "Q10 x Volume Low",
    "q10_volume_high": "Q10 x Volume High",
    "middle_volume_low": "Middle x Volume Low",
    "middle_volume_high": "Middle x Volume High",
}
_PRIMARY_PRICE_FEATURE: RankingFeatureKey = "price_sma_20_80"
_PRIMARY_VOLUME_FEATURE: RankingFeatureKey = "volume_sma_20_80"
_DEFAULT_LOOKBACK_YEARS = 10
_DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY = 80
_DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY = 400


@dataclass(frozen=True)
class Topix100SmaRatioRankFutureCloseResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    universe_key: UniverseKey
    universe_label: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    min_constituents_per_day: int
    universe_constituent_count: int
    topix100_constituent_count: int
    stock_day_count: int
    ranked_event_count: int
    valid_date_count: int
    discovery_end_date: str
    validation_start_date: str
    event_panel_df: pd.DataFrame
    ranked_panel_df: pd.DataFrame
    ranking_feature_summary_df: pd.DataFrame
    decile_future_summary_df: pd.DataFrame
    daily_group_means_df: pd.DataFrame
    global_significance_df: pd.DataFrame
    pairwise_significance_df: pd.DataFrame
    extreme_vs_middle_summary_df: pd.DataFrame
    extreme_vs_middle_daily_means_df: pd.DataFrame
    extreme_vs_middle_significance_df: pd.DataFrame
    nested_volume_split_panel_df: pd.DataFrame
    nested_volume_split_summary_df: pd.DataFrame
    nested_volume_split_daily_means_df: pd.DataFrame
    nested_volume_split_global_significance_df: pd.DataFrame
    nested_volume_split_pairwise_significance_df: pd.DataFrame
    nested_volume_split_interaction_df: pd.DataFrame
    q1_q10_volume_split_panel_df: pd.DataFrame
    q1_q10_volume_split_summary_df: pd.DataFrame
    q1_q10_volume_split_daily_means_df: pd.DataFrame
    q1_q10_volume_split_global_significance_df: pd.DataFrame
    q1_q10_volume_split_pairwise_significance_df: pd.DataFrame
    q1_q10_volume_split_interaction_df: pd.DataFrame
    q10_middle_volume_split_panel_df: pd.DataFrame
    q10_middle_volume_split_summary_df: pd.DataFrame
    q10_middle_volume_split_daily_means_df: pd.DataFrame
    q10_middle_volume_split_pairwise_significance_df: pd.DataFrame
    q10_low_hypothesis_df: pd.DataFrame
    feature_selection_df: pd.DataFrame
    selected_feature_df: pd.DataFrame
    composite_candidate_df: pd.DataFrame
    selected_composite_df: pd.DataFrame
    selected_composite_ranking_summary_df: pd.DataFrame
    selected_composite_future_summary_df: pd.DataFrame
    selected_composite_daily_group_means_df: pd.DataFrame
    selected_composite_global_significance_df: pd.DataFrame
    selected_composite_pairwise_significance_df: pd.DataFrame


def _universe_membership_sql_and_params(
    universe_key: UniverseKey,
) -> tuple[str, list[str]]:
    if universe_key == "topix100":
        return (
            "scale_category IN (?, ?)",
            list(_TOPIX100_SCALE_CATEGORIES),
        )
    if universe_key == "prime_ex_topix500":
        return (
            "market_code IN (?, ?) AND scale_category NOT IN (?, ?, ?)",
            [*_PRIME_MARKET_CODES, *_TOPIX500_SCALE_CATEGORIES],
        )
    raise ValueError(f"Unsupported universe_key: {universe_key}")


def _query_universe_stock_history(
    conn: Any,
    *,
    universe_key: UniverseKey,
    end_date: str | None,
) -> pd.DataFrame:
    return _core_query_universe_stock_history(
        conn,
        universe_key=universe_key,
        end_date=end_date,
    )


def _query_universe_date_range(
    conn: Any,
    *,
    universe_key: UniverseKey,
) -> tuple[str | None, str | None]:
    return _core_query_universe_date_range(
        conn,
        universe_key=universe_key,
    )


def _query_topix100_stock_history(
    conn: Any,
    *,
    end_date: str | None,
) -> pd.DataFrame:
    return _query_universe_stock_history(
        conn,
        universe_key="topix100",
        end_date=end_date,
    )


def _query_topix100_date_range(conn: Any) -> tuple[str | None, str | None]:
    return _query_universe_date_range(conn, universe_key="topix100")


def _default_start_date(
    *,
    available_start_date: str | None,
    available_end_date: str | None,
    lookback_years: int,
) -> str | None:
    return _core_default_start_date(
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        lookback_years=lookback_years,
    )


def _rolling_mean(
    df: pd.DataFrame,
    *,
    column_name: str,
    window: int,
) -> pd.Series:
    return _core_rolling_mean(
        df,
        column_name=column_name,
        window=window,
    )


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _core_safe_ratio(numerator, denominator)


def _ordered_feature_values(values: list[str] | pd.Series) -> list[str]:
    return _core_ordered_feature_values(
        values,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _enrich_event_panel(
    history_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    min_constituents_per_day: int,
) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame()

    panel = history_df.copy()
    panel["date"] = panel["date"].astype(str)
    panel = panel.sort_values(["code", "date"]).reset_index(drop=True)

    close_sma_cache: dict[int, pd.Series] = {}
    volume_sma_cache: dict[int, pd.Series] = {}

    for short_window, long_window in _PRICE_SMA_WINDOWS:
        close_sma_cache[short_window] = close_sma_cache.get(
            short_window,
            _rolling_mean(panel, column_name="close", window=short_window),
        )
        close_sma_cache[long_window] = close_sma_cache.get(
            long_window,
            _rolling_mean(panel, column_name="close", window=long_window),
        )
        panel[f"price_sma_{short_window}_{long_window}"] = _safe_ratio(
            close_sma_cache[short_window],
            close_sma_cache[long_window],
        )

    for short_window, long_window in _VOLUME_SMA_WINDOWS:
        volume_sma_cache[short_window] = volume_sma_cache.get(
            short_window,
            _rolling_mean(panel, column_name="volume", window=short_window),
        )
        volume_sma_cache[long_window] = volume_sma_cache.get(
            long_window,
            _rolling_mean(panel, column_name="volume", window=long_window),
        )
        panel[f"volume_sma_{short_window}_{long_window}"] = _safe_ratio(
            volume_sma_cache[short_window],
            volume_sma_cache[long_window],
        )

    for horizon_key, horizon_days in _HORIZON_DAY_MAP.items():
        future_close = (
            panel.groupby("code", sort=False)["close"].shift(-horizon_days).astype(float)
        )
        panel[f"{horizon_key}_close"] = future_close
        panel[f"{horizon_key}_return"] = _safe_ratio(future_close, panel["close"]) - 1.0

    required_mask = panel["close"].gt(0) & panel[list(RANKING_FEATURE_ORDER)].notna().all(
        axis=1
    )
    if analysis_start_date is not None:
        required_mask &= panel["date"] >= analysis_start_date
    if analysis_end_date is not None:
        required_mask &= panel["date"] <= analysis_end_date
    panel = panel.loc[required_mask].copy()
    if panel.empty:
        return panel

    panel["date_constituent_count"] = panel.groupby("date")["code"].transform("size")
    panel = panel.loc[panel["date_constituent_count"] >= min_constituents_per_day].copy()
    if panel.empty:
        return panel

    return panel.reset_index(drop=True)


def _assign_feature_deciles(ranked_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _core_assign_feature_deciles(
        ranked_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _build_ranked_panel(event_panel_df: pd.DataFrame) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        *[f"{horizon_key}_close" for horizon_key in HORIZON_ORDER],
        *[f"{horizon_key}_return" for horizon_key in HORIZON_ORDER],
    ]
    ranked_panel_df = event_panel_df.melt(
        id_vars=base_columns,
        value_vars=list(RANKING_FEATURE_ORDER),
        var_name="ranking_feature",
        value_name="ranking_value",
    )
    ranked_panel_df["ranking_feature_label"] = ranked_panel_df["ranking_feature"].map(
        _RANKING_FEATURE_LABEL_MAP
    )
    return _assign_feature_deciles(ranked_panel_df)


def _oriented_rank_score(
    event_panel_df: pd.DataFrame,
    *,
    feature_name: str,
    direction: str,
) -> pd.Series:
    rank_pct = (
        event_panel_df.groupby("date")[feature_name]
        .rank(method="first", pct=True, ascending=True)
        .astype(float)
    )
    min_score = 1.0 / event_panel_df["date_constituent_count"].astype(float)
    if direction == "high":
        return rank_pct
    return (1.0 - rank_pct + min_score).clip(lower=min_score, upper=1.0)


def _build_composite_feature_name(
    *,
    price_feature: str,
    price_direction: str,
    volume_feature: str,
    volume_direction: str,
    score_method: str,
) -> str:
    return (
        f"composite::{price_feature}:{price_direction}"
        f"__{volume_feature}:{volume_direction}"
        f"__{score_method}"
    )


def _build_composite_feature_label(
    *,
    price_feature: str,
    price_direction: str,
    volume_feature: str,
    volume_direction: str,
    score_method: str,
) -> str:
    method_label = "Rank Mean" if score_method == "rank_mean" else "Rank Product"
    return (
        f"{method_label} | "
        f"{_RANKING_FEATURE_LABEL_MAP.get(price_feature, price_feature)} ({price_direction}) + "
        f"{_RANKING_FEATURE_LABEL_MAP.get(volume_feature, volume_feature)} ({volume_direction})"
    )


def _build_composite_ranked_panel(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: str,
    price_direction: str,
    volume_feature: str,
    volume_direction: str,
    score_method: str,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    price_score = _oriented_rank_score(
        event_panel_df,
        feature_name=price_feature,
        direction=price_direction,
    )
    volume_score = _oriented_rank_score(
        event_panel_df,
        feature_name=volume_feature,
        direction=volume_direction,
    )
    if score_method == "rank_mean":
        ranking_value = (price_score + volume_score) / 2.0
    else:
        ranking_value = np.sqrt(price_score * volume_score)

    composite_name = _build_composite_feature_name(
        price_feature=price_feature,
        price_direction=price_direction,
        volume_feature=volume_feature,
        volume_direction=volume_direction,
        score_method=score_method,
    )
    composite_label = _build_composite_feature_label(
        price_feature=price_feature,
        price_direction=price_direction,
        volume_feature=volume_feature,
        volume_direction=volume_direction,
        score_method=score_method,
    )
    ranked_panel_df = event_panel_df[
        [
            "date",
            "code",
            "company_name",
            "close",
            "volume",
            "date_constituent_count",
            *[f"{horizon_key}_close" for horizon_key in HORIZON_ORDER],
            *[f"{horizon_key}_return" for horizon_key in HORIZON_ORDER],
        ]
    ].copy()
    ranked_panel_df["ranking_feature"] = composite_name
    ranked_panel_df["ranking_feature_label"] = composite_label
    ranked_panel_df["ranking_value"] = ranking_value.astype(float)
    ranked_panel_df["price_feature"] = price_feature
    ranked_panel_df["price_direction"] = price_direction
    ranked_panel_df["volume_feature"] = volume_feature
    ranked_panel_df["volume_direction"] = volume_direction
    ranked_panel_df["score_method"] = score_method
    return _assign_feature_deciles(ranked_panel_df)


def _build_horizon_panel(ranked_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_horizon_panel(
        ranked_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = _base_sort_frame(df, known_feature_order=RANKING_FEATURE_ORDER)
    if "bucket_group" in sorted_df.columns:
        sorted_df["_bucket_group_order"] = sorted_df["bucket_group"].map(
            {key: index for index, key in enumerate(BUCKET_GROUP_ORDER, start=1)}
        )
    if "nested_price_bucket" in sorted_df.columns:
        sorted_df["_nested_price_bucket_order"] = sorted_df["nested_price_bucket"].map(
            {
                key: index
                for index, key in enumerate(_NESTED_PRICE_BUCKET_ORDER, start=1)
            }
        )
    if "nested_volume_bucket" in sorted_df.columns:
        sorted_df["_nested_volume_bucket_order"] = sorted_df[
            "nested_volume_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(_NESTED_VOLUME_BUCKET_ORDER, start=1)
            }
        )
    if "nested_combined_bucket" in sorted_df.columns:
        sorted_df["_nested_combined_bucket_order"] = sorted_df[
            "nested_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(_NESTED_COMBINED_BUCKET_ORDER, start=1)
            }
        )
    if "q1_q10_price_bucket" in sorted_df.columns:
        sorted_df["_q1_q10_price_bucket_order"] = sorted_df["q1_q10_price_bucket"].map(
            {
                key: index
                for index, key in enumerate(_Q1_Q10_PRICE_BUCKET_ORDER, start=1)
            }
        )
    if "q1_q10_combined_bucket" in sorted_df.columns:
        sorted_df["_q1_q10_combined_bucket_order"] = sorted_df[
            "q1_q10_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(_Q1_Q10_COMBINED_BUCKET_ORDER, start=1)
            }
        )
    if "q10_middle_price_bucket" in sorted_df.columns:
        sorted_df["_q10_middle_price_bucket_order"] = sorted_df[
            "q10_middle_price_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(_Q10_MIDDLE_PRICE_BUCKET_ORDER, start=1)
            }
        )
    if "q10_middle_combined_bucket" in sorted_df.columns:
        sorted_df["_q10_middle_combined_bucket_order"] = sorted_df[
            "q10_middle_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(
                    _Q10_MIDDLE_COMBINED_BUCKET_ORDER, start=1
                )
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
        label_lookup.setdefault(feature, _RANKING_FEATURE_LABEL_MAP.get(feature, feature))
    return label_lookup


def _summarize_ranking_features(ranked_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _core_summarize_ranking_features(
        ranked_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _summarize_future_targets(horizon_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _core_summarize_future_targets(
        horizon_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _build_daily_group_means(horizon_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_daily_group_means(
        horizon_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _aligned_decile_pivot(
    daily_group_means_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = daily_group_means_df.loc[
        (daily_group_means_df["ranking_feature"] == ranking_feature)
        & (daily_group_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(DECILE_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="feature_decile",
            values=value_column,
        )
        .reindex(columns=list(DECILE_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _safe_kruskal(samples: list[np.ndarray]) -> tuple[float | None, float | None]:
    return _core_safe_kruskal(samples)


def _safe_friedman(samples: list[np.ndarray]) -> tuple[float | None, float | None]:
    return _core_safe_friedman(samples)


def _kendalls_w(
    *,
    friedman_statistic: float | None,
    n_dates: int,
    n_groups: int,
) -> float | None:
    return _core_kendalls_w(
        friedman_statistic=friedman_statistic,
        n_dates=n_dates,
        n_groups=n_groups,
    )


def _holm_adjust(p_values: list[float | None]) -> list[float | None]:
    return _core_holm_adjust(p_values)


def _safe_paired_t_test(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    return _core_safe_paired_t_test(left, right)


def _safe_wilcoxon(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    return _core_safe_wilcoxon(left, right)


def _build_global_significance(daily_group_means_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_global_significance(
        daily_group_means_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _build_pairwise_significance(daily_group_means_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_pairwise_significance(
        daily_group_means_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def _build_extreme_vs_middle_daily_means(
    horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if horizon_panel_df.empty:
        return pd.DataFrame()

    group_frames: list[pd.DataFrame] = []
    for bucket_group in BUCKET_GROUP_ORDER:
        bucket_deciles = _BUCKET_GROUP_DECILES[bucket_group]
        frame = horizon_panel_df.loc[
            horizon_panel_df["feature_decile"].isin(bucket_deciles)
        ].copy()
        if frame.empty:
            continue
        frame["bucket_group"] = bucket_group
        frame["bucket_group_label"] = _BUCKET_GROUP_LABEL_MAP[bucket_group]
        group_frames.append(frame)
    if not group_frames:
        return pd.DataFrame()

    bucket_panel_df = pd.concat(group_frames, ignore_index=True)
    daily_means_df = (
        bucket_panel_df.groupby(
            [
                "ranking_feature",
                "ranking_feature_label",
                "horizon_key",
                "horizon_days",
                "date",
                "bucket_group",
                "bucket_group_label",
            ],
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_ranking_value=("ranking_value", "mean"),
            group_mean_event_close=("close", "mean"),
            group_mean_future_close=("future_close", "mean"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
        )
    )
    return _sort_frame(daily_means_df)


def _summarize_extreme_vs_middle(
    extreme_vs_middle_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if extreme_vs_middle_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        extreme_vs_middle_daily_means_df.groupby(
            [
                "ranking_feature",
                "ranking_feature_label",
                "horizon_key",
                "horizon_days",
                "bucket_group",
                "bucket_group_label",
            ],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_group_size=("group_sample_count", "mean"),
            mean_ranking_value=("group_mean_ranking_value", "mean"),
            mean_event_close=("group_mean_event_close", "mean"),
            mean_future_close=("group_mean_future_close", "mean"),
            mean_future_return=("group_mean_future_return", "mean"),
            median_future_return=("group_median_future_return", "median"),
            std_future_return=("group_mean_future_return", "std"),
        )
    )
    return _sort_frame(summary_df)


def _aligned_bucket_pivot(
    extreme_vs_middle_daily_means_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = extreme_vs_middle_daily_means_df.loc[
        (extreme_vs_middle_daily_means_df["ranking_feature"] == ranking_feature)
        & (extreme_vs_middle_daily_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(BUCKET_GROUP_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="bucket_group",
            values=value_column,
        )
        .reindex(columns=list(BUCKET_GROUP_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_extreme_vs_middle_significance(
    extreme_vs_middle_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if extreme_vs_middle_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    feature_values = _ordered_feature_values(
        extreme_vs_middle_daily_means_df["ranking_feature"]
    )
    label_lookup = _ranking_feature_label_lookup(extreme_vs_middle_daily_means_df)
    for ranking_feature in feature_values:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                pivot_df = _aligned_bucket_pivot(
                    extreme_vs_middle_daily_means_df,
                    ranking_feature=ranking_feature,
                    horizon_key=horizon_key,
                    value_column=metric_columns[metric_key],
                )
                if pivot_df.empty:
                    records.append(
                        {
                            "ranking_feature": ranking_feature,
                            "ranking_feature_label": label_lookup.get(
                                ranking_feature,
                                _RANKING_FEATURE_LABEL_MAP.get(
                                    ranking_feature,
                                    ranking_feature,
                                ),
                            ),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "n_dates": 0,
                            "extreme_group_label": _BUCKET_GROUP_LABEL_MAP[
                                "q1_q10_extreme"
                            ],
                            "middle_group_label": _BUCKET_GROUP_LABEL_MAP[
                                "q4_q5_q6_middle"
                            ],
                            "extreme_mean": None,
                            "middle_mean": None,
                            "extreme_minus_middle_mean": None,
                            "paired_t_statistic": None,
                            "paired_t_p_value": None,
                            "wilcoxon_statistic": None,
                            "wilcoxon_p_value": None,
                        }
                    )
                    continue

                extreme = pivot_df["q1_q10_extreme"].to_numpy(dtype=float)
                middle = pivot_df["q4_q5_q6_middle"].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                    extreme,
                    middle,
                )
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                    extreme,
                    middle,
                )
                extreme_mean = float(extreme.mean())
                middle_mean = float(middle.mean())
                records.append(
                    {
                        "ranking_feature": ranking_feature,
                        "ranking_feature_label": label_lookup.get(
                            ranking_feature,
                            _RANKING_FEATURE_LABEL_MAP.get(
                                ranking_feature,
                                ranking_feature,
                            ),
                        ),
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": int(len(pivot_df)),
                        "extreme_group_label": _BUCKET_GROUP_LABEL_MAP[
                            "q1_q10_extreme"
                        ],
                        "middle_group_label": _BUCKET_GROUP_LABEL_MAP[
                            "q4_q5_q6_middle"
                        ],
                        "extreme_mean": extreme_mean,
                        "middle_mean": middle_mean,
                        "extreme_minus_middle_mean": extreme_mean - middle_mean,
                        "paired_t_statistic": paired_t_statistic,
                        "paired_t_p_value": paired_t_p_value,
                        "wilcoxon_statistic": wilcoxon_statistic,
                        "wilcoxon_p_value": wilcoxon_p_value,
                    }
                )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_nested_volume_split_panel(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: RankingFeatureKey = _PRIMARY_PRICE_FEATURE,
    volume_feature: RankingFeatureKey = _PRIMARY_VOLUME_FEATURE,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    panel_df = event_panel_df.copy()
    panel_df["price_rank_desc"] = (
        panel_df.groupby("date")[price_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["price_decile_index"] = (
        ((panel_df["price_rank_desc"] - 1) * len(DECILE_ORDER))
        // panel_df["date_constituent_count"]
    ) + 1
    panel_df["price_decile_index"] = panel_df["price_decile_index"].clip(
        1, len(DECILE_ORDER)
    )
    panel_df["price_decile"] = panel_df["price_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    panel_df["nested_price_bucket"] = None
    for bucket_key, bucket_deciles in _NESTED_PRICE_BUCKET_DECILES.items():
        panel_df.loc[
            panel_df["price_decile"].isin(bucket_deciles), "nested_price_bucket"
        ] = bucket_key
    panel_df = panel_df.dropna(subset=["nested_price_bucket"]).copy()
    if panel_df.empty:
        return pd.DataFrame()

    panel_df["nested_price_bucket"] = panel_df["nested_price_bucket"].astype(str)
    panel_df["nested_price_bucket_label"] = panel_df["nested_price_bucket"].map(
        _NESTED_PRICE_BUCKET_LABEL_MAP
    )
    panel_df["nested_price_bucket_size"] = panel_df.groupby(
        ["date", "nested_price_bucket"]
    )["code"].transform("size")
    panel_df["volume_rank_desc_within_price_bucket"] = (
        panel_df.groupby(["date", "nested_price_bucket"])[volume_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["nested_volume_bucket_index"] = (
        ((panel_df["volume_rank_desc_within_price_bucket"] - 1) * 2)
        // panel_df["nested_price_bucket_size"]
    ) + 1
    panel_df["nested_volume_bucket_index"] = panel_df[
        "nested_volume_bucket_index"
    ].clip(1, 2)
    panel_df["nested_volume_bucket"] = panel_df["nested_volume_bucket_index"].map(
        {1: "volume_high", 2: "volume_low"}
    )
    panel_df["nested_volume_bucket_label"] = panel_df["nested_volume_bucket"].map(
        _NESTED_VOLUME_BUCKET_LABEL_MAP
    )
    panel_df["nested_combined_bucket"] = (
        panel_df["nested_price_bucket"] + "_" + panel_df["nested_volume_bucket"]
    )
    panel_df["nested_combined_bucket_label"] = panel_df["nested_combined_bucket"].map(
        _NESTED_COMBINED_BUCKET_LABEL_MAP
    )
    panel_df["nested_price_feature"] = price_feature
    panel_df["nested_price_feature_label"] = _RANKING_FEATURE_LABEL_MAP[price_feature]
    panel_df["nested_volume_feature"] = volume_feature
    panel_df["nested_volume_feature_label"] = _RANKING_FEATURE_LABEL_MAP[volume_feature]
    return _sort_frame(panel_df.reset_index(drop=True))


def _build_nested_volume_horizon_panel(
    nested_volume_split_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if nested_volume_split_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        "nested_price_feature",
        "nested_price_feature_label",
        "nested_volume_feature",
        "nested_volume_feature_label",
        "nested_price_bucket",
        "nested_price_bucket_label",
        "nested_volume_bucket",
        "nested_volume_bucket_label",
        "nested_combined_bucket",
        "nested_combined_bucket_label",
    ]
    frames: list[pd.DataFrame] = []
    for horizon_key in HORIZON_ORDER:
        frame = nested_volume_split_panel_df[
            base_columns + [f"{horizon_key}_close", f"{horizon_key}_return"]
        ].copy()
        frame["horizon_key"] = horizon_key
        frame["horizon_days"] = _HORIZON_DAY_MAP[horizon_key]
        frame["future_close"] = frame.pop(f"{horizon_key}_close")
        frame["future_return"] = frame.pop(f"{horizon_key}_return")
        frame = frame.dropna(subset=["future_close", "future_return"]).copy()
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=base_columns + ["horizon_key", "horizon_days"])
    return _sort_frame(pd.concat(frames, ignore_index=True))


def _build_nested_volume_daily_means(
    nested_volume_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if nested_volume_horizon_panel_df.empty:
        return pd.DataFrame()

    daily_group_means_df = (
        nested_volume_horizon_panel_df.groupby(
            [
                "nested_price_feature",
                "nested_price_feature_label",
                "nested_volume_feature",
                "nested_volume_feature_label",
                "horizon_key",
                "horizon_days",
                "date",
                "nested_price_bucket",
                "nested_price_bucket_label",
                "nested_volume_bucket",
                "nested_volume_bucket_label",
                "nested_combined_bucket",
                "nested_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_event_close=("close", "mean"),
            group_mean_future_close=("future_close", "mean"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
        )
    )
    return _sort_frame(daily_group_means_df)


def _summarize_nested_volume_split(
    nested_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if nested_volume_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        nested_volume_daily_means_df.groupby(
            [
                "nested_price_feature",
                "nested_price_feature_label",
                "nested_volume_feature",
                "nested_volume_feature_label",
                "horizon_key",
                "horizon_days",
                "nested_price_bucket",
                "nested_price_bucket_label",
                "nested_volume_bucket",
                "nested_volume_bucket_label",
                "nested_combined_bucket",
                "nested_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_group_size=("group_sample_count", "mean"),
            mean_event_close=("group_mean_event_close", "mean"),
            mean_future_close=("group_mean_future_close", "mean"),
            mean_future_return=("group_mean_future_return", "mean"),
            median_future_return=("group_median_future_return", "median"),
            std_future_return=("group_mean_future_return", "std"),
        )
    )
    return _sort_frame(summary_df)


def _aligned_nested_combined_pivot(
    nested_volume_daily_means_df: pd.DataFrame,
    *,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = nested_volume_daily_means_df.loc[
        nested_volume_daily_means_df["horizon_key"] == horizon_key
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(_NESTED_COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="nested_combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(_NESTED_COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_nested_volume_global_significance(
    nested_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if nested_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_nested_combined_pivot(
                nested_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                records.append(
                    {
                        "nested_price_feature": _PRIMARY_PRICE_FEATURE,
                        "nested_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "nested_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "nested_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": 0,
                        "friedman_statistic": None,
                        "friedman_p_value": None,
                        "kendalls_w": None,
                        "kruskal_statistic": None,
                        "kruskal_p_value": None,
                    }
                )
                continue

            samples = [
                pivot_df[bucket].to_numpy(dtype=float)
                for bucket in _NESTED_COMBINED_BUCKET_ORDER
            ]
            friedman_statistic, friedman_p_value = _safe_friedman(samples)
            kruskal_statistic, kruskal_p_value = _safe_kruskal(samples)
            records.append(
                {
                    "nested_price_feature": _PRIMARY_PRICE_FEATURE,
                    "nested_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_PRICE_FEATURE
                    ],
                    "nested_volume_feature": _PRIMARY_VOLUME_FEATURE,
                    "nested_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_VOLUME_FEATURE
                    ],
                    "horizon_key": horizon_key,
                    "metric_key": metric_key,
                    "n_dates": int(len(pivot_df)),
                    "friedman_statistic": friedman_statistic,
                    "friedman_p_value": friedman_p_value,
                    "kendalls_w": _kendalls_w(
                        friedman_statistic=friedman_statistic,
                        n_dates=len(pivot_df),
                        n_groups=len(_NESTED_COMBINED_BUCKET_ORDER),
                    ),
                    "kruskal_statistic": kruskal_statistic,
                    "kruskal_p_value": kruskal_p_value,
                }
            )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_nested_volume_pairwise_significance(
    nested_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if nested_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_nested_combined_pivot(
                nested_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                for left_bucket, right_bucket in combinations(
                    _NESTED_COMBINED_BUCKET_ORDER, 2
                ):
                    records.append(
                        {
                            "nested_price_feature": _PRIMARY_PRICE_FEATURE,
                            "nested_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                                _PRIMARY_PRICE_FEATURE
                            ],
                            "nested_volume_feature": _PRIMARY_VOLUME_FEATURE,
                            "nested_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                                _PRIMARY_VOLUME_FEATURE
                            ],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_nested_combined_bucket": left_bucket,
                            "left_nested_combined_bucket_label": _NESTED_COMBINED_BUCKET_LABEL_MAP[
                                left_bucket
                            ],
                            "right_nested_combined_bucket": right_bucket,
                            "right_nested_combined_bucket_label": _NESTED_COMBINED_BUCKET_LABEL_MAP[
                                right_bucket
                            ],
                            "n_dates": 0,
                            "mean_difference": None,
                            "paired_t_statistic": None,
                            "paired_t_p_value": None,
                            "wilcoxon_statistic": None,
                            "wilcoxon_p_value": None,
                        }
                    )
                continue

            for left_bucket, right_bucket in combinations(
                _NESTED_COMBINED_BUCKET_ORDER, 2
            ):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                    left, right
                )
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "nested_price_feature": _PRIMARY_PRICE_FEATURE,
                        "nested_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "nested_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "nested_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_nested_combined_bucket": left_bucket,
                        "left_nested_combined_bucket_label": _NESTED_COMBINED_BUCKET_LABEL_MAP[
                            left_bucket
                        ],
                        "right_nested_combined_bucket": right_bucket,
                        "right_nested_combined_bucket_label": _NESTED_COMBINED_BUCKET_LABEL_MAP[
                            right_bucket
                        ],
                        "n_dates": int(len(pivot_df)),
                        "mean_difference": float((left - right).mean()),
                        "paired_t_statistic": paired_t_statistic,
                        "paired_t_p_value": paired_t_p_value,
                        "wilcoxon_statistic": wilcoxon_statistic,
                        "wilcoxon_p_value": wilcoxon_p_value,
                    }
                )

    pairwise_df = pd.DataFrame.from_records(records)
    pairwise_df["paired_t_p_value_holm"] = None
    pairwise_df["wilcoxon_p_value_holm"] = None
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            mask = (
                (pairwise_df["horizon_key"] == horizon_key)
                & (pairwise_df["metric_key"] == metric_key)
            )
            pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                pairwise_df.loc[mask, "paired_t_p_value"].tolist()
            )
            pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
            )
    return _sort_frame(pairwise_df)


def _build_nested_volume_interaction(
    nested_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if nested_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_nested_combined_pivot(
                nested_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                records.append(
                    {
                        "nested_price_feature": _PRIMARY_PRICE_FEATURE,
                        "nested_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "nested_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "nested_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": 0,
                        "price_spread_high_volume": None,
                        "price_spread_low_volume": None,
                        "interaction_difference": None,
                        "paired_t_statistic": None,
                        "paired_t_p_value": None,
                        "wilcoxon_statistic": None,
                        "wilcoxon_p_value": None,
                    }
                )
                continue

            spread_high = (
                pivot_df["extreme_volume_high"].to_numpy(dtype=float)
                - pivot_df["middle_volume_high"].to_numpy(dtype=float)
            )
            spread_low = (
                pivot_df["extreme_volume_low"].to_numpy(dtype=float)
                - pivot_df["middle_volume_low"].to_numpy(dtype=float)
            )
            paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                spread_high, spread_low
            )
            wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                spread_high, spread_low
            )
            records.append(
                {
                    "nested_price_feature": _PRIMARY_PRICE_FEATURE,
                    "nested_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_PRICE_FEATURE
                    ],
                    "nested_volume_feature": _PRIMARY_VOLUME_FEATURE,
                    "nested_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_VOLUME_FEATURE
                    ],
                    "horizon_key": horizon_key,
                    "metric_key": metric_key,
                    "n_dates": int(len(pivot_df)),
                    "price_spread_high_volume": float(spread_high.mean()),
                    "price_spread_low_volume": float(spread_low.mean()),
                    "interaction_difference": float((spread_high - spread_low).mean()),
                    "paired_t_statistic": paired_t_statistic,
                    "paired_t_p_value": paired_t_p_value,
                    "wilcoxon_statistic": wilcoxon_statistic,
                    "wilcoxon_p_value": wilcoxon_p_value,
                }
            )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_q1_q10_volume_split_panel(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: RankingFeatureKey = _PRIMARY_PRICE_FEATURE,
    volume_feature: RankingFeatureKey = _PRIMARY_VOLUME_FEATURE,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    panel_df = event_panel_df.copy()
    panel_df["price_rank_desc"] = (
        panel_df.groupby("date")[price_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["price_decile_index"] = (
        ((panel_df["price_rank_desc"] - 1) * len(DECILE_ORDER))
        // panel_df["date_constituent_count"]
    ) + 1
    panel_df["price_decile_index"] = panel_df["price_decile_index"].clip(
        1, len(DECILE_ORDER)
    )
    panel_df["price_decile"] = panel_df["price_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    panel_df["q1_q10_price_bucket"] = None
    for bucket_key, bucket_deciles in _Q1_Q10_PRICE_BUCKET_DECILES.items():
        panel_df.loc[
            panel_df["price_decile"].isin(bucket_deciles), "q1_q10_price_bucket"
        ] = bucket_key
    panel_df = panel_df.dropna(subset=["q1_q10_price_bucket"]).copy()
    if panel_df.empty:
        return pd.DataFrame()

    panel_df["q1_q10_price_bucket"] = panel_df["q1_q10_price_bucket"].astype(str)
    panel_df["q1_q10_price_bucket_label"] = panel_df["q1_q10_price_bucket"].map(
        _Q1_Q10_PRICE_BUCKET_LABEL_MAP
    )
    panel_df["q1_q10_price_bucket_size"] = panel_df.groupby(
        ["date", "q1_q10_price_bucket"]
    )["code"].transform("size")
    panel_df["q1_q10_volume_rank_desc_within_price_bucket"] = (
        panel_df.groupby(["date", "q1_q10_price_bucket"])[volume_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["q1_q10_volume_bucket_index"] = (
        ((panel_df["q1_q10_volume_rank_desc_within_price_bucket"] - 1) * 2)
        // panel_df["q1_q10_price_bucket_size"]
    ) + 1
    panel_df["q1_q10_volume_bucket_index"] = panel_df[
        "q1_q10_volume_bucket_index"
    ].clip(1, 2)
    panel_df["q1_q10_volume_bucket"] = panel_df["q1_q10_volume_bucket_index"].map(
        {1: "volume_high", 2: "volume_low"}
    )
    panel_df["q1_q10_volume_bucket_label"] = panel_df["q1_q10_volume_bucket"].map(
        _NESTED_VOLUME_BUCKET_LABEL_MAP
    )
    panel_df["q1_q10_combined_bucket"] = (
        panel_df["q1_q10_price_bucket"] + "_" + panel_df["q1_q10_volume_bucket"]
    )
    panel_df["q1_q10_combined_bucket_label"] = panel_df[
        "q1_q10_combined_bucket"
    ].map(_Q1_Q10_COMBINED_BUCKET_LABEL_MAP)
    panel_df["q1_q10_price_feature"] = price_feature
    panel_df["q1_q10_price_feature_label"] = _RANKING_FEATURE_LABEL_MAP[price_feature]
    panel_df["q1_q10_volume_feature"] = volume_feature
    panel_df["q1_q10_volume_feature_label"] = _RANKING_FEATURE_LABEL_MAP[volume_feature]
    return _sort_frame(panel_df.reset_index(drop=True))


def _build_q1_q10_volume_horizon_panel(
    q1_q10_volume_split_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if q1_q10_volume_split_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        "q1_q10_price_feature",
        "q1_q10_price_feature_label",
        "q1_q10_volume_feature",
        "q1_q10_volume_feature_label",
        "q1_q10_price_bucket",
        "q1_q10_price_bucket_label",
        "q1_q10_volume_bucket",
        "q1_q10_volume_bucket_label",
        "q1_q10_combined_bucket",
        "q1_q10_combined_bucket_label",
    ]
    frames: list[pd.DataFrame] = []
    for horizon_key in HORIZON_ORDER:
        frame = q1_q10_volume_split_panel_df[
            base_columns + [f"{horizon_key}_close", f"{horizon_key}_return"]
        ].copy()
        frame["horizon_key"] = horizon_key
        frame["horizon_days"] = _HORIZON_DAY_MAP[horizon_key]
        frame["future_close"] = frame.pop(f"{horizon_key}_close")
        frame["future_return"] = frame.pop(f"{horizon_key}_return")
        frame = frame.dropna(subset=["future_close", "future_return"]).copy()
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=base_columns + ["horizon_key", "horizon_days"])
    return _sort_frame(pd.concat(frames, ignore_index=True))


def _build_q1_q10_volume_daily_means(
    q1_q10_volume_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if q1_q10_volume_horizon_panel_df.empty:
        return pd.DataFrame()

    daily_group_means_df = (
        q1_q10_volume_horizon_panel_df.groupby(
            [
                "q1_q10_price_feature",
                "q1_q10_price_feature_label",
                "q1_q10_volume_feature",
                "q1_q10_volume_feature_label",
                "horizon_key",
                "horizon_days",
                "date",
                "q1_q10_price_bucket",
                "q1_q10_price_bucket_label",
                "q1_q10_volume_bucket",
                "q1_q10_volume_bucket_label",
                "q1_q10_combined_bucket",
                "q1_q10_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_event_close=("close", "mean"),
            group_mean_future_close=("future_close", "mean"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
        )
    )
    return _sort_frame(daily_group_means_df)


def _summarize_q1_q10_volume_split(
    q1_q10_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q1_q10_volume_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        q1_q10_volume_daily_means_df.groupby(
            [
                "q1_q10_price_feature",
                "q1_q10_price_feature_label",
                "q1_q10_volume_feature",
                "q1_q10_volume_feature_label",
                "horizon_key",
                "horizon_days",
                "q1_q10_price_bucket",
                "q1_q10_price_bucket_label",
                "q1_q10_volume_bucket",
                "q1_q10_volume_bucket_label",
                "q1_q10_combined_bucket",
                "q1_q10_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_group_size=("group_sample_count", "mean"),
            mean_event_close=("group_mean_event_close", "mean"),
            mean_future_close=("group_mean_future_close", "mean"),
            mean_future_return=("group_mean_future_return", "mean"),
            median_future_return=("group_median_future_return", "median"),
            std_future_return=("group_mean_future_return", "std"),
        )
    )
    return _sort_frame(summary_df)


def _aligned_q1_q10_combined_pivot(
    q1_q10_volume_daily_means_df: pd.DataFrame,
    *,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = q1_q10_volume_daily_means_df.loc[
        q1_q10_volume_daily_means_df["horizon_key"] == horizon_key
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(_Q1_Q10_COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="q1_q10_combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(_Q1_Q10_COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_q1_q10_volume_global_significance(
    q1_q10_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q1_q10_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_q1_q10_combined_pivot(
                q1_q10_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                records.append(
                    {
                        "q1_q10_price_feature": _PRIMARY_PRICE_FEATURE,
                        "q1_q10_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "q1_q10_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "q1_q10_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": 0,
                        "friedman_statistic": None,
                        "friedman_p_value": None,
                        "kendalls_w": None,
                        "kruskal_statistic": None,
                        "kruskal_p_value": None,
                    }
                )
                continue

            samples = [
                pivot_df[bucket].to_numpy(dtype=float)
                for bucket in _Q1_Q10_COMBINED_BUCKET_ORDER
            ]
            friedman_statistic, friedman_p_value = _safe_friedman(samples)
            kruskal_statistic, kruskal_p_value = _safe_kruskal(samples)
            records.append(
                {
                    "q1_q10_price_feature": _PRIMARY_PRICE_FEATURE,
                    "q1_q10_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_PRICE_FEATURE
                    ],
                    "q1_q10_volume_feature": _PRIMARY_VOLUME_FEATURE,
                    "q1_q10_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_VOLUME_FEATURE
                    ],
                    "horizon_key": horizon_key,
                    "metric_key": metric_key,
                    "n_dates": int(len(pivot_df)),
                    "friedman_statistic": friedman_statistic,
                    "friedman_p_value": friedman_p_value,
                    "kendalls_w": _kendalls_w(
                        friedman_statistic=friedman_statistic,
                        n_dates=len(pivot_df),
                        n_groups=len(_Q1_Q10_COMBINED_BUCKET_ORDER),
                    ),
                    "kruskal_statistic": kruskal_statistic,
                    "kruskal_p_value": kruskal_p_value,
                }
            )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_q1_q10_volume_pairwise_significance(
    q1_q10_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q1_q10_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_q1_q10_combined_pivot(
                q1_q10_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                for left_bucket, right_bucket in combinations(
                    _Q1_Q10_COMBINED_BUCKET_ORDER, 2
                ):
                    records.append(
                        {
                            "q1_q10_price_feature": _PRIMARY_PRICE_FEATURE,
                            "q1_q10_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                                _PRIMARY_PRICE_FEATURE
                            ],
                            "q1_q10_volume_feature": _PRIMARY_VOLUME_FEATURE,
                            "q1_q10_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                                _PRIMARY_VOLUME_FEATURE
                            ],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_q1_q10_combined_bucket": left_bucket,
                            "left_q1_q10_combined_bucket_label": _Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
                                left_bucket
                            ],
                            "right_q1_q10_combined_bucket": right_bucket,
                            "right_q1_q10_combined_bucket_label": _Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
                                right_bucket
                            ],
                            "n_dates": 0,
                            "mean_difference": None,
                            "paired_t_statistic": None,
                            "paired_t_p_value": None,
                            "wilcoxon_statistic": None,
                            "wilcoxon_p_value": None,
                        }
                    )
                continue

            for left_bucket, right_bucket in combinations(
                _Q1_Q10_COMBINED_BUCKET_ORDER, 2
            ):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                    left, right
                )
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "q1_q10_price_feature": _PRIMARY_PRICE_FEATURE,
                        "q1_q10_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "q1_q10_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "q1_q10_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_q1_q10_combined_bucket": left_bucket,
                        "left_q1_q10_combined_bucket_label": _Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
                            left_bucket
                        ],
                        "right_q1_q10_combined_bucket": right_bucket,
                        "right_q1_q10_combined_bucket_label": _Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
                            right_bucket
                        ],
                        "n_dates": int(len(pivot_df)),
                        "mean_difference": float((left - right).mean()),
                        "paired_t_statistic": paired_t_statistic,
                        "paired_t_p_value": paired_t_p_value,
                        "wilcoxon_statistic": wilcoxon_statistic,
                        "wilcoxon_p_value": wilcoxon_p_value,
                    }
                )

    pairwise_df = pd.DataFrame.from_records(records)
    pairwise_df["paired_t_p_value_holm"] = None
    pairwise_df["wilcoxon_p_value_holm"] = None
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            mask = (
                (pairwise_df["horizon_key"] == horizon_key)
                & (pairwise_df["metric_key"] == metric_key)
            )
            pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                pairwise_df.loc[mask, "paired_t_p_value"].tolist()
            )
            pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
            )
    return _sort_frame(pairwise_df)


def _build_q1_q10_volume_interaction(
    q1_q10_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q1_q10_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_q1_q10_combined_pivot(
                q1_q10_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                records.append(
                    {
                        "q1_q10_price_feature": _PRIMARY_PRICE_FEATURE,
                        "q1_q10_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "q1_q10_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "q1_q10_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": 0,
                        "q1_minus_q10_high_volume": None,
                        "q1_minus_q10_low_volume": None,
                        "interaction_difference": None,
                        "paired_t_statistic": None,
                        "paired_t_p_value": None,
                        "wilcoxon_statistic": None,
                        "wilcoxon_p_value": None,
                    }
                )
                continue

            spread_high = (
                pivot_df["q1_volume_high"].to_numpy(dtype=float)
                - pivot_df["q10_volume_high"].to_numpy(dtype=float)
            )
            spread_low = (
                pivot_df["q1_volume_low"].to_numpy(dtype=float)
                - pivot_df["q10_volume_low"].to_numpy(dtype=float)
            )
            paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                spread_high, spread_low
            )
            wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                spread_high, spread_low
            )
            records.append(
                {
                    "q1_q10_price_feature": _PRIMARY_PRICE_FEATURE,
                    "q1_q10_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_PRICE_FEATURE
                    ],
                    "q1_q10_volume_feature": _PRIMARY_VOLUME_FEATURE,
                    "q1_q10_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                        _PRIMARY_VOLUME_FEATURE
                    ],
                    "horizon_key": horizon_key,
                    "metric_key": metric_key,
                    "n_dates": int(len(pivot_df)),
                    "q1_minus_q10_high_volume": float(spread_high.mean()),
                    "q1_minus_q10_low_volume": float(spread_low.mean()),
                    "interaction_difference": float((spread_high - spread_low).mean()),
                    "paired_t_statistic": paired_t_statistic,
                    "paired_t_p_value": paired_t_p_value,
                    "wilcoxon_statistic": wilcoxon_statistic,
                    "wilcoxon_p_value": wilcoxon_p_value,
                }
            )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_q10_middle_volume_split_panel(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: RankingFeatureKey = _PRIMARY_PRICE_FEATURE,
    volume_feature: RankingFeatureKey = _PRIMARY_VOLUME_FEATURE,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    panel_df = event_panel_df.copy()
    panel_df["price_rank_desc"] = (
        panel_df.groupby("date")[price_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["price_decile_index"] = (
        ((panel_df["price_rank_desc"] - 1) * len(DECILE_ORDER))
        // panel_df["date_constituent_count"]
    ) + 1
    panel_df["price_decile_index"] = panel_df["price_decile_index"].clip(
        1, len(DECILE_ORDER)
    )
    panel_df["price_decile"] = panel_df["price_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    panel_df["q10_middle_price_bucket"] = None
    for bucket_key, bucket_deciles in _Q10_MIDDLE_PRICE_BUCKET_DECILES.items():
        panel_df.loc[
            panel_df["price_decile"].isin(bucket_deciles), "q10_middle_price_bucket"
        ] = bucket_key
    panel_df = panel_df.dropna(subset=["q10_middle_price_bucket"]).copy()
    if panel_df.empty:
        return pd.DataFrame()

    panel_df["q10_middle_price_bucket"] = panel_df["q10_middle_price_bucket"].astype(
        str
    )
    panel_df["q10_middle_price_bucket_label"] = panel_df[
        "q10_middle_price_bucket"
    ].map(_Q10_MIDDLE_PRICE_BUCKET_LABEL_MAP)
    panel_df["q10_middle_price_bucket_size"] = panel_df.groupby(
        ["date", "q10_middle_price_bucket"]
    )["code"].transform("size")
    panel_df["q10_middle_volume_rank_desc_within_price_bucket"] = (
        panel_df.groupby(["date", "q10_middle_price_bucket"])[volume_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["q10_middle_volume_bucket_index"] = (
        ((panel_df["q10_middle_volume_rank_desc_within_price_bucket"] - 1) * 2)
        // panel_df["q10_middle_price_bucket_size"]
    ) + 1
    panel_df["q10_middle_volume_bucket_index"] = panel_df[
        "q10_middle_volume_bucket_index"
    ].clip(1, 2)
    panel_df["q10_middle_volume_bucket"] = panel_df["q10_middle_volume_bucket_index"].map(
        {1: "volume_high", 2: "volume_low"}
    )
    panel_df["q10_middle_volume_bucket_label"] = panel_df[
        "q10_middle_volume_bucket"
    ].map(_NESTED_VOLUME_BUCKET_LABEL_MAP)
    panel_df["q10_middle_combined_bucket"] = (
        panel_df["q10_middle_price_bucket"] + "_" + panel_df["q10_middle_volume_bucket"]
    )
    panel_df["q10_middle_combined_bucket_label"] = panel_df[
        "q10_middle_combined_bucket"
    ].map(_Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP)
    panel_df["q10_middle_price_feature"] = price_feature
    panel_df["q10_middle_price_feature_label"] = _RANKING_FEATURE_LABEL_MAP[
        price_feature
    ]
    panel_df["q10_middle_volume_feature"] = volume_feature
    panel_df["q10_middle_volume_feature_label"] = _RANKING_FEATURE_LABEL_MAP[
        volume_feature
    ]
    return _sort_frame(panel_df.reset_index(drop=True))


def _build_q10_middle_volume_horizon_panel(
    q10_middle_volume_split_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_split_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        "q10_middle_price_feature",
        "q10_middle_price_feature_label",
        "q10_middle_volume_feature",
        "q10_middle_volume_feature_label",
        "q10_middle_price_bucket",
        "q10_middle_price_bucket_label",
        "q10_middle_volume_bucket",
        "q10_middle_volume_bucket_label",
        "q10_middle_combined_bucket",
        "q10_middle_combined_bucket_label",
    ]
    frames: list[pd.DataFrame] = []
    for horizon_key in HORIZON_ORDER:
        frame = q10_middle_volume_split_panel_df[
            base_columns + [f"{horizon_key}_close", f"{horizon_key}_return"]
        ].copy()
        frame["horizon_key"] = horizon_key
        frame["horizon_days"] = _HORIZON_DAY_MAP[horizon_key]
        frame["future_close"] = frame.pop(f"{horizon_key}_close")
        frame["future_return"] = frame.pop(f"{horizon_key}_return")
        frame = frame.dropna(subset=["future_close", "future_return"]).copy()
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=base_columns + ["horizon_key", "horizon_days"])
    return _sort_frame(pd.concat(frames, ignore_index=True))


def _build_q10_middle_volume_daily_means(
    q10_middle_volume_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_horizon_panel_df.empty:
        return pd.DataFrame()

    daily_group_means_df = (
        q10_middle_volume_horizon_panel_df.groupby(
            [
                "q10_middle_price_feature",
                "q10_middle_price_feature_label",
                "q10_middle_volume_feature",
                "q10_middle_volume_feature_label",
                "horizon_key",
                "horizon_days",
                "date",
                "q10_middle_price_bucket",
                "q10_middle_price_bucket_label",
                "q10_middle_volume_bucket",
                "q10_middle_volume_bucket_label",
                "q10_middle_combined_bucket",
                "q10_middle_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_event_close=("close", "mean"),
            group_mean_future_close=("future_close", "mean"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
        )
    )
    return _sort_frame(daily_group_means_df)


def _summarize_q10_middle_volume_split(
    q10_middle_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        q10_middle_volume_daily_means_df.groupby(
            [
                "q10_middle_price_feature",
                "q10_middle_price_feature_label",
                "q10_middle_volume_feature",
                "q10_middle_volume_feature_label",
                "horizon_key",
                "horizon_days",
                "q10_middle_price_bucket",
                "q10_middle_price_bucket_label",
                "q10_middle_volume_bucket",
                "q10_middle_volume_bucket_label",
                "q10_middle_combined_bucket",
                "q10_middle_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_group_size=("group_sample_count", "mean"),
            mean_event_close=("group_mean_event_close", "mean"),
            mean_future_close=("group_mean_future_close", "mean"),
            mean_future_return=("group_mean_future_return", "mean"),
            median_future_return=("group_median_future_return", "median"),
            std_future_return=("group_mean_future_return", "std"),
        )
    )
    return _sort_frame(summary_df)


def _aligned_q10_middle_combined_pivot(
    q10_middle_volume_daily_means_df: pd.DataFrame,
    *,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = q10_middle_volume_daily_means_df.loc[
        q10_middle_volume_daily_means_df["horizon_key"] == horizon_key
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(_Q10_MIDDLE_COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="q10_middle_combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(_Q10_MIDDLE_COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_q10_middle_volume_pairwise_significance(
    q10_middle_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_q10_middle_combined_pivot(
                q10_middle_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                for left_bucket, right_bucket in combinations(
                    _Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
                ):
                    records.append(
                        {
                            "q10_middle_price_feature": _PRIMARY_PRICE_FEATURE,
                            "q10_middle_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                                _PRIMARY_PRICE_FEATURE
                            ],
                            "q10_middle_volume_feature": _PRIMARY_VOLUME_FEATURE,
                            "q10_middle_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                                _PRIMARY_VOLUME_FEATURE
                            ],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_q10_middle_combined_bucket": left_bucket,
                            "left_q10_middle_combined_bucket_label": _Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
                                left_bucket
                            ],
                            "right_q10_middle_combined_bucket": right_bucket,
                            "right_q10_middle_combined_bucket_label": _Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
                                right_bucket
                            ],
                            "n_dates": 0,
                            "mean_difference": None,
                            "paired_t_statistic": None,
                            "paired_t_p_value": None,
                            "wilcoxon_statistic": None,
                            "wilcoxon_p_value": None,
                        }
                    )
                continue

            for left_bucket, right_bucket in combinations(
                _Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
            ):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                    left, right
                )
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "q10_middle_price_feature": _PRIMARY_PRICE_FEATURE,
                        "q10_middle_price_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_PRICE_FEATURE
                        ],
                        "q10_middle_volume_feature": _PRIMARY_VOLUME_FEATURE,
                        "q10_middle_volume_feature_label": _RANKING_FEATURE_LABEL_MAP[
                            _PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_q10_middle_combined_bucket": left_bucket,
                        "left_q10_middle_combined_bucket_label": _Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
                            left_bucket
                        ],
                        "right_q10_middle_combined_bucket": right_bucket,
                        "right_q10_middle_combined_bucket_label": _Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
                            right_bucket
                        ],
                        "n_dates": int(len(pivot_df)),
                        "mean_difference": float((left - right).mean()),
                        "paired_t_statistic": paired_t_statistic,
                        "paired_t_p_value": paired_t_p_value,
                        "wilcoxon_statistic": wilcoxon_statistic,
                        "wilcoxon_p_value": wilcoxon_p_value,
                    }
                )

    pairwise_df = pd.DataFrame.from_records(records)
    pairwise_df["paired_t_p_value_holm"] = None
    pairwise_df["wilcoxon_p_value_holm"] = None
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            mask = (
                (pairwise_df["horizon_key"] == horizon_key)
                & (pairwise_df["metric_key"] == metric_key)
            )
            pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                pairwise_df.loc[mask, "paired_t_p_value"].tolist()
            )
            pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
            )
    return _sort_frame(pairwise_df)


def _build_q10_low_hypothesis(
    q10_middle_volume_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_pairwise_significance_df.empty:
        return pd.DataFrame()

    hypothesis_pairs = (
        ("q10_volume_low", "q10_volume_high", "Q10 Low vs Q10 High"),
        ("q10_volume_low", "middle_volume_low", "Q10 Low vs Middle Low"),
        ("q10_volume_low", "middle_volume_high", "Q10 Low vs Middle High"),
    )
    records: list[dict[str, Any]] = []
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            scoped_df = q10_middle_volume_pairwise_significance_df[
                (q10_middle_volume_pairwise_significance_df["horizon_key"] == horizon_key)
                & (q10_middle_volume_pairwise_significance_df["metric_key"] == metric_key)
            ]
            for left_bucket, right_bucket, hypothesis_label in hypothesis_pairs:
                row = scoped_df[
                    (scoped_df["left_q10_middle_combined_bucket"] == left_bucket)
                    & (scoped_df["right_q10_middle_combined_bucket"] == right_bucket)
                ]
                if row.empty:
                    records.append(
                        {
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "hypothesis_label": hypothesis_label,
                            "left_q10_middle_combined_bucket": left_bucket,
                            "right_q10_middle_combined_bucket": right_bucket,
                            "mean_difference": None,
                            "paired_t_p_value_holm": None,
                            "wilcoxon_p_value_holm": None,
                        }
                    )
                    continue
                pairwise_row = row.iloc[0]
                records.append(
                    {
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "hypothesis_label": hypothesis_label,
                        "left_q10_middle_combined_bucket": left_bucket,
                        "right_q10_middle_combined_bucket": right_bucket,
                        "mean_difference": pairwise_row["mean_difference"],
                        "paired_t_p_value_holm": pairwise_row["paired_t_p_value_holm"],
                        "wilcoxon_p_value_holm": pairwise_row["wilcoxon_p_value_holm"],
                    }
                )
    return _sort_frame(pd.DataFrame.from_records(records))


def _filter_df_by_date_split(df: pd.DataFrame, *, split_name: str) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df.copy()
    if split_name == "discovery":
        return df.loc[df["date"] <= _DISCOVERY_END_DATE].copy()
    if split_name == "validation":
        return df.loc[df["date"] >= _VALIDATION_START_DATE].copy()
    raise ValueError(f"Unsupported split_name: {split_name}")


def _analyze_ranked_panel(ranked_panel_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if ranked_panel_df.empty:
        empty_df = pd.DataFrame()
        return {
            "ranking_feature_summary_df": empty_df,
            "decile_future_summary_df": empty_df,
            "daily_group_means_df": empty_df,
            "global_significance_df": empty_df,
            "pairwise_significance_df": empty_df,
            "extreme_vs_middle_summary_df": empty_df,
            "extreme_vs_middle_daily_means_df": empty_df,
            "extreme_vs_middle_significance_df": empty_df,
        }

    horizon_panel_df = _build_horizon_panel(ranked_panel_df)
    daily_group_means_df = _build_daily_group_means(horizon_panel_df)
    extreme_vs_middle_daily_means_df = _build_extreme_vs_middle_daily_means(
        horizon_panel_df
    )
    return {
        "ranking_feature_summary_df": _summarize_ranking_features(ranked_panel_df),
        "decile_future_summary_df": _summarize_future_targets(horizon_panel_df),
        "daily_group_means_df": daily_group_means_df,
        "global_significance_df": _build_global_significance(daily_group_means_df),
        "pairwise_significance_df": _build_pairwise_significance(daily_group_means_df),
        "extreme_vs_middle_summary_df": _summarize_extreme_vs_middle(
            extreme_vs_middle_daily_means_df
        ),
        "extreme_vs_middle_daily_means_df": extreme_vs_middle_daily_means_df,
        "extreme_vs_middle_significance_df": _build_extreme_vs_middle_significance(
            extreme_vs_middle_daily_means_df
        ),
    }


def _extract_global_row(
    global_significance_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    metric_key: MetricKey = "future_return",
) -> pd.Series | None:
    if global_significance_df.empty:
        return None
    row = global_significance_df[
        (global_significance_df["ranking_feature"] == ranking_feature)
        & (global_significance_df["horizon_key"] == horizon_key)
        & (global_significance_df["metric_key"] == metric_key)
    ]
    if row.empty:
        return None
    return row.iloc[0]


def _extract_pairwise_row(
    pairwise_significance_df: pd.DataFrame,
    *,
    ranking_feature: str,
    horizon_key: HorizonKey,
    left_decile: DecileKey = "Q1",
    right_decile: DecileKey = "Q10",
    metric_key: MetricKey = "future_return",
) -> pd.Series | None:
    if pairwise_significance_df.empty:
        return None
    row = pairwise_significance_df[
        (pairwise_significance_df["ranking_feature"] == ranking_feature)
        & (pairwise_significance_df["horizon_key"] == horizon_key)
        & (pairwise_significance_df["metric_key"] == metric_key)
        & (pairwise_significance_df["left_decile"] == left_decile)
        & (pairwise_significance_df["right_decile"] == right_decile)
    ]
    if row.empty:
        return None
    return row.iloc[0]


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _direction_from_difference(raw_difference: float | None) -> str:
    if raw_difference is None:
        return "high"
    return "high" if raw_difference >= 0 else "low"


def _aligned_difference(raw_difference: float | None, *, direction: str) -> float | None:
    if raw_difference is None:
        return None
    return raw_difference if direction == "high" else -raw_difference


def _robustness_score(
    aligned_discovery_diff: float | None,
    aligned_validation_diff: float | None,
) -> float | None:
    if aligned_discovery_diff is None or aligned_validation_diff is None:
        return None
    if aligned_discovery_diff <= 0 or aligned_validation_diff <= 0:
        return float(min(aligned_discovery_diff, aligned_validation_diff))
    return float(min(aligned_discovery_diff, aligned_validation_diff))


def _build_feature_selection(
    *,
    discovery_analysis: dict[str, pd.DataFrame],
    validation_analysis: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    records: list[dict[str, Any]] = []
    for ranking_feature in RANKING_FEATURE_ORDER:
        feature_family = "price" if ranking_feature.startswith("price_") else "volume"
        for horizon_key in HORIZON_ORDER:
            discovery_global = _extract_global_row(
                discovery_analysis["global_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            validation_global = _extract_global_row(
                validation_analysis["global_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            discovery_pairwise = _extract_pairwise_row(
                discovery_analysis["pairwise_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )
            validation_pairwise = _extract_pairwise_row(
                validation_analysis["pairwise_significance_df"],
                ranking_feature=ranking_feature,
                horizon_key=horizon_key,
            )

            discovery_diff = _as_float(
                discovery_global["q1_minus_q10_mean"] if discovery_global is not None else None
            )
            validation_diff = _as_float(
                validation_global["q1_minus_q10_mean"] if validation_global is not None else None
            )
            direction = _direction_from_difference(discovery_diff)
            aligned_discovery_diff = abs(discovery_diff) if discovery_diff is not None else None
            aligned_validation_diff = _aligned_difference(
                validation_diff,
                direction=direction,
            )
            records.append(
                {
                    "ranking_feature": ranking_feature,
                    "ranking_feature_label": _RANKING_FEATURE_LABEL_MAP[ranking_feature],
                    "feature_family": feature_family,
                    "horizon_key": horizon_key,
                    "discovery_direction": direction,
                    "discovery_q1_mean": _as_float(discovery_global["q1_mean"] if discovery_global is not None else None),
                    "discovery_q10_mean": _as_float(discovery_global["q10_mean"] if discovery_global is not None else None),
                    "discovery_q1_minus_q10_mean": discovery_diff,
                    "discovery_friedman_p_value": _as_float(discovery_global["friedman_p_value"] if discovery_global is not None else None),
                    "discovery_kendalls_w": _as_float(discovery_global["kendalls_w"] if discovery_global is not None else None),
                    "discovery_q1_q10_paired_t_p_value": _as_float(discovery_pairwise["paired_t_p_value_holm"] if discovery_pairwise is not None else None),
                    "discovery_q1_q10_wilcoxon_p_value": _as_float(discovery_pairwise["wilcoxon_p_value_holm"] if discovery_pairwise is not None else None),
                    "validation_q1_mean": _as_float(validation_global["q1_mean"] if validation_global is not None else None),
                    "validation_q10_mean": _as_float(validation_global["q10_mean"] if validation_global is not None else None),
                    "validation_q1_minus_q10_mean": validation_diff,
                    "validation_aligned_q1_minus_q10_mean": aligned_validation_diff,
                    "validation_friedman_p_value": _as_float(validation_global["friedman_p_value"] if validation_global is not None else None),
                    "validation_kendalls_w": _as_float(validation_global["kendalls_w"] if validation_global is not None else None),
                    "validation_q1_q10_paired_t_p_value": _as_float(validation_pairwise["paired_t_p_value_holm"] if validation_pairwise is not None else None),
                    "validation_q1_q10_wilcoxon_p_value": _as_float(validation_pairwise["wilcoxon_p_value_holm"] if validation_pairwise is not None else None),
                    "direction_consistent": bool(aligned_validation_diff is not None and aligned_validation_diff > 0),
                    "robustness_score": _robustness_score(
                        aligned_discovery_diff,
                        aligned_validation_diff,
                    ),
                }
            )

    feature_selection_df = _sort_frame(pd.DataFrame.from_records(records))
    if feature_selection_df.empty:
        return feature_selection_df, feature_selection_df

    sort_df = feature_selection_df.copy()
    sort_df["_robustness_score"] = pd.to_numeric(
        sort_df["robustness_score"], errors="coerce"
    ).fillna(float("-inf"))
    sort_df["_validation_p"] = pd.to_numeric(
        sort_df["validation_q1_q10_paired_t_p_value"], errors="coerce"
    ).fillna(1.0)
    sort_df["_validation_global_p"] = pd.to_numeric(
        sort_df["validation_friedman_p_value"], errors="coerce"
    ).fillna(1.0)
    sort_df = sort_df.sort_values(
        [
            "feature_family",
            "horizon_key",
            "_robustness_score",
            "_validation_p",
            "_validation_global_p",
        ],
        ascending=[True, True, False, True, True],
    )
    selected_feature_df = (
        sort_df.groupby(["feature_family", "horizon_key"], as_index=False)
        .head(1)
        .drop(columns=["_robustness_score", "_validation_p", "_validation_global_p"])
        .reset_index(drop=True)
    )
    return feature_selection_df, _sort_frame(selected_feature_df)


def _build_composite_candidates(
    event_panel_df: pd.DataFrame,
    *,
    selected_feature_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, pd.DataFrame]]]:
    selection_lookup = {
        (row["feature_family"], row["horizon_key"]): row
        for row in selected_feature_df.to_dict(orient="records")
    }
    candidate_records: list[dict[str, Any]] = []
    selected_combo_analyses: dict[str, dict[str, pd.DataFrame]] = {}
    candidate_rows_by_horizon: dict[HorizonKey, list[dict[str, Any]]] = {
        horizon_key: [] for horizon_key in HORIZON_ORDER
    }
    for horizon_key in HORIZON_ORDER:
        price_row = selection_lookup.get(("price", horizon_key))
        volume_row = selection_lookup.get(("volume", horizon_key))
        if price_row is None or volume_row is None:
            continue

        price_feature = str(price_row["ranking_feature"])
        price_direction = str(price_row["discovery_direction"])
        volume_feature = str(volume_row["ranking_feature"])
        volume_direction = str(volume_row["discovery_direction"])

        for score_method in COMPOSITE_METHOD_ORDER:
            composite_ranked_panel_df = _build_composite_ranked_panel(
                event_panel_df,
                price_feature=price_feature,
                price_direction=price_direction,
                volume_feature=volume_feature,
                volume_direction=volume_direction,
                score_method=score_method,
            )
            if composite_ranked_panel_df.empty:
                continue

            composite_name = str(composite_ranked_panel_df["ranking_feature"].iloc[0])
            discovery_analysis = _analyze_ranked_panel(
                _filter_df_by_date_split(
                    composite_ranked_panel_df,
                    split_name="discovery",
                )
            )
            validation_analysis = _analyze_ranked_panel(
                _filter_df_by_date_split(
                    composite_ranked_panel_df,
                    split_name="validation",
                )
            )
            discovery_global = _extract_global_row(
                discovery_analysis["global_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            validation_global = _extract_global_row(
                validation_analysis["global_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            discovery_pairwise = _extract_pairwise_row(
                discovery_analysis["pairwise_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            validation_pairwise = _extract_pairwise_row(
                validation_analysis["pairwise_significance_df"],
                ranking_feature=composite_name,
                horizon_key=horizon_key,
            )
            discovery_diff = _as_float(
                discovery_global["q1_minus_q10_mean"] if discovery_global is not None else None
            )
            validation_diff = _as_float(
                validation_global["q1_minus_q10_mean"] if validation_global is not None else None
            )
            candidate_record = {
                "selected_horizon_key": horizon_key,
                "ranking_feature": composite_name,
                "ranking_feature_label": composite_ranked_panel_df["ranking_feature_label"].iloc[0],
                "price_feature": price_feature,
                "price_feature_label": _RANKING_FEATURE_LABEL_MAP[price_feature],
                "price_direction": price_direction,
                "volume_feature": volume_feature,
                "volume_feature_label": _RANKING_FEATURE_LABEL_MAP[volume_feature],
                "volume_direction": volume_direction,
                "score_method": score_method,
                "discovery_q1_mean": _as_float(discovery_global["q1_mean"] if discovery_global is not None else None),
                "discovery_q10_mean": _as_float(discovery_global["q10_mean"] if discovery_global is not None else None),
                "discovery_q1_minus_q10_mean": discovery_diff,
                "discovery_friedman_p_value": _as_float(discovery_global["friedman_p_value"] if discovery_global is not None else None),
                "discovery_q1_q10_paired_t_p_value": _as_float(discovery_pairwise["paired_t_p_value_holm"] if discovery_pairwise is not None else None),
                "validation_q1_mean": _as_float(validation_global["q1_mean"] if validation_global is not None else None),
                "validation_q10_mean": _as_float(validation_global["q10_mean"] if validation_global is not None else None),
                "validation_q1_minus_q10_mean": validation_diff,
                "validation_friedman_p_value": _as_float(validation_global["friedman_p_value"] if validation_global is not None else None),
                "validation_q1_q10_paired_t_p_value": _as_float(validation_pairwise["paired_t_p_value_holm"] if validation_pairwise is not None else None),
                "validation_q1_q10_wilcoxon_p_value": _as_float(validation_pairwise["wilcoxon_p_value_holm"] if validation_pairwise is not None else None),
                "direction_consistent": bool(
                    discovery_diff is not None
                    and validation_diff is not None
                    and discovery_diff > 0
                    and validation_diff > 0
                ),
                "robustness_score": _robustness_score(
                    discovery_diff,
                    validation_diff,
                ),
            }
            candidate_records.append(candidate_record)
            candidate_rows_by_horizon[horizon_key].append(candidate_record)
            selected_combo_analyses[composite_name] = _analyze_ranked_panel(
                composite_ranked_panel_df
            )

    composite_candidate_df = pd.DataFrame.from_records(candidate_records)
    if composite_candidate_df.empty:
        return composite_candidate_df, composite_candidate_df, {}

    selected_records: list[dict[str, Any]] = []
    for horizon_key in HORIZON_ORDER:
        horizon_candidates_df = pd.DataFrame.from_records(candidate_rows_by_horizon[horizon_key])
        horizon_candidates_df["_robustness_score"] = pd.to_numeric(
            horizon_candidates_df["robustness_score"], errors="coerce"
        ).fillna(float("-inf"))
        horizon_candidates_df["_validation_p"] = pd.to_numeric(
            horizon_candidates_df["validation_q1_q10_paired_t_p_value"],
            errors="coerce",
        ).fillna(1.0)
        horizon_candidates_df["_validation_global_p"] = pd.to_numeric(
            horizon_candidates_df["validation_friedman_p_value"], errors="coerce"
        ).fillna(1.0)
        horizon_candidates_df = horizon_candidates_df.sort_values(
            ["_robustness_score", "_validation_p", "_validation_global_p"],
            ascending=[False, True, True],
        )
        selected_records.append(
            horizon_candidates_df.iloc[0]
            .drop(labels=["_robustness_score", "_validation_p", "_validation_global_p"])
            .to_dict()
        )

    selected_composite_df = _sort_frame(pd.DataFrame.from_records(selected_records))
    return _sort_frame(composite_candidate_df), selected_composite_df, selected_combo_analyses


def _collect_selected_composite_tables(
    *,
    selected_composite_df: pd.DataFrame,
    selected_combo_analyses: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    analysis_table_names = (
        "ranking_feature_summary_df",
        "decile_future_summary_df",
        "daily_group_means_df",
        "global_significance_df",
        "pairwise_significance_df",
    )
    frames_by_name: dict[str, list[pd.DataFrame]] = {
        table_name: [] for table_name in analysis_table_names
    }

    if selected_composite_df.empty:
        return {table_name: pd.DataFrame() for table_name in analysis_table_names}

    metadata_columns = [
        "selected_horizon_key",
        "price_feature",
        "price_feature_label",
        "price_direction",
        "volume_feature",
        "volume_feature_label",
        "volume_direction",
        "score_method",
    ]
    for row in selected_composite_df.to_dict(orient="records"):
        ranking_feature = str(row["ranking_feature"])
        analysis = selected_combo_analyses.get(ranking_feature)
        if analysis is None:
            continue
        for table_name in analysis_table_names:
            frame = analysis[table_name].copy()
            if frame.empty:
                continue
            for column in metadata_columns:
                frame[column] = row[column]
            frames_by_name[table_name].append(frame)

    return {
        table_name: _sort_frame(pd.concat(frames, ignore_index=True))
        if frames
        else pd.DataFrame()
        for table_name, frames in frames_by_name.items()
    }


def get_topix100_sma_ratio_rank_future_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    """Return the available date range for latest TOPIX100 constituents."""
    with _open_analysis_connection(db_path) as ctx:
        return _query_universe_date_range(ctx.connection, universe_key="topix100")


def get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    """Return the available date range for latest PRIME ex TOPIX500 constituents."""
    with _open_analysis_connection(db_path) as ctx:
        return _query_universe_date_range(
            ctx.connection,
            universe_key="prime_ex_topix500",
        )


def _run_sma_ratio_rank_future_close_research(
    db_path: str,
    *,
    universe_key: UniverseKey,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    if lookback_years <= 0:
        raise ValueError("lookback_years must be positive")
    if min_constituents_per_day <= 0:
        raise ValueError("min_constituents_per_day must be positive")
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be less than or equal to end_date")

    with _open_analysis_connection(db_path) as ctx:
        history_df = _query_universe_stock_history(
            ctx.connection,
            universe_key=universe_key,
            end_date=end_date,
        )

    if history_df.empty:
        raise ValueError(
            f"No latest {_UNIVERSE_LABEL_MAP[universe_key]} stock_data rows were found"
        )

    available_start_date = str(history_df["date"].min())
    available_end_date = str(history_df["date"].max())
    default_start = _default_start_date(
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        lookback_years=lookback_years,
    )
    resolved_start_date = start_date or default_start
    resolved_end_date = end_date or available_end_date

    event_panel_df = _enrich_event_panel(
        history_df,
        analysis_start_date=resolved_start_date,
        analysis_end_date=resolved_end_date,
        min_constituents_per_day=min_constituents_per_day,
    )
    ranked_panel_df = _build_ranked_panel(event_panel_df)
    full_analysis = _analyze_ranked_panel(ranked_panel_df)
    nested_volume_split_panel_df = _build_nested_volume_split_panel(event_panel_df)
    nested_volume_horizon_panel_df = _build_nested_volume_horizon_panel(
        nested_volume_split_panel_df
    )
    nested_volume_split_daily_means_df = _build_nested_volume_daily_means(
        nested_volume_horizon_panel_df
    )
    nested_volume_split_summary_df = _summarize_nested_volume_split(
        nested_volume_split_daily_means_df
    )
    nested_volume_split_global_significance_df = (
        _build_nested_volume_global_significance(nested_volume_split_daily_means_df)
    )
    nested_volume_split_pairwise_significance_df = (
        _build_nested_volume_pairwise_significance(nested_volume_split_daily_means_df)
    )
    nested_volume_split_interaction_df = _build_nested_volume_interaction(
        nested_volume_split_daily_means_df
    )
    q1_q10_volume_split_panel_df = _build_q1_q10_volume_split_panel(event_panel_df)
    q1_q10_volume_horizon_panel_df = _build_q1_q10_volume_horizon_panel(
        q1_q10_volume_split_panel_df
    )
    q1_q10_volume_split_daily_means_df = _build_q1_q10_volume_daily_means(
        q1_q10_volume_horizon_panel_df
    )
    q1_q10_volume_split_summary_df = _summarize_q1_q10_volume_split(
        q1_q10_volume_split_daily_means_df
    )
    q1_q10_volume_split_global_significance_df = (
        _build_q1_q10_volume_global_significance(q1_q10_volume_split_daily_means_df)
    )
    q1_q10_volume_split_pairwise_significance_df = (
        _build_q1_q10_volume_pairwise_significance(q1_q10_volume_split_daily_means_df)
    )
    q1_q10_volume_split_interaction_df = _build_q1_q10_volume_interaction(
        q1_q10_volume_split_daily_means_df
    )
    q10_middle_volume_split_panel_df = _build_q10_middle_volume_split_panel(
        event_panel_df
    )
    q10_middle_volume_horizon_panel_df = _build_q10_middle_volume_horizon_panel(
        q10_middle_volume_split_panel_df
    )
    q10_middle_volume_split_daily_means_df = _build_q10_middle_volume_daily_means(
        q10_middle_volume_horizon_panel_df
    )
    q10_middle_volume_split_summary_df = _summarize_q10_middle_volume_split(
        q10_middle_volume_split_daily_means_df
    )
    q10_middle_volume_split_pairwise_significance_df = (
        _build_q10_middle_volume_pairwise_significance(
            q10_middle_volume_split_daily_means_df
        )
    )
    q10_low_hypothesis_df = _build_q10_low_hypothesis(
        q10_middle_volume_split_pairwise_significance_df
    )
    discovery_analysis = _analyze_ranked_panel(
        _filter_df_by_date_split(ranked_panel_df, split_name="discovery")
    )
    validation_analysis = _analyze_ranked_panel(
        _filter_df_by_date_split(ranked_panel_df, split_name="validation")
    )
    feature_selection_df, selected_feature_df = _build_feature_selection(
        discovery_analysis=discovery_analysis,
        validation_analysis=validation_analysis,
    )
    composite_candidate_df, selected_composite_df, selected_combo_analyses = (
        _build_composite_candidates(
            event_panel_df,
            selected_feature_df=selected_feature_df,
        )
    )
    selected_composite_tables = _collect_selected_composite_tables(
        selected_composite_df=selected_composite_df,
        selected_combo_analyses=selected_combo_analyses,
    )

    analysis_start = str(event_panel_df["date"].min()) if not event_panel_df.empty else None
    analysis_end = str(event_panel_df["date"].max()) if not event_panel_df.empty else None

    return Topix100SmaRatioRankFutureCloseResearchResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        universe_key=universe_key,
        universe_label=_UNIVERSE_LABEL_MAP[universe_key],
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        default_start_date=default_start,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        universe_constituent_count=int(history_df["code"].nunique()),
        topix100_constituent_count=int(history_df["code"].nunique()),
        stock_day_count=int(len(event_panel_df)),
        ranked_event_count=int(len(ranked_panel_df)),
        valid_date_count=int(event_panel_df["date"].nunique())
        if not event_panel_df.empty
        else 0,
        discovery_end_date=_DISCOVERY_END_DATE,
        validation_start_date=_VALIDATION_START_DATE,
        event_panel_df=event_panel_df,
        ranked_panel_df=ranked_panel_df,
        ranking_feature_summary_df=full_analysis["ranking_feature_summary_df"],
        decile_future_summary_df=full_analysis["decile_future_summary_df"],
        daily_group_means_df=full_analysis["daily_group_means_df"],
        global_significance_df=full_analysis["global_significance_df"],
        pairwise_significance_df=full_analysis["pairwise_significance_df"],
        extreme_vs_middle_summary_df=full_analysis["extreme_vs_middle_summary_df"],
        extreme_vs_middle_daily_means_df=full_analysis[
            "extreme_vs_middle_daily_means_df"
        ],
        extreme_vs_middle_significance_df=full_analysis[
            "extreme_vs_middle_significance_df"
        ],
        nested_volume_split_panel_df=nested_volume_split_panel_df,
        nested_volume_split_summary_df=nested_volume_split_summary_df,
        nested_volume_split_daily_means_df=nested_volume_split_daily_means_df,
        nested_volume_split_global_significance_df=nested_volume_split_global_significance_df,
        nested_volume_split_pairwise_significance_df=nested_volume_split_pairwise_significance_df,
        nested_volume_split_interaction_df=nested_volume_split_interaction_df,
        q1_q10_volume_split_panel_df=q1_q10_volume_split_panel_df,
        q1_q10_volume_split_summary_df=q1_q10_volume_split_summary_df,
        q1_q10_volume_split_daily_means_df=q1_q10_volume_split_daily_means_df,
        q1_q10_volume_split_global_significance_df=q1_q10_volume_split_global_significance_df,
        q1_q10_volume_split_pairwise_significance_df=q1_q10_volume_split_pairwise_significance_df,
        q1_q10_volume_split_interaction_df=q1_q10_volume_split_interaction_df,
        q10_middle_volume_split_panel_df=q10_middle_volume_split_panel_df,
        q10_middle_volume_split_summary_df=q10_middle_volume_split_summary_df,
        q10_middle_volume_split_daily_means_df=q10_middle_volume_split_daily_means_df,
        q10_middle_volume_split_pairwise_significance_df=q10_middle_volume_split_pairwise_significance_df,
        q10_low_hypothesis_df=q10_low_hypothesis_df,
        feature_selection_df=feature_selection_df,
        selected_feature_df=selected_feature_df,
        composite_candidate_df=composite_candidate_df,
        selected_composite_df=selected_composite_df,
        selected_composite_ranking_summary_df=selected_composite_tables[
            "ranking_feature_summary_df"
        ],
        selected_composite_future_summary_df=selected_composite_tables[
            "decile_future_summary_df"
        ],
        selected_composite_daily_group_means_df=selected_composite_tables[
            "daily_group_means_df"
        ],
        selected_composite_global_significance_df=selected_composite_tables[
            "global_significance_df"
        ],
        selected_composite_pairwise_significance_df=selected_composite_tables[
            "pairwise_significance_df"
        ],
    )


def run_topix100_sma_ratio_rank_future_close_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int = _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    """
    Run TOPIX100 SMA-ratio rank vs future close research from market.duckdb.

    Each SMA ratio is ranked independently within the same-day TOPIX100
    universe. Significance is computed on date-level decile means.
    """

    return _run_sma_ratio_rank_future_close_research(
        db_path,
        universe_key="topix100",
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
    )


def run_prime_ex_topix500_sma_ratio_rank_future_close_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int = _DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    """
    Run PRIME ex TOPIX500 SMA-ratio rank vs future close research from market.duckdb.

    Each SMA ratio is ranked independently within the same-day PRIME ex TOPIX500
    universe. Significance is computed on date-level decile means.
    """

    return _run_sma_ratio_rank_future_close_research(
        db_path,
        universe_key="prime_ex_topix500",
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
    )
