"""
TOPIX100 SMA-ratio regime conditioning research analytics.

This module reuses the TOPIX100 price/volume SMA research panel and conditions
the primary price/volume bucket discussion on same-day TOPIX close-return and
NT-ratio-return regimes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NtRatioBucketKey,
    NtRatioReturnStats,
)
from src.domains.analytics.topix_regime_conditioning_core import (
    DEFAULT_SIGMA_THRESHOLD_1 as _CORE_DEFAULT_SIGMA_THRESHOLD_1,
    DEFAULT_SIGMA_THRESHOLD_2 as _CORE_DEFAULT_SIGMA_THRESHOLD_2,
    HYPOTHESIS_SPECS as _CORE_HYPOTHESIS_SPECS,
    REGIME_GROUP_LABEL_MAP as _CORE_REGIME_GROUP_LABEL_MAP,
    REGIME_GROUP_ORDER as _CORE_REGIME_GROUP_ORDER,
    REGIME_LABEL_MAP as _CORE_REGIME_LABEL_MAP,
    REGIME_TYPE_ORDER as _CORE_REGIME_TYPE_ORDER,
    _build_horizon_panel as _core_build_horizon_panel,
    _build_nt_ratio_stats as _core_build_nt_ratio_stats,
    _build_regime_assignments_df as _core_build_regime_assignments_df,
    _build_regime_daily_means as _core_build_regime_daily_means,
    _build_regime_day_counts as _core_build_regime_day_counts,
    _build_regime_group_daily_means as _core_build_regime_group_daily_means,
    _build_regime_group_day_counts as _core_build_regime_group_day_counts,
    _build_regime_group_hypothesis as _core_build_regime_group_hypothesis,
    _build_regime_group_pairwise_significance as _core_build_regime_group_pairwise_significance,
    _build_regime_hypothesis as _core_build_regime_hypothesis,
    _build_regime_market_df as _core_build_regime_market_df,
    _build_regime_pairwise_significance as _core_build_regime_pairwise_significance,
    _build_topix_close_stats as _core_build_topix_close_stats,
    _bucket_nt_ratio_return as _core_bucket_nt_ratio_return,
    _bucket_topix_close_return as _core_bucket_topix_close_return,
    _collapse_regime_bucket as _core_collapse_regime_bucket,
    _query_market_regime_history as _core_query_market_regime_history,
    _regime_bucket_sort_index as _core_regime_bucket_sort_index,
    _sort_frame as _core_sort_frame,
    _summarize_regime_daily_means as _core_summarize_regime_daily_means,
    _summarize_regime_group_daily_means as _core_summarize_regime_group_daily_means,
)
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    QUARTILE_ORDER,
    Topix100SmaRatioRankFutureCloseResearchResult,
    run_topix100_sma_ratio_rank_future_close_research,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    CloseBucketKey,
    TopixCloseReturnStats,
    _open_analysis_connection,
)

HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
RegimeType = Literal["topix_close", "nt_ratio"]
RegimeGroupKey = Literal["weak", "neutral", "strong"]
PriceBucketKey = Literal["q1", "q10", "middle"]
VolumeBucketKey = Literal["volume_high", "volume_low"]
CombinedBucketKey = Literal[
    "q1_volume_high",
    "q1_volume_low",
    "q10_volume_high",
    "q10_volume_low",
    "middle_volume_high",
    "middle_volume_low",
]

REGIME_TYPE_ORDER: tuple[RegimeType, ...] = _CORE_REGIME_TYPE_ORDER
REGIME_LABEL_MAP: dict[RegimeType, str] = _CORE_REGIME_LABEL_MAP
REGIME_GROUP_ORDER: tuple[RegimeGroupKey, ...] = _CORE_REGIME_GROUP_ORDER
REGIME_GROUP_LABEL_MAP: dict[RegimeGroupKey, str] = _CORE_REGIME_GROUP_LABEL_MAP
PRICE_BUCKET_ORDER: tuple[PriceBucketKey, ...] = ("q1", "q10", "middle")
PRICE_BUCKET_LABEL_MAP: dict[PriceBucketKey, str] = {
    "q1": "Q1",
    "q10": "Q10",
    "middle": "Q4 + Q5 + Q6",
}
PRICE_BUCKET_DECILES: dict[PriceBucketKey, tuple[str, ...]] = {
    "q1": ("Q1",),
    "q10": ("Q10",),
    "middle": ("Q4", "Q5", "Q6"),
}
VOLUME_BUCKET_LABEL_MAP: dict[VolumeBucketKey, str] = {
    "volume_high": "Volume 20 / 80 High Half",
    "volume_low": "Volume 20 / 80 Low Half",
}
COMBINED_BUCKET_ORDER: tuple[CombinedBucketKey, ...] = (
    "q1_volume_high",
    "q1_volume_low",
    "q10_volume_high",
    "q10_volume_low",
    "middle_volume_high",
    "middle_volume_low",
)
COMBINED_BUCKET_LABEL_MAP: dict[CombinedBucketKey, str] = {
    "q1_volume_high": "Q1 x Volume High",
    "q1_volume_low": "Q1 x Volume Low",
    "q10_volume_high": "Q10 x Volume High",
    "q10_volume_low": "Q10 x Volume Low",
    "middle_volume_high": "Middle x Volume High",
    "middle_volume_low": "Middle x Volume Low",
}
PRIMARY_PRICE_FEATURE = "price_sma_20_80"
PRIMARY_VOLUME_FEATURE = "volume_sma_20_80"
DEFAULT_SIGMA_THRESHOLD_1 = _CORE_DEFAULT_SIGMA_THRESHOLD_1
DEFAULT_SIGMA_THRESHOLD_2 = _CORE_DEFAULT_SIGMA_THRESHOLD_2
_HORIZON_DAY_MAP: dict[HorizonKey, int] = {
    "t_plus_1": 1,
    "t_plus_5": 5,
    "t_plus_10": 10,
}
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
HYPOTHESIS_SPECS: tuple[tuple[CombinedBucketKey, CombinedBucketKey, str], ...] = (
    _CORE_HYPOTHESIS_SPECS
)


@dataclass(frozen=True)
class Topix100SmaRatioRegimeConditioningResearchResult:
    db_path: str
    source_mode: str
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    sigma_threshold_1: float
    sigma_threshold_2: float
    universe_constituent_count: int
    valid_date_count: int
    topix_close_stats: TopixCloseReturnStats | None
    nt_ratio_stats: NtRatioReturnStats | None
    regime_market_df: pd.DataFrame
    regime_day_counts_df: pd.DataFrame
    regime_group_day_counts_df: pd.DataFrame
    split_panel_df: pd.DataFrame
    horizon_panel_df: pd.DataFrame
    regime_daily_means_df: pd.DataFrame
    regime_summary_df: pd.DataFrame
    regime_pairwise_significance_df: pd.DataFrame
    regime_hypothesis_df: pd.DataFrame
    regime_group_daily_means_df: pd.DataFrame
    regime_group_summary_df: pd.DataFrame
    regime_group_pairwise_significance_df: pd.DataFrame
    regime_group_hypothesis_df: pd.DataFrame


def _holm_adjust(p_values: list[float | None]) -> list[float | None]:
    from src.domains.analytics.topix_rank_future_close_core import _holm_adjust as _core

    return _core(p_values)


def _safe_paired_t_test(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    from src.domains.analytics.topix_rank_future_close_core import (
        _safe_paired_t_test as _core,
    )

    return _core(left, right)


def _safe_wilcoxon(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    from src.domains.analytics.topix_rank_future_close_core import (
        _safe_wilcoxon as _core,
    )

    return _core(left, right)


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    return _core_sort_frame(df)


def _regime_bucket_sort_index(
    *,
    regime_type: str | None,
    regime_bucket_key: str | None,
) -> int | None:
    return _core_regime_bucket_sort_index(
        regime_type=regime_type,
        regime_bucket_key=regime_bucket_key,
    )


def _collapse_regime_bucket(
    *,
    regime_type: RegimeType,
    regime_bucket_key: str | None,
) -> RegimeGroupKey | None:
    return _core_collapse_regime_bucket(
        regime_type=regime_type,
        regime_bucket_key=regime_bucket_key,
    )


def _query_market_regime_history(
    conn: Any,
    *,
    end_date: str | None,
) -> pd.DataFrame:
    return _core_query_market_regime_history(conn, end_date=end_date)


def _build_topix_close_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> TopixCloseReturnStats | None:
    return _core_build_topix_close_stats(
        market_df,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )


def _build_nt_ratio_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> NtRatioReturnStats | None:
    return _core_build_nt_ratio_stats(
        market_df,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )


def _bucket_topix_close_return(
    value: float | None,
    *,
    stats: TopixCloseReturnStats | None,
) -> CloseBucketKey | None:
    return _core_bucket_topix_close_return(value, stats=stats)


def _bucket_nt_ratio_return(
    value: float | None,
    *,
    stats: NtRatioReturnStats | None,
) -> NtRatioBucketKey | None:
    return _core_bucket_nt_ratio_return(value, stats=stats)


def _build_regime_market_df(
    market_df: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> tuple[pd.DataFrame, TopixCloseReturnStats | None, NtRatioReturnStats | None]:
    return _core_build_regime_market_df(
        market_df,
        start_date=start_date,
        end_date=end_date,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )


def _build_regime_assignments_df(regime_market_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_regime_assignments_df(regime_market_df)


def _build_regime_day_counts(regime_assignments_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_regime_day_counts(regime_assignments_df)


def _build_regime_group_day_counts(regime_assignments_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_regime_group_day_counts(regime_assignments_df)


def _build_price_volume_split_panel(
    event_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    panel_df = event_panel_df.copy()
    panel_df["price_rank_desc"] = (
        panel_df.groupby("date")[PRIMARY_PRICE_FEATURE]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["price_decile_index"] = (
        ((panel_df["price_rank_desc"] - 1) * len(QUARTILE_ORDER))
        // panel_df["date_constituent_count"]
    ) + 1
    panel_df["price_decile_index"] = panel_df["price_decile_index"].clip(
        1, len(QUARTILE_ORDER)
    )
    panel_df["price_decile"] = panel_df["price_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(QUARTILE_ORDER) + 1)}
    )
    panel_df["price_bucket"] = None
    for bucket_key, deciles in PRICE_BUCKET_DECILES.items():
        panel_df.loc[panel_df["price_decile"].isin(deciles), "price_bucket"] = bucket_key
    panel_df = panel_df.dropna(subset=["price_bucket"]).copy()
    if panel_df.empty:
        return pd.DataFrame()

    panel_df["price_bucket"] = panel_df["price_bucket"].astype(str)
    panel_df["price_bucket_label"] = panel_df["price_bucket"].map(PRICE_BUCKET_LABEL_MAP)
    panel_df["price_bucket_size"] = panel_df.groupby(["date", "price_bucket"])[
        "code"
    ].transform("size")
    panel_df["volume_rank_desc_within_price_bucket"] = (
        panel_df.groupby(["date", "price_bucket"])[PRIMARY_VOLUME_FEATURE]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    panel_df["volume_bucket_index"] = (
        ((panel_df["volume_rank_desc_within_price_bucket"] - 1) * 2)
        // panel_df["price_bucket_size"]
    ) + 1
    panel_df["volume_bucket_index"] = panel_df["volume_bucket_index"].clip(1, 2)
    panel_df["volume_bucket"] = panel_df["volume_bucket_index"].map(
        {1: "volume_high", 2: "volume_low"}
    )
    panel_df["volume_bucket_label"] = panel_df["volume_bucket"].map(
        VOLUME_BUCKET_LABEL_MAP
    )
    panel_df["combined_bucket"] = panel_df["price_bucket"] + "_" + panel_df["volume_bucket"]
    panel_df["combined_bucket_label"] = panel_df["combined_bucket"].map(
        COMBINED_BUCKET_LABEL_MAP
    )
    panel_df["price_feature"] = PRIMARY_PRICE_FEATURE
    panel_df["volume_feature"] = PRIMARY_VOLUME_FEATURE
    return _sort_frame(panel_df.reset_index(drop=True))


def _build_horizon_panel(split_panel_df: pd.DataFrame) -> pd.DataFrame:
    return _core_build_horizon_panel(split_panel_df)


def _build_regime_daily_means(
    horizon_panel_df: pd.DataFrame,
    regime_assignments_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_build_regime_daily_means(horizon_panel_df, regime_assignments_df)


def _summarize_regime_daily_means(regime_daily_means_df: pd.DataFrame) -> pd.DataFrame:
    return _core_summarize_regime_daily_means(regime_daily_means_df)


def _build_regime_group_daily_means(
    horizon_panel_df: pd.DataFrame,
    regime_assignments_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_build_regime_group_daily_means(
        horizon_panel_df,
        regime_assignments_df,
    )


def _summarize_regime_group_daily_means(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_summarize_regime_group_daily_means(regime_group_daily_means_df)


def _aligned_regime_pivot(
    regime_daily_means_df: pd.DataFrame,
    *,
    regime_type: RegimeType,
    regime_bucket_key: str,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = regime_daily_means_df[
        (regime_daily_means_df["regime_type"] == regime_type)
        & (regime_daily_means_df["regime_bucket_key"] == regime_bucket_key)
        & (regime_daily_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _aligned_regime_group_pivot(
    regime_group_daily_means_df: pd.DataFrame,
    *,
    regime_type: RegimeType,
    regime_group_key: RegimeGroupKey,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = regime_group_daily_means_df[
        (regime_group_daily_means_df["regime_type"] == regime_type)
        & (regime_group_daily_means_df["regime_group_key"] == regime_group_key)
        & (regime_group_daily_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_regime_pairwise_significance(
    regime_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_build_regime_pairwise_significance(regime_daily_means_df)


def _build_regime_group_pairwise_significance(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_build_regime_group_pairwise_significance(regime_group_daily_means_df)


def _build_regime_hypothesis(
    regime_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_build_regime_hypothesis(regime_pairwise_significance_df)


def _build_regime_group_hypothesis(
    regime_group_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    return _core_build_regime_group_hypothesis(
        regime_group_pairwise_significance_df
    )


def run_topix100_sma_ratio_regime_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
    sigma_threshold_1: float = DEFAULT_SIGMA_THRESHOLD_1,
    sigma_threshold_2: float = DEFAULT_SIGMA_THRESHOLD_2,
) -> Topix100SmaRatioRegimeConditioningResearchResult:
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")

    base_result: Topix100SmaRatioRankFutureCloseResearchResult = (
        run_topix100_sma_ratio_rank_future_close_research(
            db_path,
            start_date=start_date,
            end_date=end_date,
            lookback_years=lookback_years,
            min_constituents_per_day=min_constituents_per_day,
        )
    )

    with _open_analysis_connection(db_path) as ctx:
        raw_market_df = _query_market_regime_history(
            ctx.connection,
            end_date=base_result.analysis_end_date,
        )
        regime_market_df, topix_close_stats, nt_ratio_stats = _build_regime_market_df(
            raw_market_df,
            start_date=base_result.analysis_start_date,
            end_date=base_result.analysis_end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )

    regime_assignments_df = _build_regime_assignments_df(regime_market_df)
    regime_day_counts_df = _build_regime_day_counts(regime_assignments_df)
    regime_group_day_counts_df = _build_regime_group_day_counts(regime_assignments_df)
    split_panel_df = _build_price_volume_split_panel(base_result.event_panel_df)
    horizon_panel_df = _build_horizon_panel(split_panel_df)
    regime_daily_means_df = _build_regime_daily_means(
        horizon_panel_df,
        regime_assignments_df,
    )
    regime_summary_df = _summarize_regime_daily_means(regime_daily_means_df)
    regime_pairwise_significance_df = _build_regime_pairwise_significance(
        regime_daily_means_df
    )
    regime_hypothesis_df = _build_regime_hypothesis(regime_pairwise_significance_df)
    regime_group_daily_means_df = _build_regime_group_daily_means(
        horizon_panel_df,
        regime_assignments_df,
    )
    regime_group_summary_df = _summarize_regime_group_daily_means(
        regime_group_daily_means_df
    )
    regime_group_pairwise_significance_df = _build_regime_group_pairwise_significance(
        regime_group_daily_means_df
    )
    regime_group_hypothesis_df = _build_regime_group_hypothesis(
        regime_group_pairwise_significance_df
    )

    return Topix100SmaRatioRegimeConditioningResearchResult(
        db_path=db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        universe_constituent_count=base_result.universe_constituent_count,
        valid_date_count=base_result.valid_date_count,
        topix_close_stats=topix_close_stats,
        nt_ratio_stats=nt_ratio_stats,
        regime_market_df=regime_market_df,
        regime_day_counts_df=regime_day_counts_df,
        regime_group_day_counts_df=regime_group_day_counts_df,
        split_panel_df=split_panel_df,
        horizon_panel_df=horizon_panel_df,
        regime_daily_means_df=regime_daily_means_df,
        regime_summary_df=regime_summary_df,
        regime_pairwise_significance_df=regime_pairwise_significance_df,
        regime_hypothesis_df=regime_hypothesis_df,
        regime_group_daily_means_df=regime_group_daily_means_df,
        regime_group_summary_df=regime_group_summary_df,
        regime_group_pairwise_significance_df=regime_group_pairwise_significance_df,
        regime_group_hypothesis_df=regime_group_hypothesis_df,
    )
