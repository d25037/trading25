"""
TOPIX100 SMA-ratio regime conditioning research analytics.

This module reuses the TOPIX100 price/volume SMA research panel and conditions
the primary price/volume bucket discussion on same-day TOPIX close-return and
NT-ratio-return regimes.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any, Literal

import numpy as np
import pandas as pd
from scipy import stats

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_BUCKET_ORDER,
    NtRatioBucketKey,
    NtRatioReturnStats,
    format_nt_ratio_bucket_label,
)
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    HORIZON_ORDER,
    METRIC_ORDER,
    QUARTILE_ORDER,
    Topix100SmaRatioRankFutureCloseResearchResult,
    run_topix100_sma_ratio_rank_future_close_research,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    CLOSE_BUCKET_ORDER,
    CloseBucketKey,
    TopixCloseReturnStats,
    format_close_bucket_label,
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

REGIME_TYPE_ORDER: tuple[RegimeType, ...] = ("topix_close", "nt_ratio")
REGIME_LABEL_MAP: dict[RegimeType, str] = {
    "topix_close": "TOPIX Close Return",
    "nt_ratio": "NT Ratio Return",
}
REGIME_GROUP_ORDER: tuple[RegimeGroupKey, ...] = ("weak", "neutral", "strong")
REGIME_GROUP_LABEL_MAP: dict[RegimeGroupKey, str] = {
    "weak": "Weak",
    "neutral": "Neutral",
    "strong": "Strong",
}
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
DEFAULT_SIGMA_THRESHOLD_1 = 1.0
DEFAULT_SIGMA_THRESHOLD_2 = 2.0
_HORIZON_DAY_MAP: dict[HorizonKey, int] = {
    "t_plus_1": 1,
    "t_plus_5": 5,
    "t_plus_10": 10,
}
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
HYPOTHESIS_SPECS: tuple[tuple[CombinedBucketKey, CombinedBucketKey, str], ...] = (
    ("q1_volume_high", "q1_volume_low", "Q1 High vs Q1 Low"),
    ("q10_volume_low", "q10_volume_high", "Q10 Low vs Q10 High"),
    ("q1_volume_high", "middle_volume_high", "Q1 High vs Middle High"),
    ("q1_volume_low", "middle_volume_low", "Q1 Low vs Middle Low"),
    ("q10_volume_low", "middle_volume_low", "Q10 Low vs Middle Low"),
    ("q10_volume_low", "middle_volume_high", "Q10 Low vs Middle High"),
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
    indexed = [
        (index, float(p_value))
        for index, p_value in enumerate(p_values)
        if p_value is not None and not pd.isna(p_value)
    ]
    adjusted: list[float | None] = [None] * len(p_values)
    if not indexed:
        return adjusted

    indexed.sort(key=lambda item: item[1])
    running_max = 0.0
    total = len(indexed)
    for rank, (index, p_value) in enumerate(indexed):
        adjusted_value = min(1.0, p_value * (total - rank))
        running_max = max(running_max, adjusted_value)
        adjusted[index] = float(running_max)
    return adjusted


def _safe_paired_t_test(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    diff = left - right
    if len(diff) < 2:
        return None, None
    if np.allclose(diff, 0.0):
        return 0.0, 1.0
    statistic, p_value = stats.ttest_rel(left, right, nan_policy="omit")
    return float(statistic), float(p_value)


def _safe_wilcoxon(
    left: np.ndarray,
    right: np.ndarray,
) -> tuple[float | None, float | None]:
    diff = left - right
    if len(diff) == 0:
        return None, None
    if np.allclose(diff, 0.0):
        return 0.0, 1.0
    try:
        statistic, p_value = stats.wilcoxon(left, right, zero_method="wilcox")
    except ValueError:
        return None, None
    return float(statistic), float(p_value)


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ordered = df.copy()
    if "regime_type" in ordered.columns:
        ordered["_regime_type_order"] = ordered["regime_type"].map(
            {key: index for index, key in enumerate(REGIME_TYPE_ORDER, start=1)}
        )
    if "regime_bucket_key" in ordered.columns:
        ordered["_regime_bucket_order"] = ordered.apply(
            lambda row: _regime_bucket_sort_index(
                regime_type=row.get("regime_type"),
                regime_bucket_key=row.get("regime_bucket_key"),
            ),
            axis=1,
        )
    if "regime_group_key" in ordered.columns:
        ordered["_regime_group_order"] = ordered["regime_group_key"].map(
            {key: index for index, key in enumerate(REGIME_GROUP_ORDER, start=1)}
        )
    if "horizon_key" in ordered.columns:
        ordered["_horizon_order"] = ordered["horizon_key"].map(
            {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
        )
    if "combined_bucket" in ordered.columns:
        ordered["_combined_bucket_order"] = ordered["combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "left_combined_bucket" in ordered.columns:
        ordered["_left_combined_bucket_order"] = ordered["left_combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "right_combined_bucket" in ordered.columns:
        ordered["_right_combined_bucket_order"] = ordered["right_combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "date" in ordered.columns:
        ordered["_date_order"] = pd.to_datetime(ordered["date"], errors="coerce")

    sort_columns = [column for column in ordered.columns if column.startswith("_")]
    if sort_columns:
        ordered = ordered.sort_values(sort_columns, kind="stable").drop(
            columns=sort_columns
        )
    return ordered.reset_index(drop=True)


def _regime_bucket_sort_index(
    *,
    regime_type: str | None,
    regime_bucket_key: str | None,
) -> int | None:
    if regime_type == "topix_close":
        return {
            key: index for index, key in enumerate(CLOSE_BUCKET_ORDER, start=1)
        }.get(regime_bucket_key)
    if regime_type == "nt_ratio":
        return {
            key: index for index, key in enumerate(NT_RATIO_BUCKET_ORDER, start=1)
        }.get(regime_bucket_key)
    return None


def _collapse_regime_bucket(
    *,
    regime_type: RegimeType,
    regime_bucket_key: str | None,
) -> RegimeGroupKey | None:
    if regime_bucket_key is None or pd.isna(regime_bucket_key):
        return None
    if regime_type == "topix_close":
        if regime_bucket_key in {
            "close_le_negative_threshold_2",
            "close_negative_threshold_2_to_1",
        }:
            return "weak"
        if regime_bucket_key == "close_negative_threshold_1_to_threshold_1":
            return "neutral"
        return "strong"
    if regime_type == "nt_ratio":
        if regime_bucket_key in {
            "return_le_mean_minus_2sd",
            "return_mean_minus_2sd_to_minus_1sd",
        }:
            return "weak"
        if regime_bucket_key == "return_mean_minus_1sd_to_plus_1sd":
            return "neutral"
        return "strong"
    return None


def _query_market_regime_history(
    conn: Any,
    *,
    end_date: str | None,
) -> pd.DataFrame:
    date_filter_sql = ""
    params: list[Any] = [_NIKKEI_SYNTHETIC_INDEX_CODE]
    if end_date:
        date_filter_sql = " AND t.date <= ?"
        params.append(end_date)
    return conn.execute(
        f"""
        SELECT
            t.date,
            CAST(t.close AS DOUBLE) AS topix_close,
            CAST(n.close AS DOUBLE) AS n225_close
        FROM topix_data t
        JOIN indices_data n
          ON n.date = t.date
         AND n.code = ?
        WHERE t.close IS NOT NULL
          AND t.close > 0
          AND n.close IS NOT NULL
          AND n.close > 0
          {date_filter_sql}
        ORDER BY t.date
        """,
        params,
    ).fetchdf()


def _build_topix_close_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> TopixCloseReturnStats | None:
    valid = market_df["topix_close_return"].dropna()
    if valid.empty:
        return None
    mean_return = float(valid.mean())
    std_return = float(valid.std(ddof=1))
    if std_return <= 0:
        raise ValueError("topix_close_return standard deviation must be positive")
    return TopixCloseReturnStats(
        sample_count=int(valid.shape[0]),
        mean_return=mean_return,
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        threshold_1=sigma_threshold_1 * std_return,
        threshold_2=sigma_threshold_2 * std_return,
        min_return=float(valid.min()),
        q25_return=float(valid.quantile(0.25)),
        median_return=float(valid.median()),
        q75_return=float(valid.quantile(0.75)),
        max_return=float(valid.max()),
    )


def _build_nt_ratio_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> NtRatioReturnStats | None:
    valid = market_df["nt_ratio_return"].dropna()
    if valid.empty:
        return None
    mean_return = float(valid.mean())
    std_return = float(valid.std(ddof=1))
    if std_return <= 0:
        raise ValueError("nt_ratio_return standard deviation must be positive")
    return NtRatioReturnStats(
        sample_count=int(valid.shape[0]),
        mean_return=mean_return,
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        lower_threshold_2=mean_return - sigma_threshold_2 * std_return,
        lower_threshold_1=mean_return - sigma_threshold_1 * std_return,
        upper_threshold_1=mean_return + sigma_threshold_1 * std_return,
        upper_threshold_2=mean_return + sigma_threshold_2 * std_return,
        min_return=float(valid.min()),
        q25_return=float(valid.quantile(0.25)),
        median_return=float(valid.median()),
        q75_return=float(valid.quantile(0.75)),
        max_return=float(valid.max()),
    )


def _bucket_topix_close_return(
    value: float | None,
    *,
    stats: TopixCloseReturnStats | None,
) -> CloseBucketKey | None:
    if value is None or stats is None or pd.isna(value):
        return None
    if value <= -stats.threshold_2:
        return "close_le_negative_threshold_2"
    if value <= -stats.threshold_1:
        return "close_negative_threshold_2_to_1"
    if value < stats.threshold_1:
        return "close_negative_threshold_1_to_threshold_1"
    if value < stats.threshold_2:
        return "close_threshold_1_to_2"
    return "close_ge_threshold_2"


def _bucket_nt_ratio_return(
    value: float | None,
    *,
    stats: NtRatioReturnStats | None,
) -> NtRatioBucketKey | None:
    if value is None or stats is None or pd.isna(value):
        return None
    if value <= stats.lower_threshold_2:
        return "return_le_mean_minus_2sd"
    if value <= stats.lower_threshold_1:
        return "return_mean_minus_2sd_to_minus_1sd"
    if value < stats.upper_threshold_1:
        return "return_mean_minus_1sd_to_plus_1sd"
    if value < stats.upper_threshold_2:
        return "return_mean_plus_1sd_to_plus_2sd"
    return "return_ge_mean_plus_2sd"


def _build_regime_market_df(
    market_df: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> tuple[pd.DataFrame, TopixCloseReturnStats | None, NtRatioReturnStats | None]:
    if market_df.empty:
        return pd.DataFrame(), None, None

    ordered = market_df.copy()
    ordered["date"] = ordered["date"].astype(str)
    ordered["topix_close_return"] = ordered["topix_close"].pct_change()
    ordered["nt_ratio"] = ordered["n225_close"] / ordered["topix_close"]
    ordered["nt_ratio_return"] = ordered["nt_ratio"].pct_change()

    topix_stats = _build_topix_close_stats(
        ordered,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )
    nt_ratio_stats = _build_nt_ratio_stats(
        ordered,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
    )

    ordered["topix_close_bucket_key"] = ordered["topix_close_return"].map(
        lambda value: _bucket_topix_close_return(value, stats=topix_stats)
    )
    ordered["topix_close_bucket_label"] = ordered["topix_close_bucket_key"].map(
        lambda value: (
            format_close_bucket_label(
                value,
                close_threshold_1=topix_stats.threshold_1,
                close_threshold_2=topix_stats.threshold_2,
            )
            if value is not None and topix_stats is not None
            else None
        )
    )
    ordered["nt_ratio_bucket_key"] = ordered["nt_ratio_return"].map(
        lambda value: _bucket_nt_ratio_return(value, stats=nt_ratio_stats)
    )
    ordered["nt_ratio_bucket_label"] = ordered["nt_ratio_bucket_key"].map(
        lambda value: (
            format_nt_ratio_bucket_label(
                value,
                sigma_threshold_1=nt_ratio_stats.sigma_threshold_1,
                sigma_threshold_2=nt_ratio_stats.sigma_threshold_2,
            )
            if value is not None and nt_ratio_stats is not None
            else None
        )
    )

    if start_date:
        ordered = ordered.loc[ordered["date"] >= start_date].copy()
    if end_date:
        ordered = ordered.loc[ordered["date"] <= end_date].copy()
    return _sort_frame(ordered), topix_stats, nt_ratio_stats


def _build_regime_assignments_df(regime_market_df: pd.DataFrame) -> pd.DataFrame:
    if regime_market_df.empty:
        return pd.DataFrame()

    topix_df = regime_market_df[
        [
            "date",
            "topix_close",
            "n225_close",
            "topix_close_return",
            "topix_close_bucket_key",
            "topix_close_bucket_label",
        ]
    ].rename(
        columns={
            "topix_close_return": "regime_return",
            "topix_close_bucket_key": "regime_bucket_key",
            "topix_close_bucket_label": "regime_bucket_label",
        }
    )
    topix_df["regime_type"] = "topix_close"
    topix_df["regime_label"] = REGIME_LABEL_MAP["topix_close"]

    nt_df = regime_market_df[
        [
            "date",
            "topix_close",
            "n225_close",
            "nt_ratio",
            "nt_ratio_return",
            "nt_ratio_bucket_key",
            "nt_ratio_bucket_label",
        ]
    ].rename(
        columns={
            "nt_ratio_return": "regime_return",
            "nt_ratio_bucket_key": "regime_bucket_key",
            "nt_ratio_bucket_label": "regime_bucket_label",
        }
    )
    nt_df["regime_type"] = "nt_ratio"
    nt_df["regime_label"] = REGIME_LABEL_MAP["nt_ratio"]

    assignments_df = pd.concat([topix_df, nt_df], ignore_index=True, sort=False)
    assignments_df = assignments_df.dropna(subset=["regime_bucket_key"]).copy()
    assignments_df["regime_group_key"] = assignments_df.apply(
        lambda row: _collapse_regime_bucket(
            regime_type=row["regime_type"],
            regime_bucket_key=row["regime_bucket_key"],
        ),
        axis=1,
    )
    assignments_df["regime_group_label"] = assignments_df["regime_group_key"].map(
        REGIME_GROUP_LABEL_MAP
    )
    return _sort_frame(assignments_df)


def _build_regime_day_counts(regime_assignments_df: pd.DataFrame) -> pd.DataFrame:
    if regime_assignments_df.empty:
        return pd.DataFrame()
    summary_df = (
        regime_assignments_df.groupby(
            ["regime_type", "regime_label", "regime_bucket_key", "regime_bucket_label"],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_regime_return=("regime_return", "mean"),
            median_regime_return=("regime_return", "median"),
        )
    )
    return _sort_frame(summary_df)


def _build_regime_group_day_counts(regime_assignments_df: pd.DataFrame) -> pd.DataFrame:
    if regime_assignments_df.empty:
        return pd.DataFrame()
    summary_df = (
        regime_assignments_df.groupby(
            ["regime_type", "regime_label", "regime_group_key", "regime_group_label"],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_regime_return=("regime_return", "mean"),
            median_regime_return=("regime_return", "median"),
        )
    )
    return _sort_frame(summary_df)


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
    if split_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        "price_bucket",
        "price_bucket_label",
        "volume_bucket",
        "volume_bucket_label",
        "combined_bucket",
        "combined_bucket_label",
        "price_feature",
        "volume_feature",
    ]
    frames: list[pd.DataFrame] = []
    for horizon_key in HORIZON_ORDER:
        frame = split_panel_df[
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


def _build_regime_daily_means(
    horizon_panel_df: pd.DataFrame,
    regime_assignments_df: pd.DataFrame,
) -> pd.DataFrame:
    if horizon_panel_df.empty or regime_assignments_df.empty:
        return pd.DataFrame()

    merged_df = horizon_panel_df.merge(regime_assignments_df, on="date", how="inner")
    if merged_df.empty:
        return pd.DataFrame()

    daily_means_df = (
        merged_df.groupby(
            [
                "regime_type",
                "regime_label",
                "regime_bucket_key",
                "regime_bucket_label",
                "horizon_key",
                "horizon_days",
                "date",
                "combined_bucket",
                "combined_bucket_label",
                "price_bucket",
                "price_bucket_label",
                "volume_bucket",
                "volume_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_event_close=("close", "mean"),
            group_mean_future_close=("future_close", "mean"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
            group_mean_regime_return=("regime_return", "mean"),
        )
    )
    return _sort_frame(daily_means_df)


def _summarize_regime_daily_means(regime_daily_means_df: pd.DataFrame) -> pd.DataFrame:
    if regime_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        regime_daily_means_df.groupby(
            [
                "regime_type",
                "regime_label",
                "regime_bucket_key",
                "regime_bucket_label",
                "horizon_key",
                "horizon_days",
                "combined_bucket",
                "combined_bucket_label",
                "price_bucket",
                "price_bucket_label",
                "volume_bucket",
                "volume_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_group_size=("group_sample_count", "mean"),
            mean_regime_return=("group_mean_regime_return", "mean"),
            mean_event_close=("group_mean_event_close", "mean"),
            mean_future_close=("group_mean_future_close", "mean"),
            mean_future_return=("group_mean_future_return", "mean"),
            median_future_return=("group_median_future_return", "median"),
            std_future_return=("group_mean_future_return", "std"),
        )
    )
    return _sort_frame(summary_df)


def _build_regime_group_daily_means(
    horizon_panel_df: pd.DataFrame,
    regime_assignments_df: pd.DataFrame,
) -> pd.DataFrame:
    if horizon_panel_df.empty or regime_assignments_df.empty:
        return pd.DataFrame()

    merged_df = horizon_panel_df.merge(regime_assignments_df, on="date", how="inner")
    if merged_df.empty:
        return pd.DataFrame()

    daily_means_df = (
        merged_df.groupby(
            [
                "regime_type",
                "regime_label",
                "regime_group_key",
                "regime_group_label",
                "horizon_key",
                "horizon_days",
                "date",
                "combined_bucket",
                "combined_bucket_label",
                "price_bucket",
                "price_bucket_label",
                "volume_bucket",
                "volume_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            group_sample_count=("code", "size"),
            group_mean_event_close=("close", "mean"),
            group_mean_future_close=("future_close", "mean"),
            group_mean_future_return=("future_return", "mean"),
            group_median_future_return=("future_return", "median"),
            group_mean_regime_return=("regime_return", "mean"),
        )
    )
    return _sort_frame(daily_means_df)


def _summarize_regime_group_daily_means(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_group_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        regime_group_daily_means_df.groupby(
            [
                "regime_type",
                "regime_label",
                "regime_group_key",
                "regime_group_label",
                "horizon_key",
                "horizon_days",
                "combined_bucket",
                "combined_bucket_label",
                "price_bucket",
                "price_bucket_label",
                "volume_bucket",
                "volume_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            mean_group_size=("group_sample_count", "mean"),
            mean_regime_return=("group_mean_regime_return", "mean"),
            mean_event_close=("group_mean_event_close", "mean"),
            mean_future_close=("group_mean_future_close", "mean"),
            mean_future_return=("group_mean_future_return", "mean"),
            median_future_return=("group_median_future_return", "median"),
            std_future_return=("group_mean_future_return", "std"),
        )
    )
    return _sort_frame(summary_df)


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
    if regime_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    regime_bucket_orders: dict[RegimeType, tuple[str, ...]] = {
        "topix_close": tuple(CLOSE_BUCKET_ORDER),
        "nt_ratio": tuple(NT_RATIO_BUCKET_ORDER),
    }
    label_lookup = (
        regime_daily_means_df[
            ["regime_type", "regime_bucket_key", "regime_bucket_label"]
        ]
        .drop_duplicates()
        .set_index(["regime_type", "regime_bucket_key"])["regime_bucket_label"]
        .to_dict()
    )

    for regime_type in REGIME_TYPE_ORDER:
        for regime_bucket_key in regime_bucket_orders[regime_type]:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    pivot_df = _aligned_regime_pivot(
                        regime_daily_means_df,
                        regime_type=regime_type,
                        regime_bucket_key=regime_bucket_key,
                        horizon_key=horizon_key,
                        value_column=metric_columns[metric_key],
                    )
                    if pivot_df.empty:
                        for left_bucket, right_bucket in combinations(
                            COMBINED_BUCKET_ORDER, 2
                        ):
                            records.append(
                                {
                                    "regime_type": regime_type,
                                    "regime_label": REGIME_LABEL_MAP[regime_type],
                                    "regime_bucket_key": regime_bucket_key,
                                    "regime_bucket_label": label_lookup.get(
                                        (regime_type, regime_bucket_key)
                                    ),
                                    "horizon_key": horizon_key,
                                    "metric_key": metric_key,
                                    "left_combined_bucket": left_bucket,
                                    "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                                        left_bucket
                                    ],
                                    "right_combined_bucket": right_bucket,
                                    "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
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
                        COMBINED_BUCKET_ORDER, 2
                    ):
                        left = pivot_df[left_bucket].to_numpy(dtype=float)
                        right = pivot_df[right_bucket].to_numpy(dtype=float)
                        paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                            left, right
                        )
                        wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                            left, right
                        )
                        records.append(
                            {
                                "regime_type": regime_type,
                                "regime_label": REGIME_LABEL_MAP[regime_type],
                                "regime_bucket_key": regime_bucket_key,
                                "regime_bucket_label": label_lookup.get(
                                    (regime_type, regime_bucket_key)
                                ),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "left_combined_bucket": left_bucket,
                                "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                                    left_bucket
                                ],
                                "right_combined_bucket": right_bucket,
                                "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
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
    for regime_type in REGIME_TYPE_ORDER:
        regime_bucket_order = (
            CLOSE_BUCKET_ORDER if regime_type == "topix_close" else NT_RATIO_BUCKET_ORDER
        )
        for regime_bucket_key in regime_bucket_order:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    mask = (
                        (pairwise_df["regime_type"] == regime_type)
                        & (pairwise_df["regime_bucket_key"] == regime_bucket_key)
                        & (pairwise_df["horizon_key"] == horizon_key)
                        & (pairwise_df["metric_key"] == metric_key)
                    )
                    pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                        pairwise_df.loc[mask, "paired_t_p_value"].tolist()
                    )
                    pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                        pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
                    )
    return _sort_frame(pairwise_df)


def _build_regime_group_pairwise_significance(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_group_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    label_lookup = (
        regime_group_daily_means_df[
            ["regime_type", "regime_group_key", "regime_group_label"]
        ]
        .drop_duplicates()
        .set_index(["regime_type", "regime_group_key"])["regime_group_label"]
        .to_dict()
    )

    for regime_type in REGIME_TYPE_ORDER:
        for regime_group_key in REGIME_GROUP_ORDER:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    pivot_df = _aligned_regime_group_pivot(
                        regime_group_daily_means_df,
                        regime_type=regime_type,
                        regime_group_key=regime_group_key,
                        horizon_key=horizon_key,
                        value_column=metric_columns[metric_key],
                    )
                    if pivot_df.empty:
                        for left_bucket, right_bucket in combinations(
                            COMBINED_BUCKET_ORDER, 2
                        ):
                            records.append(
                                {
                                    "regime_type": regime_type,
                                    "regime_label": REGIME_LABEL_MAP[regime_type],
                                    "regime_group_key": regime_group_key,
                                    "regime_group_label": label_lookup.get(
                                        (regime_type, regime_group_key)
                                    ),
                                    "horizon_key": horizon_key,
                                    "metric_key": metric_key,
                                    "left_combined_bucket": left_bucket,
                                    "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                                        left_bucket
                                    ],
                                    "right_combined_bucket": right_bucket,
                                    "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
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
                        COMBINED_BUCKET_ORDER, 2
                    ):
                        left = pivot_df[left_bucket].to_numpy(dtype=float)
                        right = pivot_df[right_bucket].to_numpy(dtype=float)
                        paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                            left, right
                        )
                        wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                            left, right
                        )
                        records.append(
                            {
                                "regime_type": regime_type,
                                "regime_label": REGIME_LABEL_MAP[regime_type],
                                "regime_group_key": regime_group_key,
                                "regime_group_label": label_lookup.get(
                                    (regime_type, regime_group_key)
                                ),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "left_combined_bucket": left_bucket,
                                "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                                    left_bucket
                                ],
                                "right_combined_bucket": right_bucket,
                                "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
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
    for regime_type in REGIME_TYPE_ORDER:
        for regime_group_key in REGIME_GROUP_ORDER:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    mask = (
                        (pairwise_df["regime_type"] == regime_type)
                        & (pairwise_df["regime_group_key"] == regime_group_key)
                        & (pairwise_df["horizon_key"] == horizon_key)
                        & (pairwise_df["metric_key"] == metric_key)
                    )
                    pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                        pairwise_df.loc[mask, "paired_t_p_value"].tolist()
                    )
                    pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                        pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
                    )
    return _sort_frame(pairwise_df)


def _build_regime_hypothesis(
    regime_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    regime_bucket_orders: dict[RegimeType, tuple[str, ...]] = {
        "topix_close": tuple(CLOSE_BUCKET_ORDER),
        "nt_ratio": tuple(NT_RATIO_BUCKET_ORDER),
    }
    for regime_type in REGIME_TYPE_ORDER:
        for regime_bucket_key in regime_bucket_orders[regime_type]:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    scoped_df = regime_pairwise_significance_df[
                        (regime_pairwise_significance_df["regime_type"] == regime_type)
                        & (
                            regime_pairwise_significance_df["regime_bucket_key"]
                            == regime_bucket_key
                        )
                        & (regime_pairwise_significance_df["horizon_key"] == horizon_key)
                        & (regime_pairwise_significance_df["metric_key"] == metric_key)
                    ]
                    for left_bucket, right_bucket, hypothesis_label in HYPOTHESIS_SPECS:
                        row = scoped_df[
                            (scoped_df["left_combined_bucket"] == left_bucket)
                            & (scoped_df["right_combined_bucket"] == right_bucket)
                        ]
                        sign = 1.0
                        if row.empty:
                            row = scoped_df[
                                (scoped_df["left_combined_bucket"] == right_bucket)
                                & (scoped_df["right_combined_bucket"] == left_bucket)
                            ]
                            sign = -1.0
                        if row.empty:
                            records.append(
                                {
                                    "regime_type": regime_type,
                                    "regime_label": REGIME_LABEL_MAP[regime_type],
                                    "regime_bucket_key": regime_bucket_key,
                                    "regime_bucket_label": None,
                                    "horizon_key": horizon_key,
                                    "metric_key": metric_key,
                                    "hypothesis_label": hypothesis_label,
                                    "left_combined_bucket": left_bucket,
                                    "right_combined_bucket": right_bucket,
                                    "mean_difference": None,
                                    "paired_t_p_value_holm": None,
                                    "wilcoxon_p_value_holm": None,
                                }
                            )
                            continue
                        record = row.iloc[0].to_dict()
                        records.append(
                            {
                                "regime_type": regime_type,
                                "regime_label": REGIME_LABEL_MAP[regime_type],
                                "regime_bucket_key": regime_bucket_key,
                                "regime_bucket_label": record.get("regime_bucket_label"),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "hypothesis_label": hypothesis_label,
                                "left_combined_bucket": left_bucket,
                                "right_combined_bucket": right_bucket,
                                "mean_difference": (
                                    float(record["mean_difference"]) * sign
                                    if record.get("mean_difference") is not None
                                    else None
                                ),
                                "paired_t_p_value_holm": record.get(
                                    "paired_t_p_value_holm"
                                ),
                                "wilcoxon_p_value_holm": record.get(
                                    "wilcoxon_p_value_holm"
                                ),
                            }
                        )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_regime_group_hypothesis(
    regime_group_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_group_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for regime_type in REGIME_TYPE_ORDER:
        for regime_group_key in REGIME_GROUP_ORDER:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    scoped_df = regime_group_pairwise_significance_df[
                        (regime_group_pairwise_significance_df["regime_type"] == regime_type)
                        & (
                            regime_group_pairwise_significance_df["regime_group_key"]
                            == regime_group_key
                        )
                        & (regime_group_pairwise_significance_df["horizon_key"] == horizon_key)
                        & (regime_group_pairwise_significance_df["metric_key"] == metric_key)
                    ]
                    for left_bucket, right_bucket, hypothesis_label in HYPOTHESIS_SPECS:
                        row = scoped_df[
                            (scoped_df["left_combined_bucket"] == left_bucket)
                            & (scoped_df["right_combined_bucket"] == right_bucket)
                        ]
                        sign = 1.0
                        if row.empty:
                            row = scoped_df[
                                (scoped_df["left_combined_bucket"] == right_bucket)
                                & (scoped_df["right_combined_bucket"] == left_bucket)
                            ]
                            sign = -1.0
                        if row.empty:
                            records.append(
                                {
                                    "regime_type": regime_type,
                                    "regime_label": REGIME_LABEL_MAP[regime_type],
                                    "regime_group_key": regime_group_key,
                                    "regime_group_label": REGIME_GROUP_LABEL_MAP[
                                        regime_group_key
                                    ],
                                    "horizon_key": horizon_key,
                                    "metric_key": metric_key,
                                    "hypothesis_label": hypothesis_label,
                                    "left_combined_bucket": left_bucket,
                                    "right_combined_bucket": right_bucket,
                                    "mean_difference": None,
                                    "paired_t_p_value_holm": None,
                                    "wilcoxon_p_value_holm": None,
                                }
                            )
                            continue
                        record = row.iloc[0].to_dict()
                        records.append(
                            {
                                "regime_type": regime_type,
                                "regime_label": REGIME_LABEL_MAP[regime_type],
                                "regime_group_key": regime_group_key,
                                "regime_group_label": record.get("regime_group_label"),
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "hypothesis_label": hypothesis_label,
                                "left_combined_bucket": left_bucket,
                                "right_combined_bucket": right_bucket,
                                "mean_difference": (
                                    float(record["mean_difference"]) * sign
                                    if record.get("mean_difference") is not None
                                    else None
                                ),
                                "paired_t_p_value_holm": record.get(
                                    "paired_t_p_value_holm"
                                ),
                                "wilcoxon_p_value_holm": record.get(
                                    "wilcoxon_p_value_holm"
                                ),
                            }
                        )
    return _sort_frame(pd.DataFrame.from_records(records))


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
