"""
TOPIX100 price-vs-20SMA rank / future close research analytics.

This module keeps the same TOPIX100 universe and future-close test setup as
the SMA-ratio research, but uses `(close / sma20) - 1` as the primary price
feature. Volume conditioning remains `volume_sma_20_80`.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any, Literal

import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import (
    DECILE_ORDER,
    HORIZON_ORDER,
    METRIC_ORDER,
    _DEFAULT_LOOKBACK_YEARS,
    _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
    _HORIZON_DAY_MAP,
    _assign_feature_deciles,
    _build_daily_group_means,
    _build_global_significance,
    _build_horizon_panel,
    _build_pairwise_significance,
    _default_start_date,
    _holm_adjust,
    _query_topix100_date_range,
    _query_topix100_stock_history,
    _rolling_mean,
    _safe_paired_t_test,
    _safe_ratio,
    _safe_wilcoxon,
    _sort_frame as _base_sort_frame,
    _summarize_future_targets,
    _summarize_ranking_features,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)

HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
PriceBucketKey = Literal["q1", "middle", "q10"]
VolumeBucketKey = Literal["volume_high", "volume_low"]
CombinedBucketKey = Literal[
    "q1_volume_high",
    "q1_volume_low",
    "middle_volume_high",
    "middle_volume_low",
    "q10_volume_high",
    "q10_volume_low",
]

PRIMARY_PRICE_FEATURE = "price_vs_sma_20_gap"
PRIMARY_PRICE_FEATURE_LABEL = "Price vs SMA 20 Gap"
PRIMARY_VOLUME_FEATURE = "volume_sma_20_80"
PRIMARY_VOLUME_FEATURE_LABEL = "Volume SMA 20 / 80"

PRICE_BUCKET_ORDER: tuple[PriceBucketKey, ...] = ("q1", "middle", "q10")
PRICE_BUCKET_DECILES: dict[PriceBucketKey, tuple[str, ...]] = {
    "q1": ("Q1",),
    "middle": ("Q4", "Q5", "Q6"),
    "q10": ("Q10",),
}
PRICE_BUCKET_LABEL_MAP: dict[PriceBucketKey, str] = {
    "q1": "Q1",
    "middle": "Q4 + Q5 + Q6",
    "q10": "Q10",
}
VOLUME_BUCKET_ORDER: tuple[VolumeBucketKey, ...] = ("volume_high", "volume_low")
VOLUME_BUCKET_LABEL_MAP: dict[VolumeBucketKey, str] = {
    "volume_high": "Volume 20 / 80 High Half",
    "volume_low": "Volume 20 / 80 Low Half",
}
COMBINED_BUCKET_ORDER: tuple[CombinedBucketKey, ...] = (
    "q1_volume_high",
    "q1_volume_low",
    "middle_volume_high",
    "middle_volume_low",
    "q10_volume_high",
    "q10_volume_low",
)
COMBINED_BUCKET_LABEL_MAP: dict[CombinedBucketKey, str] = {
    "q1_volume_high": "Q1 x Volume High",
    "q1_volume_low": "Q1 x Volume Low",
    "middle_volume_high": "Middle x Volume High",
    "middle_volume_low": "Middle x Volume Low",
    "q10_volume_high": "Q10 x Volume High",
    "q10_volume_low": "Q10 x Volume Low",
}
GROUP_HYPOTHESIS_LABELS: tuple[tuple[PriceBucketKey, PriceBucketKey, str], ...] = (
    ("q1", "middle", "Q1 vs Middle"),
    ("q10", "middle", "Q10 vs Middle"),
    ("q1", "q10", "Q1 vs Q10"),
)
SPLIT_HYPOTHESIS_LABELS: tuple[tuple[CombinedBucketKey, CombinedBucketKey, str], ...] = (
    ("q1_volume_high", "q1_volume_low", "Q1 High vs Q1 Low"),
    ("q10_volume_low", "q10_volume_high", "Q10 Low vs Q10 High"),
    ("q1_volume_high", "middle_volume_high", "Q1 High vs Middle High"),
    ("q1_volume_low", "middle_volume_low", "Q1 Low vs Middle Low"),
    ("q10_volume_low", "middle_volume_low", "Q10 Low vs Middle Low"),
    ("q10_volume_low", "middle_volume_high", "Q10 Low vs Middle High"),
)


@dataclass(frozen=True)
class Topix100PriceVsSma20RankFutureCloseResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    min_constituents_per_day: int
    topix100_constituent_count: int
    stock_day_count: int
    valid_date_count: int
    event_panel_df: pd.DataFrame
    ranked_panel_df: pd.DataFrame
    ranking_feature_summary_df: pd.DataFrame
    decile_future_summary_df: pd.DataFrame
    daily_group_means_df: pd.DataFrame
    global_significance_df: pd.DataFrame
    pairwise_significance_df: pd.DataFrame
    price_bucket_daily_means_df: pd.DataFrame
    price_bucket_summary_df: pd.DataFrame
    price_bucket_pairwise_significance_df: pd.DataFrame
    group_hypothesis_df: pd.DataFrame
    price_volume_split_panel_df: pd.DataFrame
    price_volume_split_daily_means_df: pd.DataFrame
    price_volume_split_summary_df: pd.DataFrame
    price_volume_split_pairwise_significance_df: pd.DataFrame
    split_hypothesis_df: pd.DataFrame


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = _base_sort_frame(df, known_feature_order=[PRIMARY_PRICE_FEATURE])
    if "price_bucket" in sorted_df.columns:
        sorted_df["_price_bucket_order"] = sorted_df["price_bucket"].map(
            {key: index for index, key in enumerate(PRICE_BUCKET_ORDER, start=1)}
        )
    if "left_price_bucket" in sorted_df.columns:
        sorted_df["_left_price_bucket_order"] = sorted_df["left_price_bucket"].map(
            {key: index for index, key in enumerate(PRICE_BUCKET_ORDER, start=1)}
        )
    if "right_price_bucket" in sorted_df.columns:
        sorted_df["_right_price_bucket_order"] = sorted_df["right_price_bucket"].map(
            {key: index for index, key in enumerate(PRICE_BUCKET_ORDER, start=1)}
        )
    if "volume_bucket" in sorted_df.columns:
        sorted_df["_volume_bucket_order"] = sorted_df["volume_bucket"].map(
            {key: index for index, key in enumerate(VOLUME_BUCKET_ORDER, start=1)}
        )
    if "combined_bucket" in sorted_df.columns:
        sorted_df["_combined_bucket_order"] = sorted_df["combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "left_combined_bucket" in sorted_df.columns:
        sorted_df["_left_combined_bucket_order"] = sorted_df["left_combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )
    if "right_combined_bucket" in sorted_df.columns:
        sorted_df["_right_combined_bucket_order"] = sorted_df["right_combined_bucket"].map(
            {key: index for index, key in enumerate(COMBINED_BUCKET_ORDER, start=1)}
        )

    sort_columns = [
        column
        for column in [
            "horizon_key",
            "metric_key",
            "_price_bucket_order",
            "_volume_bucket_order",
            "_combined_bucket_order",
            "_left_price_bucket_order",
            "_right_price_bucket_order",
            "_left_combined_bucket_order",
            "_right_combined_bucket_order",
            "hypothesis_label",
            "date",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns).reset_index(drop=True)

    return sorted_df.drop(
        columns=[
            column
            for column in [
                "_price_bucket_order",
                "_left_price_bucket_order",
                "_right_price_bucket_order",
                "_volume_bucket_order",
                "_combined_bucket_order",
                "_left_combined_bucket_order",
                "_right_combined_bucket_order",
            ]
            if column in sorted_df.columns
        ]
    )


def get_topix100_price_vs_sma20_rank_future_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _query_topix100_date_range(ctx.connection)


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

    close_sma_20 = _rolling_mean(panel, column_name="close", window=20)
    volume_sma_20 = _rolling_mean(panel, column_name="volume", window=20)
    volume_sma_80 = _rolling_mean(panel, column_name="volume", window=80)

    panel["close_sma_20"] = close_sma_20
    panel[PRIMARY_PRICE_FEATURE] = _safe_ratio(panel["close"], close_sma_20) - 1.0
    panel[PRIMARY_VOLUME_FEATURE] = _safe_ratio(volume_sma_20, volume_sma_80)

    for horizon_key, horizon_days in _HORIZON_DAY_MAP.items():
        future_close = (
            panel.groupby("code", sort=False)["close"].shift(-horizon_days).astype(float)
        )
        panel[f"{horizon_key}_close"] = future_close
        panel[f"{horizon_key}_return"] = _safe_ratio(future_close, panel["close"]) - 1.0

    required_mask = (
        panel["close"].gt(0)
        & panel[PRIMARY_PRICE_FEATURE].notna()
        & panel[PRIMARY_VOLUME_FEATURE].notna()
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


def _build_ranked_panel(event_panel_df: pd.DataFrame) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

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
    ranked_panel_df["ranking_feature"] = PRIMARY_PRICE_FEATURE
    ranked_panel_df["ranking_feature_label"] = PRIMARY_PRICE_FEATURE_LABEL
    ranked_panel_df["ranking_value"] = event_panel_df[PRIMARY_PRICE_FEATURE].astype(float)
    return _assign_feature_deciles(
        ranked_panel_df,
        known_feature_order=[PRIMARY_PRICE_FEATURE],
    )


def _build_price_bucket_daily_means(horizon_panel_df: pd.DataFrame) -> pd.DataFrame:
    if horizon_panel_df.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for bucket_key in PRICE_BUCKET_ORDER:
        bucket_deciles = PRICE_BUCKET_DECILES[bucket_key]
        frame = horizon_panel_df.loc[
            horizon_panel_df["feature_decile"].isin(bucket_deciles)
        ].copy()
        if frame.empty:
            continue
        frame["price_bucket"] = bucket_key
        frame["price_bucket_label"] = PRICE_BUCKET_LABEL_MAP[bucket_key]
        frames.append(frame)
    if not frames:
        return pd.DataFrame()

    bucket_panel_df = pd.concat(frames, ignore_index=True)
    daily_means_df = (
        bucket_panel_df.groupby(
            [
                "ranking_feature",
                "ranking_feature_label",
                "horizon_key",
                "horizon_days",
                "date",
                "price_bucket",
                "price_bucket_label",
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


def _summarize_price_buckets(price_bucket_daily_means_df: pd.DataFrame) -> pd.DataFrame:
    if price_bucket_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        price_bucket_daily_means_df.groupby(
            [
                "ranking_feature",
                "ranking_feature_label",
                "horizon_key",
                "horizon_days",
                "price_bucket",
                "price_bucket_label",
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


def _aligned_price_bucket_pivot(
    price_bucket_daily_means_df: pd.DataFrame,
    *,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = price_bucket_daily_means_df.loc[
        price_bucket_daily_means_df["horizon_key"] == horizon_key
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(PRICE_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(index="date", columns="price_bucket", values=value_column)
        .reindex(columns=list(PRICE_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_price_bucket_pairwise_significance(
    price_bucket_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_bucket_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_price_bucket_pivot(
                price_bucket_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                for left_bucket, right_bucket in combinations(PRICE_BUCKET_ORDER, 2):
                    records.append(
                        {
                            "ranking_feature": PRIMARY_PRICE_FEATURE,
                            "ranking_feature_label": PRIMARY_PRICE_FEATURE_LABEL,
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_price_bucket": left_bucket,
                            "left_price_bucket_label": PRICE_BUCKET_LABEL_MAP[left_bucket],
                            "right_price_bucket": right_bucket,
                            "right_price_bucket_label": PRICE_BUCKET_LABEL_MAP[right_bucket],
                            "n_dates": 0,
                            "mean_difference": None,
                            "paired_t_statistic": None,
                            "paired_t_p_value": None,
                            "wilcoxon_statistic": None,
                            "wilcoxon_p_value": None,
                        }
                    )
                continue

            for left_bucket, right_bucket in combinations(PRICE_BUCKET_ORDER, 2):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "ranking_feature": PRIMARY_PRICE_FEATURE,
                        "ranking_feature_label": PRIMARY_PRICE_FEATURE_LABEL,
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_price_bucket": left_bucket,
                        "left_price_bucket_label": PRICE_BUCKET_LABEL_MAP[left_bucket],
                        "right_price_bucket": right_bucket,
                        "right_price_bucket_label": PRICE_BUCKET_LABEL_MAP[right_bucket],
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


def _build_group_hypothesis(
    price_bucket_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_bucket_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            scoped_df = price_bucket_pairwise_significance_df[
                (price_bucket_pairwise_significance_df["horizon_key"] == horizon_key)
                & (price_bucket_pairwise_significance_df["metric_key"] == metric_key)
            ]
            for left_bucket, right_bucket, hypothesis_label in GROUP_HYPOTHESIS_LABELS:
                row = scoped_df[
                    (scoped_df["left_price_bucket"] == left_bucket)
                    & (scoped_df["right_price_bucket"] == right_bucket)
                ]
                sign = 1.0
                if row.empty:
                    row = scoped_df[
                        (scoped_df["left_price_bucket"] == right_bucket)
                        & (scoped_df["right_price_bucket"] == left_bucket)
                    ]
                    sign = -1.0
                if row.empty:
                    records.append(
                        {
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "hypothesis_label": hypothesis_label,
                            "left_price_bucket": left_bucket,
                            "right_price_bucket": right_bucket,
                            "mean_difference": None,
                            "paired_t_p_value_holm": None,
                            "wilcoxon_p_value_holm": None,
                        }
                    )
                    continue
                pairwise_row = row.iloc[0]
                mean_difference = pairwise_row["mean_difference"]
                records.append(
                    {
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "hypothesis_label": hypothesis_label,
                        "left_price_bucket": left_bucket,
                        "right_price_bucket": right_bucket,
                        "mean_difference": (
                            None
                            if mean_difference is None or pd.isna(mean_difference)
                            else sign * float(mean_difference)
                        ),
                        "paired_t_p_value_holm": pairwise_row["paired_t_p_value_holm"],
                        "wilcoxon_p_value_holm": pairwise_row["wilcoxon_p_value_holm"],
                    }
                )
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_price_volume_split_panel(event_panel_df: pd.DataFrame) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    panel_df = event_panel_df.copy()
    panel_df["price_rank_desc"] = (
        panel_df.groupby("date")[PRIMARY_PRICE_FEATURE]
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
    panel_df["price_bucket"] = None
    for bucket_key, bucket_deciles in PRICE_BUCKET_DECILES.items():
        panel_df.loc[panel_df["price_decile"].isin(bucket_deciles), "price_bucket"] = (
            bucket_key
        )
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
    panel_df["price_feature_label"] = PRIMARY_PRICE_FEATURE_LABEL
    panel_df["volume_feature"] = PRIMARY_VOLUME_FEATURE
    panel_df["volume_feature_label"] = PRIMARY_VOLUME_FEATURE_LABEL
    return _sort_frame(panel_df.reset_index(drop=True))


def _build_price_volume_horizon_panel(
    price_volume_split_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_volume_split_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        "price_feature",
        "price_feature_label",
        "volume_feature",
        "volume_feature_label",
        "price_bucket",
        "price_bucket_label",
        "volume_bucket",
        "volume_bucket_label",
        "combined_bucket",
        "combined_bucket_label",
    ]
    frames: list[pd.DataFrame] = []
    for horizon_key in HORIZON_ORDER:
        frame = price_volume_split_panel_df[
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


def _build_price_volume_daily_means(
    price_volume_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_volume_horizon_panel_df.empty:
        return pd.DataFrame()

    daily_group_means_df = (
        price_volume_horizon_panel_df.groupby(
            [
                "price_feature",
                "price_feature_label",
                "volume_feature",
                "volume_feature_label",
                "horizon_key",
                "horizon_days",
                "date",
                "price_bucket",
                "price_bucket_label",
                "volume_bucket",
                "volume_bucket_label",
                "combined_bucket",
                "combined_bucket_label",
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


def _summarize_price_volume_split(
    price_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_volume_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        price_volume_daily_means_df.groupby(
            [
                "price_feature",
                "price_feature_label",
                "volume_feature",
                "volume_feature_label",
                "horizon_key",
                "horizon_days",
                "price_bucket",
                "price_bucket_label",
                "volume_bucket",
                "volume_bucket_label",
                "combined_bucket",
                "combined_bucket_label",
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


def _aligned_combined_bucket_pivot(
    price_volume_daily_means_df: pd.DataFrame,
    *,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = price_volume_daily_means_df.loc[
        price_volume_daily_means_df["horizon_key"] == horizon_key
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(index="date", columns="combined_bucket", values=value_column)
        .reindex(columns=list(COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_price_volume_pairwise_significance(
    price_volume_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_volume_daily_means_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            pivot_df = _aligned_combined_bucket_pivot(
                price_volume_daily_means_df,
                horizon_key=horizon_key,
                value_column=metric_columns[metric_key],
            )
            if pivot_df.empty:
                for left_bucket, right_bucket in combinations(COMBINED_BUCKET_ORDER, 2):
                    records.append(
                        {
                            "price_feature": PRIMARY_PRICE_FEATURE,
                            "price_feature_label": PRIMARY_PRICE_FEATURE_LABEL,
                            "volume_feature": PRIMARY_VOLUME_FEATURE,
                            "volume_feature_label": PRIMARY_VOLUME_FEATURE_LABEL,
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_combined_bucket": left_bucket,
                            "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                            "right_combined_bucket": right_bucket,
                            "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
                            "n_dates": 0,
                            "mean_difference": None,
                            "paired_t_statistic": None,
                            "paired_t_p_value": None,
                            "wilcoxon_statistic": None,
                            "wilcoxon_p_value": None,
                        }
                    )
                continue

            for left_bucket, right_bucket in combinations(COMBINED_BUCKET_ORDER, 2):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "price_feature": PRIMARY_PRICE_FEATURE,
                        "price_feature_label": PRIMARY_PRICE_FEATURE_LABEL,
                        "volume_feature": PRIMARY_VOLUME_FEATURE,
                        "volume_feature_label": PRIMARY_VOLUME_FEATURE_LABEL,
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_combined_bucket": left_bucket,
                        "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[left_bucket],
                        "right_combined_bucket": right_bucket,
                        "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[right_bucket],
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


def _build_split_hypothesis(
    price_volume_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if price_volume_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for horizon_key in HORIZON_ORDER:
        for metric_key in METRIC_ORDER:
            scoped_df = price_volume_pairwise_significance_df[
                (price_volume_pairwise_significance_df["horizon_key"] == horizon_key)
                & (price_volume_pairwise_significance_df["metric_key"] == metric_key)
            ]
            for left_bucket, right_bucket, hypothesis_label in SPLIT_HYPOTHESIS_LABELS:
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
                pairwise_row = row.iloc[0]
                mean_difference = pairwise_row["mean_difference"]
                records.append(
                    {
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "hypothesis_label": hypothesis_label,
                        "left_combined_bucket": left_bucket,
                        "right_combined_bucket": right_bucket,
                        "mean_difference": (
                            None
                            if mean_difference is None or pd.isna(mean_difference)
                            else sign * float(mean_difference)
                        ),
                        "paired_t_p_value_holm": pairwise_row["paired_t_p_value_holm"],
                        "wilcoxon_p_value_holm": pairwise_row["wilcoxon_p_value_holm"],
                    }
                )
    return _sort_frame(pd.DataFrame.from_records(records))


def run_topix100_price_vs_sma20_rank_future_close_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int = _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
) -> Topix100PriceVsSma20RankFutureCloseResearchResult:
    """
    Run TOPIX100 close-vs-20SMA rank vs future close research from market.duckdb.

    The primary price feature is `(close / sma20) - 1`, ranked daily within the
    TOPIX100 universe. Volume conditioning uses `volume_sma_20_80`.
    """

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _query_topix100_date_range(conn)
        default_start = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start = start_date or default_start
        history_df = _query_topix100_stock_history(conn, end_date=end_date)

    event_panel_df = _enrich_event_panel(
        history_df,
        analysis_start_date=analysis_start,
        analysis_end_date=end_date,
        min_constituents_per_day=min_constituents_per_day,
    )
    ranked_panel_df = _build_ranked_panel(event_panel_df)
    horizon_panel_df = _build_horizon_panel(
        ranked_panel_df,
        known_feature_order=[PRIMARY_PRICE_FEATURE],
    )
    daily_group_means_df = _build_daily_group_means(
        horizon_panel_df,
        known_feature_order=[PRIMARY_PRICE_FEATURE],
    )

    price_bucket_daily_means_df = _build_price_bucket_daily_means(horizon_panel_df)
    price_bucket_pairwise_significance_df = _build_price_bucket_pairwise_significance(
        price_bucket_daily_means_df
    )
    price_volume_split_panel_df = _build_price_volume_split_panel(event_panel_df)
    price_volume_horizon_panel_df = _build_price_volume_horizon_panel(
        price_volume_split_panel_df
    )
    price_volume_split_daily_means_df = _build_price_volume_daily_means(
        price_volume_horizon_panel_df
    )
    price_volume_split_pairwise_significance_df = (
        _build_price_volume_pairwise_significance(price_volume_split_daily_means_df)
    )

    analysis_start_date = (
        str(event_panel_df["date"].min()) if not event_panel_df.empty else None
    )
    analysis_end_date = str(event_panel_df["date"].max()) if not event_panel_df.empty else None

    return Topix100PriceVsSma20RankFutureCloseResearchResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        default_start_date=default_start,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        topix100_constituent_count=int(history_df["code"].nunique()),
        stock_day_count=int(len(event_panel_df)),
        valid_date_count=int(event_panel_df["date"].nunique())
        if not event_panel_df.empty
        else 0,
        event_panel_df=event_panel_df,
        ranked_panel_df=ranked_panel_df,
        ranking_feature_summary_df=_summarize_ranking_features(
            ranked_panel_df,
            known_feature_order=[PRIMARY_PRICE_FEATURE],
        ),
        decile_future_summary_df=_summarize_future_targets(
            horizon_panel_df,
            known_feature_order=[PRIMARY_PRICE_FEATURE],
        ),
        daily_group_means_df=daily_group_means_df,
        global_significance_df=_build_global_significance(
            daily_group_means_df,
            known_feature_order=[PRIMARY_PRICE_FEATURE],
        ),
        pairwise_significance_df=_build_pairwise_significance(
            daily_group_means_df,
            known_feature_order=[PRIMARY_PRICE_FEATURE],
        ),
        price_bucket_daily_means_df=price_bucket_daily_means_df,
        price_bucket_summary_df=_summarize_price_buckets(price_bucket_daily_means_df),
        price_bucket_pairwise_significance_df=price_bucket_pairwise_significance_df,
        group_hypothesis_df=_build_group_hypothesis(
            price_bucket_pairwise_significance_df
        ),
        price_volume_split_panel_df=price_volume_split_panel_df,
        price_volume_split_daily_means_df=price_volume_split_daily_means_df,
        price_volume_split_summary_df=_summarize_price_volume_split(
            price_volume_split_daily_means_df
        ),
        price_volume_split_pairwise_significance_df=price_volume_split_pairwise_significance_df,
        split_hypothesis_df=_build_split_hypothesis(
            price_volume_split_pairwise_significance_df
        ),
    )
