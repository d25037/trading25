# pyright: reportUnusedFunction=false
"""
Bucket-specific helper tables for TOPIX SMA-ratio rank / future-close research.

These helpers keep the main research entrypoint focused on orchestration while
isolating the price-bucket and price-volume split analyses.
"""

from __future__ import annotations

from itertools import combinations
from typing import Any, cast

import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import (
    _holm_adjust,
    _kendalls_w,
    _safe_friedman,
    _safe_kruskal,
    _safe_paired_t_test,
    _safe_wilcoxon,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    BUCKET_GROUP_DECILES,
    BUCKET_GROUP_LABEL_MAP,
    BUCKET_GROUP_ORDER,
    DECILE_ORDER,
    HORIZON_DAY_MAP,
    HORIZON_ORDER,
    HorizonKey,
    METRIC_ORDER,
    NESTED_COMBINED_BUCKET_LABEL_MAP,
    NESTED_COMBINED_BUCKET_ORDER,
    NESTED_PRICE_BUCKET_DECILES,
    NESTED_PRICE_BUCKET_LABEL_MAP,
    NESTED_VOLUME_BUCKET_LABEL_MAP,
    PRIMARY_PRICE_FEATURE,
    PRIMARY_VOLUME_FEATURE,
    Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP,
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    Q10_MIDDLE_PRICE_BUCKET_DECILES,
    Q10_MIDDLE_PRICE_BUCKET_LABEL_MAP,
    Q1_Q10_COMBINED_BUCKET_LABEL_MAP,
    Q1_Q10_COMBINED_BUCKET_ORDER,
    Q1_Q10_PRICE_BUCKET_DECILES,
    Q1_Q10_PRICE_BUCKET_LABEL_MAP,
    RANKING_FEATURE_LABEL_MAP,
    RankingFeatureKey,
    _ordered_feature_values,
    _ranking_feature_label_lookup,
    _sort_frame,
)


def _build_extreme_vs_middle_daily_means(
    horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if horizon_panel_df.empty:
        return pd.DataFrame()

    group_frames: list[pd.DataFrame] = []
    for bucket_group in BUCKET_GROUP_ORDER:
        bucket_deciles = BUCKET_GROUP_DECILES[bucket_group]
        frame = horizon_panel_df.loc[
            horizon_panel_df["feature_decile"].isin(bucket_deciles)
        ].copy()
        if frame.empty:
            continue
        frame["bucket_group"] = bucket_group
        frame["bucket_group_label"] = BUCKET_GROUP_LABEL_MAP[bucket_group]
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
        scoped_df.pivot(index="date", columns="bucket_group", values=value_column)
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
        feature_key = cast(RankingFeatureKey, ranking_feature)
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
                                RANKING_FEATURE_LABEL_MAP.get(
                                    feature_key,
                                    ranking_feature,
                                ),
                            ),
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "n_dates": 0,
                            "extreme_group_label": BUCKET_GROUP_LABEL_MAP[
                                "q1_q10_extreme"
                            ],
                            "middle_group_label": BUCKET_GROUP_LABEL_MAP[
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
                            RANKING_FEATURE_LABEL_MAP.get(
                                feature_key,
                                ranking_feature,
                            ),
                        ),
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "n_dates": int(len(pivot_df)),
                        "extreme_group_label": BUCKET_GROUP_LABEL_MAP[
                            "q1_q10_extreme"
                        ],
                        "middle_group_label": BUCKET_GROUP_LABEL_MAP[
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
    price_feature: RankingFeatureKey = PRIMARY_PRICE_FEATURE,
    volume_feature: RankingFeatureKey = PRIMARY_VOLUME_FEATURE,
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
    for bucket_key, bucket_deciles in NESTED_PRICE_BUCKET_DECILES.items():
        panel_df.loc[
            panel_df["price_decile"].isin(bucket_deciles), "nested_price_bucket"
        ] = bucket_key
    panel_df = panel_df.dropna(subset=["nested_price_bucket"]).copy()
    if panel_df.empty:
        return pd.DataFrame()

    panel_df["nested_price_bucket"] = panel_df["nested_price_bucket"].astype(str)
    panel_df["nested_price_bucket_label"] = panel_df["nested_price_bucket"].map(
        NESTED_PRICE_BUCKET_LABEL_MAP
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
        NESTED_VOLUME_BUCKET_LABEL_MAP
    )
    panel_df["nested_combined_bucket"] = (
        panel_df["nested_price_bucket"] + "_" + panel_df["nested_volume_bucket"]
    )
    panel_df["nested_combined_bucket_label"] = panel_df["nested_combined_bucket"].map(
        NESTED_COMBINED_BUCKET_LABEL_MAP
    )
    panel_df["nested_price_feature"] = price_feature
    panel_df["nested_price_feature_label"] = RANKING_FEATURE_LABEL_MAP[price_feature]
    panel_df["nested_volume_feature"] = volume_feature
    panel_df["nested_volume_feature_label"] = RANKING_FEATURE_LABEL_MAP[volume_feature]
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
        frame["horizon_days"] = HORIZON_DAY_MAP[horizon_key]
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
        return pd.DataFrame(columns=list(NESTED_COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="nested_combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(NESTED_COMBINED_BUCKET_ORDER))
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
                        "nested_price_feature": PRIMARY_PRICE_FEATURE,
                        "nested_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "nested_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "nested_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
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
                for bucket in NESTED_COMBINED_BUCKET_ORDER
            ]
            friedman_statistic, friedman_p_value = _safe_friedman(samples)
            kruskal_statistic, kruskal_p_value = _safe_kruskal(samples)
            records.append(
                {
                    "nested_price_feature": PRIMARY_PRICE_FEATURE,
                    "nested_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_PRICE_FEATURE
                    ],
                    "nested_volume_feature": PRIMARY_VOLUME_FEATURE,
                    "nested_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_VOLUME_FEATURE
                    ],
                    "horizon_key": horizon_key,
                    "metric_key": metric_key,
                    "n_dates": int(len(pivot_df)),
                    "friedman_statistic": friedman_statistic,
                    "friedman_p_value": friedman_p_value,
                    "kendalls_w": _kendalls_w(
                        friedman_statistic=friedman_statistic,
                        n_dates=len(pivot_df),
                        n_groups=len(NESTED_COMBINED_BUCKET_ORDER),
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
                    NESTED_COMBINED_BUCKET_ORDER, 2
                ):
                    records.append(
                        {
                            "nested_price_feature": PRIMARY_PRICE_FEATURE,
                            "nested_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                                PRIMARY_PRICE_FEATURE
                            ],
                            "nested_volume_feature": PRIMARY_VOLUME_FEATURE,
                            "nested_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                                PRIMARY_VOLUME_FEATURE
                            ],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_nested_combined_bucket": left_bucket,
                            "left_nested_combined_bucket_label": NESTED_COMBINED_BUCKET_LABEL_MAP[
                                left_bucket
                            ],
                            "right_nested_combined_bucket": right_bucket,
                            "right_nested_combined_bucket_label": NESTED_COMBINED_BUCKET_LABEL_MAP[
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
                NESTED_COMBINED_BUCKET_ORDER, 2
            ):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "nested_price_feature": PRIMARY_PRICE_FEATURE,
                        "nested_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "nested_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "nested_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_nested_combined_bucket": left_bucket,
                        "left_nested_combined_bucket_label": NESTED_COMBINED_BUCKET_LABEL_MAP[
                            left_bucket
                        ],
                        "right_nested_combined_bucket": right_bucket,
                        "right_nested_combined_bucket_label": NESTED_COMBINED_BUCKET_LABEL_MAP[
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
                        "nested_price_feature": PRIMARY_PRICE_FEATURE,
                        "nested_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "nested_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "nested_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
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
                    "nested_price_feature": PRIMARY_PRICE_FEATURE,
                    "nested_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_PRICE_FEATURE
                    ],
                    "nested_volume_feature": PRIMARY_VOLUME_FEATURE,
                    "nested_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_VOLUME_FEATURE
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
    price_feature: RankingFeatureKey = PRIMARY_PRICE_FEATURE,
    volume_feature: RankingFeatureKey = PRIMARY_VOLUME_FEATURE,
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
    for bucket_key, bucket_deciles in Q1_Q10_PRICE_BUCKET_DECILES.items():
        panel_df.loc[
            panel_df["price_decile"].isin(bucket_deciles), "q1_q10_price_bucket"
        ] = bucket_key
    panel_df = panel_df.dropna(subset=["q1_q10_price_bucket"]).copy()
    if panel_df.empty:
        return pd.DataFrame()

    panel_df["q1_q10_price_bucket"] = panel_df["q1_q10_price_bucket"].astype(str)
    panel_df["q1_q10_price_bucket_label"] = panel_df["q1_q10_price_bucket"].map(
        Q1_Q10_PRICE_BUCKET_LABEL_MAP
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
        NESTED_VOLUME_BUCKET_LABEL_MAP
    )
    panel_df["q1_q10_combined_bucket"] = (
        panel_df["q1_q10_price_bucket"] + "_" + panel_df["q1_q10_volume_bucket"]
    )
    panel_df["q1_q10_combined_bucket_label"] = panel_df[
        "q1_q10_combined_bucket"
    ].map(Q1_Q10_COMBINED_BUCKET_LABEL_MAP)
    panel_df["q1_q10_price_feature"] = price_feature
    panel_df["q1_q10_price_feature_label"] = RANKING_FEATURE_LABEL_MAP[price_feature]
    panel_df["q1_q10_volume_feature"] = volume_feature
    panel_df["q1_q10_volume_feature_label"] = RANKING_FEATURE_LABEL_MAP[volume_feature]
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
        frame["horizon_days"] = HORIZON_DAY_MAP[horizon_key]
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
        return pd.DataFrame(columns=list(Q1_Q10_COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="q1_q10_combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(Q1_Q10_COMBINED_BUCKET_ORDER))
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
                        "q1_q10_price_feature": PRIMARY_PRICE_FEATURE,
                        "q1_q10_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "q1_q10_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "q1_q10_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
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
                for bucket in Q1_Q10_COMBINED_BUCKET_ORDER
            ]
            friedman_statistic, friedman_p_value = _safe_friedman(samples)
            kruskal_statistic, kruskal_p_value = _safe_kruskal(samples)
            records.append(
                {
                    "q1_q10_price_feature": PRIMARY_PRICE_FEATURE,
                    "q1_q10_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_PRICE_FEATURE
                    ],
                    "q1_q10_volume_feature": PRIMARY_VOLUME_FEATURE,
                    "q1_q10_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_VOLUME_FEATURE
                    ],
                    "horizon_key": horizon_key,
                    "metric_key": metric_key,
                    "n_dates": int(len(pivot_df)),
                    "friedman_statistic": friedman_statistic,
                    "friedman_p_value": friedman_p_value,
                    "kendalls_w": _kendalls_w(
                        friedman_statistic=friedman_statistic,
                        n_dates=len(pivot_df),
                        n_groups=len(Q1_Q10_COMBINED_BUCKET_ORDER),
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
                    Q1_Q10_COMBINED_BUCKET_ORDER, 2
                ):
                    records.append(
                        {
                            "q1_q10_price_feature": PRIMARY_PRICE_FEATURE,
                            "q1_q10_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                                PRIMARY_PRICE_FEATURE
                            ],
                            "q1_q10_volume_feature": PRIMARY_VOLUME_FEATURE,
                            "q1_q10_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                                PRIMARY_VOLUME_FEATURE
                            ],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_q1_q10_combined_bucket": left_bucket,
                            "left_q1_q10_combined_bucket_label": Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
                                left_bucket
                            ],
                            "right_q1_q10_combined_bucket": right_bucket,
                            "right_q1_q10_combined_bucket_label": Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
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
                Q1_Q10_COMBINED_BUCKET_ORDER, 2
            ):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "q1_q10_price_feature": PRIMARY_PRICE_FEATURE,
                        "q1_q10_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "q1_q10_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "q1_q10_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_q1_q10_combined_bucket": left_bucket,
                        "left_q1_q10_combined_bucket_label": Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
                            left_bucket
                        ],
                        "right_q1_q10_combined_bucket": right_bucket,
                        "right_q1_q10_combined_bucket_label": Q1_Q10_COMBINED_BUCKET_LABEL_MAP[
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
                        "q1_q10_price_feature": PRIMARY_PRICE_FEATURE,
                        "q1_q10_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "q1_q10_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "q1_q10_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
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
                    "q1_q10_price_feature": PRIMARY_PRICE_FEATURE,
                    "q1_q10_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_PRICE_FEATURE
                    ],
                    "q1_q10_volume_feature": PRIMARY_VOLUME_FEATURE,
                    "q1_q10_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                        PRIMARY_VOLUME_FEATURE
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
    price_feature: RankingFeatureKey = PRIMARY_PRICE_FEATURE,
    volume_feature: RankingFeatureKey = PRIMARY_VOLUME_FEATURE,
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
    for bucket_key, bucket_deciles in Q10_MIDDLE_PRICE_BUCKET_DECILES.items():
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
    ].map(Q10_MIDDLE_PRICE_BUCKET_LABEL_MAP)
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
    ].map(NESTED_VOLUME_BUCKET_LABEL_MAP)
    panel_df["q10_middle_combined_bucket"] = (
        panel_df["q10_middle_price_bucket"] + "_" + panel_df["q10_middle_volume_bucket"]
    )
    panel_df["q10_middle_combined_bucket_label"] = panel_df[
        "q10_middle_combined_bucket"
    ].map(Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP)
    panel_df["q10_middle_price_feature"] = price_feature
    panel_df["q10_middle_price_feature_label"] = RANKING_FEATURE_LABEL_MAP[
        price_feature
    ]
    panel_df["q10_middle_volume_feature"] = volume_feature
    panel_df["q10_middle_volume_feature_label"] = RANKING_FEATURE_LABEL_MAP[
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
        frame["horizon_days"] = HORIZON_DAY_MAP[horizon_key]
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
        return pd.DataFrame(columns=list(Q10_MIDDLE_COMBINED_BUCKET_ORDER))
    pivot_df = (
        scoped_df.pivot(
            index="date",
            columns="q10_middle_combined_bucket",
            values=value_column,
        )
        .reindex(columns=list(Q10_MIDDLE_COMBINED_BUCKET_ORDER))
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
                    Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
                ):
                    records.append(
                        {
                            "q10_middle_price_feature": PRIMARY_PRICE_FEATURE,
                            "q10_middle_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                                PRIMARY_PRICE_FEATURE
                            ],
                            "q10_middle_volume_feature": PRIMARY_VOLUME_FEATURE,
                            "q10_middle_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                                PRIMARY_VOLUME_FEATURE
                            ],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "left_q10_middle_combined_bucket": left_bucket,
                            "left_q10_middle_combined_bucket_label": Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
                                left_bucket
                            ],
                            "right_q10_middle_combined_bucket": right_bucket,
                            "right_q10_middle_combined_bucket_label": Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
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
                Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
            ):
                left = pivot_df[left_bucket].to_numpy(dtype=float)
                right = pivot_df[right_bucket].to_numpy(dtype=float)
                paired_t_statistic, paired_t_p_value = _safe_paired_t_test(left, right)
                wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                records.append(
                    {
                        "q10_middle_price_feature": PRIMARY_PRICE_FEATURE,
                        "q10_middle_price_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_PRICE_FEATURE
                        ],
                        "q10_middle_volume_feature": PRIMARY_VOLUME_FEATURE,
                        "q10_middle_volume_feature_label": RANKING_FEATURE_LABEL_MAP[
                            PRIMARY_VOLUME_FEATURE
                        ],
                        "horizon_key": horizon_key,
                        "metric_key": metric_key,
                        "left_q10_middle_combined_bucket": left_bucket,
                        "left_q10_middle_combined_bucket_label": Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
                            left_bucket
                        ],
                        "right_q10_middle_combined_bucket": right_bucket,
                        "right_q10_middle_combined_bucket_label": Q10_MIDDLE_COMBINED_BUCKET_LABEL_MAP[
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
