"""
TOPIX100 price-vs-SMA Q10 bounce research analytics.

This module narrows the broader `price / SMA` study to the bounce-oriented
slice: `middle` vs `q10`, split by a selected volume-SMA high/low lens. The
main read is whether `q10_volume_low` outperforms the other buckets on future
returns.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any, Literal

import pandas as pd

from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP,
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRIMARY_VOLUME_FEATURE,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
    Topix100PriceVsSmaRankFutureCloseResearchResult,
    _sort_frame as _sort_price_frame,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix_rank_future_close_core import (
    HORIZON_ORDER,
    METRIC_ORDER,
    _holm_adjust,
    _safe_paired_t_test,
    _safe_wilcoxon,
)

HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
Q10MiddleCombinedBucketKey = Literal[
    "middle_volume_high",
    "middle_volume_low",
    "q10_volume_high",
    "q10_volume_low",
]

Q10_MIDDLE_COMBINED_BUCKET_ORDER: tuple[Q10MiddleCombinedBucketKey, ...] = (
    "middle_volume_high",
    "middle_volume_low",
    "q10_volume_high",
    "q10_volume_low",
)
Q10_LOW_HYPOTHESIS_LABELS: tuple[
    tuple[Q10MiddleCombinedBucketKey, Q10MiddleCombinedBucketKey, str], ...
] = (
    ("q10_volume_low", "q10_volume_high", "Q10 Low vs Q10 High"),
    ("q10_volume_low", "middle_volume_low", "Q10 Low vs Middle Low"),
    ("q10_volume_low", "middle_volume_high", "Q10 Low vs Middle High"),
)


@dataclass(frozen=True)
class Topix100PriceVsSmaQ10BounceResearchResult:
    base_result: Topix100PriceVsSmaRankFutureCloseResearchResult
    price_feature_order: tuple[str, ...]
    volume_feature_order: tuple[str, ...]
    q10_middle_volume_split_panel_df: pd.DataFrame
    q10_middle_volume_split_daily_means_df: pd.DataFrame
    q10_middle_volume_split_summary_df: pd.DataFrame
    q10_middle_volume_split_pairwise_significance_df: pd.DataFrame
    q10_low_hypothesis_df: pd.DataFrame
    q10_low_spread_daily_df: pd.DataFrame
    q10_low_scorecard_df: pd.DataFrame


def _normalize_price_features(
    price_features: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if price_features is None:
        return PRICE_FEATURE_ORDER
    requested = tuple(dict.fromkeys(str(feature) for feature in price_features))
    requested_set = set(requested)
    unsupported = sorted(requested_set - set(PRICE_FEATURE_ORDER))
    if unsupported:
        raise ValueError(
            "Unsupported price_features: "
            f"{unsupported}. Supported features are {list(PRICE_FEATURE_ORDER)}."
        )
    normalized = tuple(
        feature for feature in PRICE_FEATURE_ORDER if feature in requested_set
    )
    if not normalized:
        raise ValueError("price_features must include at least one feature.")
    return normalized


def _normalize_volume_features(
    volume_features: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if volume_features is None:
        return (PRIMARY_VOLUME_FEATURE,)
    requested = tuple(dict.fromkeys(str(feature) for feature in volume_features))
    requested_set = set(requested)
    unsupported = sorted(requested_set - set(VOLUME_FEATURE_ORDER))
    if unsupported:
        raise ValueError(
            "Unsupported volume_features: "
            f"{unsupported}. Supported features are {list(VOLUME_FEATURE_ORDER)}."
        )
    normalized = tuple(
        feature for feature in VOLUME_FEATURE_ORDER if feature in requested_set
    )
    if not normalized:
        raise ValueError("volume_features must include at least one feature.")
    return normalized


def _resolve_volume_sma_windows(
    volume_feature_order: tuple[str, ...],
) -> tuple[tuple[int, int], ...]:
    feature_to_window = {
        feature: window
        for feature, window in zip(VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True)
    }
    return tuple(feature_to_window[feature] for feature in volume_feature_order)


def _filter_q10_middle_volume_split_panel(
    base_result: Topix100PriceVsSmaRankFutureCloseResearchResult,
    *,
    price_feature_order: tuple[str, ...],
    volume_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    panel_df = base_result.price_volume_split_panel_df
    if panel_df.empty:
        return pd.DataFrame()

    filtered = panel_df.loc[
        panel_df["price_feature"].isin(price_feature_order)
        & panel_df["volume_feature"].isin(volume_feature_order)
        & panel_df["combined_bucket"].isin(Q10_MIDDLE_COMBINED_BUCKET_ORDER)
    ].copy()
    return _sort_price_frame(filtered, known_feature_order=price_feature_order)


def _filter_q10_middle_volume_daily_means(
    base_result: Topix100PriceVsSmaRankFutureCloseResearchResult,
    *,
    price_feature_order: tuple[str, ...],
    volume_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    daily_means_df = base_result.price_volume_split_daily_means_df
    if daily_means_df.empty:
        return pd.DataFrame()

    filtered = daily_means_df.loc[
        daily_means_df["price_feature"].isin(price_feature_order)
        & daily_means_df["volume_feature"].isin(volume_feature_order)
        & daily_means_df["combined_bucket"].isin(Q10_MIDDLE_COMBINED_BUCKET_ORDER)
    ].copy()
    return _sort_price_frame(filtered, known_feature_order=price_feature_order)


def _summarize_q10_middle_volume_split(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
    *,
    price_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    if q10_middle_volume_split_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = (
        q10_middle_volume_split_daily_means_df.groupby(
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
    return _sort_price_frame(summary_df, known_feature_order=price_feature_order)


def _aligned_q10_middle_combined_pivot(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
    *,
    price_feature: str,
    volume_feature: str,
    horizon_key: HorizonKey,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = q10_middle_volume_split_daily_means_df.loc[
        (q10_middle_volume_split_daily_means_df["price_feature"] == price_feature)
        & (q10_middle_volume_split_daily_means_df["volume_feature"] == volume_feature)
        & (q10_middle_volume_split_daily_means_df["horizon_key"] == horizon_key)
    ]
    if scoped_df.empty:
        return pd.DataFrame(columns=list(Q10_MIDDLE_COMBINED_BUCKET_ORDER))

    pivot_df = (
        scoped_df.pivot(index="date", columns="combined_bucket", values=value_column)
        .reindex(columns=list(Q10_MIDDLE_COMBINED_BUCKET_ORDER))
        .dropna()
    )
    pivot_df.index = pivot_df.index.astype(str)
    return pivot_df


def _build_q10_middle_volume_pairwise_significance(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
    *,
    price_feature_order: tuple[str, ...],
    volume_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    if q10_middle_volume_split_daily_means_df.empty:
        return pd.DataFrame()

    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    records: list[dict[str, Any]] = []
    for price_feature in price_feature_order:
        for volume_feature in volume_feature_order:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    pivot_df = _aligned_q10_middle_combined_pivot(
                        q10_middle_volume_split_daily_means_df,
                        price_feature=price_feature,
                        volume_feature=volume_feature,
                        horizon_key=horizon_key,
                        value_column=metric_columns[metric_key],
                    )
                    if pivot_df.empty:
                        for left_bucket, right_bucket in combinations(
                            Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
                        ):
                            records.append(
                                {
                                    "price_feature": price_feature,
                                    "price_feature_label": PRICE_FEATURE_LABEL_MAP[
                                        price_feature
                                    ],
                                    "volume_feature": volume_feature,
                                    "volume_feature_label": VOLUME_FEATURE_LABEL_MAP[
                                        volume_feature
                                    ],
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
                        Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
                    ):
                        left = pivot_df[left_bucket].to_numpy(dtype=float)
                        right = pivot_df[right_bucket].to_numpy(dtype=float)
                        paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                            left, right
                        )
                        wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(left, right)
                        records.append(
                            {
                                "price_feature": price_feature,
                                "price_feature_label": PRICE_FEATURE_LABEL_MAP[
                                    price_feature
                                ],
                                "volume_feature": volume_feature,
                                "volume_feature_label": VOLUME_FEATURE_LABEL_MAP[
                                    volume_feature
                                ],
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
    if pairwise_df.empty:
        return pairwise_df

    pairwise_df["paired_t_p_value_holm"] = None
    pairwise_df["wilcoxon_p_value_holm"] = None
    for price_feature in price_feature_order:
        for volume_feature in volume_feature_order:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    mask = (
                        (pairwise_df["price_feature"] == price_feature)
                        & (pairwise_df["volume_feature"] == volume_feature)
                        & (pairwise_df["horizon_key"] == horizon_key)
                        & (pairwise_df["metric_key"] == metric_key)
                    )
                    pairwise_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                        pairwise_df.loc[mask, "paired_t_p_value"].tolist()
                    )
                    pairwise_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                        pairwise_df.loc[mask, "wilcoxon_p_value"].tolist()
                    )
    return _sort_price_frame(pairwise_df, known_feature_order=price_feature_order)


def _build_q10_low_hypothesis(
    q10_middle_volume_split_pairwise_significance_df: pd.DataFrame,
    *,
    price_feature_order: tuple[str, ...],
    volume_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    if q10_middle_volume_split_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for price_feature in price_feature_order:
        for volume_feature in volume_feature_order:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    scoped_df = q10_middle_volume_split_pairwise_significance_df.loc[
                        (q10_middle_volume_split_pairwise_significance_df["price_feature"] == price_feature)
                        & (q10_middle_volume_split_pairwise_significance_df["volume_feature"] == volume_feature)
                        & (q10_middle_volume_split_pairwise_significance_df["horizon_key"] == horizon_key)
                        & (q10_middle_volume_split_pairwise_significance_df["metric_key"] == metric_key)
                    ]
                    for left_bucket, right_bucket, hypothesis_label in (
                        Q10_LOW_HYPOTHESIS_LABELS
                    ):
                        row = scoped_df.loc[
                            (scoped_df["left_combined_bucket"] == left_bucket)
                            & (scoped_df["right_combined_bucket"] == right_bucket)
                        ]
                        sign = 1.0
                        if row.empty:
                            row = scoped_df.loc[
                                (scoped_df["left_combined_bucket"] == right_bucket)
                                & (scoped_df["right_combined_bucket"] == left_bucket)
                            ]
                            sign = -1.0
                        if row.empty:
                            records.append(
                                {
                                    "price_feature": price_feature,
                                    "price_feature_label": PRICE_FEATURE_LABEL_MAP[
                                        price_feature
                                    ],
                                    "volume_feature": volume_feature,
                                    "volume_feature_label": VOLUME_FEATURE_LABEL_MAP[
                                        volume_feature
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
                        pairwise_row = row.iloc[0]
                        mean_difference = pairwise_row["mean_difference"]
                        records.append(
                            {
                                "price_feature": price_feature,
                                "price_feature_label": PRICE_FEATURE_LABEL_MAP[
                                    price_feature
                                ],
                                "volume_feature": volume_feature,
                                "volume_feature_label": VOLUME_FEATURE_LABEL_MAP[
                                    volume_feature
                                ],
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
                                "paired_t_p_value_holm": pairwise_row[
                                    "paired_t_p_value_holm"
                                ],
                                "wilcoxon_p_value_holm": pairwise_row[
                                    "wilcoxon_p_value_holm"
                                ],
                            }
                        )
    return _sort_price_frame(pd.DataFrame.from_records(records), known_feature_order=price_feature_order)


def _build_q10_low_spread_daily(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
    *,
    price_feature_order: tuple[str, ...],
    volume_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    if q10_middle_volume_split_daily_means_df.empty:
        return pd.DataFrame()

    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    records: list[dict[str, Any]] = []
    for price_feature in price_feature_order:
        for volume_feature in volume_feature_order:
            for horizon_key in HORIZON_ORDER:
                for metric_key, value_column in metric_columns.items():
                    pivot_df = _aligned_q10_middle_combined_pivot(
                        q10_middle_volume_split_daily_means_df,
                        price_feature=price_feature,
                        volume_feature=volume_feature,
                        horizon_key=horizon_key,
                        value_column=value_column,
                    )
                    if pivot_df.empty:
                        continue
                    for (
                        left_bucket,
                        right_bucket,
                        hypothesis_label,
                    ) in Q10_LOW_HYPOTHESIS_LABELS:
                        for date, row in pivot_df.iterrows():
                            left_value = float(row[left_bucket])
                            right_value = float(row[right_bucket])
                            records.append(
                                {
                                    "price_feature": price_feature,
                                    "price_feature_label": PRICE_FEATURE_LABEL_MAP[
                                        price_feature
                                    ],
                                    "volume_feature": volume_feature,
                                    "volume_feature_label": VOLUME_FEATURE_LABEL_MAP[
                                        volume_feature
                                    ],
                                    "horizon_key": horizon_key,
                                    "metric_key": metric_key,
                                    "date": str(date),
                                    "hypothesis_label": hypothesis_label,
                                    "left_combined_bucket": left_bucket,
                                    "left_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                                        left_bucket
                                    ],
                                    "right_combined_bucket": right_bucket,
                                    "right_combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                                        right_bucket
                                    ],
                                    "left_value": left_value,
                                    "right_value": right_value,
                                    "mean_difference": left_value - right_value,
                                }
                            )
    return _sort_price_frame(pd.DataFrame.from_records(records), known_feature_order=price_feature_order)


def _build_q10_low_scorecard(
    q10_low_spread_daily_df: pd.DataFrame,
    q10_low_hypothesis_df: pd.DataFrame,
    *,
    price_feature_order: tuple[str, ...],
) -> pd.DataFrame:
    if q10_low_spread_daily_df.empty:
        return pd.DataFrame()

    scorecard_df = (
        q10_low_spread_daily_df.groupby(
            [
                "price_feature",
                "price_feature_label",
                "volume_feature",
                "volume_feature_label",
                "horizon_key",
                "metric_key",
                "hypothesis_label",
                "left_combined_bucket",
                "left_combined_bucket_label",
                "right_combined_bucket",
                "right_combined_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            n_dates=("date", "nunique"),
            mean_difference=("mean_difference", "mean"),
            median_difference=("mean_difference", "median"),
            positive_date_share=("mean_difference", lambda values: float((values > 0).mean())),
            negative_date_share=("mean_difference", lambda values: float((values < 0).mean())),
        )
    )
    merged = scorecard_df.merge(
        q10_low_hypothesis_df[
            [
                "price_feature",
                "volume_feature",
                "horizon_key",
                "metric_key",
                "hypothesis_label",
                "paired_t_p_value_holm",
                "wilcoxon_p_value_holm",
            ]
        ],
        on=[
            "price_feature",
            "volume_feature",
            "horizon_key",
            "metric_key",
            "hypothesis_label",
        ],
        how="left",
    )
    return _sort_price_frame(merged, known_feature_order=price_feature_order)


def run_topix100_price_vs_sma_q10_bounce_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
    price_features: tuple[str, ...] | list[str] | None = None,
    volume_features: tuple[str, ...] | list[str] | None = None,
) -> Topix100PriceVsSmaQ10BounceResearchResult:
    volume_feature_order = _normalize_volume_features(volume_features)
    base_result = run_topix100_price_vs_sma_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        volume_sma_windows=_resolve_volume_sma_windows(volume_feature_order),
    )
    price_feature_order = _normalize_price_features(price_features)
    q10_middle_volume_split_panel_df = _filter_q10_middle_volume_split_panel(
        base_result,
        price_feature_order=price_feature_order,
        volume_feature_order=volume_feature_order,
    )
    q10_middle_volume_split_daily_means_df = _filter_q10_middle_volume_daily_means(
        base_result,
        price_feature_order=price_feature_order,
        volume_feature_order=volume_feature_order,
    )
    q10_middle_volume_split_summary_df = _summarize_q10_middle_volume_split(
        q10_middle_volume_split_daily_means_df,
        price_feature_order=price_feature_order,
    )
    q10_middle_volume_split_pairwise_significance_df = (
        _build_q10_middle_volume_pairwise_significance(
            q10_middle_volume_split_daily_means_df,
            price_feature_order=price_feature_order,
            volume_feature_order=volume_feature_order,
        )
    )
    q10_low_hypothesis_df = _build_q10_low_hypothesis(
        q10_middle_volume_split_pairwise_significance_df,
        price_feature_order=price_feature_order,
        volume_feature_order=volume_feature_order,
    )
    q10_low_spread_daily_df = _build_q10_low_spread_daily(
        q10_middle_volume_split_daily_means_df,
        price_feature_order=price_feature_order,
        volume_feature_order=volume_feature_order,
    )
    q10_low_scorecard_df = _build_q10_low_scorecard(
        q10_low_spread_daily_df,
        q10_low_hypothesis_df,
        price_feature_order=price_feature_order,
    )

    return Topix100PriceVsSmaQ10BounceResearchResult(
        base_result=base_result,
        price_feature_order=price_feature_order,
        volume_feature_order=volume_feature_order,
        q10_middle_volume_split_panel_df=q10_middle_volume_split_panel_df,
        q10_middle_volume_split_daily_means_df=q10_middle_volume_split_daily_means_df,
        q10_middle_volume_split_summary_df=q10_middle_volume_split_summary_df,
        q10_middle_volume_split_pairwise_significance_df=q10_middle_volume_split_pairwise_significance_df,
        q10_low_hypothesis_df=q10_low_hypothesis_df,
        q10_low_spread_daily_df=q10_low_spread_daily_df,
        q10_low_scorecard_df=q10_low_scorecard_df,
    )
