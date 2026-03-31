"""
TOPIX100 price-vs-SMA Q10 bounce research analytics.

This module narrows the broader `price / SMA` study to the bounce-oriented
slice: `middle` vs `q10`, split by a selected volume-SMA high/low lens. The
main read is whether `q10_volume_low` outperforms the other buckets on future
returns.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from itertools import combinations
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP,
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRIMARY_VOLUME_FEATURE,
    Topix100PriceVsSmaRankFutureCloseResearchResult,
    _build_research_result_from_payload as _build_price_vs_sma_result_from_payload,
    _split_research_result_payload as _split_price_vs_sma_result_payload,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
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
TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-price-vs-sma-q10-bounce"
)
_BASE_RESULT_TABLE_PREFIX = "base__"


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


def write_topix100_price_vs_sma_q10_bounce_research_bundle(
    result: Topix100PriceVsSmaQ10BounceResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_q10_bounce_research_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_price_vs_sma_q10_bounce_research",
        params={
            "start_date": result.base_result.analysis_start_date,
            "end_date": result.base_result.analysis_end_date,
            "lookback_years": result.base_result.lookback_years,
            "min_constituents_per_day": result.base_result.min_constituents_per_day,
            "price_features": list(result.price_feature_order),
            "volume_features": list(result.volume_feature_order),
        },
        db_path=result.base_result.db_path,
        analysis_start_date=result.base_result.analysis_start_date,
        analysis_end_date=result.base_result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_q10_bounce_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_price_vs_sma_q10_bounce_research_bundle(
    bundle_path: str | Path,
) -> Topix100PriceVsSmaQ10BounceResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_q10_bounce_research_result_from_payload(
        dict(info.result_metadata),
        tables,
    )


def get_topix100_price_vs_sma_q10_bounce_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_q10_bounce_research_result_payload(
    result: Topix100PriceVsSmaQ10BounceResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata: dict[str, Any] = {}
    tables: dict[str, pd.DataFrame] = {}
    for field in fields(result):
        value = getattr(result, field.name)
        if field.name == "base_result":
            base_metadata, base_tables = _split_price_vs_sma_result_payload(value)
            metadata["base_result_metadata"] = base_metadata
            tables.update(
                {
                    f"{_BASE_RESULT_TABLE_PREFIX}{table_name}": dataframe
                    for table_name, dataframe in base_tables.items()
                }
            )
            continue
        if isinstance(value, pd.DataFrame):
            tables[field.name] = value
        else:
            metadata[field.name] = value
    return metadata, tables


def _build_q10_bounce_research_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100PriceVsSmaQ10BounceResearchResult:
    normalized = dict(metadata)
    normalized["price_feature_order"] = tuple(
        str(name) for name in normalized["price_feature_order"]
    )
    normalized["volume_feature_order"] = tuple(
        str(name) for name in normalized["volume_feature_order"]
    )
    base_tables = {
        table_name.removeprefix(_BASE_RESULT_TABLE_PREFIX): dataframe
        for table_name, dataframe in tables.items()
        if table_name.startswith(_BASE_RESULT_TABLE_PREFIX)
    }
    q10_tables = {
        table_name: dataframe
        for table_name, dataframe in tables.items()
        if not table_name.startswith(_BASE_RESULT_TABLE_PREFIX)
    }
    base_result = _build_price_vs_sma_result_from_payload(
        cast(dict[str, Any], normalized["base_result_metadata"]),
        base_tables,
    )
    return Topix100PriceVsSmaQ10BounceResearchResult(
        base_result=base_result,
        price_feature_order=cast(tuple[str, ...], normalized["price_feature_order"]),
        volume_feature_order=cast(tuple[str, ...], normalized["volume_feature_order"]),
        q10_middle_volume_split_panel_df=q10_tables["q10_middle_volume_split_panel_df"],
        q10_middle_volume_split_daily_means_df=q10_tables[
            "q10_middle_volume_split_daily_means_df"
        ],
        q10_middle_volume_split_summary_df=q10_tables["q10_middle_volume_split_summary_df"],
        q10_middle_volume_split_pairwise_significance_df=q10_tables[
            "q10_middle_volume_split_pairwise_significance_df"
        ],
        q10_low_hypothesis_df=q10_tables["q10_low_hypothesis_df"],
        q10_low_spread_daily_df=q10_tables["q10_low_spread_daily_df"],
        q10_low_scorecard_df=q10_tables["q10_low_scorecard_df"],
    )


def _build_q10_bounce_research_bundle_summary_markdown(
    result: Topix100PriceVsSmaQ10BounceResearchResult,
) -> str:
    base = result.base_result
    summary_lines = [
        "# TOPIX100 Price vs SMA Q10 Bounce",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{base.source_mode}`",
        f"- Available range: `{base.available_start_date} -> {base.available_end_date}`",
        f"- Analysis range: `{base.analysis_start_date} -> {base.analysis_end_date}`",
        f"- Price features: `{', '.join(result.price_feature_order)}`",
        f"- Volume features: `{', '.join(result.volume_feature_order)}`",
        f"- TOPIX100 constituents: `{base.topix100_constituent_count}`",
        f"- Stock-day rows: `{base.stock_day_count}`",
        f"- Valid dates: `{base.valid_date_count}`",
        "",
        "## Current Read",
        "",
    ]
    strongest_rows = result.q10_low_hypothesis_df[
        (result.q10_low_hypothesis_df["metric_key"] == "future_return")
        & (result.q10_low_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.q10_low_hypothesis_df["hypothesis_label"] == "Q10 Low vs Middle High")
        & result.q10_low_hypothesis_df["mean_difference"].notna()
    ].copy()
    if strongest_rows.empty:
        summary_lines.append("- No `Q10 Low vs Middle High` rows were available in this run.")
    else:
        strongest_row = strongest_rows.sort_values("mean_difference", ascending=False).iloc[0]
        summary_lines.extend(
            [
                "- Strongest `Q10 Low vs Middle High` read on `t_plus_10 / future_return`:",
                "  "
                f"`{strongest_row['price_feature']}` x `{strongest_row['volume_feature']}` "
                f"at `{float(strongest_row['mean_difference']):+.4f}%`.",
            ]
        )
    scorecard_rows = result.q10_low_scorecard_df[
        (result.q10_low_scorecard_df["metric_key"] == "future_return")
        & (result.q10_low_scorecard_df["horizon_key"] == "t_plus_10")
        & (result.q10_low_scorecard_df["hypothesis_label"] == "Q10 Low vs Q10 High")
        & result.q10_low_scorecard_df["mean_difference"].notna()
    ].copy()
    if not scorecard_rows.empty:
        strongest_row = scorecard_rows.sort_values("mean_difference", ascending=False).iloc[0]
        summary_lines.append(
            "  "
            f"`Q10 Low vs Q10 High` best row was `{strongest_row['price_feature']}` x "
            f"`{strongest_row['volume_feature']}` with mean spread "
            f"`{float(strongest_row['mean_difference']):+.4f}%` and positive share "
            f"`{float(strongest_row['positive_date_share']):.2%}`."
        )
    summary_lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_q10_bounce_research_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)
