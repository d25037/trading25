"""
TOPIX100 price-vs-SMA Q10 bounce regime conditioning research analytics.

This module conditions the `q10 / middle` bounce slice on same-day TOPIX close
return and NT-ratio return regimes. The default target is `price_vs_sma_50_gap`,
which was the strongest Q10 bounce feature in the prior study.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any

import pandas as pd

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_BUCKET_ORDER,
    NtRatioReturnStats,
)
from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
    Q10_LOW_HYPOTHESIS_LABELS,
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    Topix100PriceVsSmaQ10BounceResearchResult,
    run_topix100_price_vs_sma_q10_bounce_research,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP,
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    TopixCloseReturnStats,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    HORIZON_ORDER,
    METRIC_ORDER,
    _holm_adjust,
    _safe_paired_t_test,
    _safe_wilcoxon,
)
from src.domains.analytics.topix_regime_conditioning_core import (
    DEFAULT_SIGMA_THRESHOLD_1,
    DEFAULT_SIGMA_THRESHOLD_2,
    REGIME_GROUP_LABEL_MAP,
    REGIME_GROUP_ORDER,
    REGIME_LABEL_MAP,
    REGIME_TYPE_ORDER,
    _build_horizon_panel,
    _build_regime_assignments_df,
    _build_regime_daily_means,
    _build_regime_day_counts,
    _build_regime_group_daily_means,
    _build_regime_group_day_counts,
    _build_regime_market_df,
    _query_market_regime_history,
    _summarize_regime_daily_means,
    _summarize_regime_group_daily_means,
    _sort_frame as _sort_regime_frame,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    CLOSE_BUCKET_ORDER,
)

DEFAULT_PRICE_FEATURE = "price_vs_sma_50_gap"


@dataclass(frozen=True)
class Topix100PriceVsSmaQ10BounceRegimeConditioningResearchResult:
    db_path: str
    source_mode: str
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    price_feature: str
    price_feature_label: str
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


def _normalize_price_feature(price_feature: str) -> str:
    normalized = str(price_feature)
    if normalized not in PRICE_FEATURE_ORDER:
        raise ValueError(
            f"Unsupported price_feature: {normalized}. "
            f"Supported features are {list(PRICE_FEATURE_ORDER)}."
        )
    return normalized


def _filter_split_panel(
    base_result: Topix100PriceVsSmaQ10BounceResearchResult,
    *,
    price_feature: str,
) -> pd.DataFrame:
    split_panel_df = base_result.q10_middle_volume_split_panel_df
    if split_panel_df.empty:
        return pd.DataFrame()
    filtered = split_panel_df.loc[
        split_panel_df["price_feature"] == price_feature
    ].copy()
    return _sort_regime_frame(filtered)


def _aligned_regime_pivot(
    regime_daily_means_df: pd.DataFrame,
    *,
    regime_type: str,
    regime_bucket_key: str,
    horizon_key: str,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = regime_daily_means_df[
        (regime_daily_means_df["regime_type"] == regime_type)
        & (regime_daily_means_df["regime_bucket_key"] == regime_bucket_key)
        & (regime_daily_means_df["horizon_key"] == horizon_key)
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


def _aligned_regime_group_pivot(
    regime_group_daily_means_df: pd.DataFrame,
    *,
    regime_type: str,
    regime_group_key: str,
    horizon_key: str,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = regime_group_daily_means_df[
        (regime_group_daily_means_df["regime_type"] == regime_type)
        & (regime_group_daily_means_df["regime_group_key"] == regime_group_key)
        & (regime_group_daily_means_df["horizon_key"] == horizon_key)
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


def _build_regime_pairwise_significance(
    regime_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_daily_means_df.empty:
        return pd.DataFrame()

    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    regime_bucket_orders: dict[str, tuple[str, ...]] = {
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

    records: list[dict[str, Any]] = []
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
                            Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
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
                        Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
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
    if pairwise_df.empty:
        return pairwise_df
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
    return _sort_regime_frame(pairwise_df)


def _build_regime_group_pairwise_significance(
    regime_group_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_group_daily_means_df.empty:
        return pd.DataFrame()

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

    records: list[dict[str, Any]] = []
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
                            Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
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
                        Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
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
    if pairwise_df.empty:
        return pairwise_df
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
    return _sort_regime_frame(pairwise_df)


def _build_regime_hypothesis(
    regime_pairwise_significance_df: pd.DataFrame,
) -> pd.DataFrame:
    if regime_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    regime_bucket_orders: dict[str, tuple[str, ...]] = {
        "topix_close": tuple(CLOSE_BUCKET_ORDER),
        "nt_ratio": tuple(NT_RATIO_BUCKET_ORDER),
    }
    for regime_type in REGIME_TYPE_ORDER:
        for regime_bucket_key in regime_bucket_orders[regime_type]:
            for horizon_key in HORIZON_ORDER:
                for metric_key in METRIC_ORDER:
                    scoped_df = regime_pairwise_significance_df[
                        (regime_pairwise_significance_df["regime_type"] == regime_type)
                        & (regime_pairwise_significance_df["regime_bucket_key"] == regime_bucket_key)
                        & (regime_pairwise_significance_df["horizon_key"] == horizon_key)
                        & (regime_pairwise_significance_df["metric_key"] == metric_key)
                    ]
                    for left_bucket, right_bucket, hypothesis_label in Q10_LOW_HYPOTHESIS_LABELS:
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
                        mean_difference = record.get("mean_difference")
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
                                    None
                                    if mean_difference is None or pd.isna(mean_difference)
                                    else float(mean_difference) * sign
                                ),
                                "paired_t_p_value_holm": record.get("paired_t_p_value_holm"),
                                "wilcoxon_p_value_holm": record.get("wilcoxon_p_value_holm"),
                            }
                        )
    return _sort_regime_frame(pd.DataFrame.from_records(records))


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
                        & (regime_group_pairwise_significance_df["regime_group_key"] == regime_group_key)
                        & (regime_group_pairwise_significance_df["horizon_key"] == horizon_key)
                        & (regime_group_pairwise_significance_df["metric_key"] == metric_key)
                    ]
                    for left_bucket, right_bucket, hypothesis_label in Q10_LOW_HYPOTHESIS_LABELS:
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
                                    "regime_group_label": REGIME_GROUP_LABEL_MAP[regime_group_key],
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
                        mean_difference = record.get("mean_difference")
                        records.append(
                            {
                                "regime_type": regime_type,
                                "regime_label": REGIME_LABEL_MAP[regime_type],
                                "regime_group_key": regime_group_key,
                                "regime_group_label": REGIME_GROUP_LABEL_MAP[regime_group_key],
                                "horizon_key": horizon_key,
                                "metric_key": metric_key,
                                "hypothesis_label": hypothesis_label,
                                "left_combined_bucket": left_bucket,
                                "right_combined_bucket": right_bucket,
                                "mean_difference": (
                                    None
                                    if mean_difference is None or pd.isna(mean_difference)
                                    else float(mean_difference) * sign
                                ),
                                "paired_t_p_value_holm": record.get("paired_t_p_value_holm"),
                                "wilcoxon_p_value_holm": record.get("wilcoxon_p_value_holm"),
                            }
                        )
    return _sort_regime_frame(pd.DataFrame.from_records(records))


def run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    sigma_threshold_1: float = DEFAULT_SIGMA_THRESHOLD_1,
    sigma_threshold_2: float = DEFAULT_SIGMA_THRESHOLD_2,
) -> Topix100PriceVsSmaQ10BounceRegimeConditioningResearchResult:
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")

    normalized_price_feature = _normalize_price_feature(price_feature)
    base_result = run_topix100_price_vs_sma_q10_bounce_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        price_features=[normalized_price_feature],
    )
    split_panel_df = _filter_split_panel(
        base_result,
        price_feature=normalized_price_feature,
    )
    horizon_panel_df = _build_horizon_panel(split_panel_df)

    with _open_analysis_connection(db_path) as ctx:
        raw_market_df = _query_market_regime_history(
            ctx.connection,
            end_date=base_result.base_result.analysis_end_date,
        )
        regime_market_df, topix_close_stats, nt_ratio_stats = _build_regime_market_df(
            raw_market_df,
            start_date=base_result.base_result.analysis_start_date,
            end_date=base_result.base_result.analysis_end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )

    regime_assignments_df = _build_regime_assignments_df(regime_market_df)
    regime_day_counts_df = _build_regime_day_counts(regime_assignments_df)
    regime_group_day_counts_df = _build_regime_group_day_counts(regime_assignments_df)
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

    base_panel = base_result.base_result
    return Topix100PriceVsSmaQ10BounceRegimeConditioningResearchResult(
        db_path=db_path,
        source_mode=base_panel.source_mode,
        source_detail=base_panel.source_detail,
        available_start_date=base_panel.available_start_date,
        available_end_date=base_panel.available_end_date,
        analysis_start_date=base_panel.analysis_start_date,
        analysis_end_date=base_panel.analysis_end_date,
        price_feature=normalized_price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[normalized_price_feature],
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        universe_constituent_count=base_panel.topix100_constituent_count,
        valid_date_count=base_panel.valid_date_count,
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
