"""
TOPIX100 price/SMA50 decile partition research analytics.

This module replaces the hand-picked `Q1 / Q4+Q5+Q6 / Q10` framing with an
exhaustive search over every contiguous three-way partition of the ten
price/SMA50 deciles. Each candidate is then evaluated again under the
`volume_sma_5_20` high/low split so the bundle can answer both:

1. Which contiguous high / middle / low price partition separates best?
2. Does the low-price bucket improve further when volume_sma_5_20 is low?
"""

from __future__ import annotations

from dataclasses import dataclass, fields
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
    Topix100PriceVsSmaRankFutureCloseResearchResult,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
)
from src.domains.analytics.topix_rank_future_close_core import (
    DECILE_ORDER,
    HORIZON_ORDER,
    METRIC_ORDER,
    _build_horizon_panel,
    _holm_adjust,
    _safe_paired_t_test,
    _safe_wilcoxon,
)

HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
PriceGroupKey = Literal["high", "middle", "low"]
VolumeBucketKey = Literal["volume_high", "volume_low"]

BASE_PRICE_FEATURE = "price_vs_sma_50_gap"
PRICE_FEATURE = "price_to_sma_50"
PRICE_FEATURE_LABEL = "Price / SMA50"
VOLUME_FEATURE = "volume_sma_5_20"
VOLUME_FEATURE_LABEL = "Volume SMA 5 / 20"
PRICE_SMA_WINDOW = 50
VOLUME_SMA_WINDOWS: tuple[tuple[int, int], ...] = ((5, 20),)
TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-price-to-sma50-decile-partitions"
)

PRICE_GROUP_ORDER: tuple[PriceGroupKey, ...] = ("high", "middle", "low")
PRICE_GROUP_LABEL_MAP: dict[PriceGroupKey, str] = {
    "high": "High",
    "middle": "Middle",
    "low": "Low",
}
PRICE_HYPOTHESIS_DEFINITIONS: tuple[
    tuple[PriceGroupKey, PriceGroupKey, str, str], ...
] = (
    ("high", "middle", "high_vs_middle", "High vs Middle"),
    ("low", "middle", "low_vs_middle", "Low vs Middle"),
    ("high", "low", "high_vs_low", "High vs Low"),
)
LOW_VOLUME_COMBINED_BUCKET_ORDER: tuple[str, ...] = (
    "middle_volume_high",
    "middle_volume_low",
    "low_volume_high",
    "low_volume_low",
)
LOW_VOLUME_HYPOTHESIS_DEFINITIONS: tuple[tuple[str, str, str, str], ...] = (
    (
        "low_volume_low",
        "low_volume_high",
        "low_volume_low_vs_low_volume_high",
        "Low Volume Low vs Low Volume High",
    ),
    (
        "low_volume_low",
        "middle_volume_low",
        "low_volume_low_vs_middle_volume_low",
        "Low Volume Low vs Middle Volume Low",
    ),
    (
        "low_volume_low",
        "middle_volume_high",
        "low_volume_low_vs_middle_volume_high",
        "Low Volume Low vs Middle Volume High",
    ),
)


@dataclass(frozen=True)
class Topix100PriceToSma50DecilePartitionsResearchResult:
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
    price_feature: str
    price_feature_label: str
    volume_feature: str
    volume_feature_label: str
    candidate_count: int
    decile_profile_df: pd.DataFrame
    decile_threshold_summary_df: pd.DataFrame
    candidate_definition_df: pd.DataFrame
    candidate_price_group_summary_df: pd.DataFrame
    candidate_price_hypothesis_df: pd.DataFrame
    candidate_low_volume_hypothesis_df: pd.DataFrame
    candidate_overall_scorecard_df: pd.DataFrame


def _format_decile_range(deciles: tuple[str, ...]) -> str:
    if not deciles:
        raise ValueError("deciles must not be empty")
    if len(deciles) == 1:
        return deciles[0]
    return f"{deciles[0]}-{deciles[-1]}"


def _build_candidate_key(
    high_deciles: tuple[str, ...],
    middle_deciles: tuple[str, ...],
    low_deciles: tuple[str, ...],
) -> str:
    return (
        f"high_{high_deciles[0].lower()}_{high_deciles[-1].lower()}__"
        f"middle_{middle_deciles[0].lower()}_{middle_deciles[-1].lower()}__"
        f"low_{low_deciles[0].lower()}_{low_deciles[-1].lower()}"
    )


def _build_candidate_definitions() -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    decile_count = len(DECILE_ORDER)
    for high_end in range(1, decile_count - 1):
        for low_start in range(high_end + 2, decile_count + 1):
            high_deciles = tuple(DECILE_ORDER[:high_end])
            middle_deciles = tuple(DECILE_ORDER[high_end : low_start - 1])
            low_deciles = tuple(DECILE_ORDER[low_start - 1 :])
            high_label = _format_decile_range(high_deciles)
            middle_label = _format_decile_range(middle_deciles)
            low_label = _format_decile_range(low_deciles)
            records.append(
                {
                    "candidate_key": _build_candidate_key(
                        high_deciles,
                        middle_deciles,
                        low_deciles,
                    ),
                    "candidate_label": f"{high_label} | {middle_label} | {low_label}",
                    "high_deciles_label": high_label,
                    "middle_deciles_label": middle_label,
                    "low_deciles_label": low_label,
                    "high_deciles": list(high_deciles),
                    "middle_deciles": list(middle_deciles),
                    "low_deciles": list(low_deciles),
                    "high_end_decile": high_deciles[-1],
                    "low_start_decile": low_deciles[0],
                    "high_decile_count": len(high_deciles),
                    "middle_decile_count": len(middle_deciles),
                    "low_decile_count": len(low_deciles),
                    "total_decile_count": (
                        len(high_deciles) + len(middle_deciles) + len(low_deciles)
                    ),
                }
            )
    return pd.DataFrame.from_records(records)


def _build_working_horizon_panel(
    base_result: Topix100PriceVsSmaRankFutureCloseResearchResult,
) -> pd.DataFrame:
    ranked_panel_df = base_result.ranked_panel_df.loc[
        base_result.ranked_panel_df["ranking_feature"] == BASE_PRICE_FEATURE
    ].copy()
    if ranked_panel_df.empty:
        return pd.DataFrame()

    horizon_panel_df = _build_horizon_panel(
        ranked_panel_df,
        known_feature_order=[BASE_PRICE_FEATURE],
    )
    volume_lookup_df = (
        base_result.event_panel_df[["date", "code", VOLUME_FEATURE]]
        .drop_duplicates(subset=["date", "code"])
        .copy()
    )
    horizon_panel_df = horizon_panel_df.merge(
        volume_lookup_df,
        on=["date", "code"],
        how="left",
        validate="many_to_one",
    )
    horizon_panel_df["price_to_sma_50"] = (
        horizon_panel_df["ranking_value"].astype(float) + 1.0
    )
    horizon_panel_df["volume_sma_5_20"] = horizon_panel_df[VOLUME_FEATURE].astype(float)
    return horizon_panel_df


def _build_decile_profile(
    working_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if working_horizon_panel_df.empty:
        return pd.DataFrame()

    summary_df = (
        working_horizon_panel_df.groupby(
            ["horizon_key", "horizon_days", "feature_decile", "feature_decile_label"],
            as_index=False,
        )
        .agg(
            date_count=("date", "nunique"),
            sample_count=("code", "size"),
            mean_group_size=("date", lambda values: float(len(values) / values.nunique())),
            mean_price_to_sma_50=("price_to_sma_50", "mean"),
            median_price_to_sma_50=("price_to_sma_50", "median"),
            mean_volume_sma_5_20=("volume_sma_5_20", "mean"),
            median_volume_sma_5_20=("volume_sma_5_20", "median"),
            mean_future_close=("future_close", "mean"),
            mean_future_return=("future_return", "mean"),
            median_future_return=("future_return", "median"),
            std_future_return=("future_return", "std"),
        )
    )
    order_map = {key: index for index, key in enumerate(DECILE_ORDER, start=1)}
    summary_df["_decile_order"] = summary_df["feature_decile"].map(order_map)
    summary_df["_horizon_order"] = summary_df["horizon_key"].map(
        {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
    )
    return (
        summary_df.sort_values(["_horizon_order", "_decile_order"])
        .drop(columns=["_decile_order", "_horizon_order"])
        .reset_index(drop=True)
    )


def _build_decile_threshold_summary(
    working_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if working_horizon_panel_df.empty:
        return pd.DataFrame()

    per_name_df = (
        working_horizon_panel_df[
            [
                "date",
                "code",
                "feature_decile",
                "feature_decile_label",
                "price_to_sma_50",
                "volume_sma_5_20",
            ]
        ]
        .drop_duplicates(subset=["date", "code"])
        .copy()
    )
    summary_df = (
        per_name_df.groupby(["feature_decile", "feature_decile_label"], as_index=False)
        .agg(
            date_count=("date", "nunique"),
            sample_count=("code", "size"),
            mean_price_to_sma_50=("price_to_sma_50", "mean"),
            median_price_to_sma_50=("price_to_sma_50", "median"),
            p10_price_to_sma_50=("price_to_sma_50", lambda values: float(values.quantile(0.10))),
            p90_price_to_sma_50=("price_to_sma_50", lambda values: float(values.quantile(0.90))),
            mean_volume_sma_5_20=("volume_sma_5_20", "mean"),
            median_volume_sma_5_20=("volume_sma_5_20", "median"),
            p10_volume_sma_5_20=("volume_sma_5_20", lambda values: float(values.quantile(0.10))),
            p90_volume_sma_5_20=("volume_sma_5_20", lambda values: float(values.quantile(0.90))),
        )
    )
    order_map = {key: index for index, key in enumerate(DECILE_ORDER, start=1)}
    summary_df["_decile_order"] = summary_df["feature_decile"].map(order_map)
    return (
        summary_df.sort_values("_decile_order")
        .drop(columns=["_decile_order"])
        .reset_index(drop=True)
    )


def _candidate_group_maps(
    candidate_row: pd.Series,
) -> tuple[dict[str, PriceGroupKey], dict[PriceGroupKey, str]]:
    group_map: dict[str, PriceGroupKey] = {}
    for decile in cast(list[str], candidate_row["high_deciles"]):
        group_map[decile] = "high"
    for decile in cast(list[str], candidate_row["middle_deciles"]):
        group_map[decile] = "middle"
    for decile in cast(list[str], candidate_row["low_deciles"]):
        group_map[decile] = "low"
    group_decile_label_map: dict[PriceGroupKey, str] = {
        "high": cast(str, candidate_row["high_deciles_label"]),
        "middle": cast(str, candidate_row["middle_deciles_label"]),
        "low": cast(str, candidate_row["low_deciles_label"]),
    }
    return group_map, group_decile_label_map


def _build_candidate_price_group_summary(
    base_result: Topix100PriceVsSmaRankFutureCloseResearchResult,
    candidate_definition_df: pd.DataFrame,
) -> pd.DataFrame:
    daily_decile_means_df = base_result.daily_group_means_df.loc[
        base_result.daily_group_means_df["ranking_feature"] == BASE_PRICE_FEATURE
    ].copy()
    if daily_decile_means_df.empty or candidate_definition_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for _, candidate_row in candidate_definition_df.iterrows():
        group_map, group_decile_label_map = _candidate_group_maps(candidate_row)
        scoped_df = daily_decile_means_df.copy()
        scoped_df["price_group"] = scoped_df["feature_decile"].map(group_map)
        scoped_df["price_group_label"] = scoped_df["price_group"].map(PRICE_GROUP_LABEL_MAP)
        scoped_df["group_deciles_label"] = scoped_df["price_group"].map(group_decile_label_map)
        scoped_df["weighted_price_to_sma_50"] = (
            (scoped_df["group_mean_ranking_value"].astype(float) + 1.0)
            * scoped_df["group_sample_count"].astype(float)
        )
        scoped_df["weighted_event_close"] = (
            scoped_df["group_mean_event_close"].astype(float)
            * scoped_df["group_sample_count"].astype(float)
        )
        scoped_df["weighted_future_close"] = (
            scoped_df["group_mean_future_close"].astype(float)
            * scoped_df["group_sample_count"].astype(float)
        )
        scoped_df["weighted_future_return"] = (
            scoped_df["group_mean_future_return"].astype(float)
            * scoped_df["group_sample_count"].astype(float)
        )
        candidate_daily_df = (
            scoped_df.groupby(
                [
                    "horizon_key",
                    "horizon_days",
                    "date",
                    "price_group",
                    "price_group_label",
                    "group_deciles_label",
                ],
                as_index=False,
            )
            .agg(
                group_sample_count=("group_sample_count", "sum"),
                weighted_price_to_sma_50=("weighted_price_to_sma_50", "sum"),
                weighted_event_close=("weighted_event_close", "sum"),
                weighted_future_close=("weighted_future_close", "sum"),
                weighted_future_return=("weighted_future_return", "sum"),
            )
        )
        candidate_daily_df["group_mean_price_to_sma_50"] = (
            candidate_daily_df["weighted_price_to_sma_50"]
            / candidate_daily_df["group_sample_count"]
        )
        candidate_daily_df["group_mean_event_close"] = (
            candidate_daily_df["weighted_event_close"]
            / candidate_daily_df["group_sample_count"]
        )
        candidate_daily_df["group_mean_future_close"] = (
            candidate_daily_df["weighted_future_close"]
            / candidate_daily_df["group_sample_count"]
        )
        candidate_daily_df["group_mean_future_return"] = (
            candidate_daily_df["weighted_future_return"]
            / candidate_daily_df["group_sample_count"]
        )
        candidate_summary_df = (
            candidate_daily_df.groupby(
                [
                    "horizon_key",
                    "horizon_days",
                    "price_group",
                    "price_group_label",
                    "group_deciles_label",
                ],
                as_index=False,
            )
            .agg(
                date_count=("date", "nunique"),
                mean_group_size=("group_sample_count", "mean"),
                mean_price_to_sma_50=("group_mean_price_to_sma_50", "mean"),
                mean_event_close=("group_mean_event_close", "mean"),
                mean_future_close=("group_mean_future_close", "mean"),
                mean_future_return=("group_mean_future_return", "mean"),
                std_future_return=("group_mean_future_return", "std"),
            )
        )
        for raw_row in candidate_summary_df.to_dict(orient="records"):
            row = cast(dict[str, Any], raw_row)
            row.update(
                {
                    "candidate_key": candidate_row["candidate_key"],
                    "candidate_label": candidate_row["candidate_label"],
                    "high_deciles_label": candidate_row["high_deciles_label"],
                    "middle_deciles_label": candidate_row["middle_deciles_label"],
                    "low_deciles_label": candidate_row["low_deciles_label"],
                }
            )
            records.append(row)

    summary_df = pd.DataFrame.from_records(records)
    if summary_df.empty:
        return summary_df
    summary_df["_horizon_order"] = summary_df["horizon_key"].map(
        {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
    )
    summary_df["_group_order"] = summary_df["price_group"].map(
        {key: index for index, key in enumerate(PRICE_GROUP_ORDER, start=1)}
    )
    return (
        summary_df.sort_values(["candidate_label", "_horizon_order", "_group_order"])
        .drop(columns=["_horizon_order", "_group_order"])
        .reset_index(drop=True)
    )


def _build_candidate_price_hypothesis(
    base_result: Topix100PriceVsSmaRankFutureCloseResearchResult,
    candidate_definition_df: pd.DataFrame,
) -> pd.DataFrame:
    daily_decile_means_df = base_result.daily_group_means_df.loc[
        base_result.daily_group_means_df["ranking_feature"] == BASE_PRICE_FEATURE
    ].copy()
    if daily_decile_means_df.empty or candidate_definition_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for _, candidate_row in candidate_definition_df.iterrows():
        group_map, group_decile_label_map = _candidate_group_maps(candidate_row)
        scoped_df = daily_decile_means_df.copy()
        scoped_df["price_group"] = scoped_df["feature_decile"].map(group_map)
        scoped_df["weighted_future_close"] = (
            scoped_df["group_mean_future_close"].astype(float)
            * scoped_df["group_sample_count"].astype(float)
        )
        scoped_df["weighted_future_return"] = (
            scoped_df["group_mean_future_return"].astype(float)
            * scoped_df["group_sample_count"].astype(float)
        )
        candidate_daily_df = (
            scoped_df.groupby(
                ["horizon_key", "horizon_days", "date", "price_group"],
                as_index=False,
            )
            .agg(
                group_sample_count=("group_sample_count", "sum"),
                weighted_future_close=("weighted_future_close", "sum"),
                weighted_future_return=("weighted_future_return", "sum"),
            )
        )
        candidate_daily_df["group_mean_future_close"] = (
            candidate_daily_df["weighted_future_close"]
            / candidate_daily_df["group_sample_count"]
        )
        candidate_daily_df["group_mean_future_return"] = (
            candidate_daily_df["weighted_future_return"]
            / candidate_daily_df["group_sample_count"]
        )
        for horizon_key in HORIZON_ORDER:
            horizon_df = candidate_daily_df.loc[
                candidate_daily_df["horizon_key"] == horizon_key
            ].copy()
            if horizon_df.empty:
                continue
            for metric_key, value_column in metric_columns.items():
                pivot_df = (
                    horizon_df.pivot(
                        index="date",
                        columns="price_group",
                        values=value_column,
                    )
                    .reindex(columns=list(PRICE_GROUP_ORDER))
                    .dropna()
                )
                pivot_df.index = pivot_df.index.astype(str)
                p_value_rows: list[tuple[str, str, float | None, float | None]] = []
                for (
                    left_group,
                    right_group,
                    hypothesis_key,
                    hypothesis_label,
                ) in PRICE_HYPOTHESIS_DEFINITIONS:
                    if pivot_df.empty:
                        left = right = pd.Series(dtype=float)
                    else:
                        left = pivot_df[left_group].astype(float)
                        right = pivot_df[right_group].astype(float)
                    mean_diff_series = left - right
                    paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                        left.to_numpy(dtype=float),
                        right.to_numpy(dtype=float),
                    )
                    wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                        left.to_numpy(dtype=float),
                        right.to_numpy(dtype=float),
                    )
                    records.append(
                        {
                            "candidate_key": candidate_row["candidate_key"],
                            "candidate_label": candidate_row["candidate_label"],
                            "high_deciles_label": candidate_row["high_deciles_label"],
                            "middle_deciles_label": candidate_row["middle_deciles_label"],
                            "low_deciles_label": candidate_row["low_deciles_label"],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "hypothesis_key": hypothesis_key,
                            "hypothesis_label": hypothesis_label,
                            "left_group": left_group,
                            "left_group_label": PRICE_GROUP_LABEL_MAP[left_group],
                            "left_group_deciles_label": group_decile_label_map[left_group],
                            "right_group": right_group,
                            "right_group_label": PRICE_GROUP_LABEL_MAP[right_group],
                            "right_group_deciles_label": group_decile_label_map[right_group],
                            "n_dates": int(len(pivot_df)),
                            "mean_difference": (
                                None
                                if mean_diff_series.empty
                                else float(mean_diff_series.mean())
                            ),
                            "median_difference": (
                                None
                                if mean_diff_series.empty
                                else float(mean_diff_series.median())
                            ),
                            "positive_date_share": (
                                None
                                if mean_diff_series.empty
                                else float((mean_diff_series > 0).mean())
                            ),
                            "negative_date_share": (
                                None
                                if mean_diff_series.empty
                                else float((mean_diff_series < 0).mean())
                            ),
                            "paired_t_statistic": paired_t_statistic,
                            "paired_t_p_value": paired_t_p_value,
                            "wilcoxon_statistic": wilcoxon_statistic,
                            "wilcoxon_p_value": wilcoxon_p_value,
                        }
                    )
                    p_value_rows.append(
                        (
                            hypothesis_key,
                            hypothesis_label,
                            paired_t_p_value,
                            wilcoxon_p_value,
                        )
                    )

    hypothesis_df = pd.DataFrame.from_records(records)
    if hypothesis_df.empty:
        return hypothesis_df

    hypothesis_df["paired_t_p_value_holm"] = None
    hypothesis_df["wilcoxon_p_value_holm"] = None
    for candidate_key in hypothesis_df["candidate_key"].unique().tolist():
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                mask = (
                    (hypothesis_df["candidate_key"] == candidate_key)
                    & (hypothesis_df["horizon_key"] == horizon_key)
                    & (hypothesis_df["metric_key"] == metric_key)
                )
                paired_t_p_values = cast(
                    list[float | None],
                    hypothesis_df.loc[mask, "paired_t_p_value"].tolist(),
                )
                wilcoxon_p_values = cast(
                    list[float | None],
                    hypothesis_df.loc[mask, "wilcoxon_p_value"].tolist(),
                )
                hypothesis_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                    paired_t_p_values
                )
                hypothesis_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                    wilcoxon_p_values
                )
    hypothesis_df["_horizon_order"] = hypothesis_df["horizon_key"].map(
        {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
    )
    hypothesis_df["_metric_order"] = hypothesis_df["metric_key"].map(
        {key: index for index, key in enumerate(METRIC_ORDER, start=1)}
    )
    hypothesis_df["_hypothesis_order"] = hypothesis_df["hypothesis_key"].map(
        {key: index for index, (_, _, key, _) in enumerate(PRICE_HYPOTHESIS_DEFINITIONS, start=1)}
    )
    return (
        hypothesis_df.sort_values(
            ["candidate_label", "_horizon_order", "_metric_order", "_hypothesis_order"]
        )
        .drop(columns=["_horizon_order", "_metric_order", "_hypothesis_order"])
        .reset_index(drop=True)
    )


def _build_candidate_low_volume_hypothesis(
    working_horizon_panel_df: pd.DataFrame,
    candidate_definition_df: pd.DataFrame,
) -> pd.DataFrame:
    if working_horizon_panel_df.empty or candidate_definition_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    for _, candidate_row in candidate_definition_df.iterrows():
        group_map, _ = _candidate_group_maps(candidate_row)
        scoped_df = working_horizon_panel_df.copy()
        scoped_df["price_group"] = scoped_df["feature_decile"].map(group_map)
        scoped_df["price_group_label"] = scoped_df["price_group"].map(PRICE_GROUP_LABEL_MAP)
        scoped_df = scoped_df.loc[scoped_df["price_group"].isin(["middle", "low"])].copy()
        if scoped_df.empty:
            continue
        scoped_df["price_group_size"] = scoped_df.groupby(
            ["horizon_key", "date", "price_group"]
        )["code"].transform("size")
        scoped_df["volume_rank_desc_within_price_group"] = (
            scoped_df.groupby(["horizon_key", "date", "price_group"])["volume_sma_5_20"]
            .rank(method="first", ascending=False)
            .astype(int)
        )
        scoped_df["volume_bucket_index"] = (
            ((scoped_df["volume_rank_desc_within_price_group"] - 1) * 2)
            // scoped_df["price_group_size"]
        ) + 1
        scoped_df["volume_bucket_index"] = scoped_df["volume_bucket_index"].clip(1, 2)
        scoped_df["volume_bucket"] = scoped_df["volume_bucket_index"].map(
            cast(dict[int, VolumeBucketKey], {1: "volume_high", 2: "volume_low"})
        )
        scoped_df["volume_bucket_label"] = scoped_df["volume_bucket"].map(
            {
                "volume_high": "Volume High",
                "volume_low": "Volume Low",
            }
        )
        scoped_df["combined_bucket"] = (
            scoped_df["price_group"].astype(str)
            + "_"
            + scoped_df["volume_bucket"].astype(str)
        )
        scoped_df["combined_bucket_label"] = (
            scoped_df["price_group_label"].astype(str)
            + " x "
            + scoped_df["volume_bucket_label"].astype(str)
        )
        daily_df = (
            scoped_df.groupby(
                [
                    "horizon_key",
                    "horizon_days",
                    "date",
                    "combined_bucket",
                    "combined_bucket_label",
                ],
                as_index=False,
            )
            .agg(
                group_sample_count=("code", "size"),
                group_mean_future_close=("future_close", "mean"),
                group_mean_future_return=("future_return", "mean"),
            )
        )
        label_lookup = (
            daily_df.drop_duplicates(subset=["combined_bucket"])
            .set_index("combined_bucket")["combined_bucket_label"]
            .to_dict()
        )
        for horizon_key in HORIZON_ORDER:
            horizon_df = daily_df.loc[daily_df["horizon_key"] == horizon_key].copy()
            if horizon_df.empty:
                continue
            for metric_key, value_column in metric_columns.items():
                pivot_df = (
                    horizon_df.pivot(
                        index="date",
                        columns="combined_bucket",
                        values=value_column,
                    )
                    .reindex(columns=list(LOW_VOLUME_COMBINED_BUCKET_ORDER))
                    .dropna()
                )
                pivot_df.index = pivot_df.index.astype(str)
                for (
                    left_bucket,
                    right_bucket,
                    hypothesis_key,
                    hypothesis_label,
                ) in LOW_VOLUME_HYPOTHESIS_DEFINITIONS:
                    if pivot_df.empty:
                        left = right = pd.Series(dtype=float)
                    else:
                        left = pivot_df[left_bucket].astype(float)
                        right = pivot_df[right_bucket].astype(float)
                    mean_diff_series = left - right
                    paired_t_statistic, paired_t_p_value = _safe_paired_t_test(
                        left.to_numpy(dtype=float),
                        right.to_numpy(dtype=float),
                    )
                    wilcoxon_statistic, wilcoxon_p_value = _safe_wilcoxon(
                        left.to_numpy(dtype=float),
                        right.to_numpy(dtype=float),
                    )
                    records.append(
                        {
                            "candidate_key": candidate_row["candidate_key"],
                            "candidate_label": candidate_row["candidate_label"],
                            "high_deciles_label": candidate_row["high_deciles_label"],
                            "middle_deciles_label": candidate_row["middle_deciles_label"],
                            "low_deciles_label": candidate_row["low_deciles_label"],
                            "horizon_key": horizon_key,
                            "metric_key": metric_key,
                            "hypothesis_key": hypothesis_key,
                            "hypothesis_label": hypothesis_label,
                            "left_bucket": left_bucket,
                            "left_bucket_label": label_lookup.get(left_bucket, left_bucket),
                            "right_bucket": right_bucket,
                            "right_bucket_label": label_lookup.get(right_bucket, right_bucket),
                            "n_dates": int(len(pivot_df)),
                            "mean_difference": (
                                None
                                if mean_diff_series.empty
                                else float(mean_diff_series.mean())
                            ),
                            "median_difference": (
                                None
                                if mean_diff_series.empty
                                else float(mean_diff_series.median())
                            ),
                            "positive_date_share": (
                                None
                                if mean_diff_series.empty
                                else float((mean_diff_series > 0).mean())
                            ),
                            "negative_date_share": (
                                None
                                if mean_diff_series.empty
                                else float((mean_diff_series < 0).mean())
                            ),
                            "paired_t_statistic": paired_t_statistic,
                            "paired_t_p_value": paired_t_p_value,
                            "wilcoxon_statistic": wilcoxon_statistic,
                            "wilcoxon_p_value": wilcoxon_p_value,
                        }
                    )

    hypothesis_df = pd.DataFrame.from_records(records)
    if hypothesis_df.empty:
        return hypothesis_df

    hypothesis_df["paired_t_p_value_holm"] = None
    hypothesis_df["wilcoxon_p_value_holm"] = None
    for candidate_key in hypothesis_df["candidate_key"].unique().tolist():
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                mask = (
                    (hypothesis_df["candidate_key"] == candidate_key)
                    & (hypothesis_df["horizon_key"] == horizon_key)
                    & (hypothesis_df["metric_key"] == metric_key)
                )
                paired_t_p_values = cast(
                    list[float | None],
                    hypothesis_df.loc[mask, "paired_t_p_value"].tolist(),
                )
                wilcoxon_p_values = cast(
                    list[float | None],
                    hypothesis_df.loc[mask, "wilcoxon_p_value"].tolist(),
                )
                hypothesis_df.loc[mask, "paired_t_p_value_holm"] = _holm_adjust(
                    paired_t_p_values
                )
                hypothesis_df.loc[mask, "wilcoxon_p_value_holm"] = _holm_adjust(
                    wilcoxon_p_values
                )
    hypothesis_df["_horizon_order"] = hypothesis_df["horizon_key"].map(
        {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
    )
    hypothesis_df["_metric_order"] = hypothesis_df["metric_key"].map(
        {key: index for index, key in enumerate(METRIC_ORDER, start=1)}
    )
    hypothesis_df["_hypothesis_order"] = hypothesis_df["hypothesis_key"].map(
        {
            key: index
            for index, (_, _, key, _) in enumerate(
                LOW_VOLUME_HYPOTHESIS_DEFINITIONS,
                start=1,
            )
        }
    )
    return (
        hypothesis_df.sort_values(
            ["candidate_label", "_horizon_order", "_metric_order", "_hypothesis_order"]
        )
        .drop(columns=["_horizon_order", "_metric_order", "_hypothesis_order"])
        .reset_index(drop=True)
    )


def _build_candidate_overall_scorecard(
    candidate_definition_df: pd.DataFrame,
    candidate_price_group_summary_df: pd.DataFrame,
    candidate_price_hypothesis_df: pd.DataFrame,
    candidate_low_volume_hypothesis_df: pd.DataFrame,
) -> pd.DataFrame:
    if candidate_definition_df.empty:
        return pd.DataFrame()

    scorecard_df = candidate_definition_df[
        [
            "candidate_key",
            "candidate_label",
            "high_deciles_label",
            "middle_deciles_label",
            "low_deciles_label",
        ]
    ].copy()
    horizon_metric_index = pd.MultiIndex.from_product(
        [HORIZON_ORDER, METRIC_ORDER],
        names=["horizon_key", "metric_key"],
    ).to_frame(index=False)
    scorecard_df = scorecard_df.merge(horizon_metric_index, how="cross")

    if not candidate_price_group_summary_df.empty:
        group_size_df = candidate_price_group_summary_df.pivot(
            index=["candidate_key", "horizon_key"],
            columns="price_group",
            values="mean_group_size",
        ).reset_index()
        group_size_df = group_size_df.rename(
            columns={
                "high": "high_mean_group_size",
                "middle": "middle_mean_group_size",
                "low": "low_mean_group_size",
            }
        )
        scorecard_df = scorecard_df.merge(
            group_size_df,
            on=["candidate_key", "horizon_key"],
            how="left",
        )
        scorecard_df["min_mean_group_size"] = scorecard_df[
            [
                "high_mean_group_size",
                "middle_mean_group_size",
                "low_mean_group_size",
            ]
        ].min(axis=1)

    if not candidate_price_hypothesis_df.empty:
        price_mean_df = candidate_price_hypothesis_df.pivot(
            index=["candidate_key", "horizon_key", "metric_key"],
            columns="hypothesis_key",
            values="mean_difference",
        ).reset_index()
        price_sig_df = (
            candidate_price_hypothesis_df.assign(
                price_wilcoxon_sig=(
                    candidate_price_hypothesis_df["wilcoxon_p_value_holm"].astype(float)
                    < 0.05
                ),
                price_paired_t_sig=(
                    candidate_price_hypothesis_df["paired_t_p_value_holm"].astype(float)
                    < 0.05
                ),
            )
            .groupby(["candidate_key", "horizon_key", "metric_key"], as_index=False)
            .agg(
                price_wilcoxon_significant_pair_count=("price_wilcoxon_sig", "sum"),
                price_paired_t_significant_pair_count=("price_paired_t_sig", "sum"),
            )
        )
        scorecard_df = scorecard_df.merge(
            price_mean_df,
            on=["candidate_key", "horizon_key", "metric_key"],
            how="left",
        )
        scorecard_df = scorecard_df.merge(
            price_sig_df,
            on=["candidate_key", "horizon_key", "metric_key"],
            how="left",
        )
        scorecard_df["abs_extreme_vs_middle_mean_difference"] = (
            scorecard_df["high_vs_middle"].abs()
            + scorecard_df["low_vs_middle"].abs()
        )

    if not candidate_low_volume_hypothesis_df.empty:
        volume_mean_df = candidate_low_volume_hypothesis_df.pivot(
            index=["candidate_key", "horizon_key", "metric_key"],
            columns="hypothesis_key",
            values="mean_difference",
        ).reset_index()
        volume_sig_df = (
            candidate_low_volume_hypothesis_df.assign(
                volume_wilcoxon_sig=(
                    candidate_low_volume_hypothesis_df["wilcoxon_p_value_holm"].astype(float)
                    < 0.05
                ),
                volume_paired_t_sig=(
                    candidate_low_volume_hypothesis_df["paired_t_p_value_holm"].astype(float)
                    < 0.05
                ),
            )
            .groupby(["candidate_key", "horizon_key", "metric_key"], as_index=False)
            .agg(
                volume_wilcoxon_significant_pair_count=("volume_wilcoxon_sig", "sum"),
                volume_paired_t_significant_pair_count=("volume_paired_t_sig", "sum"),
            )
        )
        scorecard_df = scorecard_df.merge(
            volume_mean_df,
            on=["candidate_key", "horizon_key", "metric_key"],
            how="left",
        )
        scorecard_df = scorecard_df.merge(
            volume_sig_df,
            on=["candidate_key", "horizon_key", "metric_key"],
            how="left",
        )

    scorecard_df["_horizon_order"] = scorecard_df["horizon_key"].map(
        {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
    )
    scorecard_df["_metric_order"] = scorecard_df["metric_key"].map(
        {key: index for index, key in enumerate(METRIC_ORDER, start=1)}
    )
    return (
        scorecard_df.sort_values(["candidate_label", "_horizon_order", "_metric_order"])
        .drop(columns=["_horizon_order", "_metric_order"])
        .reset_index(drop=True)
    )


def run_topix100_price_to_sma50_decile_partitions_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
) -> Topix100PriceToSma50DecilePartitionsResearchResult:
    base_result = run_topix100_price_vs_sma_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        price_sma_windows=(PRICE_SMA_WINDOW,),
        volume_sma_windows=VOLUME_SMA_WINDOWS,
    )
    working_horizon_panel_df = _build_working_horizon_panel(base_result)
    candidate_definition_df = _build_candidate_definitions()
    candidate_price_group_summary_df = _build_candidate_price_group_summary(
        base_result,
        candidate_definition_df,
    )
    candidate_price_hypothesis_df = _build_candidate_price_hypothesis(
        base_result,
        candidate_definition_df,
    )
    candidate_low_volume_hypothesis_df = _build_candidate_low_volume_hypothesis(
        working_horizon_panel_df,
        candidate_definition_df,
    )
    candidate_overall_scorecard_df = _build_candidate_overall_scorecard(
        candidate_definition_df,
        candidate_price_group_summary_df,
        candidate_price_hypothesis_df,
        candidate_low_volume_hypothesis_df,
    )
    return Topix100PriceToSma50DecilePartitionsResearchResult(
        db_path=base_result.db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        default_start_date=base_result.default_start_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        lookback_years=base_result.lookback_years,
        min_constituents_per_day=base_result.min_constituents_per_day,
        topix100_constituent_count=base_result.topix100_constituent_count,
        stock_day_count=base_result.stock_day_count,
        valid_date_count=base_result.valid_date_count,
        price_feature=PRICE_FEATURE,
        price_feature_label=PRICE_FEATURE_LABEL,
        volume_feature=VOLUME_FEATURE,
        volume_feature_label=VOLUME_FEATURE_LABEL,
        candidate_count=int(len(candidate_definition_df)),
        decile_profile_df=_build_decile_profile(working_horizon_panel_df),
        decile_threshold_summary_df=_build_decile_threshold_summary(
            working_horizon_panel_df
        ),
        candidate_definition_df=candidate_definition_df,
        candidate_price_group_summary_df=candidate_price_group_summary_df,
        candidate_price_hypothesis_df=candidate_price_hypothesis_df,
        candidate_low_volume_hypothesis_df=candidate_low_volume_hypothesis_df,
        candidate_overall_scorecard_df=candidate_overall_scorecard_df,
    )


def write_topix100_price_to_sma50_decile_partitions_research_bundle(
    result: Topix100PriceToSma50DecilePartitionsResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_research_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_price_to_sma50_decile_partitions_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "lookback_years": result.lookback_years,
            "min_constituents_per_day": result.min_constituents_per_day,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_price_to_sma50_decile_partitions_research_bundle(
    bundle_path: str | Path,
) -> Topix100PriceToSma50DecilePartitionsResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_research_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_price_to_sma50_decile_partitions_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_research_result_payload(
    result: Topix100PriceToSma50DecilePartitionsResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata: dict[str, Any] = {}
    tables: dict[str, pd.DataFrame] = {}
    for field in fields(result):
        value = getattr(result, field.name)
        if isinstance(value, pd.DataFrame):
            tables[field.name] = value
        else:
            metadata[field.name] = value
    return metadata, tables


def _build_research_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100PriceToSma50DecilePartitionsResearchResult:
    normalized = dict(metadata)
    return Topix100PriceToSma50DecilePartitionsResearchResult(
        db_path=cast(str, normalized["db_path"]),
        source_mode=cast(SourceMode, normalized["source_mode"]),
        source_detail=cast(str, normalized["source_detail"]),
        available_start_date=cast(str | None, normalized["available_start_date"]),
        available_end_date=cast(str | None, normalized["available_end_date"]),
        default_start_date=cast(str | None, normalized["default_start_date"]),
        analysis_start_date=cast(str | None, normalized["analysis_start_date"]),
        analysis_end_date=cast(str | None, normalized["analysis_end_date"]),
        lookback_years=int(normalized["lookback_years"]),
        min_constituents_per_day=int(normalized["min_constituents_per_day"]),
        topix100_constituent_count=int(normalized["topix100_constituent_count"]),
        stock_day_count=int(normalized["stock_day_count"]),
        valid_date_count=int(normalized["valid_date_count"]),
        price_feature=cast(str, normalized["price_feature"]),
        price_feature_label=cast(str, normalized["price_feature_label"]),
        volume_feature=cast(str, normalized["volume_feature"]),
        volume_feature_label=cast(str, normalized["volume_feature_label"]),
        candidate_count=int(normalized["candidate_count"]),
        decile_profile_df=tables["decile_profile_df"],
        decile_threshold_summary_df=tables["decile_threshold_summary_df"],
        candidate_definition_df=tables["candidate_definition_df"],
        candidate_price_group_summary_df=tables["candidate_price_group_summary_df"],
        candidate_price_hypothesis_df=tables["candidate_price_hypothesis_df"],
        candidate_low_volume_hypothesis_df=tables[
            "candidate_low_volume_hypothesis_df"
        ],
        candidate_overall_scorecard_df=tables["candidate_overall_scorecard_df"],
    )


def _build_research_bundle_summary_markdown(
    result: Topix100PriceToSma50DecilePartitionsResearchResult,
) -> str:
    summary_lines = [
        "# TOPIX100 Price/SMA50 Decile Partitions",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Price feature: `{result.price_feature_label}`",
        f"- Volume feature: `{result.volume_feature_label}`",
        f"- TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Stock-day rows: `{result.stock_day_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
        f"- Candidate partitions: `{result.candidate_count}`",
        "",
        "## Current Read",
        "",
    ]

    default_rows = result.candidate_overall_scorecard_df[
        (result.candidate_overall_scorecard_df["horizon_key"] == "t_plus_10")
        & (result.candidate_overall_scorecard_df["metric_key"] == "future_return")
    ].copy()
    if default_rows.empty:
        summary_lines.append("- No `t_plus_10 / future_return` candidates were available.")
    else:
        best_price_row = default_rows.sort_values(
            [
                "price_wilcoxon_significant_pair_count",
                "abs_extreme_vs_middle_mean_difference",
                "min_mean_group_size",
            ],
            ascending=[False, False, False],
        ).iloc[0]
        summary_lines.extend(
            [
                "- Best price-only contiguous split on `t_plus_10 / future_return`:",
                "  "
                f"`{best_price_row['candidate_label']}` with "
                f"`{int(best_price_row.get('price_wilcoxon_significant_pair_count', 0) or 0)}` "
                "Wilcoxon hits and extreme-vs-middle spread "
                f"`{float(best_price_row.get('abs_extreme_vs_middle_mean_difference', 0.0) or 0.0):+.4f}`.",
            ]
        )
        best_volume_row = default_rows.sort_values(
            [
                "volume_wilcoxon_significant_pair_count",
                "low_volume_low_vs_middle_volume_low",
                "min_mean_group_size",
            ],
            ascending=[False, False, False],
        ).iloc[0]
        summary_lines.append(
            "  "
            f"Best low-volume edge was `{best_volume_row['candidate_label']}` with "
            f"`Low Volume Low vs Middle Volume Low = "
            f"{float(best_volume_row.get('low_volume_low_vs_middle_volume_low', 0.0) or 0.0):+.4f}`."
        )

    summary_lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_research_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)
