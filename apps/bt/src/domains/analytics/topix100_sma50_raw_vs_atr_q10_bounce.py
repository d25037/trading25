"""
TOPIX100 SMA50 raw-gap vs ATR-gap Q10 bounce research analytics.

This module compares two SMA50 bounce lenses side by side:

- `raw_gap`: `(close / sma50) - 1`
- `atr_gap_14`: `(close - sma50) / atr14`

The research keeps the existing `q10 / middle x volume` bounce framing but uses
`signal_variant` as the public axis instead of the broader `price_feature`
family used by the existing `price_vs_sma` experiments.
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
from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
    Q10_LOW_HYPOTHESIS_LABELS,
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP,
    PRICE_BUCKET_DECILES,
    PRICE_BUCKET_LABEL_MAP,
    VOLUME_BUCKET_LABEL_MAP,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    HORIZON_ORDER,
    METRIC_ORDER,
    _DEFAULT_LOOKBACK_YEARS,
    _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
    _HORIZON_DAY_MAP,
    _assign_feature_deciles,
    _default_start_date,
    _holm_adjust,
    _query_topix100_date_range,
    _query_topix100_stock_history,
    _rolling_mean,
    _safe_paired_t_test,
    _safe_ratio,
    _safe_wilcoxon,
)
from src.domains.strategy.indicators.calculations import compute_atr

HorizonKey = Literal["t_plus_1", "t_plus_5", "t_plus_10"]
MetricKey = Literal["future_close", "future_return"]
SignalVariantKey = Literal["raw_gap", "atr_gap_14"]
PeriodSegmentKey = Literal["early", "middle", "late"]

SIGNAL_VARIANT_ORDER: tuple[SignalVariantKey, ...] = ("raw_gap", "atr_gap_14")
SIGNAL_VARIANT_LABEL_MAP: dict[SignalVariantKey, str] = {
    "raw_gap": "Raw Gap (Close / SMA50 - 1)",
    "atr_gap_14": "ATR14 Gap ((Close - SMA50) / ATR14)",
}
PERIOD_SEGMENT_ORDER: tuple[PeriodSegmentKey, ...] = ("early", "middle", "late")
PERIOD_SEGMENT_LABEL_MAP: dict[PeriodSegmentKey, str] = {
    "early": "Early",
    "middle": "Middle",
    "late": "Late",
}
DEFAULT_SIGNAL_VARIANT: SignalVariantKey = "raw_gap"
DEFAULT_VOLUME_FEATURE = "volume_sma_5_20"
SMA_WINDOW = 50
ATR_PERIOD = 14
SAMPLE_LOOKBACK_DAYS = 126
TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-sma50-raw-vs-atr-q10-bounce"
)


@dataclass(frozen=True)
class Topix100Sma50RawVsAtrQ10BounceResearchResult:
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
    signal_variant_order: tuple[str, ...]
    volume_feature: str
    volume_feature_label: str
    topix100_constituent_count: int
    stock_day_count: int
    valid_date_count: int
    event_panel_df: pd.DataFrame
    q10_middle_volume_split_panel_df: pd.DataFrame
    q10_middle_volume_split_daily_means_df: pd.DataFrame
    q10_middle_volume_split_summary_df: pd.DataFrame
    q10_middle_volume_split_pairwise_significance_df: pd.DataFrame
    q10_low_hypothesis_df: pd.DataFrame
    q10_low_spread_daily_df: pd.DataFrame
    q10_low_scorecard_df: pd.DataFrame
    sample_chart_candidates_df: pd.DataFrame


def _normalize_volume_feature(volume_feature: str | None) -> str:
    normalized = str(volume_feature or DEFAULT_VOLUME_FEATURE)
    if normalized not in VOLUME_FEATURE_ORDER:
        raise ValueError(
            f"Unsupported volume_feature: {normalized}. "
            f"Supported features are {list(VOLUME_FEATURE_ORDER)}."
        )
    return normalized


def _resolve_volume_sma_window(volume_feature: str) -> tuple[int, int]:
    feature_to_window = {
        feature: window
        for feature, window in zip(
            VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True
        )
    }
    return feature_to_window[volume_feature]


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    aux_columns: list[str] = []

    if "signal_variant" in sorted_df.columns:
        sorted_df["_signal_variant_order"] = sorted_df["signal_variant"].map(
            {key: index for index, key in enumerate(SIGNAL_VARIANT_ORDER, start=1)}
        )
        aux_columns.append("_signal_variant_order")
    if "horizon_key" in sorted_df.columns:
        sorted_df["_horizon_order"] = sorted_df["horizon_key"].map(
            {key: index for index, key in enumerate(HORIZON_ORDER, start=1)}
        )
        aux_columns.append("_horizon_order")
    if "metric_key" in sorted_df.columns:
        sorted_df["_metric_order"] = sorted_df["metric_key"].map(
            {key: index for index, key in enumerate(METRIC_ORDER, start=1)}
        )
        aux_columns.append("_metric_order")
    if "combined_bucket" in sorted_df.columns:
        sorted_df["_combined_bucket_order"] = sorted_df["combined_bucket"].map(
            {
                key: index
                for index, key in enumerate(Q10_MIDDLE_COMBINED_BUCKET_ORDER, start=1)
            }
        )
        aux_columns.append("_combined_bucket_order")
    if "left_combined_bucket" in sorted_df.columns:
        sorted_df["_left_combined_bucket_order"] = sorted_df[
            "left_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(Q10_MIDDLE_COMBINED_BUCKET_ORDER, start=1)
            }
        )
        aux_columns.append("_left_combined_bucket_order")
    if "right_combined_bucket" in sorted_df.columns:
        sorted_df["_right_combined_bucket_order"] = sorted_df[
            "right_combined_bucket"
        ].map(
            {
                key: index
                for index, key in enumerate(Q10_MIDDLE_COMBINED_BUCKET_ORDER, start=1)
            }
        )
        aux_columns.append("_right_combined_bucket_order")
    if "period_segment" in sorted_df.columns:
        sorted_df["_period_segment_order"] = sorted_df["period_segment"].map(
            {key: index for index, key in enumerate(PERIOD_SEGMENT_ORDER, start=1)}
        )
        aux_columns.append("_period_segment_order")
    if "hypothesis_label" in sorted_df.columns:
        sorted_df["_hypothesis_order"] = sorted_df["hypothesis_label"].map(
            {
                label: index
                for index, (_, _, label) in enumerate(
                    Q10_LOW_HYPOTHESIS_LABELS, start=1
                )
            }
        )
        aux_columns.append("_hypothesis_order")

    sort_columns = [
        column
        for column in [
            "_signal_variant_order",
            "_horizon_order",
            "_metric_order",
            "_combined_bucket_order",
            "_left_combined_bucket_order",
            "_right_combined_bucket_order",
            "_hypothesis_order",
            "_period_segment_order",
            "date",
            "code",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns).reset_index(drop=True)
    return sorted_df.drop(
        columns=[column for column in aux_columns if column in sorted_df.columns]
    )


def _compute_atr_by_code(panel: pd.DataFrame) -> pd.Series:
    series_by_code: list[pd.Series] = []
    for _, frame in panel.groupby("code", sort=False):
        series_by_code.append(
            compute_atr(
                frame["high"].astype(float),
                frame["low"].astype(float),
                frame["close"].astype(float),
                period=ATR_PERIOD,
            )
        )
    if not series_by_code:
        return pd.Series(dtype="float64")
    return pd.concat(series_by_code).sort_index()


def _enrich_event_panel(
    history_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    min_constituents_per_day: int,
    volume_feature: str,
) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame()

    short_window, long_window = _resolve_volume_sma_window(volume_feature)
    panel = history_df.copy()
    panel["date"] = panel["date"].astype(str)
    panel = panel.sort_values(["code", "date"]).reset_index(drop=True)

    panel["sma50"] = _rolling_mean(panel, column_name="close", window=SMA_WINDOW)
    panel["atr14"] = _compute_atr_by_code(panel)
    panel["raw_gap"] = _safe_ratio(panel["close"], panel["sma50"]) - 1.0
    panel["atr_gap_14"] = _safe_ratio(panel["close"] - panel["sma50"], panel["atr14"])

    short_volume = _rolling_mean(panel, column_name="volume", window=short_window)
    long_volume = _rolling_mean(panel, column_name="volume", window=long_window)
    panel[volume_feature] = _safe_ratio(short_volume, long_volume)

    for horizon_key, horizon_days in _HORIZON_DAY_MAP.items():
        future_close = (
            panel.groupby("code", sort=False)["close"]
            .shift(-horizon_days)
            .astype(float)
        )
        panel[f"{horizon_key}_close"] = future_close
        panel[f"{horizon_key}_return"] = _safe_ratio(future_close, panel["close"]) - 1.0

    required_mask = (
        panel["close"].gt(0)
        & panel["sma50"].notna()
        & panel["atr14"].notna()
        & panel["raw_gap"].notna()
        & panel["atr_gap_14"].notna()
        & panel[volume_feature].notna()
    )
    if analysis_start_date is not None:
        required_mask &= panel["date"] >= analysis_start_date
    if analysis_end_date is not None:
        required_mask &= panel["date"] <= analysis_end_date
    panel = panel.loc[required_mask].copy()
    if panel.empty:
        return panel

    panel["date_constituent_count"] = panel.groupby("date")["code"].transform("size")
    panel = panel.loc[
        panel["date_constituent_count"] >= min_constituents_per_day
    ].copy()
    if panel.empty:
        return panel
    return panel.reset_index(drop=True)


def _build_signal_variant_split_panel(
    event_panel_df: pd.DataFrame,
    *,
    volume_feature: str,
) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    common_columns = [
        "date",
        "code",
        "company_name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sma50",
        "atr14",
        "raw_gap",
        "atr_gap_14",
        volume_feature,
        "date_constituent_count",
        *[f"{horizon_key}_close" for horizon_key in HORIZON_ORDER],
        *[f"{horizon_key}_return" for horizon_key in HORIZON_ORDER],
    ]
    frames: list[pd.DataFrame] = []
    for signal_variant in SIGNAL_VARIANT_ORDER:
        ranked_panel_df = event_panel_df[common_columns].copy()
        ranked_panel_df["ranking_feature"] = signal_variant
        ranked_panel_df["ranking_feature_label"] = SIGNAL_VARIANT_LABEL_MAP[
            signal_variant
        ]
        ranked_panel_df["ranking_value"] = event_panel_df[signal_variant].astype(float)
        ranked_panel_df = _assign_feature_deciles(
            ranked_panel_df,
            known_feature_order=list(SIGNAL_VARIANT_ORDER),
        )
        ranked_panel_df["signal_variant"] = signal_variant
        ranked_panel_df["signal_variant_label"] = SIGNAL_VARIANT_LABEL_MAP[
            signal_variant
        ]
        ranked_panel_df["price_bucket"] = None
        for bucket_key, bucket_deciles in PRICE_BUCKET_DECILES.items():
            ranked_panel_df.loc[
                ranked_panel_df["feature_decile"].isin(bucket_deciles),
                "price_bucket",
            ] = bucket_key
        ranked_panel_df = ranked_panel_df.dropna(subset=["price_bucket"]).copy()
        if ranked_panel_df.empty:
            continue

        ranked_panel_df["price_bucket"] = ranked_panel_df["price_bucket"].astype(str)
        ranked_panel_df["price_bucket_label"] = ranked_panel_df["price_bucket"].map(
            PRICE_BUCKET_LABEL_MAP
        )
        ranked_panel_df["price_bucket_size"] = ranked_panel_df.groupby(
            ["date", "price_bucket"]
        )["code"].transform("size")
        ranked_panel_df["volume_rank_desc_within_price_bucket"] = (
            ranked_panel_df.groupby(["date", "price_bucket"])[volume_feature]
            .rank(method="first", ascending=False)
            .astype(int)
        )
        ranked_panel_df["volume_bucket_index"] = (
            ((ranked_panel_df["volume_rank_desc_within_price_bucket"] - 1) * 2)
            // ranked_panel_df["price_bucket_size"]
        ) + 1
        ranked_panel_df["volume_bucket_index"] = ranked_panel_df[
            "volume_bucket_index"
        ].clip(1, 2)
        ranked_panel_df["volume_bucket"] = ranked_panel_df["volume_bucket_index"].map(
            {1: "volume_high", 2: "volume_low"}
        )
        ranked_panel_df["volume_bucket_label"] = ranked_panel_df["volume_bucket"].map(
            VOLUME_BUCKET_LABEL_MAP
        )
        ranked_panel_df["combined_bucket"] = (
            ranked_panel_df["price_bucket"] + "_" + ranked_panel_df["volume_bucket"]
        )
        ranked_panel_df["combined_bucket_label"] = ranked_panel_df[
            "combined_bucket"
        ].map(COMBINED_BUCKET_LABEL_MAP)
        ranked_panel_df["volume_feature"] = volume_feature
        ranked_panel_df["volume_feature_label"] = VOLUME_FEATURE_LABEL_MAP[
            volume_feature
        ]
        frames.append(ranked_panel_df.reset_index(drop=True))

    if not frames:
        return pd.DataFrame()
    return _sort_frame(pd.concat(frames, ignore_index=True))


def _filter_q10_middle_volume_split_panel(split_panel_df: pd.DataFrame) -> pd.DataFrame:
    if split_panel_df.empty:
        return pd.DataFrame()
    filtered = split_panel_df.loc[
        split_panel_df["combined_bucket"].isin(Q10_MIDDLE_COMBINED_BUCKET_ORDER)
    ].copy()
    return _sort_frame(filtered)


def _build_signal_variant_horizon_panel(
    split_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if split_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sma50",
        "atr14",
        "raw_gap",
        "atr_gap_14",
        "date_constituent_count",
        "signal_variant",
        "signal_variant_label",
        "ranking_value",
        "feature_decile",
        "feature_decile_label",
        "price_bucket",
        "price_bucket_label",
        "volume_bucket",
        "volume_bucket_label",
        "combined_bucket",
        "combined_bucket_label",
        "volume_feature",
        "volume_feature_label",
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


def _build_q10_middle_volume_daily_means(
    q10_middle_volume_horizon_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_horizon_panel_df.empty:
        return pd.DataFrame()

    daily_means_df = q10_middle_volume_horizon_panel_df.groupby(
        [
            "signal_variant",
            "signal_variant_label",
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
    ).agg(
        group_sample_count=("code", "size"),
        group_mean_ranking_value=("ranking_value", "mean"),
        group_mean_event_close=("close", "mean"),
        group_mean_future_close=("future_close", "mean"),
        group_mean_future_return=("future_return", "mean"),
        group_median_future_return=("future_return", "median"),
    )
    return _sort_frame(daily_means_df)


def _summarize_q10_middle_volume_split(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_middle_volume_split_daily_means_df.empty:
        return pd.DataFrame()

    summary_df = q10_middle_volume_split_daily_means_df.groupby(
        [
            "signal_variant",
            "signal_variant_label",
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
    ).agg(
        date_count=("date", "nunique"),
        mean_group_size=("group_sample_count", "mean"),
        mean_ranking_value=("group_mean_ranking_value", "mean"),
        mean_event_close=("group_mean_event_close", "mean"),
        mean_future_close=("group_mean_future_close", "mean"),
        mean_future_return=("group_mean_future_return", "mean"),
        median_future_return=("group_median_future_return", "median"),
        std_future_return=("group_mean_future_return", "std"),
    )
    return _sort_frame(summary_df)


def _aligned_q10_middle_combined_pivot(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
    *,
    signal_variant: str,
    horizon_key: str,
    value_column: str,
) -> pd.DataFrame:
    scoped_df = q10_middle_volume_split_daily_means_df.loc[
        (q10_middle_volume_split_daily_means_df["signal_variant"] == signal_variant)
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
    volume_feature: str,
) -> pd.DataFrame:
    if q10_middle_volume_split_daily_means_df.empty:
        return pd.DataFrame()

    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    records: list[dict[str, Any]] = []
    for signal_variant in SIGNAL_VARIANT_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                pivot_df = _aligned_q10_middle_combined_pivot(
                    q10_middle_volume_split_daily_means_df,
                    signal_variant=signal_variant,
                    horizon_key=horizon_key,
                    value_column=metric_columns[metric_key],
                )
                if pivot_df.empty:
                    for left_bucket, right_bucket in combinations(
                        Q10_MIDDLE_COMBINED_BUCKET_ORDER, 2
                    ):
                        records.append(
                            {
                                "signal_variant": signal_variant,
                                "signal_variant_label": SIGNAL_VARIANT_LABEL_MAP[
                                    signal_variant
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
                            "signal_variant": signal_variant,
                            "signal_variant_label": SIGNAL_VARIANT_LABEL_MAP[
                                signal_variant
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
    for signal_variant in SIGNAL_VARIANT_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                mask = (
                    (pairwise_df["signal_variant"] == signal_variant)
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


def _build_q10_low_hypothesis(
    q10_middle_volume_split_pairwise_significance_df: pd.DataFrame,
    *,
    volume_feature: str,
) -> pd.DataFrame:
    if q10_middle_volume_split_pairwise_significance_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for signal_variant in SIGNAL_VARIANT_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key in METRIC_ORDER:
                scoped_df = q10_middle_volume_split_pairwise_significance_df.loc[
                    (
                        q10_middle_volume_split_pairwise_significance_df[
                            "signal_variant"
                        ]
                        == signal_variant
                    )
                    & (
                        q10_middle_volume_split_pairwise_significance_df["horizon_key"]
                        == horizon_key
                    )
                    & (
                        q10_middle_volume_split_pairwise_significance_df["metric_key"]
                        == metric_key
                    )
                ]
                for (
                    left_bucket,
                    right_bucket,
                    hypothesis_label,
                ) in Q10_LOW_HYPOTHESIS_LABELS:
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
                                "signal_variant": signal_variant,
                                "signal_variant_label": SIGNAL_VARIANT_LABEL_MAP[
                                    signal_variant
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
                            "signal_variant": signal_variant,
                            "signal_variant_label": SIGNAL_VARIANT_LABEL_MAP[
                                signal_variant
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
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_q10_low_spread_daily(
    q10_middle_volume_split_daily_means_df: pd.DataFrame,
    *,
    volume_feature: str,
) -> pd.DataFrame:
    if q10_middle_volume_split_daily_means_df.empty:
        return pd.DataFrame()

    metric_columns = {
        "future_close": "group_mean_future_close",
        "future_return": "group_mean_future_return",
    }
    records: list[dict[str, Any]] = []
    for signal_variant in SIGNAL_VARIANT_ORDER:
        for horizon_key in HORIZON_ORDER:
            for metric_key, value_column in metric_columns.items():
                pivot_df = _aligned_q10_middle_combined_pivot(
                    q10_middle_volume_split_daily_means_df,
                    signal_variant=signal_variant,
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
                                "signal_variant": signal_variant,
                                "signal_variant_label": SIGNAL_VARIANT_LABEL_MAP[
                                    signal_variant
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
    return _sort_frame(pd.DataFrame.from_records(records))


def _build_q10_low_scorecard(
    q10_low_spread_daily_df: pd.DataFrame,
    q10_low_hypothesis_df: pd.DataFrame,
) -> pd.DataFrame:
    if q10_low_spread_daily_df.empty:
        return pd.DataFrame()

    scorecard_df = q10_low_spread_daily_df.groupby(
        [
            "signal_variant",
            "signal_variant_label",
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
    ).agg(
        n_dates=("date", "nunique"),
        mean_difference=("mean_difference", "mean"),
        median_difference=("mean_difference", "median"),
        positive_date_share=(
            "mean_difference",
            lambda values: float((values > 0).mean()),
        ),
        negative_date_share=(
            "mean_difference",
            lambda values: float((values < 0).mean()),
        ),
    )
    merged = scorecard_df.merge(
        q10_low_hypothesis_df[
            [
                "signal_variant",
                "volume_feature",
                "horizon_key",
                "metric_key",
                "hypothesis_label",
                "paired_t_p_value_holm",
                "wilcoxon_p_value_holm",
            ]
        ],
        on=[
            "signal_variant",
            "volume_feature",
            "horizon_key",
            "metric_key",
            "hypothesis_label",
        ],
        how="left",
    )
    return _sort_frame(merged)


def _segment_dates(scoped_df: pd.DataFrame) -> pd.Series:
    unique_dates = sorted(scoped_df["date"].astype(str).unique().tolist())
    if not unique_dates:
        return pd.Series(dtype="object")
    segment_map: dict[str, str] = {}
    total = len(unique_dates)
    for index, date in enumerate(unique_dates):
        fraction = (index + 1) / total
        if fraction <= 1 / 3:
            segment_map[date] = "early"
        elif fraction <= 2 / 3:
            segment_map[date] = "middle"
        else:
            segment_map[date] = "late"
    return scoped_df["date"].astype(str).map(segment_map)


def _build_sample_chart_candidates(
    q10_middle_volume_split_panel_df: pd.DataFrame,
    *,
    volume_feature: str,
) -> pd.DataFrame:
    if q10_middle_volume_split_panel_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for signal_variant in SIGNAL_VARIANT_ORDER:
        for combined_bucket in Q10_MIDDLE_COMBINED_BUCKET_ORDER:
            scoped_df = q10_middle_volume_split_panel_df.loc[
                (q10_middle_volume_split_panel_df["signal_variant"] == signal_variant)
                & (
                    q10_middle_volume_split_panel_df["combined_bucket"]
                    == combined_bucket
                )
            ].copy()
            scoped_df = scoped_df.dropna(
                subset=["t_plus_5_return", "t_plus_10_return"]
            ).copy()
            if scoped_df.empty:
                continue
            ranking_value_median = float(scoped_df["ranking_value"].median())
            scoped_df["period_segment"] = _segment_dates(scoped_df)
            scoped_df["period_segment_label"] = scoped_df["period_segment"].map(
                PERIOD_SEGMENT_LABEL_MAP
            )
            scoped_df["abs_median_distance"] = (
                scoped_df["ranking_value"].astype(float) - ranking_value_median
            ).abs()
            bucket_records: list[dict[str, Any]] = []
            for period_segment in PERIOD_SEGMENT_ORDER:
                segment_df = scoped_df.loc[
                    scoped_df["period_segment"] == period_segment
                ].copy()
                if segment_df.empty:
                    continue
                selected = segment_df.sort_values(
                    ["abs_median_distance", "date", "code"]
                ).iloc[0]
                bucket_records.append(
                    {
                        "signal_variant": signal_variant,
                        "signal_variant_label": SIGNAL_VARIANT_LABEL_MAP[
                            signal_variant
                        ],
                        "volume_feature": volume_feature,
                        "volume_feature_label": VOLUME_FEATURE_LABEL_MAP[
                            volume_feature
                        ],
                        "combined_bucket": combined_bucket,
                        "combined_bucket_label": COMBINED_BUCKET_LABEL_MAP[
                            combined_bucket
                        ],
                        "period_segment": period_segment,
                        "period_segment_label": PERIOD_SEGMENT_LABEL_MAP[
                            period_segment
                        ],
                        "sample_rank": len(bucket_records) + 1,
                        "date": str(selected["date"]),
                        "code": str(selected["code"]),
                        "company_name": str(selected["company_name"]),
                        "ranking_value": float(selected["ranking_value"]),
                        "ranking_value_median": ranking_value_median,
                        "abs_median_distance": float(selected["abs_median_distance"]),
                        "close": float(selected["close"]),
                        "sma50": float(selected["sma50"]),
                        "atr14": float(selected["atr14"]),
                        "raw_gap": float(selected["raw_gap"]),
                        "atr_gap_14": float(selected["atr_gap_14"]),
                        "t_plus_5_return": float(selected["t_plus_5_return"]),
                        "t_plus_10_return": float(selected["t_plus_10_return"]),
                    }
                )
            records.extend(bucket_records)
    return _sort_frame(pd.DataFrame.from_records(records))


def run_topix100_sma50_raw_vs_atr_q10_bounce_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int = _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
) -> Topix100Sma50RawVsAtrQ10BounceResearchResult:
    normalized_volume_feature = _normalize_volume_feature(volume_feature)
    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _query_topix100_date_range(
            ctx.connection
        )
        default_start = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start = start_date or default_start
        history_df = _query_topix100_stock_history(ctx.connection, end_date=end_date)

    event_panel_df = _enrich_event_panel(
        history_df,
        analysis_start_date=analysis_start,
        analysis_end_date=end_date,
        min_constituents_per_day=min_constituents_per_day,
        volume_feature=normalized_volume_feature,
    )
    split_panel_df = _build_signal_variant_split_panel(
        event_panel_df,
        volume_feature=normalized_volume_feature,
    )
    q10_middle_volume_split_panel_df = _filter_q10_middle_volume_split_panel(
        split_panel_df
    )
    q10_middle_volume_horizon_panel_df = _build_signal_variant_horizon_panel(
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
            q10_middle_volume_split_daily_means_df,
            volume_feature=normalized_volume_feature,
        )
    )
    q10_low_hypothesis_df = _build_q10_low_hypothesis(
        q10_middle_volume_split_pairwise_significance_df,
        volume_feature=normalized_volume_feature,
    )
    q10_low_spread_daily_df = _build_q10_low_spread_daily(
        q10_middle_volume_split_daily_means_df,
        volume_feature=normalized_volume_feature,
    )
    q10_low_scorecard_df = _build_q10_low_scorecard(
        q10_low_spread_daily_df,
        q10_low_hypothesis_df,
    )
    sample_chart_candidates_df = _build_sample_chart_candidates(
        q10_middle_volume_split_panel_df,
        volume_feature=normalized_volume_feature,
    )
    analysis_start_date = (
        str(event_panel_df["date"].min()) if not event_panel_df.empty else None
    )
    analysis_end_date = (
        str(event_panel_df["date"].max()) if not event_panel_df.empty else None
    )

    return Topix100Sma50RawVsAtrQ10BounceResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        default_start_date=default_start,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        signal_variant_order=tuple(str(variant) for variant in SIGNAL_VARIANT_ORDER),
        volume_feature=normalized_volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[normalized_volume_feature],
        topix100_constituent_count=int(history_df["code"].nunique())
        if not history_df.empty
        else 0,
        stock_day_count=int(len(event_panel_df)),
        valid_date_count=int(event_panel_df["date"].nunique())
        if not event_panel_df.empty
        else 0,
        event_panel_df=event_panel_df,
        q10_middle_volume_split_panel_df=q10_middle_volume_split_panel_df,
        q10_middle_volume_split_daily_means_df=q10_middle_volume_split_daily_means_df,
        q10_middle_volume_split_summary_df=q10_middle_volume_split_summary_df,
        q10_middle_volume_split_pairwise_significance_df=q10_middle_volume_split_pairwise_significance_df,
        q10_low_hypothesis_df=q10_low_hypothesis_df,
        q10_low_spread_daily_df=q10_low_spread_daily_df,
        q10_low_scorecard_df=q10_low_scorecard_df,
        sample_chart_candidates_df=sample_chart_candidates_df,
    )


def write_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle(
    result: Topix100Sma50RawVsAtrQ10BounceResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_research_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_sma50_raw_vs_atr_q10_bounce_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "lookback_years": result.lookback_years,
            "min_constituents_per_day": result.min_constituents_per_day,
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


def load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle(
    bundle_path: str | Path,
) -> Topix100Sma50RawVsAtrQ10BounceResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_research_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_sma50_raw_vs_atr_q10_bounce_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_sma50_raw_vs_atr_q10_bounce_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_research_result_payload(
    result: Topix100Sma50RawVsAtrQ10BounceResearchResult,
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
) -> Topix100Sma50RawVsAtrQ10BounceResearchResult:
    normalized = dict(metadata)
    normalized["signal_variant_order"] = tuple(
        str(name) for name in normalized["signal_variant_order"]
    )
    return Topix100Sma50RawVsAtrQ10BounceResearchResult(
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
        signal_variant_order=cast(tuple[str, ...], normalized["signal_variant_order"]),
        volume_feature=cast(str, normalized["volume_feature"]),
        volume_feature_label=cast(str, normalized["volume_feature_label"]),
        topix100_constituent_count=int(normalized["topix100_constituent_count"]),
        stock_day_count=int(normalized["stock_day_count"]),
        valid_date_count=int(normalized["valid_date_count"]),
        event_panel_df=tables["event_panel_df"],
        q10_middle_volume_split_panel_df=tables["q10_middle_volume_split_panel_df"],
        q10_middle_volume_split_daily_means_df=tables[
            "q10_middle_volume_split_daily_means_df"
        ],
        q10_middle_volume_split_summary_df=tables["q10_middle_volume_split_summary_df"],
        q10_middle_volume_split_pairwise_significance_df=tables[
            "q10_middle_volume_split_pairwise_significance_df"
        ],
        q10_low_hypothesis_df=tables["q10_low_hypothesis_df"],
        q10_low_spread_daily_df=tables["q10_low_spread_daily_df"],
        q10_low_scorecard_df=tables["q10_low_scorecard_df"],
        sample_chart_candidates_df=tables["sample_chart_candidates_df"],
    )


def _build_research_bundle_summary_markdown(
    result: Topix100Sma50RawVsAtrQ10BounceResearchResult,
) -> str:
    summary_lines = [
        "# TOPIX100 SMA50 Raw vs ATR Q10 Bounce",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Signal variants: `{', '.join(result.signal_variant_order)}`",
        f"- Volume feature: `{result.volume_feature}`",
        f"- TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Stock-day rows: `{result.stock_day_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
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
        summary_lines.append(
            "- No `Q10 Low vs Middle High` rows were available in this run."
        )
    else:
        strongest_row = strongest_rows.sort_values(
            "mean_difference", ascending=False
        ).iloc[0]
        summary_lines.extend(
            [
                "- Strongest `Q10 Low vs Middle High` read on `t_plus_10 / future_return`:",
                "  "
                f"`{strongest_row['signal_variant']}` x `{strongest_row['volume_feature']}` "
                f"at `{float(strongest_row['mean_difference']):+.4f}%`.",
            ]
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
