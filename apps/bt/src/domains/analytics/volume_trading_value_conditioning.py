"""
Conditioned comparison of raw volume surge vs trading-value surge.

This study is intentionally narrower than the broader volume-ratio grid
research. It focuses on the user's next question:

- under which regimes does a surge help?
- does trading-value surge explain the effect better than raw volume surge,
  especially in Standard / Growth?
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import _default_start_date
from src.domains.analytics.volume_ratio_future_return_regime import (
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_SAMPLE_SEED,
    DEFAULT_SAMPLE_SIZE_PER_UNIVERSE,
    DEFAULT_TOP_K,
    DEFAULT_VALIDATION_RATIO,
    ENTRY_MODE_LABEL_MAP,
    ENTRY_MODE_ORDER,
    MEMBERSHIP_MODE,
    REFERENCE_LONG_PERIOD,
    REFERENCE_SHORT_PERIOD,
    REFERENCE_THRESHOLD,
    SPLIT_ORDER,
    UNIVERSE_LABEL_MAP,
    UNIVERSE_ORDER,
    _build_parameter_grid,
    _coerce_float,
    _coerce_int,
    _iter_split_frames,
    _normalize_float_sequence,
    _normalize_int_sequence,
    _prepare_universe_panel,
    _query_universe_codes,
    _query_universe_stock_history,
    _resolve_validation_split_dates,
    _safe_optional_date,
    _safe_welch_t_test,
    _sample_codes_for_universe,
)

UniverseKey = Literal["topix500", "prime_ex_topix500", "standard", "growth"]
SplitKey = Literal["full", "discovery", "validation"]
EntryModeKey = Literal["close_to_close", "next_open_to_close"]
SignalFamilyKey = Literal["volume_ratio", "trading_value_ratio"]
ConditionFamilyKey = Literal["trend_state", "adv20_quintile", "volatility20_quintile"]

VOLUME_TRADING_VALUE_CONDITIONING_EXPERIMENT_ID = (
    "market-behavior/volume-trading-value-conditioning"
)
DEFAULT_SHORT_WINDOWS: tuple[int, ...] = (REFERENCE_SHORT_PERIOD,)
DEFAULT_LONG_WINDOWS: tuple[int, ...] = (REFERENCE_LONG_PERIOD,)
DEFAULT_THRESHOLDS: tuple[float, ...] = (REFERENCE_THRESHOLD,)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20)
DEFAULT_MIN_SIGNAL_EVENTS = 30
DEFAULT_MIN_UNIQUE_CODES = 15
SIGNAL_FAMILY_ORDER: tuple[SignalFamilyKey, ...] = (
    "volume_ratio",
    "trading_value_ratio",
)
SIGNAL_FAMILY_LABEL_MAP: dict[SignalFamilyKey, str] = {
    "volume_ratio": "Volume Ratio Surge",
    "trading_value_ratio": "Trading Value Ratio Surge",
}
CONDITION_FAMILY_ORDER: tuple[ConditionFamilyKey, ...] = (
    "trend_state",
    "adv20_quintile",
    "volatility20_quintile",
)
CONDITION_FAMILY_LABEL_MAP: dict[ConditionFamilyKey, str] = {
    "trend_state": "Trend state",
    "adv20_quintile": "ADV20 quintile",
    "volatility20_quintile": "Vol20 quintile",
}
CONDITION_VALUE_ORDER: dict[ConditionFamilyKey, tuple[str, ...]] = {
    "trend_state": ("above_sma150", "below_or_at_sma150"),
    "adv20_quintile": ("q1", "q2", "q3", "q4", "q5"),
    "volatility20_quintile": ("q1", "q2", "q3", "q4", "q5"),
}
CONDITION_VALUE_LABEL_MAP: dict[tuple[ConditionFamilyKey, str], str] = {
    ("trend_state", "above_sma150"): "Close > SMA150",
    ("trend_state", "below_or_at_sma150"): "Close <= SMA150",
    ("adv20_quintile", "q1"): "ADV20 Q1 Lowest",
    ("adv20_quintile", "q2"): "ADV20 Q2",
    ("adv20_quintile", "q3"): "ADV20 Q3",
    ("adv20_quintile", "q4"): "ADV20 Q4",
    ("adv20_quintile", "q5"): "ADV20 Q5 Highest",
    ("volatility20_quintile", "q1"): "Vol20 Q1 Lowest",
    ("volatility20_quintile", "q2"): "Vol20 Q2",
    ("volatility20_quintile", "q3"): "Vol20 Q3",
    ("volatility20_quintile", "q4"): "Vol20 Q4",
    ("volatility20_quintile", "q5"): "Vol20 Q5 Highest",
}
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "parameter_grid_df",
    "universe_summary_df",
    "sampled_codes_df",
    "overall_signal_summary_df",
    "conditioned_signal_summary_df",
    "signal_family_compare_df",
    "top_condition_buckets_df",
)


@dataclass(frozen=True)
class VolumeTradingValueConditioningResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    validation_ratio: float
    analysis_use_sampled_codes: bool
    sample_seed: int
    sample_size_per_universe: int
    short_windows: tuple[int, ...]
    long_windows: tuple[int, ...]
    threshold_values: tuple[float, ...]
    horizons: tuple[int, ...]
    min_signal_events: int
    min_unique_codes: int
    top_k: int
    membership_mode: str
    parameter_grid_df: pd.DataFrame
    universe_summary_df: pd.DataFrame
    sampled_codes_df: pd.DataFrame
    overall_signal_summary_df: pd.DataFrame
    conditioned_signal_summary_df: pd.DataFrame
    signal_family_compare_df: pd.DataFrame
    top_condition_buckets_df: pd.DataFrame


def _ensure_trading_value_ma_columns(
    panel_df: pd.DataFrame,
    *,
    windows: tuple[int, ...],
) -> pd.DataFrame:
    if panel_df.empty:
        return panel_df
    result = panel_df.copy()
    grouped = result.groupby("code", sort=False)["trading_value"]
    for window in windows:
        column_name = f"trading_value_ma_{window}"
        if column_name in result.columns:
            continue
        result[column_name] = (
            grouped.rolling(window=window, min_periods=window)
            .mean()
            .reset_index(level=0, drop=True)
        )
    return result


def _assign_daily_quintile_labels(
    panel_df: pd.DataFrame,
    *,
    source_column: str,
    output_column: str,
) -> pd.DataFrame:
    result = panel_df.copy()
    ranks = result.groupby("date")[source_column].rank(method="average", pct=True)
    quintiles = np.ceil(ranks * 5.0).clip(1, 5)
    result[output_column] = quintiles.where(result[source_column].notna()).map(
        lambda value: f"q{int(value)}" if pd.notna(value) else pd.NA
    )
    return result


def _prepare_condition_panel(
    raw_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    unique_windows: tuple[int, ...],
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    panel_df = _prepare_universe_panel(
        raw_df,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        unique_volume_windows=unique_windows,
        horizons=horizons,
    )
    panel_df = _ensure_trading_value_ma_columns(panel_df, windows=unique_windows)
    panel_df = _assign_daily_quintile_labels(
        panel_df,
        source_column="adv_20",
        output_column="adv20_quintile",
    )
    panel_df = _assign_daily_quintile_labels(
        panel_df,
        source_column="volatility_20",
        output_column="volatility20_quintile",
    )
    return panel_df


def _sort_conditioning_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    if "universe_key" in sorted_df.columns:
        sorted_df["_universe_order"] = sorted_df["universe_key"].map(
            {key: index for index, key in enumerate(UNIVERSE_ORDER, start=1)}
        )
    if "split_key" in sorted_df.columns:
        sorted_df["_split_order"] = sorted_df["split_key"].map(
            {key: index for index, key in enumerate(SPLIT_ORDER, start=1)}
        )
    if "entry_mode" in sorted_df.columns:
        sorted_df["_entry_mode_order"] = sorted_df["entry_mode"].map(
            {key: index for index, key in enumerate(ENTRY_MODE_ORDER, start=1)}
        )
    if "signal_family" in sorted_df.columns:
        sorted_df["_signal_family_order"] = sorted_df["signal_family"].map(
            {key: index for index, key in enumerate(SIGNAL_FAMILY_ORDER, start=1)}
        )
    if "condition_family" in sorted_df.columns:
        sorted_df["_condition_family_order"] = sorted_df["condition_family"].map(
            {key: index for index, key in enumerate(CONDITION_FAMILY_ORDER, start=1)}
        )
    if "condition_value" in sorted_df.columns and "condition_family" in sorted_df.columns:
        sorted_df["_condition_value_order"] = sorted_df.apply(
            lambda row: (
                CONDITION_VALUE_ORDER.get(cast(ConditionFamilyKey, row["condition_family"]), ())
                .index(str(row["condition_value"]))
                + 1
                if str(row["condition_value"])
                in CONDITION_VALUE_ORDER.get(
                    cast(ConditionFamilyKey, row["condition_family"]),
                    (),
                )
                else 999
            ),
            axis=1,
        )

    sort_columns = [
        column
        for column in [
            "_universe_order",
            "_split_order",
            "_entry_mode_order",
            "_signal_family_order",
            "horizon_days",
            "short_period",
            "long_period",
            "threshold",
            "_condition_family_order",
            "_condition_value_order",
            "selection_rank",
            "sample_rank",
            "date",
            "code",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns, kind="stable").reset_index(
            drop=True
        )

    return sorted_df.drop(
        columns=[
            column
            for column in [
                "_universe_order",
                "_split_order",
                "_entry_mode_order",
                "_signal_family_order",
                "_condition_family_order",
                "_condition_value_order",
            ]
            if column in sorted_df.columns
        ]
    )


def _signal_ratio_series(
    panel_df: pd.DataFrame,
    *,
    signal_family: SignalFamilyKey,
    short_period: int,
    long_period: int,
) -> pd.Series:
    if signal_family == "volume_ratio":
        numerator = panel_df[f"volume_ma_{short_period}"]
        denominator = panel_df[f"volume_ma_{long_period}"]
    else:
        numerator = panel_df[f"trading_value_ma_{short_period}"]
        denominator = panel_df[f"trading_value_ma_{long_period}"]
    ratio = numerator / denominator
    return ratio.where(np.isfinite(ratio))


def _compute_signal_metrics(
    scoped_df: pd.DataFrame,
    *,
    score_column: str,
    future_return_column: str,
    threshold: float,
    min_signal_events: int,
    min_unique_codes: int,
) -> dict[str, Any]:
    total_event_count = int(len(scoped_df))
    total_unique_code_count = int(scoped_df["code"].nunique())
    total_date_count = int(scoped_df["date"].nunique())
    signal_mask = scoped_df[score_column] > threshold
    signal_df = scoped_df.loc[signal_mask].copy()
    non_signal_df = scoped_df.loc[~signal_mask].copy()
    signal_values = signal_df[future_return_column]
    non_signal_values = non_signal_df[future_return_column]

    signal_event_count = int(len(signal_df))
    signal_unique_code_count = int(signal_df["code"].nunique())
    signal_date_count = int(signal_df["date"].nunique())
    non_signal_mean = float(non_signal_values.mean()) if not non_signal_values.empty else None
    non_signal_positive_ratio = (
        float((non_signal_values > 0).mean()) if not non_signal_values.empty else None
    )
    t_stat, p_value = _safe_welch_t_test(signal_values, non_signal_values)

    return {
        "total_event_count": total_event_count,
        "total_unique_code_count": total_unique_code_count,
        "total_date_count": total_date_count,
        "signal_event_count": signal_event_count,
        "signal_unique_code_count": signal_unique_code_count,
        "signal_date_count": signal_date_count,
        "non_signal_event_count": int(len(non_signal_df)),
        "non_signal_unique_code_count": int(non_signal_df["code"].nunique()),
        "signal_rate": float(signal_event_count / total_event_count) if total_event_count else 0.0,
        "mean_signal_ratio": float(signal_df[score_column].mean()) if not signal_df.empty else None,
        "mean_signal_future_return": float(signal_values.mean()) if not signal_values.empty else None,
        "mean_non_signal_future_return": non_signal_mean,
        "median_signal_future_return": float(signal_values.median()) if not signal_values.empty else None,
        "positive_ratio_signal": float((signal_values > 0).mean()) if not signal_values.empty else None,
        "positive_ratio_non_signal": non_signal_positive_ratio,
        "mean_return_lift_vs_non_signal": (
            float(signal_values.mean() - non_signal_mean)
            if non_signal_mean is not None and not signal_values.empty
            else None
        ),
        "positive_ratio_lift_vs_non_signal": (
            float((signal_values > 0).mean() - non_signal_positive_ratio)
            if non_signal_positive_ratio is not None and not signal_values.empty
            else None
        ),
        "welch_t_statistic": t_stat,
        "welch_p_value": p_value,
        "meets_min_counts": bool(
            signal_event_count >= min_signal_events
            and signal_unique_code_count >= min_unique_codes
        ),
    }


def _build_universe_summary_row(
    *,
    universe_key: UniverseKey,
    all_codes_df: pd.DataFrame,
    analysis_panel_df: pd.DataFrame,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    discovery_end_date: str | None,
    validation_start_date: str | None,
    analysis_panel_mode: str,
) -> dict[str, Any]:
    return {
        "universe_key": universe_key,
        "universe_label": UNIVERSE_LABEL_MAP[universe_key],
        "membership_mode": MEMBERSHIP_MODE,
        "analysis_panel_mode": analysis_panel_mode,
        "stock_count": int(all_codes_df["code"].nunique()),
        "analysis_stock_count": int(analysis_panel_df["code"].nunique()),
        "stock_day_count": int(len(analysis_panel_df)),
        "analysis_date_count": int(analysis_panel_df["date"].nunique()),
        "available_start_date": _safe_optional_date(analysis_panel_df["date"].min()),
        "available_end_date": _safe_optional_date(analysis_panel_df["date"].max()),
        "analysis_start_date": analysis_start_date,
        "analysis_end_date": analysis_end_date,
        "discovery_end_date": discovery_end_date,
        "validation_start_date": validation_start_date,
    }


def _build_overall_signal_summary_for_universe(
    *,
    universe_key: UniverseKey,
    panel_df: pd.DataFrame,
    parameter_grid_df: pd.DataFrame,
    thresholds: tuple[float, ...],
    horizons: tuple[int, ...],
    validation_start_ts: pd.Timestamp | None,
    min_signal_events: int,
    min_unique_codes: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    base_columns = ["date", "code", "company_name"]

    for parameter_row in parameter_grid_df.itertuples(index=False):
        short_period = _coerce_int(parameter_row.short_period)
        long_period = _coerce_int(parameter_row.long_period)
        for signal_family in SIGNAL_FAMILY_ORDER:
            score_series = _signal_ratio_series(
                panel_df,
                signal_family=signal_family,
                short_period=short_period,
                long_period=long_period,
            )
            feature_df = panel_df.loc[np.isfinite(score_series), base_columns].copy()
            if feature_df.empty:
                continue
            feature_df["signal_score"] = score_series.loc[feature_df.index]
            for horizon_days in horizons:
                for entry_mode in ENTRY_MODE_ORDER:
                    feature_df[f"{entry_mode}_{horizon_days}d"] = panel_df.loc[
                        feature_df.index, f"{entry_mode}_{horizon_days}d"
                    ]

            for split_key, split_df in _iter_split_frames(
                feature_df,
                validation_start_ts=validation_start_ts,
            ):
                for entry_mode in ENTRY_MODE_ORDER:
                    for horizon_days in horizons:
                        future_return_column = f"{entry_mode}_{horizon_days}d"
                        scoped_df = split_df.dropna(subset=[future_return_column]).copy()
                        if scoped_df.empty:
                            continue
                        for threshold in thresholds:
                            metrics = _compute_signal_metrics(
                                scoped_df,
                                score_column="signal_score",
                                future_return_column=future_return_column,
                                threshold=threshold,
                                min_signal_events=min_signal_events,
                                min_unique_codes=min_unique_codes,
                            )
                            rows.append(
                                {
                                    "universe_key": universe_key,
                                    "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                                    "signal_family": signal_family,
                                    "signal_family_label": SIGNAL_FAMILY_LABEL_MAP[signal_family],
                                    "split_key": split_key,
                                    "entry_mode": entry_mode,
                                    "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                                    "horizon_days": horizon_days,
                                    "short_period": short_period,
                                    "long_period": long_period,
                                    "threshold": threshold,
                                    **metrics,
                                }
                            )
    return _sort_conditioning_table(pd.DataFrame(rows))


def _build_conditioned_signal_summary_for_universe(
    *,
    universe_key: UniverseKey,
    panel_df: pd.DataFrame,
    parameter_grid_df: pd.DataFrame,
    thresholds: tuple[float, ...],
    horizons: tuple[int, ...],
    validation_start_ts: pd.Timestamp | None,
    min_signal_events: int,
    min_unique_codes: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    base_columns = [
        "date",
        "code",
        "company_name",
        "trend_state",
        "adv20_quintile",
        "volatility20_quintile",
    ]

    for parameter_row in parameter_grid_df.itertuples(index=False):
        short_period = _coerce_int(parameter_row.short_period)
        long_period = _coerce_int(parameter_row.long_period)
        for signal_family in SIGNAL_FAMILY_ORDER:
            score_series = _signal_ratio_series(
                panel_df,
                signal_family=signal_family,
                short_period=short_period,
                long_period=long_period,
            )
            feature_df = panel_df.loc[np.isfinite(score_series), base_columns].copy()
            if feature_df.empty:
                continue
            feature_df["signal_score"] = score_series.loc[feature_df.index]
            for horizon_days in horizons:
                for entry_mode in ENTRY_MODE_ORDER:
                    feature_df[f"{entry_mode}_{horizon_days}d"] = panel_df.loc[
                        feature_df.index, f"{entry_mode}_{horizon_days}d"
                    ]

            for split_key, split_df in _iter_split_frames(
                feature_df,
                validation_start_ts=validation_start_ts,
            ):
                for entry_mode in ENTRY_MODE_ORDER:
                    for horizon_days in horizons:
                        future_return_column = f"{entry_mode}_{horizon_days}d"
                        base_scope_df = split_df.dropna(subset=[future_return_column]).copy()
                        if base_scope_df.empty:
                            continue

                        for condition_family in CONDITION_FAMILY_ORDER:
                            family_scope_df = base_scope_df.dropna(subset=[condition_family]).copy()
                            if family_scope_df.empty:
                                continue
                            for condition_value in CONDITION_VALUE_ORDER[condition_family]:
                                scoped_df = family_scope_df.loc[
                                    family_scope_df[condition_family] == condition_value
                                ].copy()
                                if scoped_df.empty:
                                    continue
                                for threshold in thresholds:
                                    metrics = _compute_signal_metrics(
                                        scoped_df,
                                        score_column="signal_score",
                                        future_return_column=future_return_column,
                                        threshold=threshold,
                                        min_signal_events=min_signal_events,
                                        min_unique_codes=min_unique_codes,
                                    )
                                    rows.append(
                                        {
                                            "universe_key": universe_key,
                                            "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                                            "signal_family": signal_family,
                                            "signal_family_label": SIGNAL_FAMILY_LABEL_MAP[signal_family],
                                            "split_key": split_key,
                                            "entry_mode": entry_mode,
                                            "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                                            "horizon_days": horizon_days,
                                            "short_period": short_period,
                                            "long_period": long_period,
                                            "threshold": threshold,
                                            "condition_family": condition_family,
                                            "condition_family_label": CONDITION_FAMILY_LABEL_MAP[
                                                condition_family
                                            ],
                                            "condition_value": condition_value,
                                            "condition_value_label": CONDITION_VALUE_LABEL_MAP[
                                                (condition_family, condition_value)
                                            ],
                                            **metrics,
                                        }
                                    )
    return _sort_conditioning_table(pd.DataFrame(rows))


def _build_signal_family_compare_table(
    conditioned_signal_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if conditioned_signal_summary_df.empty:
        return pd.DataFrame()

    merge_keys = [
        "universe_key",
        "universe_label",
        "split_key",
        "entry_mode",
        "entry_mode_label",
        "horizon_days",
        "short_period",
        "long_period",
        "threshold",
        "condition_family",
        "condition_family_label",
        "condition_value",
        "condition_value_label",
    ]
    metric_columns = [
        "signal_event_count",
        "signal_unique_code_count",
        "signal_rate",
        "mean_signal_ratio",
        "mean_signal_future_return",
        "mean_non_signal_future_return",
        "mean_return_lift_vs_non_signal",
        "positive_ratio_signal",
        "positive_ratio_non_signal",
        "positive_ratio_lift_vs_non_signal",
        "welch_p_value",
        "meets_min_counts",
    ]
    family_tables: dict[str, pd.DataFrame] = {}
    for signal_family in SIGNAL_FAMILY_ORDER:
        scoped_df = conditioned_signal_summary_df.loc[
            conditioned_signal_summary_df["signal_family"] == signal_family,
            [*merge_keys, *metric_columns],
        ].copy()
        if scoped_df.empty:
            continue
        family_tables[signal_family] = scoped_df.rename(
            columns={
                column: f"{signal_family}_{column}" for column in metric_columns
            }
        )
    if set(family_tables) != set(SIGNAL_FAMILY_ORDER):
        return pd.DataFrame()

    merged_df = family_tables["volume_ratio"].merge(
        family_tables["trading_value_ratio"],
        how="inner",
        on=merge_keys,
    )
    merged_df["lift_difference_trading_minus_volume"] = (
        merged_df["trading_value_ratio_mean_return_lift_vs_non_signal"]
        - merged_df["volume_ratio_mean_return_lift_vs_non_signal"]
    )
    merged_df["winning_signal_family"] = merged_df[
        "lift_difference_trading_minus_volume"
    ].map(
        lambda value: (
            "trading_value_ratio"
            if pd.notna(value) and value > 0
            else "volume_ratio" if pd.notna(value) and value < 0 else "tie"
        )
    )
    return _sort_conditioning_table(merged_df)


def _build_top_condition_buckets_table(
    conditioned_signal_summary_df: pd.DataFrame,
    *,
    top_k: int,
) -> pd.DataFrame:
    if conditioned_signal_summary_df.empty:
        return pd.DataFrame()
    ranked_df = conditioned_signal_summary_df.copy()
    ranked_df = ranked_df.loc[ranked_df["meets_min_counts"].fillna(False)].copy()
    if ranked_df.empty:
        return pd.DataFrame()
    ranked_df = ranked_df.sort_values(
        by=[
            "mean_return_lift_vs_non_signal",
            "signal_event_count",
            "positive_ratio_lift_vs_non_signal",
        ],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    ranked_df["selection_rank"] = (
        ranked_df.groupby(
            ["universe_key", "signal_family", "split_key", "entry_mode", "horizon_days"]
        ).cumcount()
        + 1
    )
    return _sort_conditioning_table(
        ranked_df.loc[ranked_df["selection_rank"] <= top_k].copy()
    )


def _build_summary_markdown(
    result: VolumeTradingValueConditioningResult,
) -> str:
    lines: list[str] = [
        "# Volume Vs Trading Value Conditioning",
        "",
        "## Snapshot",
        "",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        (
            f"- Analysis panel: `sampled {result.sample_size_per_universe} codes per universe`"
            if result.analysis_use_sampled_codes
            else "- Analysis panel: `full panel`"
        ),
        f"- Universes: `{', '.join(UNIVERSE_LABEL_MAP[key] for key in UNIVERSE_ORDER)}`",
        f"- Signal families: `{', '.join(SIGNAL_FAMILY_LABEL_MAP[key] for key in SIGNAL_FAMILY_ORDER)}`",
        f"- Thresholds: `{', '.join(f'{value:.1f}' for value in result.threshold_values)}`",
        f"- Parameter pairs: `{len(result.parameter_grid_df)}`",
        "",
        "## Universe Coverage",
        "",
    ]
    for row in result.universe_summary_df.itertuples(index=False):
        lines.append(
            f"- `{row.universe_label}`: universe stocks=`{row.stock_count}`, analysis stocks=`{row.analysis_stock_count}`, "
            f"stock-days=`{row.stock_day_count}`, panel=`{row.analysis_panel_mode}`"
        )

    lines.extend(["", "## Overall 5d Next-Open Lift", ""])
    overall_rows = result.overall_signal_summary_df.loc[
        (result.overall_signal_summary_df["split_key"] == "full")
        & (result.overall_signal_summary_df["entry_mode"] == "next_open_to_close")
        & (result.overall_signal_summary_df["horizon_days"] == 5)
    ].copy()
    if overall_rows.empty:
        lines.append("- No overall rows were generated.")
    else:
        for universe_key in UNIVERSE_ORDER:
            scoped_df = overall_rows.loc[
                overall_rows["universe_key"] == universe_key
            ].copy()
            if scoped_df.empty:
                continue
            for signal_family in SIGNAL_FAMILY_ORDER:
                family_df = scoped_df.loc[scoped_df["signal_family"] == signal_family]
                if family_df.empty:
                    continue
                best_row = family_df.sort_values(
                    by=["mean_return_lift_vs_non_signal", "signal_event_count"],
                    ascending=[False, False],
                ).iloc[0]
                lift = (
                    "N/A"
                    if pd.isna(best_row["mean_return_lift_vs_non_signal"])
                    else f"{float(best_row['mean_return_lift_vs_non_signal']) * 10000:.1f} bps"
                )
                lines.append(
                    f"- `{UNIVERSE_LABEL_MAP[universe_key]}` `{SIGNAL_FAMILY_LABEL_MAP[signal_family]}`: "
                    f"`short={int(best_row['short_period'])}, long={int(best_row['long_period'])}, threshold={float(best_row['threshold']):.1f}` -> `{lift}`"
                )

    lines.extend(["", "## Best Validation Buckets", ""])
    top_rows = result.top_condition_buckets_df.loc[
        (result.top_condition_buckets_df["selection_rank"] == 1)
        & (result.top_condition_buckets_df["split_key"] == "validation")
        & (result.top_condition_buckets_df["entry_mode"] == "next_open_to_close")
        & (result.top_condition_buckets_df["horizon_days"] == 5)
    ].copy()
    if top_rows.empty:
        lines.append("- No validation buckets satisfied the minimum-count gate.")
    else:
        for row in top_rows.itertuples(index=False):
            lift = (
                "N/A"
                if row.mean_return_lift_vs_non_signal is None
                else f"{_coerce_float(row.mean_return_lift_vs_non_signal) * 10000:.1f} bps"
            )
            lines.append(
                f"- `{row.universe_label}` `{row.signal_family_label}` best bucket: "
                f"`{row.condition_value_label}` -> `{lift}` (`events={_coerce_int(row.signal_event_count)}`)"
            )

    lines.extend(["", "## Trading Value Minus Volume", ""])
    compare_rows = result.signal_family_compare_df.loc[
        (result.signal_family_compare_df["split_key"] == "validation")
        & (result.signal_family_compare_df["entry_mode"] == "next_open_to_close")
        & (result.signal_family_compare_df["horizon_days"] == 5)
    ].copy()
    if compare_rows.empty:
        lines.append("- No family-comparison rows were generated.")
    else:
        for universe_key in ("standard", "growth"):
            scoped_df = compare_rows.loc[
                compare_rows["universe_key"] == universe_key
            ].copy()
            if scoped_df.empty:
                continue
            best_row = scoped_df.sort_values(
                by=["lift_difference_trading_minus_volume"],
                ascending=[False],
            ).iloc[0]
            diff = (
                "N/A"
                if pd.isna(best_row["lift_difference_trading_minus_volume"])
                else f"{float(best_row['lift_difference_trading_minus_volume']) * 10000:.1f} bps"
            )
            lines.append(
                f"- `{UNIVERSE_LABEL_MAP[universe_key]}` strongest trading-value advantage: "
                f"`{best_row['condition_value_label']}` with delta `{diff}`"
            )

    lines.extend(["", "## Artifact Tables", ""])
    for table_name in TABLE_FIELD_NAMES:
        lines.append(f"- `{table_name}`")
    return "\n".join(lines)


def _split_result_payload(
    result: VolumeTradingValueConditioningResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata_fields = {field.name for field in fields(result)} - set(TABLE_FIELD_NAMES)
    result_metadata = {
        field_name: getattr(result, field_name) for field_name in sorted(metadata_fields)
    }
    result_tables = {
        field_name: cast(pd.DataFrame, getattr(result, field_name))
        for field_name in TABLE_FIELD_NAMES
    }
    return result_metadata, result_tables


def run_volume_trading_value_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    analysis_use_sampled_codes: bool = False,
    short_windows: tuple[int, ...] | list[int] | None = None,
    long_windows: tuple[int, ...] | list[int] | None = None,
    threshold_values: tuple[float, ...] | list[float] | None = None,
    horizons: tuple[int, ...] | list[int] | None = None,
    sample_seed: int = DEFAULT_SAMPLE_SEED,
    sample_size_per_universe: int = DEFAULT_SAMPLE_SIZE_PER_UNIVERSE,
    min_signal_events: int = DEFAULT_MIN_SIGNAL_EVENTS,
    min_unique_codes: int = DEFAULT_MIN_UNIQUE_CODES,
    top_k: int = DEFAULT_TOP_K,
) -> VolumeTradingValueConditioningResult:
    if lookback_years <= 0:
        raise ValueError("lookback_years must be positive")
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= ratio < 1.0")
    if sample_size_per_universe < 0:
        raise ValueError("sample_size_per_universe must be non-negative")
    if min_signal_events < 0:
        raise ValueError("min_signal_events must be non-negative")
    if min_unique_codes < 0:
        raise ValueError("min_unique_codes must be non-negative")
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    normalized_short_windows = _normalize_int_sequence(
        short_windows,
        fallback=DEFAULT_SHORT_WINDOWS,
        name="short_windows",
    )
    normalized_long_windows = _normalize_int_sequence(
        long_windows,
        fallback=DEFAULT_LONG_WINDOWS,
        name="long_windows",
    )
    normalized_threshold_values = _normalize_float_sequence(
        threshold_values,
        fallback=DEFAULT_THRESHOLDS,
        name="threshold_values",
    )
    normalized_horizons = _normalize_int_sequence(
        horizons,
        fallback=DEFAULT_HORIZONS,
        name="horizons",
    )
    parameter_grid_df = _build_parameter_grid(
        short_windows=normalized_short_windows,
        long_windows=normalized_long_windows,
    )
    unique_windows = tuple(sorted({*normalized_short_windows, *normalized_long_windows}))

    universe_summary_rows: list[dict[str, Any]] = []
    sampled_code_tables: list[pd.DataFrame] = []
    overall_tables: list[pd.DataFrame] = []
    conditioned_tables: list[pd.DataFrame] = []
    available_start_dates: list[str] = []
    available_end_dates: list[str] = []
    analysis_start_dates: list[str] = []
    analysis_end_dates: list[str] = []
    source_mode: SourceMode = "live"
    source_detail = ""

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        source_mode = cast(SourceMode, ctx.source_mode)
        source_detail = str(ctx.source_detail)

        for universe_key in UNIVERSE_ORDER:
            universe_codes_df = _query_universe_codes(conn, universe_key=universe_key)
            history_df = _query_universe_stock_history(
                conn,
                universe_key=universe_key,
                end_date=end_date,
            )
            if history_df.empty:
                continue

            available_start_date = _safe_optional_date(history_df["date"].min())
            available_end_date = _safe_optional_date(history_df["date"].max())
            effective_start_date = start_date or _default_start_date(
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                lookback_years=lookback_years,
            )
            effective_end_date = end_date or available_end_date

            universe_panel_df = _prepare_condition_panel(
                history_df,
                analysis_start_date=effective_start_date,
                analysis_end_date=effective_end_date,
                unique_windows=unique_windows,
                horizons=normalized_horizons,
            )
            if universe_panel_df.empty:
                continue

            sampled_codes_df = _sample_codes_for_universe(
                universe_codes_df,
                universe_key=universe_key,
                sample_seed=sample_seed,
                sample_size=sample_size_per_universe,
            )
            sampled_code_tables.append(sampled_codes_df)
            analysis_panel_df = universe_panel_df
            analysis_panel_mode = "full_panel"
            if analysis_use_sampled_codes and not sampled_codes_df.empty:
                analysis_panel_df = universe_panel_df.merge(
                    sampled_codes_df[["code"]],
                    on="code",
                    how="inner",
                )
                analysis_panel_mode = "sampled_codes_only"
            if analysis_panel_df.empty:
                continue

            discovery_end_date, validation_start_date, validation_start_ts = (
                _resolve_validation_split_dates(
                    analysis_panel_df,
                    validation_ratio=validation_ratio,
                )
            )

            universe_summary_rows.append(
                _build_universe_summary_row(
                    universe_key=universe_key,
                    all_codes_df=universe_codes_df,
                    analysis_panel_df=analysis_panel_df,
                    analysis_start_date=effective_start_date,
                    analysis_end_date=effective_end_date,
                    discovery_end_date=discovery_end_date,
                    validation_start_date=validation_start_date,
                    analysis_panel_mode=analysis_panel_mode,
                )
            )
            overall_tables.append(
                _build_overall_signal_summary_for_universe(
                    universe_key=universe_key,
                    panel_df=analysis_panel_df,
                    parameter_grid_df=parameter_grid_df,
                    thresholds=normalized_threshold_values,
                    horizons=normalized_horizons,
                    validation_start_ts=validation_start_ts,
                    min_signal_events=min_signal_events,
                    min_unique_codes=min_unique_codes,
                )
            )
            conditioned_tables.append(
                _build_conditioned_signal_summary_for_universe(
                    universe_key=universe_key,
                    panel_df=analysis_panel_df,
                    parameter_grid_df=parameter_grid_df,
                    thresholds=normalized_threshold_values,
                    horizons=normalized_horizons,
                    validation_start_ts=validation_start_ts,
                    min_signal_events=min_signal_events,
                    min_unique_codes=min_unique_codes,
                )
            )

            if available_start_date is not None:
                available_start_dates.append(available_start_date)
            if available_end_date is not None:
                available_end_dates.append(available_end_date)
            if effective_start_date is not None:
                analysis_start_dates.append(effective_start_date)
            if effective_end_date is not None:
                analysis_end_dates.append(effective_end_date)

    overall_signal_summary_df = _sort_conditioning_table(
        pd.concat(overall_tables, ignore_index=True) if overall_tables else pd.DataFrame()
    )
    conditioned_signal_summary_df = _sort_conditioning_table(
        pd.concat(conditioned_tables, ignore_index=True)
        if conditioned_tables
        else pd.DataFrame()
    )
    result = VolumeTradingValueConditioningResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=min(available_start_dates) if available_start_dates else None,
        available_end_date=max(available_end_dates) if available_end_dates else None,
        analysis_start_date=min(analysis_start_dates) if analysis_start_dates else None,
        analysis_end_date=max(analysis_end_dates) if analysis_end_dates else None,
        lookback_years=lookback_years,
        validation_ratio=validation_ratio,
        analysis_use_sampled_codes=analysis_use_sampled_codes,
        sample_seed=sample_seed,
        sample_size_per_universe=sample_size_per_universe,
        short_windows=normalized_short_windows,
        long_windows=normalized_long_windows,
        threshold_values=normalized_threshold_values,
        horizons=normalized_horizons,
        min_signal_events=min_signal_events,
        min_unique_codes=min_unique_codes,
        top_k=top_k,
        membership_mode=MEMBERSHIP_MODE,
        parameter_grid_df=_sort_conditioning_table(parameter_grid_df),
        universe_summary_df=_sort_conditioning_table(pd.DataFrame(universe_summary_rows)),
        sampled_codes_df=_sort_conditioning_table(
            pd.concat(sampled_code_tables, ignore_index=True)
            if sampled_code_tables
            else pd.DataFrame()
        ),
        overall_signal_summary_df=overall_signal_summary_df,
        conditioned_signal_summary_df=conditioned_signal_summary_df,
        signal_family_compare_df=_build_signal_family_compare_table(
            conditioned_signal_summary_df
        ),
        top_condition_buckets_df=_build_top_condition_buckets_table(
            conditioned_signal_summary_df,
            top_k=top_k,
        ),
    )
    return result


def write_volume_trading_value_conditioning_research_bundle(
    result: VolumeTradingValueConditioningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=VOLUME_TRADING_VALUE_CONDITIONING_EXPERIMENT_ID,
        module=__name__,
        function="run_volume_trading_value_conditioning_research",
        params={
            "lookback_years": result.lookback_years,
            "validation_ratio": result.validation_ratio,
            "analysis_use_sampled_codes": result.analysis_use_sampled_codes,
            "sample_seed": result.sample_seed,
            "sample_size_per_universe": result.sample_size_per_universe,
            "short_windows": list(result.short_windows),
            "long_windows": list(result.long_windows),
            "threshold_values": list(result.threshold_values),
            "horizons": list(result.horizons),
            "min_signal_events": result.min_signal_events,
            "min_unique_codes": result.min_unique_codes,
            "top_k": result.top_k,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_volume_trading_value_conditioning_research_bundle(
    bundle_path: str | Path,
) -> VolumeTradingValueConditioningResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return VolumeTradingValueConditioningResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        lookback_years=int(metadata["lookback_years"]),
        validation_ratio=float(metadata["validation_ratio"]),
        analysis_use_sampled_codes=bool(metadata["analysis_use_sampled_codes"]),
        sample_seed=int(metadata["sample_seed"]),
        sample_size_per_universe=int(metadata["sample_size_per_universe"]),
        short_windows=tuple(int(value) for value in metadata["short_windows"]),
        long_windows=tuple(int(value) for value in metadata["long_windows"]),
        threshold_values=tuple(float(value) for value in metadata["threshold_values"]),
        horizons=tuple(int(value) for value in metadata["horizons"]),
        min_signal_events=int(metadata["min_signal_events"]),
        min_unique_codes=int(metadata["min_unique_codes"]),
        top_k=int(metadata["top_k"]),
        membership_mode=str(metadata["membership_mode"]),
        parameter_grid_df=tables["parameter_grid_df"],
        universe_summary_df=tables["universe_summary_df"],
        sampled_codes_df=tables["sampled_codes_df"],
        overall_signal_summary_df=tables["overall_signal_summary_df"],
        conditioned_signal_summary_df=tables["conditioned_signal_summary_df"],
        signal_family_compare_df=tables["signal_family_compare_df"],
        top_condition_buckets_df=tables["top_condition_buckets_df"],
    )


def get_volume_trading_value_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        VOLUME_TRADING_VALUE_CONDITIONING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_volume_trading_value_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        VOLUME_TRADING_VALUE_CONDITIONING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
