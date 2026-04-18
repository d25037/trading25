"""
TOPIX100 14:45 entry daily-SMA filter comparison research.

This enriches the existing 14:45 signal/regime selected-trade table with
prior-close daily SMA filters so the same candidate branch can be split into:

- above daily SMA
- at or below daily SMA

The intended first use is `15m / previous_open_vs_open / next_open`, especially
the `weak x losers` and `strong x winners` branches.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

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
from src.domains.analytics.topix100_1445_entry_signal_regime_comparison import (
    DEFAULT_BUCKET_COUNT,
    DEFAULT_ENTRY_TIME,
    DEFAULT_NEXT_SESSION_EXIT_TIME,
    DEFAULT_PERIOD_MONTHS,
    DEFAULT_TAIL_FRACTION,
    Topix1001445EntrySignalRegimeComparisonResult,
    run_topix100_1445_entry_signal_regime_comparison_research,
)
from src.domains.analytics.topix100_open_relative_intraday_path import SourceMode
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _query_topix100_stock_history,
    _rolling_mean,
    _safe_ratio,
)

TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_COMPARISON_EXPERIMENT_ID = (
    "market-behavior/topix100-1445-entry-daily-sma-filter-comparison"
)
DEFAULT_INTERVAL_MINUTES = 15
DEFAULT_SIGNAL_FAMILY = "previous_open_vs_open"
DEFAULT_EXIT_LABEL = "next_open"
DEFAULT_DAILY_SMA_WINDOWS: tuple[int, ...] = (20, 50, 100)

_MARKET_REGIME_ORDER: tuple[str, ...] = ("weak", "neutral", "strong")
_SUBGROUP_ORDER: tuple[str, ...] = ("all", "winners", "middle", "losers")
_SMA_FILTER_STATE_ORDER: tuple[str, ...] = ("all", "above", "at_or_below")
_SMA_FILTER_LABEL_MAP: dict[str, str] = {
    "all": "All",
    "above": "Above daily SMA",
    "at_or_below": "At or below daily SMA",
}
_SELECTED_BRANCH_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "signal_label",
    "target_bucket_side",
    "expected_selected_bucket_label",
    "exit_label",
    "exit_time_target",
    "subgroup_key",
    "subgroup_label",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "date",
    "code",
    "signal_ratio",
    "signal_bucket_label",
    "market_regime_return",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "current_entry_bucket_key",
    "current_entry_bucket_label",
    "entry_price",
    "entry_actual_time",
    "exit_price",
    "exit_actual_time",
    "trade_return",
)
_SMA_TRADE_LEVEL_COLUMNS: tuple[str, ...] = (
    *_SELECTED_BRANCH_COLUMNS,
    "sma_window",
    "sma_label",
    "daily_sma",
    "entry_vs_sma_ratio",
    "sma_filter_state",
    "sma_filter_label",
)
_SMA_FILTER_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "exit_label",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "subgroup_key",
    "subgroup_label",
    "sma_window",
    "sma_label",
    "sma_filter_state",
    "sma_filter_label",
    "sample_count",
    "sample_share_within_branch",
    "date_count",
    "stock_count",
    "trade_return_mean",
    "trade_return_median",
    "trade_return_sum",
    "hit_positive",
)
_SMA_FILTER_COMPARISON_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "exit_label",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "subgroup_key",
    "subgroup_label",
    "sma_window",
    "sma_label",
    "branch_sample_count",
    "above_count",
    "at_or_below_count",
    "above_trade_return_mean",
    "at_or_below_trade_return_mean",
    "all_trade_return_mean",
    "above_minus_at_or_below",
    "above_minus_all",
    "at_or_below_minus_all",
)
_PERIOD_SMA_FILTER_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "exit_label",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "subgroup_key",
    "subgroup_label",
    "sma_window",
    "sma_label",
    "sma_filter_state",
    "sma_filter_label",
    "sample_count",
    "sample_share_within_branch",
    "date_count",
    "stock_count",
    "trade_return_mean",
    "trade_return_median",
    "trade_return_sum",
    "hit_positive",
)


@dataclass(frozen=True)
class Topix1001445EntryDailySmaFilterComparisonResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    signal_family: str
    exit_label: str
    daily_sma_windows: tuple[int, ...]
    bucket_count: int
    period_months: int
    entry_time: str
    next_session_exit_time: str
    tail_fraction: float
    topix100_constituent_count: int
    selected_trade_count: int
    sma_trade_count: int
    periods_df: pd.DataFrame
    selected_trade_level_df: pd.DataFrame
    sma_trade_level_df: pd.DataFrame
    sma_filter_summary_df: pd.DataFrame
    sma_filter_comparison_df: pd.DataFrame
    period_sma_filter_summary_df: pd.DataFrame


def _empty_selected_branch_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SELECTED_BRANCH_COLUMNS))


def _empty_sma_trade_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SMA_TRADE_LEVEL_COLUMNS))


def _empty_sma_filter_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SMA_FILTER_SUMMARY_COLUMNS))


def _empty_sma_filter_comparison_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SMA_FILTER_COMPARISON_COLUMNS))


def _empty_period_sma_filter_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_SMA_FILTER_SUMMARY_COLUMNS))


def _normalize_interval_minutes(value: int) -> int:
    interval_minutes = int(value)
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")
    return interval_minutes


def _normalize_sma_windows(values: Sequence[int] | None) -> tuple[int, ...]:
    if values is None:
        values = DEFAULT_DAILY_SMA_WINDOWS
    normalized = tuple(
        sorted(dict.fromkeys(int(value) for value in values if int(value) > 0))
    )
    if not normalized:
        raise ValueError("daily_sma_windows must contain at least one positive integer")
    return normalized


def _summarize_trade_returns(
    values: pd.Series,
) -> tuple[float | None, float | None, float | None, float | None]:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None, None, None, None
    return (
        float(valid.mean()),
        float(valid.median()),
        float(valid.sum()),
        float((valid > 0).mean()),
    )


def _subtract_optional_float(left: Any, right: Any) -> float | None:
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError):
        return None
    if pd.isna(left_value) or pd.isna(right_value):
        return None
    return left_value - right_value


def _query_daily_sma_panel(
    db_path: str,
    *,
    end_date: str | None,
    daily_sma_windows: Sequence[int],
) -> pd.DataFrame:
    with _open_analysis_connection(db_path) as ctx:
        history_df = cast(
            pd.DataFrame,
            _query_topix100_stock_history(ctx.connection, end_date=end_date),
        )
    if history_df.empty:
        columns = ["date", "code", *[f"daily_sma_{window}" for window in daily_sma_windows]]
        return pd.DataFrame(columns=columns)

    panel = history_df.copy()
    panel["date"] = panel["date"].astype(str)
    panel = panel.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    panel["_lagged_close"] = panel.groupby("code", sort=False)["close"].shift(1)
    for window in daily_sma_windows:
        panel[f"daily_sma_{window}"] = _rolling_mean(
            panel,
            column_name="_lagged_close",
            window=window,
        )
    return panel.loc[
        :,
        ["date", "code", *[f"daily_sma_{window}" for window in daily_sma_windows]],
    ].copy()


def _build_selected_branch_df(
    base_result: Topix1001445EntrySignalRegimeComparisonResult,
    *,
    interval_minutes: int,
    signal_family: str,
    exit_label: str,
) -> pd.DataFrame:
    selected_df = base_result.selected_trade_level_df.loc[
        (pd.to_numeric(base_result.selected_trade_level_df["interval_minutes"], errors="coerce") == interval_minutes)
        & (base_result.selected_trade_level_df["signal_family"].astype(str) == signal_family)
        & (base_result.selected_trade_level_df["exit_label"].astype(str) == exit_label)
    ].copy()
    if selected_df.empty:
        return _empty_selected_branch_df()
    return (
        selected_df.loc[:, list(_SELECTED_BRANCH_COLUMNS)]
        .reset_index(drop=True)
        .copy()
    )


def _build_sma_trade_level_df(
    selected_trade_level_df: pd.DataFrame,
    *,
    daily_sma_panel_df: pd.DataFrame,
    daily_sma_windows: Sequence[int],
) -> pd.DataFrame:
    if selected_trade_level_df.empty or daily_sma_panel_df.empty:
        return _empty_sma_trade_level_df()

    joined_df = selected_trade_level_df.merge(
        daily_sma_panel_df,
        how="left",
        on=["date", "code"],
    )
    frames: list[pd.DataFrame] = []
    for window in daily_sma_windows:
        sma_column = f"daily_sma_{window}"
        window_df = joined_df.loc[joined_df[sma_column].notna()].copy()
        if window_df.empty:
            continue
        window_entry = pd.to_numeric(window_df["entry_price"], errors="coerce")
        window_sma = pd.to_numeric(window_df[sma_column], errors="coerce")
        window_df["sma_window"] = window
        window_df["sma_label"] = f"SMA{window}"
        window_df["daily_sma"] = window_sma
        window_df["entry_vs_sma_ratio"] = _safe_ratio(window_entry, window_sma) - 1.0
        window_df["sma_filter_state"] = np.where(
            window_entry > window_sma,
            "above",
            "at_or_below",
        )
        window_df["sma_filter_label"] = window_df["sma_filter_state"].map(
            _SMA_FILTER_LABEL_MAP
        )
        frames.append(window_df.loc[:, list(_SMA_TRADE_LEVEL_COLUMNS)].copy())

        all_df = window_df.copy()
        all_df["sma_filter_state"] = "all"
        all_df["sma_filter_label"] = _SMA_FILTER_LABEL_MAP["all"]
        frames.append(all_df.loc[:, list(_SMA_TRADE_LEVEL_COLUMNS)].copy())

    if not frames:
        return _empty_sma_trade_level_df()
    return pd.concat(frames, ignore_index=True)


def _build_summary_rows(
    trade_df: pd.DataFrame,
    *,
    group_columns: Sequence[str],
    total_group_columns: Sequence[str],
    total_trade_df: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if trade_df.empty:
        return []
    if total_trade_df is None:
        total_trade_df = trade_df
    group_sizes = (
        total_trade_df.groupby(list(total_group_columns), as_index=False)
        .agg(total_sample_count=("code", "size"))
    )
    size_lookup = {
        tuple(row[column] for column in total_group_columns): int(row["total_sample_count"])
        for row in group_sizes.to_dict(orient="records")
    }
    rows: list[dict[str, Any]] = []
    for group_key, group_df in trade_df.groupby(list(group_columns), sort=True):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        key_map = dict(zip(group_columns, group_key, strict=True))
        lookup_key = tuple(key_map[column] for column in total_group_columns)
        total_sample_count = size_lookup[lookup_key]
        mean_value, median_value, sum_value, hit_positive = _summarize_trade_returns(
            group_df["trade_return"]
        )
        rows.append(
            {
                **key_map,
                "sample_count": int(len(group_df)),
                "sample_share_within_branch": float(len(group_df) / total_sample_count),
                "date_count": int(group_df["date"].nunique()),
                "stock_count": int(group_df["code"].nunique()),
                "trade_return_mean": mean_value,
                "trade_return_median": median_value,
                "trade_return_sum": sum_value,
                "hit_positive": hit_positive,
            }
        )
    return rows


def _sort_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    if "market_regime_bucket_key" in sorted_df.columns:
        sorted_df["_regime_order"] = sorted_df["market_regime_bucket_key"].map(
            {key: index for index, key in enumerate(_MARKET_REGIME_ORDER, start=1)}
        )
    if "subgroup_key" in sorted_df.columns:
        sorted_df["_subgroup_order"] = sorted_df["subgroup_key"].map(
            {key: index for index, key in enumerate(_SUBGROUP_ORDER, start=1)}
        )
    if "sma_filter_state" in sorted_df.columns:
        sorted_df["_sma_filter_order"] = sorted_df["sma_filter_state"].map(
            {key: index for index, key in enumerate(_SMA_FILTER_STATE_ORDER, start=1)}
        )
    sort_columns = [
        column
        for column in [
            "interval_minutes",
            "signal_family",
            "exit_label",
            "period_index",
            "_regime_order",
            "_subgroup_order",
            "sma_window",
            "_sma_filter_order",
            "date",
            "code",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)
    return sorted_df.drop(
        columns=[
            column
            for column in ["_regime_order", "_subgroup_order", "_sma_filter_order"]
            if column in sorted_df.columns
        ]
    )


def _build_sma_filter_summary_df(sma_trade_level_df: pd.DataFrame) -> pd.DataFrame:
    if sma_trade_level_df.empty:
        return _empty_sma_filter_summary_df()
    total_trade_df = sma_trade_level_df.loc[
        sma_trade_level_df["sma_filter_state"] == "all"
    ].copy()
    rows = _build_summary_rows(
        sma_trade_level_df,
        group_columns=[
            "interval_minutes",
            "signal_family",
            "exit_label",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
            "sma_window",
            "sma_label",
            "sma_filter_state",
            "sma_filter_label",
        ],
        total_group_columns=[
            "interval_minutes",
            "signal_family",
            "exit_label",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
            "sma_window",
            "sma_label",
        ],
        total_trade_df=total_trade_df,
    )
    return _sort_summary_frame(
        pd.DataFrame.from_records(rows, columns=_SMA_FILTER_SUMMARY_COLUMNS)
    )


def _build_sma_filter_comparison_df(
    sma_filter_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if sma_filter_summary_df.empty:
        return _empty_sma_filter_comparison_df()
    rows: list[dict[str, Any]] = []
    summary_df = sma_filter_summary_df.copy()
    key_columns = [
        "interval_minutes",
        "signal_family",
        "exit_label",
        "market_regime_bucket_key",
        "market_regime_bucket_label",
        "subgroup_key",
        "subgroup_label",
        "sma_window",
        "sma_label",
    ]
    for group_key, group_df in summary_df.groupby(key_columns, sort=True):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        key_map = dict(zip(key_columns, group_key, strict=True))
        by_state = {
            str(row["sma_filter_state"]): row
            for row in group_df.to_dict(orient="records")
        }
        all_row = by_state.get("all", {})
        above_row = by_state.get("above", {})
        below_row = by_state.get("at_or_below", {})
        rows.append(
            {
                **key_map,
                "branch_sample_count": all_row.get("sample_count"),
                "above_count": above_row.get("sample_count"),
                "at_or_below_count": below_row.get("sample_count"),
                "above_trade_return_mean": above_row.get("trade_return_mean"),
                "at_or_below_trade_return_mean": below_row.get("trade_return_mean"),
                "all_trade_return_mean": all_row.get("trade_return_mean"),
                "above_minus_at_or_below": _subtract_optional_float(
                    above_row.get("trade_return_mean"),
                    below_row.get("trade_return_mean"),
                ),
                "above_minus_all": _subtract_optional_float(
                    above_row.get("trade_return_mean"),
                    all_row.get("trade_return_mean"),
                ),
                "at_or_below_minus_all": _subtract_optional_float(
                    below_row.get("trade_return_mean"),
                    all_row.get("trade_return_mean"),
                ),
            }
        )
    return _sort_summary_frame(
        pd.DataFrame.from_records(rows, columns=_SMA_FILTER_COMPARISON_COLUMNS)
    )


def _build_period_sma_filter_summary_df(
    sma_trade_level_df: pd.DataFrame,
) -> pd.DataFrame:
    if sma_trade_level_df.empty:
        return _empty_period_sma_filter_summary_df()
    total_trade_df = sma_trade_level_df.loc[
        sma_trade_level_df["sma_filter_state"] == "all"
    ].copy()
    rows = _build_summary_rows(
        sma_trade_level_df,
        group_columns=[
            "interval_minutes",
            "signal_family",
            "exit_label",
            "period_index",
            "period_label",
            "period_start_date",
            "period_end_date",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
            "sma_window",
            "sma_label",
            "sma_filter_state",
            "sma_filter_label",
        ],
        total_group_columns=[
            "interval_minutes",
            "signal_family",
            "exit_label",
            "period_index",
            "period_label",
            "period_start_date",
            "period_end_date",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
            "sma_window",
            "sma_label",
        ],
        total_trade_df=total_trade_df,
    )
    return _sort_summary_frame(
        pd.DataFrame.from_records(rows, columns=_PERIOD_SMA_FILTER_SUMMARY_COLUMNS)
    )


def run_topix100_1445_entry_daily_sma_filter_comparison_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    signal_family: str = DEFAULT_SIGNAL_FAMILY,
    exit_label: str = DEFAULT_EXIT_LABEL,
    daily_sma_windows: Sequence[int] | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    period_months: int = DEFAULT_PERIOD_MONTHS,
    entry_time: str = DEFAULT_ENTRY_TIME,
    next_session_exit_time: str = DEFAULT_NEXT_SESSION_EXIT_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
) -> Topix1001445EntryDailySmaFilterComparisonResult:
    validated_interval_minutes = _normalize_interval_minutes(interval_minutes)
    validated_daily_sma_windows = _normalize_sma_windows(daily_sma_windows)

    base_result = run_topix100_1445_entry_signal_regime_comparison_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        interval_minutes_list=[validated_interval_minutes],
        bucket_count=bucket_count,
        period_months=period_months,
        entry_time=entry_time,
        next_session_exit_time=next_session_exit_time,
        tail_fraction=tail_fraction,
    )
    selected_trade_level_df = _build_selected_branch_df(
        base_result,
        interval_minutes=validated_interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
    )
    daily_sma_panel_df = _query_daily_sma_panel(
        db_path,
        end_date=base_result.analysis_end_date,
        daily_sma_windows=validated_daily_sma_windows,
    )
    sma_trade_level_df = _build_sma_trade_level_df(
        selected_trade_level_df,
        daily_sma_panel_df=daily_sma_panel_df,
        daily_sma_windows=validated_daily_sma_windows,
    )
    sma_filter_summary_df = _build_sma_filter_summary_df(sma_trade_level_df)
    sma_filter_comparison_df = _build_sma_filter_comparison_df(sma_filter_summary_df)
    period_sma_filter_summary_df = _build_period_sma_filter_summary_df(
        sma_trade_level_df
    )
    return Topix1001445EntryDailySmaFilterComparisonResult(
        db_path=db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        interval_minutes=validated_interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
        daily_sma_windows=validated_daily_sma_windows,
        bucket_count=bucket_count,
        period_months=period_months,
        entry_time=entry_time,
        next_session_exit_time=next_session_exit_time,
        tail_fraction=tail_fraction,
        topix100_constituent_count=base_result.topix100_constituent_count,
        selected_trade_count=int(len(selected_trade_level_df)),
        sma_trade_count=int(
            len(sma_trade_level_df.loc[sma_trade_level_df["sma_filter_state"] == "all"])
        ),
        periods_df=base_result.periods_df,
        selected_trade_level_df=selected_trade_level_df,
        sma_trade_level_df=sma_trade_level_df,
        sma_filter_summary_df=sma_filter_summary_df,
        sma_filter_comparison_df=sma_filter_comparison_df,
        period_sma_filter_summary_df=period_sma_filter_summary_df,
    )


def _split_result_payload(
    result: Topix1001445EntryDailySmaFilterComparisonResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    return (
        {
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "signal_family": result.signal_family,
            "exit_label": result.exit_label,
            "daily_sma_windows": list(result.daily_sma_windows),
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
            "entry_time": result.entry_time,
            "next_session_exit_time": result.next_session_exit_time,
            "tail_fraction": result.tail_fraction,
            "topix100_constituent_count": result.topix100_constituent_count,
            "selected_trade_count": result.selected_trade_count,
            "sma_trade_count": result.sma_trade_count,
        },
        {
            "periods_df": result.periods_df,
            "selected_trade_level_df": result.selected_trade_level_df,
            "sma_trade_level_df": result.sma_trade_level_df,
            "sma_filter_summary_df": result.sma_filter_summary_df,
            "sma_filter_comparison_df": result.sma_filter_comparison_df,
            "period_sma_filter_summary_df": result.period_sma_filter_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix1001445EntryDailySmaFilterComparisonResult:
    return Topix1001445EntryDailySmaFilterComparisonResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes=int(metadata["interval_minutes"]),
        signal_family=str(metadata["signal_family"]),
        exit_label=str(metadata["exit_label"]),
        daily_sma_windows=tuple(int(value) for value in metadata["daily_sma_windows"]),
        bucket_count=int(metadata["bucket_count"]),
        period_months=int(metadata["period_months"]),
        entry_time=str(metadata["entry_time"]),
        next_session_exit_time=str(metadata["next_session_exit_time"]),
        tail_fraction=float(metadata["tail_fraction"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        selected_trade_count=int(metadata["selected_trade_count"]),
        sma_trade_count=int(metadata["sma_trade_count"]),
        periods_df=tables["periods_df"],
        selected_trade_level_df=tables["selected_trade_level_df"],
        sma_trade_level_df=tables["sma_trade_level_df"],
        sma_filter_summary_df=tables["sma_filter_summary_df"],
        sma_filter_comparison_df=tables["sma_filter_comparison_df"],
        period_sma_filter_summary_df=tables["period_sma_filter_summary_df"],
    )


def _format_optional_pct(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(numeric):
        return "n/a"
    return f"{numeric * 100:+.4f}%"


def _branch_summary_lines(
    comparison_df: pd.DataFrame,
    *,
    regime_key: str,
    subgroup_key: str,
) -> list[str]:
    branch_df = comparison_df.loc[
        (comparison_df["market_regime_bucket_key"] == regime_key)
        & (comparison_df["subgroup_key"] == subgroup_key)
    ].copy()
    if branch_df.empty:
        return [f"- `{regime_key} / {subgroup_key}` branch had no rows."]
    lines: list[str] = []
    for row in branch_df.itertuples(index=False):
        lines.append(
            f"- `{regime_key} / {subgroup_key} / SMA{row.sma_window}` "
            f"all `{_format_optional_pct(row.all_trade_return_mean)}`, "
            f"above `{_format_optional_pct(row.above_trade_return_mean)}`, "
            f"at-or-below `{_format_optional_pct(row.at_or_below_trade_return_mean)}`."
        )
    return lines


def _build_research_bundle_summary_markdown(
    result: Topix1001445EntryDailySmaFilterComparisonResult,
) -> str:
    lines = [
        "# TOPIX100 14:45 Entry Daily SMA Filter Comparison",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{result.interval_minutes}`",
        f"- Signal family: `{result.signal_family}`",
        f"- Exit label: `{result.exit_label}`",
        f"- Daily SMA windows: `{', '.join(str(value) for value in result.daily_sma_windows)}`",
        f"- Entry time: `{result.entry_time}`",
        f"- Tail fraction per side: `{result.tail_fraction * 100:.1f}%`",
        f"- Base selected-trade rows: `{result.selected_trade_count}`",
        f"- SMA-eligible rows across windows: `{result.sma_trade_count}`",
        "",
        "## Weak Losers Focus",
        "",
        *_branch_summary_lines(
            result.sma_filter_comparison_df,
            regime_key="weak",
            subgroup_key="losers",
        ),
        "",
        "## Strong Winners Focus",
        "",
        *_branch_summary_lines(
            result.sma_filter_comparison_df,
            regime_key="strong",
            subgroup_key="winners",
        ),
        "",
        "## Caveat",
        "",
        "- Daily SMA filters use only prior daily closes (`shift(1)` before the rolling mean). The same-day 14:45 entry price is compared against a causal daily SMA baseline.",
        "- The underlying signal/regime study still uses current TOPIX100 membership and ex-post half-year signal buckets. Treat these SMA splits as conditional research on top of that baseline, not a live-ready threshold set.",
        "",
        "## Artifact Tables",
        "",
        "- `periods_df`",
        "- `selected_trade_level_df`",
        "- `sma_trade_level_df`",
        "- `sma_filter_summary_df`",
        "- `sma_filter_comparison_df`",
        "- `period_sma_filter_summary_df`",
    ]
    return "\n".join(lines)


def write_topix100_1445_entry_daily_sma_filter_comparison_research_bundle(
    result: Topix1001445EntryDailySmaFilterComparisonResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_COMPARISON_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_1445_entry_daily_sma_filter_comparison_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "signal_family": result.signal_family,
            "exit_label": result.exit_label,
            "daily_sma_windows": list(result.daily_sma_windows),
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
            "entry_time": result.entry_time,
            "next_session_exit_time": result.next_session_exit_time,
            "tail_fraction": result.tail_fraction,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=metadata,
        result_tables=tables,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_1445_entry_daily_sma_filter_comparison_research_bundle(
    bundle_path: str | Path,
) -> Topix1001445EntryDailySmaFilterComparisonResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_1445_entry_daily_sma_filter_comparison_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_COMPARISON_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_1445_entry_daily_sma_filter_comparison_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_COMPARISON_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
