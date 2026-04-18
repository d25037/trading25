"""
TOPIX100 14:45 entry daily-SMA branch portfolio research.

This converts a chosen branch from the daily-SMA filter comparison study into a
full-span equal-weight daily portfolio with idle days explicitly included as
zero-return sessions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix100_1445_entry_daily_sma_filter_comparison import (
    DEFAULT_EXIT_LABEL,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_SIGNAL_FAMILY,
    Topix1001445EntryDailySmaFilterComparisonResult,
    run_topix100_1445_entry_daily_sma_filter_comparison_research,
)
from src.domains.analytics.topix100_1445_entry_signal_regime_comparison import (
    DEFAULT_BUCKET_COUNT,
    DEFAULT_ENTRY_TIME,
    DEFAULT_NEXT_SESSION_EXIT_TIME,
    DEFAULT_PERIOD_MONTHS,
    DEFAULT_TAIL_FRACTION,
)
from src.domains.analytics.topix_return_standard_deviation_exposure_timing import (
    _build_drawdown_series,
    _compute_return_series_stats,
)

TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_PORTFOLIO_EXPERIMENT_ID = (
    "market-behavior/topix100-1445-entry-daily-sma-filter-portfolio"
)
DEFAULT_MARKET_REGIME_BUCKET_KEY = "weak"
DEFAULT_SUBGROUP_KEY = "losers"
DEFAULT_SMA_WINDOW = 50
DEFAULT_SMA_FILTER_STATE = "at_or_below"

_SERIES_NAME_ALL = "branch_all"
_SERIES_NAME_TARGET = "branch_target"
_SERIES_ORDER: tuple[str, ...] = (_SERIES_NAME_ALL, _SERIES_NAME_TARGET)
_PORTFOLIO_DAILY_COLUMNS: tuple[str, ...] = (
    "series_name",
    "series_label",
    "date",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "active_day",
    "active_trade_count",
    "active_name_count",
    "portfolio_return",
    "mean_trade_return",
    "median_trade_return",
    "best_trade_return",
    "worst_trade_return",
    "equity_curve",
    "drawdown",
)
_PORTFOLIO_STATS_COLUMNS: tuple[str, ...] = (
    "series_name",
    "series_label",
    "trading_day_count",
    "active_day_count",
    "idle_day_count",
    "active_day_ratio",
    "trade_count",
    "avg_trade_count_per_day",
    "avg_trade_count_per_active_day",
    "median_trade_count_per_active_day",
    "max_trade_count_per_day",
    "avg_return_on_active_days",
    "median_return_on_active_days",
    "day_count",
    "avg_daily_return",
    "median_daily_return",
    "daily_standard_deviation",
    "annualized_standard_deviation",
    "downside_standard_deviation",
    "sharpe_ratio",
    "sortino_ratio",
    "max_drawdown",
    "total_return",
    "cagr",
    "calmar_ratio",
    "positive_rate",
    "non_negative_rate",
    "best_day_return",
    "worst_day_return",
)
_PERIOD_PORTFOLIO_STATS_COLUMNS: tuple[str, ...] = (
    "series_name",
    "series_label",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    *(_PORTFOLIO_STATS_COLUMNS[2:]),
)
_PORTFOLIO_COMPARISON_COLUMNS: tuple[str, ...] = (
    "target_series_name",
    "baseline_series_name",
    "target_minus_baseline_total_return",
    "target_minus_baseline_cagr",
    "target_minus_baseline_sharpe_ratio",
    "target_minus_baseline_sortino_ratio",
    "target_minus_baseline_max_drawdown",
    "target_minus_baseline_active_day_ratio",
    "target_minus_baseline_avg_trade_count_per_active_day",
)


@dataclass(frozen=True)
class Topix1001445EntryDailySmaFilterPortfolioResult:
    db_path: str
    source_mode: str
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    signal_family: str
    exit_label: str
    market_regime_bucket_key: str
    market_regime_bucket_label: str
    subgroup_key: str
    subgroup_label: str
    expected_selected_bucket_label: str
    sma_window: int
    sma_filter_state: str
    sma_filter_label: str
    bucket_count: int
    period_months: int
    entry_time: str
    next_session_exit_time: str
    tail_fraction: float
    trading_day_count: int
    all_branch_trade_count: int
    target_branch_trade_count: int
    all_active_day_count: int
    target_active_day_count: int
    series_trade_level_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_stats_df: pd.DataFrame
    period_portfolio_stats_df: pd.DataFrame
    portfolio_comparison_df: pd.DataFrame


def _empty_series_trade_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["series_name", "series_label"])


def _empty_portfolio_daily_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PORTFOLIO_DAILY_COLUMNS))


def _empty_portfolio_stats_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PORTFOLIO_STATS_COLUMNS))


def _empty_period_portfolio_stats_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_PORTFOLIO_STATS_COLUMNS))


def _empty_portfolio_comparison_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PORTFOLIO_COMPARISON_COLUMNS))


def _sort_series_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    order_map = {key: index for index, key in enumerate(_SERIES_ORDER, start=1)}
    sorted_df = df.copy()
    sorted_df["_series_order"] = sorted_df["series_name"].map(order_map).fillna(999)
    sort_columns = [
        column
        for column in (
            "_series_order",
            "period_index",
            "date",
        )
        if column in sorted_df.columns
    ]
    sorted_df = sorted_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)
    return sorted_df.drop(columns=["_series_order"])


def _to_scalar_string(value: Any, *, fallback: str) -> str:
    if value is None:
        return fallback
    text = str(value)
    return text if text and text.lower() != "nan" else fallback


def _build_analysis_calendar_df(
    comparison_result: Topix1001445EntryDailySmaFilterComparisonResult,
    *,
    interval_minutes: int,
    signal_family: str,
    exit_label: str,
) -> pd.DataFrame:
    selected_df = comparison_result.selected_trade_level_df.loc[
        (pd.to_numeric(comparison_result.selected_trade_level_df["interval_minutes"], errors="coerce") == interval_minutes)
        & (comparison_result.selected_trade_level_df["signal_family"].astype(str) == signal_family)
        & (comparison_result.selected_trade_level_df["exit_label"].astype(str) == exit_label)
        & (comparison_result.selected_trade_level_df["subgroup_key"].astype(str) == "all")
    ].copy()
    if selected_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "period_index",
                "period_label",
                "period_start_date",
                "period_end_date",
            ]
        )
    calendar_df = (
        selected_df.loc[
            :,
            [
                "date",
                "period_index",
                "period_label",
                "period_start_date",
                "period_end_date",
            ],
        ]
        .drop_duplicates()
        .sort_values(["date"], kind="stable")
        .reset_index(drop=True)
    )
    return calendar_df


def _select_series_trade_df(
    comparison_result: Topix1001445EntryDailySmaFilterComparisonResult,
    *,
    interval_minutes: int,
    signal_family: str,
    exit_label: str,
    market_regime_bucket_key: str,
    subgroup_key: str,
    sma_window: int,
    sma_filter_state: str,
    series_name: str,
    series_label: str,
) -> pd.DataFrame:
    trade_df = comparison_result.sma_trade_level_df.loc[
        (pd.to_numeric(comparison_result.sma_trade_level_df["interval_minutes"], errors="coerce") == interval_minutes)
        & (comparison_result.sma_trade_level_df["signal_family"].astype(str) == signal_family)
        & (comparison_result.sma_trade_level_df["exit_label"].astype(str) == exit_label)
        & (comparison_result.sma_trade_level_df["market_regime_bucket_key"].astype(str) == market_regime_bucket_key)
        & (comparison_result.sma_trade_level_df["subgroup_key"].astype(str) == subgroup_key)
        & (pd.to_numeric(comparison_result.sma_trade_level_df["sma_window"], errors="coerce") == sma_window)
        & (comparison_result.sma_trade_level_df["sma_filter_state"].astype(str) == sma_filter_state)
    ].copy()
    if trade_df.empty:
        return _empty_series_trade_level_df()
    trade_df["series_name"] = series_name
    trade_df["series_label"] = series_label
    ordered_columns = ["series_name", "series_label", *trade_df.columns.drop(["series_name", "series_label"])]
    return trade_df.loc[:, ordered_columns].sort_values(["date", "code"], kind="stable").reset_index(drop=True)


def _build_single_series_portfolio_daily_df(
    *,
    calendar_df: pd.DataFrame,
    trade_df: pd.DataFrame,
    series_name: str,
    series_label: str,
) -> pd.DataFrame:
    if calendar_df.empty:
        return _empty_portfolio_daily_df()
    if trade_df.empty:
        result_df = calendar_df.copy()
        result_df["series_name"] = series_name
        result_df["series_label"] = series_label
        result_df["active_day"] = False
        result_df["active_trade_count"] = 0
        result_df["active_name_count"] = 0
        result_df["portfolio_return"] = 0.0
        result_df["mean_trade_return"] = pd.NA
        result_df["median_trade_return"] = pd.NA
        result_df["best_trade_return"] = pd.NA
        result_df["worst_trade_return"] = pd.NA
        result_df["equity_curve"] = (1.0 + result_df["portfolio_return"]).cumprod()
        result_df["drawdown"] = _build_drawdown_series(result_df["equity_curve"])
        return result_df.loc[:, list(_PORTFOLIO_DAILY_COLUMNS)].copy()

    aggregated_df = (
        trade_df.groupby("date", as_index=False)
        .agg(
            active_trade_count=("code", "size"),
            active_name_count=("code", "nunique"),
            mean_trade_return=("trade_return", "mean"),
            median_trade_return=("trade_return", "median"),
            best_trade_return=("trade_return", "max"),
            worst_trade_return=("trade_return", "min"),
        )
        .sort_values(["date"], kind="stable")
        .reset_index(drop=True)
    )
    aggregated_df["portfolio_return"] = aggregated_df["mean_trade_return"].astype(float)

    result_df = calendar_df.merge(
        aggregated_df,
        on="date",
        how="left",
        validate="one_to_one",
    )
    result_df["series_name"] = series_name
    result_df["series_label"] = series_label
    result_df["active_trade_count"] = (
        pd.to_numeric(result_df["active_trade_count"], errors="coerce").fillna(0).astype(int)
    )
    result_df["active_name_count"] = (
        pd.to_numeric(result_df["active_name_count"], errors="coerce").fillna(0).astype(int)
    )
    result_df["active_day"] = result_df["active_trade_count"] > 0
    result_df["portfolio_return"] = (
        pd.to_numeric(result_df["portfolio_return"], errors="coerce").fillna(0.0)
    )
    result_df["equity_curve"] = (1.0 + result_df["portfolio_return"]).cumprod()
    result_df["drawdown"] = _build_drawdown_series(result_df["equity_curve"])
    return result_df.loc[:, list(_PORTFOLIO_DAILY_COLUMNS)].copy()


def _build_portfolio_stats_df(portfolio_daily_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_daily_df.empty:
        return _empty_portfolio_stats_df()
    rows: list[dict[str, Any]] = []
    for (series_name, series_label), group_df in portfolio_daily_df.groupby(
        ["series_name", "series_label"], sort=False
    ):
        ordered_df = group_df.sort_values(["date"], kind="stable").reset_index(drop=True)
        return_series = pd.to_numeric(ordered_df["portfolio_return"], errors="coerce").fillna(0.0)
        active_mask = ordered_df["active_day"].astype(bool)
        active_trade_counts = pd.to_numeric(
            ordered_df.loc[active_mask, "active_trade_count"],
            errors="coerce",
        ).dropna()
        active_returns = pd.to_numeric(
            ordered_df.loc[active_mask, "portfolio_return"],
            errors="coerce",
        ).dropna()
        rows.append(
            {
                "series_name": str(series_name),
                "series_label": str(series_label),
                "trading_day_count": int(len(ordered_df)),
                "active_day_count": int(active_mask.sum()),
                "idle_day_count": int((~active_mask).sum()),
                "active_day_ratio": float(active_mask.mean()) if len(ordered_df) else float("nan"),
                "trade_count": int(
                    pd.to_numeric(ordered_df["active_trade_count"], errors="coerce").fillna(0).sum()
                ),
                "avg_trade_count_per_day": float(
                    pd.to_numeric(ordered_df["active_trade_count"], errors="coerce").fillna(0).mean()
                ),
                "avg_trade_count_per_active_day": (
                    float(active_trade_counts.mean()) if not active_trade_counts.empty else float("nan")
                ),
                "median_trade_count_per_active_day": (
                    float(active_trade_counts.median()) if not active_trade_counts.empty else float("nan")
                ),
                "max_trade_count_per_day": float(
                    pd.to_numeric(ordered_df["active_trade_count"], errors="coerce").fillna(0).max()
                ),
                "avg_return_on_active_days": (
                    float(active_returns.mean()) if not active_returns.empty else float("nan")
                ),
                "median_return_on_active_days": (
                    float(active_returns.median()) if not active_returns.empty else float("nan")
                ),
                **_compute_return_series_stats(return_series),
            }
        )
    return _sort_series_frame(
        pd.DataFrame.from_records(rows, columns=_PORTFOLIO_STATS_COLUMNS)
    )


def _build_period_portfolio_stats_df(portfolio_daily_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_daily_df.empty:
        return _empty_period_portfolio_stats_df()
    rows: list[dict[str, Any]] = []
    for group_key, group_df in portfolio_daily_df.groupby(
        [
            "series_name",
            "series_label",
            "period_index",
            "period_label",
            "period_start_date",
            "period_end_date",
        ],
        sort=False,
    ):
        (
            series_name,
            series_label,
            period_index,
            period_label,
            period_start_date,
            period_end_date,
        ) = group_key
        ordered_df = group_df.sort_values(["date"], kind="stable").reset_index(drop=True)
        return_series = pd.to_numeric(ordered_df["portfolio_return"], errors="coerce").fillna(0.0)
        active_mask = ordered_df["active_day"].astype(bool)
        active_trade_counts = pd.to_numeric(
            ordered_df.loc[active_mask, "active_trade_count"],
            errors="coerce",
        ).dropna()
        active_returns = pd.to_numeric(
            ordered_df.loc[active_mask, "portfolio_return"],
            errors="coerce",
        ).dropna()
        rows.append(
            {
                "series_name": str(series_name),
                "series_label": str(series_label),
                "period_index": int(period_index),
                "period_label": str(period_label),
                "period_start_date": _to_scalar_string(period_start_date, fallback=""),
                "period_end_date": _to_scalar_string(period_end_date, fallback=""),
                "trading_day_count": int(len(ordered_df)),
                "active_day_count": int(active_mask.sum()),
                "idle_day_count": int((~active_mask).sum()),
                "active_day_ratio": float(active_mask.mean()) if len(ordered_df) else float("nan"),
                "trade_count": int(
                    pd.to_numeric(ordered_df["active_trade_count"], errors="coerce").fillna(0).sum()
                ),
                "avg_trade_count_per_day": float(
                    pd.to_numeric(ordered_df["active_trade_count"], errors="coerce").fillna(0).mean()
                ),
                "avg_trade_count_per_active_day": (
                    float(active_trade_counts.mean()) if not active_trade_counts.empty else float("nan")
                ),
                "median_trade_count_per_active_day": (
                    float(active_trade_counts.median()) if not active_trade_counts.empty else float("nan")
                ),
                "max_trade_count_per_day": float(
                    pd.to_numeric(ordered_df["active_trade_count"], errors="coerce").fillna(0).max()
                ),
                "avg_return_on_active_days": (
                    float(active_returns.mean()) if not active_returns.empty else float("nan")
                ),
                "median_return_on_active_days": (
                    float(active_returns.median()) if not active_returns.empty else float("nan")
                ),
                **_compute_return_series_stats(return_series),
            }
        )
    return _sort_series_frame(
        pd.DataFrame.from_records(rows, columns=_PERIOD_PORTFOLIO_STATS_COLUMNS)
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


def _build_portfolio_comparison_df(portfolio_stats_df: pd.DataFrame) -> pd.DataFrame:
    if portfolio_stats_df.empty:
        return _empty_portfolio_comparison_df()
    by_series = {
        str(row["series_name"]): row for row in portfolio_stats_df.to_dict(orient="records")
    }
    baseline = by_series.get(_SERIES_NAME_ALL)
    target = by_series.get(_SERIES_NAME_TARGET)
    if baseline is None or target is None:
        return _empty_portfolio_comparison_df()
    return pd.DataFrame.from_records(
        [
            {
                "target_series_name": _SERIES_NAME_TARGET,
                "baseline_series_name": _SERIES_NAME_ALL,
                "target_minus_baseline_total_return": _subtract_optional_float(
                    target.get("total_return"),
                    baseline.get("total_return"),
                ),
                "target_minus_baseline_cagr": _subtract_optional_float(
                    target.get("cagr"),
                    baseline.get("cagr"),
                ),
                "target_minus_baseline_sharpe_ratio": _subtract_optional_float(
                    target.get("sharpe_ratio"),
                    baseline.get("sharpe_ratio"),
                ),
                "target_minus_baseline_sortino_ratio": _subtract_optional_float(
                    target.get("sortino_ratio"),
                    baseline.get("sortino_ratio"),
                ),
                "target_minus_baseline_max_drawdown": _subtract_optional_float(
                    target.get("max_drawdown"),
                    baseline.get("max_drawdown"),
                ),
                "target_minus_baseline_active_day_ratio": _subtract_optional_float(
                    target.get("active_day_ratio"),
                    baseline.get("active_day_ratio"),
                ),
                "target_minus_baseline_avg_trade_count_per_active_day": _subtract_optional_float(
                    target.get("avg_trade_count_per_active_day"),
                    baseline.get("avg_trade_count_per_active_day"),
                ),
            }
        ],
        columns=_PORTFOLIO_COMPARISON_COLUMNS,
    )


def _build_branch_series_label(
    *,
    market_regime_bucket_key: str,
    subgroup_key: str,
    expected_selected_bucket_label: str,
    sma_window: int,
    sma_filter_state: str,
    sma_filter_label: str,
) -> tuple[str, str]:
    branch_stub = (
        f"{market_regime_bucket_key}/{subgroup_key}/{expected_selected_bucket_label}"
    )
    all_label = f"{branch_stub} all"
    if sma_filter_state == "all":
        target_label = all_label
    else:
        target_label = f"{branch_stub} {sma_filter_label} SMA{sma_window}"
    return all_label, target_label


def run_topix100_1445_entry_daily_sma_filter_portfolio_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    signal_family: str = DEFAULT_SIGNAL_FAMILY,
    exit_label: str = DEFAULT_EXIT_LABEL,
    market_regime_bucket_key: str = DEFAULT_MARKET_REGIME_BUCKET_KEY,
    subgroup_key: str = DEFAULT_SUBGROUP_KEY,
    sma_window: int = DEFAULT_SMA_WINDOW,
    sma_filter_state: str = DEFAULT_SMA_FILTER_STATE,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    period_months: int = DEFAULT_PERIOD_MONTHS,
    entry_time: str = DEFAULT_ENTRY_TIME,
    next_session_exit_time: str = DEFAULT_NEXT_SESSION_EXIT_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
) -> Topix1001445EntryDailySmaFilterPortfolioResult:
    comparison_result = run_topix100_1445_entry_daily_sma_filter_comparison_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
        daily_sma_windows=[sma_window],
        bucket_count=bucket_count,
        period_months=period_months,
        entry_time=entry_time,
        next_session_exit_time=next_session_exit_time,
        tail_fraction=tail_fraction,
    )

    calendar_df = _build_analysis_calendar_df(
        comparison_result,
        interval_minutes=interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
    )
    all_trade_df = _select_series_trade_df(
        comparison_result,
        interval_minutes=interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
        market_regime_bucket_key=market_regime_bucket_key,
        subgroup_key=subgroup_key,
        sma_window=sma_window,
        sma_filter_state="all",
        series_name=_SERIES_NAME_ALL,
        series_label="",
    )
    target_trade_df = _select_series_trade_df(
        comparison_result,
        interval_minutes=interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
        market_regime_bucket_key=market_regime_bucket_key,
        subgroup_key=subgroup_key,
        sma_window=sma_window,
        sma_filter_state=sma_filter_state,
        series_name=_SERIES_NAME_TARGET,
        series_label="",
    )

    label_source_df = all_trade_df if not all_trade_df.empty else target_trade_df
    market_regime_bucket_label = _to_scalar_string(
        label_source_df["market_regime_bucket_label"].iloc[0]
        if not label_source_df.empty
        else market_regime_bucket_key,
        fallback=market_regime_bucket_key,
    )
    subgroup_label = _to_scalar_string(
        label_source_df["subgroup_label"].iloc[0] if not label_source_df.empty else subgroup_key,
        fallback=subgroup_key,
    )
    expected_selected_bucket_label = _to_scalar_string(
        label_source_df["expected_selected_bucket_label"].iloc[0]
        if not label_source_df.empty
        else "n/a",
        fallback="n/a",
    )
    resolved_sma_filter_label = _to_scalar_string(
        target_trade_df["sma_filter_label"].iloc[0]
        if not target_trade_df.empty
        else sma_filter_state.replace("_", " "),
        fallback=sma_filter_state.replace("_", " "),
    )
    all_series_label, target_series_label = _build_branch_series_label(
        market_regime_bucket_key=market_regime_bucket_key,
        subgroup_key=subgroup_key,
        expected_selected_bucket_label=expected_selected_bucket_label,
        sma_window=sma_window,
        sma_filter_state=sma_filter_state,
        sma_filter_label=resolved_sma_filter_label,
    )
    if not all_trade_df.empty:
        all_trade_df["series_label"] = all_series_label
    if not target_trade_df.empty:
        target_trade_df["series_label"] = target_series_label

    all_daily_df = _build_single_series_portfolio_daily_df(
        calendar_df=calendar_df,
        trade_df=all_trade_df,
        series_name=_SERIES_NAME_ALL,
        series_label=all_series_label,
    )
    target_daily_df = _build_single_series_portfolio_daily_df(
        calendar_df=calendar_df,
        trade_df=target_trade_df,
        series_name=_SERIES_NAME_TARGET,
        series_label=target_series_label,
    )
    series_trade_level_df = _sort_series_frame(
        pd.concat([all_trade_df, target_trade_df], ignore_index=True)
        if (not all_trade_df.empty or not target_trade_df.empty)
        else _empty_series_trade_level_df()
    )
    portfolio_daily_df = _sort_series_frame(
        pd.concat([all_daily_df, target_daily_df], ignore_index=True)
    )
    portfolio_stats_df = _build_portfolio_stats_df(portfolio_daily_df)
    period_portfolio_stats_df = _build_period_portfolio_stats_df(portfolio_daily_df)
    portfolio_comparison_df = _build_portfolio_comparison_df(portfolio_stats_df)
    stats_lookup = {
        str(row["series_name"]): row for row in portfolio_stats_df.to_dict(orient="records")
    }

    return Topix1001445EntryDailySmaFilterPortfolioResult(
        db_path=db_path,
        source_mode=str(comparison_result.source_mode),
        source_detail=str(comparison_result.source_detail),
        available_start_date=comparison_result.available_start_date,
        available_end_date=comparison_result.available_end_date,
        analysis_start_date=comparison_result.analysis_start_date,
        analysis_end_date=comparison_result.analysis_end_date,
        interval_minutes=interval_minutes,
        signal_family=signal_family,
        exit_label=exit_label,
        market_regime_bucket_key=market_regime_bucket_key,
        market_regime_bucket_label=market_regime_bucket_label,
        subgroup_key=subgroup_key,
        subgroup_label=subgroup_label,
        expected_selected_bucket_label=expected_selected_bucket_label,
        sma_window=sma_window,
        sma_filter_state=sma_filter_state,
        sma_filter_label=resolved_sma_filter_label,
        bucket_count=bucket_count,
        period_months=period_months,
        entry_time=entry_time,
        next_session_exit_time=next_session_exit_time,
        tail_fraction=tail_fraction,
        trading_day_count=int(len(calendar_df)),
        all_branch_trade_count=int(len(all_trade_df)),
        target_branch_trade_count=int(len(target_trade_df)),
        all_active_day_count=int(stats_lookup.get(_SERIES_NAME_ALL, {}).get("active_day_count", 0)),
        target_active_day_count=int(stats_lookup.get(_SERIES_NAME_TARGET, {}).get("active_day_count", 0)),
        series_trade_level_df=series_trade_level_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_stats_df=portfolio_stats_df,
        period_portfolio_stats_df=period_portfolio_stats_df,
        portfolio_comparison_df=portfolio_comparison_df,
    )


def _split_result_payload(
    result: Topix1001445EntryDailySmaFilterPortfolioResult,
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
            "market_regime_bucket_key": result.market_regime_bucket_key,
            "market_regime_bucket_label": result.market_regime_bucket_label,
            "subgroup_key": result.subgroup_key,
            "subgroup_label": result.subgroup_label,
            "expected_selected_bucket_label": result.expected_selected_bucket_label,
            "sma_window": result.sma_window,
            "sma_filter_state": result.sma_filter_state,
            "sma_filter_label": result.sma_filter_label,
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
            "entry_time": result.entry_time,
            "next_session_exit_time": result.next_session_exit_time,
            "tail_fraction": result.tail_fraction,
            "trading_day_count": result.trading_day_count,
            "all_branch_trade_count": result.all_branch_trade_count,
            "target_branch_trade_count": result.target_branch_trade_count,
            "all_active_day_count": result.all_active_day_count,
            "target_active_day_count": result.target_active_day_count,
        },
        {
            "series_trade_level_df": result.series_trade_level_df,
            "portfolio_daily_df": result.portfolio_daily_df,
            "portfolio_stats_df": result.portfolio_stats_df,
            "period_portfolio_stats_df": result.period_portfolio_stats_df,
            "portfolio_comparison_df": result.portfolio_comparison_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix1001445EntryDailySmaFilterPortfolioResult:
    return Topix1001445EntryDailySmaFilterPortfolioResult(
        db_path=str(metadata["db_path"]),
        source_mode=str(metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        interval_minutes=int(metadata["interval_minutes"]),
        signal_family=str(metadata["signal_family"]),
        exit_label=str(metadata["exit_label"]),
        market_regime_bucket_key=str(metadata["market_regime_bucket_key"]),
        market_regime_bucket_label=str(metadata["market_regime_bucket_label"]),
        subgroup_key=str(metadata["subgroup_key"]),
        subgroup_label=str(metadata["subgroup_label"]),
        expected_selected_bucket_label=str(metadata["expected_selected_bucket_label"]),
        sma_window=int(metadata["sma_window"]),
        sma_filter_state=str(metadata["sma_filter_state"]),
        sma_filter_label=str(metadata["sma_filter_label"]),
        bucket_count=int(metadata["bucket_count"]),
        period_months=int(metadata["period_months"]),
        entry_time=str(metadata["entry_time"]),
        next_session_exit_time=str(metadata["next_session_exit_time"]),
        tail_fraction=float(metadata["tail_fraction"]),
        trading_day_count=int(metadata["trading_day_count"]),
        all_branch_trade_count=int(metadata["all_branch_trade_count"]),
        target_branch_trade_count=int(metadata["target_branch_trade_count"]),
        all_active_day_count=int(metadata["all_active_day_count"]),
        target_active_day_count=int(metadata["target_active_day_count"]),
        series_trade_level_df=tables["series_trade_level_df"],
        portfolio_daily_df=tables["portfolio_daily_df"],
        portfolio_stats_df=tables["portfolio_stats_df"],
        period_portfolio_stats_df=tables["period_portfolio_stats_df"],
        portfolio_comparison_df=tables["portfolio_comparison_df"],
    )


def _format_optional_percent(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(numeric):
        return "n/a"
    return f"{numeric * 100:+.2f}%"


def _format_optional_ratio(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(numeric):
        return "n/a"
    return f"{numeric:.2f}"


def _build_research_bundle_summary_markdown(
    result: Topix1001445EntryDailySmaFilterPortfolioResult,
) -> str:
    stats_lookup = {
        str(row["series_name"]): row for row in result.portfolio_stats_df.to_dict(orient="records")
    }
    baseline = stats_lookup.get(_SERIES_NAME_ALL, {})
    target = stats_lookup.get(_SERIES_NAME_TARGET, {})
    comparison = (
        result.portfolio_comparison_df.iloc[0].to_dict()
        if not result.portfolio_comparison_df.empty
        else {}
    )
    lines = [
        "# TOPIX100 14:45 Daily SMA Filter Portfolio",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Branch: `{result.market_regime_bucket_key} / {result.subgroup_key} / {result.expected_selected_bucket_label}`",
        f"- Target filter: `{result.sma_filter_label} SMA{result.sma_window}`",
        f"- Interval / signal / exit: `{result.interval_minutes}m / {result.signal_family} / {result.exit_label}`",
        f"- Trading days: `{result.trading_day_count}`",
        "",
        "## Portfolio Metrics",
        "",
        f"- All branch total return `{_format_optional_percent(baseline.get('total_return'))}`, CAGR `{_format_optional_percent(baseline.get('cagr'))}`, Sharpe `{_format_optional_ratio(baseline.get('sharpe_ratio'))}`, max DD `{_format_optional_percent(baseline.get('max_drawdown'))}`, active days `{baseline.get('active_day_count', 'n/a')}`.",
        f"- Target filter total return `{_format_optional_percent(target.get('total_return'))}`, CAGR `{_format_optional_percent(target.get('cagr'))}`, Sharpe `{_format_optional_ratio(target.get('sharpe_ratio'))}`, max DD `{_format_optional_percent(target.get('max_drawdown'))}`, active days `{target.get('active_day_count', 'n/a')}`.",
        "",
        "## Target Minus All",
        "",
        f"- Total return gap `{_format_optional_percent(comparison.get('target_minus_baseline_total_return'))}`",
        f"- CAGR gap `{_format_optional_percent(comparison.get('target_minus_baseline_cagr'))}`",
        f"- Sharpe gap `{_format_optional_ratio(comparison.get('target_minus_baseline_sharpe_ratio'))}`",
        f"- Max DD gap `{_format_optional_percent(comparison.get('target_minus_baseline_max_drawdown'))}`",
        "",
        "## Caveat",
        "",
        "- Portfolio daily returns are equal-weight averages across the selected names on each entry date, with idle days included as `0%` returns.",
        "- The branch itself inherits the same descriptive caveat as the source study: current TOPIX100 membership and ex-post half-year signal buckets remain research-only scaffolding.",
        "",
        "## Artifact Tables",
        "",
        "- `series_trade_level_df`",
        "- `portfolio_daily_df`",
        "- `portfolio_stats_df`",
        "- `period_portfolio_stats_df`",
        "- `portfolio_comparison_df`",
    ]
    return "\n".join(lines)


def write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle(
    result: Topix1001445EntryDailySmaFilterPortfolioResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_PORTFOLIO_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_1445_entry_daily_sma_filter_portfolio_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "signal_family": result.signal_family,
            "exit_label": result.exit_label,
            "market_regime_bucket_key": result.market_regime_bucket_key,
            "subgroup_key": result.subgroup_key,
            "sma_window": result.sma_window,
            "sma_filter_state": result.sma_filter_state,
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


def load_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle(
    bundle_path: str | Path,
) -> Topix1001445EntryDailySmaFilterPortfolioResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_1445_entry_daily_sma_filter_portfolio_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_PORTFOLIO_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_1445_entry_daily_sma_filter_portfolio_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_PORTFOLIO_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
