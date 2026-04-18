"""
TOPIX100 14:45 entry signal/regime comparison research.

This study compares two stock-level opening-volume signals under a same-day
14:45 entry workflow:

- previous_open_vs_open: current opening volume / previous-session opening volume
- previous_close_vs_open: current opening volume / previous-session closing volume

The working hypotheses are:

- previous_open_vs_open -> long Q1 (low-ratio names)
- previous_close_vs_open -> long Q4 (high-ratio names)

At the same time, the study brings back the original same-day TOPIX100
cross-sectional classification at the 14:45 snapshot and conditions the result
on market-strength buckets measured from the cross-sectional mean return from
previous close to 14:45.

The signal itself is causal, but the half-year Q-buckets and whole-sample
market regime buckets remain descriptive ex-post groupings for research.
"""

from __future__ import annotations

import math
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
from src.domains.analytics.topix100_open_relative_intraday_path import (
    DEFAULT_INTERVAL_MINUTES,
    SourceMode,
    _date_filter_sql,
    _fetch_available_date_range,
    _fetch_topix100_constituent_count,
    _normalize_code_sql,
    _open_analysis_connection,
    _topix100_stocks_cte,
)

TOPIX100_1445_ENTRY_SIGNAL_REGIME_COMPARISON_EXPERIMENT_ID = (
    "market-behavior/topix100-1445-entry-signal-regime-comparison"
)
DEFAULT_BUCKET_COUNT = 4
DEFAULT_PERIOD_MONTHS = 6
DEFAULT_ENTRY_TIME = "14:45"
DEFAULT_NEXT_SESSION_EXIT_TIME = "10:30"
DEFAULT_TAIL_FRACTION = 0.10

_PERIOD_COLUMNS: tuple[str, ...] = (
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
)
_MARKET_REGIME_COLUMNS: tuple[str, ...] = (
    "entry_date",
    "market_regime_return",
    "sample_count",
    "stock_count",
    "market_regime_rank",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
)
_BASE_SESSION_COLUMNS: tuple[str, ...] = (
    "date",
    "code",
    "previous_session_date",
    "next_session_date",
    "previous_close",
    "entry_price",
    "entry_actual_time",
    "same_day_close_price",
    "same_day_close_time",
    "next_session_open_price",
    "next_session_open_time",
    "next_session_1030_price",
    "next_session_1030_time",
    "prev_close_to_entry_return",
    "entry_to_close_return",
    "close_to_next_open_return",
    "next_open_to_next_1030_return",
    "entry_to_next_open_return",
    "entry_to_next_1030_return",
    "entry_split_rank",
    "entry_split_group",
    "entry_split_session_count",
    "current_entry_bucket_key",
    "current_entry_bucket_label",
    "market_regime_return",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
)
_SIGNAL_SESSION_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "signal_label",
    "target_bucket_side",
    "expected_selected_bucket_label",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "date",
    "code",
    "previous_session_date",
    "next_session_date",
    "previous_opening_volume",
    "previous_closing_volume",
    "opening_volume",
    "signal_ratio",
    "signal_bucket_index",
    "signal_bucket_label",
    "signal_selected",
    "previous_close",
    "entry_price",
    "entry_actual_time",
    "next_session_open_price",
    "next_session_open_time",
    "next_session_1030_price",
    "next_session_1030_time",
    "prev_close_to_entry_return",
    "entry_to_next_open_return",
    "entry_to_next_1030_return",
    "current_entry_bucket_key",
    "current_entry_bucket_label",
    "market_regime_return",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
)
_SELECTED_TRADE_LEVEL_COLUMNS: tuple[str, ...] = (
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
_MARKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "exit_label",
    "exit_time_target",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "subgroup_key",
    "subgroup_label",
    "sample_count",
    "sample_share",
    "date_count",
    "stock_count",
    "trade_return_mean",
    "trade_return_median",
    "trade_return_sum",
    "hit_positive",
)
_SIGNAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "signal_label",
    "target_bucket_side",
    "expected_selected_bucket_label",
    "exit_label",
    "exit_time_target",
    "subgroup_key",
    "subgroup_label",
    "sample_count",
    "sample_share",
    "date_count",
    "stock_count",
    "trade_return_mean",
    "trade_return_median",
    "trade_return_sum",
    "hit_positive",
)
_INTERSECTION_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "signal_label",
    "target_bucket_side",
    "expected_selected_bucket_label",
    "exit_label",
    "exit_time_target",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "subgroup_key",
    "subgroup_label",
    "sample_count",
    "sample_share",
    "date_count",
    "stock_count",
    "trade_return_mean",
    "trade_return_median",
    "trade_return_sum",
    "hit_positive",
)
_PERIOD_INTERSECTION_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "signal_family",
    "signal_label",
    "target_bucket_side",
    "expected_selected_bucket_label",
    "exit_label",
    "exit_time_target",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "subgroup_key",
    "subgroup_label",
    "sample_count",
    "sample_share",
    "date_count",
    "stock_count",
    "trade_return_mean",
    "trade_return_median",
    "trade_return_sum",
    "hit_positive",
)

_SIGNAL_FAMILY_DEFS: tuple[dict[str, str], ...] = (
    {
        "signal_family": "previous_open_vs_open",
        "signal_label": "Previous open vs current open",
        "target_bucket_side": "low",
        "expected_selected_bucket_label": "Q1",
        "reference_column": "previous_opening_volume",
    },
    {
        "signal_family": "previous_close_vs_open",
        "signal_label": "Previous close vs current open",
        "target_bucket_side": "high",
        "expected_selected_bucket_label": "Q4",
        "reference_column": "previous_closing_volume",
    },
)


@dataclass(frozen=True)
class Topix1001445EntrySignalRegimeComparisonResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes_list: tuple[int, ...]
    bucket_count: int
    period_months: int
    entry_time: str
    next_session_exit_time: str
    tail_fraction: float
    topix100_constituent_count: int
    total_session_count: int
    regime_day_count: int
    selected_signal_session_count: int
    periods_df: pd.DataFrame
    market_regime_df: pd.DataFrame
    base_session_df: pd.DataFrame
    signal_session_df: pd.DataFrame
    selected_trade_level_df: pd.DataFrame
    market_summary_df: pd.DataFrame
    signal_summary_df: pd.DataFrame
    intersection_summary_df: pd.DataFrame
    period_intersection_summary_df: pd.DataFrame


def _empty_periods_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_COLUMNS))


def _empty_market_regime_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_MARKET_REGIME_COLUMNS))


def _empty_base_session_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_BASE_SESSION_COLUMNS))


def _empty_signal_session_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SIGNAL_SESSION_COLUMNS))


def _empty_selected_trade_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SELECTED_TRADE_LEVEL_COLUMNS))


def _empty_market_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_MARKET_SUMMARY_COLUMNS))


def _empty_signal_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SIGNAL_SUMMARY_COLUMNS))


def _empty_intersection_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_INTERSECTION_SUMMARY_COLUMNS))


def _empty_period_intersection_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_INTERSECTION_SUMMARY_COLUMNS))


def _normalize_interval_minutes(
    values: Sequence[int] | None,
) -> tuple[int, ...]:
    if values is None:
        values = DEFAULT_INTERVAL_MINUTES
    normalized = tuple(
        sorted(dict.fromkeys(int(value) for value in values if int(value) > 0))
    )
    if not normalized:
        raise ValueError("interval_minutes_list must contain at least one positive integer")
    return normalized


def _validate_bucket_count(value: int) -> int:
    bucket_count = int(value)
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    return bucket_count


def _validate_period_months(value: int) -> int:
    period_months = int(value)
    if period_months <= 0:
        raise ValueError("period_months must be positive")
    return period_months


def _validate_tail_fraction(value: float) -> float:
    tail_fraction = float(value)
    if tail_fraction <= 0 or tail_fraction > 0.5:
        raise ValueError("tail_fraction must be within (0, 0.5].")
    return tail_fraction


def _validate_time_label(value: str, *, argument_name: str) -> str:
    normalized = str(value).strip()
    if len(normalized) != 5 or normalized[2] != ":":
        raise ValueError(f"{argument_name} must be formatted as HH:MM")
    return normalized


def _parse_time_to_minute(value: str) -> int:
    return int(value[:2]) * 60 + int(value[3:])


def _resolve_analysis_range(
    *,
    available_start_date: str | None,
    available_end_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, str]:
    if available_start_date is None or available_end_date is None:
        raise ValueError("No TOPIX100 minute bars were available for analysis.")

    resolved_start_date = (
        max(available_start_date, start_date)
        if start_date is not None
        else available_start_date
    )
    resolved_end_date = (
        min(available_end_date, end_date) if end_date is not None else available_end_date
    )
    if resolved_start_date > resolved_end_date:
        raise ValueError(
            "The selected date range does not overlap the available TOPIX100 minute bars."
        )
    return resolved_start_date, resolved_end_date


def _build_periods(
    *,
    start_date: str,
    end_date: str,
    period_months: int,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if start_ts > end_ts:
        raise ValueError("start_date must be before or equal to end_date")

    rows: list[dict[str, Any]] = []
    current_start = start_ts
    period_index = 1
    while current_start <= end_ts:
        next_start = current_start + pd.DateOffset(months=period_months)
        period_end = min(end_ts, next_start - pd.Timedelta(days=1))
        rows.append(
            {
                "period_index": period_index,
                "period_label": (
                    f"P{period_index} ({current_start.date()} to {period_end.date()})"
                ),
                "period_start_date": str(current_start.date()),
                "period_end_date": str(period_end.date()),
            }
        )
        current_start = next_start
        period_index += 1
    return pd.DataFrame.from_records(rows, columns=_PERIOD_COLUMNS)


def _coerce_optional_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _format_optional_pct(value: Any) -> str:
    numeric = _coerce_optional_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric * 100:+.4f}%"


def _assign_quantile_bucket(series: pd.Series, *, bucket_count: int) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    rank_pct = valid.rank(method="first", pct=True)
    bucket = (rank_pct * bucket_count).apply(np.ceil).clip(1, bucket_count)
    return cast(pd.Series, bucket.astype(int))


def _build_current_entry_bucket_label_map(
    tail_fraction: float,
    *,
    entry_time: str,
) -> dict[str, str]:
    tail_pct = int(round(tail_fraction * 100))
    middle_pct = max(0, 100 - (tail_pct * 2))
    return {
        "winners": f"Current {entry_time} top {tail_pct}%",
        "middle": f"Current {entry_time} middle {middle_pct}%",
        "losers": f"Current {entry_time} bottom {tail_pct}%",
    }


def _subgroup_label_map(
    tail_fraction: float,
    *,
    entry_time: str,
) -> dict[str, str]:
    mapping = _build_current_entry_bucket_label_map(
        tail_fraction,
        entry_time=entry_time,
    )
    mapping["all"] = "All"
    return mapping


def _query_base_session_df_from_connection(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    entry_time: str,
    next_session_exit_time: str,
) -> pd.DataFrame:
    entry_minute = _parse_time_to_minute(entry_time)
    next_session_exit_minute = _parse_time_to_minute(next_session_exit_time)
    date_filter_sql, date_params = _date_filter_sql(
        column_name="m.date",
        start_date=start_date,
        end_date=end_date,
    )
    params: list[Any] = [
        *date_params,
        entry_minute,
        entry_minute,
        next_session_exit_minute,
        next_session_exit_minute,
    ]
    base_session_df = cast(
        pd.DataFrame,
        conn.execute(
            f"""
            WITH
            {_topix100_stocks_cte()},
            minute_rows AS (
                SELECT
                    m.date,
                    {_normalize_code_sql('m.code')} AS code,
                    m.time,
                    CAST(substr(m.time, 1, 2) AS INTEGER) * 60
                        + CAST(substr(m.time, 4, 2) AS INTEGER) AS minute_of_day,
                    m.open,
                    m.close
                FROM stock_data_minute_raw m
                JOIN topix100_stocks s
                  ON s.normalized_code = {_normalize_code_sql('m.code')}
                WHERE m.time IS NOT NULL
                  AND m.open IS NOT NULL
                  AND m.close IS NOT NULL
                  AND m.open > 0
                  {date_filter_sql}
            ),
            daily AS (
                SELECT
                    date,
                    code,
                    arg_min(open, minute_of_day) AS day_open,
                    arg_min(time, minute_of_day) AS day_open_time,
                    arg_min(open, minute_of_day) FILTER (
                        WHERE minute_of_day >= ?
                    ) AS entry_price,
                    arg_min(time, minute_of_day) FILTER (
                        WHERE minute_of_day >= ?
                    ) AS entry_actual_time,
                    arg_max(close, minute_of_day) FILTER (
                        WHERE minute_of_day <= ?
                    ) AS exit_1030_price,
                    arg_max(time, minute_of_day) FILTER (
                        WHERE minute_of_day <= ?
                    ) AS exit_1030_actual_time,
                    arg_max(close, minute_of_day) AS same_day_close_price,
                    arg_max(time, minute_of_day) AS same_day_close_time
                FROM minute_rows
                GROUP BY date, code
            ),
            ordered AS (
                SELECT
                    date,
                    code,
                    lag(date) OVER (PARTITION BY code ORDER BY date) AS previous_session_date,
                    lag(same_day_close_price) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS previous_close,
                    entry_price,
                    entry_actual_time,
                    same_day_close_price,
                    same_day_close_time,
                    lead(date) OVER (PARTITION BY code ORDER BY date) AS next_session_date,
                    lead(day_open) OVER (PARTITION BY code ORDER BY date) AS next_session_open_price,
                    lead(day_open_time) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS next_session_open_time,
                    lead(exit_1030_price) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS next_session_1030_price,
                    lead(exit_1030_actual_time) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS next_session_1030_time
                FROM daily
                WHERE entry_price IS NOT NULL
                  AND entry_price > 0
            )
            SELECT
                date,
                code,
                previous_session_date,
                next_session_date,
                previous_close,
                entry_price,
                entry_actual_time,
                same_day_close_price,
                same_day_close_time,
                next_session_open_price,
                next_session_open_time,
                next_session_1030_price,
                next_session_1030_time,
                entry_price / NULLIF(previous_close, 0) - 1.0 AS prev_close_to_entry_return,
                same_day_close_price / NULLIF(entry_price, 0) - 1.0 AS entry_to_close_return,
                next_session_open_price / NULLIF(same_day_close_price, 0) - 1.0 AS close_to_next_open_return,
                next_session_1030_price / NULLIF(next_session_open_price, 0) - 1.0 AS next_open_to_next_1030_return,
                next_session_open_price / NULLIF(entry_price, 0) - 1.0 AS entry_to_next_open_return,
                next_session_1030_price / NULLIF(entry_price, 0) - 1.0 AS entry_to_next_1030_return
            FROM ordered
            ORDER BY date, code
            """,
            params,
        ).fetchdf(),
    )
    if base_session_df.empty:
        return _empty_base_session_df()

    base_session_df = base_session_df.copy()
    base_session_df["entry_split_rank"] = pd.NA
    base_session_df["entry_split_group"] = pd.NA
    base_session_df["entry_split_session_count"] = pd.NA
    base_session_df["current_entry_bucket_key"] = pd.NA
    base_session_df["current_entry_bucket_label"] = pd.NA
    base_session_df["market_regime_return"] = pd.NA
    base_session_df["market_regime_bucket_key"] = pd.NA
    base_session_df["market_regime_bucket_label"] = pd.NA
    return base_session_df.loc[:, list(_BASE_SESSION_COLUMNS)].copy()


def _query_interval_signal_volumes_df_from_connection(
    conn: Any,
    *,
    interval_minutes: int,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    date_filter_sql, date_params = _date_filter_sql(
        column_name="m.date",
        start_date=start_date,
        end_date=end_date,
    )
    params: list[Any] = [
        *date_params,
        interval_minutes,
        interval_minutes,
    ]
    return cast(
        pd.DataFrame,
        conn.execute(
            f"""
            WITH
            {_topix100_stocks_cte()},
            minute_rows AS (
                SELECT
                    m.date,
                    {_normalize_code_sql('m.code')} AS code,
                    CAST(substr(m.time, 1, 2) AS INTEGER) * 60
                        + CAST(substr(m.time, 4, 2) AS INTEGER) AS minute_of_day,
                    m.volume
                FROM stock_data_minute_raw m
                JOIN topix100_stocks s
                  ON s.normalized_code = {_normalize_code_sql('m.code')}
                WHERE m.time IS NOT NULL
                  AND m.volume IS NOT NULL
                  {date_filter_sql}
            ),
            session_bounds AS (
                SELECT
                    date,
                    code,
                    min(minute_of_day) AS first_minute,
                    max(minute_of_day) AS last_minute
                FROM minute_rows
                GROUP BY date, code
            ),
            daily AS (
                SELECT
                    m.date,
                    m.code,
                    sum(m.volume) FILTER (
                        WHERE m.minute_of_day >= b.first_minute
                          AND m.minute_of_day < b.first_minute + ?
                    ) AS opening_volume,
                    sum(m.volume) FILTER (
                        WHERE m.minute_of_day > b.last_minute - ?
                          AND m.minute_of_day <= b.last_minute
                    ) AS closing_volume
                FROM minute_rows m
                JOIN session_bounds b USING (date, code)
                GROUP BY m.date, m.code
            ),
            ordered AS (
                SELECT
                    date,
                    code,
                    lag(date) OVER (PARTITION BY code ORDER BY date) AS previous_session_date,
                    lag(opening_volume) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS previous_opening_volume,
                    lag(closing_volume) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS previous_closing_volume,
                    opening_volume
                FROM daily
                WHERE opening_volume IS NOT NULL
                  AND opening_volume > 0
            )
            SELECT
                date,
                code,
                previous_session_date,
                previous_opening_volume,
                previous_closing_volume,
                opening_volume
            FROM ordered
            ORDER BY date, code
            """,
            params,
        ).fetchdf(),
    )


def _assign_rank_groups(
    base_session_df: pd.DataFrame,
    *,
    tail_fraction: float,
    entry_time: str,
) -> pd.DataFrame:
    if base_session_df.empty:
        return _empty_base_session_df()

    result_df = base_session_df.copy()
    metric_mask = result_df["prev_close_to_entry_return"].notna()
    if not bool(metric_mask.any()):
        return result_df.loc[:, list(_BASE_SESSION_COLUMNS)].copy()

    ranked_df = result_df.loc[
        metric_mask,
        ["date", "code", "prev_close_to_entry_return"],
    ].copy()
    ranked_df = ranked_df.sort_values(
        ["date", "prev_close_to_entry_return", "code"],
        ascending=[True, False, True],
        kind="stable",
    )
    ranked_df["entry_split_rank"] = ranked_df.groupby("date").cumcount() + 1
    ranked_df["entry_split_session_count"] = ranked_df.groupby("date")["code"].transform(
        "size"
    )
    ranked_df["tail_count"] = ranked_df["entry_split_session_count"].map(
        lambda value: max(
            1,
            min(
                int(value) // 2,
                int(math.floor(int(value) * tail_fraction)),
            ),
        )
    )
    ranked_df["entry_split_group"] = pd.Series(
        pd.NA,
        index=ranked_df.index,
        dtype="string",
    )
    ranked_df.loc[
        ranked_df["entry_split_rank"] <= ranked_df["tail_count"],
        "entry_split_group",
    ] = "winners"
    ranked_df.loc[
        ranked_df["entry_split_rank"]
        > (ranked_df["entry_split_session_count"] - ranked_df["tail_count"]),
        "entry_split_group",
    ] = "losers"
    ranked_df = ranked_df.loc[
        :,
        ["date", "code", "entry_split_rank", "entry_split_group", "entry_split_session_count"],
    ]
    result_df = result_df.drop(
        columns=["entry_split_rank", "entry_split_group", "entry_split_session_count"],
        errors="ignore",
    ).merge(ranked_df, how="left", on=["date", "code"])
    result_df["current_entry_bucket_key"] = (
        result_df["entry_split_group"].fillna("middle").astype(str)
    )
    label_map = _build_current_entry_bucket_label_map(
        tail_fraction,
        entry_time=entry_time,
    )
    result_df["current_entry_bucket_label"] = result_df["current_entry_bucket_key"].map(
        label_map
    )
    return result_df.loc[:, list(_BASE_SESSION_COLUMNS)].copy()


def _assign_market_regime_buckets(base_session_df: pd.DataFrame) -> pd.DataFrame:
    eligible_df = base_session_df.loc[
        base_session_df["prev_close_to_entry_return"].notna()
    ].copy()
    if eligible_df.empty:
        return _empty_market_regime_df()

    regime_market_df = (
        eligible_df.groupby("date", as_index=False)
        .agg(
            market_regime_return=("prev_close_to_entry_return", "mean"),
            sample_count=("code", "size"),
            stock_count=("code", "nunique"),
        )
        .rename(columns={"date": "entry_date"})
        .sort_values(["entry_date"], kind="stable")
        .reset_index(drop=True)
    )
    ordered_df = regime_market_df.sort_values(
        ["market_regime_return", "entry_date"],
        ascending=[True, True],
        kind="stable",
    ).reset_index(drop=True)
    day_count = len(ordered_df)
    bucket_count = min(3, day_count)
    if bucket_count == 1:
        keys = ["neutral"]
        labels = {"neutral": "Neutral"}
    elif bucket_count == 2:
        keys = ["weak", "strong"]
        labels = {
            "weak": "Weak regime",
            "strong": "Strong regime",
        }
    else:
        keys = ["weak", "neutral", "strong"]
        labels = {
            "weak": "Weak regime (bottom tercile)",
            "neutral": "Neutral regime (middle tercile)",
            "strong": "Strong regime (top tercile)",
        }

    bucket_key_values: list[str] = []
    for rank_index in range(day_count):
        bucket_index = min(
            bucket_count - 1,
            int(math.floor(rank_index * bucket_count / day_count)),
        )
        bucket_key_values.append(keys[bucket_index])

    ordered_df["market_regime_rank"] = range(1, day_count + 1)
    ordered_df["market_regime_bucket_key"] = bucket_key_values
    ordered_df["market_regime_bucket_label"] = ordered_df["market_regime_bucket_key"].map(labels)
    return ordered_df.loc[:, list(_MARKET_REGIME_COLUMNS)].copy()


def _attach_market_regime(
    base_session_df: pd.DataFrame,
    market_regime_df: pd.DataFrame,
) -> pd.DataFrame:
    if base_session_df.empty:
        return _empty_base_session_df()
    if market_regime_df.empty:
        return base_session_df.loc[:, list(_BASE_SESSION_COLUMNS)].copy()
    enriched_df = base_session_df.drop(
        columns=[
            "market_regime_return",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
        ],
        errors="ignore",
    ).merge(
        market_regime_df.loc[
            :,
            [
                "entry_date",
                "market_regime_return",
                "market_regime_bucket_key",
                "market_regime_bucket_label",
            ],
        ],
        how="left",
        left_on="date",
        right_on="entry_date",
    )
    enriched_df = enriched_df.drop(columns=["entry_date"], errors="ignore")
    return enriched_df.loc[:, list(_BASE_SESSION_COLUMNS)].copy()


def _assign_periods_to_signal_sessions(
    signal_session_df: pd.DataFrame,
    *,
    periods_df: pd.DataFrame,
) -> pd.DataFrame:
    if signal_session_df.empty or periods_df.empty:
        return _empty_signal_session_df()

    working_df = signal_session_df.copy()
    date_ts = pd.to_datetime(working_df["date"])
    period_frames: list[pd.DataFrame] = []
    for period in periods_df.itertuples(index=False):
        period_index = int(cast(Any, period.period_index))
        period_label = str(cast(Any, period.period_label))
        period_start_date = str(cast(Any, period.period_start_date))
        period_end_date = str(cast(Any, period.period_end_date))
        period_mask = (
            date_ts >= pd.Timestamp(period_start_date)
        ) & (date_ts <= pd.Timestamp(period_end_date))
        if not bool(period_mask.any()):
            continue
        period_df = working_df.loc[period_mask].copy()
        period_df["period_index"] = period_index
        period_df["period_label"] = period_label
        period_df["period_start_date"] = period_start_date
        period_df["period_end_date"] = period_end_date
        period_frames.append(period_df)
    if not period_frames:
        return _empty_signal_session_df()
    combined = pd.concat(period_frames, ignore_index=True)
    return combined.loc[:, list(_SIGNAL_SESSION_COLUMNS)].copy()


def _build_signal_session_df(
    *,
    base_session_df: pd.DataFrame,
    conn: Any,
    interval_minutes_list: Sequence[int],
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    if base_session_df.empty:
        return _empty_signal_session_df()

    session_frames: list[pd.DataFrame] = []
    base_join_columns = [
        "date",
        "code",
        "previous_session_date",
        "next_session_date",
        "previous_close",
        "entry_price",
        "entry_actual_time",
        "next_session_open_price",
        "next_session_open_time",
        "next_session_1030_price",
        "next_session_1030_time",
        "prev_close_to_entry_return",
        "entry_to_next_open_return",
        "entry_to_next_1030_return",
        "current_entry_bucket_key",
        "current_entry_bucket_label",
        "market_regime_return",
        "market_regime_bucket_key",
        "market_regime_bucket_label",
    ]
    for interval_minutes in interval_minutes_list:
        volume_df = _query_interval_signal_volumes_df_from_connection(
            conn,
            interval_minutes=interval_minutes,
            start_date=start_date,
            end_date=end_date,
        )
        if volume_df.empty:
            continue
        merged_df = volume_df.merge(
            base_session_df.loc[:, base_join_columns],
            how="inner",
            on=["date", "code", "previous_session_date"],
        )
        for signal_def in _SIGNAL_FAMILY_DEFS:
            reference_column = signal_def["reference_column"]
            family_df = merged_df.copy()
            family_df["signal_ratio"] = (
                pd.to_numeric(family_df["opening_volume"], errors="coerce")
                / pd.to_numeric(family_df[reference_column], errors="coerce").replace(0, pd.NA)
            )
            family_df = family_df.loc[family_df["signal_ratio"].notna()].copy()
            if family_df.empty:
                continue
            family_df.insert(0, "interval_minutes", interval_minutes)
            family_df.insert(1, "signal_family", signal_def["signal_family"])
            family_df.insert(2, "signal_label", signal_def["signal_label"])
            family_df.insert(3, "target_bucket_side", signal_def["target_bucket_side"])
            family_df.insert(
                4,
                "expected_selected_bucket_label",
                signal_def["expected_selected_bucket_label"],
            )
            family_df["period_index"] = pd.NA
            family_df["period_label"] = pd.NA
            family_df["period_start_date"] = pd.NA
            family_df["period_end_date"] = pd.NA
            family_df["signal_bucket_index"] = pd.NA
            family_df["signal_bucket_label"] = pd.NA
            family_df["signal_selected"] = False
            session_frames.append(
                family_df.loc[:, list(_SIGNAL_SESSION_COLUMNS)].copy()
            )
    if not session_frames:
        return _empty_signal_session_df()
    return pd.concat(session_frames, ignore_index=True)


def _assign_signal_buckets(
    signal_session_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    if signal_session_df.empty:
        return _empty_signal_session_df()

    bucketed_frames: list[pd.DataFrame] = []
    group_columns = ["interval_minutes", "signal_family", "period_index"]
    for _, group_df in signal_session_df.groupby(group_columns, sort=True):
        scoped_df = group_df.copy().sort_values(["date", "code"]).reset_index(drop=True)
        effective_bucket_count = min(bucket_count, len(scoped_df))
        if effective_bucket_count <= 1:
            scoped_df["signal_bucket_index"] = 1
            scoped_df["signal_bucket_label"] = "Q1"
        else:
            scoped_df["signal_bucket_index"] = _assign_quantile_bucket(
                scoped_df["signal_ratio"],
                bucket_count=effective_bucket_count,
            )
            scoped_df["signal_bucket_label"] = scoped_df["signal_bucket_index"].map(
                lambda value: f"Q{int(value)}"
            )
        max_bucket_index = int(pd.Series(scoped_df["signal_bucket_index"]).max())
        scoped_df["signal_selected"] = False
        low_mask = scoped_df["target_bucket_side"].astype(str) == "low"
        high_mask = scoped_df["target_bucket_side"].astype(str) == "high"
        scoped_df.loc[low_mask, "signal_selected"] = (
            pd.to_numeric(scoped_df.loc[low_mask, "signal_bucket_index"], errors="coerce")
            == 1
        )
        scoped_df.loc[high_mask, "signal_selected"] = (
            pd.to_numeric(scoped_df.loc[high_mask, "signal_bucket_index"], errors="coerce")
            == max_bucket_index
        )
        bucketed_frames.append(scoped_df)

    combined = pd.concat(bucketed_frames, ignore_index=True)
    combined["signal_bucket_index"] = combined["signal_bucket_index"].astype(int)
    combined["signal_bucket_label"] = combined["signal_bucket_label"].astype(str)
    combined["signal_selected"] = combined["signal_selected"].astype(bool)
    return combined.loc[:, list(_SIGNAL_SESSION_COLUMNS)].copy()


def _build_selected_trade_level_df(
    signal_session_df: pd.DataFrame,
    *,
    entry_time: str,
    next_session_exit_time: str,
    tail_fraction: float,
) -> pd.DataFrame:
    selected_df = signal_session_df.loc[signal_session_df["signal_selected"]].copy()
    if selected_df.empty:
        return _empty_selected_trade_level_df()

    subgroup_label_map = _subgroup_label_map(
        tail_fraction,
        entry_time=entry_time,
    )
    subgroup_frames: list[pd.DataFrame] = []
    all_df = selected_df.copy()
    all_df["subgroup_key"] = "all"
    all_df["subgroup_label"] = subgroup_label_map["all"]
    subgroup_frames.append(all_df)
    bucket_df = selected_df.copy()
    bucket_df["subgroup_key"] = bucket_df["current_entry_bucket_key"].astype(str)
    bucket_df["subgroup_label"] = bucket_df["subgroup_key"].map(subgroup_label_map)
    subgroup_frames.append(bucket_df)
    subgroup_df = pd.concat(subgroup_frames, ignore_index=True)

    trade_frames: list[pd.DataFrame] = []
    for exit_label, exit_time_target, price_column, time_column in (
        ("next_open", "next open", "next_session_open_price", "next_session_open_time"),
        (
            "next_1030",
            f"next {next_session_exit_time}",
            "next_session_1030_price",
            "next_session_1030_time",
        ),
    ):
        trade_df = subgroup_df.copy()
        trade_df["exit_label"] = exit_label
        trade_df["exit_time_target"] = exit_time_target
        trade_df["exit_price"] = pd.to_numeric(trade_df[price_column], errors="coerce")
        trade_df["exit_actual_time"] = trade_df[time_column]
        trade_df["trade_return"] = np.nan
        valid_mask = (
            pd.to_numeric(trade_df["entry_price"], errors="coerce").notna()
            & (pd.to_numeric(trade_df["entry_price"], errors="coerce") > 0)
            & trade_df["exit_price"].notna()
            & (trade_df["exit_price"] > 0)
        )
        trade_df.loc[valid_mask, "trade_return"] = (
            trade_df.loc[valid_mask, "exit_price"]
            / pd.to_numeric(trade_df.loc[valid_mask, "entry_price"], errors="coerce")
            - 1.0
        )
        trade_frames.append(
            trade_df.loc[
                trade_df["trade_return"].notna(),
                list(_SELECTED_TRADE_LEVEL_COLUMNS),
            ].copy()
        )
    if not trade_frames:
        return _empty_selected_trade_level_df()
    return pd.concat(trade_frames, ignore_index=True)


def _build_market_trade_frame(
    base_session_df: pd.DataFrame,
    *,
    entry_time: str,
    next_session_exit_time: str,
    tail_fraction: float,
) -> pd.DataFrame:
    if base_session_df.empty:
        return pd.DataFrame()

    subgroup_label_map = _subgroup_label_map(
        tail_fraction,
        entry_time=entry_time,
    )
    subgroup_frames: list[pd.DataFrame] = []
    all_df = base_session_df.copy()
    all_df["subgroup_key"] = "all"
    all_df["subgroup_label"] = subgroup_label_map["all"]
    subgroup_frames.append(all_df)
    bucket_df = base_session_df.copy()
    bucket_df["subgroup_key"] = bucket_df["current_entry_bucket_key"].astype(str)
    bucket_df["subgroup_label"] = bucket_df["subgroup_key"].map(subgroup_label_map)
    subgroup_frames.append(bucket_df)
    subgroup_df = pd.concat(subgroup_frames, ignore_index=True)

    trade_frames: list[pd.DataFrame] = []
    for exit_label, exit_time_target, return_column in (
        ("next_open", "next open", "entry_to_next_open_return"),
        ("next_1030", f"next {next_session_exit_time}", "entry_to_next_1030_return"),
    ):
        trade_df = subgroup_df.copy()
        trade_df["exit_label"] = exit_label
        trade_df["exit_time_target"] = exit_time_target
        trade_df["trade_return"] = pd.to_numeric(trade_df[return_column], errors="coerce")
        trade_frames.append(trade_df.loc[trade_df["trade_return"].notna()].copy())
    if not trade_frames:
        return pd.DataFrame()
    return pd.concat(trade_frames, ignore_index=True)


def _summarize_trade_returns(values: pd.Series) -> tuple[float | None, float | None, float | None, float | None]:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None, None, None, None
    return (
        float(valid.mean()),
        float(valid.median()),
        float(valid.sum()),
        float((valid > 0).mean()),
    )


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
        (
            trade_mean,
            trade_median,
            trade_sum,
            hit_positive,
        ) = _summarize_trade_returns(group_df["trade_return"])
        row = {
            **key_map,
            "sample_count": int(len(group_df)),
            "sample_share": float(len(group_df) / total_sample_count),
            "date_count": int(group_df["date"].nunique()),
            "stock_count": int(group_df["code"].nunique()),
            "trade_return_mean": trade_mean,
            "trade_return_median": trade_median,
            "trade_return_sum": trade_sum,
            "hit_positive": hit_positive,
        }
        rows.append(row)
    return rows


def _build_market_summary_df(market_trade_df: pd.DataFrame) -> pd.DataFrame:
    rows = _build_summary_rows(
        market_trade_df,
        group_columns=[
            "exit_label",
            "exit_time_target",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
        ],
        total_group_columns=["exit_label", "exit_time_target", "subgroup_key", "subgroup_label"],
    )
    if not rows:
        return _empty_market_summary_df()
    return pd.DataFrame.from_records(rows, columns=_MARKET_SUMMARY_COLUMNS)


def _build_signal_summary_df(selected_trade_level_df: pd.DataFrame) -> pd.DataFrame:
    total_trade_df = selected_trade_level_df.loc[
        selected_trade_level_df["subgroup_key"] == "all"
    ].copy()
    rows = _build_summary_rows(
        selected_trade_level_df,
        group_columns=[
            "interval_minutes",
            "signal_family",
            "signal_label",
            "target_bucket_side",
            "expected_selected_bucket_label",
            "exit_label",
            "exit_time_target",
            "subgroup_key",
            "subgroup_label",
        ],
        total_group_columns=[
            "interval_minutes",
            "signal_family",
            "signal_label",
            "target_bucket_side",
            "expected_selected_bucket_label",
            "exit_label",
            "exit_time_target",
        ],
        total_trade_df=total_trade_df,
    )
    if not rows:
        return _empty_signal_summary_df()
    return pd.DataFrame.from_records(rows, columns=_SIGNAL_SUMMARY_COLUMNS)


def _build_intersection_summary_df(selected_trade_level_df: pd.DataFrame) -> pd.DataFrame:
    rows = _build_summary_rows(
        selected_trade_level_df,
        group_columns=[
            "interval_minutes",
            "signal_family",
            "signal_label",
            "target_bucket_side",
            "expected_selected_bucket_label",
            "exit_label",
            "exit_time_target",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
        ],
        total_group_columns=[
            "interval_minutes",
            "signal_family",
            "signal_label",
            "target_bucket_side",
            "expected_selected_bucket_label",
            "exit_label",
            "exit_time_target",
            "subgroup_key",
            "subgroup_label",
        ],
    )
    if not rows:
        return _empty_intersection_summary_df()
    return pd.DataFrame.from_records(rows, columns=_INTERSECTION_SUMMARY_COLUMNS)


def _build_period_intersection_summary_df(
    selected_trade_level_df: pd.DataFrame,
) -> pd.DataFrame:
    rows = _build_summary_rows(
        selected_trade_level_df,
        group_columns=[
            "interval_minutes",
            "signal_family",
            "signal_label",
            "target_bucket_side",
            "expected_selected_bucket_label",
            "exit_label",
            "exit_time_target",
            "period_index",
            "period_label",
            "period_start_date",
            "period_end_date",
            "market_regime_bucket_key",
            "market_regime_bucket_label",
            "subgroup_key",
            "subgroup_label",
        ],
        total_group_columns=[
            "interval_minutes",
            "signal_family",
            "signal_label",
            "target_bucket_side",
            "expected_selected_bucket_label",
            "exit_label",
            "exit_time_target",
            "period_index",
            "period_label",
            "period_start_date",
            "period_end_date",
            "subgroup_key",
            "subgroup_label",
        ],
    )
    if not rows:
        return _empty_period_intersection_summary_df()
    return pd.DataFrame.from_records(rows, columns=_PERIOD_INTERSECTION_SUMMARY_COLUMNS)


def run_topix100_1445_entry_signal_regime_comparison_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes_list: Sequence[int] | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    period_months: int = DEFAULT_PERIOD_MONTHS,
    entry_time: str = DEFAULT_ENTRY_TIME,
    next_session_exit_time: str = DEFAULT_NEXT_SESSION_EXIT_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
) -> Topix1001445EntrySignalRegimeComparisonResult:
    validated_intervals = _normalize_interval_minutes(interval_minutes_list)
    validated_bucket_count = _validate_bucket_count(bucket_count)
    validated_period_months = _validate_period_months(period_months)
    validated_entry_time = _validate_time_label(entry_time, argument_name="entry_time")
    validated_next_session_exit_time = _validate_time_label(
        next_session_exit_time,
        argument_name="next_session_exit_time",
    )
    validated_tail_fraction = _validate_tail_fraction(tail_fraction)

    with _open_analysis_connection(db_path) as ctx:
        available_start_date, available_end_date = _fetch_available_date_range(
            ctx.connection
        )
        analysis_start_date, analysis_end_date = _resolve_analysis_range(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            start_date=start_date,
            end_date=end_date,
        )
        topix100_constituent_count = _fetch_topix100_constituent_count(ctx.connection)

        base_session_df = _query_base_session_df_from_connection(
            ctx.connection,
            start_date=analysis_start_date,
            end_date=analysis_end_date,
            entry_time=validated_entry_time,
            next_session_exit_time=validated_next_session_exit_time,
        )
        if base_session_df.empty:
            periods_df = _empty_periods_df()
            market_regime_df = _empty_market_regime_df()
            signal_session_df = _empty_signal_session_df()
            selected_trade_level_df = _empty_selected_trade_level_df()
            market_summary_df = _empty_market_summary_df()
            signal_summary_df = _empty_signal_summary_df()
            intersection_summary_df = _empty_intersection_summary_df()
            period_intersection_summary_df = _empty_period_intersection_summary_df()
            total_session_count = 0
            regime_day_count = 0
            selected_signal_session_count = 0
        else:
            base_session_df = _assign_rank_groups(
                base_session_df,
                tail_fraction=validated_tail_fraction,
                entry_time=validated_entry_time,
            )
            market_regime_df = _assign_market_regime_buckets(base_session_df)
            base_session_df = _attach_market_regime(base_session_df, market_regime_df)
            periods_df = _build_periods(
                start_date=analysis_start_date,
                end_date=analysis_end_date,
                period_months=validated_period_months,
            )
            signal_session_df = _build_signal_session_df(
                base_session_df=base_session_df,
                conn=ctx.connection,
                interval_minutes_list=validated_intervals,
                start_date=analysis_start_date,
                end_date=analysis_end_date,
            )
            signal_session_df = _assign_periods_to_signal_sessions(
                signal_session_df,
                periods_df=periods_df,
            )
            signal_session_df = _assign_signal_buckets(
                signal_session_df,
                bucket_count=validated_bucket_count,
            )
            selected_trade_level_df = _build_selected_trade_level_df(
                signal_session_df,
                entry_time=validated_entry_time,
                next_session_exit_time=validated_next_session_exit_time,
                tail_fraction=validated_tail_fraction,
            )
            market_trade_df = _build_market_trade_frame(
                base_session_df,
                entry_time=validated_entry_time,
                next_session_exit_time=validated_next_session_exit_time,
                tail_fraction=validated_tail_fraction,
            )
            market_summary_df = _build_market_summary_df(market_trade_df)
            signal_summary_df = _build_signal_summary_df(selected_trade_level_df)
            intersection_summary_df = _build_intersection_summary_df(
                selected_trade_level_df
            )
            period_intersection_summary_df = _build_period_intersection_summary_df(
                selected_trade_level_df
            )
            total_session_count = int(len(base_session_df))
            regime_day_count = int(market_regime_df["entry_date"].nunique())
            selected_signal_session_count = int(
                signal_session_df.loc[signal_session_df["signal_selected"]].shape[0]
            )

    return Topix1001445EntrySignalRegimeComparisonResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        interval_minutes_list=validated_intervals,
        bucket_count=validated_bucket_count,
        period_months=validated_period_months,
        entry_time=validated_entry_time,
        next_session_exit_time=validated_next_session_exit_time,
        tail_fraction=validated_tail_fraction,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        regime_day_count=regime_day_count,
        selected_signal_session_count=selected_signal_session_count,
        periods_df=periods_df,
        market_regime_df=market_regime_df,
        base_session_df=base_session_df,
        signal_session_df=signal_session_df,
        selected_trade_level_df=selected_trade_level_df,
        market_summary_df=market_summary_df,
        signal_summary_df=signal_summary_df,
        intersection_summary_df=intersection_summary_df,
        period_intersection_summary_df=period_intersection_summary_df,
    )


def _split_result_payload(
    result: Topix1001445EntrySignalRegimeComparisonResult,
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
            "interval_minutes_list": list(result.interval_minutes_list),
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
            "entry_time": result.entry_time,
            "next_session_exit_time": result.next_session_exit_time,
            "tail_fraction": result.tail_fraction,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
            "regime_day_count": result.regime_day_count,
            "selected_signal_session_count": result.selected_signal_session_count,
        },
        {
            "periods_df": result.periods_df,
            "market_regime_df": result.market_regime_df,
            "base_session_df": result.base_session_df,
            "signal_session_df": result.signal_session_df,
            "selected_trade_level_df": result.selected_trade_level_df,
            "market_summary_df": result.market_summary_df,
            "signal_summary_df": result.signal_summary_df,
            "intersection_summary_df": result.intersection_summary_df,
            "period_intersection_summary_df": result.period_intersection_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix1001445EntrySignalRegimeComparisonResult:
    return Topix1001445EntrySignalRegimeComparisonResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes_list=tuple(int(value) for value in metadata["interval_minutes_list"]),
        bucket_count=int(metadata["bucket_count"]),
        period_months=int(metadata["period_months"]),
        entry_time=str(metadata["entry_time"]),
        next_session_exit_time=str(metadata["next_session_exit_time"]),
        tail_fraction=float(metadata["tail_fraction"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        regime_day_count=int(metadata["regime_day_count"]),
        selected_signal_session_count=int(metadata["selected_signal_session_count"]),
        periods_df=tables["periods_df"],
        market_regime_df=tables["market_regime_df"],
        base_session_df=tables["base_session_df"],
        signal_session_df=tables["signal_session_df"],
        selected_trade_level_df=tables["selected_trade_level_df"],
        market_summary_df=tables["market_summary_df"],
        signal_summary_df=tables["signal_summary_df"],
        intersection_summary_df=tables["intersection_summary_df"],
        period_intersection_summary_df=tables["period_intersection_summary_df"],
    )


def _build_published_summary(
    result: Topix1001445EntrySignalRegimeComparisonResult,
) -> dict[str, Any]:
    return {
        "intervalMinutesList": list(result.interval_minutes_list),
        "bucketCount": result.bucket_count,
        "periodMonths": result.period_months,
        "entryTime": result.entry_time,
        "nextSessionExitTime": result.next_session_exit_time,
        "tailFraction": result.tail_fraction,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "topix100ConstituentCount": result.topix100_constituent_count,
        "totalSessionCount": result.total_session_count,
        "regimeDayCount": result.regime_day_count,
        "selectedSignalSessionCount": result.selected_signal_session_count,
        "signalSummary": result.signal_summary_df.to_dict(orient="records"),
        "intersectionSummary": result.intersection_summary_df.to_dict(orient="records"),
    }


def _build_research_bundle_summary_markdown(
    result: Topix1001445EntrySignalRegimeComparisonResult,
) -> str:
    lines = [
        "# TOPIX100 14:45 Entry Signal/Regime Comparison",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{', '.join(str(value) for value in result.interval_minutes_list)}`",
        f"- Entry time: `{result.entry_time}`",
        f"- Next-session timed exit: `{result.next_session_exit_time}`",
        f"- Bucket count: `{result.bucket_count}`",
        f"- Period months: `{result.period_months}`",
        f"- Tail fraction per side: `{result.tail_fraction * 100:.1f}%`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Base session count: `{result.total_session_count}`",
        f"- Market regime day count: `{result.regime_day_count}`",
        f"- Selected signal session count: `{result.selected_signal_session_count}`",
        "",
        "## Current Read",
        "",
    ]
    if result.signal_summary_df.empty:
        lines.append("- Signal summary was empty.")
    else:
        top_signal_df = (
            result.signal_summary_df.loc[
                result.signal_summary_df["subgroup_key"] == "all"
            ]
            .sort_values("trade_return_mean", ascending=False, kind="stable")
            .head(6)
        )
        for row in top_signal_df.itertuples(index=False):
            lines.append(
                f"- `{row.signal_family} / {row.interval_minutes}m / {row.exit_label}` "
                f"selected `{row.expected_selected_bucket_label}` hypothesis mean "
                f"`{_format_optional_pct(row.trade_return_mean)}`."
            )
    if not result.intersection_summary_df.empty:
        lines.extend(["", "## Best Intersections", ""])
        top_intersection_df = (
            result.intersection_summary_df.loc[
                result.intersection_summary_df["subgroup_key"] == "all"
            ]
            .sort_values("trade_return_mean", ascending=False, kind="stable")
            .head(6)
        )
        for row in top_intersection_df.itertuples(index=False):
            lines.append(
                f"- `{row.signal_family} / {row.interval_minutes}m / {row.exit_label} / "
                f"{row.market_regime_bucket_key}` mean "
                f"`{_format_optional_pct(row.trade_return_mean)}`."
            )
    lines.extend(
        [
            "",
            "## Caveat",
            "",
            "- This study uses the current TOPIX100 constituent set available in `stocks.scale_category` for the minute-bar join. Treat the result as a current-universe retrospective study, not a point-in-time historical membership reconstruction.",
            "- The half-year signal buckets are descriptive ex-post groupings built from the realized ratio distribution inside each period. Use them for research comparison, not as-is for live thresholding.",
            "- The weak/neutral/strong market-regime buckets are also descriptive ex-post groupings across the realized 14:45 cross-sectional mean return path.",
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(lines)


def write_topix100_1445_entry_signal_regime_comparison_research_bundle(
    result: Topix1001445EntrySignalRegimeComparisonResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_1445_ENTRY_SIGNAL_REGIME_COMPARISON_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_1445_entry_signal_regime_comparison_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes_list": list(result.interval_minutes_list),
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
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_1445_entry_signal_regime_comparison_research_bundle(
    bundle_path: str | Path,
) -> Topix1001445EntrySignalRegimeComparisonResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_1445_entry_signal_regime_comparison_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_1445_ENTRY_SIGNAL_REGIME_COMPARISON_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_1445_entry_signal_regime_comparison_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_1445_ENTRY_SIGNAL_REGIME_COMPARISON_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
