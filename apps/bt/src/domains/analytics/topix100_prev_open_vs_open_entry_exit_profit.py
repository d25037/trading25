"""
TOPIX100 prev-open-vs-open entry/exit profit research.

This study fixes the signal to:

    current-session opening-bucket volume / previous-session opening-bucket volume

for 5-minute, 15-minute, and 30-minute opening buckets on the current TOPIX100
constituent set. It then evaluates fixed entry/exit timing combinations:

- entry: 09:05 / 09:15 / 09:30 (matching the opening-bucket horizon)
- exit: 10:30 / 14:45 / close / next open

The ratio is bucketed into approximately quartile-style groups within each
half-year window so the user can compare Q1-Q4 trade profitability. The raw
signal itself is causal, but the half-year bucket labels remain descriptive
ex-post groups rather than live-tradable thresholds.
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

TOPIX100_PREV_OPEN_VS_OPEN_ENTRY_EXIT_PROFIT_EXPERIMENT_ID = (
    "market-behavior/topix100-prev-open-vs-open-entry-exit-profit"
)
DEFAULT_BUCKET_COUNT = 4
DEFAULT_PERIOD_MONTHS = 6
DEFAULT_ROUND_TRIP_COST_BPS = 0.0
ENTRY_TIME_BY_INTERVAL: dict[int, str] = {
    5: "09:05",
    15: "09:15",
    30: "09:30",
}
EXIT_SPECS: tuple[tuple[str, str, str, str], ...] = (
    ("10:30", "10:30", "exit_1030_price", "exit_1030_actual_time"),
    ("14:45", "14:45", "exit_1445_price", "exit_1445_actual_time"),
    ("close", "close", "same_day_close_price", "same_day_close_time"),
    ("next_open", "next open", "next_session_open_price", "next_session_open_time"),
)

_PERIOD_COLUMNS: tuple[str, ...] = (
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
)
_SESSION_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "date",
    "code",
    "previous_session_date",
    "previous_opening_volume",
    "opening_volume",
    "prev_open_vs_open_ratio",
    "day_open",
    "day_open_time",
    "entry_price",
    "entry_actual_time",
    "exit_1030_price",
    "exit_1030_actual_time",
    "exit_1445_price",
    "exit_1445_actual_time",
    "same_day_close_price",
    "same_day_close_time",
    "next_session_date",
    "next_session_open_price",
    "next_session_open_time",
    "ratio_bucket_index",
    "ratio_bucket_label",
)
_TRADE_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "exit_label",
    "exit_time_target",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "date",
    "code",
    "previous_session_date",
    "next_session_date",
    "previous_opening_volume",
    "opening_volume",
    "prev_open_vs_open_ratio",
    "ratio_bucket_index",
    "ratio_bucket_label",
    "entry_price",
    "entry_actual_time",
    "exit_price",
    "exit_actual_time",
    "gross_long_return",
    "net_long_return",
    "gross_short_return",
    "net_short_return",
)
_PERIOD_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "exit_label",
    "exit_time_target",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "ratio_bucket_index",
    "ratio_bucket_label",
    "sample_count",
    "sample_share",
    "stock_count",
    "date_count",
    "ratio_min",
    "ratio_p25",
    "ratio_median",
    "ratio_p75",
    "ratio_max",
    "ratio_mean",
    "net_long_return_mean",
    "net_long_return_median",
    "net_long_return_sum",
    "net_long_hit_positive",
    "net_short_return_mean",
    "net_short_return_median",
    "net_short_return_sum",
    "net_short_hit_positive",
)
_INTERVAL_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "exit_label",
    "exit_time_target",
    "ratio_bucket_index",
    "ratio_bucket_label",
    "sample_count",
    "sample_share",
    "stock_count",
    "date_count",
    "period_count",
    "ratio_min",
    "ratio_p25",
    "ratio_median",
    "ratio_p75",
    "ratio_max",
    "ratio_mean",
    "net_long_return_mean",
    "net_long_return_median",
    "net_long_return_sum",
    "net_long_hit_positive",
    "net_short_return_mean",
    "net_short_return_median",
    "net_short_return_sum",
    "net_short_hit_positive",
)
_PERIOD_INTERVAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "exit_label",
    "exit_time_target",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "sample_count",
    "low_ratio_bucket_label",
    "high_ratio_bucket_label",
    "low_ratio_mean",
    "high_ratio_mean",
    "ratio_mean_spread_high_minus_low",
    "low_net_long_return_mean",
    "high_net_long_return_mean",
    "net_long_return_mean_spread_high_minus_low",
    "low_net_short_return_mean",
    "high_net_short_return_mean",
    "net_short_return_mean_spread_high_minus_low",
)
_INTERVAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "exit_label",
    "exit_time_target",
    "period_count",
    "sample_count",
    "low_ratio_bucket_label",
    "high_ratio_bucket_label",
    "low_ratio_mean",
    "high_ratio_mean",
    "ratio_mean_spread_high_minus_low",
    "low_net_long_return_mean",
    "high_net_long_return_mean",
    "net_long_return_mean_spread_high_minus_low",
    "low_net_short_return_mean",
    "high_net_short_return_mean",
    "net_short_return_mean_spread_high_minus_low",
)


@dataclass(frozen=True)
class Topix100PrevOpenVsOpenEntryExitProfitResult:
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
    round_trip_cost_bps: float
    topix100_constituent_count: int
    total_session_count: int
    periods_df: pd.DataFrame
    session_level_df: pd.DataFrame
    trade_level_df: pd.DataFrame
    period_bucket_summary_df: pd.DataFrame
    interval_bucket_summary_df: pd.DataFrame
    period_interval_summary_df: pd.DataFrame
    interval_summary_df: pd.DataFrame


def _empty_periods_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_COLUMNS))


def _empty_session_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SESSION_LEVEL_COLUMNS))


def _empty_trade_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_TRADE_LEVEL_COLUMNS))


def _empty_period_bucket_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_BUCKET_SUMMARY_COLUMNS))


def _empty_interval_bucket_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_INTERVAL_BUCKET_SUMMARY_COLUMNS))


def _empty_period_interval_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_INTERVAL_SUMMARY_COLUMNS))


def _empty_interval_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_INTERVAL_SUMMARY_COLUMNS))


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
    unsupported = tuple(
        value for value in normalized if value not in ENTRY_TIME_BY_INTERVAL
    )
    if unsupported:
        raise ValueError(
            "Unsupported interval_minutes_list value(s): "
            + ", ".join(str(value) for value in unsupported)
            + ". Supported values are 5, 15, 30."
        )
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


def _validate_round_trip_cost_bps(value: float) -> float:
    round_trip_cost_bps = float(value)
    if round_trip_cost_bps < 0:
        raise ValueError("round_trip_cost_bps must be non-negative")
    return round_trip_cost_bps


def _parse_time_to_minute(value: str) -> int:
    normalized = str(value).strip()
    if len(normalized) != 5 or normalized[2] != ":":
        raise ValueError(f"time must be formatted as HH:MM, got {value!r}")
    return int(normalized[:2]) * 60 + int(normalized[3:])


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


def _assign_periods_to_sessions(
    session_level_df: pd.DataFrame,
    *,
    periods_df: pd.DataFrame,
) -> pd.DataFrame:
    if session_level_df.empty or periods_df.empty:
        return _empty_session_level_df()

    working_df = session_level_df.copy()
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
        return _empty_session_level_df()
    combined = pd.concat(period_frames, ignore_index=True)
    return combined.loc[:, list(_SESSION_LEVEL_COLUMNS[:-2])].copy()


def _assign_quantile_bucket(series: pd.Series, *, bucket_count: int) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    rank_pct = valid.rank(method="first", pct=True)
    bucket = (rank_pct * bucket_count).apply(np.ceil).clip(1, bucket_count)
    return cast(pd.Series, bucket.astype(int))


def _coerce_optional_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _subtract_optional_float(high: Any, low: Any) -> float | None:
    high_value = _coerce_optional_float(high)
    low_value = _coerce_optional_float(low)
    if high_value is None or low_value is None:
        return None
    return high_value - low_value


def _query_session_level_df_from_connection(
    conn: Any,
    *,
    interval_minutes: int,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    entry_time = ENTRY_TIME_BY_INTERVAL[interval_minutes]
    entry_minute = _parse_time_to_minute(entry_time)
    exit_1030_minute = _parse_time_to_minute("10:30")
    exit_1445_minute = _parse_time_to_minute("14:45")
    date_filter_sql, date_params = _date_filter_sql(
        column_name="m.date",
        start_date=start_date,
        end_date=end_date,
    )
    params: list[Any] = [
        *date_params,
        interval_minutes,
        entry_minute,
        entry_minute,
        exit_1030_minute,
        exit_1030_minute,
        exit_1445_minute,
        exit_1445_minute,
    ]
    session_level_df = cast(
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
                    m.close,
                    m.volume
                FROM stock_data_minute_raw m
                JOIN topix100_stocks s
                  ON s.normalized_code = {_normalize_code_sql('m.code')}
                WHERE m.time IS NOT NULL
                  AND m.open IS NOT NULL
                  AND m.close IS NOT NULL
                  AND m.volume IS NOT NULL
                  AND m.open > 0
                  {date_filter_sql}
            ),
            daily AS (
                SELECT
                    date,
                    code,
                    sum(volume) FILTER (
                        WHERE minute_of_day >= 540
                          AND minute_of_day < 540 + ?
                    ) AS opening_volume,
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
                    arg_max(close, minute_of_day) FILTER (
                        WHERE minute_of_day <= ?
                    ) AS exit_1445_price,
                    arg_max(time, minute_of_day) FILTER (
                        WHERE minute_of_day <= ?
                    ) AS exit_1445_actual_time,
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
                    lag(opening_volume) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS previous_opening_volume,
                    opening_volume,
                    day_open,
                    day_open_time,
                    entry_price,
                    entry_actual_time,
                    exit_1030_price,
                    exit_1030_actual_time,
                    exit_1445_price,
                    exit_1445_actual_time,
                    same_day_close_price,
                    same_day_close_time,
                    lead(date) OVER (PARTITION BY code ORDER BY date) AS next_session_date,
                    lead(day_open) OVER (PARTITION BY code ORDER BY date) AS next_session_open_price,
                    lead(day_open_time) OVER (
                        PARTITION BY code
                        ORDER BY date
                    ) AS next_session_open_time
                FROM daily
                WHERE opening_volume IS NOT NULL
                  AND opening_volume > 0
                  AND day_open IS NOT NULL
                  AND day_open > 0
            )
            SELECT
                date,
                code,
                previous_session_date,
                previous_opening_volume,
                opening_volume,
                opening_volume / NULLIF(previous_opening_volume, 0) AS prev_open_vs_open_ratio,
                day_open,
                day_open_time,
                entry_price,
                entry_actual_time,
                exit_1030_price,
                exit_1030_actual_time,
                exit_1445_price,
                exit_1445_actual_time,
                same_day_close_price,
                same_day_close_time,
                next_session_date,
                next_session_open_price,
                next_session_open_time
            FROM ordered
            WHERE previous_opening_volume IS NOT NULL
              AND previous_opening_volume > 0
              AND entry_price IS NOT NULL
              AND entry_price > 0
            ORDER BY date, code
            """,
            params,
        ).fetchdf(),
    )
    if session_level_df.empty:
        return _empty_session_level_df()

    session_level_df = session_level_df.copy()
    session_level_df.insert(0, "entry_time", entry_time)
    session_level_df.insert(0, "interval_minutes", interval_minutes)
    session_level_df["period_index"] = pd.NA
    session_level_df["period_label"] = pd.NA
    session_level_df["period_start_date"] = pd.NA
    session_level_df["period_end_date"] = pd.NA
    session_level_df["ratio_bucket_index"] = pd.NA
    session_level_df["ratio_bucket_label"] = pd.NA
    return session_level_df.loc[:, list(_SESSION_LEVEL_COLUMNS)].copy()


def _assign_ratio_buckets(
    session_level_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_session_level_df()

    bucketed_frames: list[pd.DataFrame] = []
    for _, group_df in session_level_df.groupby(
        ["interval_minutes", "period_index"],
        sort=True,
    ):
        scoped_df = group_df.copy().sort_values(["date", "code"]).reset_index(drop=True)
        effective_bucket_count = min(bucket_count, len(scoped_df))
        if effective_bucket_count <= 1:
            scoped_df["ratio_bucket_index"] = 1
            scoped_df["ratio_bucket_label"] = "Q1"
            bucketed_frames.append(scoped_df)
            continue

        scoped_df["ratio_bucket_index"] = _assign_quantile_bucket(
            scoped_df["prev_open_vs_open_ratio"],
            bucket_count=effective_bucket_count,
        )
        scoped_df["ratio_bucket_label"] = scoped_df["ratio_bucket_index"].map(
            lambda value: f"Q{int(value)}"
        )
        bucketed_frames.append(scoped_df)

    combined = pd.concat(bucketed_frames, ignore_index=True)
    combined["ratio_bucket_index"] = combined["ratio_bucket_index"].astype(int)
    combined["ratio_bucket_label"] = combined["ratio_bucket_label"].astype(str)
    return combined.loc[:, list(_SESSION_LEVEL_COLUMNS)].copy()


def _build_trade_level_df(
    session_level_df: pd.DataFrame,
    *,
    round_trip_cost_bps: float,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_trade_level_df()

    cost = round_trip_cost_bps / 10_000.0
    trade_frames: list[pd.DataFrame] = []
    for exit_label, exit_time_target, price_column, time_column in EXIT_SPECS:
        trade_df = session_level_df.copy()
        trade_df["exit_label"] = exit_label
        trade_df["exit_time_target"] = exit_time_target
        trade_df["exit_price"] = pd.to_numeric(trade_df[price_column], errors="coerce")
        trade_df["exit_actual_time"] = trade_df[time_column]
        trade_df["entry_price"] = pd.to_numeric(trade_df["entry_price"], errors="coerce")
        valid_mask = (
            trade_df["entry_price"].notna()
            & (trade_df["entry_price"] > 0)
            & trade_df["exit_price"].notna()
            & (trade_df["exit_price"] > 0)
        )
        trade_df["gross_long_return"] = np.nan
        trade_df.loc[valid_mask, "gross_long_return"] = (
            trade_df.loc[valid_mask, "exit_price"]
            / trade_df.loc[valid_mask, "entry_price"]
            - 1.0
        )
        trade_df["gross_short_return"] = np.nan
        trade_df.loc[valid_mask, "gross_short_return"] = (
            -pd.to_numeric(trade_df.loc[valid_mask, "gross_long_return"], errors="coerce")
        )
        trade_df["net_long_return"] = np.nan
        trade_df.loc[valid_mask, "net_long_return"] = (
            pd.to_numeric(trade_df.loc[valid_mask, "gross_long_return"], errors="coerce")
            - cost
        )
        trade_df["net_short_return"] = np.nan
        trade_df.loc[valid_mask, "net_short_return"] = (
            pd.to_numeric(trade_df.loc[valid_mask, "gross_short_return"], errors="coerce")
            - cost
        )
        trade_frames.append(
            trade_df.loc[
                :,
                [
                    "interval_minutes",
                    "entry_time",
                    "exit_label",
                    "exit_time_target",
                    "period_index",
                    "period_label",
                    "period_start_date",
                    "period_end_date",
                    "date",
                    "code",
                    "previous_session_date",
                    "next_session_date",
                    "previous_opening_volume",
                    "opening_volume",
                    "prev_open_vs_open_ratio",
                    "ratio_bucket_index",
                    "ratio_bucket_label",
                    "entry_price",
                    "entry_actual_time",
                    "exit_price",
                    "exit_actual_time",
                    "gross_long_return",
                    "net_long_return",
                    "gross_short_return",
                    "net_short_return",
                ],
            ].copy()
        )

    combined = pd.concat(trade_frames, ignore_index=True)
    return combined.loc[:, list(_TRADE_LEVEL_COLUMNS)].copy()


def _summarize_return_series(
    values: pd.Series,
) -> tuple[float | None, float | None, float | None, float | None, int]:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None, None, None, None, 0
    return (
        float(valid.mean()),
        float(valid.median()),
        float(valid.sum()),
        float((valid > 0).mean()),
        int(len(valid)),
    )


def _build_bucket_summary_rows(
    trade_level_df: pd.DataFrame,
    *,
    include_period_columns: bool,
) -> list[dict[str, Any]]:
    if trade_level_df.empty:
        return []

    valid_df = trade_level_df.loc[trade_level_df["net_long_return"].notna()].copy()
    if valid_df.empty:
        return []

    group_columns = ["interval_minutes", "entry_time", "exit_label", "exit_time_target"]
    total_group_columns = ["interval_minutes", "entry_time", "exit_label", "exit_time_target"]
    if include_period_columns:
        group_columns.extend(
            ["period_index", "period_label", "period_start_date", "period_end_date"]
        )
        total_group_columns.extend(
            ["period_index", "period_label", "period_start_date", "period_end_date"]
        )
    group_columns.extend(["ratio_bucket_index", "ratio_bucket_label"])

    group_sizes = (
        valid_df.groupby(total_group_columns, as_index=False)
        .agg(total_sample_count=("code", "size"))
    )
    size_lookup = {
        tuple(row[column] for column in total_group_columns): int(row["total_sample_count"])
        for row in group_sizes.to_dict(orient="records")
    }

    rows: list[dict[str, Any]] = []
    for group_key, bucket_df in valid_df.groupby(group_columns, sort=True):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        key_map = dict(zip(group_columns, group_key, strict=True))
        total_lookup_key = tuple(key_map[column] for column in total_group_columns)
        total_sample_count = size_lookup[total_lookup_key]

        ratio_series = pd.to_numeric(
            bucket_df["prev_open_vs_open_ratio"],
            errors="coerce",
        ).dropna()
        (
            net_long_mean,
            net_long_median,
            net_long_sum,
            net_long_hit,
            _,
        ) = _summarize_return_series(bucket_df["net_long_return"])
        (
            net_short_mean,
            net_short_median,
            net_short_sum,
            net_short_hit,
            _,
        ) = _summarize_return_series(bucket_df["net_short_return"])
        row = {
            "interval_minutes": int(key_map["interval_minutes"]),
            "entry_time": str(key_map["entry_time"]),
            "exit_label": str(key_map["exit_label"]),
            "exit_time_target": str(key_map["exit_time_target"]),
            "ratio_bucket_index": int(key_map["ratio_bucket_index"]),
            "ratio_bucket_label": str(key_map["ratio_bucket_label"]),
            "sample_count": int(len(bucket_df)),
            "sample_share": float(len(bucket_df) / total_sample_count),
            "stock_count": int(bucket_df["code"].nunique()),
            "date_count": int(bucket_df["date"].nunique()),
            "ratio_min": float(ratio_series.min()),
            "ratio_p25": float(ratio_series.quantile(0.25)),
            "ratio_median": float(ratio_series.median()),
            "ratio_p75": float(ratio_series.quantile(0.75)),
            "ratio_max": float(ratio_series.max()),
            "ratio_mean": float(ratio_series.mean()),
            "net_long_return_mean": net_long_mean,
            "net_long_return_median": net_long_median,
            "net_long_return_sum": net_long_sum,
            "net_long_hit_positive": net_long_hit,
            "net_short_return_mean": net_short_mean,
            "net_short_return_median": net_short_median,
            "net_short_return_sum": net_short_sum,
            "net_short_hit_positive": net_short_hit,
        }
        if include_period_columns:
            row.update(
                {
                    "period_index": int(key_map["period_index"]),
                    "period_label": str(key_map["period_label"]),
                    "period_start_date": str(key_map["period_start_date"]),
                    "period_end_date": str(key_map["period_end_date"]),
                }
            )
        else:
            row["period_count"] = int(bucket_df["period_index"].nunique())
        rows.append(row)
    return rows


def _build_period_bucket_summary_df(trade_level_df: pd.DataFrame) -> pd.DataFrame:
    rows = _build_bucket_summary_rows(trade_level_df, include_period_columns=True)
    if not rows:
        return _empty_period_bucket_summary_df()
    return pd.DataFrame.from_records(rows, columns=_PERIOD_BUCKET_SUMMARY_COLUMNS)


def _build_interval_bucket_summary_df(trade_level_df: pd.DataFrame) -> pd.DataFrame:
    rows = _build_bucket_summary_rows(trade_level_df, include_period_columns=False)
    if not rows:
        return _empty_interval_bucket_summary_df()
    return pd.DataFrame.from_records(rows, columns=_INTERVAL_BUCKET_SUMMARY_COLUMNS)


def _build_spread_summary_row(
    low_row: pd.Series,
    high_row: pd.Series,
    *,
    include_period_columns: bool,
    period_count: int | None = None,
    total_sample_count: int,
) -> dict[str, Any]:
    row = {
        "interval_minutes": int(low_row["interval_minutes"]),
        "entry_time": str(low_row["entry_time"]),
        "exit_label": str(low_row["exit_label"]),
        "exit_time_target": str(low_row["exit_time_target"]),
        "sample_count": total_sample_count,
        "low_ratio_bucket_label": str(low_row["ratio_bucket_label"]),
        "high_ratio_bucket_label": str(high_row["ratio_bucket_label"]),
        "low_ratio_mean": _coerce_optional_float(low_row["ratio_mean"]),
        "high_ratio_mean": _coerce_optional_float(high_row["ratio_mean"]),
        "ratio_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["ratio_mean"],
            low_row["ratio_mean"],
        ),
        "low_net_long_return_mean": _coerce_optional_float(low_row["net_long_return_mean"]),
        "high_net_long_return_mean": _coerce_optional_float(
            high_row["net_long_return_mean"]
        ),
        "net_long_return_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["net_long_return_mean"],
            low_row["net_long_return_mean"],
        ),
        "low_net_short_return_mean": _coerce_optional_float(
            low_row["net_short_return_mean"]
        ),
        "high_net_short_return_mean": _coerce_optional_float(
            high_row["net_short_return_mean"]
        ),
        "net_short_return_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["net_short_return_mean"],
            low_row["net_short_return_mean"],
        ),
    }
    if include_period_columns:
        row.update(
            {
                "period_index": int(low_row["period_index"]),
                "period_label": str(low_row["period_label"]),
                "period_start_date": str(low_row["period_start_date"]),
                "period_end_date": str(low_row["period_end_date"]),
            }
        )
    else:
        row["period_count"] = int(period_count or 0)
    return row


def _build_period_interval_summary_df(
    period_bucket_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if period_bucket_summary_df.empty:
        return _empty_period_interval_summary_df()

    rows: list[dict[str, Any]] = []
    group_columns = [
        "interval_minutes",
        "entry_time",
        "exit_label",
        "exit_time_target",
        "period_index",
        "period_label",
        "period_start_date",
        "period_end_date",
    ]
    for _, summary_df in period_bucket_summary_df.groupby(group_columns, sort=True):
        ordered_df = summary_df.sort_values("ratio_bucket_index", kind="stable")
        low_row = ordered_df.iloc[0]
        high_row = ordered_df.iloc[-1]
        rows.append(
            _build_spread_summary_row(
                low_row,
                high_row,
                include_period_columns=True,
                total_sample_count=int(summary_df["sample_count"].sum()),
            )
        )
    return pd.DataFrame.from_records(rows, columns=_PERIOD_INTERVAL_SUMMARY_COLUMNS)


def _build_interval_summary_df(
    interval_bucket_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if interval_bucket_summary_df.empty:
        return _empty_interval_summary_df()

    rows: list[dict[str, Any]] = []
    group_columns = ["interval_minutes", "entry_time", "exit_label", "exit_time_target"]
    for _, summary_df in interval_bucket_summary_df.groupby(group_columns, sort=True):
        ordered_df = summary_df.sort_values("ratio_bucket_index", kind="stable")
        low_row = ordered_df.iloc[0]
        high_row = ordered_df.iloc[-1]
        rows.append(
            _build_spread_summary_row(
                low_row,
                high_row,
                include_period_columns=False,
                period_count=int(summary_df["period_count"].max()),
                total_sample_count=int(summary_df["sample_count"].sum()),
            )
        )
    return pd.DataFrame.from_records(rows, columns=_INTERVAL_SUMMARY_COLUMNS)


def run_topix100_prev_open_vs_open_entry_exit_profit_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes_list: Sequence[int] | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    period_months: int = DEFAULT_PERIOD_MONTHS,
    round_trip_cost_bps: float = DEFAULT_ROUND_TRIP_COST_BPS,
) -> Topix100PrevOpenVsOpenEntryExitProfitResult:
    validated_intervals = _normalize_interval_minutes(interval_minutes_list)
    validated_bucket_count = _validate_bucket_count(bucket_count)
    validated_period_months = _validate_period_months(period_months)
    validated_round_trip_cost_bps = _validate_round_trip_cost_bps(round_trip_cost_bps)

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

        session_frames: list[pd.DataFrame] = []
        for interval_minutes in validated_intervals:
            session_frames.append(
                _query_session_level_df_from_connection(
                    ctx.connection,
                    interval_minutes=interval_minutes,
                    start_date=analysis_start_date,
                    end_date=analysis_end_date,
                )
            )

    combined_session_df = (
        pd.concat(session_frames, ignore_index=True)
        if session_frames
        else _empty_session_level_df()
    )
    if combined_session_df.empty:
        periods_df = _empty_periods_df()
        trade_level_df = _empty_trade_level_df()
        period_bucket_summary_df = _empty_period_bucket_summary_df()
        interval_bucket_summary_df = _empty_interval_bucket_summary_df()
        period_interval_summary_df = _empty_period_interval_summary_df()
        interval_summary_df = _empty_interval_summary_df()
        total_session_count = 0
    else:
        periods_df = _build_periods(
            start_date=analysis_start_date,
            end_date=analysis_end_date,
            period_months=validated_period_months,
        )
        combined_session_df = _assign_periods_to_sessions(
            combined_session_df,
            periods_df=periods_df,
        )
        combined_session_df = _assign_ratio_buckets(
            combined_session_df,
            bucket_count=validated_bucket_count,
        )
        trade_level_df = _build_trade_level_df(
            combined_session_df,
            round_trip_cost_bps=validated_round_trip_cost_bps,
        )
        period_bucket_summary_df = _build_period_bucket_summary_df(trade_level_df)
        interval_bucket_summary_df = _build_interval_bucket_summary_df(trade_level_df)
        period_interval_summary_df = _build_period_interval_summary_df(
            period_bucket_summary_df
        )
        interval_summary_df = _build_interval_summary_df(interval_bucket_summary_df)
        total_session_count = int(
            combined_session_df.groupby("interval_minutes").size().max()
        )

    return Topix100PrevOpenVsOpenEntryExitProfitResult(
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
        round_trip_cost_bps=validated_round_trip_cost_bps,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        periods_df=periods_df,
        session_level_df=combined_session_df,
        trade_level_df=trade_level_df,
        period_bucket_summary_df=period_bucket_summary_df,
        interval_bucket_summary_df=interval_bucket_summary_df,
        period_interval_summary_df=period_interval_summary_df,
        interval_summary_df=interval_summary_df,
    )


def _split_result_payload(
    result: Topix100PrevOpenVsOpenEntryExitProfitResult,
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
            "round_trip_cost_bps": result.round_trip_cost_bps,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
        },
        {
            "periods_df": result.periods_df,
            "session_level_df": result.session_level_df,
            "trade_level_df": result.trade_level_df,
            "period_bucket_summary_df": result.period_bucket_summary_df,
            "interval_bucket_summary_df": result.interval_bucket_summary_df,
            "period_interval_summary_df": result.period_interval_summary_df,
            "interval_summary_df": result.interval_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100PrevOpenVsOpenEntryExitProfitResult:
    return Topix100PrevOpenVsOpenEntryExitProfitResult(
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
        round_trip_cost_bps=float(
            metadata.get("round_trip_cost_bps", DEFAULT_ROUND_TRIP_COST_BPS)
        ),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        periods_df=tables["periods_df"],
        session_level_df=tables["session_level_df"],
        trade_level_df=tables["trade_level_df"],
        period_bucket_summary_df=tables["period_bucket_summary_df"],
        interval_bucket_summary_df=tables["interval_bucket_summary_df"],
        period_interval_summary_df=tables["period_interval_summary_df"],
        interval_summary_df=tables["interval_summary_df"],
    )


def _build_published_summary(
    result: Topix100PrevOpenVsOpenEntryExitProfitResult,
) -> dict[str, Any]:
    return {
        "intervalMinutesList": list(result.interval_minutes_list),
        "bucketCount": result.bucket_count,
        "periodMonths": result.period_months,
        "roundTripCostBps": result.round_trip_cost_bps,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "topix100ConstituentCount": result.topix100_constituent_count,
        "totalSessionCount": result.total_session_count,
        "intervalSummary": result.interval_summary_df.to_dict(orient="records"),
        "periodIntervalSummary": result.period_interval_summary_df.to_dict(orient="records"),
    }


def _format_optional_pct(value: Any) -> str:
    numeric = _coerce_optional_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric * 100:+.4f}%"


def _build_research_bundle_summary_markdown(
    result: Topix100PrevOpenVsOpenEntryExitProfitResult,
) -> str:
    lines = [
        "# TOPIX100 Prev-Open-vs-Open Entry/Exit Profit",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{', '.join(str(value) for value in result.interval_minutes_list)}`",
        "- Signal: `current opening-bucket volume / previous-session opening-bucket volume`",
        f"- Bucket count: `{result.bucket_count}`",
        f"- Period months: `{result.period_months}`",
        f"- Round-trip cost: `{result.round_trip_cost_bps:.2f} bps`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Session count per interval (max): `{result.total_session_count}`",
        "",
        "## Current Read",
        "",
    ]
    if result.interval_summary_df.empty:
        lines.append("- Interval summary was empty.")
    else:
        for row in result.interval_summary_df.itertuples(index=False):
            lines.append(
                f"- `{row.interval_minutes}m / {row.entry_time} -> {row.exit_label}`: "
                f"`{row.high_ratio_bucket_label}` minus `{row.low_ratio_bucket_label}` "
                f"long mean `{_format_optional_pct(row.net_long_return_mean_spread_high_minus_low)}`, "
                f"short mean `{_format_optional_pct(row.net_short_return_mean_spread_high_minus_low)}`."
            )
    lines.extend(
        [
            "",
            "## Caveat",
            "",
            "- This study uses the current TOPIX100 constituent set available in `stocks.scale_category` for the minute-bar join. Treat the result as a current-universe retrospective study, not a point-in-time historical membership reconstruction.",
            "- The half-year Q1-Q4 buckets are descriptive ex-post groupings built from the realized ratio distribution inside each period. Use them for research comparison, not as-is for live thresholding.",
            "- `next_open` exits naturally lose the last available session because the next session is not yet known inside the local DB snapshot.",
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


def write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle(
    result: Topix100PrevOpenVsOpenEntryExitProfitResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_PREV_OPEN_VS_OPEN_ENTRY_EXIT_PROFIT_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_prev_open_vs_open_entry_exit_profit_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes_list": list(result.interval_minutes_list),
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
            "round_trip_cost_bps": result.round_trip_cost_bps,
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


def load_topix100_prev_open_vs_open_entry_exit_profit_research_bundle(
    bundle_path: str | Path,
) -> Topix100PrevOpenVsOpenEntryExitProfitResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_prev_open_vs_open_entry_exit_profit_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_PREV_OPEN_VS_OPEN_ENTRY_EXIT_PROFIT_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_prev_open_vs_open_entry_exit_profit_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_PREV_OPEN_VS_OPEN_ENTRY_EXIT_PROFIT_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
