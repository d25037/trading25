"""
TOPIX100 session-boundary volume ratio conditioning research.

This study reads minute bars from stock_data_minute_raw, resamples them into
N-minute buckets, compares session-boundary auction-like buckets, then groups
the resulting volume ratios into approximately quartile-style buckets inside
each half-year window.

The goal is to compare:

- same-day intraday return: open -> close
- overnight return: close -> next open
- next-session intraday return: next open -> next close
- close -> next close total move

for the current TOPIX100 universe across 5-minute, 15-minute, and 30-minute
bars. The underlying ratio signals are causal, but the half-year Q1-Q4 bucket
labels are descriptive ex-post groups rather than live-tradable thresholds.
"""

from __future__ import annotations
from collections.abc import Sequence
from dataclasses import dataclass
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
from src.domains.analytics.topix100_open_relative_intraday_path import (
    DEFAULT_INTERVAL_MINUTES,
    SourceMode,
    _fetch_available_date_range,
    _fetch_topix100_constituent_count,
    _open_analysis_connection,
    _query_resampled_topix100_intraday_bars_from_connection,
)

TOPIX100_OPEN_CLOSE_VOLUME_RATIO_CONDITIONING_EXPERIMENT_ID = (
    "market-behavior/topix100-open-close-volume-ratio-conditioning"
)
DEFAULT_BUCKET_COUNT = 4
DEFAULT_PERIOD_MONTHS = 6
DEFAULT_RATIO_MODE = "same_session_close_vs_open"
RatioMode = Literal[
    "same_session_close_vs_open",
    "previous_open_vs_open",
    "previous_close_vs_open",
    "previous_close_vs_close",
]

_PERIOD_COLUMNS: tuple[str, ...] = (
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
)
_SESSION_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "date",
    "code",
    "reference_date",
    "reference_bucket_time",
    "comparison_date",
    "comparison_bucket_time",
    "reference_volume",
    "comparison_volume",
    "comparison_to_reference_volume_ratio",
    "day_open",
    "day_close",
    "same_day_intraday_return",
    "next_session_date",
    "next_session_open",
    "next_session_close",
    "overnight_return",
    "next_session_intraday_return",
    "close_to_next_close_return",
    "ratio_bucket_index",
    "ratio_bucket_label",
)
_PERIOD_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
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
    "reference_volume_mean",
    "comparison_volume_mean",
    "same_day_intraday_mean",
    "same_day_intraday_median",
    "same_day_intraday_hit_positive",
    "overnight_mean",
    "overnight_median",
    "overnight_hit_positive",
    "next_session_intraday_mean",
    "next_session_intraday_median",
    "next_session_intraday_hit_positive",
    "close_to_next_close_mean",
    "close_to_next_close_median",
    "close_to_next_close_hit_positive",
    "next_session_sample_count",
)
_INTERVAL_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
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
    "reference_volume_mean",
    "comparison_volume_mean",
    "same_day_intraday_mean",
    "same_day_intraday_median",
    "same_day_intraday_hit_positive",
    "overnight_mean",
    "overnight_median",
    "overnight_hit_positive",
    "next_session_intraday_mean",
    "next_session_intraday_median",
    "next_session_intraday_hit_positive",
    "close_to_next_close_mean",
    "close_to_next_close_median",
    "close_to_next_close_hit_positive",
    "next_session_sample_count",
)
_PERIOD_INTERVAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "sample_count",
    "next_session_sample_count",
    "low_ratio_bucket_label",
    "high_ratio_bucket_label",
    "low_ratio_mean",
    "high_ratio_mean",
    "ratio_mean_spread_high_minus_low",
    "low_same_day_intraday_mean",
    "high_same_day_intraday_mean",
    "same_day_intraday_mean_spread_high_minus_low",
    "low_overnight_mean",
    "high_overnight_mean",
    "overnight_mean_spread_high_minus_low",
    "low_next_session_intraday_mean",
    "high_next_session_intraday_mean",
    "next_session_intraday_mean_spread_high_minus_low",
    "low_close_to_next_close_mean",
    "high_close_to_next_close_mean",
    "close_to_next_close_mean_spread_high_minus_low",
)
_INTERVAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "period_count",
    "sample_count",
    "next_session_sample_count",
    "low_ratio_bucket_label",
    "high_ratio_bucket_label",
    "low_ratio_mean",
    "high_ratio_mean",
    "ratio_mean_spread_high_minus_low",
    "low_same_day_intraday_mean",
    "high_same_day_intraday_mean",
    "same_day_intraday_mean_spread_high_minus_low",
    "low_overnight_mean",
    "high_overnight_mean",
    "overnight_mean_spread_high_minus_low",
    "low_next_session_intraday_mean",
    "high_next_session_intraday_mean",
    "next_session_intraday_mean_spread_high_minus_low",
    "low_close_to_next_close_mean",
    "high_close_to_next_close_mean",
    "close_to_next_close_mean_spread_high_minus_low",
)


@dataclass(frozen=True)
class Topix100OpenCloseVolumeRatioConditioningResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes_list: tuple[int, ...]
    ratio_mode: RatioMode
    bucket_count: int
    period_months: int
    topix100_constituent_count: int
    total_session_count: int
    periods_df: pd.DataFrame
    session_level_df: pd.DataFrame
    period_bucket_summary_df: pd.DataFrame
    interval_bucket_summary_df: pd.DataFrame
    period_interval_summary_df: pd.DataFrame
    interval_summary_df: pd.DataFrame


def _empty_session_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SESSION_LEVEL_COLUMNS))


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
        return DEFAULT_INTERVAL_MINUTES
    normalized = tuple(
        sorted(dict.fromkeys(int(value) for value in values if int(value) > 0))
    )
    if not normalized:
        raise ValueError("interval_minutes_list must contain at least one positive integer")
    return normalized


def _validate_bucket_count(value: int) -> int:
    bucket_count = int(value)
    if bucket_count < 2:
        raise ValueError("bucket_count must be at least 2")
    return bucket_count


def _validate_period_months(value: int) -> int:
    period_months = int(value)
    if period_months <= 0:
        raise ValueError("period_months must be positive")
    return period_months


def _validate_ratio_mode(value: str) -> RatioMode:
    if value not in {
        "same_session_close_vs_open",
        "previous_open_vs_open",
        "previous_close_vs_open",
        "previous_close_vs_close",
    }:
        raise ValueError(
            "ratio_mode must be one of 'same_session_close_vs_open', "
            "'previous_open_vs_open', 'previous_close_vs_open', or "
            "'previous_close_vs_close'"
        )
    return cast(RatioMode, value)


def _ratio_mode_definition(ratio_mode: RatioMode) -> str:
    if ratio_mode == "same_session_close_vs_open":
        return "closing bucket volume / opening bucket volume"
    if ratio_mode == "previous_open_vs_open":
        return "current opening bucket volume / previous session opening bucket volume"
    if ratio_mode == "previous_close_vs_open":
        return "current opening bucket volume / previous session closing bucket volume"
    return "current closing bucket volume / previous session closing bucket volume"


def _ratio_mode_label(ratio_mode: RatioMode) -> str:
    if ratio_mode == "same_session_close_vs_open":
        return "Same-session close vs open"
    if ratio_mode == "previous_open_vs_open":
        return "Previous open vs current open"
    if ratio_mode == "previous_close_vs_open":
        return "Previous close vs current open"
    return "Previous close vs current close"


def _same_day_intraday_enabled(ratio_mode: RatioMode) -> bool:
    return ratio_mode != "previous_close_vs_close"


def _close_to_next_close_enabled(ratio_mode: RatioMode) -> bool:
    return ratio_mode != "previous_close_vs_close"


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


def _assign_quantile_bucket(series: pd.Series, *, bucket_count: int) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    rank_pct = valid.rank(method="first", pct=True)
    bucket = (rank_pct * bucket_count).apply(np.ceil).clip(1, bucket_count)
    return cast(pd.Series, bucket.astype(int))


def _build_session_level_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
    ratio_mode: RatioMode,
) -> pd.DataFrame:
    if bars_df.empty:
        return _empty_session_level_df()

    working_df = bars_df.sort_values(["date", "code", "bucket_minute"]).copy()
    working_df["bar_index"] = working_df.groupby(["date", "code"]).cumcount()

    first_df = working_df.loc[
        working_df["bar_index"] == 0,
        ["date", "code", "bucket_time", "volume", "day_open"],
    ].rename(
        columns={
            "bucket_time": "first_bucket_time",
            "volume": "opening_volume",
        }
    )
    close_df = (
        working_df.groupby(["date", "code"], as_index=False)
        .tail(1)
        .loc[:, ["date", "code", "bucket_time", "volume", "close"]]
        .rename(
            columns={
                "bucket_time": "close_bucket_time",
                "volume": "closing_volume",
                "close": "day_close",
            }
        )
    )
    day_level_df = first_df.merge(close_df, on=["date", "code"], how="inner")
    day_level_df = day_level_df.loc[
        (day_level_df["opening_volume"].astype(float) > 0)
        & (day_level_df["closing_volume"].astype(float) > 0)
    ].copy()
    if day_level_df.empty:
        return _empty_session_level_df()

    day_level_df["same_day_intraday_return"] = (
        day_level_df["day_close"] / day_level_df["day_open"] - 1.0
    )

    day_level_df = day_level_df.sort_values(["code", "date"]).reset_index(drop=True)
    grouped = day_level_df.groupby("code", sort=False)
    day_level_df["next_session_date"] = grouped["date"].shift(-1)
    day_level_df["next_session_open"] = grouped["day_open"].shift(-1)
    day_level_df["next_session_close"] = grouped["day_close"].shift(-1)
    day_level_df["overnight_return"] = (
        day_level_df["next_session_open"] / day_level_df["day_close"].replace(0, pd.NA) - 1.0
    )
    day_level_df["next_session_intraday_return"] = (
        day_level_df["next_session_close"]
        / day_level_df["next_session_open"].replace(0, pd.NA)
        - 1.0
    )
    day_level_df["close_to_next_close_return"] = (
        day_level_df["next_session_close"] / day_level_df["day_close"].replace(0, pd.NA) - 1.0
    )

    if ratio_mode == "same_session_close_vs_open":
        day_level_df["reference_date"] = day_level_df["date"]
        day_level_df["reference_bucket_time"] = day_level_df["first_bucket_time"]
        day_level_df["comparison_date"] = day_level_df["date"]
        day_level_df["comparison_bucket_time"] = day_level_df["close_bucket_time"]
        day_level_df["reference_volume"] = day_level_df["opening_volume"]
        day_level_df["comparison_volume"] = day_level_df["closing_volume"]
    elif ratio_mode == "previous_open_vs_open":
        day_level_df["reference_date"] = grouped["date"].shift(1)
        day_level_df["reference_bucket_time"] = grouped["first_bucket_time"].shift(1)
        day_level_df["comparison_date"] = day_level_df["date"]
        day_level_df["comparison_bucket_time"] = day_level_df["first_bucket_time"]
        day_level_df["reference_volume"] = grouped["opening_volume"].shift(1)
        day_level_df["comparison_volume"] = day_level_df["opening_volume"]
    elif ratio_mode == "previous_close_vs_open":
        day_level_df["reference_date"] = grouped["date"].shift(1)
        day_level_df["reference_bucket_time"] = grouped["close_bucket_time"].shift(1)
        day_level_df["comparison_date"] = day_level_df["date"]
        day_level_df["comparison_bucket_time"] = day_level_df["first_bucket_time"]
        day_level_df["reference_volume"] = grouped["closing_volume"].shift(1)
        day_level_df["comparison_volume"] = day_level_df["opening_volume"]
    else:
        day_level_df["reference_date"] = grouped["date"].shift(1)
        day_level_df["reference_bucket_time"] = grouped["close_bucket_time"].shift(1)
        day_level_df["comparison_date"] = day_level_df["date"]
        day_level_df["comparison_bucket_time"] = day_level_df["close_bucket_time"]
        day_level_df["reference_volume"] = grouped["closing_volume"].shift(1)
        day_level_df["comparison_volume"] = day_level_df["closing_volume"]

    session_level_df = day_level_df.loc[
        pd.to_numeric(day_level_df["reference_volume"], errors="coerce").fillna(0) > 0
    ].copy()
    if session_level_df.empty:
        return _empty_session_level_df()

    session_level_df["comparison_to_reference_volume_ratio"] = (
        session_level_df["comparison_volume"]
        / session_level_df["reference_volume"].replace(0, pd.NA)
    )
    if not _same_day_intraday_enabled(ratio_mode):
        session_level_df["same_day_intraday_return"] = pd.NA
    if not _close_to_next_close_enabled(ratio_mode):
        session_level_df["close_to_next_close_return"] = pd.NA
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
    group_columns = ["interval_minutes", "period_index"]
    for _, group_df in session_level_df.groupby(group_columns, sort=True):
        scoped_df = group_df.copy().sort_values(["date", "code"]).reset_index(drop=True)
        effective_bucket_count = min(bucket_count, len(scoped_df))
        if effective_bucket_count <= 1:
            scoped_df["ratio_bucket_index"] = 1
            scoped_df["ratio_bucket_label"] = "Q1"
            bucketed_frames.append(scoped_df)
            continue

        scoped_df["ratio_bucket_index"] = _assign_quantile_bucket(
            scoped_df["comparison_to_reference_volume_ratio"],
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


def _summarize_return_series(values: pd.Series) -> tuple[float | None, float | None, float | None, int]:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None, None, None, 0
    return (
        float(valid.mean()),
        float(valid.median()),
        float((valid > 0).mean()),
        int(len(valid)),
    )


def _build_bucket_summary_rows(
    session_level_df: pd.DataFrame,
    *,
    include_period_columns: bool,
) -> list[dict[str, Any]]:
    if session_level_df.empty:
        return []

    group_columns = ["interval_minutes"]
    total_group_columns = ["interval_minutes"]
    if include_period_columns:
        group_columns.extend(
            ["period_index", "period_label", "period_start_date", "period_end_date"]
        )
        total_group_columns.extend(
            ["period_index", "period_label", "period_start_date", "period_end_date"]
        )
    group_columns.extend(["ratio_bucket_index", "ratio_bucket_label"])

    group_sizes = (
        session_level_df.groupby(total_group_columns, as_index=False)
        .agg(total_sample_count=("code", "size"))
    )
    size_lookup = {
        tuple(row[column] for column in total_group_columns): int(row["total_sample_count"])
        for row in group_sizes.to_dict(orient="records")
    }

    rows: list[dict[str, Any]] = []
    for group_key, bucket_df in session_level_df.groupby(group_columns, sort=True):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        key_map = dict(zip(group_columns, group_key, strict=True))
        total_lookup_key = tuple(key_map[column] for column in total_group_columns)
        total_sample_count = size_lookup[total_lookup_key]

        same_day_mean, same_day_median, same_day_hit, _ = _summarize_return_series(
            bucket_df["same_day_intraday_return"]
        )
        overnight_mean, overnight_median, overnight_hit, overnight_count = (
            _summarize_return_series(bucket_df["overnight_return"])
        )
        next_day_mean, next_day_median, next_day_hit, next_day_count = (
            _summarize_return_series(bucket_df["next_session_intraday_return"])
        )
        close_to_next_close_mean, close_to_next_close_median, close_to_next_close_hit, close_to_next_close_count = (
            _summarize_return_series(bucket_df["close_to_next_close_return"])
        )
        next_session_sample_count = max(
            overnight_count,
            next_day_count,
            close_to_next_close_count,
        )

        ratio_series = bucket_df["comparison_to_reference_volume_ratio"].astype(float)
        row = {
            "interval_minutes": int(key_map["interval_minutes"]),
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
            "reference_volume_mean": float(bucket_df["reference_volume"].astype(float).mean()),
            "comparison_volume_mean": float(bucket_df["comparison_volume"].astype(float).mean()),
            "same_day_intraday_mean": same_day_mean,
            "same_day_intraday_median": same_day_median,
            "same_day_intraday_hit_positive": same_day_hit,
            "overnight_mean": overnight_mean,
            "overnight_median": overnight_median,
            "overnight_hit_positive": overnight_hit,
            "next_session_intraday_mean": next_day_mean,
            "next_session_intraday_median": next_day_median,
            "next_session_intraday_hit_positive": next_day_hit,
            "close_to_next_close_mean": close_to_next_close_mean,
            "close_to_next_close_median": close_to_next_close_median,
            "close_to_next_close_hit_positive": close_to_next_close_hit,
            "next_session_sample_count": next_session_sample_count,
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
            row.update(
                {
                    "period_count": int(bucket_df["period_index"].nunique()),
                }
            )
        rows.append(row)
    return rows


def _build_period_bucket_summary_df(session_level_df: pd.DataFrame) -> pd.DataFrame:
    rows = _build_bucket_summary_rows(
        session_level_df,
        include_period_columns=True,
    )
    if not rows:
        return _empty_period_bucket_summary_df()
    return pd.DataFrame.from_records(rows, columns=_PERIOD_BUCKET_SUMMARY_COLUMNS)


def _build_interval_bucket_summary_df(session_level_df: pd.DataFrame) -> pd.DataFrame:
    rows = _build_bucket_summary_rows(
        session_level_df,
        include_period_columns=False,
    )
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
    total_next_session_sample_count: int,
) -> dict[str, Any]:
    row = {
        "interval_minutes": int(low_row["interval_minutes"]),
        "sample_count": total_sample_count,
        "next_session_sample_count": total_next_session_sample_count,
        "low_ratio_bucket_label": str(low_row["ratio_bucket_label"]),
        "high_ratio_bucket_label": str(high_row["ratio_bucket_label"]),
        "low_ratio_mean": _coerce_optional_float(low_row["ratio_mean"]),
        "high_ratio_mean": _coerce_optional_float(high_row["ratio_mean"]),
        "ratio_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["ratio_mean"],
            low_row["ratio_mean"],
        ),
        "low_same_day_intraday_mean": _coerce_optional_float(
            low_row["same_day_intraday_mean"]
        ),
        "high_same_day_intraday_mean": _coerce_optional_float(
            high_row["same_day_intraday_mean"]
        ),
        "same_day_intraday_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["same_day_intraday_mean"],
            low_row["same_day_intraday_mean"],
        ),
        "low_overnight_mean": _coerce_optional_float(low_row["overnight_mean"]),
        "high_overnight_mean": _coerce_optional_float(high_row["overnight_mean"]),
        "overnight_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["overnight_mean"],
            low_row["overnight_mean"],
        ),
        "low_next_session_intraday_mean": _coerce_optional_float(
            low_row["next_session_intraday_mean"]
        ),
        "high_next_session_intraday_mean": _coerce_optional_float(
            high_row["next_session_intraday_mean"]
        ),
        "next_session_intraday_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["next_session_intraday_mean"],
            low_row["next_session_intraday_mean"],
        ),
        "low_close_to_next_close_mean": _coerce_optional_float(
            low_row["close_to_next_close_mean"]
        ),
        "high_close_to_next_close_mean": _coerce_optional_float(
            high_row["close_to_next_close_mean"]
        ),
        "close_to_next_close_mean_spread_high_minus_low": _subtract_optional_float(
            high_row["close_to_next_close_mean"],
            low_row["close_to_next_close_mean"],
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
        "period_index",
        "period_label",
        "period_start_date",
        "period_end_date",
    ]
    for _, group_df in period_bucket_summary_df.groupby(group_columns, sort=True):
        sorted_df = group_df.sort_values("ratio_bucket_index").reset_index(drop=True)
        if len(sorted_df) < 2:
            continue
        rows.append(
            _build_spread_summary_row(
                sorted_df.iloc[0],
                sorted_df.iloc[-1],
                include_period_columns=True,
                total_sample_count=int(group_df["sample_count"].sum()),
                total_next_session_sample_count=int(
                    group_df["next_session_sample_count"].sum()
                ),
            )
        )
    if not rows:
        return _empty_period_interval_summary_df()
    return pd.DataFrame.from_records(rows, columns=_PERIOD_INTERVAL_SUMMARY_COLUMNS)


def _build_interval_summary_df(
    interval_bucket_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if interval_bucket_summary_df.empty:
        return _empty_interval_summary_df()

    rows: list[dict[str, Any]] = []
    for _, group_df in interval_bucket_summary_df.groupby("interval_minutes", sort=True):
        sorted_df = group_df.sort_values("ratio_bucket_index").reset_index(drop=True)
        if len(sorted_df) < 2:
            continue
        rows.append(
            _build_spread_summary_row(
                sorted_df.iloc[0],
                sorted_df.iloc[-1],
                include_period_columns=False,
                period_count=int(sorted_df["period_count"].max()),
                total_sample_count=int(group_df["sample_count"].sum()),
                total_next_session_sample_count=int(
                    group_df["next_session_sample_count"].sum()
                ),
            )
        )
    if not rows:
        return _empty_interval_summary_df()
    return pd.DataFrame.from_records(rows, columns=_INTERVAL_SUMMARY_COLUMNS)


def run_topix100_open_close_volume_ratio_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes_list: Sequence[int] | None = None,
    ratio_mode: str = DEFAULT_RATIO_MODE,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    period_months: int = DEFAULT_PERIOD_MONTHS,
) -> Topix100OpenCloseVolumeRatioConditioningResult:
    validated_intervals = _normalize_interval_minutes(interval_minutes_list)
    validated_ratio_mode = _validate_ratio_mode(ratio_mode)
    validated_bucket_count = _validate_bucket_count(bucket_count)
    validated_period_months = _validate_period_months(period_months)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_available_date_range(conn)
        topix100_constituent_count = _fetch_topix100_constituent_count(conn)
        analysis_start_date, analysis_end_date = _resolve_analysis_range(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            start_date=start_date,
            end_date=end_date,
        )
        periods_df = _build_periods(
            start_date=analysis_start_date,
            end_date=analysis_end_date,
            period_months=validated_period_months,
        )

        session_frames: list[pd.DataFrame] = []
        total_session_count = 0
        for interval_minutes in validated_intervals:
            bars_df = _query_resampled_topix100_intraday_bars_from_connection(
                conn,
                interval_minutes=interval_minutes,
                start_date=analysis_start_date,
                end_date=analysis_end_date,
            )
            interval_session_df = _build_session_level_df(
                bars_df,
                interval_minutes=interval_minutes,
                ratio_mode=validated_ratio_mode,
            )
            interval_session_df = _assign_periods_to_sessions(
                interval_session_df,
                periods_df=periods_df,
            )
            session_frames.append(interval_session_df)
            total_session_count = max(total_session_count, int(len(interval_session_df)))

    session_level_df = (
        pd.concat(session_frames, ignore_index=True)
        if session_frames
        else _empty_session_level_df()
    )
    if session_level_df.empty:
        raise ValueError(
            "No TOPIX100 sessions had both an opening bucket and a closing bucket in the selected range."
        )

    session_level_df = _assign_ratio_buckets(
        session_level_df,
        bucket_count=validated_bucket_count,
    )
    period_bucket_summary_df = _build_period_bucket_summary_df(session_level_df)
    interval_bucket_summary_df = _build_interval_bucket_summary_df(session_level_df)
    period_interval_summary_df = _build_period_interval_summary_df(period_bucket_summary_df)
    interval_summary_df = _build_interval_summary_df(interval_bucket_summary_df)

    return Topix100OpenCloseVolumeRatioConditioningResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        interval_minutes_list=validated_intervals,
        ratio_mode=validated_ratio_mode,
        bucket_count=validated_bucket_count,
        period_months=validated_period_months,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        periods_df=periods_df,
        session_level_df=session_level_df,
        period_bucket_summary_df=period_bucket_summary_df,
        interval_bucket_summary_df=interval_bucket_summary_df,
        period_interval_summary_df=period_interval_summary_df,
        interval_summary_df=interval_summary_df,
    )


def _split_result_payload(
    result: Topix100OpenCloseVolumeRatioConditioningResult,
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
            "ratio_mode": result.ratio_mode,
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
        },
        {
            "periods_df": result.periods_df,
            "session_level_df": result.session_level_df,
            "period_bucket_summary_df": result.period_bucket_summary_df,
            "interval_bucket_summary_df": result.interval_bucket_summary_df,
            "period_interval_summary_df": result.period_interval_summary_df,
            "interval_summary_df": result.interval_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100OpenCloseVolumeRatioConditioningResult:
    return Topix100OpenCloseVolumeRatioConditioningResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes_list=tuple(int(value) for value in metadata["interval_minutes_list"]),
        ratio_mode=_validate_ratio_mode(
            cast(str, metadata.get("ratio_mode", DEFAULT_RATIO_MODE))
        ),
        bucket_count=int(metadata["bucket_count"]),
        period_months=int(metadata["period_months"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        periods_df=tables["periods_df"],
        session_level_df=tables["session_level_df"],
        period_bucket_summary_df=tables["period_bucket_summary_df"],
        interval_bucket_summary_df=tables["interval_bucket_summary_df"],
        period_interval_summary_df=tables["period_interval_summary_df"],
        interval_summary_df=tables["interval_summary_df"],
    )


def _build_published_summary(
    result: Topix100OpenCloseVolumeRatioConditioningResult,
) -> dict[str, Any]:
    return {
        "intervalMinutesList": list(result.interval_minutes_list),
        "ratioMode": result.ratio_mode,
        "bucketCount": result.bucket_count,
        "periodMonths": result.period_months,
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
    result: Topix100OpenCloseVolumeRatioConditioningResult,
) -> str:
    lines = [
        "# TOPIX100 Session-Boundary Volume Ratio Conditioning",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{', '.join(str(value) for value in result.interval_minutes_list)}`",
        f"- Ratio mode: `{result.ratio_mode}` ({_ratio_mode_label(result.ratio_mode)})",
        f"- Ratio definition: `{_ratio_mode_definition(result.ratio_mode)}`",
        f"- Bucket count: `{result.bucket_count}`",
        f"- Period months: `{result.period_months}`",
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
            metric_parts: list[str] = []
            if _same_day_intraday_enabled(result.ratio_mode):
                metric_parts.append(
                    "same-day intraday spread "
                    f"`{_format_optional_pct(row.same_day_intraday_mean_spread_high_minus_low)}`"
                )
            metric_parts.append(
                f"overnight spread `{_format_optional_pct(row.overnight_mean_spread_high_minus_low)}`"
            )
            metric_parts.append(
                "next-session intraday spread "
                f"`{_format_optional_pct(row.next_session_intraday_mean_spread_high_minus_low)}`"
            )
            if _close_to_next_close_enabled(result.ratio_mode):
                metric_parts.append(
                    "close->next close spread "
                    f"`{_format_optional_pct(row.close_to_next_close_mean_spread_high_minus_low)}`"
                )
            lines.append(
                f"- `{row.interval_minutes}m`: `{row.high_ratio_bucket_label}` minus "
                f"`{row.low_ratio_bucket_label}` " + ", ".join(metric_parts) + "."
            )
    lines.extend(
        [
            "",
            "## Caveat",
            "",
            "- This study uses the current TOPIX100 constituent set available in `stocks.scale_category` for the minute-bar join. Treat the result as a current-universe retrospective study, not a point-in-time historical membership reconstruction.",
            "- The half-year Q1-Q4 buckets are descriptive ex-post groupings built from the realized ratio distribution inside each period. Use them for research comparison, not as-is for live thresholding.",
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


def write_topix100_open_close_volume_ratio_conditioning_research_bundle(
    result: Topix100OpenCloseVolumeRatioConditioningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_OPEN_CLOSE_VOLUME_RATIO_CONDITIONING_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_open_close_volume_ratio_conditioning_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes_list": list(result.interval_minutes_list),
            "ratio_mode": result.ratio_mode,
            "bucket_count": result.bucket_count,
            "period_months": result.period_months,
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


def load_topix100_open_close_volume_ratio_conditioning_research_bundle(
    bundle_path: str | Path,
) -> Topix100OpenCloseVolumeRatioConditioningResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_open_close_volume_ratio_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_OPEN_CLOSE_VOLUME_RATIO_CONDITIONING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_open_close_volume_ratio_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_OPEN_CLOSE_VOLUME_RATIO_CONDITIONING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
