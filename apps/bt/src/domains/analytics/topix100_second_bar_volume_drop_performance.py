"""
TOPIX100 second-bar volume-drop performance research.

This study asks a narrower intraday question on the current TOPIX100 universe:

- how quickly does volume fade from the opening bar into the next bar?
- if the next bar volume collapses unusually hard, does the stock underperform
  through the rest of the day?

The research defines "sharp volume drop" from the empirical cross-sectional
distribution of:

    second_bar_volume / opening_bar_volume

for each intraday interval independently.
"""

from __future__ import annotations

import importlib
import os
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

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

SECOND_BAR_VOLUME_DROP_PERFORMANCE_EXPERIMENT_ID = (
    "market-behavior/topix100-second-bar-volume-drop-performance"
)
SECOND_BAR_VOLUME_DROP_OVERVIEW_PLOT_FILENAME = (
    "second_bar_volume_drop_overview.png"
)
DEFAULT_DROP_PERCENTILE = 0.20
DEFAULT_RATIO_BUCKET_COUNT = 10

_SESSION_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "date",
    "code",
    "first_bucket_time",
    "second_bucket_time",
    "close_bucket_time",
    "first_volume",
    "second_volume",
    "second_to_first_volume_ratio",
    "open_to_second_return",
    "open_to_close_return",
    "second_to_close_return",
)
_DISTRIBUTION_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "sample_count",
    "threshold_percentile",
    "threshold_ratio",
    "mean_ratio",
    "std_ratio",
    "min_ratio",
    "p01_ratio",
    "p05_ratio",
    "p10_ratio",
    "p20_ratio",
    "p25_ratio",
    "p50_ratio",
    "p75_ratio",
    "p80_ratio",
    "p90_ratio",
    "p95_ratio",
    "p99_ratio",
    "max_ratio",
    "share_ratio_le_025",
    "share_ratio_le_050",
    "share_ratio_le_100",
)
_RATIO_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "ratio_bucket_index",
    "ratio_bucket_label",
    "sample_count",
    "sample_share",
    "ratio_min",
    "ratio_max",
    "ratio_mean",
    "open_to_second_mean",
    "open_to_second_median",
    "open_to_close_mean",
    "open_to_close_median",
    "open_to_close_hit_positive",
    "second_to_close_mean",
    "second_to_close_median",
    "second_to_close_hit_positive",
)
_GROUP_COMPARISON_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "group_key",
    "group_label",
    "sample_count",
    "sample_share",
    "threshold_percentile",
    "threshold_ratio",
    "ratio_mean",
    "ratio_median",
    "open_to_second_mean",
    "open_to_second_median",
    "open_to_close_mean",
    "open_to_close_median",
    "open_to_close_hit_positive",
    "second_to_close_mean",
    "second_to_close_median",
    "second_to_close_hit_positive",
)
_INTERVAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "sample_count",
    "stock_count",
    "threshold_percentile",
    "threshold_ratio",
    "sharp_drop_count",
    "sharp_drop_share",
    "non_sharp_drop_count",
    "non_sharp_drop_share",
    "sharp_drop_open_to_close_mean",
    "non_sharp_drop_open_to_close_mean",
    "open_to_close_mean_spread",
    "sharp_drop_open_to_close_hit_positive",
    "non_sharp_drop_open_to_close_hit_positive",
    "open_to_close_hit_positive_spread",
    "open_to_close_welch_t_stat",
    "open_to_close_welch_p_value",
    "sharp_drop_second_to_close_mean",
    "non_sharp_drop_second_to_close_mean",
    "second_to_close_mean_spread",
    "sharp_drop_second_to_close_hit_positive",
    "non_sharp_drop_second_to_close_hit_positive",
    "second_to_close_hit_positive_spread",
    "second_to_close_welch_t_stat",
    "second_to_close_welch_p_value",
)


@dataclass(frozen=True)
class Topix100SecondBarVolumeDropPerformanceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes_list: tuple[int, ...]
    drop_percentile: float
    topix100_constituent_count: int
    total_session_count: int
    session_level_df: pd.DataFrame
    distribution_summary_df: pd.DataFrame
    ratio_bucket_summary_df: pd.DataFrame
    group_comparison_df: pd.DataFrame
    interval_summary_df: pd.DataFrame


def _empty_session_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SESSION_LEVEL_COLUMNS))


def _empty_distribution_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_DISTRIBUTION_SUMMARY_COLUMNS))


def _empty_ratio_bucket_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_RATIO_BUCKET_SUMMARY_COLUMNS))


def _empty_group_comparison_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_GROUP_COMPARISON_COLUMNS))


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


def _validate_drop_percentile(value: float) -> float:
    percentile = float(value)
    if percentile <= 0 or percentile >= 1:
        raise ValueError("drop_percentile must be between 0 and 1 (exclusive)")
    return percentile


def _coerce_optional_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _safe_welch_t_test(
    left: pd.Series,
    right: pd.Series,
) -> tuple[float | None, float | None]:
    if len(left) < 2 or len(right) < 2:
        return None, None
    scipy_stats = importlib.import_module("scipy.stats")
    statistic, p_value = scipy_stats.ttest_ind(
        left.to_numpy(dtype=float),
        right.to_numpy(dtype=float),
        equal_var=False,
        nan_policy="omit",
    )
    return _coerce_optional_float(statistic), _coerce_optional_float(p_value)


def _build_session_level_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
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
            "volume": "first_volume",
        }
    )
    second_df = working_df.loc[
        working_df["bar_index"] == 1,
        ["date", "code", "bucket_time", "volume", "close"],
    ].rename(
        columns={
            "bucket_time": "second_bucket_time",
            "volume": "second_volume",
            "close": "second_close",
        }
    )
    close_df = (
        working_df.groupby(["date", "code"], as_index=False)
        .tail(1)
        .loc[:, ["date", "code", "bucket_time", "close"]]
        .rename(
            columns={
                "bucket_time": "close_bucket_time",
                "close": "day_close",
            }
        )
    )
    session_level_df = first_df.merge(second_df, on=["date", "code"], how="inner").merge(
        close_df,
        on=["date", "code"],
        how="inner",
    )
    session_level_df = session_level_df.loc[
        session_level_df["first_volume"].astype(float) > 0
    ].copy()
    if session_level_df.empty:
        return _empty_session_level_df()

    session_level_df["second_to_first_volume_ratio"] = (
        session_level_df["second_volume"] / session_level_df["first_volume"]
    )
    session_level_df["open_to_second_return"] = (
        session_level_df["second_close"] / session_level_df["day_open"] - 1.0
    )
    session_level_df["open_to_close_return"] = (
        session_level_df["day_close"] / session_level_df["day_open"] - 1.0
    )
    session_level_df["second_to_close_return"] = (
        session_level_df["day_close"] / session_level_df["second_close"] - 1.0
    )
    session_level_df.insert(0, "interval_minutes", interval_minutes)
    return session_level_df.loc[:, list(_SESSION_LEVEL_COLUMNS)].copy()


def _build_distribution_summary_row(
    session_level_df: pd.DataFrame,
    *,
    interval_minutes: int,
    drop_percentile: float,
) -> dict[str, Any]:
    if session_level_df.empty:
        return {
            "interval_minutes": interval_minutes,
            "sample_count": 0,
            "threshold_percentile": drop_percentile,
            "threshold_ratio": None,
            "mean_ratio": None,
            "std_ratio": None,
            "min_ratio": None,
            "p01_ratio": None,
            "p05_ratio": None,
            "p10_ratio": None,
            "p20_ratio": None,
            "p25_ratio": None,
            "p50_ratio": None,
            "p75_ratio": None,
            "p80_ratio": None,
            "p90_ratio": None,
            "p95_ratio": None,
            "p99_ratio": None,
            "max_ratio": None,
            "share_ratio_le_025": None,
            "share_ratio_le_050": None,
            "share_ratio_le_100": None,
        }

    ratio_series = session_level_df["second_to_first_volume_ratio"].astype(float)
    quantiles = ratio_series.quantile(
        [0.01, 0.05, 0.10, 0.20, 0.25, 0.50, 0.75, 0.80, 0.90, 0.95, 0.99]
    )
    return {
        "interval_minutes": interval_minutes,
        "sample_count": int(len(session_level_df)),
        "threshold_percentile": drop_percentile,
        "threshold_ratio": float(ratio_series.quantile(drop_percentile)),
        "mean_ratio": float(ratio_series.mean()),
        "std_ratio": float(ratio_series.std(ddof=1)),
        "min_ratio": float(ratio_series.min()),
        "p01_ratio": float(quantiles.loc[0.01]),
        "p05_ratio": float(quantiles.loc[0.05]),
        "p10_ratio": float(quantiles.loc[0.10]),
        "p20_ratio": float(quantiles.loc[0.20]),
        "p25_ratio": float(quantiles.loc[0.25]),
        "p50_ratio": float(quantiles.loc[0.50]),
        "p75_ratio": float(quantiles.loc[0.75]),
        "p80_ratio": float(quantiles.loc[0.80]),
        "p90_ratio": float(quantiles.loc[0.90]),
        "p95_ratio": float(quantiles.loc[0.95]),
        "p99_ratio": float(quantiles.loc[0.99]),
        "max_ratio": float(ratio_series.max()),
        "share_ratio_le_025": float((ratio_series <= 0.25).mean()),
        "share_ratio_le_050": float((ratio_series <= 0.50).mean()),
        "share_ratio_le_100": float((ratio_series <= 1.00).mean()),
    }


def _build_ratio_bucket_summary_df(
    session_level_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_ratio_bucket_summary_df()

    working_df = session_level_df.copy()
    bucket_count = min(DEFAULT_RATIO_BUCKET_COUNT, len(working_df))
    if bucket_count <= 1:
        ratio_value = float(working_df["second_to_first_volume_ratio"].iloc[0])
        return pd.DataFrame.from_records(
            [
                {
                    "interval_minutes": interval_minutes,
                    "ratio_bucket_index": 1,
                    "ratio_bucket_label": "D1",
                    "sample_count": 1,
                    "sample_share": 1.0,
                    "ratio_min": ratio_value,
                    "ratio_max": ratio_value,
                    "ratio_mean": ratio_value,
                    "open_to_second_mean": float(working_df["open_to_second_return"].iloc[0]),
                    "open_to_second_median": float(working_df["open_to_second_return"].iloc[0]),
                    "open_to_close_mean": float(working_df["open_to_close_return"].iloc[0]),
                    "open_to_close_median": float(working_df["open_to_close_return"].iloc[0]),
                    "open_to_close_hit_positive": float(
                        working_df["open_to_close_return"].iloc[0] > 0
                    ),
                    "second_to_close_mean": float(
                        working_df["second_to_close_return"].iloc[0]
                    ),
                    "second_to_close_median": float(
                        working_df["second_to_close_return"].iloc[0]
                    ),
                    "second_to_close_hit_positive": float(
                        working_df["second_to_close_return"].iloc[0] > 0
                    ),
                }
            ],
            columns=_RATIO_BUCKET_SUMMARY_COLUMNS,
        )

    working_df["ratio_bucket_index"] = pd.qcut(
        working_df["second_to_first_volume_ratio"],
        q=bucket_count,
        labels=False,
        duplicates="drop",
    )
    working_df = working_df.loc[working_df["ratio_bucket_index"].notna()].copy()
    working_df["ratio_bucket_index"] = working_df["ratio_bucket_index"].astype(int) + 1
    total_count = len(working_df)
    summary_df = (
        working_df.groupby("ratio_bucket_index", as_index=False)
        .agg(
            sample_count=("code", "size"),
            ratio_min=("second_to_first_volume_ratio", "min"),
            ratio_max=("second_to_first_volume_ratio", "max"),
            ratio_mean=("second_to_first_volume_ratio", "mean"),
            open_to_second_mean=("open_to_second_return", "mean"),
            open_to_second_median=("open_to_second_return", "median"),
            open_to_close_mean=("open_to_close_return", "mean"),
            open_to_close_median=("open_to_close_return", "median"),
            open_to_close_hit_positive=(
                "open_to_close_return",
                lambda values: float((values > 0).mean()),
            ),
            second_to_close_mean=("second_to_close_return", "mean"),
            second_to_close_median=("second_to_close_return", "median"),
            second_to_close_hit_positive=(
                "second_to_close_return",
                lambda values: float((values > 0).mean()),
            ),
        )
        .sort_values("ratio_bucket_index")
        .reset_index(drop=True)
    )
    summary_df["ratio_bucket_label"] = summary_df["ratio_bucket_index"].map(
        lambda value: f"D{int(value)}"
    )
    summary_df["sample_share"] = summary_df["sample_count"] / float(total_count)
    summary_df.insert(0, "interval_minutes", interval_minutes)
    return summary_df.loc[:, list(_RATIO_BUCKET_SUMMARY_COLUMNS)].copy()


def _build_group_comparison_df(
    session_level_df: pd.DataFrame,
    *,
    interval_minutes: int,
    drop_percentile: float,
    threshold_ratio: float,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_group_comparison_df()

    working_df = session_level_df.copy()
    working_df["group_key"] = working_df["second_to_first_volume_ratio"].le(
        threshold_ratio
    ).map({True: "sharp_drop", False: "non_sharp_drop"})
    working_df["group_label"] = working_df["group_key"].map(
        {
            "sharp_drop": "Sharp drop",
            "non_sharp_drop": "Non-sharp drop",
        }
    )
    total_count = len(working_df)
    summary_df = (
        working_df.groupby(["group_key", "group_label"], as_index=False)
        .agg(
            sample_count=("code", "size"),
            ratio_mean=("second_to_first_volume_ratio", "mean"),
            ratio_median=("second_to_first_volume_ratio", "median"),
            open_to_second_mean=("open_to_second_return", "mean"),
            open_to_second_median=("open_to_second_return", "median"),
            open_to_close_mean=("open_to_close_return", "mean"),
            open_to_close_median=("open_to_close_return", "median"),
            open_to_close_hit_positive=(
                "open_to_close_return",
                lambda values: float((values > 0).mean()),
            ),
            second_to_close_mean=("second_to_close_return", "mean"),
            second_to_close_median=("second_to_close_return", "median"),
            second_to_close_hit_positive=(
                "second_to_close_return",
                lambda values: float((values > 0).mean()),
            ),
        )
        .reset_index(drop=True)
    )
    summary_df["sample_share"] = summary_df["sample_count"] / float(total_count)
    summary_df["threshold_percentile"] = drop_percentile
    summary_df["threshold_ratio"] = threshold_ratio
    summary_df.insert(0, "interval_minutes", interval_minutes)
    order_map = {"sharp_drop": 0, "non_sharp_drop": 1}
    summary_df["_group_order"] = summary_df["group_key"].map(order_map)
    summary_df = summary_df.sort_values("_group_order").drop(columns="_group_order")
    return summary_df.loc[:, list(_GROUP_COMPARISON_COLUMNS)].copy()


def _build_interval_summary_row(
    session_level_df: pd.DataFrame,
    group_comparison_df: pd.DataFrame,
    *,
    interval_minutes: int,
    drop_percentile: float,
    threshold_ratio: float | None,
) -> dict[str, Any]:
    if session_level_df.empty or threshold_ratio is None or group_comparison_df.empty:
        return {
            "interval_minutes": interval_minutes,
            "sample_count": 0,
            "stock_count": 0,
            "threshold_percentile": drop_percentile,
            "threshold_ratio": threshold_ratio,
            "sharp_drop_count": 0,
            "sharp_drop_share": 0.0,
            "non_sharp_drop_count": 0,
            "non_sharp_drop_share": 0.0,
            "sharp_drop_open_to_close_mean": None,
            "non_sharp_drop_open_to_close_mean": None,
            "open_to_close_mean_spread": None,
            "sharp_drop_open_to_close_hit_positive": None,
            "non_sharp_drop_open_to_close_hit_positive": None,
            "open_to_close_hit_positive_spread": None,
            "open_to_close_welch_t_stat": None,
            "open_to_close_welch_p_value": None,
            "sharp_drop_second_to_close_mean": None,
            "non_sharp_drop_second_to_close_mean": None,
            "second_to_close_mean_spread": None,
            "sharp_drop_second_to_close_hit_positive": None,
            "non_sharp_drop_second_to_close_hit_positive": None,
            "second_to_close_hit_positive_spread": None,
            "second_to_close_welch_t_stat": None,
            "second_to_close_welch_p_value": None,
        }

    sharp_df = session_level_df.loc[
        session_level_df["second_to_first_volume_ratio"] <= threshold_ratio
    ].copy()
    non_sharp_df = session_level_df.loc[
        session_level_df["second_to_first_volume_ratio"] > threshold_ratio
    ].copy()
    sharp_row = group_comparison_df.loc[
        group_comparison_df["group_key"] == "sharp_drop"
    ].iloc[0]
    non_row = group_comparison_df.loc[
        group_comparison_df["group_key"] == "non_sharp_drop"
    ].iloc[0]
    open_to_close_t_stat, open_to_close_p_value = _safe_welch_t_test(
        sharp_df["open_to_close_return"],
        non_sharp_df["open_to_close_return"],
    )
    second_to_close_t_stat, second_to_close_p_value = _safe_welch_t_test(
        sharp_df["second_to_close_return"],
        non_sharp_df["second_to_close_return"],
    )
    sharp_open_to_close_mean = float(cast(Any, sharp_row)["open_to_close_mean"])
    non_open_to_close_mean = float(cast(Any, non_row)["open_to_close_mean"])
    sharp_second_to_close_mean = float(cast(Any, sharp_row)["second_to_close_mean"])
    non_second_to_close_mean = float(cast(Any, non_row)["second_to_close_mean"])
    sharp_open_hit = float(cast(Any, sharp_row)["open_to_close_hit_positive"])
    non_open_hit = float(cast(Any, non_row)["open_to_close_hit_positive"])
    sharp_second_hit = float(cast(Any, sharp_row)["second_to_close_hit_positive"])
    non_second_hit = float(cast(Any, non_row)["second_to_close_hit_positive"])
    return {
        "interval_minutes": interval_minutes,
        "sample_count": int(len(session_level_df)),
        "stock_count": int(session_level_df["code"].nunique()),
        "threshold_percentile": drop_percentile,
        "threshold_ratio": threshold_ratio,
        "sharp_drop_count": int(len(sharp_df)),
        "sharp_drop_share": float(len(sharp_df) / len(session_level_df)),
        "non_sharp_drop_count": int(len(non_sharp_df)),
        "non_sharp_drop_share": float(len(non_sharp_df) / len(session_level_df)),
        "sharp_drop_open_to_close_mean": sharp_open_to_close_mean,
        "non_sharp_drop_open_to_close_mean": non_open_to_close_mean,
        "open_to_close_mean_spread": sharp_open_to_close_mean - non_open_to_close_mean,
        "sharp_drop_open_to_close_hit_positive": sharp_open_hit,
        "non_sharp_drop_open_to_close_hit_positive": non_open_hit,
        "open_to_close_hit_positive_spread": sharp_open_hit - non_open_hit,
        "open_to_close_welch_t_stat": open_to_close_t_stat,
        "open_to_close_welch_p_value": open_to_close_p_value,
        "sharp_drop_second_to_close_mean": sharp_second_to_close_mean,
        "non_sharp_drop_second_to_close_mean": non_second_to_close_mean,
        "second_to_close_mean_spread": sharp_second_to_close_mean
        - non_second_to_close_mean,
        "sharp_drop_second_to_close_hit_positive": sharp_second_hit,
        "non_sharp_drop_second_to_close_hit_positive": non_second_hit,
        "second_to_close_hit_positive_spread": sharp_second_hit - non_second_hit,
        "second_to_close_welch_t_stat": second_to_close_t_stat,
        "second_to_close_welch_p_value": second_to_close_p_value,
    }


def run_topix100_second_bar_volume_drop_performance_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes_list: Sequence[int] | None = None,
    drop_percentile: float = DEFAULT_DROP_PERCENTILE,
) -> Topix100SecondBarVolumeDropPerformanceResult:
    validated_intervals = _normalize_interval_minutes(interval_minutes_list)
    validated_drop_percentile = _validate_drop_percentile(drop_percentile)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_available_date_range(conn)
        topix100_constituent_count = _fetch_topix100_constituent_count(conn)

        session_level_frames: list[pd.DataFrame] = []
        distribution_summary_rows: list[dict[str, Any]] = []
        ratio_bucket_frames: list[pd.DataFrame] = []
        group_comparison_frames: list[pd.DataFrame] = []
        interval_summary_rows: list[dict[str, Any]] = []
        analysis_start_date: str | None = None
        analysis_end_date: str | None = None
        total_session_count = 0

        for interval_minutes in validated_intervals:
            bars_df = _query_resampled_topix100_intraday_bars_from_connection(
                conn,
                interval_minutes=interval_minutes,
                start_date=start_date,
                end_date=end_date,
            )
            session_level_df = _build_session_level_df(
                bars_df,
                interval_minutes=interval_minutes,
            )
            session_level_frames.append(session_level_df)

            if not session_level_df.empty:
                if analysis_start_date is None:
                    analysis_start_date = str(session_level_df["date"].min())
                if analysis_end_date is None:
                    analysis_end_date = str(session_level_df["date"].max())
                total_session_count = max(total_session_count, int(len(session_level_df)))

            distribution_row = _build_distribution_summary_row(
                session_level_df,
                interval_minutes=interval_minutes,
                drop_percentile=validated_drop_percentile,
            )
            distribution_summary_rows.append(distribution_row)
            threshold_ratio = cast(float | None, distribution_row["threshold_ratio"])

            ratio_bucket_frames.append(
                _build_ratio_bucket_summary_df(
                    session_level_df,
                    interval_minutes=interval_minutes,
                )
            )
            group_comparison_df = (
                _build_group_comparison_df(
                    session_level_df,
                    interval_minutes=interval_minutes,
                    drop_percentile=validated_drop_percentile,
                    threshold_ratio=threshold_ratio,
                )
                if threshold_ratio is not None
                else _empty_group_comparison_df()
            )
            group_comparison_frames.append(group_comparison_df)
            interval_summary_rows.append(
                _build_interval_summary_row(
                    session_level_df,
                    group_comparison_df,
                    interval_minutes=interval_minutes,
                    drop_percentile=validated_drop_percentile,
                    threshold_ratio=threshold_ratio,
                )
            )

    session_level_df = (
        pd.concat(session_level_frames, ignore_index=True)
        if session_level_frames
        else _empty_session_level_df()
    )
    distribution_summary_df = (
        pd.DataFrame.from_records(
            distribution_summary_rows,
            columns=_DISTRIBUTION_SUMMARY_COLUMNS,
        )
        if distribution_summary_rows
        else _empty_distribution_summary_df()
    )
    ratio_bucket_summary_df = (
        pd.concat(ratio_bucket_frames, ignore_index=True)
        if ratio_bucket_frames
        else _empty_ratio_bucket_summary_df()
    )
    group_comparison_df = (
        pd.concat(group_comparison_frames, ignore_index=True)
        if group_comparison_frames
        else _empty_group_comparison_df()
    )
    interval_summary_df = (
        pd.DataFrame.from_records(interval_summary_rows, columns=_INTERVAL_SUMMARY_COLUMNS)
        if interval_summary_rows
        else _empty_interval_summary_df()
    )

    if session_level_df.empty and total_session_count == 0:
        raise ValueError("No TOPIX100 sessions had at least two intraday bars in the selected range.")

    return Topix100SecondBarVolumeDropPerformanceResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        interval_minutes_list=validated_intervals,
        drop_percentile=validated_drop_percentile,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        session_level_df=session_level_df,
        distribution_summary_df=distribution_summary_df,
        ratio_bucket_summary_df=ratio_bucket_summary_df,
        group_comparison_df=group_comparison_df,
        interval_summary_df=interval_summary_df,
    )


def _split_result_payload(
    result: Topix100SecondBarVolumeDropPerformanceResult,
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
            "drop_percentile": result.drop_percentile,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
        },
        {
            "session_level_df": result.session_level_df,
            "distribution_summary_df": result.distribution_summary_df,
            "ratio_bucket_summary_df": result.ratio_bucket_summary_df,
            "group_comparison_df": result.group_comparison_df,
            "interval_summary_df": result.interval_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100SecondBarVolumeDropPerformanceResult:
    return Topix100SecondBarVolumeDropPerformanceResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes_list=tuple(int(value) for value in metadata["interval_minutes_list"]),
        drop_percentile=float(metadata["drop_percentile"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        session_level_df=tables["session_level_df"],
        distribution_summary_df=tables["distribution_summary_df"],
        ratio_bucket_summary_df=tables["ratio_bucket_summary_df"],
        group_comparison_df=tables["group_comparison_df"],
        interval_summary_df=tables["interval_summary_df"],
    )


def _build_published_summary(
    result: Topix100SecondBarVolumeDropPerformanceResult,
) -> dict[str, Any]:
    return {
        "intervalMinutesList": list(result.interval_minutes_list),
        "dropPercentile": result.drop_percentile,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "topix100ConstituentCount": result.topix100_constituent_count,
        "totalSessionCount": result.total_session_count,
        "intervalSummary": result.interval_summary_df.to_dict(orient="records"),
    }


def _build_research_bundle_summary_markdown(
    result: Topix100SecondBarVolumeDropPerformanceResult,
) -> str:
    lines = [
        "# TOPIX100 Second-Bar Volume Drop Performance",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{', '.join(str(value) for value in result.interval_minutes_list)}`",
        f"- Sharp-drop definition: bottom `{result.drop_percentile * 100:.0f}%` of `second bar volume / opening bar volume` for each interval",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Total stock sessions: `{result.total_session_count}`",
        "",
        "## Current Read",
        "",
    ]
    if result.interval_summary_df.empty:
        lines.append("- Interval summary was empty.")
    else:
        for row in result.interval_summary_df.itertuples(index=False):
            if row.threshold_ratio is None:
                lines.append(f"- `{row.interval_minutes}m`: no analyzable rows.")
                continue
            threshold_ratio = float(cast(Any, row.threshold_ratio))
            open_spread = float(cast(Any, row.open_to_close_mean_spread))
            second_spread = float(cast(Any, row.second_to_close_mean_spread))
            lines.append(
                f"- `{row.interval_minutes}m`: sharp drop = ratio `<= {threshold_ratio:.4f}`. "
                f"Same-day open→close spread was `{open_spread * 100:+.4f}%`, "
                f"and second-bar-end→close spread was `{second_spread * 100:+.4f}%` "
                f"(sharp drop minus non-sharp drop)."
            )
    lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{SECOND_BAR_VOLUME_DROP_OVERVIEW_PLOT_FILENAME}`",
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


def _import_matplotlib_pyplot() -> Any:
    mpl_config_dir = Path(tempfile.gettempdir()) / "trading25-matplotlib"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config_dir))
    matplotlib = importlib.import_module("matplotlib")
    use_backend = getattr(matplotlib, "use", None)
    if callable(use_backend):
        use_backend("Agg", force=True)
    return importlib.import_module("matplotlib.pyplot")


def write_topix100_second_bar_volume_drop_overview_plot(
    result: Topix100SecondBarVolumeDropPerformanceResult,
    *,
    output_path: str | Path,
) -> Path:
    if result.session_level_df.empty or result.interval_summary_df.empty:
        raise ValueError("No summary data was available to plot.")

    plt = _import_matplotlib_pyplot()
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(
        len(result.interval_minutes_list),
        2,
        figsize=(14, 4.2 * len(result.interval_minutes_list)),
        constrained_layout=True,
    )
    if len(result.interval_minutes_list) == 1:
        axes = [axes]

    for axis_row, interval_minutes in zip(axes, result.interval_minutes_list, strict=False):
        hist_ax = axis_row[0]
        perf_ax = axis_row[1]
        session_df = result.session_level_df.loc[
            result.session_level_df["interval_minutes"] == interval_minutes
        ].copy()
        interval_row = result.interval_summary_df.loc[
            result.interval_summary_df["interval_minutes"] == interval_minutes
        ].iloc[0]
        group_df = result.group_comparison_df.loc[
            result.group_comparison_df["interval_minutes"] == interval_minutes
        ].copy()
        if session_df.empty:
            hist_ax.set_visible(False)
            perf_ax.set_visible(False)
            continue

        threshold_ratio = float(cast(Any, interval_row["threshold_ratio"]))
        display_max = float(
            max(
                session_df["second_to_first_volume_ratio"].quantile(0.95),
                threshold_ratio * 1.4,
            )
        )
        clipped_ratio = session_df["second_to_first_volume_ratio"].clip(upper=display_max)
        hist_ax.hist(
            clipped_ratio,
            bins=40,
            color="#93c5fd",
            edgecolor="#1d4ed8",
            alpha=0.85,
        )
        hist_ax.axvline(
            threshold_ratio,
            color="#dc2626",
            linestyle="--",
            linewidth=1.6,
            label=f"Sharp-drop cutoff {threshold_ratio:.3f}",
        )
        hist_ax.set_title(f"{interval_minutes}m volume-ratio distribution")
        hist_ax.set_xlabel("Second bar volume / opening bar volume")
        hist_ax.set_ylabel("Session count")
        hist_ax.legend(loc="best", frameon=False)
        hist_ax.grid(axis="y", alpha=0.25, linewidth=0.7)

        perf_plot_df = group_df.copy()
        perf_plot_df["open_to_close_pct"] = perf_plot_df["open_to_close_mean"] * 100.0
        perf_plot_df["second_to_close_pct"] = (
            perf_plot_df["second_to_close_mean"] * 100.0
        )
        x_positions = [0, 1]
        bar_width = 0.36
        perf_ax.bar(
            [value - bar_width / 2 for value in x_positions],
            perf_plot_df["open_to_close_pct"],
            width=bar_width,
            color="#2563eb",
            label="Open → Close mean",
        )
        perf_ax.bar(
            [value + bar_width / 2 for value in x_positions],
            perf_plot_df["second_to_close_pct"],
            width=bar_width,
            color="#059669",
            label="Second bar end → Close mean",
        )
        perf_ax.axhline(0.0, color="#111827", linewidth=1.0, alpha=0.85)
        perf_ax.set_xticks(x_positions)
        perf_ax.set_xticklabels(list(perf_plot_df["group_label"]))
        perf_ax.set_ylabel("Mean return (%)")
        perf_ax.set_title(f"{interval_minutes}m same-day performance split")
        perf_ax.legend(loc="best", frameon=False)
        perf_ax.grid(axis="y", alpha=0.25, linewidth=0.7)

    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_second_bar_volume_drop_performance_research_bundle(
    result: Topix100SecondBarVolumeDropPerformanceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    bundle = write_research_bundle(
        experiment_id=SECOND_BAR_VOLUME_DROP_PERFORMANCE_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_second_bar_volume_drop_performance_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes_list": list(result.interval_minutes_list),
            "drop_percentile": result.drop_percentile,
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
    write_topix100_second_bar_volume_drop_overview_plot(
        result,
        output_path=bundle.bundle_dir / SECOND_BAR_VOLUME_DROP_OVERVIEW_PLOT_FILENAME,
    )
    return bundle


def load_topix100_second_bar_volume_drop_performance_research_bundle(
    bundle_path: str | Path,
) -> Topix100SecondBarVolumeDropPerformanceResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_second_bar_volume_drop_performance_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        SECOND_BAR_VOLUME_DROP_PERFORMANCE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_second_bar_volume_drop_performance_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        SECOND_BAR_VOLUME_DROP_PERFORMANCE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )

