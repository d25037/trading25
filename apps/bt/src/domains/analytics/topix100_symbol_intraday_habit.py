"""
TOPIX100 symbol-level intraday habit research.

This study focuses on a small symbol set drawn from the current TOPIX100
constituents:

- Advantest (6857) is always included.
- Four additional names are selected with deterministic random sampling.
- The available analysis window is split into half-year periods.
- For each symbol and period, the study summarizes the mean close/open path
  through the session and checks whether the sign of each time bucket tends to
  persist across periods.
"""

from __future__ import annotations

import importlib
import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.deterministic_sampling import (
    select_deterministic_samples,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_bundle_artifact,
    write_research_bundle,
)
from src.domains.analytics.topix100_open_relative_intraday_path import (
    SourceMode,
    _fetch_available_date_range,
    _fetch_topix100_constituent_count,
    _format_bucket_time,
    _open_analysis_connection,
    _query_resampled_topix100_intraday_bars_from_connection,
    _topix100_stocks_cte,
)

TOPIX100_SYMBOL_INTRADAY_HABIT_EXPERIMENT_ID = (
    "market-behavior/topix100-symbol-intraday-habit"
)
TOPIX100_SYMBOL_INTRADAY_HABIT_OVERVIEW_PLOT_FILENAME = (
    "symbol_period_intraday_path_overview.png"
)
TOPIX100_SYMBOL_INTRADAY_HABIT_OVERLAY_PLOT_FILENAME = (
    "symbol_intraday_period_overlay.png"
)
DEFAULT_INTERVAL_MINUTES = 30
DEFAULT_SAMPLE_SEED = 42
DEFAULT_RANDOM_SAMPLE_SIZE = 4
DEFAULT_ANCHOR_CODE = "6857"
DEFAULT_ANALYSIS_PERIOD_MONTHS = 6
PERSISTENT_SIGN_MAGNITUDE_THRESHOLD = 0.0005

_SAMPLED_SYMBOL_COLUMNS: tuple[str, ...] = (
    "sample_order",
    "code",
    "company_name",
    "scale_category",
    "selection_reason",
    "sampling_seed",
)
_PERIOD_COLUMNS: tuple[str, ...] = (
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
)
_PATH_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "code",
    "company_name",
    "bucket_minute",
    "bucket_time",
    "sample_count",
    "session_count",
    "mean_close_return",
    "median_close_return",
    "std_close_return",
    "positive_ratio",
    "below_open_ratio",
)
_PERIOD_SYMBOL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "period_index",
    "period_label",
    "period_start_date",
    "period_end_date",
    "code",
    "company_name",
    "session_count",
    "bar_count",
    "lowest_mean_bucket_time",
    "lowest_mean_close_return",
    "highest_mean_bucket_time",
    "highest_mean_close_return",
    "close_bucket_time",
    "close_bucket_mean_return",
)
_HABIT_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "code",
    "company_name",
    "bucket_minute",
    "bucket_time",
    "period_count",
    "positive_periods",
    "negative_periods",
    "flat_periods",
    "sign_consistency",
    "dominant_direction",
    "mean_of_period_means",
    "median_of_period_means",
    "std_of_period_means",
    "min_period_mean",
    "max_period_mean",
    "average_positive_ratio",
    "is_persistent_sign",
    "is_material_persistent_sign",
)


@dataclass(frozen=True)
class Topix100SymbolIntradayHabitResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    sample_seed: int
    anchor_code: str
    random_sample_size: int
    analysis_period_months: int
    topix100_constituent_count: int
    total_session_count: int
    sampled_symbols_df: pd.DataFrame
    periods_df: pd.DataFrame
    path_summary_df: pd.DataFrame
    period_symbol_summary_df: pd.DataFrame
    habit_summary_df: pd.DataFrame


def _empty_sampled_symbols_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SAMPLED_SYMBOL_COLUMNS))


def _empty_path_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PATH_SUMMARY_COLUMNS))


def _empty_period_symbol_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PERIOD_SYMBOL_SUMMARY_COLUMNS))


def _empty_habit_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_HABIT_SUMMARY_COLUMNS))


def _normalize_code_value(value: str) -> str:
    normalized = str(value).strip()
    if len(normalized) in (5, 6) and normalized.endswith("0"):
        return normalized[:-1]
    return normalized


def _validate_interval_minutes(value: int) -> int:
    interval_minutes = int(value)
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")
    return interval_minutes


def _validate_random_sample_size(value: int) -> int:
    sample_size = int(value)
    if sample_size < 0:
        raise ValueError("random_sample_size must be non-negative")
    return sample_size


def _fetch_topix100_constituents_df(conn: Any) -> pd.DataFrame:
    constituents_df = cast(
        pd.DataFrame,
        conn.execute(
            f"""
            WITH
            {_topix100_stocks_cte()}
            SELECT
                normalized_code AS code,
                company_name,
                scale_category
            FROM topix100_stocks
            ORDER BY normalized_code
            """
        ).fetchdf(),
    )
    if constituents_df.empty:
        return _empty_sampled_symbols_df().drop(columns=["sample_order", "selection_reason", "sampling_seed"])

    constituents_df = constituents_df.copy()
    constituents_df["code"] = constituents_df["code"].astype(str)
    constituents_df["company_name"] = constituents_df["company_name"].astype(str)
    constituents_df["scale_category"] = constituents_df["scale_category"].astype(str)
    return constituents_df


def _select_focus_symbols(
    constituents_df: pd.DataFrame,
    *,
    anchor_code: str,
    random_sample_size: int,
    sample_seed: int,
) -> pd.DataFrame:
    if constituents_df.empty:
        raise ValueError("Current TOPIX100 constituents were not available.")

    normalized_anchor_code = _normalize_code_value(anchor_code)
    anchor_df = constituents_df.loc[
        constituents_df["code"] == normalized_anchor_code
    ].copy()
    if anchor_df.empty:
        raise ValueError(
            f"Anchor code {normalized_anchor_code} was not found in the current TOPIX100 universe."
        )

    other_candidates_df = constituents_df.loc[
        constituents_df["code"] != normalized_anchor_code
    ].copy()
    sample_count = min(random_sample_size, len(other_candidates_df))
    if sample_count > 0:
        sampled_random_df = select_deterministic_samples(
            other_candidates_df.assign(
                sample_partition="topix100",
                sample_seed=str(sample_seed),
            ),
            sample_size=sample_count,
            partition_columns=["sample_partition"],
            hash_columns=["sample_seed", "code"],
            final_order_columns=["sample_rank", "code"],
        ).drop(columns=["sample_partition", "sample_rank"])
    else:
        sampled_random_df = other_candidates_df.iloc[0:0].copy()

    anchor_output_df = anchor_df.assign(
        sample_order=1,
        selection_reason="fixed_anchor",
        sampling_seed=sample_seed,
    )
    if sampled_random_df.empty:
        combined_df = anchor_output_df
    else:
        sampled_random_df = sampled_random_df.assign(
            sample_order=range(2, len(sampled_random_df) + 2),
            selection_reason=f"deterministic_random_seed_{sample_seed}",
            sampling_seed=sample_seed,
        )
        combined_df = pd.concat(
            [anchor_output_df, sampled_random_df],
            ignore_index=True,
        )

    return combined_df.loc[:, list(_SAMPLED_SYMBOL_COLUMNS)].copy()


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
        min(available_end_date, end_date)
        if end_date is not None
        else available_end_date
    )
    if resolved_start_date > resolved_end_date:
        raise ValueError("The selected date range does not overlap the available TOPIX100 minute bars.")
    return resolved_start_date, resolved_end_date


def _build_half_year_periods(
    *,
    start_date: str,
    end_date: str,
    period_months: int,
) -> pd.DataFrame:
    if period_months <= 0:
        raise ValueError("period_months must be positive")

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if start_ts > end_ts:
        raise ValueError("start_date must be before or equal to end_date")

    period_rows: list[dict[str, Any]] = []
    current_start = start_ts
    period_index = 1
    while current_start <= end_ts:
        next_start = current_start + pd.DateOffset(months=period_months)
        period_end = min(end_ts, next_start - pd.Timedelta(days=1))
        period_rows.append(
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

    return pd.DataFrame.from_records(period_rows, columns=_PERIOD_COLUMNS)


def _assign_periods_to_bars(
    bars_df: pd.DataFrame,
    *,
    periods_df: pd.DataFrame,
) -> pd.DataFrame:
    if bars_df.empty or periods_df.empty:
        return pd.DataFrame()

    working_df = bars_df.copy()
    date_ts = pd.to_datetime(working_df["date"])
    period_frames: list[pd.DataFrame] = []
    for period in periods_df.itertuples(index=False):
        period_index = int(cast(Any, period.period_index))
        period_label = str(cast(Any, period.period_label))
        period_start_date = str(cast(Any, period.period_start_date))
        period_end_date = str(cast(Any, period.period_end_date))
        period_start_ts = pd.Timestamp(period_start_date)
        period_end_ts = pd.Timestamp(period_end_date)
        period_mask = (date_ts >= period_start_ts) & (date_ts <= period_end_ts)
        if not bool(period_mask.any()):
            continue
        period_df = working_df.loc[period_mask].copy()
        period_df["period_index"] = period_index
        period_df["period_label"] = period_label
        period_df["period_start_date"] = period_start_date
        period_df["period_end_date"] = period_end_date
        period_frames.append(period_df)

    if not period_frames:
        return pd.DataFrame()
    return pd.concat(period_frames, ignore_index=True)


def _series_std(values: pd.Series) -> float:
    if len(values) <= 1:
        return 0.0
    return float(values.std(ddof=0))


def _build_path_summary_df(
    scoped_bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if scoped_bars_df.empty:
        return _empty_path_summary_df()

    working_df = scoped_bars_df.assign(
        session_key=(
            scoped_bars_df["date"].astype(str)
            + "|"
            + scoped_bars_df["code"].astype(str)
        )
    )
    summary_df = (
        working_df.groupby(
            [
                "period_index",
                "period_label",
                "period_start_date",
                "period_end_date",
                "code",
                "company_name",
                "bucket_minute",
                "bucket_time",
            ],
            as_index=False,
        )
        .agg(
            sample_count=("session_key", "size"),
            session_count=("session_key", "nunique"),
            mean_close_return=("close_return_from_open", "mean"),
            median_close_return=("close_return_from_open", "median"),
            std_close_return=("close_return_from_open", _series_std),
            positive_ratio=(
                "close_return_from_open",
                lambda values: float((values > 0).mean()),
            ),
            below_open_ratio=(
                "close_return_from_open",
                lambda values: float((values < 0).mean()),
            ),
        )
        .sort_values(
            ["code", "period_index", "bucket_minute"],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    summary_df.insert(0, "interval_minutes", interval_minutes)
    return summary_df.loc[:, list(_PATH_SUMMARY_COLUMNS)].copy()


def _pick_group_row(
    group_df: pd.DataFrame,
    *,
    value_column: str,
    ascending: bool,
) -> pd.Series:
    return group_df.sort_values(
        [value_column, "bucket_minute"],
        ascending=[ascending, True],
        kind="stable",
    ).iloc[0]


def _build_period_symbol_summary_df(
    path_summary_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if path_summary_df.empty:
        return _empty_period_symbol_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = path_summary_df.groupby(
        [
            "period_index",
            "period_label",
            "period_start_date",
            "period_end_date",
            "code",
            "company_name",
        ],
        sort=True,
    )
    for group_key, group_df in grouped:
        lowest_row = _pick_group_row(
            group_df,
            value_column="mean_close_return",
            ascending=True,
        )
        highest_row = _pick_group_row(
            group_df,
            value_column="mean_close_return",
            ascending=False,
        )
        close_row = group_df.sort_values(
            ["bucket_minute"],
            kind="stable",
        ).iloc[-1]
        rows.append(
            {
                "interval_minutes": interval_minutes,
                "period_index": int(group_key[0]),
                "period_label": str(group_key[1]),
                "period_start_date": str(group_key[2]),
                "period_end_date": str(group_key[3]),
                "code": str(group_key[4]),
                "company_name": str(group_key[5]),
                "session_count": int(group_df["session_count"].max()),
                "bar_count": int(group_df["sample_count"].sum()),
                "lowest_mean_bucket_time": str(lowest_row["bucket_time"]),
                "lowest_mean_close_return": float(lowest_row["mean_close_return"]),
                "highest_mean_bucket_time": str(highest_row["bucket_time"]),
                "highest_mean_close_return": float(highest_row["mean_close_return"]),
                "close_bucket_time": str(close_row["bucket_time"]),
                "close_bucket_mean_return": float(close_row["mean_close_return"]),
            }
        )

    return pd.DataFrame.from_records(
        rows,
        columns=_PERIOD_SYMBOL_SUMMARY_COLUMNS,
    )


def _build_habit_summary_df(
    path_summary_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if path_summary_df.empty:
        return _empty_habit_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = path_summary_df.groupby(
        ["code", "company_name", "bucket_minute", "bucket_time"],
        sort=True,
    )
    for group_key, group_df in grouped:
        means = group_df["mean_close_return"].astype(float)
        positive_periods = int((means > 0).sum())
        negative_periods = int((means < 0).sum())
        flat_periods = int((means == 0).sum())
        period_count = int(len(group_df))
        dominant_direction = "mixed"
        if positive_periods > negative_periods:
            dominant_direction = "positive"
        elif negative_periods > positive_periods:
            dominant_direction = "negative"
        sign_consistency = (
            max(positive_periods, negative_periods) / period_count
            if period_count > 0
            else 0.0
        )
        mean_of_period_means = float(means.mean()) if period_count > 0 else 0.0
        is_persistent_sign = bool(
            period_count > 0
            and (positive_periods == period_count or negative_periods == period_count)
        )
        rows.append(
            {
                "interval_minutes": interval_minutes,
                "code": str(group_key[0]),
                "company_name": str(group_key[1]),
                "bucket_minute": int(group_key[2]),
                "bucket_time": str(group_key[3]),
                "period_count": period_count,
                "positive_periods": positive_periods,
                "negative_periods": negative_periods,
                "flat_periods": flat_periods,
                "sign_consistency": float(sign_consistency),
                "dominant_direction": dominant_direction,
                "mean_of_period_means": mean_of_period_means,
                "median_of_period_means": float(means.median()),
                "std_of_period_means": _series_std(means),
                "min_period_mean": float(means.min()),
                "max_period_mean": float(means.max()),
                "average_positive_ratio": float(group_df["positive_ratio"].mean()),
                "is_persistent_sign": is_persistent_sign,
                "is_material_persistent_sign": bool(
                    is_persistent_sign
                    and abs(mean_of_period_means)
                    >= PERSISTENT_SIGN_MAGNITUDE_THRESHOLD
                ),
            }
        )

    habit_summary_df = pd.DataFrame.from_records(
        rows,
        columns=_HABIT_SUMMARY_COLUMNS,
    )
    return habit_summary_df.sort_values(
        ["code", "bucket_minute"],
        kind="stable",
    ).reset_index(drop=True)


def run_topix100_symbol_intraday_habit_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    sample_seed: int = DEFAULT_SAMPLE_SEED,
    random_sample_size: int = DEFAULT_RANDOM_SAMPLE_SIZE,
    anchor_code: str = DEFAULT_ANCHOR_CODE,
    analysis_period_months: int = DEFAULT_ANALYSIS_PERIOD_MONTHS,
) -> Topix100SymbolIntradayHabitResult:
    validated_interval_minutes = _validate_interval_minutes(interval_minutes)
    validated_random_sample_size = _validate_random_sample_size(random_sample_size)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_available_date_range(conn)
        topix100_constituent_count = _fetch_topix100_constituent_count(conn)
        resolved_start_date, resolved_end_date = _resolve_analysis_range(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            start_date=start_date,
            end_date=end_date,
        )
        constituents_df = _fetch_topix100_constituents_df(conn)
        sampled_symbols_df = _select_focus_symbols(
            constituents_df,
            anchor_code=anchor_code,
            random_sample_size=validated_random_sample_size,
            sample_seed=sample_seed,
        )
        periods_df = _build_half_year_periods(
            start_date=resolved_start_date,
            end_date=resolved_end_date,
            period_months=analysis_period_months,
        )

        all_topix100_bars_df = _query_resampled_topix100_intraday_bars_from_connection(
            conn,
            interval_minutes=validated_interval_minutes,
            start_date=resolved_start_date,
            end_date=resolved_end_date,
        )

    focused_bars_df = all_topix100_bars_df.merge(
        sampled_symbols_df[["code", "company_name"]],
        on="code",
        how="inner",
    )
    if focused_bars_df.empty:
        raise ValueError("No minute bars were available for the sampled symbols.")

    focused_bars_df = focused_bars_df.copy()
    analysis_start_date = str(focused_bars_df["date"].min())
    analysis_end_date = str(focused_bars_df["date"].max())
    scoped_bars_df = _assign_periods_to_bars(
        focused_bars_df,
        periods_df=periods_df,
    )
    if scoped_bars_df.empty:
        raise ValueError("No sampled symbol rows were assignable to the half-year periods.")

    path_summary_df = _build_path_summary_df(
        scoped_bars_df,
        interval_minutes=validated_interval_minutes,
    )
    period_symbol_summary_df = _build_period_symbol_summary_df(
        path_summary_df,
        interval_minutes=validated_interval_minutes,
    )
    habit_summary_df = _build_habit_summary_df(
        path_summary_df,
        interval_minutes=validated_interval_minutes,
    )
    total_session_count = int(
        scoped_bars_df[["date", "code"]].drop_duplicates().shape[0]
    )

    return Topix100SymbolIntradayHabitResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        interval_minutes=validated_interval_minutes,
        sample_seed=sample_seed,
        anchor_code=_normalize_code_value(anchor_code),
        random_sample_size=validated_random_sample_size,
        analysis_period_months=analysis_period_months,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        sampled_symbols_df=sampled_symbols_df,
        periods_df=periods_df,
        path_summary_df=path_summary_df,
        period_symbol_summary_df=period_symbol_summary_df,
        habit_summary_df=habit_summary_df,
    )


def _split_result_payload(
    result: Topix100SymbolIntradayHabitResult,
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
            "sample_seed": result.sample_seed,
            "anchor_code": result.anchor_code,
            "random_sample_size": result.random_sample_size,
            "analysis_period_months": result.analysis_period_months,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
        },
        {
            "sampled_symbols_df": result.sampled_symbols_df,
            "periods_df": result.periods_df,
            "path_summary_df": result.path_summary_df,
            "period_symbol_summary_df": result.period_symbol_summary_df,
            "habit_summary_df": result.habit_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100SymbolIntradayHabitResult:
    return Topix100SymbolIntradayHabitResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes=int(metadata["interval_minutes"]),
        sample_seed=int(metadata["sample_seed"]),
        anchor_code=str(metadata["anchor_code"]),
        random_sample_size=int(metadata["random_sample_size"]),
        analysis_period_months=int(metadata["analysis_period_months"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        sampled_symbols_df=tables["sampled_symbols_df"],
        periods_df=tables["periods_df"],
        path_summary_df=tables["path_summary_df"],
        period_symbol_summary_df=tables["period_symbol_summary_df"],
        habit_summary_df=tables["habit_summary_df"],
    )


def _build_top_persistent_rows(
    habit_summary_df: pd.DataFrame,
    *,
    code: str,
    direction: str,
) -> pd.DataFrame:
    symbol_df = habit_summary_df.loc[
        (habit_summary_df["code"] == code)
        & (habit_summary_df["is_persistent_sign"])
        & (habit_summary_df["dominant_direction"] == direction)
    ].copy()
    if symbol_df.empty:
        return symbol_df
    symbol_df["abs_mean_of_period_means"] = symbol_df["mean_of_period_means"].abs()
    return symbol_df.sort_values(
        ["abs_mean_of_period_means", "bucket_minute"],
        ascending=[False, True],
        kind="stable",
    )


def _format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:+.4f}%"


def _build_published_summary(
    result: Topix100SymbolIntradayHabitResult,
) -> dict[str, Any]:
    persistent_df = result.habit_summary_df.loc[
        result.habit_summary_df["is_material_persistent_sign"]
    ].copy()
    if not persistent_df.empty:
        persistent_df["abs_mean_of_period_means"] = persistent_df[
            "mean_of_period_means"
        ].abs()
        persistent_rows = persistent_df.sort_values(
            ["abs_mean_of_period_means", "code", "bucket_minute"],
            ascending=[False, True, True],
            kind="stable",
        ).head(10)
    else:
        persistent_rows = persistent_df

    return {
        "intervalMinutes": result.interval_minutes,
        "sampleSeed": result.sample_seed,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "sampledSymbols": result.sampled_symbols_df.to_dict(orient="records"),
        "periods": result.periods_df.to_dict(orient="records"),
        "topPersistentHabits": persistent_rows.to_dict(orient="records"),
    }


def _build_research_bundle_summary_markdown(
    result: Topix100SymbolIntradayHabitResult,
) -> str:
    summary_lines = [
        "# TOPIX100 Symbol Intraday Habit",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{result.interval_minutes}`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Total sampled stock sessions: `{result.total_session_count}`",
        f"- Deterministic sample seed: `{result.sample_seed}`",
        "",
        "## Focus Symbols",
        "",
    ]
    for row in result.sampled_symbols_df.itertuples(index=False):
        summary_lines.append(
            f"- `{row.sample_order}. {row.code} {row.company_name}` "
            f"({row.selection_reason})"
        )

    summary_lines.extend(
        [
            "",
            "## Half-Year Periods",
            "",
        ]
    )
    for row in result.periods_df.itertuples(index=False):
        summary_lines.append(
            f"- `{row.period_label}`"
        )

    summary_lines.extend(
        [
            "",
            "## Current Read",
            "",
        ]
    )
    for symbol_row in result.sampled_symbols_df.itertuples(index=False):
        symbol_code = str(symbol_row.code)
        symbol_name = str(symbol_row.company_name)
        symbol_period_df = result.period_symbol_summary_df.loc[
            result.period_symbol_summary_df["code"] == symbol_code
        ].copy()
        if symbol_period_df.empty:
            summary_lines.append(f"- `{symbol_code} {symbol_name}`: no analyzable rows.")
            continue

        lowest_counts = symbol_period_df["lowest_mean_bucket_time"].value_counts()
        highest_counts = symbol_period_df["highest_mean_bucket_time"].value_counts()
        dominant_low = (
            f"{lowest_counts.index[0]} ({int(lowest_counts.iloc[0])}/{len(symbol_period_df)} periods)"
            if not lowest_counts.empty
            else "n/a"
        )
        dominant_high = (
            f"{highest_counts.index[0]} ({int(highest_counts.iloc[0])}/{len(symbol_period_df)} periods)"
            if not highest_counts.empty
            else "n/a"
        )

        strongest_positive_df = _build_top_persistent_rows(
            result.habit_summary_df,
            code=symbol_code,
            direction="positive",
        )
        strongest_negative_df = _build_top_persistent_rows(
            result.habit_summary_df,
            code=symbol_code,
            direction="negative",
        )
        strongest_positive_text = "none"
        strongest_negative_text = "none"
        if not strongest_positive_df.empty:
            positive_row = strongest_positive_df.iloc[0]
            strongest_positive_text = (
                f"{positive_row['bucket_time']} ({_format_pct(float(positive_row['mean_of_period_means']))})"
            )
        if not strongest_negative_df.empty:
            negative_row = strongest_negative_df.iloc[0]
            strongest_negative_text = (
                f"{negative_row['bucket_time']} ({_format_pct(float(negative_row['mean_of_period_means']))})"
            )

        summary_lines.append(
            f"- `{symbol_code} {symbol_name}`: recurring low bucket `{dominant_low}`, "
            f"recurring high bucket `{dominant_high}`, strongest persistent positive bucket "
            f"`{strongest_positive_text}`, strongest persistent negative bucket "
            f"`{strongest_negative_text}`."
        )

    summary_lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{TOPIX100_SYMBOL_INTRADAY_HABIT_OVERVIEW_PLOT_FILENAME}`",
            f"- `{TOPIX100_SYMBOL_INTRADAY_HABIT_OVERLAY_PLOT_FILENAME}`",
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)


def _import_matplotlib_pyplot() -> Any:
    mpl_config_dir = Path(tempfile.gettempdir()) / "trading25-matplotlib"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config_dir))
    matplotlib = importlib.import_module("matplotlib")
    use_backend = getattr(matplotlib, "use", None)
    if callable(use_backend):
        use_backend("Agg", force=True)
    return importlib.import_module("matplotlib.pyplot")


def _build_axes_grid(axes: Any, row_count: int, column_count: int) -> list[list[Any]]:
    if row_count == 1 and column_count == 1:
        return [[axes]]
    if row_count == 1:
        return [list(axes)]
    if column_count == 1:
        return [[axis] for axis in axes]
    return [list(row_axes) for row_axes in axes]


def _coerce_period_tuple(period: Any) -> tuple[int, str, str, str]:
    return (
        int(cast(Any, period.period_index)),
        str(cast(Any, period.period_label)),
        str(cast(Any, period.period_start_date)),
        str(cast(Any, period.period_end_date)),
    )


def _build_plot_symbol_label(symbol: Any) -> str:
    symbol_code = str(symbol.code)
    symbol_name = str(symbol.company_name)
    try:
        symbol_name.encode("ascii")
    except UnicodeEncodeError:
        return f"{symbol_code}\nReturn (%)"
    return f"{symbol_code}\n{symbol_name}\nReturn (%)"


def _build_tick_minutes(path_summary_df: pd.DataFrame) -> list[int]:
    bucket_minutes = sorted(int(value) for value in path_summary_df["bucket_minute"].unique())
    if not bucket_minutes:
        return []
    if len(bucket_minutes) <= 7:
        return bucket_minutes
    step = max(1, math.ceil(len(bucket_minutes) / 7))
    tick_minutes = bucket_minutes[::step]
    if tick_minutes[-1] != bucket_minutes[-1]:
        tick_minutes.append(bucket_minutes[-1])
    return tick_minutes


def _build_y_limits(path_summary_df: pd.DataFrame) -> tuple[float, float]:
    values = path_summary_df["mean_close_return"].astype(float) * 100.0
    if values.empty:
        return (-0.1, 0.1)
    value_min = float(values.min())
    value_max = float(values.max())
    if math.isclose(value_min, value_max):
        padding = max(abs(value_min) * 0.2, 0.05)
    else:
        padding = max((value_max - value_min) * 0.12, 0.05)
    return value_min - padding, value_max + padding


def write_topix100_symbol_intraday_habit_overview_plot(
    result: Topix100SymbolIntradayHabitResult,
    *,
    output_path: str | Path,
) -> Path:
    if result.path_summary_df.empty or result.sampled_symbols_df.empty or result.periods_df.empty:
        raise ValueError("No summary data was available to plot.")

    plt = _import_matplotlib_pyplot()
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    symbol_rows = list(result.sampled_symbols_df.itertuples(index=False))
    period_rows = list(result.periods_df.itertuples(index=False))
    row_count = len(symbol_rows)
    column_count = len(period_rows)
    fig, axes = plt.subplots(
        row_count,
        column_count,
        figsize=(4.0 * column_count, 2.4 * row_count),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )
    axes_grid = _build_axes_grid(axes, row_count, column_count)
    y_min, y_max = _build_y_limits(result.path_summary_df)
    tick_minutes = _build_tick_minutes(result.path_summary_df)
    period_colors = {
        _coerce_period_tuple(period)[0]: color
        for period, color in zip(
            period_rows,
            ("#2563eb", "#dc2626", "#059669", "#7c3aed"),
            strict=False,
        )
    }

    for row_index, symbol in enumerate(symbol_rows):
        for column_index, period in enumerate(period_rows):
            period_index, _, period_start_date, period_end_date = _coerce_period_tuple(
                period
            )
            axis = axes_grid[row_index][column_index]
            panel_df = result.path_summary_df.loc[
                (result.path_summary_df["code"] == str(symbol.code))
                & (result.path_summary_df["period_index"] == period_index)
            ].copy()
            axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
            axis.grid(axis="y", alpha=0.2, linewidth=0.6)
            axis.set_ylim(y_min, y_max)

            if panel_df.empty:
                axis.text(
                    0.5,
                    0.5,
                    "No data",
                    transform=axis.transAxes,
                    ha="center",
                    va="center",
                    fontsize=9,
                    color="#6b7280",
                )
            else:
                color = period_colors.get(period_index, "#1f2937")
                x_values = panel_df["bucket_minute"].astype(float) / 60.0
                y_values = panel_df["mean_close_return"].astype(float) * 100.0
                axis.plot(
                    x_values,
                    y_values,
                    color=color,
                    linewidth=1.8,
                )
                trough_row = panel_df.sort_values(
                    ["mean_close_return", "bucket_minute"],
                    ascending=[True, True],
                    kind="stable",
                ).iloc[0]
                axis.scatter(
                    [float(trough_row["bucket_minute"]) / 60.0],
                    [float(trough_row["mean_close_return"]) * 100.0],
                    color=color,
                    s=14,
                    zorder=3,
                )

            if row_index == 0:
                axis.set_title(
                    f"P{period_index}\n{period_start_date} -> {period_end_date}",
                    fontsize=9,
                )
            if column_index == 0:
                axis.set_ylabel(
                    _build_plot_symbol_label(symbol),
                    fontsize=8,
                )
            if row_index == row_count - 1 and tick_minutes:
                axis.set_xticks([minute / 60.0 for minute in tick_minutes])
                axis.set_xticklabels(
                    [_format_bucket_time(minute) for minute in tick_minutes],
                    fontsize=8,
                )
            elif tick_minutes:
                axis.set_xticks([minute / 60.0 for minute in tick_minutes])

    fig.suptitle(
        "Sampled TOPIX100 symbol intraday path by half-year period",
        fontsize=12,
    )
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_symbol_intraday_habit_overlay_plot(
    result: Topix100SymbolIntradayHabitResult,
    *,
    output_path: str | Path,
) -> Path:
    if result.path_summary_df.empty or result.sampled_symbols_df.empty or result.periods_df.empty:
        raise ValueError("No summary data was available to plot.")

    plt = _import_matplotlib_pyplot()
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    symbol_rows = list(result.sampled_symbols_df.itertuples(index=False))
    period_rows = list(result.periods_df.itertuples(index=False))
    row_count = len(symbol_rows)
    fig, axes = plt.subplots(
        row_count,
        1,
        figsize=(12, 2.6 * row_count),
        sharex=True,
        constrained_layout=True,
    )
    axes_grid = _build_axes_grid(axes, row_count, 1)
    tick_minutes = _build_tick_minutes(result.path_summary_df)
    period_colors = {
        _coerce_period_tuple(period)[0]: color
        for period, color in zip(
            period_rows,
            ("#2563eb", "#dc2626", "#059669", "#7c3aed"),
            strict=False,
        )
    }

    for row_index, symbol in enumerate(symbol_rows):
        axis = axes_grid[row_index][0]
        axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
        axis.grid(axis="y", alpha=0.2, linewidth=0.6)
        for period in period_rows:
            period_index, _, _, _ = _coerce_period_tuple(period)
            panel_df = result.path_summary_df.loc[
                (result.path_summary_df["code"] == str(symbol.code))
                & (result.path_summary_df["period_index"] == period_index)
            ].copy()
            if panel_df.empty:
                continue
            axis.plot(
                panel_df["bucket_minute"].astype(float) / 60.0,
                panel_df["mean_close_return"].astype(float) * 100.0,
                linewidth=1.8,
                color=period_colors.get(period_index, "#1f2937"),
                label=f"P{period_index}",
            )

        axis.set_ylabel(
            _build_plot_symbol_label(symbol),
            fontsize=8,
        )
        axis.legend(loc="best", ncol=min(4, len(period_rows)), frameon=False, fontsize=8)

    if tick_minutes:
        axes_grid[-1][0].set_xticks([minute / 60.0 for minute in tick_minutes])
        axes_grid[-1][0].set_xticklabels(
            [_format_bucket_time(minute) for minute in tick_minutes],
            fontsize=8,
        )
    axes_grid[-1][0].set_xlabel("JST time")
    fig.suptitle(
        "Sampled TOPIX100 symbol intraday path overlays by half-year period",
        fontsize=12,
    )
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_symbol_intraday_habit_research_bundle(
    result: Topix100SymbolIntradayHabitResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    bundle = write_research_bundle(
        experiment_id=TOPIX100_SYMBOL_INTRADAY_HABIT_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_symbol_intraday_habit_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "sample_seed": result.sample_seed,
            "anchor_code": result.anchor_code,
            "random_sample_size": result.random_sample_size,
            "analysis_period_months": result.analysis_period_months,
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
    write_bundle_artifact(
        bundle,
        TOPIX100_SYMBOL_INTRADAY_HABIT_OVERVIEW_PLOT_FILENAME,
        lambda output_path: write_topix100_symbol_intraday_habit_overview_plot(
            result,
            output_path=output_path,
        ),
    )
    write_bundle_artifact(
        bundle,
        TOPIX100_SYMBOL_INTRADAY_HABIT_OVERLAY_PLOT_FILENAME,
        lambda output_path: write_topix100_symbol_intraday_habit_overlay_plot(
            result,
            output_path=output_path,
        ),
    )
    return bundle


def load_topix100_symbol_intraday_habit_research_bundle(
    bundle_path: str | Path,
) -> Topix100SymbolIntradayHabitResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_symbol_intraday_habit_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_SYMBOL_INTRADAY_HABIT_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_symbol_intraday_habit_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_SYMBOL_INTRADAY_HABIT_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
