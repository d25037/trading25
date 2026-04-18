"""
TOPIX100 open-relative intraday path research.

This module reads minute bars from stock_data_minute_raw, aggregates them into
N-minute bars, and summarizes how prices evolve from the session open through
the close for the current TOPIX100 constituent set.
"""

from __future__ import annotations

import importlib
import os
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    _connect_duckdb as _shared_connect_duckdb,
    normalize_code_sql as _normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_payload_research_bundle,
    write_payload_research_bundle,
)

TOPIX100_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
)
DEFAULT_INTERVAL_MINUTES: tuple[int, ...] = (5, 15, 30)
_RESAMPLED_BAR_COLUMNS: tuple[str, ...] = (
    "date",
    "code",
    "bucket_minute",
    "bucket_time",
    "bucket_start_time",
    "bucket_end_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover_value",
    "source_bar_count",
    "day_open",
    "close_return_from_open",
    "low_return_from_open",
    "high_return_from_open",
)
_PATH_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "bucket_minute",
    "bucket_time",
    "sample_count",
    "session_count",
    "stock_count",
    "mean_close_return",
    "median_close_return",
    "mean_low_return",
    "median_low_return",
    "mean_high_return",
    "median_high_return",
    "close_below_open_ratio",
    "low_below_open_ratio",
)
_EXTREMA_TIMING_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "bucket_minute",
    "bucket_time",
    "session_min_close_count",
    "session_min_close_share",
    "session_min_low_count",
    "session_min_low_share",
    "session_max_high_count",
    "session_max_high_share",
)
_INTERVAL_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "session_count",
    "stock_count",
    "bar_count",
    "lowest_mean_close_bucket_time",
    "lowest_mean_close_return",
    "lowest_median_close_bucket_time",
    "lowest_median_close_return",
    "highest_session_min_low_bucket_time",
    "highest_session_min_low_share",
    "highest_session_min_close_bucket_time",
    "highest_session_min_close_share",
    "highest_session_max_high_bucket_time",
    "highest_session_max_high_share",
    "close_bucket_time",
    "close_bucket_mean_return",
)
TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-open-relative-intraday-path"
)
TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME = (
    "intraday_path_overview.png"
)


@dataclass(frozen=True)
class Topix100OpenRelativeIntradayPathResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes_list: tuple[int, ...]
    topix100_constituent_count: int
    total_session_count: int
    path_summary_df: pd.DataFrame
    extrema_timing_df: pd.DataFrame
    interval_summary_df: pd.DataFrame


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="topix100-open-relative-intraday-path-",
        connect_fn=_connect_duckdb,
    )


def _topix100_stocks_cte() -> str:
    normalized_code_sql = _normalize_code_sql("code")
    return f"""
        topix100_stocks AS (
            SELECT
                normalized_code,
                company_name,
                coalesce(scale_category, '') AS scale_category
            FROM (
                SELECT
                    {normalized_code_sql} AS normalized_code,
                    company_name,
                    scale_category,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS row_priority
                FROM stocks
                WHERE coalesce(scale_category, '') IN {cast(Any, TOPIX100_SCALE_CATEGORIES)}
            ) stock_candidates
            WHERE row_priority = 1
        )
    """


def _date_filter_sql(
    *,
    column_name: str,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, list[str]]:
    conditions: list[str] = []
    params: list[str] = []
    if start_date:
        conditions.append(f"{column_name} >= ?")
        params.append(start_date)
    if end_date:
        conditions.append(f"{column_name} <= ?")
        params.append(end_date)
    if not conditions:
        return "", []
    return " AND " + " AND ".join(conditions), params


def _format_bucket_time(value: int) -> str:
    hour = value // 60
    minute = value % 60
    return f"{hour:02d}:{minute:02d}"


def _empty_resampled_bars_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_RESAMPLED_BAR_COLUMNS))


def _empty_path_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PATH_SUMMARY_COLUMNS))


def _empty_extrema_timing_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_EXTREMA_TIMING_COLUMNS))


def _empty_interval_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_INTERVAL_SUMMARY_COLUMNS))


def _validate_interval_minutes(
    interval_minutes: Sequence[int] | None,
) -> tuple[int, ...]:
    if interval_minutes is None:
        return DEFAULT_INTERVAL_MINUTES

    normalized: list[int] = []
    seen: set[int] = set()
    for raw_value in interval_minutes:
        value = int(raw_value)
        if value <= 0:
            raise ValueError("interval_minutes values must be positive")
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)

    if not normalized:
        raise ValueError("interval_minutes must contain at least one positive value")
    return tuple(normalized)


def _fetch_available_date_range(conn: Any) -> tuple[str | None, str | None]:
    row = conn.execute(
        f"""
        WITH
        {_topix100_stocks_cte()},
        filtered_minutes AS (
            SELECT m.date
            FROM stock_data_minute_raw m
            JOIN topix100_stocks s
              ON s.normalized_code = {_normalize_code_sql('m.code')}
        )
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM filtered_minutes
        """
    ).fetchone()
    return (
        str(row[0]) if row and row[0] is not None else None,
        str(row[1]) if row and row[1] is not None else None,
    )


def _fetch_topix100_constituent_count(conn: Any) -> int:
    row = conn.execute(
        f"""
        WITH
        {_topix100_stocks_cte()}
        SELECT COUNT(*) FROM topix100_stocks
        """
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _query_resampled_topix100_intraday_bars_from_connection(
    conn: Any,
    *,
    interval_minutes: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")

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
    bars_df = cast(
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
                    m.high,
                    m.low,
                    m.close,
                    m.volume,
                    m.turnover_value
                FROM stock_data_minute_raw m
                JOIN topix100_stocks s
                  ON s.normalized_code = {_normalize_code_sql('m.code')}
                WHERE m.time IS NOT NULL
                  AND m.open IS NOT NULL
                  AND m.high IS NOT NULL
                  AND m.low IS NOT NULL
                  AND m.close IS NOT NULL
                  AND m.volume IS NOT NULL
                  AND m.open > 0
                  {date_filter_sql}
            ),
            daily_open AS (
                SELECT
                    date,
                    code,
                    arg_min(open, minute_of_day) AS day_open
                FROM minute_rows
                GROUP BY date, code
            ),
            resampled AS (
                SELECT
                    date,
                    code,
                    CAST(FLOOR(minute_of_day / ?) AS INTEGER) * ? AS bucket_minute,
                    min(time) AS bucket_start_time,
                    max(time) AS bucket_end_time,
                    arg_min(open, minute_of_day) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    arg_max(close, minute_of_day) AS close,
                    sum(volume) AS volume,
                    sum(coalesce(turnover_value, 0.0)) AS turnover_value,
                    count(*) AS source_bar_count
                FROM minute_rows
                GROUP BY date, code, bucket_minute
            )
            SELECT
                r.date,
                r.code,
                r.bucket_minute,
                r.bucket_start_time,
                r.bucket_end_time,
                r.open,
                r.high,
                r.low,
                r.close,
                r.volume,
                r.turnover_value,
                r.source_bar_count,
                d.day_open,
                r.close / NULLIF(d.day_open, 0) - 1 AS close_return_from_open,
                r.low / NULLIF(d.day_open, 0) - 1 AS low_return_from_open,
                r.high / NULLIF(d.day_open, 0) - 1 AS high_return_from_open
            FROM resampled r
            JOIN daily_open d USING (date, code)
            ORDER BY r.date, r.code, r.bucket_minute
            """,
            params,
        ).fetchdf(),
    )
    if bars_df.empty:
        return _empty_resampled_bars_df()

    bars_df = bars_df.copy()
    bars_df["bucket_minute"] = bars_df["bucket_minute"].astype(int)
    bars_df["bucket_time"] = bars_df["bucket_minute"].map(_format_bucket_time)
    return bars_df.loc[:, list(_RESAMPLED_BAR_COLUMNS)].copy()


def query_topix100_resampled_intraday_bars(
    db_path: str,
    *,
    interval_minutes: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    with _open_analysis_connection(db_path) as ctx:
        return _query_resampled_topix100_intraday_bars_from_connection(
            ctx.connection,
            interval_minutes=interval_minutes,
            start_date=start_date,
            end_date=end_date,
        )


def _build_path_summary_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if bars_df.empty:
        return _empty_path_summary_df()

    session_keys = bars_df["date"].astype(str) + "|" + bars_df["code"].astype(str)
    working_df = bars_df.assign(session_key=session_keys)
    summary_df = (
        working_df.groupby(["bucket_minute", "bucket_time"], as_index=False)
        .agg(
            sample_count=("session_key", "size"),
            session_count=("session_key", "nunique"),
            stock_count=("code", "nunique"),
            mean_close_return=("close_return_from_open", "mean"),
            median_close_return=("close_return_from_open", "median"),
            mean_low_return=("low_return_from_open", "mean"),
            median_low_return=("low_return_from_open", "median"),
            mean_high_return=("high_return_from_open", "mean"),
            median_high_return=("high_return_from_open", "median"),
            close_below_open_ratio=(
                "close_return_from_open",
                lambda values: float((values < 0).mean()),
            ),
            low_below_open_ratio=(
                "low_return_from_open",
                lambda values: float((values < 0).mean()),
            ),
        )
        .sort_values("bucket_minute")
        .reset_index(drop=True)
    )
    summary_df.insert(0, "interval_minutes", interval_minutes)
    return summary_df.loc[:, list(_PATH_SUMMARY_COLUMNS)].copy()


def _summarize_extrema_counts(
    bars_df: pd.DataFrame,
    *,
    metric_column: str,
    extremum: Literal["min", "max"],
) -> pd.DataFrame:
    session_cols = ["date", "code"]
    if bars_df.empty:
        return pd.DataFrame(columns=[*session_cols, "bucket_minute"])

    if extremum == "min":
        extrema_values = bars_df.groupby(session_cols)[metric_column].transform("min")
        selected = bars_df.loc[bars_df[metric_column].eq(extrema_values), session_cols + ["bucket_minute"]]
        aggregated = selected.groupby(session_cols, as_index=False).agg(
            bucket_minute=("bucket_minute", "min")
        )
    else:
        extrema_values = bars_df.groupby(session_cols)[metric_column].transform("max")
        selected = bars_df.loc[bars_df[metric_column].eq(extrema_values), session_cols + ["bucket_minute"]]
        aggregated = selected.groupby(session_cols, as_index=False).agg(
            bucket_minute=("bucket_minute", "min")
        )
    return aggregated


def _build_extrema_timing_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if bars_df.empty:
        return _empty_extrema_timing_df()

    session_total = bars_df[["date", "code"]].drop_duplicates().shape[0]
    if session_total <= 0:
        return _empty_extrema_timing_df()

    min_close_df = _summarize_extrema_counts(
        bars_df,
        metric_column="close_return_from_open",
        extremum="min",
    )
    min_low_df = _summarize_extrema_counts(
        bars_df,
        metric_column="low_return_from_open",
        extremum="min",
    )
    max_high_df = _summarize_extrema_counts(
        bars_df,
        metric_column="high_return_from_open",
        extremum="max",
    )

    all_bucket_df = (
        bars_df[["bucket_minute", "bucket_time"]]
        .drop_duplicates()
        .sort_values("bucket_minute")
        .reset_index(drop=True)
    )
    min_close_counts = min_close_df.groupby("bucket_minute").size().rename(
        "session_min_close_count"
    )
    min_low_counts = min_low_df.groupby("bucket_minute").size().rename(
        "session_min_low_count"
    )
    max_high_counts = max_high_df.groupby("bucket_minute").size().rename(
        "session_max_high_count"
    )

    timing_df = all_bucket_df.merge(
        min_close_counts,
        how="left",
        left_on="bucket_minute",
        right_index=True,
    ).merge(
        min_low_counts,
        how="left",
        left_on="bucket_minute",
        right_index=True,
    ).merge(
        max_high_counts,
        how="left",
        left_on="bucket_minute",
        right_index=True,
    )
    for column in (
        "session_min_close_count",
        "session_min_low_count",
        "session_max_high_count",
    ):
        timing_df[column] = timing_df[column].fillna(0).astype(int)
    timing_df["session_min_close_share"] = (
        timing_df["session_min_close_count"] / session_total
    )
    timing_df["session_min_low_share"] = (
        timing_df["session_min_low_count"] / session_total
    )
    timing_df["session_max_high_share"] = (
        timing_df["session_max_high_count"] / session_total
    )
    timing_df.insert(0, "interval_minutes", interval_minutes)
    return timing_df.loc[:, list(_EXTREMA_TIMING_COLUMNS)].copy()


def _pick_summary_row(
    df: pd.DataFrame,
    *,
    sort_columns: list[str],
    ascending: list[bool],
) -> pd.Series | None:
    if df.empty:
        return None
    return df.sort_values(sort_columns, ascending=ascending).iloc[0]


def _build_interval_summary_row(
    bars_df: pd.DataFrame,
    path_summary_df: pd.DataFrame,
    extrema_timing_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> dict[str, Any]:
    if path_summary_df.empty or extrema_timing_df.empty:
        return {
            "interval_minutes": interval_minutes,
            "session_count": 0,
            "stock_count": 0,
            "bar_count": 0,
            "lowest_mean_close_bucket_time": None,
            "lowest_mean_close_return": None,
            "lowest_median_close_bucket_time": None,
            "lowest_median_close_return": None,
            "highest_session_min_low_bucket_time": None,
            "highest_session_min_low_share": None,
            "highest_session_min_close_bucket_time": None,
            "highest_session_min_close_share": None,
            "highest_session_max_high_bucket_time": None,
            "highest_session_max_high_share": None,
            "close_bucket_time": None,
            "close_bucket_mean_return": None,
        }

    lowest_mean_close = _pick_summary_row(
        path_summary_df,
        sort_columns=["mean_close_return", "bucket_minute"],
        ascending=[True, True],
    )
    lowest_median_close = _pick_summary_row(
        path_summary_df,
        sort_columns=["median_close_return", "bucket_minute"],
        ascending=[True, True],
    )
    highest_session_min_low = _pick_summary_row(
        extrema_timing_df,
        sort_columns=["session_min_low_share", "bucket_minute"],
        ascending=[False, True],
    )
    highest_session_min_close = _pick_summary_row(
        extrema_timing_df,
        sort_columns=["session_min_close_share", "bucket_minute"],
        ascending=[False, True],
    )
    highest_session_max_high = _pick_summary_row(
        extrema_timing_df,
        sort_columns=["session_max_high_share", "bucket_minute"],
        ascending=[False, True],
    )
    close_bucket = _pick_summary_row(
        path_summary_df,
        sort_columns=["bucket_minute"],
        ascending=[False],
    )
    session_count = int(bars_df[["date", "code"]].drop_duplicates().shape[0])
    stock_count = int(bars_df["code"].nunique())

    return {
        "interval_minutes": interval_minutes,
        "session_count": session_count,
        "stock_count": stock_count,
        "bar_count": int(len(bars_df)),
        "lowest_mean_close_bucket_time": cast(Any, lowest_mean_close)["bucket_time"],
        "lowest_mean_close_return": float(cast(Any, lowest_mean_close)["mean_close_return"]),
        "lowest_median_close_bucket_time": cast(Any, lowest_median_close)["bucket_time"],
        "lowest_median_close_return": float(cast(Any, lowest_median_close)["median_close_return"]),
        "highest_session_min_low_bucket_time": cast(Any, highest_session_min_low)["bucket_time"],
        "highest_session_min_low_share": float(cast(Any, highest_session_min_low)["session_min_low_share"]),
        "highest_session_min_close_bucket_time": cast(Any, highest_session_min_close)["bucket_time"],
        "highest_session_min_close_share": float(cast(Any, highest_session_min_close)["session_min_close_share"]),
        "highest_session_max_high_bucket_time": cast(Any, highest_session_max_high)["bucket_time"],
        "highest_session_max_high_share": float(cast(Any, highest_session_max_high)["session_max_high_share"]),
        "close_bucket_time": cast(Any, close_bucket)["bucket_time"],
        "close_bucket_mean_return": float(cast(Any, close_bucket)["mean_close_return"]),
    }


def run_topix100_open_relative_intraday_path_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes_list: Sequence[int] | None = None,
) -> Topix100OpenRelativeIntradayPathResult:
    validated_intervals = _validate_interval_minutes(interval_minutes_list)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_available_date_range(conn)
        topix100_constituent_count = _fetch_topix100_constituent_count(conn)

        path_summary_frames: list[pd.DataFrame] = []
        extrema_timing_frames: list[pd.DataFrame] = []
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
            if bars_df.empty:
                path_summary_frames.append(_empty_path_summary_df())
                extrema_timing_frames.append(_empty_extrema_timing_df())
                interval_summary_rows.append(
                    _build_interval_summary_row(
                        bars_df,
                        _empty_path_summary_df(),
                        _empty_extrema_timing_df(),
                        interval_minutes=interval_minutes,
                    )
                )
                continue

            if analysis_start_date is None:
                analysis_start_date = str(bars_df["date"].min())
            if analysis_end_date is None:
                analysis_end_date = str(bars_df["date"].max())
            total_session_count = max(
                total_session_count,
                int(bars_df[["date", "code"]].drop_duplicates().shape[0]),
            )

            path_summary_df = _build_path_summary_df(
                bars_df,
                interval_minutes=interval_minutes,
            )
            extrema_timing_df = _build_extrema_timing_df(
                bars_df,
                interval_minutes=interval_minutes,
            )
            path_summary_frames.append(path_summary_df)
            extrema_timing_frames.append(extrema_timing_df)
            interval_summary_rows.append(
                _build_interval_summary_row(
                    bars_df,
                    path_summary_df,
                    extrema_timing_df,
                    interval_minutes=interval_minutes,
                )
            )

    path_summary_df = (
        pd.concat(path_summary_frames, ignore_index=True)
        if path_summary_frames
        else _empty_path_summary_df()
    )
    extrema_timing_df = (
        pd.concat(extrema_timing_frames, ignore_index=True)
        if extrema_timing_frames
        else _empty_extrema_timing_df()
    )
    interval_summary_df = (
        pd.DataFrame.from_records(interval_summary_rows, columns=_INTERVAL_SUMMARY_COLUMNS)
        if interval_summary_rows
        else _empty_interval_summary_df()
    )
    if path_summary_df.empty and total_session_count == 0:
        raise ValueError("No TOPIX100 minute bars were available for the selected range.")

    return Topix100OpenRelativeIntradayPathResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        interval_minutes_list=validated_intervals,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        path_summary_df=path_summary_df,
        extrema_timing_df=extrema_timing_df,
        interval_summary_df=interval_summary_df,
    )


def _split_result_payload(
    result: Topix100OpenRelativeIntradayPathResult,
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
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
        },
        {
            "path_summary_df": result.path_summary_df,
            "extrema_timing_df": result.extrema_timing_df,
            "interval_summary_df": result.interval_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100OpenRelativeIntradayPathResult:
    return Topix100OpenRelativeIntradayPathResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes_list=tuple(int(value) for value in metadata["interval_minutes_list"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        path_summary_df=tables["path_summary_df"],
        extrema_timing_df=tables["extrema_timing_df"],
        interval_summary_df=tables["interval_summary_df"],
    )


def _build_published_summary(
    result: Topix100OpenRelativeIntradayPathResult,
) -> dict[str, Any]:
    return {
        "intervalMinutesList": list(result.interval_minutes_list),
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "topix100ConstituentCount": result.topix100_constituent_count,
        "totalSessionCount": result.total_session_count,
        "intervalSummary": result.interval_summary_df.to_dict(orient="records"),
    }


def _build_research_bundle_summary_markdown(
    result: Topix100OpenRelativeIntradayPathResult,
) -> str:
    summary_lines = [
        "# TOPIX100 Open-Relative Intraday Path",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{', '.join(str(value) for value in result.interval_minutes_list)}`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Total stock sessions: `{result.total_session_count}`",
        "",
        "## Current Read",
        "",
    ]
    if result.interval_summary_df.empty:
        summary_lines.append("- Interval summary was empty.")
    else:
        for row in result.interval_summary_df.itertuples(index=False):
            if row.lowest_mean_close_bucket_time is None:
                summary_lines.append(f"- `{row.interval_minutes}m`: no analyzable rows.")
                continue
            lowest_mean_close_return = float(cast(Any, row.lowest_mean_close_return))
            highest_session_min_low_share = float(
                cast(Any, row.highest_session_min_low_share)
            )
            summary_lines.append(
                f"- `{row.interval_minutes}m`: lowest mean close/open bucket was "
                f"`{row.lowest_mean_close_bucket_time}` (`{lowest_mean_close_return * 100:+.4f}%`), "
                f"and the session low appeared most often at "
                f"`{row.highest_session_min_low_bucket_time}` (`{highest_session_min_low_share * 100:.2f}%`)."
            )
    summary_lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME}`",
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


def write_topix100_open_relative_intraday_path_overview_plot(
    result: Topix100OpenRelativeIntradayPathResult,
    *,
    output_path: str | Path,
) -> Path:
    if result.path_summary_df.empty or result.extrema_timing_df.empty:
        raise ValueError("No summary data was available to plot.")

    plt = _import_matplotlib_pyplot()
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    path_summary_df = result.path_summary_df.sort_values(
        ["interval_minutes", "bucket_minute"]
    ).copy()
    extrema_timing_df = result.extrema_timing_df.sort_values(
        ["interval_minutes", "bucket_minute"]
    ).copy()
    interval_colors = {
        interval: color
        for interval, color in zip(
            result.interval_minutes_list,
            ("#2563eb", "#dc2626", "#059669", "#7c3aed", "#ea580c"),
            strict=False,
        )
    }
    fig, axes = plt.subplots(
        2,
        1,
        figsize=(14, 9),
        sharex=True,
        constrained_layout=True,
    )
    summary_axes = axes[0]
    timing_axes = axes[1]
    for interval_minutes in result.interval_minutes_list:
        color = interval_colors.get(interval_minutes, "#1f2937")
        interval_path_df = path_summary_df.loc[
            path_summary_df["interval_minutes"] == interval_minutes
        ].copy()
        interval_extrema_df = extrema_timing_df.loc[
            extrema_timing_df["interval_minutes"] == interval_minutes
        ].copy()
        if interval_path_df.empty or interval_extrema_df.empty:
            continue

        x_values = interval_path_df["bucket_minute"] / 60.0
        summary_axes.plot(
            x_values,
            interval_path_df["mean_close_return"] * 100.0,
            label=f"{interval_minutes}m mean close/open",
            color=color,
            linewidth=2.0,
        )
        trough_row = interval_path_df.sort_values(
            ["mean_close_return", "bucket_minute"]
        ).iloc[0]
        trough_x = float(trough_row["bucket_minute"]) / 60.0
        trough_y = float(trough_row["mean_close_return"]) * 100.0
        summary_axes.scatter([trough_x], [trough_y], color=color, s=28, zorder=3)
        summary_axes.annotate(
            f"{interval_minutes}m low {trough_row['bucket_time']}",
            xy=(trough_x, trough_y),
            xytext=(0, 10 if interval_minutes % 2 else -16),
            textcoords="offset points",
            color=color,
            fontsize=9,
            ha="center",
        )

        extrema_x_values = interval_extrema_df["bucket_minute"] / 60.0
        timing_axes.plot(
            extrema_x_values,
            interval_extrema_df["session_min_low_share"] * 100.0,
            label=f"{interval_minutes}m session-low timing",
            color=color,
            linewidth=2.0,
        )
        peak_row = interval_extrema_df.sort_values(
            ["session_min_low_share", "bucket_minute"],
            ascending=[False, True],
        ).iloc[0]
        peak_x = float(peak_row["bucket_minute"]) / 60.0
        peak_y = float(peak_row["session_min_low_share"]) * 100.0
        timing_axes.scatter([peak_x], [peak_y], color=color, s=28, zorder=3)
        timing_axes.annotate(
            f"{interval_minutes}m peak {peak_row['bucket_time']}",
            xy=(peak_x, peak_y),
            xytext=(0, 10 if interval_minutes % 2 else -16),
            textcoords="offset points",
            color=color,
            fontsize=9,
            ha="center",
        )

    for axes_item in (summary_axes, timing_axes):
        axes_item.axvline(9.5, color="#6b7280", linestyle="--", linewidth=1.0, alpha=0.8)
        axes_item.grid(axis="y", alpha=0.25, linewidth=0.7)

    summary_axes.axhline(0.0, color="#111827", linewidth=1.0, alpha=0.8)
    summary_axes.set_ylabel("Mean close/open return (%)")
    summary_axes.set_title(
        "TOPIX100 intraday path vs session open"
        f" ({result.analysis_start_date} to {result.analysis_end_date})"
    )
    summary_axes.legend(loc="best", frameon=False)

    timing_axes.set_ylabel("Share of sessions with session low (%)")
    timing_axes.set_xlabel("JST time")
    timing_axes.legend(loc="best", frameon=False)

    tick_minutes = [540, 570, 600, 630, 660, 690, 720, 750, 780, 810, 840, 870, 900, 930]
    timing_axes.set_xticks([minute / 60.0 for minute in tick_minutes])
    timing_axes.set_xticklabels([_format_bucket_time(minute) for minute in tick_minutes])

    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_open_relative_intraday_path_research_bundle(
    result: Topix100OpenRelativeIntradayPathResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    bundle = write_payload_research_bundle(
        experiment_id=TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_open_relative_intraday_path_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes_list": list(result.interval_minutes_list),
        },
        result=result,
        split_result_payload=_split_result_payload,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )
    write_topix100_open_relative_intraday_path_overview_plot(
        result,
        output_path=(
            bundle.bundle_dir
            / TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME
        ),
    )
    return bundle


def load_topix100_open_relative_intraday_path_research_bundle(
    bundle_path: str | Path,
) -> Topix100OpenRelativeIntradayPathResult:
    return load_payload_research_bundle(
        bundle_path,
        build_result_from_payload=_build_result_from_payload,
    )


def get_topix100_open_relative_intraday_path_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_open_relative_intraday_path_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
