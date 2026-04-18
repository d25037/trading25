"""
TOPIX100 13:30 entry to next-session 10:45 exit research.

This study evaluates a reverse intraday/overnight pattern:

- enter at 13:30 on day D
- exit at 10:45 on day D+1

The study reports the unconditional outcome for the full current TOPIX100
universe and compares the top/bottom tails ranked by the entry price versus the
previous close at 13:30.
"""

from __future__ import annotations

import math
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
    write_bundle_artifact,
    write_research_bundle,
)
from src.domains.analytics.topix100_open_relative_intraday_path import (
    SourceMode,
    _fetch_available_date_range,
    _fetch_topix100_constituent_count,
    _import_matplotlib_pyplot,
    _open_analysis_connection,
    _query_resampled_topix100_intraday_bars_from_connection,
)
from src.domains.analytics.topix100_second_bar_volume_drop_performance import (
    _safe_welch_t_test,
)

TOPIX100_1330_ENTRY_NEXT_1045_EXIT_EXPERIMENT_ID = (
    "market-behavior/topix100-1330-entry-next-1045-exit"
)
TOPIX100_1330_ENTRY_NEXT_1045_EXIT_OVERVIEW_PLOT_FILENAME = (
    "topix100_1330_entry_next_1045_exit_overview.png"
)
DEFAULT_INTERVAL_MINUTES = 5
DEFAULT_ENTRY_TIME = "13:30"
DEFAULT_EXIT_TIME = "10:45"
DEFAULT_TAIL_FRACTION = 0.10

_SESSION_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_date",
    "code",
    "previous_close",
    "entry_time",
    "entry_bucket_minute",
    "entry_close",
    "same_day_close_time",
    "same_day_close_bucket_minute",
    "same_day_close",
    "next_date",
    "next_day_open_time",
    "next_day_open_bucket_minute",
    "next_day_open",
    "exit_time",
    "exit_bucket_minute",
    "next_day_exit_close",
    "prev_close_to_entry_return",
    "entry_to_close_return",
    "close_to_next_open_return",
    "next_open_to_exit_return",
    "entry_to_next_exit_return",
    "prev_close_to_next_exit_return",
    "entry_split_rank",
    "entry_split_group",
    "entry_split_session_count",
)
_SESSION_BASE_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_date",
    "code",
    "previous_close",
    "entry_time",
    "entry_bucket_minute",
    "entry_close",
    "same_day_close_time",
    "same_day_close_bucket_minute",
    "same_day_close",
    "next_date",
    "next_day_open_time",
    "next_day_open_bucket_minute",
    "next_day_open",
    "exit_time",
    "exit_bucket_minute",
    "next_day_exit_close",
    "prev_close_to_entry_return",
    "entry_to_close_return",
    "close_to_next_open_return",
    "next_open_to_exit_return",
    "entry_to_next_exit_return",
    "prev_close_to_next_exit_return",
)
_GROUP_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "group_label",
    "sample_count",
    "sample_share",
    "mean_prev_close_to_entry_return",
    "median_prev_close_to_entry_return",
    "mean_entry_to_close_return",
    "median_entry_to_close_return",
    "entry_to_close_hit_positive",
    "mean_close_to_next_open_return",
    "median_close_to_next_open_return",
    "close_to_next_open_hit_positive",
    "mean_next_open_to_exit_return",
    "median_next_open_to_exit_return",
    "next_open_to_exit_hit_positive",
    "mean_entry_to_next_exit_return",
    "median_entry_to_next_exit_return",
    "entry_to_next_exit_hit_positive",
)
_COMPARISON_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_time",
    "exit_time",
    "sample_count",
    "winners_count",
    "losers_count",
    "winners_mean_prev_close_to_entry_return",
    "losers_mean_prev_close_to_entry_return",
    "prev_close_to_entry_mean_spread",
    "winners_entry_to_close_mean",
    "losers_entry_to_close_mean",
    "entry_to_close_mean_spread",
    "entry_to_close_welch_t_stat",
    "entry_to_close_welch_p_value",
    "winners_close_to_next_open_mean",
    "losers_close_to_next_open_mean",
    "close_to_next_open_mean_spread",
    "close_to_next_open_welch_t_stat",
    "close_to_next_open_welch_p_value",
    "winners_next_open_to_exit_mean",
    "losers_next_open_to_exit_mean",
    "next_open_to_exit_mean_spread",
    "next_open_to_exit_welch_t_stat",
    "next_open_to_exit_welch_p_value",
    "winners_entry_to_next_exit_mean",
    "losers_entry_to_next_exit_mean",
    "entry_to_next_exit_mean_spread",
    "entry_to_next_exit_welch_t_stat",
    "entry_to_next_exit_welch_p_value",
)
_GROUP_PATH_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "group_label",
    "day_offset",
    "bucket_minute",
    "bucket_time",
    "timeline_offset_minutes",
    "timeline_label",
    "sample_count",
    "session_count",
    "stock_count",
    "mean_return",
    "median_return",
    "positive_ratio",
)


@dataclass(frozen=True)
class Topix1001330EntryNext1045ExitResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    entry_time: str
    entry_bucket_minute: int
    exit_time: str
    exit_bucket_minute: int
    tail_fraction: float
    topix100_constituent_count: int
    total_entry_session_count: int
    eligible_session_count: int
    excluded_sessions_without_prev_close: int
    excluded_sessions_without_next_session: int
    excluded_sessions_without_exit_bar: int
    session_level_df: pd.DataFrame
    group_summary_df: pd.DataFrame
    comparison_summary_df: pd.DataFrame
    group_path_summary_df: pd.DataFrame


def _empty_session_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SESSION_LEVEL_COLUMNS))


def _empty_group_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_GROUP_SUMMARY_COLUMNS))


def _empty_comparison_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_COMPARISON_SUMMARY_COLUMNS))


def _empty_group_path_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_GROUP_PATH_SUMMARY_COLUMNS))


def _validate_interval_minutes(value: int) -> int:
    interval_minutes = int(value)
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")
    return interval_minutes


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
        raise ValueError(
            "The selected date range does not overlap the available TOPIX100 minute bars."
        )
    return resolved_start_date, resolved_end_date


def _build_session_level_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
    entry_time: str,
    exit_time: str,
) -> pd.DataFrame:
    if bars_df.empty:
        return _empty_session_level_df()

    working_df = bars_df.sort_values(["code", "date", "bucket_minute"], kind="stable").copy()
    session_close_df = (
        working_df.groupby(["date", "code"], as_index=False)
        .tail(1)
        .loc[:, ["date", "code", "bucket_time", "bucket_minute", "close"]]
        .rename(
            columns={
                "date": "entry_date",
                "bucket_time": "same_day_close_time",
                "bucket_minute": "same_day_close_bucket_minute",
                "close": "same_day_close",
            }
        )
    )
    session_open_df = (
        working_df.groupby(["date", "code"], as_index=False)
        .head(1)
        .loc[:, ["date", "code", "bucket_time", "bucket_minute", "day_open"]]
        .rename(
            columns={
                "date": "next_date",
                "bucket_time": "next_day_open_time",
                "bucket_minute": "next_day_open_bucket_minute",
                "day_open": "next_day_open",
            }
        )
    )
    entry_df = working_df.loc[
        working_df["bucket_time"] == entry_time,
        ["date", "code", "bucket_minute", "close"],
    ].rename(
        columns={
            "date": "entry_date",
            "bucket_minute": "entry_bucket_minute",
            "close": "entry_close",
        }
    )
    if entry_df.empty:
        raise ValueError(f"Entry time {entry_time} was not present in the intraday bars.")

    exit_df = working_df.loc[
        working_df["bucket_time"] == exit_time,
        ["date", "code", "bucket_minute", "close"],
    ].rename(
        columns={
            "date": "next_date",
            "bucket_minute": "exit_bucket_minute",
            "close": "next_day_exit_close",
        }
    )
    if exit_df.empty:
        raise ValueError(f"Exit time {exit_time} was not present in the intraday bars.")

    session_meta_df = session_close_df.copy()
    session_meta_df["entry_date_ts"] = pd.to_datetime(session_meta_df["entry_date"])
    session_meta_df = session_meta_df.sort_values(
        ["code", "entry_date_ts"],
        kind="stable",
    ).reset_index(drop=True)
    session_meta_df["previous_close"] = session_meta_df.groupby("code")["same_day_close"].shift(1)
    session_meta_df["next_date"] = session_meta_df.groupby("code")["entry_date"].shift(-1)
    session_meta_df = session_meta_df.drop(columns=["entry_date_ts"])

    session_level_df = entry_df.merge(
        session_meta_df,
        how="left",
        on=["entry_date", "code"],
    ).merge(
        session_open_df,
        how="left",
        on=["next_date", "code"],
    ).merge(
        exit_df,
        how="left",
        on=["next_date", "code"],
    )
    session_level_df.insert(0, "interval_minutes", interval_minutes)
    session_level_df["entry_time"] = entry_time
    session_level_df["exit_time"] = exit_time
    session_level_df["prev_close_to_entry_return"] = (
        session_level_df["entry_close"] / session_level_df["previous_close"] - 1.0
    )
    session_level_df.loc[
        session_level_df["previous_close"].isna(),
        "prev_close_to_entry_return",
    ] = pd.NA
    session_level_df["entry_to_close_return"] = (
        session_level_df["same_day_close"] / session_level_df["entry_close"] - 1.0
    )
    session_level_df["close_to_next_open_return"] = (
        session_level_df["next_day_open"] / session_level_df["same_day_close"] - 1.0
    )
    session_level_df["next_open_to_exit_return"] = (
        session_level_df["next_day_exit_close"] / session_level_df["next_day_open"] - 1.0
    )
    session_level_df["entry_to_next_exit_return"] = (
        session_level_df["next_day_exit_close"] / session_level_df["entry_close"] - 1.0
    )
    session_level_df["prev_close_to_next_exit_return"] = (
        session_level_df["next_day_exit_close"] / session_level_df["previous_close"] - 1.0
    )
    return session_level_df.loc[:, list(_SESSION_BASE_COLUMNS)].copy()


def _assign_rank_groups(
    session_level_df: pd.DataFrame,
    *,
    metric_column: str,
    rank_column: str,
    group_column: str,
    count_column: str,
    tail_fraction: float,
    eligibility_mask: pd.Series | None = None,
) -> pd.DataFrame:
    result_df = session_level_df.drop(
        columns=[rank_column, group_column, count_column],
        errors="ignore",
    ).copy()
    metric_mask = session_level_df[metric_column].notna()
    if eligibility_mask is not None:
        metric_mask = metric_mask & eligibility_mask
    if not bool(metric_mask.any()):
        result_df[rank_column] = pd.Series(pd.NA, index=result_df.index, dtype="Int64")
        result_df[group_column] = pd.Series(pd.NA, index=result_df.index, dtype="string")
        result_df[count_column] = pd.Series(pd.NA, index=result_df.index, dtype="Int64")
        return result_df

    ranked_df = session_level_df.loc[metric_mask, ["entry_date", "code", metric_column]].copy()
    ranked_df = ranked_df.sort_values(
        ["entry_date", metric_column, "code"],
        ascending=[True, False, True],
        kind="stable",
    )
    ranked_df[rank_column] = ranked_df.groupby("entry_date").cumcount() + 1
    ranked_df[count_column] = ranked_df.groupby("entry_date")["code"].transform("size")
    ranked_df["tail_count"] = ranked_df[count_column].map(
        lambda value: max(
            1,
            min(
                int(value) // 2,
                int(math.floor(int(value) * tail_fraction)),
            ),
        )
    )
    ranked_df[group_column] = pd.Series(pd.NA, index=ranked_df.index, dtype="string")
    ranked_df.loc[
        ranked_df[rank_column] <= ranked_df["tail_count"],
        group_column,
    ] = "winners"
    ranked_df.loc[
        ranked_df[rank_column] > (ranked_df[count_column] - ranked_df["tail_count"]),
        group_column,
    ] = "losers"
    ranked_df = ranked_df.loc[
        :,
        ["entry_date", "code", rank_column, group_column, count_column],
    ]
    return result_df.merge(
        ranked_df,
        how="left",
        on=["entry_date", "code"],
    )


def _summarize_returns(
    df: pd.DataFrame,
    *,
    value_column: str,
) -> tuple[float | None, float | None, float | None]:
    if df.empty:
        return None, None, None
    values = df[value_column].dropna()
    if values.empty:
        return None, None, None
    return (
        float(values.mean()),
        float(values.median()),
        float((values > 0).mean()),
    )


def _build_group_summary_df(session_level_df: pd.DataFrame) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_group_summary_df()

    eligible_df = session_level_df.loc[
        session_level_df["prev_close_to_entry_return"].notna()
        & session_level_df["entry_to_next_exit_return"].notna()
    ].copy()
    if eligible_df.empty:
        return _empty_group_summary_df()

    tail_df = eligible_df.loc[eligible_df["entry_split_group"].notna()].copy()
    rows: list[dict[str, Any]] = []
    for group_label, group_df in (
        ("all", eligible_df),
        ("winners", tail_df.loc[tail_df["entry_split_group"] == "winners"].copy()),
        ("losers", tail_df.loc[tail_df["entry_split_group"] == "losers"].copy()),
    ):
        if group_df.empty:
            continue
        (
            mean_prev_close_to_entry,
            median_prev_close_to_entry,
            _,
        ) = _summarize_returns(group_df, value_column="prev_close_to_entry_return")
        (
            mean_entry_to_close,
            median_entry_to_close,
            hit_entry_to_close,
        ) = _summarize_returns(group_df, value_column="entry_to_close_return")
        (
            mean_close_to_next_open,
            median_close_to_next_open,
            hit_close_to_next_open,
        ) = _summarize_returns(group_df, value_column="close_to_next_open_return")
        (
            mean_next_open_to_exit,
            median_next_open_to_exit,
            hit_next_open_to_exit,
        ) = _summarize_returns(group_df, value_column="next_open_to_exit_return")
        (
            mean_entry_to_next_exit,
            median_entry_to_next_exit,
            hit_entry_to_next_exit,
        ) = _summarize_returns(group_df, value_column="entry_to_next_exit_return")
        rows.append(
            {
                "interval_minutes": int(group_df["interval_minutes"].iloc[0]),
                "group_label": group_label,
                "sample_count": int(len(group_df)),
                "sample_share": float(len(group_df) / len(eligible_df)),
                "mean_prev_close_to_entry_return": mean_prev_close_to_entry,
                "median_prev_close_to_entry_return": median_prev_close_to_entry,
                "mean_entry_to_close_return": mean_entry_to_close,
                "median_entry_to_close_return": median_entry_to_close,
                "entry_to_close_hit_positive": hit_entry_to_close,
                "mean_close_to_next_open_return": mean_close_to_next_open,
                "median_close_to_next_open_return": median_close_to_next_open,
                "close_to_next_open_hit_positive": hit_close_to_next_open,
                "mean_next_open_to_exit_return": mean_next_open_to_exit,
                "median_next_open_to_exit_return": median_next_open_to_exit,
                "next_open_to_exit_hit_positive": hit_next_open_to_exit,
                "mean_entry_to_next_exit_return": mean_entry_to_next_exit,
                "median_entry_to_next_exit_return": median_entry_to_next_exit,
                "entry_to_next_exit_hit_positive": hit_entry_to_next_exit,
            }
        )

    return pd.DataFrame.from_records(rows, columns=_GROUP_SUMMARY_COLUMNS)


def _build_comparison_summary_df(
    session_level_df: pd.DataFrame,
    *,
    entry_time: str,
    exit_time: str,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_comparison_summary_df()

    eligible_df = session_level_df.loc[
        session_level_df["prev_close_to_entry_return"].notna()
        & session_level_df["entry_to_next_exit_return"].notna()
    ].copy()
    tail_df = eligible_df.loc[eligible_df["entry_split_group"].notna()].copy()
    winners_df = tail_df.loc[tail_df["entry_split_group"] == "winners"].copy()
    losers_df = tail_df.loc[tail_df["entry_split_group"] == "losers"].copy()
    if winners_df.empty or losers_df.empty:
        return _empty_comparison_summary_df()

    entry_to_close_t_stat, entry_to_close_p_value = _safe_welch_t_test(
        winners_df["entry_to_close_return"],
        losers_df["entry_to_close_return"],
    )
    close_to_next_open_t_stat, close_to_next_open_p_value = _safe_welch_t_test(
        winners_df["close_to_next_open_return"],
        losers_df["close_to_next_open_return"],
    )
    next_open_to_exit_t_stat, next_open_to_exit_p_value = _safe_welch_t_test(
        winners_df["next_open_to_exit_return"],
        losers_df["next_open_to_exit_return"],
    )
    entry_to_next_exit_t_stat, entry_to_next_exit_p_value = _safe_welch_t_test(
        winners_df["entry_to_next_exit_return"],
        losers_df["entry_to_next_exit_return"],
    )
    return pd.DataFrame.from_records(
        [
            {
                "interval_minutes": int(eligible_df["interval_minutes"].iloc[0]),
                "entry_time": entry_time,
                "exit_time": exit_time,
                "sample_count": int(len(eligible_df)),
                "winners_count": int(len(winners_df)),
                "losers_count": int(len(losers_df)),
                "winners_mean_prev_close_to_entry_return": float(
                    winners_df["prev_close_to_entry_return"].mean()
                ),
                "losers_mean_prev_close_to_entry_return": float(
                    losers_df["prev_close_to_entry_return"].mean()
                ),
                "prev_close_to_entry_mean_spread": float(
                    winners_df["prev_close_to_entry_return"].mean()
                    - losers_df["prev_close_to_entry_return"].mean()
                ),
                "winners_entry_to_close_mean": float(
                    winners_df["entry_to_close_return"].mean()
                ),
                "losers_entry_to_close_mean": float(
                    losers_df["entry_to_close_return"].mean()
                ),
                "entry_to_close_mean_spread": float(
                    winners_df["entry_to_close_return"].mean()
                    - losers_df["entry_to_close_return"].mean()
                ),
                "entry_to_close_welch_t_stat": entry_to_close_t_stat,
                "entry_to_close_welch_p_value": entry_to_close_p_value,
                "winners_close_to_next_open_mean": float(
                    winners_df["close_to_next_open_return"].mean()
                ),
                "losers_close_to_next_open_mean": float(
                    losers_df["close_to_next_open_return"].mean()
                ),
                "close_to_next_open_mean_spread": float(
                    winners_df["close_to_next_open_return"].mean()
                    - losers_df["close_to_next_open_return"].mean()
                ),
                "close_to_next_open_welch_t_stat": close_to_next_open_t_stat,
                "close_to_next_open_welch_p_value": close_to_next_open_p_value,
                "winners_next_open_to_exit_mean": float(
                    winners_df["next_open_to_exit_return"].mean()
                ),
                "losers_next_open_to_exit_mean": float(
                    losers_df["next_open_to_exit_return"].mean()
                ),
                "next_open_to_exit_mean_spread": float(
                    winners_df["next_open_to_exit_return"].mean()
                    - losers_df["next_open_to_exit_return"].mean()
                ),
                "next_open_to_exit_welch_t_stat": next_open_to_exit_t_stat,
                "next_open_to_exit_welch_p_value": next_open_to_exit_p_value,
                "winners_entry_to_next_exit_mean": float(
                    winners_df["entry_to_next_exit_return"].mean()
                ),
                "losers_entry_to_next_exit_mean": float(
                    losers_df["entry_to_next_exit_return"].mean()
                ),
                "entry_to_next_exit_mean_spread": float(
                    winners_df["entry_to_next_exit_return"].mean()
                    - losers_df["entry_to_next_exit_return"].mean()
                ),
                "entry_to_next_exit_welch_t_stat": entry_to_next_exit_t_stat,
                "entry_to_next_exit_welch_p_value": entry_to_next_exit_p_value,
            }
        ],
        columns=_COMPARISON_SUMMARY_COLUMNS,
    )


def _build_group_path_summary_df(
    bars_df: pd.DataFrame,
    session_level_df: pd.DataFrame,
    *,
    interval_minutes: int,
) -> pd.DataFrame:
    if bars_df.empty or session_level_df.empty:
        return _empty_group_path_summary_df()

    eligible_df = session_level_df.loc[
        session_level_df["prev_close_to_entry_return"].notna()
        & session_level_df["entry_to_next_exit_return"].notna()
    ].copy()
    if eligible_df.empty:
        return _empty_group_path_summary_df()
    common_entry_bucket_minute = int(
        eligible_df["entry_bucket_minute"].dropna().astype(int).mode().iloc[0]
    )
    common_same_day_close_bucket_minute = int(
        eligible_df["same_day_close_bucket_minute"].dropna().astype(int).mode().iloc[0]
    )
    common_next_day_open_bucket_minute = int(
        eligible_df["next_day_open_bucket_minute"].dropna().astype(int).mode().iloc[0]
    )

    eligible_df["session_key"] = (
        eligible_df["entry_date"].astype(str) + "|" + eligible_df["code"].astype(str)
    )
    all_group_df = eligible_df.loc[
        :,
        [
            "entry_date",
            "code",
            "session_key",
            "entry_close",
            "entry_bucket_minute",
            "same_day_close_bucket_minute",
            "next_date",
            "next_day_open_bucket_minute",
        ],
    ].copy()
    all_group_df["group_label"] = "all"
    tail_group_df = eligible_df.loc[
        eligible_df["entry_split_group"].notna(),
        [
            "entry_date",
            "code",
            "session_key",
            "entry_close",
            "entry_bucket_minute",
            "same_day_close_bucket_minute",
            "next_date",
            "next_day_open_bucket_minute",
            "entry_split_group",
        ],
    ].rename(columns={"entry_split_group": "group_label"})
    group_session_df = pd.concat(
        [all_group_df, tail_group_df],
        ignore_index=True,
    )

    same_day_path_df = bars_df.merge(
        group_session_df,
        how="inner",
        left_on=["date", "code"],
        right_on=["entry_date", "code"],
    )
    same_day_path_df = same_day_path_df.loc[
        same_day_path_df["bucket_minute"].astype(int)
        >= common_entry_bucket_minute
    ].copy()
    same_day_path_df["day_offset"] = 0
    same_day_path_df["timeline_offset_minutes"] = (
        same_day_path_df["bucket_minute"].astype(int) - common_entry_bucket_minute
    )
    same_day_path_df["timeline_label"] = "D " + same_day_path_df["bucket_time"].astype(str)
    same_day_path_df["entry_relative_return"] = (
        same_day_path_df["close"] / same_day_path_df["entry_close"] - 1.0
    )

    next_day_path_df = bars_df.merge(
        group_session_df,
        how="inner",
        left_on=["date", "code"],
        right_on=["next_date", "code"],
    )
    next_day_path_df = next_day_path_df.loc[
        next_day_path_df["bucket_minute"].astype(int)
        <= session_level_df["exit_bucket_minute"].dropna().astype(int).min()
    ].copy()
    next_day_path_df["day_offset"] = 1
    next_day_path_df["timeline_offset_minutes"] = (
        common_same_day_close_bucket_minute
        - common_entry_bucket_minute
        + interval_minutes
        + (
            next_day_path_df["bucket_minute"].astype(int)
            - common_next_day_open_bucket_minute
        )
    )
    next_day_path_df["timeline_label"] = "D+1 " + next_day_path_df["bucket_time"].astype(str)
    next_day_path_df["entry_relative_return"] = (
        next_day_path_df["close"] / next_day_path_df["entry_close"] - 1.0
    )

    timeline_df = pd.concat(
        [same_day_path_df, next_day_path_df],
        ignore_index=True,
    )
    if timeline_df.empty:
        return _empty_group_path_summary_df()

    summary_df = (
        timeline_df.groupby(
            [
                "group_label",
                "day_offset",
                "bucket_minute",
                "bucket_time",
                "timeline_offset_minutes",
                "timeline_label",
            ],
            as_index=False,
        )
        .agg(
            sample_count=("session_key", "size"),
            session_count=("session_key", "nunique"),
            stock_count=("code", "nunique"),
            mean_return=("entry_relative_return", "mean"),
            median_return=("entry_relative_return", "median"),
            positive_ratio=(
                "entry_relative_return",
                lambda values: float((values > 0).mean()),
            ),
        )
        .sort_values(
            ["group_label", "timeline_offset_minutes", "day_offset", "bucket_minute"],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    summary_df.insert(0, "interval_minutes", interval_minutes)
    return summary_df.loc[:, list(_GROUP_PATH_SUMMARY_COLUMNS)].copy()


def run_topix100_1330_entry_next_1045_exit_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    entry_time: str = DEFAULT_ENTRY_TIME,
    exit_time: str = DEFAULT_EXIT_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
) -> Topix1001330EntryNext1045ExitResult:
    validated_interval_minutes = _validate_interval_minutes(interval_minutes)
    validated_entry_time = _validate_time_label(
        entry_time,
        argument_name="entry_time",
    )
    validated_exit_time = _validate_time_label(
        exit_time,
        argument_name="exit_time",
    )
    validated_tail_fraction = _validate_tail_fraction(tail_fraction)

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
        bars_df = _query_resampled_topix100_intraday_bars_from_connection(
            conn,
            interval_minutes=validated_interval_minutes,
            start_date=resolved_start_date,
            end_date=resolved_end_date,
        )

    if bars_df.empty:
        raise ValueError("No TOPIX100 minute bars were available for the selected range.")

    entry_rows = bars_df.loc[bars_df["bucket_time"] == validated_entry_time].copy()
    exit_rows = bars_df.loc[bars_df["bucket_time"] == validated_exit_time].copy()
    if entry_rows.empty:
        raise ValueError(f"Entry time {validated_entry_time} was not present in the bars.")
    if exit_rows.empty:
        raise ValueError(f"Exit time {validated_exit_time} was not present in the bars.")

    session_level_df = _build_session_level_df(
        bars_df,
        interval_minutes=validated_interval_minutes,
        entry_time=validated_entry_time,
        exit_time=validated_exit_time,
    )
    total_entry_session_count = int(len(session_level_df))
    excluded_sessions_without_prev_close = int(
        session_level_df["previous_close"].isna().sum()
    )
    excluded_sessions_without_next_session = int(session_level_df["next_date"].isna().sum())
    excluded_sessions_without_exit_bar = int(
        (
            session_level_df["next_date"].notna()
            & session_level_df["next_day_exit_close"].isna()
        ).sum()
    )
    eligible_mask = (
        session_level_df["prev_close_to_entry_return"].notna()
        & session_level_df["entry_to_next_exit_return"].notna()
    )
    eligible_session_count = int(eligible_mask.sum())

    session_level_df = _assign_rank_groups(
        session_level_df,
        metric_column="prev_close_to_entry_return",
        rank_column="entry_split_rank",
        group_column="entry_split_group",
        count_column="entry_split_session_count",
        tail_fraction=validated_tail_fraction,
        eligibility_mask=eligible_mask,
    )
    session_level_df = session_level_df.loc[:, list(_SESSION_LEVEL_COLUMNS)].copy()

    group_summary_df = _build_group_summary_df(session_level_df)
    comparison_summary_df = _build_comparison_summary_df(
        session_level_df,
        entry_time=validated_entry_time,
        exit_time=validated_exit_time,
    )
    group_path_summary_df = _build_group_path_summary_df(
        bars_df,
        session_level_df,
        interval_minutes=validated_interval_minutes,
    )

    return Topix1001330EntryNext1045ExitResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=str(bars_df["date"].min()),
        analysis_end_date=str(bars_df["date"].max()),
        interval_minutes=validated_interval_minutes,
        entry_time=validated_entry_time,
        entry_bucket_minute=int(entry_rows["bucket_minute"].astype(int).min()),
        exit_time=validated_exit_time,
        exit_bucket_minute=int(exit_rows["bucket_minute"].astype(int).min()),
        tail_fraction=validated_tail_fraction,
        topix100_constituent_count=topix100_constituent_count,
        total_entry_session_count=total_entry_session_count,
        eligible_session_count=eligible_session_count,
        excluded_sessions_without_prev_close=excluded_sessions_without_prev_close,
        excluded_sessions_without_next_session=excluded_sessions_without_next_session,
        excluded_sessions_without_exit_bar=excluded_sessions_without_exit_bar,
        session_level_df=session_level_df,
        group_summary_df=group_summary_df,
        comparison_summary_df=comparison_summary_df,
        group_path_summary_df=group_path_summary_df,
    )


def _split_result_payload(
    result: Topix1001330EntryNext1045ExitResult,
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
            "entry_time": result.entry_time,
            "entry_bucket_minute": result.entry_bucket_minute,
            "exit_time": result.exit_time,
            "exit_bucket_minute": result.exit_bucket_minute,
            "tail_fraction": result.tail_fraction,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_entry_session_count": result.total_entry_session_count,
            "eligible_session_count": result.eligible_session_count,
            "excluded_sessions_without_prev_close": result.excluded_sessions_without_prev_close,
            "excluded_sessions_without_next_session": result.excluded_sessions_without_next_session,
            "excluded_sessions_without_exit_bar": result.excluded_sessions_without_exit_bar,
        },
        {
            "session_level_df": result.session_level_df,
            "group_summary_df": result.group_summary_df,
            "comparison_summary_df": result.comparison_summary_df,
            "group_path_summary_df": result.group_path_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix1001330EntryNext1045ExitResult:
    return Topix1001330EntryNext1045ExitResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes=int(metadata["interval_minutes"]),
        entry_time=str(metadata["entry_time"]),
        entry_bucket_minute=int(metadata["entry_bucket_minute"]),
        exit_time=str(metadata["exit_time"]),
        exit_bucket_minute=int(metadata["exit_bucket_minute"]),
        tail_fraction=float(metadata["tail_fraction"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_entry_session_count=int(metadata["total_entry_session_count"]),
        eligible_session_count=int(metadata["eligible_session_count"]),
        excluded_sessions_without_prev_close=int(
            metadata["excluded_sessions_without_prev_close"]
        ),
        excluded_sessions_without_next_session=int(
            metadata["excluded_sessions_without_next_session"]
        ),
        excluded_sessions_without_exit_bar=int(metadata["excluded_sessions_without_exit_bar"]),
        session_level_df=tables["session_level_df"],
        group_summary_df=tables["group_summary_df"],
        comparison_summary_df=tables["comparison_summary_df"],
        group_path_summary_df=tables["group_path_summary_df"],
    )


def _build_published_summary(
    result: Topix1001330EntryNext1045ExitResult,
) -> dict[str, Any]:
    return {
        "intervalMinutes": result.interval_minutes,
        "entryTime": result.entry_time,
        "exitTime": result.exit_time,
        "tailFraction": result.tail_fraction,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "groupSummary": result.group_summary_df.to_dict(orient="records"),
        "comparisonSummary": result.comparison_summary_df.to_dict(orient="records"),
    }


def _build_research_bundle_summary_markdown(
    result: Topix1001330EntryNext1045ExitResult,
) -> str:
    summary_lines = [
        "# TOPIX100 13:30 Entry -> Next 10:45 Exit",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{result.interval_minutes}`",
        f"- Entry time: `{result.entry_time}`",
        f"- Exit time: `{result.exit_time}`",
        f"- Tail fraction per side: `{result.tail_fraction * 100:.1f}%`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Total entry sessions: `{result.total_entry_session_count}`",
        f"- Eligible entry sessions: `{result.eligible_session_count}`",
        f"- Sessions without previous close: `{result.excluded_sessions_without_prev_close}`",
        f"- Sessions without next session: `{result.excluded_sessions_without_next_session}`",
        f"- Sessions without next-day exit bar: `{result.excluded_sessions_without_exit_bar}`",
        "",
        "## Current Read",
        "",
    ]
    if result.group_summary_df.empty:
        summary_lines.append("- No analyzable rows were available.")
    else:
        group_rows = {
            str(row.group_label): row
            for row in result.group_summary_df.itertuples(index=False)
        }
        for group_label in ("all", "winners", "losers"):
            row = group_rows.get(group_label)
            if row is None:
                continue
            label = {
                "all": "all sessions",
                "winners": f"top {result.tail_fraction * 100:.0f}%",
                "losers": f"bottom {result.tail_fraction * 100:.0f}%",
            }[group_label]
            mean_total = float(cast(Any, row.mean_entry_to_next_exit_return))
            mean_signal = float(cast(Any, row.mean_prev_close_to_entry_return))
            summary_lines.append(
                f"- `{label}`: `{result.entry_time} -> D+1 {result.exit_time}` "
                f"`{mean_total * 100:+.4f}%` "
                f"(entry vs prev close `{mean_signal * 100:+.4f}%`)."
            )

    if not result.comparison_summary_df.empty:
        row = result.comparison_summary_df.iloc[0]
        summary_lines.extend(
            [
                "",
                "## Tail Spread",
                "",
                (
                    f"- top-minus-bottom `{result.entry_time} -> D+1 {result.exit_time}` spread "
                    f"`{float(row['entry_to_next_exit_mean_spread']) * 100:+.4f}%` "
                    f"(p=`{row['entry_to_next_exit_welch_p_value']}`)."
                ),
                (
                    f"- same-day `{result.entry_time} -> close` spread "
                    f"`{float(row['entry_to_close_mean_spread']) * 100:+.4f}%`, "
                    f"overnight `close -> next open` spread "
                    f"`{float(row['close_to_next_open_mean_spread']) * 100:+.4f}%`, "
                    f"next-morning `open -> {result.exit_time}` spread "
                    f"`{float(row['next_open_to_exit_mean_spread']) * 100:+.4f}%`."
                ),
            ]
        )

    summary_lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{TOPIX100_1330_ENTRY_NEXT_1045_EXIT_OVERVIEW_PLOT_FILENAME}`",
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


def _plot_group_lines(axis: Any, plot_df: pd.DataFrame, *, tail_fraction: float) -> None:
    tail_pct = tail_fraction * 100.0
    color_map = {
        "all": "#6b7280",
        "winners": "#2563eb",
        "losers": "#dc2626",
    }
    label_map = {
        "all": "All",
        "winners": f"Top {tail_pct:.0f}%",
        "losers": f"Bottom {tail_pct:.0f}%",
    }
    for group_label in ("all", "winners", "losers"):
        group_df = plot_df.loc[plot_df["group_label"] == group_label].copy()
        if group_df.empty:
            continue
        axis.plot(
            group_df["timeline_offset_minutes"].astype(float),
            group_df["mean_return"].astype(float) * 100.0,
            color=color_map[group_label],
            linewidth=2.0,
            label=label_map[group_label],
        )
    axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
    axis.set_ylabel("Mean return from 13:30 entry (%)")
    axis.grid(axis="y", alpha=0.2, linewidth=0.6)
    axis.legend(loc="best", frameon=False, fontsize=8)


def _build_segment_plot_df(result: Topix1001330EntryNext1045ExitResult) -> pd.DataFrame:
    if result.group_summary_df.empty:
        return pd.DataFrame(
            columns=["group_label", "segment_label", "mean_return"],
        )
    rows: list[dict[str, Any]] = []
    segment_specs = (
        ("mean_entry_to_close_return", f"{result.entry_time}->close"),
        ("mean_close_to_next_open_return", "close->next open"),
        ("mean_next_open_to_exit_return", f"next open->{result.exit_time}"),
        ("mean_entry_to_next_exit_return", f"{result.entry_time}->D+1 {result.exit_time}"),
    )
    for row in result.group_summary_df.itertuples(index=False):
        for column_name, segment_label in segment_specs:
            rows.append(
                {
                    "group_label": str(row.group_label),
                    "segment_label": segment_label,
                    "mean_return": float(cast(Any, getattr(row, column_name))),
                }
            )
    return pd.DataFrame.from_records(rows)


def write_topix100_1330_entry_next_1045_exit_overview_plot(
    result: Topix1001330EntryNext1045ExitResult,
    *,
    output_path: str | Path,
) -> Path:
    if result.group_path_summary_df.empty or result.group_summary_df.empty:
        raise ValueError("No group summary data was available to plot.")

    plt = _import_matplotlib_pyplot()
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), constrained_layout=True)
    path_axis = axes[0]
    segment_axis = axes[1]

    plot_df = result.group_path_summary_df.sort_values(
        ["group_label", "timeline_offset_minutes", "day_offset", "bucket_minute"],
        kind="stable",
    ).copy()
    _plot_group_lines(path_axis, plot_df, tail_fraction=result.tail_fraction)

    preferred_labels = [
        f"D {result.entry_time}",
        "D 14:30",
        "D 15:30",
        "D+1 09:00",
        "D+1 10:00",
        f"D+1 {result.exit_time}",
    ]
    tick_df = plot_df.loc[:, ["timeline_offset_minutes", "timeline_label"]].drop_duplicates()
    selected_tick_df = tick_df.loc[tick_df["timeline_label"].isin(preferred_labels)].copy()
    if selected_tick_df.empty:
        step = max(1, len(tick_df) // 6)
        selected_tick_df = tick_df.iloc[::step].copy()
    path_axis.set_xticks(selected_tick_df["timeline_offset_minutes"].astype(float))
    path_axis.set_xticklabels(selected_tick_df["timeline_label"], fontsize=8)
    path_axis.set_title(
        f"TOPIX100 {result.entry_time} entry path to next-session {result.exit_time}",
        fontsize=11,
    )
    path_axis.set_xlabel("Path timeline")

    same_day_cutoff = plot_df.loc[
        plot_df["day_offset"] == 0,
        "timeline_offset_minutes",
    ].max()
    if pd.notna(same_day_cutoff):
        divider_x = float(cast(Any, same_day_cutoff)) + (result.interval_minutes / 2.0)
        path_axis.axvline(
            divider_x,
            color="#6b7280",
            linestyle="--",
            linewidth=0.9,
            alpha=0.8,
        )
        path_axis.annotate(
            "overnight gap",
            xy=(divider_x, 0.0),
            xytext=(6, 10),
            textcoords="offset points",
            fontsize=8,
            color="#6b7280",
        )

    segment_df = _build_segment_plot_df(result)
    group_order = ("all", "winners", "losers")
    group_label_map = {
        "all": "All",
        "winners": f"Top {result.tail_fraction * 100:.0f}%",
        "losers": f"Bottom {result.tail_fraction * 100:.0f}%",
    }
    color_map = {
        "all": "#6b7280",
        "winners": "#2563eb",
        "losers": "#dc2626",
    }
    segment_labels = list(dict.fromkeys(segment_df["segment_label"].tolist()))
    x_positions = list(range(len(segment_labels)))
    width = 0.22
    for index, group_label in enumerate(group_order):
        group_segment_df = segment_df.loc[
            segment_df["group_label"] == group_label
        ].copy()
        if group_segment_df.empty:
            continue
        heights = [
            float(
                group_segment_df.loc[
                    group_segment_df["segment_label"] == segment_label,
                    "mean_return",
                ].iloc[0]
            )
            * 100.0
            for segment_label in segment_labels
        ]
        shifted_positions = [
            position + (index - 1) * width
            for position in x_positions
        ]
        segment_axis.bar(
            shifted_positions,
            heights,
            width=width,
            color=color_map[group_label],
            label=group_label_map[group_label],
        )
    segment_axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
    segment_axis.set_xticks(x_positions)
    segment_axis.set_xticklabels(segment_labels, fontsize=8)
    segment_axis.set_ylabel("Mean return (%)")
    segment_axis.set_title("Return decomposition by group", fontsize=11)
    segment_axis.grid(axis="y", alpha=0.2, linewidth=0.6)
    segment_axis.legend(loc="best", frameon=False, fontsize=8)

    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_1330_entry_next_1045_exit_research_bundle(
    result: Topix1001330EntryNext1045ExitResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    bundle = write_research_bundle(
        experiment_id=TOPIX100_1330_ENTRY_NEXT_1045_EXIT_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_1330_entry_next_1045_exit_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "entry_time": result.entry_time,
            "exit_time": result.exit_time,
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
    write_bundle_artifact(
        bundle,
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_OVERVIEW_PLOT_FILENAME,
        lambda output_path: write_topix100_1330_entry_next_1045_exit_overview_plot(
            result,
            output_path=output_path,
        ),
    )
    return bundle


def load_topix100_1330_entry_next_1045_exit_research_bundle(
    bundle_path: str | Path,
) -> Topix1001330EntryNext1045ExitResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_1330_entry_next_1045_exit_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_1330_entry_next_1045_exit_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
