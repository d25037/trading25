"""
TOPIX100 peak-time winner/loser intraday path research.

This study verifies whether the 5-minute morning path peaks closer to 10:30 or
10:45, then splits each day's current TOPIX100 cross-section into winners and
losers at that peak anchor.

Two split bases are evaluated:

- primary: session open -> peak anchor return
- secondary: previous close -> peak anchor return

The resulting groups are tracked through the rest of the session with both
open-relative and peak-relative path summaries.
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
    _format_bucket_time,
    _import_matplotlib_pyplot,
    _open_analysis_connection,
    _query_resampled_topix100_intraday_bars_from_connection,
)
from src.domains.analytics.topix100_second_bar_volume_drop_performance import (
    _safe_welch_t_test,
)

TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_EXPERIMENT_ID = (
    "market-behavior/topix100-peak-winner-loser-intraday-path"
)
TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME = (
    "peak_winner_loser_intraday_path_overview.png"
)
DEFAULT_INTERVAL_MINUTES = 5
DEFAULT_ANCHOR_CANDIDATE_TIMES: tuple[str, ...] = ("10:30", "10:45")
DEFAULT_MIDDAY_REFERENCE_TIME = "13:30"
DEFAULT_TAIL_FRACTION = 0.10

_ANCHOR_SELECTION_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "candidate_time",
    "candidate_bucket_minute",
    "sample_count",
    "session_count",
    "stock_count",
    "mean_close_return",
    "mean_high_return",
    "mean_low_return",
    "candidate_rank",
    "is_selected_anchor",
)
_SESSION_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "date",
    "code",
    "day_open",
    "previous_close",
    "anchor_time",
    "anchor_bucket_minute",
    "anchor_close",
    "midday_reference_time",
    "midday_close",
    "session_close",
    "open_to_anchor_return",
    "prev_close_to_anchor_return",
    "anchor_to_midday_return",
    "anchor_to_close_return",
    "midday_to_close_return",
    "open_to_close_return",
    "open_split_rank",
    "open_split_group",
    "open_split_session_count",
    "prev_close_split_rank",
    "prev_close_split_group",
    "prev_close_split_session_count",
)
_SESSION_BASE_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "date",
    "code",
    "day_open",
    "previous_close",
    "anchor_time",
    "anchor_bucket_minute",
    "anchor_close",
    "midday_reference_time",
    "midday_close",
    "session_close",
    "open_to_anchor_return",
    "prev_close_to_anchor_return",
    "anchor_to_midday_return",
    "anchor_to_close_return",
    "midday_to_close_return",
    "open_to_close_return",
)
_GROUP_PATH_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "split_basis",
    "path_basis",
    "group_label",
    "bucket_minute",
    "bucket_time",
    "minutes_from_anchor",
    "sample_count",
    "session_count",
    "stock_count",
    "mean_return",
    "median_return",
    "positive_ratio",
)
_GROUP_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "split_basis",
    "group_label",
    "sample_count",
    "sample_share",
    "mean_anchor_return",
    "median_anchor_return",
    "mean_anchor_to_midday_return",
    "median_anchor_to_midday_return",
    "anchor_to_midday_hit_positive",
    "mean_anchor_to_close_return",
    "median_anchor_to_close_return",
    "anchor_to_close_hit_positive",
    "mean_midday_to_close_return",
    "median_midday_to_close_return",
    "midday_to_close_hit_positive",
    "mean_open_to_close_return",
    "median_open_to_close_return",
    "open_to_close_hit_positive",
)
_COMPARISON_SUMMARY_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "split_basis",
    "anchor_time",
    "midday_reference_time",
    "sample_count",
    "winners_count",
    "losers_count",
    "winners_mean_anchor_return",
    "losers_mean_anchor_return",
    "anchor_return_mean_spread",
    "winners_anchor_to_midday_mean",
    "losers_anchor_to_midday_mean",
    "anchor_to_midday_mean_spread",
    "anchor_to_midday_welch_t_stat",
    "anchor_to_midday_welch_p_value",
    "winners_anchor_to_close_mean",
    "losers_anchor_to_close_mean",
    "anchor_to_close_mean_spread",
    "anchor_to_close_welch_t_stat",
    "anchor_to_close_welch_p_value",
    "winners_midday_to_close_mean",
    "losers_midday_to_close_mean",
    "midday_to_close_mean_spread",
    "midday_to_close_welch_t_stat",
    "midday_to_close_welch_p_value",
)


@dataclass(frozen=True)
class Topix100PeakWinnerLoserIntradayPathResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    anchor_candidate_times: tuple[str, ...]
    selected_anchor_time: str
    selected_anchor_bucket_minute: int
    midday_reference_time: str
    tail_fraction: float
    topix100_constituent_count: int
    total_session_count: int
    excluded_sessions_without_anchor: int
    excluded_sessions_without_prev_close: int
    anchor_selection_df: pd.DataFrame
    session_level_df: pd.DataFrame
    group_path_summary_df: pd.DataFrame
    group_summary_df: pd.DataFrame
    comparison_summary_df: pd.DataFrame


def _empty_anchor_selection_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_ANCHOR_SELECTION_COLUMNS))


def _empty_session_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SESSION_LEVEL_COLUMNS))


def _empty_group_path_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_GROUP_PATH_SUMMARY_COLUMNS))


def _empty_group_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_GROUP_SUMMARY_COLUMNS))


def _empty_comparison_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_COMPARISON_SUMMARY_COLUMNS))


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


def _normalize_anchor_candidate_times(values: tuple[str, ...] | None) -> tuple[str, ...]:
    if values is None:
        return DEFAULT_ANCHOR_CANDIDATE_TIMES
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        value = str(raw_value).strip()
        if not value:
            continue
        if len(value) != 5 or value[2] != ":":
            raise ValueError("anchor candidate times must be formatted as HH:MM")
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if not normalized:
        raise ValueError("anchor candidate times must contain at least one HH:MM value")
    return tuple(normalized)


def _build_anchor_selection_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
    anchor_candidate_times: tuple[str, ...],
) -> pd.DataFrame:
    if bars_df.empty:
        return _empty_anchor_selection_df()

    candidate_df = bars_df.loc[
        bars_df["bucket_time"].isin(anchor_candidate_times)
    ].copy()
    if candidate_df.empty:
        raise ValueError(
            "None of the requested anchor candidate times were present in the intraday bars."
        )

    session_keys = candidate_df["date"].astype(str) + "|" + candidate_df["code"].astype(str)
    candidate_df = candidate_df.assign(session_key=session_keys)
    summary_df = (
        candidate_df.groupby(["bucket_minute", "bucket_time"], as_index=False)
        .agg(
            sample_count=("session_key", "size"),
            session_count=("session_key", "nunique"),
            stock_count=("code", "nunique"),
            mean_close_return=("close_return_from_open", "mean"),
            mean_high_return=("high_return_from_open", "mean"),
            mean_low_return=("low_return_from_open", "mean"),
        )
        .sort_values(
            ["mean_close_return", "bucket_minute"],
            ascending=[False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    summary_df.insert(0, "interval_minutes", interval_minutes)
    summary_df = summary_df.rename(
        columns={
            "bucket_time": "candidate_time",
            "bucket_minute": "candidate_bucket_minute",
        }
    )
    summary_df["candidate_rank"] = range(1, len(summary_df) + 1)
    summary_df["is_selected_anchor"] = summary_df["candidate_rank"] == 1
    return summary_df.loc[:, list(_ANCHOR_SELECTION_COLUMNS)].copy()


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


def _build_session_level_df(
    bars_df: pd.DataFrame,
    *,
    interval_minutes: int,
    anchor_time: str,
    midday_reference_time: str,
) -> pd.DataFrame:
    if bars_df.empty:
        return _empty_session_level_df()

    working_df = bars_df.sort_values(["code", "date", "bucket_minute"], kind="stable").copy()
    session_close_df = (
        working_df.groupby(["date", "code"], as_index=False)
        .tail(1)
        .loc[:, ["date", "code", "close"]]
        .rename(columns={"close": "session_close"})
    )
    anchor_df = working_df.loc[
        working_df["bucket_time"] == anchor_time,
        ["date", "code", "bucket_minute", "close", "day_open"],
    ].rename(
        columns={
            "bucket_minute": "anchor_bucket_minute",
            "close": "anchor_close",
        }
    )
    if anchor_df.empty:
        raise ValueError(f"Anchor time {anchor_time} was not present in the intraday bars.")

    midday_df = working_df.loc[
        working_df["bucket_time"] == midday_reference_time,
        ["date", "code", "close"],
    ].rename(columns={"close": "midday_close"})

    session_level_df = anchor_df.merge(
        midday_df,
        how="left",
        on=["date", "code"],
    ).merge(
        session_close_df,
        how="left",
        on=["date", "code"],
    )
    session_level_df.insert(0, "interval_minutes", interval_minutes)
    session_level_df["anchor_time"] = anchor_time
    session_level_df["midday_reference_time"] = midday_reference_time
    session_level_df["date_ts"] = pd.to_datetime(session_level_df["date"])
    session_level_df = session_level_df.sort_values(
        ["code", "date_ts"],
        kind="stable",
    ).reset_index(drop=True)
    session_level_df["previous_close"] = session_level_df.groupby("code")[
        "session_close"
    ].shift(1)
    session_level_df["open_to_anchor_return"] = (
        session_level_df["anchor_close"] / session_level_df["day_open"] - 1.0
    )
    session_level_df["prev_close_to_anchor_return"] = (
        session_level_df["anchor_close"] / session_level_df["previous_close"] - 1.0
    )
    session_level_df.loc[
        session_level_df["previous_close"].isna(),
        "prev_close_to_anchor_return",
    ] = pd.NA
    session_level_df["anchor_to_midday_return"] = (
        session_level_df["midday_close"] / session_level_df["anchor_close"] - 1.0
    )
    session_level_df["anchor_to_close_return"] = (
        session_level_df["session_close"] / session_level_df["anchor_close"] - 1.0
    )
    session_level_df["midday_to_close_return"] = (
        session_level_df["session_close"] / session_level_df["midday_close"] - 1.0
    )
    session_level_df["open_to_close_return"] = (
        session_level_df["session_close"] / session_level_df["day_open"] - 1.0
    )

    session_level_df = session_level_df.drop(columns=["date_ts"])
    return session_level_df.loc[:, list(_SESSION_BASE_COLUMNS)].copy()


def _assign_rank_groups(
    session_level_df: pd.DataFrame,
    *,
    metric_column: str,
    rank_column: str,
    group_column: str,
    count_column: str,
    tail_fraction: float,
) -> pd.DataFrame:
    result_df = session_level_df.drop(
        columns=[rank_column, group_column, count_column],
        errors="ignore",
    ).copy()

    metric_mask = session_level_df[metric_column].notna()
    if not bool(metric_mask.any()):
        result_df[rank_column] = pd.Series(pd.NA, index=result_df.index, dtype="Int64")
        result_df[group_column] = pd.Series(pd.NA, index=result_df.index, dtype="string")
        result_df[count_column] = pd.Series(pd.NA, index=result_df.index, dtype="Int64")
        return result_df

    ranked_df = session_level_df.loc[metric_mask, ["date", "code", metric_column]].copy()
    ranked_df = ranked_df.sort_values(
        ["date", metric_column, "code"],
        ascending=[True, False, True],
        kind="stable",
    )
    ranked_df[rank_column] = ranked_df.groupby("date").cumcount() + 1
    ranked_df[count_column] = ranked_df.groupby("date")["code"].transform("size")
    ranked_df["tail_count"] = ranked_df[count_column].map(
        lambda value: max(1, min(int(value) // 2, int(math.floor(int(value) * tail_fraction))))
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
    ranked_df = ranked_df.loc[:, ["date", "code", rank_column, group_column, count_column]]
    return result_df.merge(
        ranked_df,
        how="left",
        on=["date", "code"],
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


def _build_group_summary_df(
    session_level_df: pd.DataFrame,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_group_summary_df()

    rows: list[dict[str, Any]] = []
    split_specs = (
        ("open_to_peak", "open_to_anchor_return", "open_split_group"),
        ("prev_close_to_peak", "prev_close_to_anchor_return", "prev_close_split_group"),
    )
    for split_basis, anchor_column, group_column in split_specs:
        eligible_df = session_level_df.loc[
            session_level_df[anchor_column].notna()
        ].copy()
        if eligible_df.empty:
            continue
        tail_df = eligible_df.loc[eligible_df[group_column].notna()].copy()
        group_frames = [
            ("all", eligible_df),
            ("winners", tail_df.loc[tail_df[group_column] == "winners"].copy()),
            ("losers", tail_df.loc[tail_df[group_column] == "losers"].copy()),
        ]
        sample_count = len(eligible_df)
        for group_label, group_df in group_frames:
            if group_df.empty:
                continue
            mean_anchor, median_anchor, _ = _summarize_returns(
                group_df,
                value_column=anchor_column,
            )
            mean_anchor_to_midday, median_anchor_to_midday, hit_anchor_to_midday = _summarize_returns(
                group_df,
                value_column="anchor_to_midday_return",
            )
            mean_anchor_to_close, median_anchor_to_close, hit_anchor_to_close = _summarize_returns(
                group_df,
                value_column="anchor_to_close_return",
            )
            mean_midday_to_close, median_midday_to_close, hit_midday_to_close = _summarize_returns(
                group_df,
                value_column="midday_to_close_return",
            )
            mean_open_to_close, median_open_to_close, hit_open_to_close = _summarize_returns(
                group_df,
                value_column="open_to_close_return",
            )
            rows.append(
                {
                    "interval_minutes": int(group_df["interval_minutes"].iloc[0]),
                    "split_basis": split_basis,
                    "group_label": group_label,
                    "sample_count": int(len(group_df)),
                    "sample_share": float(len(group_df) / sample_count),
                    "mean_anchor_return": mean_anchor,
                    "median_anchor_return": median_anchor,
                    "mean_anchor_to_midday_return": mean_anchor_to_midday,
                    "median_anchor_to_midday_return": median_anchor_to_midday,
                    "anchor_to_midday_hit_positive": hit_anchor_to_midday,
                    "mean_anchor_to_close_return": mean_anchor_to_close,
                    "median_anchor_to_close_return": median_anchor_to_close,
                    "anchor_to_close_hit_positive": hit_anchor_to_close,
                    "mean_midday_to_close_return": mean_midday_to_close,
                    "median_midday_to_close_return": median_midday_to_close,
                    "midday_to_close_hit_positive": hit_midday_to_close,
                    "mean_open_to_close_return": mean_open_to_close,
                    "median_open_to_close_return": median_open_to_close,
                    "open_to_close_hit_positive": hit_open_to_close,
                }
            )

    return pd.DataFrame.from_records(rows, columns=_GROUP_SUMMARY_COLUMNS)


def _build_comparison_summary_df(
    session_level_df: pd.DataFrame,
    *,
    anchor_time: str,
    midday_reference_time: str,
) -> pd.DataFrame:
    if session_level_df.empty:
        return _empty_comparison_summary_df()

    rows: list[dict[str, Any]] = []
    split_specs = (
        ("open_to_peak", "open_to_anchor_return", "open_split_group"),
        ("prev_close_to_peak", "prev_close_to_anchor_return", "prev_close_split_group"),
    )
    for split_basis, anchor_column, group_column in split_specs:
        eligible_df = session_level_df.loc[
            session_level_df[anchor_column].notna()
        ].copy()
        if eligible_df.empty:
            continue
        tail_df = eligible_df.loc[eligible_df[group_column].notna()].copy()
        winners_df = tail_df.loc[tail_df[group_column] == "winners"].copy()
        losers_df = tail_df.loc[tail_df[group_column] == "losers"].copy()
        if winners_df.empty or losers_df.empty:
            continue

        winners_mean_anchor = float(winners_df[anchor_column].mean())
        losers_mean_anchor = float(losers_df[anchor_column].mean())
        winners_anchor_to_midday = float(winners_df["anchor_to_midday_return"].mean())
        losers_anchor_to_midday = float(losers_df["anchor_to_midday_return"].mean())
        winners_anchor_to_close = float(winners_df["anchor_to_close_return"].mean())
        losers_anchor_to_close = float(losers_df["anchor_to_close_return"].mean())
        winners_midday_to_close = float(winners_df["midday_to_close_return"].mean())
        losers_midday_to_close = float(losers_df["midday_to_close_return"].mean())

        anchor_to_midday_t_stat, anchor_to_midday_p_value = _safe_welch_t_test(
            winners_df["anchor_to_midday_return"],
            losers_df["anchor_to_midday_return"],
        )
        anchor_to_close_t_stat, anchor_to_close_p_value = _safe_welch_t_test(
            winners_df["anchor_to_close_return"],
            losers_df["anchor_to_close_return"],
        )
        midday_to_close_t_stat, midday_to_close_p_value = _safe_welch_t_test(
            winners_df["midday_to_close_return"],
            losers_df["midday_to_close_return"],
        )
        rows.append(
            {
                "interval_minutes": int(eligible_df["interval_minutes"].iloc[0]),
                "split_basis": split_basis,
                "anchor_time": anchor_time,
                "midday_reference_time": midday_reference_time,
                "sample_count": int(len(eligible_df)),
                "winners_count": int(len(winners_df)),
                "losers_count": int(len(losers_df)),
                "winners_mean_anchor_return": winners_mean_anchor,
                "losers_mean_anchor_return": losers_mean_anchor,
                "anchor_return_mean_spread": winners_mean_anchor - losers_mean_anchor,
                "winners_anchor_to_midday_mean": winners_anchor_to_midday,
                "losers_anchor_to_midday_mean": losers_anchor_to_midday,
                "anchor_to_midday_mean_spread": winners_anchor_to_midday - losers_anchor_to_midday,
                "anchor_to_midday_welch_t_stat": anchor_to_midday_t_stat,
                "anchor_to_midday_welch_p_value": anchor_to_midday_p_value,
                "winners_anchor_to_close_mean": winners_anchor_to_close,
                "losers_anchor_to_close_mean": losers_anchor_to_close,
                "anchor_to_close_mean_spread": winners_anchor_to_close - losers_anchor_to_close,
                "anchor_to_close_welch_t_stat": anchor_to_close_t_stat,
                "anchor_to_close_welch_p_value": anchor_to_close_p_value,
                "winners_midday_to_close_mean": winners_midday_to_close,
                "losers_midday_to_close_mean": losers_midday_to_close,
                "midday_to_close_mean_spread": winners_midday_to_close - losers_midday_to_close,
                "midday_to_close_welch_t_stat": midday_to_close_t_stat,
                "midday_to_close_welch_p_value": midday_to_close_p_value,
            }
        )

    return pd.DataFrame.from_records(rows, columns=_COMPARISON_SUMMARY_COLUMNS)


def _build_group_path_summary_df(
    bars_df: pd.DataFrame,
    session_level_df: pd.DataFrame,
    *,
    interval_minutes: int,
    selected_anchor_bucket_minute: int,
) -> pd.DataFrame:
    if bars_df.empty or session_level_df.empty:
        return _empty_group_path_summary_df()

    split_specs = (
        ("open_to_peak", "open_to_anchor_return", "open_split_group"),
        ("prev_close_to_peak", "prev_close_to_anchor_return", "prev_close_split_group"),
    )
    frames: list[pd.DataFrame] = []
    for split_basis, anchor_column, group_column in split_specs:
        eligible_session_df = session_level_df.loc[
            session_level_df[anchor_column].notna(),
            ["date", "code", "anchor_close"],
        ].copy()
        if eligible_session_df.empty:
            continue
        eligible_session_df["group_label"] = "all"
        tail_session_df = session_level_df.loc[
            session_level_df[group_column].notna(),
            ["date", "code", "anchor_close", group_column],
        ].rename(columns={group_column: "group_label"})
        group_session_df = pd.concat(
            [eligible_session_df, tail_session_df],
            ignore_index=True,
        )

        working_df = bars_df.merge(
            group_session_df,
            how="inner",
            on=["date", "code"],
        )
        if working_df.empty:
            continue
        working_df["split_basis"] = split_basis
        working_df["minutes_from_anchor"] = (
            working_df["bucket_minute"].astype(int) - selected_anchor_bucket_minute
        )
        working_df["anchor_relative_return"] = (
            working_df["close"] / working_df["anchor_close"] - 1.0
        )

        full_day_summary_df = (
            working_df.groupby(
                ["split_basis", "group_label", "bucket_minute", "bucket_time", "minutes_from_anchor"],
                as_index=False,
            )
            .agg(
                sample_count=("code", "size"),
                session_count=("date", "nunique"),
                stock_count=("code", "nunique"),
                mean_return=("close_return_from_open", "mean"),
                median_return=("close_return_from_open", "median"),
                positive_ratio=("close_return_from_open", lambda values: float((values > 0).mean())),
            )
            .sort_values(["split_basis", "group_label", "bucket_minute"], kind="stable")
            .reset_index(drop=True)
        )
        full_day_summary_df.insert(1, "path_basis", "open_relative")

        anchor_df = working_df.loc[
            working_df["bucket_minute"].astype(int) >= selected_anchor_bucket_minute
        ].copy()
        anchor_summary_df = (
            anchor_df.groupby(
                ["split_basis", "group_label", "bucket_minute", "bucket_time", "minutes_from_anchor"],
                as_index=False,
            )
            .agg(
                sample_count=("code", "size"),
                session_count=("date", "nunique"),
                stock_count=("code", "nunique"),
                mean_return=("anchor_relative_return", "mean"),
                median_return=("anchor_relative_return", "median"),
                positive_ratio=("anchor_relative_return", lambda values: float((values > 0).mean())),
            )
            .sort_values(["split_basis", "group_label", "bucket_minute"], kind="stable")
            .reset_index(drop=True)
        )
        anchor_summary_df.insert(1, "path_basis", "anchor_relative")

        frames.extend([full_day_summary_df, anchor_summary_df])

    if not frames:
        return _empty_group_path_summary_df()

    result_df = pd.concat(frames, ignore_index=True)
    result_df.insert(0, "interval_minutes", interval_minutes)
    return result_df.loc[:, list(_GROUP_PATH_SUMMARY_COLUMNS)].copy()


def run_topix100_peak_winner_loser_intraday_path_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    anchor_candidate_times: tuple[str, ...] | None = None,
    midday_reference_time: str = DEFAULT_MIDDAY_REFERENCE_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
) -> Topix100PeakWinnerLoserIntradayPathResult:
    validated_interval_minutes = _validate_interval_minutes(interval_minutes)
    normalized_anchor_candidate_times = _normalize_anchor_candidate_times(anchor_candidate_times)
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

    anchor_selection_df = _build_anchor_selection_df(
        bars_df,
        interval_minutes=validated_interval_minutes,
        anchor_candidate_times=normalized_anchor_candidate_times,
    )
    selected_anchor_row = anchor_selection_df.loc[
        anchor_selection_df["is_selected_anchor"]
    ].iloc[0]
    selected_anchor_time = str(selected_anchor_row["candidate_time"])
    selected_anchor_bucket_minute = int(selected_anchor_row["candidate_bucket_minute"])

    session_level_df = _build_session_level_df(
        bars_df,
        interval_minutes=validated_interval_minutes,
        anchor_time=selected_anchor_time,
        midday_reference_time=midday_reference_time,
    )
    total_session_count = int(len(session_level_df))
    excluded_sessions_without_prev_close = int(
        session_level_df["previous_close"].isna().sum()
    )

    session_level_df = _assign_rank_groups(
        session_level_df,
        metric_column="open_to_anchor_return",
        rank_column="open_split_rank",
        group_column="open_split_group",
        count_column="open_split_session_count",
        tail_fraction=validated_tail_fraction,
    )
    session_level_df = _assign_rank_groups(
        session_level_df,
        metric_column="prev_close_to_anchor_return",
        rank_column="prev_close_split_rank",
        group_column="prev_close_split_group",
        count_column="prev_close_split_session_count",
        tail_fraction=validated_tail_fraction,
    )
    session_level_df = session_level_df.loc[:, list(_SESSION_LEVEL_COLUMNS)].copy()

    group_path_summary_df = _build_group_path_summary_df(
        bars_df,
        session_level_df,
        interval_minutes=validated_interval_minutes,
        selected_anchor_bucket_minute=selected_anchor_bucket_minute,
    )
    group_summary_df = _build_group_summary_df(session_level_df)
    comparison_summary_df = _build_comparison_summary_df(
        session_level_df,
        anchor_time=selected_anchor_time,
        midday_reference_time=midday_reference_time,
    )

    return Topix100PeakWinnerLoserIntradayPathResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=str(bars_df["date"].min()),
        analysis_end_date=str(bars_df["date"].max()),
        interval_minutes=validated_interval_minutes,
        anchor_candidate_times=normalized_anchor_candidate_times,
        selected_anchor_time=selected_anchor_time,
        selected_anchor_bucket_minute=selected_anchor_bucket_minute,
        midday_reference_time=midday_reference_time,
        tail_fraction=validated_tail_fraction,
        topix100_constituent_count=topix100_constituent_count,
        total_session_count=total_session_count,
        excluded_sessions_without_anchor=0,
        excluded_sessions_without_prev_close=excluded_sessions_without_prev_close,
        anchor_selection_df=anchor_selection_df,
        session_level_df=session_level_df,
        group_path_summary_df=group_path_summary_df,
        group_summary_df=group_summary_df,
        comparison_summary_df=comparison_summary_df,
    )


def _split_result_payload(
    result: Topix100PeakWinnerLoserIntradayPathResult,
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
            "anchor_candidate_times": list(result.anchor_candidate_times),
            "selected_anchor_time": result.selected_anchor_time,
            "selected_anchor_bucket_minute": result.selected_anchor_bucket_minute,
            "midday_reference_time": result.midday_reference_time,
            "tail_fraction": result.tail_fraction,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_session_count": result.total_session_count,
            "excluded_sessions_without_anchor": result.excluded_sessions_without_anchor,
            "excluded_sessions_without_prev_close": result.excluded_sessions_without_prev_close,
        },
        {
            "anchor_selection_df": result.anchor_selection_df,
            "session_level_df": result.session_level_df,
            "group_path_summary_df": result.group_path_summary_df,
            "group_summary_df": result.group_summary_df,
            "comparison_summary_df": result.comparison_summary_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix100PeakWinnerLoserIntradayPathResult:
    return Topix100PeakWinnerLoserIntradayPathResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes=int(metadata["interval_minutes"]),
        anchor_candidate_times=tuple(str(value) for value in metadata["anchor_candidate_times"]),
        selected_anchor_time=str(metadata["selected_anchor_time"]),
        selected_anchor_bucket_minute=int(metadata["selected_anchor_bucket_minute"]),
        midday_reference_time=str(metadata["midday_reference_time"]),
        tail_fraction=float(metadata["tail_fraction"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_session_count=int(metadata["total_session_count"]),
        excluded_sessions_without_anchor=int(metadata["excluded_sessions_without_anchor"]),
        excluded_sessions_without_prev_close=int(metadata["excluded_sessions_without_prev_close"]),
        anchor_selection_df=tables["anchor_selection_df"],
        session_level_df=tables["session_level_df"],
        group_path_summary_df=tables["group_path_summary_df"],
        group_summary_df=tables["group_summary_df"],
        comparison_summary_df=tables["comparison_summary_df"],
    )


def _build_published_summary(
    result: Topix100PeakWinnerLoserIntradayPathResult,
) -> dict[str, Any]:
    return {
        "intervalMinutes": result.interval_minutes,
        "anchorCandidateTimes": list(result.anchor_candidate_times),
        "selectedAnchorTime": result.selected_anchor_time,
        "tailFraction": result.tail_fraction,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "anchorSelection": result.anchor_selection_df.to_dict(orient="records"),
        "comparisonSummary": result.comparison_summary_df.to_dict(orient="records"),
    }


def _build_research_bundle_summary_markdown(
    result: Topix100PeakWinnerLoserIntradayPathResult,
) -> str:
    summary_lines = [
        "# TOPIX100 Peak Winner/Loser Intraday Path",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{result.interval_minutes}`",
        f"- Anchor candidate times: `{', '.join(result.anchor_candidate_times)}`",
        f"- Selected anchor time: `{result.selected_anchor_time}`",
        f"- Midday reference time: `{result.midday_reference_time}`",
        f"- Tail fraction per side: `{result.tail_fraction * 100:.1f}%`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Total stock sessions with anchor: `{result.total_session_count}`",
        f"- Sessions without previous close: `{result.excluded_sessions_without_prev_close}`",
        "",
        "## Anchor Selection",
        "",
    ]
    for row in result.anchor_selection_df.itertuples(index=False):
        mean_close_return = float(cast(Any, row.mean_close_return))
        summary_lines.append(
            f"- `{row.candidate_time}`: mean close/open `{mean_close_return * 100:+.4f}%`"
            + (" <- selected" if bool(row.is_selected_anchor) else "")
        )

    summary_lines.extend(["", "## Current Read", ""])
    if result.comparison_summary_df.empty:
        summary_lines.append("- No comparison rows were available.")
    else:
        for row in result.comparison_summary_df.itertuples(index=False):
            anchor_to_close_mean_spread = float(cast(Any, row.anchor_to_close_mean_spread))
            winners_anchor_to_close_mean = float(cast(Any, row.winners_anchor_to_close_mean))
            losers_anchor_to_close_mean = float(cast(Any, row.losers_anchor_to_close_mean))
            summary_lines.append(
                f"- `{row.split_basis}`: top-minus-bottom `{result.tail_fraction * 100:.1f}%` "
                f"`{result.selected_anchor_time} -> close` spread "
                f"`{anchor_to_close_mean_spread * 100:+.4f}%` "
                f"(winners `{winners_anchor_to_close_mean * 100:+.4f}%`, "
                f"losers `{losers_anchor_to_close_mean * 100:+.4f}%`, "
                f"p=`{row.anchor_to_close_welch_p_value}`)"
            )

    summary_lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME}`",
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


def _plot_group_lines(
    axis: Any,
    plot_df: pd.DataFrame,
    *,
    x_column: str,
    selected_anchor_x: float,
    x_label: str,
    y_label: str,
    title: str,
    anchor_vertical_label: str,
    tail_fraction: float,
) -> None:
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
            group_df[x_column].astype(float),
            group_df["mean_return"].astype(float) * 100.0,
            color=color_map[group_label],
            linewidth=2.0,
            label=label_map[group_label],
        )
    axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
    axis.axvline(selected_anchor_x, color="#6b7280", linestyle="--", linewidth=0.9, alpha=0.8)
    axis.set_title(title, fontsize=10)
    axis.set_xlabel(x_label)
    axis.set_ylabel(y_label)
    axis.grid(axis="y", alpha=0.2, linewidth=0.6)
    axis.legend(loc="best", frameon=False, fontsize=8, title=anchor_vertical_label)


def write_topix100_peak_winner_loser_intraday_path_overview_plot(
    result: Topix100PeakWinnerLoserIntradayPathResult,
    *,
    output_path: str | Path,
) -> Path:
    if result.group_path_summary_df.empty:
        raise ValueError("No group path summary data was available to plot.")

    plt = _import_matplotlib_pyplot()
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(14, 9), constrained_layout=True, sharey="row")
    split_order = ("open_to_peak", "prev_close_to_peak")
    plot_specs = (
        ("open_relative", "bucket_minute", result.selected_anchor_bucket_minute / 60.0, "JST time", "Mean close/open return (%)", "Full-day open-relative path"),
        ("anchor_relative", "minutes_from_anchor", 0.0, "Minutes from anchor", "Mean return from anchor (%)", "Post-anchor path"),
    )
    anchor_label = f"anchor {result.selected_anchor_time}"

    for row_index, split_basis in enumerate(split_order):
        for column_index, (path_basis, x_column, selected_anchor_x, x_label, y_label, title_suffix) in enumerate(plot_specs):
            axis = axes[row_index][column_index]
            plot_df = result.group_path_summary_df.loc[
                (result.group_path_summary_df["split_basis"] == split_basis)
                & (result.group_path_summary_df["path_basis"] == path_basis)
            ].copy()
            if plot_df.empty:
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
                continue

            if path_basis == "open_relative":
                plot_df["bucket_hour"] = plot_df["bucket_minute"].astype(float) / 60.0
                _plot_group_lines(
                    axis,
                    plot_df,
                    x_column="bucket_hour",
                    selected_anchor_x=selected_anchor_x,
                    x_label=x_label,
                    y_label=y_label,
                    title=f"{split_basis}: {title_suffix}",
                    anchor_vertical_label=anchor_label,
                    tail_fraction=result.tail_fraction,
                )
                tick_minutes = sorted(int(value) for value in plot_df["bucket_minute"].unique())
                tick_minutes = tick_minutes[:: max(1, len(tick_minutes) // 8)] if len(tick_minutes) > 8 else tick_minutes
                axis.set_xticks([minute / 60.0 for minute in tick_minutes])
                axis.set_xticklabels([_format_bucket_time(minute) for minute in tick_minutes], fontsize=8)
            else:
                _plot_group_lines(
                    axis,
                    plot_df,
                    x_column=x_column,
                    selected_anchor_x=selected_anchor_x,
                    x_label=x_label,
                    y_label=y_label,
                    title=f"{split_basis}: {title_suffix}",
                    anchor_vertical_label=anchor_label,
                    tail_fraction=result.tail_fraction,
                )

    fig.suptitle(
        f"TOPIX100 top/bottom {result.tail_fraction * 100:.0f}% paths anchored at {result.selected_anchor_time}",
        fontsize=12,
    )
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_peak_winner_loser_intraday_path_research_bundle(
    result: Topix100PeakWinnerLoserIntradayPathResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    bundle = write_research_bundle(
        experiment_id=TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_peak_winner_loser_intraday_path_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "anchor_candidate_times": list(result.anchor_candidate_times),
            "midday_reference_time": result.midday_reference_time,
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
        TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME,
        lambda output_path: write_topix100_peak_winner_loser_intraday_path_overview_plot(
            result,
            output_path=output_path,
        ),
    )
    return bundle


def load_topix100_peak_winner_loser_intraday_path_research_bundle(
    bundle_path: str | Path,
) -> Topix100PeakWinnerLoserIntradayPathResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_peak_winner_loser_intraday_path_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_peak_winner_loser_intraday_path_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
