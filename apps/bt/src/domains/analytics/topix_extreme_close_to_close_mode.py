"""
TOPIX extreme close-to-close mode research.

The mode definition is intentionally simple:

- Look back over the latest ``X`` trading-day close-to-close returns.
- Find the return with the largest absolute magnitude.
- If that dominant return is non-negative, classify the day as ``bullish``.
- Otherwise classify it as ``bearish``.

Candidate ``X`` values are evaluated by how well the resulting modes separate
future TOPIX close-to-close returns across one or more forward horizons.
"""

from __future__ import annotations

import math
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
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _date_where_clause,
    _fetch_date_range,
    _open_analysis_connection,
)

ModeKey = Literal["bullish", "bearish"]
SampleSplit = Literal["discovery", "validation"]

DEFAULT_CANDIDATE_WINDOWS: tuple[int, ...] = tuple(range(2, 61))
DEFAULT_FUTURE_HORIZONS: tuple[int, ...] = (1, 5, 10, 20)
DEFAULT_VALIDATION_RATIO = 0.3
DEFAULT_MIN_MODE_DAYS = 60
DEFAULT_SHORT_WINDOW_MAX = 10
DEFAULT_LONG_WINDOW_MIN = 20
MODE_ORDER: tuple[ModeKey, ...] = ("bullish", "bearish")
WINDOW_SCORE_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")
MULTI_TIMEFRAME_STATE_ORDER: tuple[str, ...] = (
    "long_bullish__short_bullish",
    "long_bullish__short_bearish",
    "long_bearish__short_bullish",
    "long_bearish__short_bearish",
)
TOPIX_EXTREME_CLOSE_TO_CLOSE_MODE_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix-extreme-close-to-close-mode"
)


@dataclass(frozen=True)
class TopixExtremeCloseToCloseModeResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    candidate_windows: tuple[int, ...]
    future_horizons: tuple[int, ...]
    validation_ratio: float
    min_mode_days: int
    selected_window_days: int
    selected_short_window_days: int
    selected_long_window_days: int
    selection_metric: str
    topix_daily_df: pd.DataFrame
    mode_assignments_df: pd.DataFrame
    mode_segment_df: pd.DataFrame
    segment_summary_df: pd.DataFrame
    mode_summary_df: pd.DataFrame
    window_score_df: pd.DataFrame
    selected_window_comparison_df: pd.DataFrame
    selected_window_daily_df: pd.DataFrame
    multi_timeframe_state_daily_df: pd.DataFrame
    multi_timeframe_state_segment_df: pd.DataFrame
    multi_timeframe_state_summary_df: pd.DataFrame
    multi_timeframe_state_segment_summary_df: pd.DataFrame


def get_topix_extreme_close_to_close_mode_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def run_topix_extreme_close_to_close_mode_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    candidate_windows: Sequence[int] | None = None,
    future_horizons: Sequence[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    min_mode_days: int = DEFAULT_MIN_MODE_DAYS,
) -> TopixExtremeCloseToCloseModeResearchResult:
    resolved_windows = _normalize_positive_int_sequence(
        candidate_windows,
        default=DEFAULT_CANDIDATE_WINDOWS,
        name="candidate_windows",
    )
    resolved_horizons = _normalize_positive_int_sequence(
        future_horizons,
        default=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")
    if min_mode_days <= 0:
        raise ValueError("min_mode_days must be positive")

    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _fetch_date_range(
            ctx.connection,
            table_name="topix_data",
        )
        topix_daily_df = _query_topix_daily_frame(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            future_horizons=resolved_horizons,
        )

    prepared_topix_df = _mark_common_comparison_window(
        topix_daily_df,
        candidate_windows=resolved_windows,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
    )
    comparable_topix_df = (
        prepared_topix_df[prepared_topix_df["analysis_eligible"]]
        .copy()
        .reset_index(drop=True)
    )
    mode_assignments_df = _build_mode_assignments_df(
        prepared_topix_df,
        candidate_windows=resolved_windows,
        future_horizons=resolved_horizons,
    )
    mode_segment_df = _build_mode_segment_df(mode_assignments_df)
    segment_summary_df = _build_segment_summary_df(mode_segment_df)
    mode_summary_df = _build_mode_summary_df(
        mode_assignments_df,
        future_horizons=resolved_horizons,
    )
    window_score_df = _build_window_score_df(
        segment_summary_df,
        candidate_windows=resolved_windows,
        min_mode_days=min_mode_days,
    )
    selected_window_days = _select_best_window_days(window_score_df)
    selected_short_window_days = _select_best_window_days(
        window_score_df,
        max_window_days=DEFAULT_SHORT_WINDOW_MAX,
        fallback_window_days=selected_window_days,
    )
    selected_long_window_days = _select_best_window_days(
        window_score_df,
        min_window_days=DEFAULT_LONG_WINDOW_MIN,
        fallback_window_days=selected_window_days,
    )
    selected_window_daily_df = (
        mode_assignments_df[mode_assignments_df["window_days"] == selected_window_days]
        .copy()
        .reset_index(drop=True)
    )
    selected_window_comparison_df = _build_window_comparison_df(
        segment_summary_df,
        window_days=selected_window_days,
    )
    multi_timeframe_state_daily_df = _build_multi_timeframe_state_daily_df(
        mode_assignments_df,
        short_window_days=selected_short_window_days,
        long_window_days=selected_long_window_days,
        future_horizons=resolved_horizons,
    )
    multi_timeframe_state_segment_df = _build_multi_timeframe_state_segment_df(
        multi_timeframe_state_daily_df
    )
    multi_timeframe_state_summary_df = _build_multi_timeframe_state_summary_df(
        multi_timeframe_state_daily_df,
        future_horizons=resolved_horizons,
    )
    multi_timeframe_state_segment_summary_df = _build_multi_timeframe_state_segment_summary_df(
        multi_timeframe_state_segment_df
    )

    analysis_start_date = (
        str(comparable_topix_df["date"].iloc[0]) if not comparable_topix_df.empty else None
    )
    analysis_end_date = (
        str(comparable_topix_df["date"].iloc[-1]) if not comparable_topix_df.empty else None
    )

    return TopixExtremeCloseToCloseModeResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        candidate_windows=resolved_windows,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        min_mode_days=min_mode_days,
        selected_window_days=selected_window_days,
        selected_short_window_days=selected_short_window_days,
        selected_long_window_days=selected_long_window_days,
        selection_metric="discovery segment composite score",
        topix_daily_df=comparable_topix_df,
        mode_assignments_df=mode_assignments_df,
        mode_segment_df=mode_segment_df,
        segment_summary_df=segment_summary_df,
        mode_summary_df=mode_summary_df,
        window_score_df=window_score_df,
        selected_window_comparison_df=selected_window_comparison_df,
        selected_window_daily_df=selected_window_daily_df,
        multi_timeframe_state_daily_df=multi_timeframe_state_daily_df,
        multi_timeframe_state_segment_df=multi_timeframe_state_segment_df,
        multi_timeframe_state_summary_df=multi_timeframe_state_summary_df,
        multi_timeframe_state_segment_summary_df=multi_timeframe_state_segment_summary_df,
    )


def write_topix_extreme_close_to_close_mode_research_bundle(
    result: TopixExtremeCloseToCloseModeResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX_EXTREME_CLOSE_TO_CLOSE_MODE_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_extreme_close_to_close_mode_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "candidate_windows": list(result.candidate_windows),
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "min_mode_days": result.min_mode_days,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "candidate_windows": list(result.candidate_windows),
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "min_mode_days": result.min_mode_days,
            "selected_window_days": result.selected_window_days,
            "selected_short_window_days": result.selected_short_window_days,
            "selected_long_window_days": result.selected_long_window_days,
            "selection_metric": result.selection_metric,
        },
        result_tables={
            "topix_daily_df": result.topix_daily_df,
            "mode_assignments_df": result.mode_assignments_df,
            "mode_segment_df": result.mode_segment_df,
            "segment_summary_df": result.segment_summary_df,
            "mode_summary_df": result.mode_summary_df,
            "window_score_df": result.window_score_df,
            "selected_window_comparison_df": result.selected_window_comparison_df,
            "selected_window_daily_df": result.selected_window_daily_df,
            "multi_timeframe_state_daily_df": result.multi_timeframe_state_daily_df,
            "multi_timeframe_state_segment_df": result.multi_timeframe_state_segment_df,
            "multi_timeframe_state_summary_df": result.multi_timeframe_state_summary_df,
            "multi_timeframe_state_segment_summary_df": (
                result.multi_timeframe_state_segment_summary_df
            ),
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_extreme_close_to_close_mode_research_bundle(
    bundle_path: str | Path,
) -> TopixExtremeCloseToCloseModeResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return TopixExtremeCloseToCloseModeResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        candidate_windows=tuple(int(value) for value in metadata["candidate_windows"]),
        future_horizons=tuple(int(value) for value in metadata["future_horizons"]),
        validation_ratio=float(metadata["validation_ratio"]),
        min_mode_days=int(metadata["min_mode_days"]),
        selected_window_days=int(metadata["selected_window_days"]),
        selected_short_window_days=int(metadata["selected_short_window_days"]),
        selected_long_window_days=int(metadata["selected_long_window_days"]),
        selection_metric=str(metadata["selection_metric"]),
        topix_daily_df=tables["topix_daily_df"],
        mode_assignments_df=tables["mode_assignments_df"],
        mode_segment_df=tables["mode_segment_df"],
        segment_summary_df=tables["segment_summary_df"],
        mode_summary_df=tables["mode_summary_df"],
        window_score_df=tables["window_score_df"],
        selected_window_comparison_df=tables["selected_window_comparison_df"],
        selected_window_daily_df=tables["selected_window_daily_df"],
        multi_timeframe_state_daily_df=tables["multi_timeframe_state_daily_df"],
        multi_timeframe_state_segment_df=tables["multi_timeframe_state_segment_df"],
        multi_timeframe_state_summary_df=tables["multi_timeframe_state_summary_df"],
        multi_timeframe_state_segment_summary_df=tables[
            "multi_timeframe_state_segment_summary_df"
        ],
    )


def get_topix_extreme_close_to_close_mode_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_EXTREME_CLOSE_TO_CLOSE_MODE_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_extreme_close_to_close_mode_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_EXTREME_CLOSE_TO_CLOSE_MODE_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def build_multi_timeframe_state_tables(
    mode_assignments_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
    short_window_days: int,
    long_window_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    state_daily_df = _build_multi_timeframe_state_daily_df(
        mode_assignments_df,
        short_window_days=short_window_days,
        long_window_days=long_window_days,
        future_horizons=future_horizons,
    )
    state_segment_df = _build_multi_timeframe_state_segment_df(state_daily_df)
    state_summary_df = _build_multi_timeframe_state_summary_df(
        state_daily_df,
        future_horizons=future_horizons,
    )
    state_segment_summary_df = _build_multi_timeframe_state_segment_summary_df(
        state_segment_df
    )
    return (
        state_daily_df,
        state_segment_df,
        state_summary_df,
        state_segment_summary_df,
    )


def _normalize_positive_int_sequence(
    values: Sequence[int] | None,
    *,
    default: tuple[int, ...],
    name: str,
) -> tuple[int, ...]:
    raw_values = tuple(default if values is None else tuple(int(value) for value in values))
    if not raw_values:
        raise ValueError(f"{name} must not be empty")
    if any(value <= 0 for value in raw_values):
        raise ValueError(f"{name} must contain only positive integers")
    return tuple(sorted(set(raw_values)))


def _query_topix_daily_frame(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    where_sql, params = _date_where_clause("date", start_date, end_date)
    topix_df = cast(
        pd.DataFrame,
        conn.execute(
            f"""
            SELECT
                date,
                open,
                high,
                low,
                close
            FROM topix_data
            {where_sql}
            ORDER BY date
            """,
            params,
        ).fetchdf(),
    )
    if topix_df.empty:
        raise ValueError("No TOPIX rows were found in the selected date range")
    topix_df = topix_df.reset_index(drop=True)
    topix_df["date"] = topix_df["date"].astype(str)
    topix_df["close"] = topix_df["close"].astype(float)
    topix_df["close_return"] = topix_df["close"].pct_change()
    for horizon in future_horizons:
        future_close = topix_df["close"].shift(-horizon)
        topix_df[f"future_return_{horizon}d"] = future_close / topix_df["close"] - 1.0
        topix_df[f"future_diff_{horizon}d"] = future_close - topix_df["close"]
    return topix_df


def _mark_common_comparison_window(
    topix_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    future_horizons: Sequence[int],
    validation_ratio: float,
) -> pd.DataFrame:
    max_window = max(candidate_windows)
    max_horizon = max(future_horizons)
    required_rows = max_window + max_horizon + 1
    if len(topix_df) < required_rows:
        raise ValueError(
            "Not enough TOPIX rows for the requested windows/horizons: "
            f"need at least {required_rows}, got {len(topix_df)}"
        )
    comparable_index = topix_df.index[max_window : len(topix_df) - max_horizon]
    if len(comparable_index) == 0:
        raise ValueError("No comparable TOPIX rows remained after warmup/horizon trimming")

    split_labels = _build_sample_split_labels(
        len(comparable_index),
        validation_ratio=validation_ratio,
    )
    prepared_df = topix_df.copy()
    prepared_df["sample_split"] = "excluded"
    prepared_df["analysis_eligible"] = False
    prepared_df.loc[comparable_index, "sample_split"] = split_labels
    prepared_df.loc[comparable_index, "analysis_eligible"] = True
    return prepared_df.reset_index(drop=True)


def _build_sample_split_labels(
    sample_count: int,
    *,
    validation_ratio: float,
) -> list[str]:
    if sample_count <= 0:
        raise ValueError("sample_count must be positive")

    validation_count = int(round(sample_count * validation_ratio))
    if validation_ratio > 0.0 and validation_count == 0 and sample_count >= 2:
        validation_count = 1
    if validation_count >= sample_count and sample_count >= 2:
        validation_count = sample_count - 1
    discovery_count = sample_count - validation_count
    if discovery_count <= 0:
        raise ValueError("discovery split would be empty; reduce validation_ratio")
    return (["discovery"] * discovery_count) + (["validation"] * validation_count)


def _build_mode_assignments_df(
    prepared_topix_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    full_topix_df = prepared_topix_df.reset_index(drop=True)
    eligible_indices = full_topix_df.index[full_topix_df["analysis_eligible"]].to_numpy(dtype=int)
    if len(eligible_indices) == 0:
        raise ValueError("No comparable TOPIX rows were marked as analysis eligible")
    returns = full_topix_df["close_return"].to_numpy(dtype=float)
    dates = full_topix_df["date"].to_numpy(dtype=object)
    splits = full_topix_df["sample_split"].to_numpy(dtype=object)
    close = full_topix_df["close"].to_numpy(dtype=float)
    future_returns = {
        horizon: full_topix_df[f"future_return_{horizon}d"].to_numpy(dtype=float)
        for horizon in future_horizons
    }
    future_diffs = {
        horizon: full_topix_df[f"future_diff_{horizon}d"].to_numpy(dtype=float)
        for horizon in future_horizons
    }

    assignment_frames: list[pd.DataFrame] = []
    for window_days in candidate_windows:
        end_indices, dominant_returns_all, dominant_event_indices_all = _extract_dominant_returns(
            returns,
            window_days=window_days,
        )
        start_pos = int(eligible_indices[0] - window_days)
        stop_pos = start_pos + len(eligible_indices)
        if start_pos < 0 or stop_pos > len(end_indices):
            raise ValueError("Eligible TOPIX window alignment failed")
        aligned_end_indices = end_indices[start_pos:stop_pos]
        if not np.array_equal(aligned_end_indices, eligible_indices):
            raise ValueError("Eligible TOPIX window indices do not align across candidates")
        dominant_returns = dominant_returns_all[start_pos:stop_pos]
        dominant_event_indices = dominant_event_indices_all[start_pos:stop_pos]
        window_df = pd.DataFrame(
            {
                "date": dates[eligible_indices],
                "sample_split": splits[eligible_indices],
                "window_days": window_days,
                "close": close[eligible_indices],
                "close_return": returns[eligible_indices],
                "mode": np.where(dominant_returns >= 0.0, "bullish", "bearish"),
                "dominant_close_return": dominant_returns,
                "dominant_abs_close_return": np.abs(dominant_returns),
                "dominant_event_date": dates[dominant_event_indices],
            }
        )
        for horizon in future_horizons:
            window_df[f"future_return_{horizon}d"] = future_returns[horizon][eligible_indices]
            window_df[f"future_diff_{horizon}d"] = future_diffs[horizon][eligible_indices]
        assignment_frames.append(window_df)

    mode_assignments_df = pd.concat(assignment_frames, ignore_index=True)
    mode_assignments_df["mode"] = pd.Categorical(
        mode_assignments_df["mode"],
        categories=list(MODE_ORDER),
        ordered=True,
    )
    return mode_assignments_df


def _extract_dominant_returns(
    close_returns: np.ndarray,
    *,
    window_days: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    valid_returns = close_returns[1:]
    if len(valid_returns) < window_days:
        raise ValueError("close_returns is too short for the requested window_days")
    window_view = np.lib.stride_tricks.sliding_window_view(valid_returns, window_days)
    dominant_offsets = np.abs(window_view).argmax(axis=1)
    row_offsets = np.arange(window_view.shape[0], dtype=int)
    dominant_returns = window_view[row_offsets, dominant_offsets]
    end_indices = row_offsets + window_days
    dominant_event_indices = row_offsets + dominant_offsets + 1
    return end_indices, dominant_returns, dominant_event_indices


def _build_mode_summary_df(
    mode_assignments_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", mode_assignments_df)]
    for split_name in ("discovery", "validation"):
        split_df = mode_assignments_df[mode_assignments_df["sample_split"] == split_name]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        for horizon in future_horizons:
            return_col = f"future_return_{horizon}d"
            grouped = (
                split_df.groupby(["window_days", "mode"], observed=True)[return_col]
                .agg(
                    sample_count="count",
                    mean_future_return="mean",
                    median_future_return="median",
                    std_future_return="std",
                    mean_abs_future_return=lambda values: float(values.abs().mean()),
                    up_count=lambda values: int((values > 0).sum()),
                    down_count=lambda values: int((values < 0).sum()),
                    flat_count=lambda values: int((values == 0).sum()),
                )
                .reset_index()
            )
            if grouped.empty:
                continue
            grouped["sample_split"] = split_name
            grouped["horizon_days"] = horizon
            grouped["hit_rate_positive"] = grouped["up_count"] / grouped["sample_count"]
            grouped["hit_rate_negative"] = grouped["down_count"] / grouped["sample_count"]
            summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build any mode summary rows")

    mode_summary_df = pd.concat(summary_frames, ignore_index=True)
    return mode_summary_df[
        [
            "sample_split",
            "window_days",
            "horizon_days",
            "mode",
            "sample_count",
            "up_count",
            "down_count",
            "flat_count",
            "hit_rate_positive",
            "hit_rate_negative",
            "mean_future_return",
            "median_future_return",
            "std_future_return",
            "mean_abs_future_return",
        ]
    ]


def _build_mode_segment_df(mode_assignments_df: pd.DataFrame) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", mode_assignments_df)]
    for split_name in ("discovery", "validation"):
        split_df = mode_assignments_df[mode_assignments_df["sample_split"] == split_name]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    segment_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        for window_days in sorted(split_df["window_days"].unique()):
            window_df = (
                split_df[split_df["window_days"] == window_days]
                .sort_values("date", kind="stable")
                .reset_index(drop=True)
            )
            if window_df.empty:
                continue
            segment_keys = (
                window_df["mode"].astype(str)
                != window_df["mode"].astype(str).shift(fill_value=window_df["mode"].iloc[0])
            ).cumsum()
            grouped = (
                window_df.groupby(segment_keys, observed=True)
                .agg(
                    mode=("mode", "first"),
                    start_date=("date", "first"),
                    end_date=("date", "last"),
                    start_close=("close", "first"),
                    end_close=("close", "last"),
                    segment_day_count=("date", "count"),
                    mean_daily_close_return=("close_return", "mean"),
                    mean_dominant_close_return=("dominant_close_return", "mean"),
                    max_dominant_abs_close_return=("dominant_abs_close_return", "max"),
                )
                .reset_index(drop=True)
            )
            grouped["sample_split"] = split_name
            grouped["window_days"] = int(window_days)
            grouped["segment_id"] = np.arange(1, len(grouped) + 1, dtype=int)
            grouped["segment_return"] = grouped["end_close"] / grouped["start_close"] - 1.0
            segment_frames.append(grouped)

    if not segment_frames:
        raise ValueError("Failed to build any mode segment rows")
    return pd.concat(segment_frames, ignore_index=True)


def _build_segment_summary_df(mode_segment_df: pd.DataFrame) -> pd.DataFrame:
    summary_df = (
        mode_segment_df.groupby(["sample_split", "window_days", "mode"], observed=True)
        .agg(
            segment_count=("segment_id", "count"),
            total_segment_days=("segment_day_count", "sum"),
            mean_segment_day_count=("segment_day_count", "mean"),
            median_segment_day_count=("segment_day_count", "median"),
            mean_segment_return=("segment_return", "mean"),
            median_segment_return=("segment_return", "median"),
            std_segment_return=("segment_return", "std"),
            positive_segment_count=(
                "segment_return",
                lambda values: int((values > 0).sum()),
            ),
            negative_segment_count=(
                "segment_return",
                lambda values: int((values < 0).sum()),
            ),
            flat_segment_count=(
                "segment_return",
                lambda values: int((values == 0).sum()),
            ),
        )
        .reset_index()
    )
    summary_df["positive_segment_ratio"] = (
        summary_df["positive_segment_count"] / summary_df["segment_count"]
    )
    summary_df["negative_segment_ratio"] = (
        summary_df["negative_segment_count"] / summary_df["segment_count"]
    )
    return summary_df


def _build_window_score_df(
    segment_summary_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    min_mode_days: int,
) -> pd.DataFrame:
    summary_lookup: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in segment_summary_df.to_dict(orient="records"):
        summary_lookup[
            (
                str(row["sample_split"]),
                int(row["window_days"]),
                str(row["mode"]),
            )
        ] = row

    score_rows: list[dict[str, Any]] = []
    for split_name in WINDOW_SCORE_SPLIT_ORDER:
        for window_days in candidate_windows:
            bullish_row = summary_lookup.get((split_name, window_days, "bullish"))
            bearish_row = summary_lookup.get((split_name, window_days, "bearish"))
            if bullish_row is None or bearish_row is None:
                continue

            bullish_count = int(bullish_row["total_segment_days"])
            bearish_count = int(bearish_row["total_segment_days"])
            bullish_segment_count = int(bullish_row["segment_count"])
            bearish_segment_count = int(bearish_row["segment_count"])
            total_count = bullish_count + bearish_count
            balance_ratio = (
                min(bullish_count, bearish_count) / max(bullish_count, bearish_count)
                if bullish_count > 0 and bearish_count > 0
                else 0.0
            )
            bullish_share = bullish_count / total_count if total_count > 0 else math.nan

            bullish_mean = float(bullish_row["mean_segment_return"])
            bearish_mean = float(bearish_row["mean_segment_return"])
            bullish_std = (
                float(bullish_row["std_segment_return"])
                if pd.notna(bullish_row["std_segment_return"])
                else 0.0
            )
            bearish_std = (
                float(bearish_row["std_segment_return"])
                if pd.notna(bearish_row["std_segment_return"])
                else 0.0
            )
            pooled_std = math.sqrt((bullish_std**2 + bearish_std**2) / 2.0)
            mean_return_separation = bullish_mean - bearish_mean
            mean_effect_size = (
                mean_return_separation / pooled_std if pooled_std > 0 else 0.0
            )
            mean_directional_accuracy = (
                float(bullish_row["positive_segment_ratio"])
                + float(bearish_row["negative_segment_ratio"])
            ) / 2.0
            directionally_clean = bullish_mean > 0.0 and bearish_mean < 0.0
            directional_consistency = 1.0 if directionally_clean else 0.0
            weighting = (
                1.0 if directionally_clean else (0.5 if mean_return_separation > 0.0 else 0.0)
            )
            composite_score = mean_effect_size * mean_directional_accuracy * weighting * balance_ratio
            selection_eligible = (
                split_name == "discovery"
                and bullish_count >= min_mode_days
                and bearish_count >= min_mode_days
                and bullish_segment_count > 0
                and bearish_segment_count > 0
            )
            selection_score = composite_score if selection_eligible else math.nan

            score_rows.append(
                {
                    "sample_split": split_name,
                    "window_days": window_days,
                    "bullish_count": bullish_count,
                    "bearish_count": bearish_count,
                    "bullish_segment_count": bullish_segment_count,
                    "bearish_segment_count": bearish_segment_count,
                    "bullish_share": bullish_share,
                    "balance_ratio": balance_ratio,
                    "valid_horizon_count": 1,
                    "directional_consistency": directional_consistency,
                    "mean_directional_accuracy": mean_directional_accuracy,
                    "mean_effect_size": mean_effect_size,
                    "mean_return_separation": mean_return_separation,
                    "composite_score": composite_score,
                    "selection_eligible": selection_eligible,
                    "selection_score": selection_score,
                    "selection_rank": pd.NA,
                }
            )

    window_score_df = pd.DataFrame(score_rows)
    discovery_rank_df = (
        window_score_df[
            (window_score_df["sample_split"] == "discovery")
            & window_score_df["selection_eligible"]
            & window_score_df["selection_score"].notna()
        ]
        .sort_values(
            [
                "selection_score",
                "directional_consistency",
                "balance_ratio",
                "window_days",
            ],
            ascending=[False, False, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    rank_lookup = {
        int(row["window_days"]): index + 1
        for index, row in discovery_rank_df.iterrows()
    }
    selection_rank_values = []
    for row in window_score_df.to_dict(orient="records"):
        if str(row["sample_split"]) == "discovery":
            selection_rank_values.append(rank_lookup.get(int(row["window_days"]), pd.NA))
        else:
            selection_rank_values.append(pd.NA)
    window_score_df["selection_rank"] = selection_rank_values
    return window_score_df


def _select_best_window_days(
    window_score_df: pd.DataFrame,
    *,
    min_window_days: int | None = None,
    max_window_days: int | None = None,
    fallback_window_days: int | None = None,
) -> int:
    candidates_df = window_score_df[
        (window_score_df["sample_split"] == "discovery")
        & window_score_df["selection_eligible"]
        & window_score_df["selection_score"].notna()
    ].copy()
    if min_window_days is not None:
        candidates_df = candidates_df[candidates_df["window_days"] >= min_window_days]
    if max_window_days is not None:
        candidates_df = candidates_df[candidates_df["window_days"] <= max_window_days]
    if candidates_df.empty:
        if fallback_window_days is not None:
            return int(fallback_window_days)
        raise ValueError("No eligible discovery windows were available for selection")
    candidates_df = candidates_df.sort_values(
        [
            "selection_score",
            "directional_consistency",
            "balance_ratio",
            "window_days",
        ],
        ascending=[False, False, False, True],
        kind="stable",
    )
    return int(candidates_df.iloc[0]["window_days"])


def _build_window_comparison_df(
    segment_summary_df: pd.DataFrame,
    *,
    window_days: int,
) -> pd.DataFrame:
    summary_df = segment_summary_df[segment_summary_df["window_days"] == window_days].copy()
    comparison_rows: list[dict[str, Any]] = []
    for split_name in WINDOW_SCORE_SPLIT_ORDER:
        split_df = summary_df[summary_df["sample_split"] == split_name]
        if split_df.empty:
            continue
        bullish_row = split_df[split_df["mode"] == "bullish"]
        bearish_row = split_df[split_df["mode"] == "bearish"]
        if bullish_row.empty or bearish_row.empty:
            continue
        bullish = bullish_row.iloc[0]
        bearish = bearish_row.iloc[0]
        bullish_mean = float(bullish["mean_segment_return"])
        bearish_mean = float(bearish["mean_segment_return"])
        comparison_rows.append(
            {
                "sample_split": split_name,
                "window_days": window_days,
                "bullish_count": int(bullish["total_segment_days"]),
                "bearish_count": int(bearish["total_segment_days"]),
                "bullish_segment_count": int(bullish["segment_count"]),
                "bearish_segment_count": int(bearish["segment_count"]),
                "bullish_mean_segment_return": bullish_mean,
                "bearish_mean_segment_return": bearish_mean,
                "bullish_median_segment_return": float(bullish["median_segment_return"]),
                "bearish_median_segment_return": float(bearish["median_segment_return"]),
                "bullish_positive_segment_ratio": float(bullish["positive_segment_ratio"]),
                "bearish_negative_segment_ratio": float(bearish["negative_segment_ratio"]),
                "bullish_mean_segment_day_count": float(bullish["mean_segment_day_count"]),
                "bearish_mean_segment_day_count": float(bearish["mean_segment_day_count"]),
                "mean_return_separation": bullish_mean - bearish_mean,
                "directional_accuracy": (
                    float(bullish["positive_segment_ratio"])
                    + float(bearish["negative_segment_ratio"])
                )
                / 2.0,
            }
        )
    return pd.DataFrame(comparison_rows)


def _build_multi_timeframe_state_daily_df(
    mode_assignments_df: pd.DataFrame,
    *,
    short_window_days: int,
    long_window_days: int,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    short_df = (
        mode_assignments_df[mode_assignments_df["window_days"] == short_window_days]
        .copy()
        .rename(
            columns={
                "mode": "short_mode",
                "dominant_close_return": "short_dominant_close_return",
                "dominant_abs_close_return": "short_dominant_abs_close_return",
                "dominant_event_date": "short_dominant_event_date",
            }
        )
    )
    long_df = (
        mode_assignments_df[mode_assignments_df["window_days"] == long_window_days]
        .copy()
        .rename(
            columns={
                "mode": "long_mode",
                "dominant_close_return": "long_dominant_close_return",
                "dominant_abs_close_return": "long_dominant_abs_close_return",
                "dominant_event_date": "long_dominant_event_date",
            }
        )
    )
    if short_df.empty or long_df.empty:
        raise ValueError("Short/long window rows were not found in mode_assignments_df")

    short_columns = [
        "date",
        "sample_split",
        "close",
        "close_return",
        "short_mode",
        "short_dominant_close_return",
        "short_dominant_abs_close_return",
        "short_dominant_event_date",
    ]
    short_columns.extend(f"future_return_{horizon}d" for horizon in future_horizons)
    short_columns.extend(f"future_diff_{horizon}d" for horizon in future_horizons)
    long_columns = [
        "date",
        "sample_split",
        "long_mode",
        "long_dominant_close_return",
        "long_dominant_abs_close_return",
        "long_dominant_event_date",
    ]
    merged_df = short_df[short_columns].merge(
        long_df[long_columns],
        on=["date", "sample_split"],
        how="inner",
        validate="one_to_one",
    )
    merged_df["short_window_days"] = short_window_days
    merged_df["long_window_days"] = long_window_days
    merged_df["state_key"] = merged_df.apply(
        lambda row: _build_multi_timeframe_state_key(
            long_mode=str(row["long_mode"]),
            short_mode=str(row["short_mode"]),
        ),
        axis=1,
    )
    merged_df["state_label"] = merged_df["state_key"].map(_format_multi_timeframe_state_label)
    merged_df["state_key"] = pd.Categorical(
        merged_df["state_key"],
        categories=list(MULTI_TIMEFRAME_STATE_ORDER),
        ordered=True,
    )
    return merged_df


def _build_multi_timeframe_state_segment_df(
    multi_timeframe_state_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", multi_timeframe_state_daily_df)]
    for split_name in ("discovery", "validation"):
        split_df = multi_timeframe_state_daily_df[
            multi_timeframe_state_daily_df["sample_split"] == split_name
        ]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    segment_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        ordered_df = split_df.sort_values("date", kind="stable").reset_index(drop=True)
        if ordered_df.empty:
            continue
        segment_keys = (
            ordered_df["state_key"].astype(str)
            != ordered_df["state_key"].astype(str).shift(fill_value=ordered_df["state_key"].iloc[0])
        ).cumsum()
        grouped = (
            ordered_df.groupby(segment_keys, observed=True)
            .agg(
                state_key=("state_key", "first"),
                state_label=("state_label", "first"),
                long_mode=("long_mode", "first"),
                short_mode=("short_mode", "first"),
                start_date=("date", "first"),
                end_date=("date", "last"),
                start_close=("close", "first"),
                end_close=("close", "last"),
                segment_day_count=("date", "count"),
                mean_daily_close_return=("close_return", "mean"),
                mean_short_dominant_close_return=("short_dominant_close_return", "mean"),
                mean_long_dominant_close_return=("long_dominant_close_return", "mean"),
            )
            .reset_index(drop=True)
        )
        grouped["sample_split"] = split_name
        grouped["short_window_days"] = int(ordered_df["short_window_days"].iloc[0])
        grouped["long_window_days"] = int(ordered_df["long_window_days"].iloc[0])
        grouped["segment_id"] = np.arange(1, len(grouped) + 1, dtype=int)
        grouped["segment_return"] = grouped["end_close"] / grouped["start_close"] - 1.0
        segment_frames.append(grouped)

    if not segment_frames:
        raise ValueError("Failed to build any multi-timeframe state segments")
    return pd.concat(segment_frames, ignore_index=True)


def _build_multi_timeframe_state_summary_df(
    multi_timeframe_state_daily_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", multi_timeframe_state_daily_df)]
    for split_name in ("discovery", "validation"):
        split_df = multi_timeframe_state_daily_df[
            multi_timeframe_state_daily_df["sample_split"] == split_name
        ]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        for horizon in future_horizons:
            return_col = f"future_return_{horizon}d"
            grouped = (
                split_df.groupby(["state_key", "state_label", "long_mode", "short_mode"], observed=True)[
                    return_col
                ]
                .agg(
                    day_count="count",
                    mean_future_return="mean",
                    median_future_return="median",
                    std_future_return="std",
                    up_count=lambda values: int((values > 0).sum()),
                    down_count=lambda values: int((values < 0).sum()),
                    flat_count=lambda values: int((values == 0).sum()),
                )
                .reset_index()
            )
            if grouped.empty:
                continue
            grouped["sample_split"] = split_name
            grouped["horizon_days"] = horizon
            grouped["hit_rate_positive"] = grouped["up_count"] / grouped["day_count"]
            grouped["hit_rate_negative"] = grouped["down_count"] / grouped["day_count"]
            summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build multi-timeframe state summary rows")
    return pd.concat(summary_frames, ignore_index=True)


def _build_multi_timeframe_state_segment_summary_df(
    multi_timeframe_state_segment_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_df = (
        multi_timeframe_state_segment_df.groupby(
            ["sample_split", "state_key", "state_label", "long_mode", "short_mode"],
            observed=True,
        )
        .agg(
            segment_count=("segment_id", "count"),
            total_segment_days=("segment_day_count", "sum"),
            mean_segment_day_count=("segment_day_count", "mean"),
            median_segment_day_count=("segment_day_count", "median"),
            mean_segment_return=("segment_return", "mean"),
            median_segment_return=("segment_return", "median"),
            std_segment_return=("segment_return", "std"),
            positive_segment_count=(
                "segment_return",
                lambda values: int((values > 0).sum()),
            ),
            negative_segment_count=(
                "segment_return",
                lambda values: int((values < 0).sum()),
            ),
            flat_segment_count=(
                "segment_return",
                lambda values: int((values == 0).sum()),
            ),
        )
        .reset_index()
    )
    summary_df["positive_segment_ratio"] = (
        summary_df["positive_segment_count"] / summary_df["segment_count"]
    )
    summary_df["negative_segment_ratio"] = (
        summary_df["negative_segment_count"] / summary_df["segment_count"]
    )
    return summary_df


def _build_multi_timeframe_state_key(*, long_mode: str, short_mode: str) -> str:
    return f"long_{long_mode}__short_{short_mode}"


def _format_multi_timeframe_state_label(state_key: str) -> str:
    return state_key.replace("long_", "Long ").replace("__short_", " / Short ").replace(
        "_", " "
    ).title()


def _build_research_bundle_summary_markdown(
    result: TopixExtremeCloseToCloseModeResearchResult,
) -> str:
    discovery_scores = (
        result.window_score_df[
            result.window_score_df["sample_split"] == "discovery"
        ]
        .sort_values(
            [
                "selection_score",
                "directional_consistency",
                "balance_ratio",
                "window_days",
            ],
            ascending=[False, False, False, True],
            kind="stable",
        )
        .head(5)
    )
    selected_comparison = result.selected_window_comparison_df[
        result.selected_window_comparison_df["sample_split"].isin(["discovery", "validation"])
    ].copy()

    candidate_window_label = _format_int_sequence(result.candidate_windows)
    future_horizon_label = _format_int_sequence(result.future_horizons)
    lines = [
        "# TOPIX Extreme Close-to-Close Mode",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Candidate windows: `{candidate_window_label}`",
        f"- Future horizons: `{future_horizon_label}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Minimum mode days for selection: `{result.min_mode_days}`",
        "",
        "## Selected X",
        "",
        f"- Selected overall window days: `{result.selected_window_days}`",
        f"- Selected short window days: `{result.selected_short_window_days}`",
        f"- Selected long window days: `{result.selected_long_window_days}`",
        f"- Selection metric: `{result.selection_metric}`",
    ]

    selected_validation_rows = selected_comparison[
        selected_comparison["sample_split"] == "validation"
    ]
    if not selected_validation_rows.empty:
        lines.extend(["", "## Validation Mode-Segment Readout", ""])
        for row in selected_validation_rows.to_dict(orient="records"):
            lines.append(
                "- "
                f"bull={_format_return(float(row['bullish_mean_segment_return']))}, "
                f"bear={_format_return(float(row['bearish_mean_segment_return']))}, "
                f"spread={_format_return(float(row['mean_return_separation']))}, "
                f"dir-acc={float(row['directional_accuracy']):.1%}, "
                f"bull-days={float(row['bullish_mean_segment_day_count']):.1f}, "
                f"bear-days={float(row['bearish_mean_segment_day_count']):.1f}"
            )

    validation_forward_rows = result.mode_summary_df[
        (result.mode_summary_df["sample_split"] == "validation")
        & (result.mode_summary_df["window_days"] == result.selected_window_days)
    ].copy()
    if not validation_forward_rows.empty:
        lines.extend(["", "## Validation Forward Snapshot", ""])
        for horizon_days in sorted(validation_forward_rows["horizon_days"].unique()):
            horizon_df = validation_forward_rows[
                validation_forward_rows["horizon_days"] == horizon_days
            ]
            bullish_df = horizon_df[horizon_df["mode"] == "bullish"]
            bearish_df = horizon_df[horizon_df["mode"] == "bearish"]
            if bullish_df.empty or bearish_df.empty:
                continue
            bullish_row = bullish_df.iloc[0]
            bearish_row = bearish_df.iloc[0]
            lines.append(
                "- "
                f"{int(horizon_days)}d: "
                f"bull={_format_return(float(bullish_row['mean_future_return']))}, "
                f"bear={_format_return(float(bearish_row['mean_future_return']))}, "
                f"spread={_format_return(float(bullish_row['mean_future_return']) - float(bearish_row['mean_future_return']))}"
            )

    if not discovery_scores.empty:
        lines.extend(["", "## Top Discovery Candidates", ""])
        for row in discovery_scores.to_dict(orient="records"):
            selection_score = row["selection_score"]
            selection_text = (
                "N/A"
                if selection_score is None or pd.isna(selection_score)
                else f"{float(selection_score):.4f}"
            )
            lines.append(
                "- "
                f"X={int(row['window_days'])}: "
                f"score={selection_text}, "
                f"consistency={float(row['directional_consistency']):.1%}, "
                f"balance={float(row['balance_ratio']):.1%}"
            )

    validation_state_segments = result.multi_timeframe_state_segment_summary_df[
        result.multi_timeframe_state_segment_summary_df["sample_split"] == "validation"
    ].copy()
    if not validation_state_segments.empty:
        lines.extend(
            [
                "",
                "## Validation 4-State Segment Matrix",
                "",
            ]
        )
        for state_key in MULTI_TIMEFRAME_STATE_ORDER:
            state_df = validation_state_segments[
                validation_state_segments["state_key"] == state_key
            ]
            if state_df.empty:
                continue
            row = state_df.iloc[0]
            lines.append(
                "- "
                f"{str(row['state_label'])}: "
                f"seg-ret={_format_return(float(row['mean_segment_return']))}, "
                f"seg-days={float(row['mean_segment_day_count']):.1f}, "
                f"positive={float(row['positive_segment_ratio']):.1%}"
            )

    validation_state_forward = result.multi_timeframe_state_summary_df[
        (result.multi_timeframe_state_summary_df["sample_split"] == "validation")
        & (result.multi_timeframe_state_summary_df["horizon_days"] == 5)
    ].copy()
    if not validation_state_forward.empty:
        lines.extend(["", "## Validation 4-State Forward 5D", ""])
        for state_key in MULTI_TIMEFRAME_STATE_ORDER:
            state_df = validation_state_forward[
                validation_state_forward["state_key"] == state_key
            ]
            if state_df.empty:
                continue
            row = state_df.iloc[0]
            lines.append(
                "- "
                f"{str(row['state_label'])}: "
                f"mean={_format_return(float(row['mean_future_return']))}, "
                f"hit+={float(row['hit_rate_positive']):.1%}, "
                f"days={int(row['day_count'])}"
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `topix_daily_df`",
            "- `mode_assignments_df`",
            "- `mode_segment_df`",
            "- `segment_summary_df`",
            "- `mode_summary_df`",
            "- `window_score_df`",
            "- `selected_window_comparison_df`",
            "- `selected_window_daily_df`",
            "- `multi_timeframe_state_daily_df`",
            "- `multi_timeframe_state_segment_df`",
            "- `multi_timeframe_state_summary_df`",
            "- `multi_timeframe_state_segment_summary_df`",
        ]
    )
    return "\n".join(lines)


def _format_int_sequence(values: Sequence[int]) -> str:
    if not values:
        return ""
    if len(values) > 10:
        return f"{values[0]}..{values[-1]} ({len(values)} values)"
    return ",".join(str(value) for value in values)


def _format_return(value: float) -> str:
    return f"{value * 100.0:.2f}%"
