"""
TOPIX100 transfer study for the fixed streak 3/53 state model.

This study does not re-optimize parameters on TOPIX100. It takes the short/long
streak-candle pair discovered on TOPIX itself (3 / 53) and applies the same
four-state labeling to each TOPIX100 constituent's own price series.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_FUTURE_HORIZONS,
    DEFAULT_VALIDATION_RATIO,
    _build_streak_tables,
    _classify_close_return_mode,
    _mark_common_comparison_window,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _query_topix100_date_range,
    _query_topix100_stock_history,
)
from src.domains.analytics.topix_streak_extreme_mode import (
    _build_mode_assignments_df,
    _build_sample_split_labels,
    _format_int_sequence,
    _format_return,
    _prepare_streak_candle_frame,
)
from src.domains.analytics.topix_streak_multi_timeframe_mode import (
    MULTI_TIMEFRAME_STATE_ORDER,
    _build_multi_timeframe_state_key,
    _build_multi_timeframe_state_streak_df,
    _format_multi_timeframe_state_label,
)

DEFAULT_SHORT_WINDOW_STREAKS = 3
DEFAULT_LONG_WINDOW_STREAKS = 53
DEFAULT_MIN_STOCK_EVENTS_PER_STATE = 3
DEFAULT_MIN_CONSTITUENTS_PER_DATE_STATE = 8
TOPIX100_STREAK_353_TRANSFER_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-transfer"
)
_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "state_event_df",
    "state_horizon_event_df",
    "state_event_summary_df",
    "state_date_panel_df",
    "state_date_summary_df",
    "stock_state_mean_df",
    "state_stock_consistency_df",
)

_STATE_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    "code",
    "company_name",
    "date",
    "segment_id",
    "current_streak_mode",
    "current_streak_day_count",
    "current_streak_segment_return",
    "current_streak_segment_abs_return",
    "short_mode",
    "long_mode",
    "state_key",
    "state_label",
    "short_window_streaks",
    "long_window_streaks",
)

_DAILY_STATE_PANEL_COLUMNS: tuple[str, ...] = (
    "state_event_id",
    "code",
    "company_name",
    "date",
    "sample_split",
    "segment_id",
    "base_streak_mode",
    "segment_day_count",
    "segment_return",
    "segment_abs_return",
    "short_mode",
    "long_mode",
    "state_key",
    "state_label",
    "current_streak_mode",
    "current_streak_day_count",
    "current_streak_segment_return",
    "current_streak_segment_abs_return",
    "short_window_streaks",
    "long_window_streaks",
)


@dataclass(frozen=True)
class Topix100Streak353TransferResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    short_window_streaks: int
    long_window_streaks: int
    future_horizons: tuple[int, ...]
    validation_ratio: float
    min_stock_events_per_state: int
    min_constituents_per_date_state: int
    topix100_constituent_count: int
    covered_constituent_count: int
    valid_event_count: int
    valid_date_count: int
    state_event_df: pd.DataFrame
    state_horizon_event_df: pd.DataFrame
    state_event_summary_df: pd.DataFrame
    state_date_panel_df: pd.DataFrame
    state_date_summary_df: pd.DataFrame
    stock_state_mean_df: pd.DataFrame
    state_stock_consistency_df: pd.DataFrame


def get_topix100_streak_353_transfer_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _query_topix100_date_range(ctx.connection)


def run_topix100_streak_353_transfer_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    future_horizons: Sequence[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    min_stock_events_per_state: int = DEFAULT_MIN_STOCK_EVENTS_PER_STATE,
    min_constituents_per_date_state: int = DEFAULT_MIN_CONSTITUENTS_PER_DATE_STATE,
) -> Topix100Streak353TransferResearchResult:
    resolved_horizons = _normalize_positive_int_sequence(
        future_horizons,
        default=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    if short_window_streaks <= 0 or long_window_streaks <= 0:
        raise ValueError("short_window_streaks and long_window_streaks must be positive")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")
    if min_stock_events_per_state <= 0:
        raise ValueError("min_stock_events_per_state must be positive")
    if min_constituents_per_date_state <= 0:
        raise ValueError("min_constituents_per_date_state must be positive")

    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _query_topix100_date_range(
            ctx.connection
        )
        history_df = _query_topix100_stock_history(
            ctx.connection,
            end_date=end_date,
        )

    if history_df.empty:
        raise ValueError("No TOPIX100 constituent stock history was available")

    history_df = history_df.copy()
    history_df["date"] = history_df["date"].astype(str)
    if start_date is not None:
        history_df = history_df[history_df["date"] >= start_date].copy()
    if end_date is not None:
        history_df = history_df[history_df["date"] <= end_date].copy()
    if history_df.empty:
        raise ValueError("No TOPIX100 constituent rows remained after date filters")

    state_event_df = _build_state_event_df(
        history_df,
        future_horizons=resolved_horizons,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
    )
    state_event_df = _assign_global_sample_split(
        state_event_df,
        validation_ratio=validation_ratio,
    )
    state_horizon_event_df = _build_state_horizon_event_df(
        state_event_df,
        future_horizons=resolved_horizons,
    )
    state_event_summary_df = _build_state_event_summary_df(state_horizon_event_df)
    state_date_panel_df = _build_state_date_panel_df(
        state_horizon_event_df,
        min_constituents_per_date_state=min_constituents_per_date_state,
    )
    state_date_summary_df = _build_state_date_summary_df(state_date_panel_df)
    stock_state_mean_df = _build_stock_state_mean_df(
        state_horizon_event_df,
        min_stock_events_per_state=min_stock_events_per_state,
    )
    state_stock_consistency_df = _build_state_stock_consistency_df(stock_state_mean_df)

    analysis_start_date = (
        str(state_event_df["segment_end_date"].iloc[0]) if not state_event_df.empty else None
    )
    analysis_end_date = (
        str(state_event_df["segment_end_date"].iloc[-1]) if not state_event_df.empty else None
    )

    return Topix100Streak353TransferResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        min_stock_events_per_state=min_stock_events_per_state,
        min_constituents_per_date_state=min_constituents_per_date_state,
        topix100_constituent_count=int(history_df["code"].nunique()),
        covered_constituent_count=int(state_event_df["code"].nunique()),
        valid_event_count=int(len(state_event_df)),
        valid_date_count=int(state_event_df["segment_end_date"].nunique()),
        state_event_df=state_event_df,
        state_horizon_event_df=state_horizon_event_df,
        state_event_summary_df=state_event_summary_df,
        state_date_panel_df=state_date_panel_df,
        state_date_summary_df=state_date_summary_df,
        stock_state_mean_df=stock_state_mean_df,
        state_stock_consistency_df=state_stock_consistency_df,
    )


def write_topix100_streak_353_transfer_research_bundle(
    result: Topix100Streak353TransferResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_TRANSFER_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_transfer_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "min_stock_events_per_state": result.min_stock_events_per_state,
            "min_constituents_per_date_state": result.min_constituents_per_date_state,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_transfer_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353TransferResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353TransferResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_transfer_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_TRANSFER_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_transfer_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_TRANSFER_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _prepare_stock_daily_frame(
    stock_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    prepared_df = stock_df.copy()
    prepared_df["date"] = prepared_df["date"].astype(str)
    prepared_df = prepared_df.sort_values("date", kind="stable").reset_index(drop=True)
    close = prepared_df["close"].astype(float)
    prepared_df["close_return"] = close.div(close.shift(1)).sub(1.0)
    for horizon in future_horizons:
        future_close = close.shift(-horizon).astype(float)
        prepared_df[f"future_return_{horizon}d"] = future_close.div(close).sub(1.0)
        prepared_df[f"future_diff_{horizon}d"] = future_close.sub(close)
    return prepared_df


def _build_state_event_df(
    history_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
    short_window_streaks: int,
    long_window_streaks: int,
) -> pd.DataFrame:
    candidate_windows = tuple(sorted({short_window_streaks, long_window_streaks}))
    state_frames: list[pd.DataFrame] = []
    grouped_history = history_df.groupby(["code", "company_name"], sort=False, observed=True)
    for (code, company_name), stock_df in grouped_history:
        prepared_daily_df = _prepare_stock_daily_frame(
            stock_df,
            future_horizons=future_horizons,
        )
        if len(prepared_daily_df) < (max(future_horizons) + 2):
            continue
        try:
            prepared_daily_df = _mark_common_comparison_window(
                prepared_daily_df,
                future_horizons=future_horizons,
                validation_ratio=0.0,
            )
            _, streak_segment_df = _build_streak_tables(
                prepared_daily_df,
                future_horizons=future_horizons,
            )
            prepared_streak_df = _prepare_streak_candle_frame(
                streak_segment_df,
                candidate_windows=candidate_windows,
                future_horizons=future_horizons,
                validation_ratio=0.0,
            )
            mode_assignments_df = _build_mode_assignments_df(
                prepared_streak_df,
                candidate_windows=candidate_windows,
                future_horizons=future_horizons,
            )
            state_df = _build_multi_timeframe_state_streak_df(
                mode_assignments_df,
                short_window_streaks=short_window_streaks,
                long_window_streaks=long_window_streaks,
                future_horizons=future_horizons,
            )
        except ValueError:
            continue

        state_df = state_df.copy()
        state_df["code"] = str(code)
        state_df["company_name"] = str(company_name)
        state_df["state_event_id"] = (
            state_df["code"].astype(str) + ":" + state_df["segment_id"].astype(int).astype(str)
        )
        state_frames.append(state_df)

    if not state_frames:
        raise ValueError("No TOPIX100 stock produced a valid streak 3/53 state panel")
    combined_df = pd.concat(state_frames, ignore_index=True)
    return _sort_state_frame(combined_df)


def build_topix100_streak_daily_state_panel_df(
    history_df: pd.DataFrame,
    *,
    analysis_start_date: str | None = None,
    analysis_end_date: str | None = None,
    validation_ratio: float | None = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
) -> pd.DataFrame:
    if short_window_streaks <= 0 or long_window_streaks <= 0:
        raise ValueError("short_window_streaks and long_window_streaks must be positive")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if validation_ratio is not None and not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")
    if history_df.empty:
        return pd.DataFrame(columns=list(_DAILY_STATE_PANEL_COLUMNS))

    filtered_history_df = history_df.copy()
    filtered_history_df["date"] = filtered_history_df["date"].astype(str)
    if analysis_end_date is not None:
        filtered_history_df = filtered_history_df[
            filtered_history_df["date"] <= analysis_end_date
        ].copy()
    if filtered_history_df.empty:
        return pd.DataFrame(columns=list(_DAILY_STATE_PANEL_COLUMNS))

    state_frames: list[pd.DataFrame] = []
    grouped_history = filtered_history_df.groupby(
        ["code", "company_name"], sort=False, observed=True
    )
    for (code, company_name), stock_df in grouped_history:
        stock_state_df = _build_stock_daily_state_panel_df(
            stock_df,
            code=str(code),
            company_name=str(company_name),
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )
        if not stock_state_df.empty:
            state_frames.append(stock_state_df)

    if not state_frames:
        return pd.DataFrame(columns=list(_DAILY_STATE_PANEL_COLUMNS))

    state_panel_df = pd.concat(state_frames, ignore_index=True)
    if analysis_start_date is not None:
        state_panel_df = state_panel_df[state_panel_df["date"] >= analysis_start_date].copy()
    if analysis_end_date is not None:
        state_panel_df = state_panel_df[state_panel_df["date"] <= analysis_end_date].copy()
    if state_panel_df.empty:
        return pd.DataFrame(columns=list(_DAILY_STATE_PANEL_COLUMNS))

    if validation_ratio is None:
        state_panel_df["sample_split"] = "full"
    else:
        state_panel_df = _assign_daily_sample_split(
            state_panel_df,
            validation_ratio=validation_ratio,
        )

    ordered_columns = [
        column for column in _DAILY_STATE_PANEL_COLUMNS if column in state_panel_df.columns
    ]
    return state_panel_df[ordered_columns].sort_values(
        ["sample_split", "date", "code"],
        kind="stable",
    ).reset_index(drop=True)


def build_topix100_streak_state_snapshot_df(
    history_df: pd.DataFrame,
    *,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
) -> pd.DataFrame:
    if short_window_streaks <= 0 or long_window_streaks <= 0:
        raise ValueError("short_window_streaks and long_window_streaks must be positive")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if history_df.empty:
        return pd.DataFrame(columns=list(_STATE_SNAPSHOT_COLUMNS))

    state_panel_df = build_topix100_streak_daily_state_panel_df(
        history_df,
        validation_ratio=None,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
    )
    if state_panel_df.empty:
        return pd.DataFrame(columns=list(_STATE_SNAPSHOT_COLUMNS))

    latest_state_df = (
        state_panel_df.sort_values(["code", "date"], kind="stable")
        .groupby("code", observed=True, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )
    snapshot_df = latest_state_df[
        [
            "code",
            "company_name",
            "date",
            "segment_id",
            "current_streak_mode",
            "current_streak_day_count",
            "current_streak_segment_return",
            "current_streak_segment_abs_return",
            "short_mode",
            "long_mode",
            "state_key",
            "state_label",
            "short_window_streaks",
            "long_window_streaks",
        ]
    ].copy()
    return snapshot_df.sort_values(["date", "code"], kind="stable").reset_index(drop=True)


def _build_stock_daily_state_panel_df(
    stock_df: pd.DataFrame,
    *,
    code: str,
    company_name: str,
    short_window_streaks: int,
    long_window_streaks: int,
) -> pd.DataFrame:
    prepared_df = stock_df.copy()
    prepared_df["date"] = prepared_df["date"].astype(str)
    prepared_df = prepared_df.sort_values("date", kind="stable").reset_index(drop=True)
    if len(prepared_df) < 2:
        return pd.DataFrame(columns=list(_DAILY_STATE_PANEL_COLUMNS))

    close_values = prepared_df["close"].astype(float).tolist()
    date_values = prepared_df["date"].astype(str).tolist()
    current_mode: str | None = None
    current_segment_start_index = -1
    current_segment_id = 1
    segment_returns: list[float] = []
    row_records: list[dict[str, Any]] = []

    for row_index in range(1, len(prepared_df)):
        prior_close = close_values[row_index - 1]
        current_close = close_values[row_index]
        if prior_close == 0.0:
            continue

        close_return = current_close / prior_close - 1.0
        mode = _classify_close_return_mode(close_return)
        if current_mode is None or mode != current_mode:
            current_segment_id += 1
            current_mode = mode
            current_segment_start_index = row_index
            segment_returns.append(0.0)

        anchor_close = close_values[current_segment_start_index - 1]
        if anchor_close == 0.0:
            continue

        current_segment_return = current_close / anchor_close - 1.0
        segment_returns[-1] = current_segment_return
        current_segment_day_count = row_index - current_segment_start_index + 1
        if len(segment_returns) < long_window_streaks:
            continue

        short_mode = _resolve_window_mode_from_segment_returns(
            segment_returns,
            window_streaks=short_window_streaks,
        )
        long_mode = _resolve_window_mode_from_segment_returns(
            segment_returns,
            window_streaks=long_window_streaks,
        )
        state_key = _build_multi_timeframe_state_key(
            long_mode=long_mode,
            short_mode=short_mode,
        )
        row_records.append(
            {
                "state_event_id": f"{code}:{date_values[row_index]}",
                "code": code,
                "company_name": company_name,
                "date": date_values[row_index],
                "sample_split": "full",
                "segment_id": current_segment_id,
                "base_streak_mode": current_mode,
                "segment_day_count": current_segment_day_count,
                "segment_return": current_segment_return,
                "segment_abs_return": abs(current_segment_return),
                "short_mode": short_mode,
                "long_mode": long_mode,
                "state_key": state_key,
                "state_label": _format_multi_timeframe_state_label(state_key),
                "current_streak_mode": current_mode,
                "current_streak_day_count": current_segment_day_count,
                "current_streak_segment_return": current_segment_return,
                "current_streak_segment_abs_return": abs(current_segment_return),
                "short_window_streaks": short_window_streaks,
                "long_window_streaks": long_window_streaks,
            }
        )

    if not row_records:
        return pd.DataFrame(columns=list(_DAILY_STATE_PANEL_COLUMNS))
    return pd.DataFrame.from_records(row_records, columns=list(_DAILY_STATE_PANEL_COLUMNS))


def _resolve_window_mode_from_segment_returns(
    segment_returns: Sequence[float],
    *,
    window_streaks: int,
) -> str:
    if len(segment_returns) < window_streaks:
        raise ValueError("Not enough streak segments for the requested window")
    dominant_return = max(segment_returns[-window_streaks:], key=abs)
    return "bullish" if float(dominant_return) >= 0.0 else "bearish"


def _assign_daily_sample_split(
    state_panel_df: pd.DataFrame,
    *,
    validation_ratio: float,
) -> pd.DataFrame:
    unique_dates = sorted(state_panel_df["date"].astype(str).unique())
    split_labels = _build_sample_split_labels(
        len(unique_dates),
        validation_ratio=validation_ratio,
    )
    date_to_split = dict(zip(unique_dates, split_labels, strict=True))
    split_df = state_panel_df.copy()
    split_df["sample_split"] = split_df["date"].astype(str).map(date_to_split)
    return split_df


def _assign_global_sample_split(
    state_event_df: pd.DataFrame,
    *,
    validation_ratio: float,
) -> pd.DataFrame:
    if state_event_df.empty:
        raise ValueError("state_event_df must not be empty")

    unique_dates = sorted(state_event_df["segment_end_date"].astype(str).unique())
    split_labels = _build_sample_split_labels(
        len(unique_dates),
        validation_ratio=validation_ratio,
    )
    date_to_split = dict(zip(unique_dates, split_labels, strict=True))

    split_df = state_event_df.copy()
    split_df["sample_split"] = split_df["segment_end_date"].astype(str).map(date_to_split)
    return _sort_state_frame(split_df)


def _build_state_horizon_event_df(
    state_event_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    base_columns = [
        "state_event_id",
        "code",
        "company_name",
        "sample_split",
        "segment_id",
        "segment_start_date",
        "segment_end_date",
        "segment_return",
        "segment_day_count",
        "base_streak_mode",
        "short_mode",
        "long_mode",
        "state_key",
        "state_label",
        "short_window_streaks",
        "long_window_streaks",
    ]
    event_frames: list[pd.DataFrame] = []
    for horizon in future_horizons:
        frame = state_event_df[
            [
                *base_columns,
                f"future_return_{horizon}d",
                f"future_diff_{horizon}d",
            ]
        ].copy()
        frame = frame.rename(
            columns={
                "segment_end_date": "date",
                f"future_return_{horizon}d": "future_return",
                f"future_diff_{horizon}d": "future_diff",
            }
        )
        frame["horizon_days"] = int(horizon)
        frame = frame[frame["future_return"].notna()].copy()
        if not frame.empty:
            event_frames.append(frame)

    if not event_frames:
        raise ValueError("No future-return rows were available for the requested horizons")
    return _sort_state_frame(pd.concat(event_frames, ignore_index=True))


def _build_state_event_summary_df(
    state_horizon_event_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in _iter_split_frames(state_horizon_event_df):
        grouped = (
            split_df.groupby(
                [
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "long_mode",
                    "short_mode",
                ],
                observed=True,
            )
            .agg(
                event_count=("future_return", "count"),
                stock_count=("code", "nunique"),
                date_count=("date", "nunique"),
                mean_future_return=("future_return", "mean"),
                median_future_return=("future_return", "median"),
                std_future_return=("future_return", "std"),
                mean_abs_future_return=(
                    "future_return",
                    lambda values: float(values.abs().mean()),
                ),
                up_count=("future_return", lambda values: int((values > 0).sum())),
                down_count=("future_return", lambda values: int((values < 0).sum())),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        grouped["hit_rate_positive"] = grouped["up_count"] / grouped["event_count"]
        grouped["hit_rate_negative"] = grouped["down_count"] / grouped["event_count"]
        summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build any state event summary rows")
    return _sort_state_frame(pd.concat(summary_frames, ignore_index=True))


def _build_state_date_panel_df(
    state_horizon_event_df: pd.DataFrame,
    *,
    min_constituents_per_date_state: int,
) -> pd.DataFrame:
    panel_frames: list[pd.DataFrame] = []
    for split_name, split_df in _iter_split_frames(state_horizon_event_df):
        grouped = (
            split_df.groupby(
                [
                    "date",
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "long_mode",
                    "short_mode",
                ],
                observed=True,
            )["future_return"]
            .agg(
                constituent_count="count",
                equal_weight_return="mean",
                median_stock_return="median",
                positive_share=lambda values: float((values > 0).mean()),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        panel_frames.append(grouped)

    if not panel_frames:
        raise ValueError("Failed to build any date/state panel rows")
    date_panel_df = pd.concat(panel_frames, ignore_index=True)
    filtered_df = date_panel_df[
        date_panel_df["constituent_count"] >= min_constituents_per_date_state
    ].copy()
    if filtered_df.empty:
        raise ValueError(
            "No date/state rows satisfied min_constituents_per_date_state; lower the threshold"
        )
    return _sort_state_frame(filtered_df)


def _build_state_date_summary_df(
    state_date_panel_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in _iter_split_frames(state_date_panel_df):
        grouped = (
            split_df.groupby(
                [
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "long_mode",
                    "short_mode",
                ],
                observed=True,
            )
            .agg(
                date_count=("date", "count"),
                mean_equal_weight_return=("equal_weight_return", "mean"),
                median_equal_weight_return=("equal_weight_return", "median"),
                std_equal_weight_return=("equal_weight_return", "std"),
                positive_date_count=(
                    "equal_weight_return",
                    lambda values: int((values > 0).sum()),
                ),
                mean_constituent_count=("constituent_count", "mean"),
                median_constituent_count=("constituent_count", "median"),
                mean_cross_section_median_return=("median_stock_return", "mean"),
                mean_positive_share=("positive_share", "mean"),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        grouped["hit_rate_positive"] = (
            grouped["positive_date_count"] / grouped["date_count"]
        )
        summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build any state date summary rows")
    return _sort_state_frame(pd.concat(summary_frames, ignore_index=True))


def _build_stock_state_mean_df(
    state_horizon_event_df: pd.DataFrame,
    *,
    min_stock_events_per_state: int,
) -> pd.DataFrame:
    stock_frames: list[pd.DataFrame] = []
    for split_name, split_df in _iter_split_frames(state_horizon_event_df):
        grouped = (
            split_df.groupby(
                [
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "long_mode",
                    "short_mode",
                    "code",
                    "company_name",
                ],
                observed=True,
            )["future_return"]
            .agg(
                event_count="count",
                mean_future_return="mean",
                median_future_return="median",
                std_future_return="std",
                up_count=lambda values: int((values > 0).sum()),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        stock_frames.append(grouped)

    if not stock_frames:
        raise ValueError("Failed to build any stock/state summary rows")
    stock_mean_df = pd.concat(stock_frames, ignore_index=True)
    filtered_df = stock_mean_df[
        stock_mean_df["event_count"] >= min_stock_events_per_state
    ].copy()
    if filtered_df.empty:
        raise ValueError(
            "No stock/state summaries satisfied min_stock_events_per_state; lower the threshold"
        )
    filtered_df["hit_rate_positive"] = filtered_df["up_count"] / filtered_df["event_count"]
    return _sort_state_frame(filtered_df)


def _build_state_stock_consistency_df(
    stock_state_mean_df: pd.DataFrame,
) -> pd.DataFrame:
    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in _iter_split_frames(stock_state_mean_df):
        grouped = (
            split_df.groupby(
                [
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "long_mode",
                    "short_mode",
                ],
                observed=True,
            )
            .agg(
                stock_count=("code", "count"),
                mean_stock_mean_return=("mean_future_return", "mean"),
                median_stock_mean_return=("mean_future_return", "median"),
                positive_stock_count=(
                    "mean_future_return",
                    lambda values: int((values > 0).sum()),
                ),
                mean_event_count_per_stock=("event_count", "mean"),
                median_event_count_per_stock=("event_count", "median"),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        grouped["positive_stock_ratio"] = (
            grouped["positive_stock_count"] / grouped["stock_count"]
        )
        summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build any stock-breadth summary rows")
    return _sort_state_frame(pd.concat(summary_frames, ignore_index=True))


def _sort_state_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    sort_columns: list[str] = []
    if "sample_split" in sorted_df.columns:
        sorted_df["sample_split"] = pd.Categorical(
            sorted_df["sample_split"],
            categories=list(_SPLIT_ORDER),
            ordered=True,
        )
        sort_columns.append("sample_split")
    if "date" in sorted_df.columns:
        sort_columns.append("date")
    if "segment_end_date" in sorted_df.columns:
        sort_columns.append("segment_end_date")
    if "horizon_days" in sorted_df.columns:
        sort_columns.append("horizon_days")
    if "state_key" in sorted_df.columns:
        sorted_df["state_key"] = pd.Categorical(
            sorted_df["state_key"].astype(str),
            categories=list(MULTI_TIMEFRAME_STATE_ORDER),
            ordered=True,
        )
        sort_columns.append("state_key")
    if "code" in sorted_df.columns:
        sort_columns.append("code")
    if "segment_id" in sorted_df.columns:
        sort_columns.append("segment_id")
    if not sort_columns:
        return sorted_df.reset_index(drop=True)
    return sorted_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)


def _iter_split_frames(
    df: pd.DataFrame,
) -> list[tuple[str, pd.DataFrame]]:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", df)]
    for split_name in ("discovery", "validation"):
        split_df = df[df["sample_split"] == split_name].copy()
        if not split_df.empty:
            split_frames.append((split_name, split_df))
    return split_frames


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353TransferResearchResult,
) -> str:
    validation_date_summary = result.state_date_summary_df[
        result.state_date_summary_df["sample_split"] == "validation"
    ].copy()
    validation_stock_consistency = result.state_stock_consistency_df[
        result.state_stock_consistency_df["sample_split"] == "validation"
    ].copy()

    lines = [
        "# TOPIX100 Streak 3/53 Transfer Study",
        "",
        "This study applies the fixed streak pair discovered on TOPIX itself (3 / 53) to each TOPIX100 constituent's own price series, then asks whether the same four-state hierarchy survives at the stock level.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Fixed short / long pair: `{result.short_window_streaks} / {result.long_window_streaks}` streak candles",
        f"- Future horizons: `{_format_int_sequence(result.future_horizons)}` trading days",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- TOPIX100 constituents in panel: `{result.topix100_constituent_count}`",
        f"- Constituents with valid state events: `{result.covered_constituent_count}`",
        f"- Valid state events: `{result.valid_event_count}`",
        f"- Valid event dates: `{result.valid_date_count}`",
        "",
        "## Validation Equal-Weight Read",
        "",
    ]

    if validation_date_summary.empty:
        lines.append("- No validation date-balanced state summary rows were available.")
    else:
        for horizon in result.future_horizons:
            horizon_df = validation_date_summary[
                validation_date_summary["horizon_days"] == horizon
            ].copy()
            if horizon_df.empty:
                continue
            ordered_df = horizon_df.sort_values(
                ["mean_equal_weight_return", "state_key"],
                ascending=[False, True],
                kind="stable",
            )
            ordering = " > ".join(
                f"{str(row['state_label'])} ({_format_return(float(row['mean_equal_weight_return']))})"
                for row in ordered_df.to_dict(orient="records")
            )
            lines.append(f"- {int(horizon)}d: {ordering}")

    lines.extend(["", "## Validation Breadth", ""])
    if validation_stock_consistency.empty:
        lines.append("- No stock-level consistency rows satisfied the event-count threshold.")
    else:
        for horizon in result.future_horizons:
            horizon_df = validation_stock_consistency[
                validation_stock_consistency["horizon_days"] == horizon
            ].copy()
            if horizon_df.empty:
                continue
            strongest_row = horizon_df.sort_values(
                ["mean_stock_mean_return", "state_key"],
                ascending=[False, True],
                kind="stable",
            ).iloc[0]
            lines.append(
                "- "
                f"{int(horizon)}d strongest stock-breadth state: "
                f"`{strongest_row['state_label']}` at "
                f"`{_format_return(float(strongest_row['mean_stock_mean_return']))}` "
                f"with `{float(strongest_row['positive_stock_ratio']):.1%}` of stocks positive on average."
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `state_event_df`",
            "- `state_horizon_event_df`",
            "- `state_event_summary_df`",
            "- `state_date_panel_df`",
            "- `state_date_summary_df`",
            "- `stock_state_mean_df`",
            "- `state_stock_consistency_df`",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353TransferResearchResult,
) -> dict[str, Any]:
    validation_date_summary = result.state_date_summary_df[
        result.state_date_summary_df["sample_split"] == "validation"
    ].copy()
    validation_stock_consistency = result.state_stock_consistency_df[
        result.state_stock_consistency_df["sample_split"] == "validation"
    ].copy()

    strongest_date_row = _select_primary_summary_row(
        validation_date_summary,
        value_column="mean_equal_weight_return",
        largest=True,
    )
    weakest_date_row = _select_primary_summary_row(
        validation_date_summary,
        value_column="mean_equal_weight_return",
        largest=False,
    )
    strongest_stock_row = _select_primary_summary_row(
        validation_stock_consistency,
        value_column="mean_stock_mean_return",
        largest=True,
    )

    result_bullets = [
        "This is a transfer test, not a fresh search. Every TOPIX100 constituent uses the same streak-candle pair learned on TOPIX itself: short=3 and long=53.",
        "The primary table here is the date-balanced equal-weight read. It answers whether the stock basket behaves differently when many names independently enter the same streak state on the same day.",
    ]
    highlights = [
        {
            "label": "Fixed pair",
            "value": f"{result.short_window_streaks} / {result.long_window_streaks}",
            "tone": "accent",
            "detail": "streak candles",
        },
        {
            "label": "Covered constituents",
            "value": str(result.covered_constituent_count),
            "tone": "neutral",
            "detail": f"{result.valid_event_count} valid state events",
        },
    ]

    if strongest_date_row is not None and weakest_date_row is not None:
        strongest_label = str(strongest_date_row["state_label"])
        weakest_label = str(weakest_date_row["state_label"])
        strongest_horizon = int(strongest_date_row["horizon_days"])
        weakest_horizon = int(weakest_date_row["horizon_days"])
        strongest_value = float(strongest_date_row["mean_equal_weight_return"])
        weakest_value = float(weakest_date_row["mean_equal_weight_return"])
        result_bullets.append(
            f"The transfer held best where the validation equal-weight basket stayed strongest in {strongest_label} at {strongest_horizon}d {_format_return(strongest_value)}, while {weakest_label} was weakest at {weakest_horizon}d {_format_return(weakest_value)}."
        )
        result_bullets.append(
            "That means the stock-universe read should be interpreted the same way as the index read: as a multi-timeframe exhaustion map, not as a classic trend-hierarchy signal."
        )
        highlights.append(
            {
                "label": "Strongest validation state",
                "value": strongest_label,
                "tone": "success",
                "detail": f"{strongest_horizon}d {_format_return(strongest_value)}",
            }
        )
    if strongest_stock_row is not None:
        result_bullets.append(
            f"At the stock-breadth layer, {strongest_stock_row['state_label']} also led on stock-level mean return, with {float(strongest_stock_row['positive_stock_ratio']):.1%} of stocks positive in validation at {int(strongest_stock_row['horizon_days'])}d."
        )
        highlights.append(
            {
                "label": "Best breadth",
                "value": str(strongest_stock_row["state_label"]),
                "tone": "success",
                "detail": f"{float(strongest_stock_row['positive_stock_ratio']):.1%} stocks positive",
            }
        )

    if strongest_date_row is None:
        headline = (
            "The fixed TOPIX streak 3/53 transfer bundle was published, but the validation summaries were too sparse to promote a strong state ordering."
        )
    else:
        headline = (
            "Applying the fixed TOPIX streak 3/53 pair to TOPIX100 constituents preserved a usable four-state hierarchy, and the read still looks more like exhaustion/mean-reversion context than simple trend-following."
        )

    return {
        "title": "TOPIX100 Streak 3/53 Transfer Study",
        "tags": ["TOPIX100", "streaks", "multi-timeframe", "mean-reversion"],
        "purpose": (
            "Apply the short=3 / long=53 streak-candle state model discovered on TOPIX to each TOPIX100 constituent, then measure whether the same four-state hierarchy survives at the stock-universe level."
        ),
        "method": [
            "Build streak candles separately for every TOPIX100 constituent by merging consecutive positive or negative close-to-close moves.",
            "Label each eligible streak endpoint with the fixed 3 / 53 short-long pair, producing the same four states used in the TOPIX market study.",
            "Read the result three ways: pooled event summary, date-balanced equal-weight state summary, and per-stock consistency after a minimum number of state events.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This is the right research shape if the next action is stock selection. The state is now each stock's own streak state, not the market state's label pasted onto every name.",
            "The most important read is the date-balanced summary, not the raw pooled-event mean. Pooled events can be distorted by stocks that simply generate more streak endpoints than others.",
            "If this transfers cleanly enough, the next step is a direct cross-sectional rule test, for example buying only the states that remain strongest after next-open execution and ranking within that state by a secondary filter.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": f"{result.short_window_streaks} streaks"},
            {"label": "Long X", "value": f"{result.long_window_streaks} streaks"},
            {"label": "Future horizons", "value": _format_int_sequence(result.future_horizons)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
            {
                "label": "Min stock events / state",
                "value": str(result.min_stock_events_per_state),
            },
            {
                "label": "Min names / date-state",
                "value": str(result.min_constituents_per_date_state),
            },
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "state_date_summary_df",
                "label": "Date-balanced summary",
                "description": "Equal-weight TOPIX100 forward returns by fixed streak 3/53 state.",
            },
            {
                "name": "state_stock_consistency_df",
                "label": "Stock-breadth summary",
                "description": "How many constituents stay positive on average within each state.",
            },
            {
                "name": "state_event_summary_df",
                "label": "Raw event summary",
                "description": "Pooled event-level forward returns for the same four states.",
            },
        ],
    }


def _select_primary_summary_row(
    summary_df: pd.DataFrame,
    *,
    value_column: str,
    largest: bool,
) -> pd.Series | None:
    if summary_df.empty or value_column not in summary_df.columns:
        return None
    sorted_df = summary_df.sort_values(
        [value_column, "horizon_days", "state_key"],
        ascending=[not largest, True, True],
        kind="stable",
    )
    if sorted_df.empty:
        return None
    return sorted_df.iloc[0]
