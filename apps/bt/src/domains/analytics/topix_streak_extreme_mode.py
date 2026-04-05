"""
TOPIX extreme mode research over synthesized streak candles.

Workflow:

1. Merge consecutive positive/negative close-to-close returns into one
   synthesized streak candle.
2. Look back over the latest ``X`` streak candles.
3. Find the streak candle with the largest absolute total return.
4. If that dominant streak return is non-negative, classify the current streak
   candle as ``bullish``; otherwise ``bearish``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_FUTURE_HORIZONS,
    DEFAULT_VALIDATION_RATIO,
    _build_streak_tables,
    _mark_common_comparison_window,
    _normalize_positive_int_sequence,
    _query_topix_daily_frame,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _fetch_date_range,
    _open_analysis_connection,
)

ModeKey = Literal["bullish", "bearish"]

DEFAULT_CANDIDATE_WINDOWS: tuple[int, ...] = tuple(range(2, 61))
DEFAULT_MIN_MODE_CANDLES = 40
MODE_ORDER: tuple[ModeKey, ...] = ("bullish", "bearish")
WINDOW_SCORE_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")
TOPIX_STREAK_EXTREME_MODE_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix-streak-extreme-mode"
)


@dataclass(frozen=True)
class TopixStreakExtremeModeResearchResult:
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
    min_mode_candles: int
    selected_window_streaks: int
    selection_metric: str
    topix_daily_df: pd.DataFrame
    streak_candle_df: pd.DataFrame
    mode_assignments_df: pd.DataFrame
    mode_segment_df: pd.DataFrame
    segment_summary_df: pd.DataFrame
    mode_summary_df: pd.DataFrame
    window_score_df: pd.DataFrame
    selected_window_comparison_df: pd.DataFrame
    selected_window_streak_df: pd.DataFrame


def get_topix_streak_extreme_mode_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def run_topix_streak_extreme_mode_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    candidate_windows: Sequence[int] | None = None,
    future_horizons: Sequence[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    min_mode_candles: int = DEFAULT_MIN_MODE_CANDLES,
) -> TopixStreakExtremeModeResearchResult:
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
    if min_mode_candles <= 0:
        raise ValueError("min_mode_candles must be positive")

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
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
    )
    streak_all_df, streak_segment_df = _build_streak_tables(
        prepared_topix_df,
        future_horizons=resolved_horizons,
    )
    prepared_streak_df = _prepare_streak_candle_frame(
        streak_segment_df,
        candidate_windows=resolved_windows,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
    )
    comparable_streak_df = (
        prepared_streak_df[prepared_streak_df["analysis_eligible"]]
        .copy()
        .reset_index(drop=True)
    )
    mode_assignments_df = _build_mode_assignments_df(
        prepared_streak_df,
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
        min_mode_candles=min_mode_candles,
    )
    selected_window_streaks = _select_best_window_streaks(window_score_df)
    selected_window_streak_df = (
        mode_assignments_df[
            mode_assignments_df["window_streaks"] == selected_window_streaks
        ]
        .copy()
        .reset_index(drop=True)
    )
    selected_window_comparison_df = _build_window_comparison_df(
        segment_summary_df,
        window_streaks=selected_window_streaks,
    )

    analysis_start_date = (
        str(comparable_streak_df["end_date"].iloc[0])
        if not comparable_streak_df.empty
        else None
    )
    analysis_end_date = (
        str(comparable_streak_df["end_date"].iloc[-1])
        if not comparable_streak_df.empty
        else None
    )

    return TopixStreakExtremeModeResearchResult(
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
        min_mode_candles=min_mode_candles,
        selected_window_streaks=selected_window_streaks,
        selection_metric="discovery mode-segment composite score on streak candles",
        topix_daily_df=(
            prepared_topix_df[prepared_topix_df["analysis_eligible"]]
            .copy()
            .reset_index(drop=True)
        ),
        streak_candle_df=comparable_streak_df,
        mode_assignments_df=mode_assignments_df,
        mode_segment_df=mode_segment_df,
        segment_summary_df=segment_summary_df,
        mode_summary_df=mode_summary_df,
        window_score_df=window_score_df,
        selected_window_comparison_df=selected_window_comparison_df,
        selected_window_streak_df=selected_window_streak_df,
    )


def write_topix_streak_extreme_mode_research_bundle(
    result: TopixStreakExtremeModeResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX_STREAK_EXTREME_MODE_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_streak_extreme_mode_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "candidate_windows": list(result.candidate_windows),
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "min_mode_candles": result.min_mode_candles,
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
            "min_mode_candles": result.min_mode_candles,
            "selected_window_streaks": result.selected_window_streaks,
            "selection_metric": result.selection_metric,
        },
        result_tables={
            "topix_daily_df": result.topix_daily_df,
            "streak_candle_df": result.streak_candle_df,
            "mode_assignments_df": result.mode_assignments_df,
            "mode_segment_df": result.mode_segment_df,
            "segment_summary_df": result.segment_summary_df,
            "mode_summary_df": result.mode_summary_df,
            "window_score_df": result.window_score_df,
            "selected_window_comparison_df": result.selected_window_comparison_df,
            "selected_window_streak_df": result.selected_window_streak_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_streak_extreme_mode_research_bundle(
    bundle_path: str | Path,
) -> TopixStreakExtremeModeResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return TopixStreakExtremeModeResearchResult(
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
        min_mode_candles=int(metadata["min_mode_candles"]),
        selected_window_streaks=int(metadata["selected_window_streaks"]),
        selection_metric=str(metadata["selection_metric"]),
        topix_daily_df=tables["topix_daily_df"],
        streak_candle_df=tables["streak_candle_df"],
        mode_assignments_df=tables["mode_assignments_df"],
        mode_segment_df=tables["mode_segment_df"],
        segment_summary_df=tables["segment_summary_df"],
        mode_summary_df=tables["mode_summary_df"],
        window_score_df=tables["window_score_df"],
        selected_window_comparison_df=tables["selected_window_comparison_df"],
        selected_window_streak_df=tables["selected_window_streak_df"],
    )


def get_topix_streak_extreme_mode_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_STREAK_EXTREME_MODE_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_streak_extreme_mode_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_STREAK_EXTREME_MODE_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _prepare_streak_candle_frame(
    streak_segment_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    future_horizons: Sequence[int],
    validation_ratio: float,
) -> pd.DataFrame:
    max_window = max(candidate_windows)
    required_columns = [f"future_return_{horizon}d" for horizon in future_horizons]
    complete_df = (
        streak_segment_df[streak_segment_df["is_complete"]]
        .copy()
        .sort_values("segment_id", kind="stable")
        .reset_index(drop=True)
    )
    complete_df["analysis_eligible"] = False
    complete_df["sample_split"] = "excluded"
    eligible_mask = complete_df[required_columns].notna().all(axis=1)
    eligible_index = complete_df.index[
        eligible_mask & (complete_df.index >= (max_window - 1))
    ]
    if len(eligible_index) == 0:
        raise ValueError("No comparable streak candles remained after warmup/horizon trimming")

    split_labels = _build_sample_split_labels(
        len(eligible_index),
        validation_ratio=validation_ratio,
    )
    complete_df.loc[eligible_index, "analysis_eligible"] = True
    complete_df.loc[eligible_index, "sample_split"] = split_labels
    return complete_df


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
    prepared_streak_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    eligible_indices = prepared_streak_df.index[
        prepared_streak_df["analysis_eligible"]
    ].to_list()
    if not eligible_indices:
        raise ValueError("No comparable streak candles were marked as analysis eligible")

    assignment_rows: list[dict[str, Any]] = []
    for window_streaks in candidate_windows:
        for segment_index in eligible_indices:
            window_start = segment_index - window_streaks + 1
            if window_start < 0:
                continue
            window_df = prepared_streak_df.iloc[window_start : segment_index + 1]
            dominant_position = int(window_df["segment_return"].abs().argmax())
            dominant_row = window_df.iloc[dominant_position]
            current_row = prepared_streak_df.iloc[segment_index]
            row = {
                "segment_id": int(current_row["segment_id"]),
                "sample_split": str(current_row["sample_split"]),
                "window_streaks": int(window_streaks),
                "segment_start_date": str(current_row["start_date"]),
                "segment_end_date": str(current_row["end_date"]),
                "synthetic_open": float(current_row["synthetic_open"]),
                "synthetic_close": float(current_row["synthetic_close"]),
                "segment_return": float(current_row["segment_return"]),
                "segment_day_count": int(current_row["segment_day_count"]),
                "base_streak_mode": str(current_row["mode"]),
                "mode": "bullish"
                if float(dominant_row["segment_return"]) >= 0.0
                else "bearish",
                "dominant_segment_id": int(dominant_row["segment_id"]),
                "dominant_segment_return": float(dominant_row["segment_return"]),
                "dominant_abs_segment_return": float(abs(dominant_row["segment_return"])),
                "dominant_segment_start_date": str(dominant_row["start_date"]),
                "dominant_segment_end_date": str(dominant_row["end_date"]),
                "dominant_segment_day_count": int(dominant_row["segment_day_count"]),
            }
            for horizon in future_horizons:
                row[f"future_return_{horizon}d"] = float(current_row[f"future_return_{horizon}d"])
                row[f"future_diff_{horizon}d"] = float(current_row[f"future_diff_{horizon}d"])
            assignment_rows.append(row)

    mode_assignments_df = pd.DataFrame(assignment_rows)
    if mode_assignments_df.empty:
        raise ValueError("Failed to build any streak extreme mode assignments")
    mode_assignments_df["mode"] = pd.Categorical(
        mode_assignments_df["mode"],
        categories=list(MODE_ORDER),
        ordered=True,
    )
    return mode_assignments_df


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
                split_df.groupby(["window_streaks", "mode"], observed=True)[return_col]
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
    return mode_summary_df


def _build_mode_segment_df(mode_assignments_df: pd.DataFrame) -> pd.DataFrame:
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", mode_assignments_df)]
    for split_name in ("discovery", "validation"):
        split_df = mode_assignments_df[mode_assignments_df["sample_split"] == split_name]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    segment_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        for window_streaks in sorted(split_df["window_streaks"].unique()):
            window_df = (
                split_df[split_df["window_streaks"] == window_streaks]
                .sort_values("segment_id", kind="stable")
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
                    start_segment_start_date=("segment_start_date", "first"),
                    end_segment_end_date=("segment_end_date", "last"),
                    start_open=("synthetic_open", "first"),
                    end_close=("synthetic_close", "last"),
                    mode_candle_count=("segment_id", "count"),
                    mode_day_count=("segment_day_count", "sum"),
                    mean_streak_return=("segment_return", "mean"),
                    mean_dominant_segment_return=("dominant_segment_return", "mean"),
                    max_dominant_abs_segment_return=("dominant_abs_segment_return", "max"),
                )
                .reset_index(drop=True)
            )
            grouped["sample_split"] = split_name
            grouped["window_streaks"] = int(window_streaks)
            grouped["segment_id"] = range(1, len(grouped) + 1)
            grouped["segment_return"] = grouped["end_close"] / grouped["start_open"] - 1.0
            segment_frames.append(grouped)

    if not segment_frames:
        raise ValueError("Failed to build any mode segment rows")
    return pd.concat(segment_frames, ignore_index=True)


def _build_segment_summary_df(mode_segment_df: pd.DataFrame) -> pd.DataFrame:
    summary_df = (
        mode_segment_df.groupby(
            ["sample_split", "window_streaks", "mode"],
            observed=True,
        )
        .agg(
            segment_count=("segment_id", "count"),
            total_mode_candles=("mode_candle_count", "sum"),
            total_mode_days=("mode_day_count", "sum"),
            mean_mode_candle_count=("mode_candle_count", "mean"),
            mean_mode_day_count=("mode_day_count", "mean"),
            median_mode_day_count=("mode_day_count", "median"),
            mean_segment_return=("segment_return", "mean"),
            median_segment_return=("segment_return", "median"),
            std_segment_return=("segment_return", "std"),
            positive_segment_count=("segment_return", lambda values: int((values > 0).sum())),
            negative_segment_count=("segment_return", lambda values: int((values < 0).sum())),
            flat_segment_count=("segment_return", lambda values: int((values == 0).sum())),
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
    min_mode_candles: int,
) -> pd.DataFrame:
    summary_lookup: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in segment_summary_df.to_dict(orient="records"):
        summary_lookup[
            (str(row["sample_split"]), int(row["window_streaks"]), str(row["mode"]))
        ] = row

    score_rows: list[dict[str, Any]] = []
    for split_name in WINDOW_SCORE_SPLIT_ORDER:
        for window_streaks in candidate_windows:
            bullish_row = summary_lookup.get((split_name, window_streaks, "bullish"))
            bearish_row = summary_lookup.get((split_name, window_streaks, "bearish"))
            if bullish_row is None or bearish_row is None:
                continue

            bullish_count = int(bullish_row["total_mode_candles"])
            bearish_count = int(bearish_row["total_mode_candles"])
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
                and bullish_count >= min_mode_candles
                and bearish_count >= min_mode_candles
                and bullish_segment_count > 0
                and bearish_segment_count > 0
            )
            selection_score = composite_score if selection_eligible else math.nan

            score_rows.append(
                {
                    "sample_split": split_name,
                    "window_streaks": window_streaks,
                    "bullish_count": bullish_count,
                    "bearish_count": bearish_count,
                    "bullish_segment_count": bullish_segment_count,
                    "bearish_segment_count": bearish_segment_count,
                    "bullish_share": bullish_share,
                    "balance_ratio": balance_ratio,
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
                "window_streaks",
            ],
            ascending=[False, False, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    rank_lookup = {
        int(row["window_streaks"]): index + 1
        for index, row in discovery_rank_df.iterrows()
    }
    selection_rank_values = []
    for row in window_score_df.to_dict(orient="records"):
        if str(row["sample_split"]) == "discovery":
            selection_rank_values.append(
                rank_lookup.get(int(row["window_streaks"]), pd.NA)
            )
        else:
            selection_rank_values.append(pd.NA)
    window_score_df["selection_rank"] = selection_rank_values
    return window_score_df


def _select_best_window_streaks(window_score_df: pd.DataFrame) -> int:
    candidates_df = window_score_df[
        (window_score_df["sample_split"] == "discovery")
        & window_score_df["selection_eligible"]
        & window_score_df["selection_score"].notna()
    ].copy()
    if candidates_df.empty:
        raise ValueError("No eligible discovery windows were available for selection")
    candidates_df = candidates_df.sort_values(
        [
            "selection_score",
            "directional_consistency",
            "balance_ratio",
            "window_streaks",
        ],
        ascending=[False, False, False, True],
        kind="stable",
    )
    return int(candidates_df.iloc[0]["window_streaks"])


def _build_window_comparison_df(
    segment_summary_df: pd.DataFrame,
    *,
    window_streaks: int,
) -> pd.DataFrame:
    summary_df = segment_summary_df[
        segment_summary_df["window_streaks"] == window_streaks
    ].copy()
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
                "window_streaks": window_streaks,
                "bullish_count": int(bullish["total_mode_candles"]),
                "bearish_count": int(bearish["total_mode_candles"]),
                "bullish_segment_count": int(bullish["segment_count"]),
                "bearish_segment_count": int(bearish["segment_count"]),
                "bullish_mean_segment_return": bullish_mean,
                "bearish_mean_segment_return": bearish_mean,
                "bullish_positive_segment_ratio": float(bullish["positive_segment_ratio"]),
                "bearish_negative_segment_ratio": float(bearish["negative_segment_ratio"]),
                "bullish_mean_mode_candle_count": float(bullish["mean_mode_candle_count"]),
                "bearish_mean_mode_candle_count": float(bearish["mean_mode_candle_count"]),
                "bullish_mean_mode_day_count": float(bullish["mean_mode_day_count"]),
                "bearish_mean_mode_day_count": float(bearish["mean_mode_day_count"]),
                "mean_return_separation": bullish_mean - bearish_mean,
                "directional_accuracy": (
                    float(bullish["positive_segment_ratio"])
                    + float(bearish["negative_segment_ratio"])
                )
                / 2.0,
            }
        )
    return pd.DataFrame(comparison_rows)


def _build_research_bundle_summary_markdown(
    result: TopixStreakExtremeModeResearchResult,
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
                "window_streaks",
            ],
            ascending=[False, False, False, True],
            kind="stable",
        )
        .head(5)
    )
    selected_comparison = result.selected_window_comparison_df[
        result.selected_window_comparison_df["sample_split"].isin(
            ["discovery", "validation"]
        )
    ].copy()

    lines = [
        "# TOPIX Streak Extreme Mode",
        "",
        "Consecutive positive/negative daily close changes are first merged into streak candles, then the dominant candle inside the latest `X` streak candles decides the mode.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Candidate windows (streak candles): `{_format_int_sequence(result.candidate_windows)}`",
        f"- Future horizons (calendar days from streak end): `{_format_int_sequence(result.future_horizons)}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Minimum mode candles for selection: `{result.min_mode_candles}`",
        "",
        "## Selected X",
        "",
        f"- Selected window streaks: `{result.selected_window_streaks}`",
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
                f"bull-candles={float(row['bullish_mean_mode_candle_count']):.1f}, "
                f"bear-candles={float(row['bearish_mean_mode_candle_count']):.1f}, "
                f"bull-days={float(row['bullish_mean_mode_day_count']):.1f}, "
                f"bear-days={float(row['bearish_mean_mode_day_count']):.1f}"
            )

    validation_forward_rows = result.mode_summary_df[
        (result.mode_summary_df["sample_split"] == "validation")
        & (result.mode_summary_df["window_streaks"] == result.selected_window_streaks)
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
                f"X={int(row['window_streaks'])}: "
                f"score={selection_text}, "
                f"consistency={float(row['directional_consistency']):.1%}, "
                f"balance={float(row['balance_ratio']):.1%}"
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `topix_daily_df`",
            "- `streak_candle_df`",
            "- `mode_assignments_df`",
            "- `mode_segment_df`",
            "- `segment_summary_df`",
            "- `mode_summary_df`",
            "- `window_score_df`",
            "- `selected_window_comparison_df`",
            "- `selected_window_streak_df`",
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
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:+.2f}%"
