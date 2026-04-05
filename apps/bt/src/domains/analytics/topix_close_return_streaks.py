"""
TOPIX close-to-close streak research.

This study treats consecutive positive or negative close-to-close returns as one
composite move (a synthesized candlestick segment). It supports two views:

- Daily state view: "the current streak is bullish/bearish and we are on day N".
- Segment view: "a bullish/bearish streak completed with length L and total move R".
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
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _date_where_clause,
    _fetch_date_range,
    _open_analysis_connection,
)

ModeKey = Literal["bullish", "bearish", "flat"]

DEFAULT_FUTURE_HORIZONS: tuple[int, ...] = (1, 5, 10, 20)
DEFAULT_VALIDATION_RATIO = 0.3
DEFAULT_MAX_STREAK_DAY_BUCKET = 8
DEFAULT_MAX_SEGMENT_LENGTH_BUCKET = 8
MODE_ORDER: tuple[ModeKey, ...] = ("bullish", "bearish", "flat")
TOPIX_CLOSE_RETURN_STREAKS_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix-close-return-streaks"
)


@dataclass(frozen=True)
class TopixCloseReturnStreaksResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    future_horizons: tuple[int, ...]
    validation_ratio: float
    max_streak_day_bucket: int
    max_segment_length_bucket: int
    topix_daily_df: pd.DataFrame
    streak_daily_df: pd.DataFrame
    streak_segment_df: pd.DataFrame
    streak_state_summary_df: pd.DataFrame
    segment_summary_df: pd.DataFrame
    segment_end_summary_df: pd.DataFrame


def get_topix_close_return_streaks_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def run_topix_close_return_streaks_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    future_horizons: Sequence[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    max_streak_day_bucket: int = DEFAULT_MAX_STREAK_DAY_BUCKET,
    max_segment_length_bucket: int = DEFAULT_MAX_SEGMENT_LENGTH_BUCKET,
) -> TopixCloseReturnStreaksResearchResult:
    resolved_horizons = _normalize_positive_int_sequence(
        future_horizons,
        default=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")
    if max_streak_day_bucket <= 0:
        raise ValueError("max_streak_day_bucket must be positive")
    if max_segment_length_bucket <= 0:
        raise ValueError("max_segment_length_bucket must be positive")

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
    comparable_topix_df = (
        prepared_topix_df[prepared_topix_df["analysis_eligible"]]
        .copy()
        .reset_index(drop=True)
    )
    streak_all_df, streak_segment_df = _build_streak_tables(
        prepared_topix_df,
        future_horizons=resolved_horizons,
    )
    streak_daily_df = (
        streak_all_df[streak_all_df["analysis_eligible"]]
        .copy()
        .reset_index(drop=True)
    )
    streak_state_summary_df = _build_streak_state_summary_df(
        streak_daily_df,
        future_horizons=resolved_horizons,
        max_streak_day_bucket=max_streak_day_bucket,
    )
    segment_summary_df = _build_segment_summary_df(
        streak_segment_df,
        max_segment_length_bucket=max_segment_length_bucket,
    )
    segment_end_summary_df = _build_segment_end_summary_df(
        streak_segment_df,
        future_horizons=resolved_horizons,
        max_segment_length_bucket=max_segment_length_bucket,
    )

    analysis_start_date = (
        str(comparable_topix_df["date"].iloc[0]) if not comparable_topix_df.empty else None
    )
    analysis_end_date = (
        str(comparable_topix_df["date"].iloc[-1]) if not comparable_topix_df.empty else None
    )

    return TopixCloseReturnStreaksResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        max_streak_day_bucket=max_streak_day_bucket,
        max_segment_length_bucket=max_segment_length_bucket,
        topix_daily_df=comparable_topix_df,
        streak_daily_df=streak_daily_df,
        streak_segment_df=streak_segment_df,
        streak_state_summary_df=streak_state_summary_df,
        segment_summary_df=segment_summary_df,
        segment_end_summary_df=segment_end_summary_df,
    )


def write_topix_close_return_streaks_research_bundle(
    result: TopixCloseReturnStreaksResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX_CLOSE_RETURN_STREAKS_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_close_return_streaks_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "max_streak_day_bucket": result.max_streak_day_bucket,
            "max_segment_length_bucket": result.max_segment_length_bucket,
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
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "max_streak_day_bucket": result.max_streak_day_bucket,
            "max_segment_length_bucket": result.max_segment_length_bucket,
        },
        result_tables={
            "topix_daily_df": result.topix_daily_df,
            "streak_daily_df": result.streak_daily_df,
            "streak_segment_df": result.streak_segment_df,
            "streak_state_summary_df": result.streak_state_summary_df,
            "segment_summary_df": result.segment_summary_df,
            "segment_end_summary_df": result.segment_end_summary_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_close_return_streaks_research_bundle(
    bundle_path: str | Path,
) -> TopixCloseReturnStreaksResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return TopixCloseReturnStreaksResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        future_horizons=tuple(int(value) for value in metadata["future_horizons"]),
        validation_ratio=float(metadata["validation_ratio"]),
        max_streak_day_bucket=int(metadata["max_streak_day_bucket"]),
        max_segment_length_bucket=int(metadata["max_segment_length_bucket"]),
        topix_daily_df=tables["topix_daily_df"],
        streak_daily_df=tables["streak_daily_df"],
        streak_segment_df=tables["streak_segment_df"],
        streak_state_summary_df=tables["streak_state_summary_df"],
        segment_summary_df=tables["segment_summary_df"],
        segment_end_summary_df=tables["segment_end_summary_df"],
    )


def get_topix_close_return_streaks_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_CLOSE_RETURN_STREAKS_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_close_return_streaks_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_CLOSE_RETURN_STREAKS_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
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
    for column in ("open", "high", "low", "close"):
        topix_df[column] = topix_df[column].astype(float)
    topix_df["close_return"] = topix_df["close"].pct_change()
    for horizon in future_horizons:
        future_close = topix_df["close"].shift(-horizon)
        topix_df[f"future_return_{horizon}d"] = future_close / topix_df["close"] - 1.0
        topix_df[f"future_diff_{horizon}d"] = future_close - topix_df["close"]
    return topix_df


def _mark_common_comparison_window(
    topix_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
    validation_ratio: float,
) -> pd.DataFrame:
    max_horizon = max(future_horizons)
    comparable_index = topix_df.index[1 : len(topix_df) - max_horizon]
    if len(comparable_index) == 0:
        raise ValueError(
            "Not enough TOPIX rows for the requested horizons: "
            f"need at least {max_horizon + 2}, got {len(topix_df)}"
        )

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


def _classify_close_return_mode(value: float) -> str:
    if value > 0.0:
        return "bullish"
    if value < 0.0:
        return "bearish"
    return "flat"


def _build_streak_tables(
    prepared_topix_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    full_df = prepared_topix_df.reset_index(drop=True).copy()
    valid_df = full_df.iloc[1:].copy().reset_index().rename(columns={"index": "row_index"})
    if valid_df.empty:
        raise ValueError("Not enough TOPIX rows to build streak tables")

    valid_df["mode"] = valid_df["close_return"].map(_classify_close_return_mode)
    valid_df["mode"] = pd.Categorical(
        valid_df["mode"],
        categories=list(MODE_ORDER),
        ordered=True,
    )
    mode_values = valid_df["mode"].astype(str)
    valid_df["segment_id"] = (
        mode_values != mode_values.shift(fill_value=cast(str, mode_values.iloc[0]))
    ).cumsum() + 1
    valid_df["streak_day"] = valid_df.groupby("segment_id", observed=True).cumcount() + 1

    grouped = (
        valid_df.groupby("segment_id", observed=True)
        .agg(
            mode=("mode", "first"),
            start_row_index=("row_index", "first"),
            end_row_index=("row_index", "last"),
            start_date=("date", "first"),
            end_date=("date", "last"),
            segment_day_count=("date", "count"),
            mean_daily_close_return=("close_return", "mean"),
            median_daily_close_return=("close_return", "median"),
            max_daily_close_return=("close_return", "max"),
            min_daily_close_return=("close_return", "min"),
        )
        .reset_index()
    )
    close = full_df["close"].to_numpy(dtype=float)
    high = full_df["high"].to_numpy(dtype=float)
    low = full_df["low"].to_numpy(dtype=float)
    end_splits = full_df["sample_split"].to_numpy(dtype=object)
    eligible_flags = full_df["analysis_eligible"].to_numpy(dtype=bool)

    start_rows = grouped["start_row_index"].to_numpy(dtype=int)
    end_rows = grouped["end_row_index"].to_numpy(dtype=int)
    anchor_rows = start_rows - 1
    grouped["synthetic_open"] = close[anchor_rows]
    grouped["synthetic_close"] = close[end_rows]
    grouped["synthetic_high"] = [
        max(close[anchor_row], float(high[start_row : end_row + 1].max()))
        for anchor_row, start_row, end_row in zip(anchor_rows, start_rows, end_rows, strict=True)
    ]
    grouped["synthetic_low"] = [
        min(close[anchor_row], float(low[start_row : end_row + 1].min()))
        for anchor_row, start_row, end_row in zip(anchor_rows, start_rows, end_rows, strict=True)
    ]
    grouped["segment_return"] = grouped["synthetic_close"] / grouped["synthetic_open"] - 1.0
    grouped["is_complete"] = end_rows < (len(full_df) - 1)
    grouped["segment_sample_split"] = end_splits[end_rows]
    grouped["segment_analysis_eligible"] = eligible_flags[end_rows]
    for horizon in future_horizons:
        grouped[f"future_return_{horizon}d"] = full_df.iloc[end_rows][
            f"future_return_{horizon}d"
        ].to_numpy(dtype=float)
        grouped[f"future_diff_{horizon}d"] = full_df.iloc[end_rows][
            f"future_diff_{horizon}d"
        ].to_numpy(dtype=float)

    segment_metadata = grouped[
        [
            "segment_id",
            "start_date",
            "end_date",
            "segment_day_count",
            "synthetic_open",
            "synthetic_high",
            "synthetic_low",
            "synthetic_close",
            "segment_return",
            "is_complete",
            "segment_sample_split",
            "segment_analysis_eligible",
        ]
    ].rename(
        columns={
            "start_date": "segment_start_date",
            "end_date": "segment_end_date",
        }
    )
    streak_df = valid_df.merge(
        segment_metadata,
        on="segment_id",
        how="left",
        validate="many_to_one",
    )
    streak_df["remaining_segment_days"] = (
        streak_df["segment_day_count"] - streak_df["streak_day"]
    )
    streak_df["mode"] = pd.Categorical(
        streak_df["mode"],
        categories=list(MODE_ORDER),
        ordered=True,
    )
    return streak_df.reset_index(drop=True), grouped.reset_index(drop=True)


def _assign_bucket_columns(
    df: pd.DataFrame,
    *,
    source_column: str,
    bucket_column: str,
    label_column: str,
    cap: int,
) -> pd.DataFrame:
    bucketed = df.copy()
    raw_values = bucketed[source_column].astype(int)
    bucketed[bucket_column] = raw_values.clip(upper=cap)
    bucketed[label_column] = np.where(
        raw_values >= cap,
        f"{cap}+",
        raw_values.astype(str),
    )
    return bucketed


def _build_streak_state_summary_df(
    streak_daily_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
    max_streak_day_bucket: int,
) -> pd.DataFrame:
    bucketed_df = _assign_bucket_columns(
        streak_daily_df,
        source_column="streak_day",
        bucket_column="streak_day_bucket",
        label_column="streak_day_label",
        cap=max_streak_day_bucket,
    )
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", bucketed_df)]
    for split_name in ("discovery", "validation"):
        split_df = bucketed_df[bucketed_df["sample_split"] == split_name]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    summary_rows: list[dict[str, Any]] = []
    for split_name, split_df in split_frames:
        grouped = split_df.groupby(
            ["mode", "streak_day_bucket", "streak_day_label"],
            observed=True,
        )
        for (mode, streak_day_bucket, streak_day_label), group_df in grouped:
            if group_df.empty:
                continue
            future_1d = group_df["future_return_1d"] if "future_return_1d" in group_df else pd.Series(dtype=float)
            continuation_rate_1d = _compute_directional_rate(future_1d, mode=str(mode))
            completion_rate = float((group_df["remaining_segment_days"] == 0).mean())
            for horizon in future_horizons:
                return_col = f"future_return_{horizon}d"
                diff_col = f"future_diff_{horizon}d"
                summary_rows.append(
                    {
                        "sample_split": split_name,
                        "mode": str(mode),
                        "streak_day_bucket": int(streak_day_bucket),
                        "streak_day_label": str(streak_day_label),
                        "sample_count": int(len(group_df)),
                        "horizon_days": int(horizon),
                        "mean_close_return": float(group_df["close_return"].mean()),
                        "mean_segment_return": float(group_df["segment_return"].mean()),
                        "mean_segment_day_count": float(group_df["segment_day_count"].mean()),
                        "mean_remaining_segment_days": float(
                            group_df["remaining_segment_days"].mean()
                        ),
                        "completion_rate": completion_rate,
                        "continuation_rate_1d": continuation_rate_1d,
                        "mean_future_return": float(group_df[return_col].mean()),
                        "median_future_return": float(group_df[return_col].median()),
                        "std_future_return": float(group_df[return_col].std()),
                        "mean_future_diff": float(group_df[diff_col].mean()),
                        "hit_rate_positive": float((group_df[return_col] > 0).mean()),
                        "hit_rate_negative": float((group_df[return_col] < 0).mean()),
                    }
                )

    if not summary_rows:
        raise ValueError("Failed to build any streak state summary rows")
    return pd.DataFrame(summary_rows)


def _build_segment_summary_df(
    streak_segment_df: pd.DataFrame,
    *,
    max_segment_length_bucket: int,
) -> pd.DataFrame:
    eligible_segments = streak_segment_df[streak_segment_df["is_complete"]].copy()
    eligible_segments = _assign_bucket_columns(
        eligible_segments,
        source_column="segment_day_count",
        bucket_column="segment_length_bucket",
        label_column="segment_length_label",
        cap=max_segment_length_bucket,
    )

    split_frames: list[tuple[str, pd.DataFrame]] = [("full", eligible_segments)]
    for split_name in ("discovery", "validation"):
        split_df = eligible_segments[
            (eligible_segments["segment_sample_split"] == split_name)
            & (eligible_segments["segment_analysis_eligible"])
        ]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    summary_frames: list[pd.DataFrame] = []
    for split_name, split_df in split_frames:
        grouped = (
            split_df.groupby(
                ["mode", "segment_length_bucket", "segment_length_label"],
                observed=True,
            )
            .agg(
                segment_count=("segment_id", "count"),
                mean_segment_day_count=("segment_day_count", "mean"),
                median_segment_day_count=("segment_day_count", "median"),
                mean_segment_return=("segment_return", "mean"),
                median_segment_return=("segment_return", "median"),
                std_segment_return=("segment_return", "std"),
                mean_synthetic_range=(
                    "segment_id",
                    lambda values, frame=split_df: float(
                        (
                            frame.loc[values.index, "synthetic_high"]
                            / frame.loc[values.index, "synthetic_low"]
                            - 1.0
                        ).mean()
                    ),
                ),
                positive_segment_count=(
                    "segment_return",
                    lambda values: int((values > 0).sum()),
                ),
                negative_segment_count=(
                    "segment_return",
                    lambda values: int((values < 0).sum()),
                ),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        grouped["positive_segment_ratio"] = (
            grouped["positive_segment_count"] / grouped["segment_count"]
        )
        grouped["negative_segment_ratio"] = (
            grouped["negative_segment_count"] / grouped["segment_count"]
        )
        summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build any segment summary rows")
    return pd.concat(summary_frames, ignore_index=True)


def _build_segment_end_summary_df(
    streak_segment_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
    max_segment_length_bucket: int,
) -> pd.DataFrame:
    eligible_segments = streak_segment_df[
        streak_segment_df["is_complete"] & streak_segment_df["segment_analysis_eligible"]
    ].copy()
    eligible_segments = _assign_bucket_columns(
        eligible_segments,
        source_column="segment_day_count",
        bucket_column="segment_length_bucket",
        label_column="segment_length_label",
        cap=max_segment_length_bucket,
    )

    split_frames: list[tuple[str, pd.DataFrame]] = [("full", eligible_segments)]
    for split_name in ("discovery", "validation"):
        split_df = eligible_segments[
            eligible_segments["segment_sample_split"] == split_name
        ]
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    summary_rows: list[dict[str, Any]] = []
    for split_name, split_df in split_frames:
        grouped = split_df.groupby(
            ["mode", "segment_length_bucket", "segment_length_label"],
            observed=True,
        )
        for (mode, length_bucket, length_label), group_df in grouped:
            if group_df.empty:
                continue
            for horizon in future_horizons:
                return_col = f"future_return_{horizon}d"
                diff_col = f"future_diff_{horizon}d"
                summary_rows.append(
                    {
                        "sample_split": split_name,
                        "mode": str(mode),
                        "segment_length_bucket": int(length_bucket),
                        "segment_length_label": str(length_label),
                        "horizon_days": int(horizon),
                        "sample_count": int(len(group_df)),
                        "mean_segment_return": float(group_df["segment_return"].mean()),
                        "mean_future_return": float(group_df[return_col].mean()),
                        "median_future_return": float(group_df[return_col].median()),
                        "std_future_return": float(group_df[return_col].std()),
                        "mean_future_diff": float(group_df[diff_col].mean()),
                        "hit_rate_positive": float((group_df[return_col] > 0).mean()),
                        "hit_rate_negative": float((group_df[return_col] < 0).mean()),
                    }
                )

    if not summary_rows:
        raise ValueError("Failed to build any segment-end summary rows")
    return pd.DataFrame(summary_rows)


def _compute_directional_rate(values: pd.Series, *, mode: str) -> float:
    if values.empty:
        return float("nan")
    if mode == "bullish":
        return float((values > 0).mean())
    if mode == "bearish":
        return float((values < 0).mean())
    return float((values == 0).mean())


def _format_return(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:+.2f}%"


def _build_research_bundle_summary_markdown(
    result: TopixCloseReturnStreaksResearchResult,
) -> str:
    validation_segments = result.segment_summary_df[
        result.segment_summary_df["sample_split"] == "validation"
    ].copy()
    validation_streak_5d = result.streak_state_summary_df[
        (result.streak_state_summary_df["sample_split"] == "validation")
        & (result.streak_state_summary_df["horizon_days"] == 5)
    ].copy()
    validation_segment_end_5d = result.segment_end_summary_df[
        (result.segment_end_summary_df["sample_split"] == "validation")
        & (result.segment_end_summary_df["horizon_days"] == 5)
    ].copy()

    lines = [
        "# TOPIX Close Return Streaks",
        "",
        "Consecutive positive/negative close-to-close returns are treated as one composite move.",
        "",
        "## Configuration",
        "",
        f"- Analysis range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}` (`{result.source_detail}`)",
        f"- Future horizons: `{', '.join(str(value) for value in result.future_horizons)}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Streak day bucket cap: `{result.max_streak_day_bucket}+`",
        f"- Segment length bucket cap: `{result.max_segment_length_bucket}+`",
        "",
        "## Validation Segment Summary",
        "",
    ]

    if validation_segments.empty:
        lines.append("- No validation segment rows were produced.")
    else:
        for mode in ("bullish", "bearish", "flat"):
            mode_rows = validation_segments[validation_segments["mode"] == mode]
            if mode_rows.empty:
                continue
            aggregate = (
                result.streak_segment_df[
                    (result.streak_segment_df["is_complete"])
                    & (result.streak_segment_df["segment_sample_split"] == "validation")
                    & (result.streak_segment_df["segment_analysis_eligible"])
                    & (result.streak_segment_df["mode"] == mode)
                ]
                .copy()
            )
            if aggregate.empty:
                continue
            lines.append(
                "- "
                f"`{mode}`: segments={len(aggregate)}, "
                f"mean-days={aggregate['segment_day_count'].mean():.2f}, "
                f"mean-segment-return={_format_return(float(aggregate['segment_return'].mean()))}"
            )

    lines.extend(["", "## Validation Streak-Day Forward 5D", ""])
    if validation_streak_5d.empty:
        lines.append("- No validation streak-day forward rows were produced.")
    else:
        streak_rows = validation_streak_5d.sort_values(
            ["mode", "streak_day_bucket"],
            ascending=[True, True],
            kind="stable",
        )
        for row in streak_rows.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['mode']}` day `{row['streak_day_label']}`: "
                f"count={int(row['sample_count'])}, "
                f"5d={_format_return(float(row['mean_future_return']))}, "
                f"next-day-same-sign={float(row['continuation_rate_1d']) * 100:.1f}%, "
                f"remaining-days={float(row['mean_remaining_segment_days']):.2f}"
            )

    lines.extend(["", "## Validation Segment-End Forward 5D", ""])
    if validation_segment_end_5d.empty:
        lines.append("- No validation segment-end rows were produced.")
    else:
        segment_end_rows = validation_segment_end_5d.sort_values(
            ["mode", "segment_length_bucket"],
            ascending=[True, True],
            kind="stable",
        )
        for row in segment_end_rows.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['mode']}` length `{row['segment_length_label']}`: "
                f"count={int(row['sample_count'])}, "
                f"segment={_format_return(float(row['mean_segment_return']))}, "
                f"5d-after-end={_format_return(float(row['mean_future_return']))}"
            )

    lines.extend(
        [
            "",
            "## Output Tables",
            "",
            "- `topix_daily_df`",
            "- `streak_daily_df`",
            "- `streak_segment_df`",
            "- `streak_state_summary_df`",
            "- `segment_summary_df`",
            "- `segment_end_summary_df`",
        ]
    )
    return "\n".join(lines)
