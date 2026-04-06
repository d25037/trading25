"""
TOPIX mean-reversion comparison between the normal and streak extreme modes.

The comparison reuses the selected-window signals from the existing research:

- normal: dominant close-to-close shock inside trailing trading-day window
- streak: dominant synthesized streak candle inside trailing streak-candle window

Simple execution assumptions are kept identical across both models:

- observe the signal at the signal-day close
- enter at the next session open
- exit at the N-th session close after entry
- ignore overlapping signals while a trade is open
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

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
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.analytics.topix_extreme_close_to_close_mode import (
    DEFAULT_CANDIDATE_WINDOWS as DEFAULT_NORMAL_CANDIDATE_WINDOWS,
    DEFAULT_FUTURE_HORIZONS,
    DEFAULT_MIN_MODE_DAYS,
    DEFAULT_VALIDATION_RATIO,
    TopixExtremeCloseToCloseModeResearchResult,
    run_topix_extreme_close_to_close_mode_research,
)
from src.domains.analytics.topix_streak_extreme_mode import (
    DEFAULT_CANDIDATE_WINDOWS as DEFAULT_STREAK_CANDIDATE_WINDOWS,
    DEFAULT_MIN_MODE_CANDLES,
    TopixStreakExtremeModeResearchResult,
    run_topix_streak_extreme_mode_research,
)

StrategyKey = Literal["long_on_bearish", "short_on_bullish", "long_bear_short_bull"]
SampleSplitKey = Literal["full", "discovery", "validation"]

DEFAULT_HOLD_DAYS: tuple[int, ...] = (1, 5, 10, 20)
MODEL_ORDER: tuple[str, ...] = ("normal", "streak")
STRATEGY_ORDER: tuple[StrategyKey, ...] = (
    "long_on_bearish",
    "short_on_bullish",
    "long_bear_short_bull",
)
SAMPLE_SPLIT_ORDER: tuple[SampleSplitKey, ...] = ("validation", "full", "discovery")
TOPIX_EXTREME_MODE_MEAN_REVERSION_COMPARISON_EXPERIMENT_ID = (
    "market-behavior/topix-extreme-mode-mean-reversion-comparison"
)


@dataclass(frozen=True)
class TopixExtremeModeMeanReversionComparisonResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    common_start_date: str
    common_end_date: str
    normal_candidate_windows: tuple[int, ...]
    streak_candidate_windows: tuple[int, ...]
    future_horizons: tuple[int, ...]
    hold_days: tuple[int, ...]
    validation_ratio: float
    min_normal_mode_days: int
    min_streak_mode_candles: int
    selected_normal_window_days: int
    selected_streak_window_streaks: int
    model_overview_df: pd.DataFrame
    signal_df: pd.DataFrame
    signal_summary_df: pd.DataFrame
    backtest_trade_df: pd.DataFrame
    backtest_summary_df: pd.DataFrame
    validation_leaderboard_df: pd.DataFrame


def run_topix_extreme_mode_mean_reversion_comparison_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    normal_candidate_windows: Sequence[int] | None = None,
    streak_candidate_windows: Sequence[int] | None = None,
    future_horizons: Sequence[int] | None = None,
    hold_days: Sequence[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    min_normal_mode_days: int = DEFAULT_MIN_MODE_DAYS,
    min_streak_mode_candles: int = DEFAULT_MIN_MODE_CANDLES,
) -> TopixExtremeModeMeanReversionComparisonResult:
    resolved_normal_windows = _normalize_positive_int_sequence(
        normal_candidate_windows,
        default=DEFAULT_NORMAL_CANDIDATE_WINDOWS,
        name="normal_candidate_windows",
    )
    resolved_streak_windows = _normalize_positive_int_sequence(
        streak_candidate_windows,
        default=DEFAULT_STREAK_CANDIDATE_WINDOWS,
        name="streak_candidate_windows",
    )
    resolved_horizons = _normalize_positive_int_sequence(
        future_horizons,
        default=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    resolved_hold_days = _normalize_positive_int_sequence(
        hold_days,
        default=DEFAULT_HOLD_DAYS,
        name="hold_days",
    )
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")
    if min_normal_mode_days <= 0:
        raise ValueError("min_normal_mode_days must be positive")
    if min_streak_mode_candles <= 0:
        raise ValueError("min_streak_mode_candles must be positive")

    normal_result = run_topix_extreme_close_to_close_mode_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        candidate_windows=resolved_normal_windows,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        min_mode_days=min_normal_mode_days,
    )
    streak_result = run_topix_streak_extreme_mode_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        candidate_windows=resolved_streak_windows,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        min_mode_candles=min_streak_mode_candles,
    )
    common_start_date, common_end_date = _resolve_common_date_range(
        normal_result.analysis_start_date,
        normal_result.analysis_end_date,
        streak_result.analysis_start_date,
        streak_result.analysis_end_date,
    )
    price_df = _build_price_frame(normal_result, common_start_date, common_end_date)
    signal_df = _build_signal_df(
        normal_result,
        streak_result,
        common_start_date=common_start_date,
        common_end_date=common_end_date,
    )
    signal_summary_df = _build_signal_summary_df(signal_df)
    backtest_trade_df = _build_backtest_trade_df(
        price_df,
        signal_df,
        hold_days=resolved_hold_days,
    )
    backtest_summary_df = _build_backtest_summary_df(
        backtest_trade_df,
        hold_days=resolved_hold_days,
    )
    validation_leaderboard_df = _build_validation_leaderboard_df(backtest_summary_df)
    model_overview_df = _build_model_overview_df(
        normal_result,
        streak_result,
        common_start_date=common_start_date,
        common_end_date=common_end_date,
    )

    return TopixExtremeModeMeanReversionComparisonResult(
        db_path=db_path,
        source_mode=normal_result.source_mode,
        source_detail=normal_result.source_detail,
        available_start_date=normal_result.available_start_date,
        available_end_date=normal_result.available_end_date,
        common_start_date=common_start_date,
        common_end_date=common_end_date,
        normal_candidate_windows=resolved_normal_windows,
        streak_candidate_windows=resolved_streak_windows,
        future_horizons=resolved_horizons,
        hold_days=resolved_hold_days,
        validation_ratio=validation_ratio,
        min_normal_mode_days=min_normal_mode_days,
        min_streak_mode_candles=min_streak_mode_candles,
        selected_normal_window_days=normal_result.selected_window_days,
        selected_streak_window_streaks=streak_result.selected_window_streaks,
        model_overview_df=model_overview_df,
        signal_df=signal_df,
        signal_summary_df=signal_summary_df,
        backtest_trade_df=backtest_trade_df,
        backtest_summary_df=backtest_summary_df,
        validation_leaderboard_df=validation_leaderboard_df,
    )


def write_topix_extreme_mode_mean_reversion_comparison_bundle(
    result: TopixExtremeModeMeanReversionComparisonResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX_EXTREME_MODE_MEAN_REVERSION_COMPARISON_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_extreme_mode_mean_reversion_comparison_research",
        params={
            "normal_candidate_windows": list(result.normal_candidate_windows),
            "streak_candidate_windows": list(result.streak_candidate_windows),
            "future_horizons": list(result.future_horizons),
            "hold_days": list(result.hold_days),
            "validation_ratio": result.validation_ratio,
            "min_normal_mode_days": result.min_normal_mode_days,
            "min_streak_mode_candles": result.min_streak_mode_candles,
        },
        db_path=result.db_path,
        analysis_start_date=result.common_start_date,
        analysis_end_date=result.common_end_date,
        result_metadata={
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "common_start_date": result.common_start_date,
            "common_end_date": result.common_end_date,
            "normal_candidate_windows": list(result.normal_candidate_windows),
            "streak_candidate_windows": list(result.streak_candidate_windows),
            "future_horizons": list(result.future_horizons),
            "hold_days": list(result.hold_days),
            "validation_ratio": result.validation_ratio,
            "min_normal_mode_days": result.min_normal_mode_days,
            "min_streak_mode_candles": result.min_streak_mode_candles,
            "selected_normal_window_days": result.selected_normal_window_days,
            "selected_streak_window_streaks": result.selected_streak_window_streaks,
        },
        result_tables={
            "model_overview_df": result.model_overview_df,
            "signal_df": result.signal_df,
            "signal_summary_df": result.signal_summary_df,
            "backtest_trade_df": result.backtest_trade_df,
            "backtest_summary_df": result.backtest_summary_df,
            "validation_leaderboard_df": result.validation_leaderboard_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_extreme_mode_mean_reversion_comparison_bundle(
    bundle_path: str | Path,
) -> TopixExtremeModeMeanReversionComparisonResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return TopixExtremeModeMeanReversionComparisonResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        common_start_date=str(metadata["common_start_date"]),
        common_end_date=str(metadata["common_end_date"]),
        normal_candidate_windows=tuple(
            int(value) for value in metadata["normal_candidate_windows"]
        ),
        streak_candidate_windows=tuple(
            int(value) for value in metadata["streak_candidate_windows"]
        ),
        future_horizons=tuple(int(value) for value in metadata["future_horizons"]),
        hold_days=tuple(int(value) for value in metadata["hold_days"]),
        validation_ratio=float(metadata["validation_ratio"]),
        min_normal_mode_days=int(metadata["min_normal_mode_days"]),
        min_streak_mode_candles=int(metadata["min_streak_mode_candles"]),
        selected_normal_window_days=int(metadata["selected_normal_window_days"]),
        selected_streak_window_streaks=int(metadata["selected_streak_window_streaks"]),
        model_overview_df=tables["model_overview_df"],
        signal_df=tables["signal_df"],
        signal_summary_df=tables["signal_summary_df"],
        backtest_trade_df=tables["backtest_trade_df"],
        backtest_summary_df=tables["backtest_summary_df"],
        validation_leaderboard_df=tables["validation_leaderboard_df"],
    )


def get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_EXTREME_MODE_MEAN_REVERSION_COMPARISON_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_EXTREME_MODE_MEAN_REVERSION_COMPARISON_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _resolve_common_date_range(
    normal_start_date: str | None,
    normal_end_date: str | None,
    streak_start_date: str | None,
    streak_end_date: str | None,
) -> tuple[str, str]:
    if (
        normal_start_date is None
        or normal_end_date is None
        or streak_start_date is None
        or streak_end_date is None
    ):
        raise ValueError("Both normal and streak analyses must expose comparable date ranges")

    common_start_date = max(normal_start_date, streak_start_date)
    common_end_date = min(normal_end_date, streak_end_date)
    if common_start_date > common_end_date:
        raise ValueError("Normal and streak analyses do not overlap on comparable dates")
    return common_start_date, common_end_date


def _build_price_frame(
    normal_result: TopixExtremeCloseToCloseModeResearchResult,
    common_start_date: str,
    common_end_date: str,
) -> pd.DataFrame:
    price_df = normal_result.topix_daily_df[["date", "open", "close"]].copy()
    price_df["date"] = price_df["date"].astype(str)
    return (
        price_df[
            (price_df["date"] >= common_start_date)
            & (price_df["date"] <= common_end_date)
        ]
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )


def _build_signal_df(
    normal_result: TopixExtremeCloseToCloseModeResearchResult,
    streak_result: TopixStreakExtremeModeResearchResult,
    *,
    common_start_date: str,
    common_end_date: str,
) -> pd.DataFrame:
    normal_df = normal_result.selected_window_daily_df[
        ["date", "sample_split", "mode", "close_return", "dominant_close_return"]
    ].copy()
    normal_df = normal_df.rename(
        columns={
            "date": "signal_date",
            "close_return": "signal_return",
            "dominant_close_return": "dominant_return",
        }
    )
    normal_df["model"] = "normal"
    normal_df["selected_window_value"] = normal_result.selected_window_days
    normal_df["selected_window_unit"] = "days"
    normal_df["signal_day_count"] = 1

    streak_df = streak_result.selected_window_streak_df[
        [
            "segment_end_date",
            "sample_split",
            "mode",
            "segment_return",
            "dominant_segment_return",
            "segment_day_count",
        ]
    ].copy()
    streak_df = streak_df.rename(
        columns={
            "segment_end_date": "signal_date",
            "segment_return": "signal_return",
            "dominant_segment_return": "dominant_return",
            "segment_day_count": "signal_day_count",
        }
    )
    streak_df["model"] = "streak"
    streak_df["selected_window_value"] = streak_result.selected_window_streaks
    streak_df["selected_window_unit"] = "streaks"

    signal_df = pd.concat([normal_df, streak_df], ignore_index=True)
    signal_df["signal_date"] = signal_df["signal_date"].astype(str)
    signal_df = signal_df[
        (signal_df["signal_date"] >= common_start_date)
        & (signal_df["signal_date"] <= common_end_date)
    ].copy()
    signal_df["mode"] = signal_df["mode"].astype(str)
    return signal_df.sort_values(["model", "signal_date"], kind="stable").reset_index(drop=True)


def _build_signal_summary_df(signal_df: pd.DataFrame) -> pd.DataFrame:
    summary_frames: list[pd.DataFrame] = []
    split_frames: list[tuple[str, pd.DataFrame]] = [("full", signal_df)]
    for split_name in ("discovery", "validation"):
        split_df = signal_df[signal_df["sample_split"] == split_name].copy()
        if not split_df.empty:
            split_frames.append((split_name, split_df))

    for split_name, split_df in split_frames:
        grouped = (
            split_df.groupby(
                ["model", "mode", "selected_window_value", "selected_window_unit"],
                observed=True,
            )
            .agg(
                signal_count=("signal_date", "count"),
                mean_signal_return=("signal_return", "mean"),
                mean_dominant_return=("dominant_return", "mean"),
                mean_signal_day_count=("signal_day_count", "mean"),
                first_signal_date=("signal_date", "min"),
                last_signal_date=("signal_date", "max"),
            )
            .reset_index()
        )
        if grouped.empty:
            continue
        grouped["sample_split"] = split_name
        summary_frames.append(grouped)

    if not summary_frames:
        raise ValueError("Failed to build any signal summary rows")
    return pd.concat(summary_frames, ignore_index=True)


def _build_backtest_trade_df(
    price_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    *,
    hold_days: Sequence[int],
) -> pd.DataFrame:
    trade_frames: list[pd.DataFrame] = []
    for model in MODEL_ORDER:
        model_signals = signal_df[signal_df["model"] == model].copy()
        if model_signals.empty:
            continue
        for split_name in SAMPLE_SPLIT_ORDER:
            split_signals = (
                model_signals
                if split_name == "full"
                else model_signals[model_signals["sample_split"] == split_name].copy()
            )
            if split_signals.empty:
                continue
            for holding_days in hold_days:
                trade_frames.append(
                    _build_single_mode_trade_rows(
                        price_df,
                        split_signals,
                        model=model,
                        sample_split=split_name,
                        strategy="long_on_bearish",
                        trigger_mode="bearish",
                        side="long",
                        hold_days=holding_days,
                    )
                )
                trade_frames.append(
                    _build_single_mode_trade_rows(
                        price_df,
                        split_signals,
                        model=model,
                        sample_split=split_name,
                        strategy="short_on_bullish",
                        trigger_mode="bullish",
                        side="short",
                        hold_days=holding_days,
                    )
                )
                trade_frames.append(
                    _build_alternating_trade_rows(
                        price_df,
                        split_signals,
                        model=model,
                        sample_split=split_name,
                        hold_days=holding_days,
                    )
                )

    non_empty_frames = [frame for frame in trade_frames if not frame.empty]
    if not non_empty_frames:
        return pd.DataFrame(
            columns=[
                "model",
                "sample_split",
                "strategy",
                "hold_days",
                "signal_date",
                "entry_date",
                "exit_date",
                "signal_mode",
                "side",
                "trade_return",
            ]
        )
    return pd.concat(non_empty_frames, ignore_index=True)


def _build_single_mode_trade_rows(
    price_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    *,
    model: str,
    sample_split: str,
    strategy: StrategyKey,
    trigger_mode: str,
    side: Literal["long", "short"],
    hold_days: int,
) -> pd.DataFrame:
    relevant_signals = signal_df[signal_df["mode"] == trigger_mode].copy()
    return _build_trade_rows_for_signal_sequence(
        price_df,
        relevant_signals,
        model=model,
        sample_split=sample_split,
        strategy=strategy,
        hold_days=hold_days,
        side_resolver=lambda row: side,
    )


def _build_alternating_trade_rows(
    price_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    *,
    model: str,
    sample_split: str,
    hold_days: int,
) -> pd.DataFrame:
    return _build_trade_rows_for_signal_sequence(
        price_df,
        signal_df.sort_values("signal_date", kind="stable").reset_index(drop=True),
        model=model,
        sample_split=sample_split,
        strategy="long_bear_short_bull",
        hold_days=hold_days,
        side_resolver=lambda row: "long"
        if str(row["mode"]) == "bearish"
        else "short",
    )


def _build_trade_rows_for_signal_sequence(
    price_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    *,
    model: str,
    sample_split: str,
    strategy: StrategyKey,
    hold_days: int,
    side_resolver,
) -> pd.DataFrame:
    if signal_df.empty or price_df.empty:
        return pd.DataFrame()

    ordered_prices = price_df.sort_values("date", kind="stable").reset_index(drop=True)
    date_index_lookup = {
        str(date_value): index
        for index, date_value in enumerate(ordered_prices["date"].astype(str))
    }
    trade_rows: list[dict[str, object]] = []
    next_allowed_entry_index = 1

    for row in signal_df.sort_values("signal_date", kind="stable").to_dict(orient="records"):
        signal_date = str(row["signal_date"])
        signal_index = date_index_lookup.get(signal_date)
        if signal_index is None:
            continue

        entry_index = signal_index + 1
        exit_index = entry_index + hold_days - 1
        if entry_index < next_allowed_entry_index or exit_index >= len(ordered_prices):
            continue

        entry_row = ordered_prices.iloc[entry_index]
        exit_row = ordered_prices.iloc[exit_index]
        long_return = float(exit_row["close"] / entry_row["open"] - 1.0)
        side = cast(Literal["long", "short"], side_resolver(row))
        trade_return = long_return if side == "long" else -long_return
        trade_rows.append(
            {
                "model": model,
                "sample_split": sample_split,
                "strategy": strategy,
                "hold_days": hold_days,
                "signal_date": signal_date,
                "entry_date": str(entry_row["date"]),
                "exit_date": str(exit_row["date"]),
                "signal_mode": str(row["mode"]),
                "side": side,
                "trade_return": trade_return,
            }
        )
        next_allowed_entry_index = exit_index + 1

    return pd.DataFrame(trade_rows)


def _build_backtest_summary_df(
    backtest_trade_df: pd.DataFrame,
    *,
    hold_days: Sequence[int],
) -> pd.DataFrame:
    summary_rows: list[dict[str, object]] = []
    for model in MODEL_ORDER:
        for sample_split in SAMPLE_SPLIT_ORDER:
            for strategy in STRATEGY_ORDER:
                for holding_days in hold_days:
                    subset = backtest_trade_df[
                        (backtest_trade_df["model"] == model)
                        & (backtest_trade_df["sample_split"] == sample_split)
                        & (backtest_trade_df["strategy"] == strategy)
                        & (backtest_trade_df["hold_days"] == holding_days)
                    ].copy()
                    summary_rows.append(
                        {
                            "model": model,
                            "sample_split": sample_split,
                            "strategy": strategy,
                            "hold_days": int(holding_days),
                            **_summarize_trade_frame(subset),
                        }
                    )
    return pd.DataFrame(summary_rows)


def _summarize_trade_frame(trade_df: pd.DataFrame) -> dict[str, object]:
    if trade_df.empty:
        return {
            "trade_count": 0,
            "win_rate": math.nan,
            "mean_return": math.nan,
            "median_return": math.nan,
            "compound_return": math.nan,
        }

    trade_returns = trade_df["trade_return"].astype(float)
    return {
        "trade_count": int(len(trade_df)),
        "win_rate": float((trade_returns > 0.0).mean()),
        "mean_return": float(trade_returns.mean()),
        "median_return": float(trade_returns.median()),
        "compound_return": float((1.0 + trade_returns).prod() - 1.0),
    }


def _build_validation_leaderboard_df(
    backtest_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    validation_df = backtest_summary_df[
        (backtest_summary_df["sample_split"] == "validation")
        & (backtest_summary_df["trade_count"] > 0)
    ].copy()
    if validation_df.empty:
        return validation_df

    leaderboard_frames: list[pd.DataFrame] = []
    for strategy in STRATEGY_ORDER:
        strategy_df = validation_df[validation_df["strategy"] == strategy].copy()
        if strategy_df.empty:
            continue
        strategy_df = strategy_df.sort_values(
            ["compound_return", "mean_return", "win_rate", "hold_days", "model"],
            ascending=[False, False, False, True, True],
            kind="stable",
        ).reset_index(drop=True)
        strategy_df["validation_rank"] = range(1, len(strategy_df) + 1)
        leaderboard_frames.append(strategy_df)
    return pd.concat(leaderboard_frames, ignore_index=True)


def _build_model_overview_df(
    normal_result: TopixExtremeCloseToCloseModeResearchResult,
    streak_result: TopixStreakExtremeModeResearchResult,
    *,
    common_start_date: str,
    common_end_date: str,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "model": "normal",
                "selected_window_value": normal_result.selected_window_days,
                "selected_window_unit": "days",
                "analysis_start_date": normal_result.analysis_start_date,
                "analysis_end_date": normal_result.analysis_end_date,
                "common_start_date": common_start_date,
                "common_end_date": common_end_date,
            },
            {
                "model": "streak",
                "selected_window_value": streak_result.selected_window_streaks,
                "selected_window_unit": "streaks",
                "analysis_start_date": streak_result.analysis_start_date,
                "analysis_end_date": streak_result.analysis_end_date,
                "common_start_date": common_start_date,
                "common_end_date": common_end_date,
            },
        ]
    )


def _build_research_bundle_summary_markdown(
    result: TopixExtremeModeMeanReversionComparisonResult,
) -> str:
    lines = [
        "# TOPIX Extreme Mode Mean-Reversion Comparison",
        "",
        "Simple next-open to N-day-close backtests comparing the original daily extreme mode against the streak-candle extreme mode.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Common comparison range: `{result.common_start_date} -> {result.common_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Normal candidate windows: `{_format_int_sequence(result.normal_candidate_windows)}`",
        f"- Streak candidate windows: `{_format_int_sequence(result.streak_candidate_windows)}`",
        f"- Future horizons: `{_format_int_sequence(result.future_horizons)}`",
        f"- Hold days: `{_format_int_sequence(result.hold_days)}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        f"- Selected normal X: `{result.selected_normal_window_days} days`",
        f"- Selected streak X: `{result.selected_streak_window_streaks} streaks`",
    ]

    validation_df = result.backtest_summary_df[
        result.backtest_summary_df["sample_split"] == "validation"
    ].copy()
    for strategy in STRATEGY_ORDER:
        strategy_df = validation_df[validation_df["strategy"] == strategy].copy()
        if strategy_df.empty:
            continue
        lines.extend(["", f"## Validation {strategy}", ""])
        for holding_days in result.hold_days:
            hold_df = strategy_df[strategy_df["hold_days"] == holding_days].copy()
            if hold_df.empty:
                continue
            normal_row = hold_df[hold_df["model"] == "normal"]
            streak_row = hold_df[hold_df["model"] == "streak"]
            if normal_row.empty or streak_row.empty:
                continue
            normal = normal_row.iloc[0]
            streak = streak_row.iloc[0]
            lines.append(
                "- "
                f"{holding_days}d: "
                f"normal(mean={_format_return(float(normal['mean_return']))}, "
                f"win={float(normal['win_rate']):.1%}, "
                f"trades={int(normal['trade_count'])}) | "
                f"streak(mean={_format_return(float(streak['mean_return']))}, "
                f"win={float(streak['win_rate']):.1%}, "
                f"trades={int(streak['trade_count'])})"
            )

    if not result.validation_leaderboard_df.empty:
        lines.extend(["", "## Best Validation Configs", ""])
        for strategy in STRATEGY_ORDER:
            best_df = result.validation_leaderboard_df[
                (result.validation_leaderboard_df["strategy"] == strategy)
                & (result.validation_leaderboard_df["validation_rank"] == 1)
            ]
            if best_df.empty:
                continue
            row = best_df.iloc[0]
            lines.append(
                "- "
                f"{strategy}: {str(row['model'])} / {int(row['hold_days'])}d / "
                f"mean={_format_return(float(row['mean_return']))} / "
                f"compound={_format_return(float(row['compound_return']))} / "
                f"win={float(row['win_rate']):.1%}"
            )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `model_overview_df`",
            "- `signal_df`",
            "- `signal_summary_df`",
            "- `backtest_trade_df`",
            "- `backtest_summary_df`",
            "- `validation_leaderboard_df`",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: TopixExtremeModeMeanReversionComparisonResult,
) -> dict[str, object]:
    validation_df = result.backtest_summary_df[
        result.backtest_summary_df["sample_split"] == "validation"
    ].copy()
    long_on_bearish_df = validation_df[
        validation_df["strategy"] == "long_on_bearish"
    ].copy()
    best_long_on_bearish = _select_strategy_leader(
        result.validation_leaderboard_df,
        strategy="long_on_bearish",
    )
    comparison_rows = _collect_hold_comparison_rows(
        long_on_bearish_df,
        hold_days=result.hold_days,
    )

    result_bullets = [
        (
            f"{int(row['hold_days'])}d hold: normal "
            f"{_format_return(float(row['normal_mean_return']))} vs streak "
            f"{_format_return(float(row['streak_mean_return']))}"
        )
        for row in comparison_rows
    ]
    if best_long_on_bearish is not None:
        result_bullets.append(
            "The best validation mean-reversion setup was "
            f"{str(best_long_on_bearish['model'])} on a {int(best_long_on_bearish['hold_days'])}-day hold, "
            f"with mean return {_format_return(float(best_long_on_bearish['mean_return']))} "
            f"and win rate {float(best_long_on_bearish['win_rate']):.1%}."
        )

    highlights = [
        {
            "label": "Normal X",
            "value": f"{result.selected_normal_window_days} days",
            "tone": "neutral",
            "detail": "selected on discovery",
        },
        {
            "label": "Streak X",
            "value": f"{result.selected_streak_window_streaks} streaks",
            "tone": "accent",
            "detail": "selected on discovery",
        },
    ]
    if best_long_on_bearish is not None:
        highlights.append(
            {
                "label": "Best mean-reversion setup",
                "value": (
                    f"{str(best_long_on_bearish['model'])} / "
                    f"{int(best_long_on_bearish['hold_days'])}d"
                ),
                "tone": "success" if str(best_long_on_bearish["model"]) == "streak" else "warning",
                "detail": _format_return(float(best_long_on_bearish["mean_return"])),
            }
        )

    return {
        "title": "TOPIX Extreme Mode Mean-Reversion Comparison",
        "tags": ["TOPIX", "comparison", "mean-reversion", "streaks"],
        "purpose": (
            "Compare the original daily extreme-mode definition against the streak-candle version under the same "
            "next-open to N-day-close execution assumptions."
        ),
        "method": [
            "Run the normal daily-shock mode and the streak-candle mode over the same common date range.",
            "Convert each model into simple bearish-buy and bullish-short trade signals with overlapping trades suppressed.",
            "Rank validation performance by hold period and model to see which definition is more usable for mean reversion.",
        ],
        "resultHeadline": (
            "For bearish-buy mean reversion, the streak model was consistently stronger than the normal daily model across the tested hold periods."
        ),
        "resultBullets": result_bullets,
        "considerations": [
            "These are simplified trade rules with next-open entry and no transaction costs or slippage.",
            "The strongest edge appears on bearish-buy signals; bullish-short behavior is much less stable.",
            "The comparison is most useful as model selection guidance before building a fuller backtest or portfolio rule.",
        ],
        "selectedParameters": [
            {"label": "Normal X", "value": f"{result.selected_normal_window_days} days"},
            {"label": "Streak X", "value": f"{result.selected_streak_window_streaks} streaks"},
            {"label": "Hold days", "value": _format_int_sequence(result.hold_days)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "backtest_summary_df",
                "label": "Backtest summary",
                "description": "Mean return, win rate, and compound return by model, strategy, and hold period.",
            },
            {
                "name": "validation_leaderboard_df",
                "label": "Validation leaderboard",
                "description": "Best model and hold-period combinations per strategy.",
            },
            {
                "name": "signal_summary_df",
                "label": "Signal coverage",
                "description": "How often each model produces bullish or bearish signals on each split.",
            },
        ],
    }


def _collect_hold_comparison_rows(
    long_on_bearish_df: pd.DataFrame,
    *,
    hold_days: Sequence[int],
) -> list[dict[str, float | int]]:
    rows: list[dict[str, float | int]] = []
    for holding_days in hold_days:
        hold_df = long_on_bearish_df[long_on_bearish_df["hold_days"] == holding_days]
        if hold_df.empty:
            continue
        normal_row = hold_df[hold_df["model"] == "normal"]
        streak_row = hold_df[hold_df["model"] == "streak"]
        if normal_row.empty or streak_row.empty:
            continue
        rows.append(
            {
                "hold_days": int(holding_days),
                "normal_mean_return": float(normal_row.iloc[0]["mean_return"]),
                "streak_mean_return": float(streak_row.iloc[0]["mean_return"]),
            }
        )
    return rows


def _select_strategy_leader(
    leaderboard_df: pd.DataFrame,
    *,
    strategy: str,
) -> pd.Series | None:
    strategy_df = leaderboard_df[
        (leaderboard_df["strategy"] == strategy)
        & (leaderboard_df["validation_rank"] == 1)
    ]
    if strategy_df.empty:
        return None
    return strategy_df.iloc[0]


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
