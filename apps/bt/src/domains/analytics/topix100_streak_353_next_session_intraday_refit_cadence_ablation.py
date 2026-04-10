"""
Refit-cadence ablation for TOPIX100 next-session intraday LightGBM.

This study keeps the same trailing train window and feature family, but changes
how often the model is retrained in a live-style rolling loop:

- cadence 126: refit every 126 signal dates, score the next 126
- cadence 20: refit every 20 signal dates, score the next 20
- cadence 1: refit every signal date, score only the next one

The goal is to see whether frequent retraining helps or hurts the realized
Top-k long / bottom-k short portfolio and how much it destabilizes rankings.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRICE_SMA_WINDOW_ORDER,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
    DEFAULT_TOP_K_VALUES,
    _build_baseline_lookup_df,
    _build_baseline_scorecard,
    _build_baseline_validation_prediction_df,
    _build_feature_panel_df,
    _build_lightgbm_validation_prediction_df,
    _build_validation_model_comparison_df,
    _build_validation_model_summary_df,
    _build_validation_topk_tables,
    _format_int_sequence,
    _format_return,
    _load_lightgbm_regressor_cls,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm_walkforward import (
    _compute_daily_return_distribution_stats,
    _compute_portfolio_performance_stats,
    _resolve_primary_top_k,
)
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    run_topix100_streak_353_transfer_research,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode

DEFAULT_REFIT_CADENCE_DAYS: tuple[int, ...] = (1, 5, 20, 63, 126)
DEFAULT_REFERENCE_CADENCE_DAYS = 126
TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_REFIT_CADENCE_ABLATION_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-intraday-refit-cadence-ablation"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "cadence_config_df",
    "cadence_schedule_df",
    "cadence_prediction_df",
    "cadence_topk_pick_df",
    "cadence_topk_daily_df",
    "cadence_model_summary_df",
    "cadence_model_comparison_df",
    "cadence_vs_reference_df",
    "cadence_score_alignment_df",
    "cadence_book_overlap_df",
    "cadence_turnover_df",
    "portfolio_stats_df",
    "daily_return_distribution_df",
    "cadence_feature_importance_split_df",
    "cadence_feature_importance_df",
)
_CADENCE_METADATA_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "refit_index",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
    "is_partial_tail",
)
_CADENCE_VS_REFERENCE_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "model_name",
    "top_k",
    "series_name",
    "series_label",
    "date_count",
    "avg_daily_return",
    "volatility",
    "sharpe_ratio",
    "sortino_ratio",
    "win_loss_ratio",
    "positive_rate",
    "max_drawdown",
    "cagr",
    "total_return",
    "ending_equity",
    "reference_avg_daily_return",
    "reference_positive_rate",
    "reference_max_drawdown",
    "reference_sharpe_ratio",
    "reference_total_return",
    "reference_cadence_days",
    "avg_daily_return_delta_vs_reference",
    "positive_rate_delta_vs_reference",
    "max_drawdown_delta_vs_reference",
    "sharpe_ratio_delta_vs_reference",
    "total_return_delta_vs_reference",
)
_CADENCE_SCORE_ALIGNMENT_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "reference_cadence_days",
    "model_name",
    "date_count",
    "avg_score_rank_corr",
    "median_score_rank_corr",
    "min_score_rank_corr",
    "avg_score_pearson_corr",
)
_CADENCE_BOOK_OVERLAP_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "reference_cadence_days",
    "model_name",
    "top_k",
    "date_count",
    "avg_long_overlap_rate",
    "avg_short_overlap_rate",
    "avg_signed_book_overlap_rate",
)
_CADENCE_TURNOVER_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "model_name",
    "top_k",
    "transition_count",
    "avg_long_overlap_prev_day",
    "avg_short_overlap_prev_day",
    "avg_signed_book_overlap_prev_day",
)
_CADENCE_PORTFOLIO_STATS_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "model_name",
    "top_k",
    "series_name",
    "series_label",
    "date_count",
    "avg_daily_return",
    "volatility",
    "sharpe_ratio",
    "sortino_ratio",
    "win_loss_ratio",
    "positive_rate",
    "max_drawdown",
    "cagr",
    "total_return",
    "ending_equity",
)
_CADENCE_RETURN_DISTRIBUTION_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "model_name",
    "top_k",
    "series_name",
    "series_label",
    "count",
    "mean",
    "std",
    "min",
    "p01",
    "p05",
    "p25",
    "p50",
    "p75",
    "p95",
    "p99",
    "max",
)
_CADENCE_FEATURE_IMPORTANCE_COLUMNS: tuple[str, ...] = (
    "cadence_days",
    "model_name",
    "feature_name",
    "mean_importance_gain",
    "mean_importance_share",
    "refit_count",
    "importance_rank",
)


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


@dataclass(frozen=True)
class _RefitScheduleBlock:
    cadence_days: int
    refit_index: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    is_partial_tail: bool


@dataclass(frozen=True)
class _CadencePredictionArtifacts:
    cadence_config_df: pd.DataFrame
    cadence_schedule_df: pd.DataFrame
    cadence_prediction_df: pd.DataFrame
    cadence_topk_pick_df: pd.DataFrame
    cadence_topk_daily_df: pd.DataFrame
    cadence_feature_importance_split_df: pd.DataFrame


@dataclass(frozen=True)
class Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    price_feature: str
    price_feature_label: str
    volume_feature: str
    volume_feature_label: str
    short_window_streaks: int
    long_window_streaks: int
    validation_ratio: float
    top_k_values: tuple[int, ...]
    train_window: int
    purge_signal_dates: int
    refit_cadence_days: tuple[int, ...]
    reference_cadence_days: int
    categorical_feature_columns: tuple[str, ...]
    continuous_feature_columns: tuple[str, ...]
    cadence_config_df: pd.DataFrame
    cadence_schedule_df: pd.DataFrame
    cadence_prediction_df: pd.DataFrame
    cadence_topk_pick_df: pd.DataFrame
    cadence_topk_daily_df: pd.DataFrame
    cadence_model_summary_df: pd.DataFrame
    cadence_model_comparison_df: pd.DataFrame
    cadence_vs_reference_df: pd.DataFrame
    cadence_score_alignment_df: pd.DataFrame
    cadence_book_overlap_df: pd.DataFrame
    cadence_turnover_df: pd.DataFrame
    portfolio_stats_df: pd.DataFrame
    daily_return_distribution_df: pd.DataFrame
    cadence_feature_importance_split_df: pd.DataFrame
    cadence_feature_importance_df: pd.DataFrame


def run_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    top_k_values: tuple[int, ...] | list[int] | None = None,
    train_window: int = 756,
    purge_signal_dates: int = 0,
    refit_cadence_days: tuple[int, ...] | list[int] | None = None,
    reference_cadence_days: int = DEFAULT_REFERENCE_CADENCE_DAYS,
) -> Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if train_window <= 0:
        raise ValueError("train_window must be positive")
    if purge_signal_dates < 0:
        raise ValueError("purge_signal_dates must be >= 0")

    resolved_top_k_values = _normalize_positive_int_sequence(
        top_k_values,
        default=DEFAULT_TOP_K_VALUES,
        name="top_k_values",
    )
    resolved_refit_cadence_days = _normalize_positive_int_sequence(
        refit_cadence_days,
        default=DEFAULT_REFIT_CADENCE_DAYS,
        name="refit_cadence_days",
    )
    if reference_cadence_days not in set(resolved_refit_cadence_days):
        raise ValueError("reference_cadence_days must be included in refit_cadence_days")

    price_feature_to_window = {
        feature: window
        for feature, window in zip(PRICE_FEATURE_ORDER, PRICE_SMA_WINDOW_ORDER, strict=True)
    }
    volume_feature_to_window = {
        feature: window
        for feature, window in zip(VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True)
    }

    price_result = run_topix100_price_vs_sma_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        price_sma_windows=(price_feature_to_window[price_feature],),
        volume_sma_windows=(volume_feature_to_window[volume_feature],),
    )
    state_result = run_topix100_streak_353_transfer_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        future_horizons=(1,),
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )
    feature_panel_df = _build_feature_panel_df(
        event_panel_df=price_result.event_panel_df,
        state_result=state_result,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    if feature_panel_df.empty:
        raise ValueError("Feature panel is empty")

    artifacts = _build_refit_cadence_prediction_artifacts(
        feature_panel_df=feature_panel_df,
        top_k_values=resolved_top_k_values,
        train_window=train_window,
        purge_signal_dates=purge_signal_dates,
        refit_cadence_days=resolved_refit_cadence_days,
        categorical_feature_columns=DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
        continuous_feature_columns=(
            price_feature,
            volume_feature,
            *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
        ),
    )
    cadence_model_summary_df = _build_cadence_model_summary_df(artifacts.cadence_topk_daily_df)
    cadence_model_comparison_df = _build_cadence_model_comparison_df(cadence_model_summary_df)
    portfolio_stats_df = _build_cadence_portfolio_stats_df(artifacts.cadence_topk_daily_df)
    daily_return_distribution_df = _build_cadence_daily_return_distribution_df(
        artifacts.cadence_topk_daily_df
    )
    cadence_vs_reference_df = _build_cadence_vs_reference_df(
        portfolio_stats_df,
        reference_cadence_days=reference_cadence_days,
    )
    cadence_score_alignment_df = _build_cadence_score_alignment_df(
        artifacts.cadence_prediction_df,
        reference_cadence_days=reference_cadence_days,
    )
    cadence_book_overlap_df = _build_cadence_book_overlap_df(
        artifacts.cadence_topk_pick_df,
        reference_cadence_days=reference_cadence_days,
    )
    cadence_turnover_df = _build_cadence_turnover_df(artifacts.cadence_topk_pick_df)
    cadence_feature_importance_df = _build_cadence_feature_importance_df(
        artifacts.cadence_feature_importance_split_df
    )

    return Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=str(feature_panel_df["date"].min()),
        available_end_date=str(feature_panel_df["date"].max()),
        analysis_start_date=str(feature_panel_df["date"].min()),
        analysis_end_date=str(feature_panel_df["date"].max()),
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        validation_ratio=validation_ratio,
        top_k_values=resolved_top_k_values,
        train_window=train_window,
        purge_signal_dates=purge_signal_dates,
        refit_cadence_days=resolved_refit_cadence_days,
        reference_cadence_days=reference_cadence_days,
        categorical_feature_columns=DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
        continuous_feature_columns=(
            price_feature,
            volume_feature,
            *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
        ),
        cadence_config_df=artifacts.cadence_config_df,
        cadence_schedule_df=artifacts.cadence_schedule_df,
        cadence_prediction_df=artifacts.cadence_prediction_df,
        cadence_topk_pick_df=artifacts.cadence_topk_pick_df,
        cadence_topk_daily_df=artifacts.cadence_topk_daily_df,
        cadence_model_summary_df=cadence_model_summary_df,
        cadence_model_comparison_df=cadence_model_comparison_df,
        cadence_vs_reference_df=cadence_vs_reference_df,
        cadence_score_alignment_df=cadence_score_alignment_df,
        cadence_book_overlap_df=cadence_book_overlap_df,
        cadence_turnover_df=cadence_turnover_df,
        portfolio_stats_df=portfolio_stats_df,
        daily_return_distribution_df=daily_return_distribution_df,
        cadence_feature_importance_split_df=artifacts.cadence_feature_importance_split_df,
        cadence_feature_importance_df=cadence_feature_importance_df,
    )


def _build_refit_cadence_prediction_artifacts(
    *,
    feature_panel_df: pd.DataFrame,
    top_k_values: tuple[int, ...],
    train_window: int,
    purge_signal_dates: int,
    refit_cadence_days: tuple[int, ...],
    categorical_feature_columns: tuple[str, ...],
    continuous_feature_columns: tuple[str, ...],
) -> _CadencePredictionArtifacts:
    unique_dates = pd.to_datetime(
        sorted(feature_panel_df["date"].astype(str).unique())
    ).unique()
    regressor_cls = _load_lightgbm_regressor_cls()

    config_records: list[dict[str, Any]] = []
    schedule_records: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    topk_pick_frames: list[pd.DataFrame] = []
    topk_daily_frames: list[pd.DataFrame] = []
    feature_importance_frames: list[pd.DataFrame] = []

    for cadence_days in refit_cadence_days:
        schedule_blocks = _build_refit_schedule(
            unique_dates=pd.DatetimeIndex(unique_dates),
            train_window=train_window,
            purge_signal_dates=purge_signal_dates,
            cadence_days=cadence_days,
        )
        if not schedule_blocks:
            raise ValueError(f"No valid refit schedule was generated for cadence {cadence_days}")

        cadence_day_count = 0
        executed_refit_count = 0
        for block in schedule_blocks:
            train_feature_df = _slice_by_date_range(
                feature_panel_df,
                start_date=block.train_start,
                end_date=block.train_end,
            )
            test_feature_df = _slice_by_date_range(
                feature_panel_df,
                start_date=block.test_start,
                end_date=block.test_end,
            )
            if train_feature_df.empty or test_feature_df.empty:
                continue

            schedule_records.append(
                {
                    "cadence_days": cadence_days,
                    "refit_index": block.refit_index,
                    "train_start": block.train_start,
                    "train_end": block.train_end,
                    "test_start": block.test_start,
                    "test_end": block.test_end,
                    "train_row_count": int(len(train_feature_df)),
                    "test_row_count": int(len(test_feature_df)),
                    "train_date_count": int(train_feature_df["date"].nunique()),
                    "test_date_count": int(test_feature_df["date"].nunique()),
                    "is_partial_tail": block.is_partial_tail,
                }
            )
            executed_refit_count += 1
            cadence_day_count += int(test_feature_df["date"].nunique())

            baseline_lookup_df = _build_baseline_lookup_df(
                train_feature_df.assign(sample_split="discovery"),
            )
            baseline_scorecard = _build_baseline_scorecard(baseline_lookup_df)
            scoped_feature_df = pd.concat(
                [
                    train_feature_df.assign(sample_split="discovery"),
                    test_feature_df.assign(sample_split="validation"),
                ],
                ignore_index=True,
            )
            baseline_prediction_df = _build_baseline_validation_prediction_df(
                scoped_feature_df,
                baseline_scorecard=baseline_scorecard,
            )
            lightgbm_prediction_df, _config_record, importance_df = (
                _build_lightgbm_validation_prediction_df(
                    scoped_feature_df,
                    regressor_cls=regressor_cls,
                    categorical_feature_columns=categorical_feature_columns,
                    continuous_feature_columns=continuous_feature_columns,
                )
            )
            scoped_prediction_df = pd.concat(
                [baseline_prediction_df, lightgbm_prediction_df],
                ignore_index=True,
            ).assign(
                cadence_days=cadence_days,
                refit_index=block.refit_index,
                train_start=block.train_start,
                train_end=block.train_end,
                test_start=block.test_start,
                test_end=block.test_end,
                is_partial_tail=block.is_partial_tail,
            )
            prediction_frames.append(scoped_prediction_df)

            scoped_pick_df, scoped_daily_df = _build_validation_topk_tables(
                scoped_prediction_df,
                top_k_values=top_k_values,
            )
            topk_pick_frames.append(
                scoped_pick_df.assign(
                    cadence_days=cadence_days,
                    refit_index=block.refit_index,
                    train_start=block.train_start,
                    train_end=block.train_end,
                    test_start=block.test_start,
                    test_end=block.test_end,
                    is_partial_tail=block.is_partial_tail,
                )
            )
            topk_daily_frames.append(
                scoped_daily_df.assign(
                    cadence_days=cadence_days,
                    refit_index=block.refit_index,
                    train_start=block.train_start,
                    train_end=block.train_end,
                    test_start=block.test_start,
                    test_end=block.test_end,
                    is_partial_tail=block.is_partial_tail,
                )
            )
            feature_importance_frames.append(
                importance_df.assign(
                    cadence_days=cadence_days,
                    refit_index=block.refit_index,
                    train_start=block.train_start,
                    train_end=block.train_end,
                    test_start=block.test_start,
                    test_end=block.test_end,
                    is_partial_tail=block.is_partial_tail,
                )
            )

        config_records.append(
            {
                "cadence_days": cadence_days,
                "refit_count": executed_refit_count,
                "covered_day_count": cadence_day_count,
            }
        )

    if not prediction_frames or not topk_pick_frames or not topk_daily_frames:
        raise ValueError("No cadence prediction artifacts were generated")
    cadence_prediction_df = pd.concat(prediction_frames, ignore_index=True)
    cadence_topk_pick_df = pd.concat(topk_pick_frames, ignore_index=True)
    cadence_topk_daily_df = pd.concat(topk_daily_frames, ignore_index=True)
    cadence_feature_importance_split_df = (
        pd.concat(feature_importance_frames, ignore_index=True)
        if feature_importance_frames
        else pd.DataFrame()
    )
    return _CadencePredictionArtifacts(
        cadence_config_df=pd.DataFrame.from_records(config_records).sort_values(
            "cadence_days",
            kind="stable",
        ).reset_index(drop=True),
        cadence_schedule_df=pd.DataFrame.from_records(schedule_records).sort_values(
            ["cadence_days", "refit_index"],
            kind="stable",
        ).reset_index(drop=True),
        cadence_prediction_df=cadence_prediction_df.sort_values(
            ["cadence_days", "model_name", "date", "code"],
            kind="stable",
        ).reset_index(drop=True),
        cadence_topk_pick_df=cadence_topk_pick_df.sort_values(
            ["cadence_days", "top_k", "model_name", "date", "selection_side", "selection_rank", "code"],
            kind="stable",
        ).reset_index(drop=True),
        cadence_topk_daily_df=cadence_topk_daily_df.sort_values(
            ["cadence_days", "top_k", "model_name", "date"],
            kind="stable",
        ).reset_index(drop=True),
        cadence_feature_importance_split_df=cadence_feature_importance_split_df.sort_values(
            ["cadence_days", "refit_index", "model_name", "importance_rank"],
            kind="stable",
        ).reset_index(drop=True),
    )


def _build_refit_schedule(
    *,
    unique_dates: pd.DatetimeIndex,
    train_window: int,
    purge_signal_dates: int,
    cadence_days: int,
) -> list[_RefitScheduleBlock]:
    dates = unique_dates.sort_values().unique()
    if train_window <= 0 or cadence_days <= 0:
        raise ValueError("train_window and cadence_days must be positive")
    total = len(dates)
    start_index = train_window + purge_signal_dates
    if start_index >= total:
        return []

    blocks: list[_RefitScheduleBlock] = []
    refit_index = 1
    while start_index < total:
        train_start_index = start_index - purge_signal_dates - train_window
        train_end_index = start_index - purge_signal_dates - 1
        if train_start_index < 0 or train_end_index < train_start_index:
            break
        test_end_index = min(start_index + cadence_days - 1, total - 1)
        blocks.append(
            _RefitScheduleBlock(
                cadence_days=cadence_days,
                refit_index=refit_index,
                train_start=dates[train_start_index].date().isoformat(),
                train_end=dates[train_end_index].date().isoformat(),
                test_start=dates[start_index].date().isoformat(),
                test_end=dates[test_end_index].date().isoformat(),
                is_partial_tail=test_end_index < start_index + cadence_days - 1,
            )
        )
        start_index += cadence_days
        refit_index += 1
    return blocks


def _slice_by_date_range(
    df: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()


def _build_cadence_model_summary_df(cadence_topk_daily_df: pd.DataFrame) -> pd.DataFrame:
    if cadence_topk_daily_df.empty:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for cadence_days, scoped_df in cadence_topk_daily_df.groupby(
        "cadence_days",
        observed=True,
        sort=False,
    ):
        cadence_days_value = int(cast(Any, cadence_days))
        frames.append(
            _build_validation_model_summary_df(scoped_df).assign(
                cadence_days=cadence_days_value
            )
        )
    return pd.concat(frames, ignore_index=True).sort_values(
        ["cadence_days", "top_k", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cadence_model_comparison_df(cadence_model_summary_df: pd.DataFrame) -> pd.DataFrame:
    if cadence_model_summary_df.empty:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for cadence_days, scoped_df in cadence_model_summary_df.groupby(
        "cadence_days",
        observed=True,
        sort=False,
    ):
        cadence_days_value = int(cast(Any, cadence_days))
        frames.append(
            _build_validation_model_comparison_df(scoped_df).assign(
                cadence_days=cadence_days_value
            )
        )
    return pd.concat(frames, ignore_index=True).sort_values(
        ["cadence_days", "top_k"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cadence_portfolio_stats_df(cadence_topk_daily_df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (cadence_days, model_name, top_k), scoped_df in cadence_topk_daily_df.groupby(
        ["cadence_days", "model_name", "top_k"],
        observed=True,
        sort=False,
    ):
        ordered_df = scoped_df.sort_values("date", kind="stable").reset_index(drop=True)
        for series_name, series_label, series in _iter_daily_return_series(ordered_df):
            records.append(
                {
                    "cadence_days": int(cadence_days),
                    "model_name": str(model_name),
                    "top_k": int(top_k),
                    "series_name": series_name,
                    "series_label": series_label,
                    **_compute_portfolio_performance_stats(series),
                }
            )
    if not records:
        return _empty_frame(_CADENCE_PORTFOLIO_STATS_COLUMNS)
    return pd.DataFrame.from_records(records).sort_values(
        ["cadence_days", "top_k", "model_name", "series_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cadence_daily_return_distribution_df(
    cadence_topk_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (cadence_days, model_name, top_k), scoped_df in cadence_topk_daily_df.groupby(
        ["cadence_days", "model_name", "top_k"],
        observed=True,
        sort=False,
    ):
        ordered_df = scoped_df.sort_values("date", kind="stable").reset_index(drop=True)
        for series_name, series_label, series in _iter_daily_return_series(ordered_df):
            records.append(
                {
                    "cadence_days": int(cadence_days),
                    "model_name": str(model_name),
                    "top_k": int(top_k),
                    "series_name": series_name,
                    "series_label": series_label,
                    **_compute_daily_return_distribution_stats(series),
                }
            )
    if not records:
        return _empty_frame(_CADENCE_RETURN_DISTRIBUTION_COLUMNS)
    return pd.DataFrame.from_records(records).sort_values(
        ["cadence_days", "top_k", "model_name", "series_name"],
        kind="stable",
    ).reset_index(drop=True)


def _iter_daily_return_series(
    ordered_df: pd.DataFrame,
) -> tuple[tuple[str, str, pd.Series], ...]:
    return (
        ("long", "Long leg", ordered_df["long_return_mean"].astype(float)),
        ("short_edge", "Short edge", ordered_df["short_edge_mean"].astype(float)),
        ("gross_spread", "Gross spread", ordered_df["gross_edge"].astype(float)),
        (
            "pair_50_50",
            "Pair 50/50",
            ordered_df["gross_edge"].astype(float) / 2.0,
        ),
    )


def _build_cadence_vs_reference_df(
    portfolio_stats_df: pd.DataFrame,
    *,
    reference_cadence_days: int,
) -> pd.DataFrame:
    reference_df = portfolio_stats_df[
        portfolio_stats_df["cadence_days"] == reference_cadence_days
    ].copy()
    candidate_df = portfolio_stats_df[
        portfolio_stats_df["cadence_days"] != reference_cadence_days
    ].copy()
    if candidate_df.empty or reference_df.empty:
        return _empty_frame(_CADENCE_VS_REFERENCE_COLUMNS)

    merged_df = candidate_df.merge(
        reference_df[
            [
                "model_name",
                "top_k",
                "series_name",
                "avg_daily_return",
                "positive_rate",
                "max_drawdown",
                "sharpe_ratio",
                "total_return",
            ]
        ].rename(
            columns={
                "avg_daily_return": "reference_avg_daily_return",
                "positive_rate": "reference_positive_rate",
                "max_drawdown": "reference_max_drawdown",
                "sharpe_ratio": "reference_sharpe_ratio",
                "total_return": "reference_total_return",
            }
        ),
        on=["model_name", "top_k", "series_name"],
        how="left",
        validate="many_to_one",
    )
    merged_df["reference_cadence_days"] = int(reference_cadence_days)
    merged_df["avg_daily_return_delta_vs_reference"] = (
        merged_df["avg_daily_return"] - merged_df["reference_avg_daily_return"]
    )
    merged_df["positive_rate_delta_vs_reference"] = (
        merged_df["positive_rate"] - merged_df["reference_positive_rate"]
    )
    merged_df["max_drawdown_delta_vs_reference"] = (
        merged_df["max_drawdown"] - merged_df["reference_max_drawdown"]
    )
    merged_df["sharpe_ratio_delta_vs_reference"] = (
        merged_df["sharpe_ratio"] - merged_df["reference_sharpe_ratio"]
    )
    merged_df["total_return_delta_vs_reference"] = (
        merged_df["total_return"] - merged_df["reference_total_return"]
    )
    return merged_df.sort_values(
        ["cadence_days", "top_k", "model_name", "series_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cadence_score_alignment_df(
    cadence_prediction_df: pd.DataFrame,
    *,
    reference_cadence_days: int,
) -> pd.DataFrame:
    reference_df = cadence_prediction_df[
        cadence_prediction_df["cadence_days"] == reference_cadence_days
    ][["model_name", "date", "code", "score"]].rename(columns={"score": "reference_score"})
    records: list[dict[str, Any]] = []

    for (cadence_days, model_name), scoped_df in cadence_prediction_df.groupby(
        ["cadence_days", "model_name"],
        observed=True,
        sort=False,
    ):
        if int(cadence_days) == reference_cadence_days:
            continue
        merged_df = scoped_df[["date", "code", "score"]].merge(
            reference_df[reference_df["model_name"] == model_name][["date", "code", "reference_score"]],
            on=["date", "code"],
            how="inner",
            validate="one_to_one",
        )
        date_records: list[dict[str, Any]] = []
        for date_value, date_df in merged_df.groupby("date", observed=True, sort=False):
            score_series = date_df["score"].astype(float)
            reference_series = date_df["reference_score"].astype(float)
            rank_corr = score_series.rank(method="average").corr(
                reference_series.rank(method="average")
            )
            pearson_corr = score_series.corr(reference_series)
            date_records.append(
                {
                    "date": str(date_value),
                    "score_rank_corr": float(rank_corr) if pd.notna(rank_corr) else float("nan"),
                    "score_pearson_corr": float(pearson_corr) if pd.notna(pearson_corr) else float("nan"),
                }
            )
        if not date_records:
            continue
        date_df = pd.DataFrame.from_records(date_records)
        records.append(
            {
                "cadence_days": int(cadence_days),
                "reference_cadence_days": int(reference_cadence_days),
                "model_name": str(model_name),
                "date_count": int(len(date_df)),
                "avg_score_rank_corr": float(date_df["score_rank_corr"].mean()),
                "median_score_rank_corr": float(date_df["score_rank_corr"].median()),
                "min_score_rank_corr": float(date_df["score_rank_corr"].min()),
                "avg_score_pearson_corr": float(date_df["score_pearson_corr"].mean()),
            }
        )
    if not records:
        return _empty_frame(_CADENCE_SCORE_ALIGNMENT_COLUMNS)
    return pd.DataFrame.from_records(records).sort_values(
        ["cadence_days", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cadence_book_overlap_df(
    cadence_topk_pick_df: pd.DataFrame,
    *,
    reference_cadence_days: int,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (cadence_days, model_name, top_k), scoped_df in cadence_topk_pick_df.groupby(
        ["cadence_days", "model_name", "top_k"],
        observed=True,
        sort=False,
    ):
        if int(cadence_days) == reference_cadence_days:
            continue
        reference_df = cadence_topk_pick_df[
            (cadence_topk_pick_df["cadence_days"] == reference_cadence_days)
            & (cadence_topk_pick_df["model_name"] == model_name)
            & (cadence_topk_pick_df["top_k"] == top_k)
        ].copy()
        date_values = sorted(set(scoped_df["date"].astype(str)) & set(reference_df["date"].astype(str)))
        if not date_values:
            continue
        overlap_rows: list[dict[str, Any]] = []
        for date_value in date_values:
            candidate_date_df = scoped_df[scoped_df["date"] == date_value].copy()
            reference_date_df = reference_df[reference_df["date"] == date_value].copy()
            overlap_rows.append(
                {
                    "date": date_value,
                    "long_overlap_rate": _compute_side_overlap_rate(
                        candidate_date_df,
                        reference_date_df,
                        side="long",
                        expected_size=int(top_k),
                    ),
                    "short_overlap_rate": _compute_side_overlap_rate(
                        candidate_date_df,
                        reference_date_df,
                        side="short",
                        expected_size=int(top_k),
                    ),
                    "signed_book_overlap_rate": _compute_signed_book_overlap_rate(
                        candidate_date_df,
                        reference_date_df,
                        expected_size=int(top_k) * 2,
                    ),
                }
            )
        overlap_df = pd.DataFrame.from_records(overlap_rows)
        records.append(
            {
                "cadence_days": int(cadence_days),
                "reference_cadence_days": int(reference_cadence_days),
                "model_name": str(model_name),
                "top_k": int(top_k),
                "date_count": int(len(overlap_df)),
                "avg_long_overlap_rate": float(overlap_df["long_overlap_rate"].mean()),
                "avg_short_overlap_rate": float(overlap_df["short_overlap_rate"].mean()),
                "avg_signed_book_overlap_rate": float(overlap_df["signed_book_overlap_rate"].mean()),
            }
        )
    if not records:
        return _empty_frame(_CADENCE_BOOK_OVERLAP_COLUMNS)
    return pd.DataFrame.from_records(records).sort_values(
        ["cadence_days", "top_k", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cadence_turnover_df(cadence_topk_pick_df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (cadence_days, model_name, top_k), scoped_df in cadence_topk_pick_df.groupby(
        ["cadence_days", "model_name", "top_k"],
        observed=True,
        sort=False,
    ):
        ordered_dates = sorted(scoped_df["date"].astype(str).unique())
        if len(ordered_dates) <= 1:
            continue
        turnover_rows: list[dict[str, float]] = []
        previous_df: pd.DataFrame | None = None
        for date_value in ordered_dates:
            current_df = scoped_df[scoped_df["date"] == date_value].copy()
            if previous_df is not None:
                turnover_rows.append(
                    {
                        "date": date_value,
                        "long_overlap_prev_day": _compute_side_overlap_rate(
                            current_df,
                            previous_df,
                            side="long",
                            expected_size=int(top_k),
                        ),
                        "short_overlap_prev_day": _compute_side_overlap_rate(
                            current_df,
                            previous_df,
                            side="short",
                            expected_size=int(top_k),
                        ),
                        "signed_book_overlap_prev_day": _compute_signed_book_overlap_rate(
                            current_df,
                            previous_df,
                            expected_size=int(top_k) * 2,
                        ),
                    }
                )
            previous_df = current_df
        turnover_df = pd.DataFrame.from_records(turnover_rows)
        records.append(
            {
                "cadence_days": int(cadence_days),
                "model_name": str(model_name),
                "top_k": int(top_k),
                "transition_count": int(len(turnover_df)),
                "avg_long_overlap_prev_day": float(turnover_df["long_overlap_prev_day"].mean()),
                "avg_short_overlap_prev_day": float(turnover_df["short_overlap_prev_day"].mean()),
                "avg_signed_book_overlap_prev_day": float(
                    turnover_df["signed_book_overlap_prev_day"].mean()
                ),
            }
        )
    if not records:
        return _empty_frame(_CADENCE_TURNOVER_COLUMNS)
    return pd.DataFrame.from_records(records).sort_values(
        ["cadence_days", "top_k", "model_name"],
        kind="stable",
    ).reset_index(drop=True)


def _compute_side_overlap_rate(
    candidate_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    *,
    side: str,
    expected_size: int,
) -> float:
    candidate_set = set(
        candidate_df[candidate_df["selection_side"] == side]["code"].astype(str)
    )
    reference_set = set(
        reference_df[reference_df["selection_side"] == side]["code"].astype(str)
    )
    if expected_size <= 0:
        return float("nan")
    return len(candidate_set & reference_set) / expected_size


def _compute_signed_book_overlap_rate(
    candidate_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    *,
    expected_size: int,
) -> float:
    candidate_set = {
        f"{row.selection_side}:{row.code}"
        for row in candidate_df[["selection_side", "code"]].itertuples(index=False)
    }
    reference_set = {
        f"{row.selection_side}:{row.code}"
        for row in reference_df[["selection_side", "code"]].itertuples(index=False)
    }
    if expected_size <= 0:
        return float("nan")
    return len(candidate_set & reference_set) / expected_size


def _build_cadence_feature_importance_df(
    cadence_feature_importance_split_df: pd.DataFrame,
) -> pd.DataFrame:
    if cadence_feature_importance_split_df.empty:
        return _empty_frame(_CADENCE_FEATURE_IMPORTANCE_COLUMNS)
    aggregated_df = (
        cadence_feature_importance_split_df.groupby(
            ["cadence_days", "model_name", "feature_name"],
            observed=True,
            sort=False,
        )
        .agg(
            mean_importance_gain=("importance_gain", "mean"),
            mean_importance_share=("importance_share", "mean"),
            refit_count=("refit_index", "nunique"),
        )
        .reset_index()
        .sort_values(
            ["cadence_days", "model_name", "mean_importance_gain", "feature_name"],
            ascending=[True, True, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    aggregated_df["importance_rank"] = (
        aggregated_df.groupby(["cadence_days", "model_name"], observed=True).cumcount() + 1
    )
    return aggregated_df


def write_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research_bundle(
    result: Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_REFIT_CADENCE_ABLATION_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "top_k_values": list(result.top_k_values),
            "train_window": result.train_window,
            "purge_signal_dates": result.purge_signal_dates,
            "refit_cadence_days": list(result.refit_cadence_days),
            "reference_cadence_days": result.reference_cadence_days,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research_bundle(
    bundle_path: str | Path,
) -> Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_streak_353_next_session_intraday_refit_cadence_ablation_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_REFIT_CADENCE_ABLATION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_streak_353_next_session_intraday_refit_cadence_ablation_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STREAK_353_NEXT_SESSION_INTRADAY_REFIT_CADENCE_ABLATION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult,
) -> str:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    reference_row = _select_cadence_stats_row(
        result.portfolio_stats_df,
        cadence_days=result.reference_cadence_days,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    daily_row = _select_cadence_stats_row(
        result.portfolio_stats_df,
        cadence_days=1,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    daily_vs_reference = _select_cadence_reference_row(
        result.cadence_vs_reference_df,
        cadence_days=1,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    daily_alignment = _select_score_alignment_row(
        result.cadence_score_alignment_df,
        cadence_days=1,
        model_name="lightgbm",
    )
    daily_overlap = _select_book_overlap_row(
        result.cadence_book_overlap_df,
        cadence_days=1,
        model_name="lightgbm",
        top_k=primary_top_k,
    )

    lines = [
        "# TOPIX100 Next-Session Intraday Refit Cadence Ablation",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Train window: `{result.train_window}` signal dates",
        f"- Purge signal dates: `{result.purge_signal_dates}`",
        f"- Refit cadence grid: `{_format_int_sequence(result.refit_cadence_days)}`",
        f"- Reference cadence: `{result.reference_cadence_days}`",
        f"- Top-k evaluation: `{_format_int_sequence(result.top_k_values)}`",
        "",
        "## Current Read",
        "",
        "This is the retrain-cadence check for the live-style intraday scorer. Every variant uses the same trailing train window, but changes how frequently the model is refit before scoring the next block.",
    ]
    if reference_row is not None:
        lines.append(
            f"- Reference cadence {result.reference_cadence_days} Pair 50/50 Top/Bottom {primary_top_k}: avg `{_format_return(float(reference_row['avg_daily_return']))}`, positive `{float(reference_row['positive_rate']):.2%}`, max DD `{_format_return(float(reference_row['max_drawdown']))}`."
        )
    if daily_row is not None:
        lines.append(
            f"- Daily refit cadence 1 Pair 50/50 Top/Bottom {primary_top_k}: avg `{_format_return(float(daily_row['avg_daily_return']))}`, positive `{float(daily_row['positive_rate']):.2%}`, max DD `{_format_return(float(daily_row['max_drawdown']))}`."
        )
    if daily_vs_reference is not None:
        lines.append(
            f"- Daily refit delta vs cadence {result.reference_cadence_days}: avg `{_format_return(float(daily_vs_reference['avg_daily_return_delta_vs_reference']))}`, positive `{float(daily_vs_reference['positive_rate_delta_vs_reference']):+.2%}`, max DD `{_format_return(float(daily_vs_reference['max_drawdown_delta_vs_reference']))}`."
        )
    if daily_alignment is not None:
        lines.append(
            f"- Daily refit score alignment vs cadence {result.reference_cadence_days}: avg rank corr `{float(daily_alignment['avg_score_rank_corr']):.3f}`, avg pearson `{float(daily_alignment['avg_score_pearson_corr']):.3f}`."
        )
    if daily_overlap is not None:
        lines.append(
            f"- Daily refit book overlap vs cadence {result.reference_cadence_days} at Top/Bottom {primary_top_k}: long `{float(daily_overlap['avg_long_overlap_rate']):.2%}`, short `{float(daily_overlap['avg_short_overlap_rate']):.2%}`, signed book `{float(daily_overlap['avg_signed_book_overlap_rate']):.2%}`."
        )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Streak353NextSessionIntradayRefitCadenceAblationResearchResult,
) -> dict[str, Any]:
    primary_top_k = _resolve_primary_top_k(result.top_k_values)
    reference_row = _select_cadence_stats_row(
        result.portfolio_stats_df,
        cadence_days=result.reference_cadence_days,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    daily_row = _select_cadence_stats_row(
        result.portfolio_stats_df,
        cadence_days=1,
        model_name="lightgbm",
        top_k=primary_top_k,
        series_name="pair_50_50",
    )
    daily_overlap = _select_book_overlap_row(
        result.cadence_book_overlap_df,
        cadence_days=1,
        model_name="lightgbm",
        top_k=primary_top_k,
    )

    result_bullets = [
        "Every cadence uses the same trailing train window and the same decile-only intraday LightGBM feature family.",
        "The only thing that changes is how long the fitted model is reused before the next retrain.",
    ]
    if reference_row is not None:
        result_bullets.append(
            f"At cadence {result.reference_cadence_days}, the LightGBM Top/Bottom {primary_top_k} pair averaged {_format_return(float(reference_row['avg_daily_return']))} with positive days {float(reference_row['positive_rate']):.2%}."
        )
    if daily_row is not None:
        result_bullets.append(
            f"At daily refit cadence 1, the same pair averaged {_format_return(float(daily_row['avg_daily_return']))} with positive days {float(daily_row['positive_rate']):.2%}."
        )
    if daily_overlap is not None:
        result_bullets.append(
            f"Daily refit matched the cadence {result.reference_cadence_days} signed book only {float(daily_overlap['avg_signed_book_overlap_rate']):.2%} of the time at Top/Bottom {primary_top_k}."
        )

    highlights = [
        {
            "label": "Train window",
            "value": str(result.train_window),
            "tone": "neutral",
            "detail": "signal dates",
        },
        {
            "label": "Cadence grid",
            "value": _format_int_sequence(result.refit_cadence_days),
            "tone": "accent",
            "detail": "refit days",
        },
        {
            "label": "Reference cadence",
            "value": str(result.reference_cadence_days),
            "tone": "accent",
            "detail": "comparison anchor",
        },
    ]
    if reference_row is not None:
        highlights.append(
            {
                "label": f"Pair {result.reference_cadence_days}",
                "value": _format_return(float(reference_row["avg_daily_return"])),
                "tone": "success",
                "detail": f"Top/Bottom {primary_top_k}",
            }
        )
    if daily_row is not None:
        highlights.append(
            {
                "label": "Pair 1",
                "value": _format_return(float(daily_row["avg_daily_return"])),
                "tone": "danger",
                "detail": f"Top/Bottom {primary_top_k}",
            }
        )

    return {
        "title": "TOPIX100 Next-Session Intraday Refit Cadence Ablation",
        "tags": ["TOPIX100", "intraday", "lightgbm", "refit cadence", "robustness"],
        "purpose": (
            "Test whether the intraday LightGBM edge is robust to retrain frequency or whether frequent refits destabilize the ranked book."
        ),
        "method": [
            "Build the same decile-only next-session intraday feature panel used by the runtime scorer.",
            "Refit LightGBM and the lookup baseline on the trailing train window, then reuse the fitted model for the next cadence block.",
            "Compare cadence-specific return quality, score alignment, and book overlap against a slower reference cadence.",
        ],
        "resultHeadline": (
            "This ablation isolates retrain frequency as a first-class strategy parameter rather than treating it as an implementation detail."
        ),
        "resultBullets": result_bullets,
        "considerations": [
            "This is a live-style rolling study, so it is more relevant to the runtime scorer than the original fixed completed-block walk-forward bundle.",
            "Execution still ignores fees, slippage, and borrow constraints.",
            "If daily refit underperforms slower cadences, the likely mechanism is rank instability rather than weaker average signal quality per se.",
        ],
        "selectedParameters": [
            {"label": "Short X", "value": str(result.short_window_streaks)},
            {"label": "Long X", "value": str(result.long_window_streaks)},
            {"label": "Train window", "value": str(result.train_window)},
            {"label": "Purge", "value": str(result.purge_signal_dates)},
            {"label": "Cadences", "value": _format_int_sequence(result.refit_cadence_days)},
            {"label": "Reference", "value": str(result.reference_cadence_days)},
            {"label": "Top-K grid", "value": _format_int_sequence(result.top_k_values)},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "cadence_vs_reference_df",
                "label": "Return delta vs reference cadence",
                "description": "Difference in pair return, positive rate, Sharpe, and drawdown versus the slower reference cadence.",
            },
            {
                "name": "cadence_score_alignment_df",
                "label": "Score alignment",
                "description": "How closely each cadence's daily score ordering tracks the reference cadence.",
            },
            {
                "name": "cadence_book_overlap_df",
                "label": "Book overlap",
                "description": "How often the selected Top/Bottom books match the reference cadence on the same date.",
            },
            {
                "name": "cadence_turnover_df",
                "label": "Day-to-day turnover proxy",
                "description": "Consecutive-day overlap inside each cadence, useful for diagnosing rank instability.",
            },
        ],
    }


def _select_cadence_stats_row(
    stats_df: pd.DataFrame,
    *,
    cadence_days: int,
    model_name: str,
    top_k: int,
    series_name: str,
) -> pd.Series | None:
    required_columns = {"cadence_days", "model_name", "top_k", "series_name"}
    if stats_df.empty or not required_columns.issubset(stats_df.columns):
        return None
    scoped_df = stats_df[
        (stats_df["cadence_days"] == cadence_days)
        & (stats_df["model_name"] == model_name)
        & (stats_df["top_k"] == top_k)
        & (stats_df["series_name"] == series_name)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_cadence_reference_row(
    comparison_df: pd.DataFrame,
    *,
    cadence_days: int,
    model_name: str,
    top_k: int,
    series_name: str,
) -> pd.Series | None:
    required_columns = {"cadence_days", "model_name", "top_k", "series_name"}
    if comparison_df.empty or not required_columns.issubset(comparison_df.columns):
        return None
    scoped_df = comparison_df[
        (comparison_df["cadence_days"] == cadence_days)
        & (comparison_df["model_name"] == model_name)
        & (comparison_df["top_k"] == top_k)
        & (comparison_df["series_name"] == series_name)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_score_alignment_row(
    alignment_df: pd.DataFrame,
    *,
    cadence_days: int,
    model_name: str,
) -> pd.Series | None:
    required_columns = {"cadence_days", "model_name"}
    if alignment_df.empty or not required_columns.issubset(alignment_df.columns):
        return None
    scoped_df = alignment_df[
        (alignment_df["cadence_days"] == cadence_days)
        & (alignment_df["model_name"] == model_name)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_book_overlap_row(
    overlap_df: pd.DataFrame,
    *,
    cadence_days: int,
    model_name: str,
    top_k: int,
) -> pd.Series | None:
    required_columns = {"cadence_days", "model_name", "top_k"}
    if overlap_df.empty or not required_columns.issubset(overlap_df.columns):
        return None
    scoped_df = overlap_df[
        (overlap_df["cadence_days"] == cadence_days)
        & (overlap_df["model_name"] == model_name)
        & (overlap_df["top_k"] == top_k)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]
