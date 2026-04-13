"""
TOPIX downside return-standard-deviation shock-confirmation vote overlay research.

This study fixes one stress family plus one trend family and one breadth family,
then compares family-level confirmation logic:

- `stress_and_trend_and_breadth`
- `stress_and_trend_or_breadth`
- `two_of_three_vote`

Execution convention:

- signals use information available at date X close
- rebalancing happens at X+1 open
- overnight X close -> X+1 open earns the previous exposure ratio
- intraday X+1 open -> X+1 close earns the new exposure ratio
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import product
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
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
    _query_topix_daily_frame,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _fetch_date_range,
    _open_analysis_connection,
)
from src.domains.analytics.topix_downside_return_standard_deviation_exposure_timing import (
    ANNUALIZATION_FACTOR,
    EXECUTION_TIMING,
    _annotate_market_frame_with_split_labels,
    _build_baseline_daily_df,
    _build_drawdown_series,
    _coerce_numeric_value,
    _coerce_sortable_number,
    _compute_activity_stats,
    _compute_downside_standard_deviation,
    _compute_return_series_stats,
    _format_float_sequence,
    _format_int_sequence,
    _format_percent,
    _format_ratio,
    _normalize_exposure_ratio_sequence,
    _normalize_non_negative_float_sequence,
    _prepare_topix_market_frame,
    _subset_daily_df_for_split,
)
from src.domains.analytics.topix_downside_return_standard_deviation_trend_breadth_overlay import (
    DEFAULT_MIN_CONSTITUENTS_PER_DAY,
    _build_common_signal_frame_with_regimes,
    _build_topix100_breadth_daily_df,
    _build_topix_trend_feature_df,
    _lookup_selection_row,
    _lookup_split_row,
    _normalize_rule_sequence,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _query_topix100_stock_history,
)

DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS: tuple[int, ...] = (5,)
DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS: tuple[int, ...] = (1, 2)
DEFAULT_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS: tuple[
    float, ...
] = (0.24, 0.25)
DEFAULT_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS: tuple[
    float, ...
] = (0.20, 0.22)
DEFAULT_REDUCED_EXPOSURE_RATIOS: tuple[float, ...] = (0.0, 0.10)
DEFAULT_TREND_FAMILY_RULES: tuple[str, ...] = (
    "close_below_sma20",
    "sma20_below_sma60",
    "drawdown_63d_le_neg0p05",
    "return_10d_le_neg0p03",
)
DEFAULT_BREADTH_FAMILY_RULES: tuple[str, ...] = (
    "topix100_above_sma20_le_0p40",
    "topix100_positive_5d_le_0p40",
    "topix100_at_20d_low_ge_0p20",
)
DEFAULT_TREND_VOTE_THRESHOLDS: tuple[int, ...] = (1, 2, 3, 4)
DEFAULT_BREADTH_VOTE_THRESHOLDS: tuple[int, ...] = (1, 2, 3)
DEFAULT_CONFIRMATION_MODES: tuple[str, ...] = (
    "stress_and_trend_and_breadth",
    "stress_and_trend_or_breadth",
    "two_of_three_vote",
)
DEFAULT_RANK_TOP_KS: tuple[int, ...] = (10, 20, 50)
DEFAULT_DISCOVERY_WINDOW_DAYS = 252 * 3
DEFAULT_VALIDATION_WINDOW_DAYS = 252
DEFAULT_STEP_WINDOW_DAYS = 126
TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_VOTE_OVERLAY_EXPERIMENT_ID = (
    "market-behavior/topix-downside-return-standard-deviation-shock-confirmation-vote-overlay"
)
_VALID_TREND_RULES = frozenset(DEFAULT_TREND_FAMILY_RULES)
_VALID_BREADTH_RULES = frozenset(DEFAULT_BREADTH_FAMILY_RULES)
_VALID_CONFIRMATION_MODES = frozenset(DEFAULT_CONFIRMATION_MODES)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "topix_daily_df",
    "breadth_daily_df",
    "baseline_metrics_df",
    "candidate_metrics_df",
    "candidate_comparison_df",
    "selection_summary_df",
    "rank_stability_df",
    "walkforward_fold_candidate_rank_df",
    "walkforward_rank_diagnostics_df",
    "walkforward_top1_df",
    "walkforward_top1_summary_df",
    "top1_selection_frequency_df",
    "best_sharpe_daily_df",
)
_PARAMETER_COLUMNS: tuple[str, ...] = (
    "downside_return_standard_deviation_window_days",
    "downside_return_standard_deviation_mean_window_days",
    "high_annualized_downside_return_standard_deviation_threshold",
    "low_annualized_downside_return_standard_deviation_threshold",
    "reduced_exposure_ratio",
    "trend_vote_threshold",
    "breadth_vote_threshold",
    "confirmation_mode",
)


@dataclass(frozen=True)
class TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    breadth_available_start_date: str | None
    breadth_available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    validation_ratio: float
    annualization_factor: float
    execution_timing: str
    breadth_universe: str
    min_constituents_per_day: int
    downside_return_standard_deviation_window_days: tuple[int, ...]
    downside_return_standard_deviation_mean_window_days: tuple[int, ...]
    high_annualized_downside_return_standard_deviation_thresholds: tuple[float, ...]
    low_annualized_downside_return_standard_deviation_thresholds: tuple[float, ...]
    reduced_exposure_ratios: tuple[float, ...]
    trend_family_rules: tuple[str, ...]
    breadth_family_rules: tuple[str, ...]
    trend_vote_thresholds: tuple[int, ...]
    breadth_vote_thresholds: tuple[int, ...]
    confirmation_modes: tuple[str, ...]
    rank_top_ks: tuple[int, ...]
    discovery_window_days: int
    validation_window_days: int
    step_window_days: int
    candidate_count: int
    fold_count: int
    topix_daily_df: pd.DataFrame
    breadth_daily_df: pd.DataFrame
    baseline_metrics_df: pd.DataFrame
    candidate_metrics_df: pd.DataFrame
    candidate_comparison_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    rank_stability_df: pd.DataFrame
    walkforward_fold_candidate_rank_df: pd.DataFrame
    walkforward_rank_diagnostics_df: pd.DataFrame
    walkforward_top1_df: pd.DataFrame
    walkforward_top1_summary_df: pd.DataFrame
    top1_selection_frequency_df: pd.DataFrame
    best_sharpe_daily_df: pd.DataFrame


def get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    downside_return_standard_deviation_window_days: Sequence[int] | None = None,
    downside_return_standard_deviation_mean_window_days: Sequence[int] | None = None,
    high_annualized_downside_return_standard_deviation_thresholds: Sequence[float]
    | None = None,
    low_annualized_downside_return_standard_deviation_thresholds: Sequence[float]
    | None = None,
    reduced_exposure_ratios: Sequence[float] | None = None,
    trend_family_rules: Sequence[str] | None = None,
    breadth_family_rules: Sequence[str] | None = None,
    trend_vote_thresholds: Sequence[int] | None = None,
    breadth_vote_thresholds: Sequence[int] | None = None,
    confirmation_modes: Sequence[str] | None = None,
    min_constituents_per_day: int = DEFAULT_MIN_CONSTITUENTS_PER_DAY,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    rank_top_ks: Sequence[int] | None = None,
    discovery_window_days: int = DEFAULT_DISCOVERY_WINDOW_DAYS,
    validation_window_days: int = DEFAULT_VALIDATION_WINDOW_DAYS,
    step_window_days: int = DEFAULT_STEP_WINDOW_DAYS,
) -> TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult:
    resolved_std_windows = _normalize_positive_int_sequence(
        downside_return_standard_deviation_window_days,
        default=DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
        name="downside_return_standard_deviation_window_days",
    )
    resolved_mean_windows = _normalize_positive_int_sequence(
        downside_return_standard_deviation_mean_window_days,
        default=DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS,
        name="downside_return_standard_deviation_mean_window_days",
    )
    resolved_high_thresholds = _normalize_non_negative_float_sequence(
        high_annualized_downside_return_standard_deviation_thresholds,
        default=DEFAULT_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
        name="high_annualized_downside_return_standard_deviation_thresholds",
    )
    resolved_low_thresholds = _normalize_non_negative_float_sequence(
        low_annualized_downside_return_standard_deviation_thresholds,
        default=DEFAULT_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
        name="low_annualized_downside_return_standard_deviation_thresholds",
    )
    resolved_reduced_exposures = _normalize_exposure_ratio_sequence(
        reduced_exposure_ratios,
        default=DEFAULT_REDUCED_EXPOSURE_RATIOS,
        name="reduced_exposure_ratios",
    )
    resolved_trend_family_rules = _normalize_rule_sequence(
        trend_family_rules,
        default=DEFAULT_TREND_FAMILY_RULES,
        name="trend_family_rules",
        valid_values=_VALID_TREND_RULES,
    )
    resolved_breadth_family_rules = _normalize_rule_sequence(
        breadth_family_rules,
        default=DEFAULT_BREADTH_FAMILY_RULES,
        name="breadth_family_rules",
        valid_values=_VALID_BREADTH_RULES,
    )
    resolved_trend_vote_thresholds = _normalize_positive_int_sequence(
        trend_vote_thresholds,
        default=DEFAULT_TREND_VOTE_THRESHOLDS,
        name="trend_vote_thresholds",
    )
    resolved_breadth_vote_thresholds = _normalize_positive_int_sequence(
        breadth_vote_thresholds,
        default=DEFAULT_BREADTH_VOTE_THRESHOLDS,
        name="breadth_vote_thresholds",
    )
    resolved_confirmation_modes = _normalize_confirmation_modes(confirmation_modes)
    resolved_rank_top_ks = _normalize_positive_int_sequence(
        rank_top_ks,
        default=DEFAULT_RANK_TOP_KS,
        name="rank_top_ks",
    )
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")
    if min_constituents_per_day <= 0:
        raise ValueError("min_constituents_per_day must be positive")
    if discovery_window_days <= 0 or validation_window_days <= 0 or step_window_days <= 0:
        raise ValueError(
            "discovery_window_days, validation_window_days, and step_window_days must be positive"
        )
    if max(resolved_trend_vote_thresholds) > len(resolved_trend_family_rules):
        raise ValueError("trend_vote_thresholds must not exceed the trend family size")
    if max(resolved_breadth_vote_thresholds) > len(resolved_breadth_family_rules):
        raise ValueError("breadth_vote_thresholds must not exceed the breadth family size")

    parameter_grid = [
        {
            "candidate_id": _build_candidate_id(
                stddev_window_days=std_window,
                mean_window_days=mean_window,
                high_threshold=high_threshold,
                low_threshold=low_threshold,
                reduced_exposure_ratio=reduced_exposure_ratio,
                trend_vote_threshold=trend_vote_threshold,
                breadth_vote_threshold=breadth_vote_threshold,
                confirmation_mode=confirmation_mode,
            ),
            "downside_return_standard_deviation_window_days": std_window,
            "downside_return_standard_deviation_mean_window_days": mean_window,
            "high_annualized_downside_return_standard_deviation_threshold": high_threshold,
            "low_annualized_downside_return_standard_deviation_threshold": low_threshold,
            "reduced_exposure_ratio": reduced_exposure_ratio,
            "trend_vote_threshold": trend_vote_threshold,
            "breadth_vote_threshold": breadth_vote_threshold,
            "confirmation_mode": confirmation_mode,
        }
        for std_window, mean_window, high_threshold, low_threshold, reduced_exposure_ratio, trend_vote_threshold, breadth_vote_threshold, confirmation_mode in product(
            resolved_std_windows,
            resolved_mean_windows,
            resolved_high_thresholds,
            resolved_low_thresholds,
            resolved_reduced_exposures,
            resolved_trend_vote_thresholds,
            resolved_breadth_vote_thresholds,
            resolved_confirmation_modes,
        )
        if low_threshold <= high_threshold
    ]
    if not parameter_grid:
        raise ValueError("No valid parameter combinations remained after low/high threshold filtering")
    if max(resolved_rank_top_ks) > len(parameter_grid):
        raise ValueError("rank_top_ks must not exceed the candidate count")

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
            future_horizons=(1,),
        )
        breadth_history_df = _query_topix100_stock_history(
            ctx.connection,
            end_date=end_date,
        )

    if breadth_history_df.empty:
        raise ValueError("No TOPIX100 breadth history was available")
    breadth_history_df = breadth_history_df.copy()
    breadth_history_df["date"] = breadth_history_df["date"].astype(str)
    if start_date is not None:
        breadth_history_df = breadth_history_df[breadth_history_df["date"] >= start_date].copy()
    if end_date is not None:
        breadth_history_df = breadth_history_df[breadth_history_df["date"] <= end_date].copy()
    if breadth_history_df.empty:
        raise ValueError("No TOPIX100 breadth rows remained after date filters")

    breadth_available_start_date = str(breadth_history_df["date"].min())
    breadth_available_end_date = str(breadth_history_df["date"].max())

    market_frame_df = _build_topix_trend_feature_df(_prepare_topix_market_frame(topix_daily_df))
    breadth_daily_df = _build_topix100_breadth_daily_df(
        breadth_history_df,
        min_constituents_per_day=min_constituents_per_day,
    )
    signal_base_df = _build_common_signal_frame_with_regimes(
        market_frame_df,
        breadth_daily_df=breadth_daily_df,
        max_downside_return_standard_deviation_window_days=max(resolved_std_windows),
        max_downside_return_standard_deviation_mean_window_days=max(resolved_mean_windows),
        validation_ratio=validation_ratio,
    )
    baseline_daily_df = _build_baseline_daily_df(signal_base_df)
    baseline_metrics_df = _build_baseline_metrics_df(baseline_daily_df)

    candidate_metric_rows: list[dict[str, Any]] = []
    candidate_return_series_by_id: dict[str, pd.Series] = {}
    best_sharpe_daily_df = pd.DataFrame()
    best_sharpe_key: tuple[float, float, float, str] | None = None
    signal_frame_cache: dict[tuple[int, int], pd.DataFrame] = {}

    for params in parameter_grid:
        std_window = int(params["downside_return_standard_deviation_window_days"])
        mean_window = int(params["downside_return_standard_deviation_mean_window_days"])
        high_threshold = float(
            params["high_annualized_downside_return_standard_deviation_threshold"]
        )
        low_threshold = float(params["low_annualized_downside_return_standard_deviation_threshold"])
        reduced_exposure_ratio = float(params["reduced_exposure_ratio"])
        trend_vote_threshold = int(params["trend_vote_threshold"])
        breadth_vote_threshold = int(params["breadth_vote_threshold"])
        confirmation_mode = str(params["confirmation_mode"])
        signal_key = (std_window, mean_window)
        if signal_key not in signal_frame_cache:
            signal_frame_cache[signal_key] = _build_candidate_signal_frame_on_common_base(
                market_frame_df,
                signal_base_df=signal_base_df,
                stddev_window_days=std_window,
                mean_window_days=mean_window,
            )
        candidate_signal_df = signal_frame_cache[signal_key]
        candidate_id = str(params["candidate_id"])
        candidate_daily_df = _simulate_candidate_daily_df_with_family_votes(
            candidate_id=candidate_id,
            candidate_signal_df=candidate_signal_df,
            high_annualized_downside_return_standard_deviation_threshold=high_threshold,
            low_annualized_downside_return_standard_deviation_threshold=low_threshold,
            reduced_exposure_ratio=reduced_exposure_ratio,
            trend_family_rules=resolved_trend_family_rules,
            breadth_family_rules=resolved_breadth_family_rules,
            trend_vote_threshold=trend_vote_threshold,
            breadth_vote_threshold=breadth_vote_threshold,
            confirmation_mode=confirmation_mode,
        )
        candidate_metric_rows.extend(
            _build_candidate_metric_rows(
                candidate_daily_df,
                baseline_daily_df=baseline_daily_df,
                stddev_window_days=std_window,
                mean_window_days=mean_window,
                high_threshold=high_threshold,
                low_threshold=low_threshold,
                reduced_exposure_ratio=reduced_exposure_ratio,
                trend_vote_threshold=trend_vote_threshold,
                breadth_vote_threshold=breadth_vote_threshold,
                confirmation_mode=confirmation_mode,
            )
        )
        candidate_return_series_by_id[candidate_id] = pd.Series(
            candidate_daily_df["strategy_return"].astype(float).to_numpy(),
            index=candidate_daily_df["realized_date"].astype(str),
        )
        discovery_row = candidate_daily_df[candidate_daily_df["sample_split"] == "discovery"]
        discovery_stats = _compute_return_series_stats(discovery_row["strategy_return"])
        discovery_baseline_stats = _compute_return_series_stats(discovery_row["baseline_return"])
        discovery_drawdown_improvement = (
            _coerce_sortable_number(discovery_stats["max_drawdown"])
            - _coerce_sortable_number(discovery_baseline_stats["max_drawdown"])
        )
        candidate_key = (
            _coerce_sortable_number(discovery_stats["sharpe_ratio"]),
            _coerce_sortable_number(discovery_stats["cagr"]),
            _coerce_sortable_number(discovery_drawdown_improvement),
            candidate_id,
        )
        if best_sharpe_key is None or candidate_key > best_sharpe_key:
            best_sharpe_key = candidate_key
            best_sharpe_daily_df = candidate_daily_df.copy()

    candidate_metrics_df = pd.DataFrame(candidate_metric_rows)
    candidate_metrics_df = candidate_metrics_df.sort_values(
        [
            "sample_split",
            "sharpe_ratio_improvement",
            "sharpe_ratio",
            "cagr",
            "candidate_id",
        ],
        ascending=[True, False, False, False, True],
        ignore_index=True,
    )
    candidate_comparison_df = _build_candidate_comparison_df(candidate_metrics_df)
    selection_summary_df = _build_selection_summary_df(
        baseline_metrics_df=baseline_metrics_df,
        candidate_comparison_df=candidate_comparison_df,
    )
    rank_stability_df = _build_rank_stability_df(
        candidate_comparison_df,
        top_ks=resolved_rank_top_ks,
    )
    (
        walkforward_fold_candidate_rank_df,
        walkforward_rank_diagnostics_df,
        walkforward_top1_df,
    ) = _build_walkforward_top1_outputs(
        baseline_daily_df=baseline_daily_df,
        candidate_comparison_df=candidate_comparison_df,
        candidate_return_series_by_id=candidate_return_series_by_id,
        rank_top_ks=resolved_rank_top_ks,
        discovery_window_days=discovery_window_days,
        validation_window_days=validation_window_days,
        step_window_days=step_window_days,
    )
    walkforward_top1_summary_df = _build_walkforward_top1_summary_df(walkforward_top1_df)
    top1_selection_frequency_df = _build_top1_selection_frequency_df(
        walkforward_top1_df=walkforward_top1_df,
        candidate_comparison_df=candidate_comparison_df,
        fold_count=int(walkforward_rank_diagnostics_df["fold_count"].iloc[0]),
    )
    market_frame_df = _annotate_market_frame_with_split_labels(
        market_frame_df,
        signal_base_df=signal_base_df,
    )
    analysis_start_date = (
        str(signal_base_df["realized_date"].iloc[0]) if not signal_base_df.empty else None
    )
    analysis_end_date = (
        str(signal_base_df["realized_date"].iloc[-1]) if not signal_base_df.empty else None
    )

    return TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, source_mode),
        source_detail=str(source_detail),
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        breadth_available_start_date=breadth_available_start_date,
        breadth_available_end_date=breadth_available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        validation_ratio=validation_ratio,
        annualization_factor=ANNUALIZATION_FACTOR,
        execution_timing=EXECUTION_TIMING,
        breadth_universe="topix100",
        min_constituents_per_day=min_constituents_per_day,
        downside_return_standard_deviation_window_days=resolved_std_windows,
        downside_return_standard_deviation_mean_window_days=resolved_mean_windows,
        high_annualized_downside_return_standard_deviation_thresholds=resolved_high_thresholds,
        low_annualized_downside_return_standard_deviation_thresholds=resolved_low_thresholds,
        reduced_exposure_ratios=resolved_reduced_exposures,
        trend_family_rules=resolved_trend_family_rules,
        breadth_family_rules=resolved_breadth_family_rules,
        trend_vote_thresholds=resolved_trend_vote_thresholds,
        breadth_vote_thresholds=resolved_breadth_vote_thresholds,
        confirmation_modes=resolved_confirmation_modes,
        rank_top_ks=resolved_rank_top_ks,
        discovery_window_days=discovery_window_days,
        validation_window_days=validation_window_days,
        step_window_days=step_window_days,
        candidate_count=len(parameter_grid),
        fold_count=int(walkforward_rank_diagnostics_df["fold_count"].iloc[0])
        if not walkforward_rank_diagnostics_df.empty
        else 0,
        topix_daily_df=market_frame_df,
        breadth_daily_df=breadth_daily_df,
        baseline_metrics_df=baseline_metrics_df,
        candidate_metrics_df=candidate_metrics_df,
        candidate_comparison_df=candidate_comparison_df,
        selection_summary_df=selection_summary_df,
        rank_stability_df=rank_stability_df,
        walkforward_fold_candidate_rank_df=walkforward_fold_candidate_rank_df,
        walkforward_rank_diagnostics_df=walkforward_rank_diagnostics_df,
        walkforward_top1_df=walkforward_top1_df,
        walkforward_top1_summary_df=walkforward_top1_summary_df,
        top1_selection_frequency_df=top1_selection_frequency_df,
        best_sharpe_daily_df=best_sharpe_daily_df,
    )


def write_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle(
    result: TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_VOTE_OVERLAY_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "validation_ratio": result.validation_ratio,
            "min_constituents_per_day": result.min_constituents_per_day,
            "downside_return_standard_deviation_window_days": list(
                result.downside_return_standard_deviation_window_days
            ),
            "downside_return_standard_deviation_mean_window_days": list(
                result.downside_return_standard_deviation_mean_window_days
            ),
            "high_annualized_downside_return_standard_deviation_thresholds": list(
                result.high_annualized_downside_return_standard_deviation_thresholds
            ),
            "low_annualized_downside_return_standard_deviation_thresholds": list(
                result.low_annualized_downside_return_standard_deviation_thresholds
            ),
            "reduced_exposure_ratios": list(result.reduced_exposure_ratios),
            "trend_family_rules": list(result.trend_family_rules),
            "breadth_family_rules": list(result.breadth_family_rules),
            "trend_vote_thresholds": list(result.trend_vote_thresholds),
            "breadth_vote_thresholds": list(result.breadth_vote_thresholds),
            "confirmation_modes": list(result.confirmation_modes),
            "rank_top_ks": list(result.rank_top_ks),
            "discovery_window_days": result.discovery_window_days,
            "validation_window_days": result.validation_window_days,
            "step_window_days": result.step_window_days,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle(
    bundle_path: str | Path,
) -> TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_VOTE_OVERLAY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_VOTE_OVERLAY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_candidate_signal_frame_on_common_base(
    market_frame_df: pd.DataFrame,
    *,
    signal_base_df: pd.DataFrame,
    stddev_window_days: int,
    mean_window_days: int,
) -> pd.DataFrame:
    indicator_df = market_frame_df[["date", "close_return"]].copy()
    indicator_df["downside_return_standard_deviation"] = indicator_df["close_return"].rolling(
        stddev_window_days,
        min_periods=stddev_window_days,
    ).apply(_compute_downside_standard_deviation, raw=False)
    indicator_df["annualized_downside_return_standard_deviation"] = (
        indicator_df["downside_return_standard_deviation"] * math.sqrt(ANNUALIZATION_FACTOR)
    )
    indicator_df["annualized_downside_return_standard_deviation_mean"] = indicator_df[
        "annualized_downside_return_standard_deviation"
    ].rolling(
        mean_window_days,
        min_periods=mean_window_days,
    ).mean()
    candidate_signal_df = signal_base_df.merge(
        indicator_df[
            [
                "date",
                "downside_return_standard_deviation",
                "annualized_downside_return_standard_deviation",
                "annualized_downside_return_standard_deviation_mean",
            ]
        ],
        left_on="signal_date",
        right_on="date",
        how="left",
    ).drop(columns=["date"])
    if candidate_signal_df["annualized_downside_return_standard_deviation_mean"].isna().any():
        raise ValueError(
            "Common signal frame reached rows without enough downside return-standard-deviation history"
        )
    return candidate_signal_df


def _simulate_candidate_daily_df_with_family_votes(
    *,
    candidate_id: str,
    candidate_signal_df: pd.DataFrame,
    high_annualized_downside_return_standard_deviation_threshold: float,
    low_annualized_downside_return_standard_deviation_threshold: float,
    reduced_exposure_ratio: float,
    trend_family_rules: Sequence[str],
    breadth_family_rules: Sequence[str],
    trend_vote_threshold: int,
    breadth_vote_threshold: int,
    confirmation_mode: str,
) -> pd.DataFrame:
    active_exposure_ratio = 1.0
    rows: list[dict[str, Any]] = []
    for row in candidate_signal_df.itertuples(index=False):
        indicator_value = _coerce_numeric_value(row.annualized_downside_return_standard_deviation_mean)
        stress_triggered = (
            indicator_value > high_annualized_downside_return_standard_deviation_threshold
        )
        trend_vote_count = _compute_trend_vote_count(row, trend_family_rules=trend_family_rules)
        breadth_vote_count = _compute_breadth_vote_count(
            row,
            breadth_family_rules=breadth_family_rules,
        )
        trend_family_triggered = trend_vote_count >= trend_vote_threshold
        breadth_family_triggered = breadth_vote_count >= breadth_vote_threshold

        next_exposure_ratio = active_exposure_ratio
        signal_state = "inside_band"
        if indicator_value < low_annualized_downside_return_standard_deviation_threshold:
            next_exposure_ratio = 1.0
            signal_state = "below_low_threshold"
        elif active_exposure_ratio < 1.0:
            next_exposure_ratio = active_exposure_ratio
            signal_state = "hold_reduced_until_low"
        elif _confirmation_triggered(
            stress_triggered=stress_triggered,
            trend_family_triggered=trend_family_triggered,
            breadth_family_triggered=breadth_family_triggered,
            confirmation_mode=confirmation_mode,
        ):
            next_exposure_ratio = reduced_exposure_ratio
            signal_state = "risk_off_confirmed"
        elif stress_triggered:
            signal_state = "stress_unconfirmed"
        elif trend_family_triggered or breadth_family_triggered:
            signal_state = "non_stress_confirmation_only"

        realized_overnight_return = _coerce_numeric_value(row.realized_overnight_return)
        realized_intraday_return = _coerce_numeric_value(row.realized_intraday_return)
        realized_close_return = _coerce_numeric_value(row.realized_close_return)
        strategy_return = (
            (1.0 + active_exposure_ratio * realized_overnight_return)
            * (1.0 + next_exposure_ratio * realized_intraday_return)
            - 1.0
        )
        rows.append(
            {
                "candidate_id": candidate_id,
                "signal_date": str(row.signal_date),
                "realized_date": str(row.realized_date),
                "sample_split": str(row.sample_split),
                "signal_downside_return_standard_deviation": _coerce_numeric_value(
                    row.downside_return_standard_deviation
                ),
                "signal_annualized_downside_return_standard_deviation": _coerce_numeric_value(
                    row.annualized_downside_return_standard_deviation
                ),
                "signal_annualized_downside_return_standard_deviation_mean": indicator_value,
                "stress_triggered": stress_triggered,
                "trend_vote_count": trend_vote_count,
                "breadth_vote_count": breadth_vote_count,
                "trend_family_triggered": trend_family_triggered,
                "breadth_family_triggered": breadth_family_triggered,
                "signal_state": signal_state,
                "exposure_ratio_before_rebalance": active_exposure_ratio,
                "target_exposure_ratio": next_exposure_ratio,
                "exposure_change": next_exposure_ratio - active_exposure_ratio,
                "rebalanced": not math.isclose(
                    next_exposure_ratio,
                    active_exposure_ratio,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                ),
                "realized_overnight_return": realized_overnight_return,
                "realized_intraday_return": realized_intraday_return,
                "baseline_return": realized_close_return,
                "strategy_return": float(strategy_return),
                "excess_return": float(strategy_return - realized_close_return),
            }
        )
        active_exposure_ratio = next_exposure_ratio

    candidate_daily_df = pd.DataFrame(rows)
    candidate_daily_df["strategy_equity_curve"] = (
        1.0 + candidate_daily_df["strategy_return"].astype(float)
    ).cumprod()
    candidate_daily_df["baseline_equity_curve"] = (
        1.0 + candidate_daily_df["baseline_return"].astype(float)
    ).cumprod()
    candidate_daily_df["strategy_drawdown"] = _build_drawdown_series(
        candidate_daily_df["strategy_equity_curve"]
    )
    candidate_daily_df["baseline_drawdown"] = _build_drawdown_series(
        candidate_daily_df["baseline_equity_curve"]
    )
    return candidate_daily_df


def _confirmation_triggered(
    *,
    stress_triggered: bool,
    trend_family_triggered: bool,
    breadth_family_triggered: bool,
    confirmation_mode: str,
) -> bool:
    if confirmation_mode == "stress_and_trend_and_breadth":
        return stress_triggered and trend_family_triggered and breadth_family_triggered
    if confirmation_mode == "stress_and_trend_or_breadth":
        return stress_triggered and (trend_family_triggered or breadth_family_triggered)
    if confirmation_mode == "two_of_three_vote":
        return (
            int(stress_triggered)
            + int(trend_family_triggered)
            + int(breadth_family_triggered)
        ) >= 2
    raise ValueError(f"Unsupported confirmation_mode: {confirmation_mode}")


def _compute_trend_vote_count(
    row: Any,
    *,
    trend_family_rules: Sequence[str],
) -> int:
    return sum(1 for rule in trend_family_rules if _evaluate_trend_rule(row, trend_rule=rule))


def _compute_breadth_vote_count(
    row: Any,
    *,
    breadth_family_rules: Sequence[str],
) -> int:
    return sum(
        1 for rule in breadth_family_rules if _evaluate_breadth_rule(row, breadth_rule=rule)
    )


def _evaluate_trend_rule(row: Any, *, trend_rule: str) -> bool:
    if trend_rule == "close_below_sma20":
        return bool(row.topix_close_below_sma20)
    if trend_rule == "sma20_below_sma60":
        return bool(row.topix_sma20_below_sma60)
    if trend_rule == "drawdown_63d_le_neg0p05":
        return _coerce_numeric_value(row.topix_drawdown_63d) <= -0.05
    if trend_rule == "return_10d_le_neg0p03":
        return _coerce_numeric_value(row.topix_return_10d) <= -0.03
    raise ValueError(f"Unsupported trend_rule: {trend_rule}")


def _evaluate_breadth_rule(row: Any, *, breadth_rule: str) -> bool:
    if breadth_rule == "topix100_above_sma20_le_0p40":
        return _coerce_numeric_value(row.breadth_above_sma20_ratio) <= 0.40
    if breadth_rule == "topix100_positive_5d_le_0p40":
        return _coerce_numeric_value(row.breadth_positive_5d_ratio) <= 0.40
    if breadth_rule == "topix100_at_20d_low_ge_0p20":
        return _coerce_numeric_value(row.breadth_at_20d_low_ratio) >= 0.20
    raise ValueError(f"Unsupported breadth_rule: {breadth_rule}")


def _build_baseline_metrics_df(baseline_daily_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sample_split in ("full", "discovery", "validation"):
        split_df = _subset_daily_df_for_split(baseline_daily_df, sample_split)
        rows.append(
            {
                "sample_split": sample_split,
                **_compute_return_series_stats(split_df["strategy_return"]),
                **_compute_activity_stats(split_df),
            }
        )
    return pd.DataFrame(rows)


def _build_candidate_metric_rows(
    candidate_daily_df: pd.DataFrame,
    *,
    baseline_daily_df: pd.DataFrame,
    stddev_window_days: int,
    mean_window_days: int,
    high_threshold: float,
    low_threshold: float,
    reduced_exposure_ratio: float,
    trend_vote_threshold: int,
    breadth_vote_threshold: int,
    confirmation_mode: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidate_id = str(candidate_daily_df["candidate_id"].iloc[0])
    for sample_split in ("full", "discovery", "validation"):
        strategy_split_df = _subset_daily_df_for_split(candidate_daily_df, sample_split)
        baseline_split_df = _subset_daily_df_for_split(baseline_daily_df, sample_split)
        strategy_stats = _compute_return_series_stats(strategy_split_df["strategy_return"])
        baseline_stats = _compute_return_series_stats(baseline_split_df["strategy_return"])
        activity_stats = _compute_activity_stats(strategy_split_df)
        rows.append(
            {
                "candidate_id": candidate_id,
                "sample_split": sample_split,
                "downside_return_standard_deviation_window_days": stddev_window_days,
                "downside_return_standard_deviation_mean_window_days": mean_window_days,
                "high_annualized_downside_return_standard_deviation_threshold": high_threshold,
                "low_annualized_downside_return_standard_deviation_threshold": low_threshold,
                "reduced_exposure_ratio": reduced_exposure_ratio,
                "trend_vote_threshold": trend_vote_threshold,
                "breadth_vote_threshold": breadth_vote_threshold,
                "confirmation_mode": confirmation_mode,
                **strategy_stats,
                **activity_stats,
                "baseline_total_return": baseline_stats["total_return"],
                "baseline_cagr": baseline_stats["cagr"],
                "baseline_sharpe_ratio": baseline_stats["sharpe_ratio"],
                "baseline_sortino_ratio": baseline_stats["sortino_ratio"],
                "baseline_max_drawdown": baseline_stats["max_drawdown"],
                "baseline_calmar_ratio": baseline_stats["calmar_ratio"],
                "total_return_improvement": (
                    strategy_stats["total_return"] - baseline_stats["total_return"]
                ),
                "cagr_improvement": strategy_stats["cagr"] - baseline_stats["cagr"],
                "sharpe_ratio_improvement": (
                    strategy_stats["sharpe_ratio"] - baseline_stats["sharpe_ratio"]
                ),
                "sortino_ratio_improvement": (
                    strategy_stats["sortino_ratio"] - baseline_stats["sortino_ratio"]
                ),
                "max_drawdown_improvement": (
                    strategy_stats["max_drawdown"] - baseline_stats["max_drawdown"]
                ),
                "calmar_ratio_improvement": (
                    strategy_stats["calmar_ratio"] - baseline_stats["calmar_ratio"]
                ),
            }
        )
    return rows


def _build_candidate_comparison_df(candidate_metrics_df: pd.DataFrame) -> pd.DataFrame:
    value_columns = [
        "total_return",
        "cagr",
        "sharpe_ratio",
        "sortino_ratio",
        "max_drawdown",
        "calmar_ratio",
        "average_exposure_ratio",
        "reduced_state_rate",
        "turnover_total",
        "turnover_mean",
        "switch_count",
        "total_return_improvement",
        "cagr_improvement",
        "sharpe_ratio_improvement",
        "sortino_ratio_improvement",
        "max_drawdown_improvement",
        "calmar_ratio_improvement",
    ]
    comparison_df = (
        candidate_metrics_df[
            [
                "candidate_id",
                *_PARAMETER_COLUMNS,
                "sample_split",
                *value_columns,
            ]
        ]
        .pivot_table(
            index=["candidate_id", *_PARAMETER_COLUMNS],
            columns="sample_split",
            values=value_columns,
            aggfunc="first",
        )
        .sort_index(axis=1)
    )
    comparison_df.columns = [
        f"{metric_name}_{sample_split}"
        for metric_name, sample_split in comparison_df.columns.to_flat_index()
    ]
    comparison_df = comparison_df.reset_index()
    return comparison_df.sort_values(
        [
            "sharpe_ratio_discovery",
            "cagr_discovery",
            "max_drawdown_improvement_discovery",
            "candidate_id",
        ],
        ascending=[False, False, False, True],
        ignore_index=True,
    )


def _build_selection_summary_df(
    *,
    baseline_metrics_df: pd.DataFrame,
    candidate_comparison_df: pd.DataFrame,
) -> pd.DataFrame:
    baseline_wide: dict[str, Any] = {"selection_label": "baseline"}
    for row in baseline_metrics_df.itertuples(index=False):
        split = str(row.sample_split)
        baseline_wide[f"cagr_{split}"] = row.cagr
        baseline_wide[f"sharpe_ratio_{split}"] = row.sharpe_ratio
        baseline_wide[f"sortino_ratio_{split}"] = row.sortino_ratio
        baseline_wide[f"max_drawdown_{split}"] = row.max_drawdown
        baseline_wide[f"calmar_ratio_{split}"] = row.calmar_ratio
    rows: list[dict[str, Any]] = [baseline_wide]

    selection_specs = (
        ("best_discovery_sharpe", "sharpe_ratio_discovery"),
        ("best_discovery_cagr", "cagr_discovery"),
        ("best_discovery_max_drawdown_improvement", "max_drawdown_improvement_discovery"),
    )
    for selection_label, sort_column in selection_specs:
        selected_row = _select_best_candidate_row(
            candidate_comparison_df,
            sort_column=sort_column,
        )
        if selected_row is None:
            continue
        payload = selected_row.to_dict()
        payload["selection_label"] = selection_label
        rows.append(payload)
    return pd.DataFrame(rows)


def _select_best_candidate_row(
    candidate_comparison_df: pd.DataFrame,
    *,
    sort_column: str,
) -> pd.Series | None:
    if candidate_comparison_df.empty or sort_column not in candidate_comparison_df.columns:
        return None
    ordered_df = candidate_comparison_df.sort_values(
        [sort_column, "cagr_discovery", "candidate_id"],
        ascending=[False, False, True],
        ignore_index=True,
    )
    return ordered_df.iloc[0] if not ordered_df.empty else None


def _build_rank_stability_df(
    candidate_comparison_df: pd.DataFrame,
    *,
    top_ks: Sequence[int],
) -> pd.DataFrame:
    sharpe_discovery = candidate_comparison_df["sharpe_ratio_discovery"].astype(float)
    sharpe_validation = candidate_comparison_df["sharpe_ratio_validation"].astype(float)
    cagr_discovery = candidate_comparison_df["cagr_discovery"].astype(float)
    cagr_validation = candidate_comparison_df["cagr_validation"].astype(float)
    row: dict[str, Any] = {
        "candidate_count": int(len(candidate_comparison_df)),
        "pearson_sharpe": float(sharpe_discovery.corr(sharpe_validation, method="pearson")),
        "spearman_sharpe": float(
            sharpe_discovery.rank(method="average").corr(
                sharpe_validation.rank(method="average"),
                method="pearson",
            )
        ),
        "pearson_cagr": float(cagr_discovery.corr(cagr_validation, method="pearson")),
        "spearman_cagr": float(
            cagr_discovery.rank(method="average").corr(
                cagr_validation.rank(method="average"),
                method="pearson",
            )
        ),
    }
    for top_k in top_ks:
        top_discovery = set(
            candidate_comparison_df.nlargest(
                int(top_k),
                ["sharpe_ratio_discovery", "cagr_discovery"],
            )["candidate_id"].astype(str)
        )
        top_validation = set(
            candidate_comparison_df.nlargest(
                int(top_k),
                ["sharpe_ratio_validation", "cagr_validation"],
            )["candidate_id"].astype(str)
        )
        overlap_count = len(top_discovery & top_validation)
        row[f"top{int(top_k)}_overlap_count"] = overlap_count
        row[f"top{int(top_k)}_overlap_ratio"] = float(overlap_count / top_k)
    return pd.DataFrame([row])


def _build_walkforward_top1_outputs(
    *,
    baseline_daily_df: pd.DataFrame,
    candidate_comparison_df: pd.DataFrame,
    candidate_return_series_by_id: dict[str, pd.Series],
    rank_top_ks: Sequence[int],
    discovery_window_days: int,
    validation_window_days: int,
    step_window_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not candidate_return_series_by_id:
        raise ValueError("No candidate return series were available")

    ordered_candidates = candidate_comparison_df["candidate_id"].astype(str).tolist()
    base_dates = pd.to_datetime(baseline_daily_df["realized_date"]).reset_index(drop=True)
    if len(base_dates) < discovery_window_days + validation_window_days:
        raise ValueError("No walk-forward splits were generated for the selected overlay window")

    common_dates = baseline_daily_df["realized_date"].astype(str).reset_index(drop=True)
    candidate_return_panel_df = pd.concat(
        [
            pd.DataFrame({"realized_date": common_dates}),
            pd.DataFrame(
                {
                    candidate_id: candidate_return_series_by_id[candidate_id]
                    .reindex(common_dates)
                    .to_numpy()
                    for candidate_id in ordered_candidates
                }
            ),
        ],
        axis=1,
    )
    baseline_returns = baseline_daily_df["strategy_return"].astype(float).reset_index(drop=True)
    parameter_lookup = {
        str(row["candidate_id"]): {column: row[column] for column in _PARAMETER_COLUMNS}
        for _, row in candidate_comparison_df.iterrows()
    }

    fold_candidate_rows: list[dict[str, Any]] = []
    top1_rows: list[dict[str, Any]] = []
    fold_spearman_sharpe_values: list[float] = []
    overlap_ratios_by_top_k: dict[int, list[float]] = {int(top_k): [] for top_k in rank_top_ks}

    fold_index = 0
    for start_index in range(
        0,
        len(base_dates) - discovery_window_days - validation_window_days + 1,
        step_window_days,
    ):
        discovery_slice = slice(start_index, start_index + discovery_window_days)
        validation_slice = slice(
            start_index + discovery_window_days,
            start_index + discovery_window_days + validation_window_days,
        )
        candidate_fold_df = _build_fold_candidate_rank_df(
            candidate_return_panel_df=candidate_return_panel_df,
            parameter_lookup=parameter_lookup,
            ordered_candidates=ordered_candidates,
            fold_index=fold_index,
            discovery_slice=discovery_slice,
            validation_slice=validation_slice,
            base_dates=base_dates,
        )
        if candidate_fold_df.empty:
            continue
        fold_candidate_rows.extend(
            cast(list[dict[str, Any]], candidate_fold_df.to_dict(orient="records"))
        )
        fold_spearman_sharpe_values.append(
            float(
                candidate_fold_df["discovery_rank_by_sharpe"].corr(
                    candidate_fold_df["validation_rank_by_sharpe"],
                    method="pearson",
                )
            )
        )
        for top_k in rank_top_ks:
            top_discovery = set(
                candidate_fold_df.nsmallest(int(top_k), "discovery_rank_by_sharpe")[
                    "candidate_id"
                ].astype(str)
            )
            top_validation = set(
                candidate_fold_df.nsmallest(int(top_k), "validation_rank_by_sharpe")[
                    "candidate_id"
                ].astype(str)
            )
            overlap_ratios_by_top_k[int(top_k)].append(len(top_discovery & top_validation) / top_k)

        baseline_discovery_stats = _compute_return_series_stats(
            baseline_returns.iloc[discovery_slice]
        )
        baseline_validation_stats = _compute_return_series_stats(
            baseline_returns.iloc[validation_slice]
        )
        top_row = candidate_fold_df.nsmallest(1, "discovery_rank_by_sharpe").iloc[0]
        selected_candidate_id = str(top_row["candidate_id"])
        selected_discovery_returns = candidate_return_panel_df[selected_candidate_id].iloc[
            discovery_slice
        ]
        selected_validation_returns = candidate_return_panel_df[selected_candidate_id].iloc[
            validation_slice
        ]
        selected_discovery_stats = _compute_return_series_stats(selected_discovery_returns)
        selected_validation_stats = _compute_return_series_stats(selected_validation_returns)
        top1_rows.append(
            {
                "fold_index": fold_index,
                "discovery_start_date": base_dates.iloc[discovery_slice.start].strftime(
                    "%Y-%m-%d"
                ),
                "discovery_end_date": base_dates.iloc[discovery_slice.stop - 1].strftime(
                    "%Y-%m-%d"
                ),
                "validation_start_date": base_dates.iloc[validation_slice.start].strftime(
                    "%Y-%m-%d"
                ),
                "validation_end_date": base_dates.iloc[validation_slice.stop - 1].strftime(
                    "%Y-%m-%d"
                ),
                "candidate_id": selected_candidate_id,
                **parameter_lookup[selected_candidate_id],
                "discovery_rank_by_sharpe": int(top_row["discovery_rank_by_sharpe"]),
                "validation_rank_by_sharpe": int(top_row["validation_rank_by_sharpe"]),
                "candidate_discovery_sharpe_ratio": selected_discovery_stats["sharpe_ratio"],
                "candidate_discovery_cagr": selected_discovery_stats["cagr"],
                "candidate_validation_sharpe_ratio": selected_validation_stats["sharpe_ratio"],
                "candidate_validation_cagr": selected_validation_stats["cagr"],
                "candidate_validation_max_drawdown": selected_validation_stats["max_drawdown"],
                "baseline_discovery_sharpe_ratio": baseline_discovery_stats["sharpe_ratio"],
                "baseline_discovery_cagr": baseline_discovery_stats["cagr"],
                "baseline_validation_sharpe_ratio": baseline_validation_stats["sharpe_ratio"],
                "baseline_validation_cagr": baseline_validation_stats["cagr"],
                "baseline_validation_max_drawdown": baseline_validation_stats["max_drawdown"],
                "validation_sharpe_ratio_excess": (
                    selected_validation_stats["sharpe_ratio"]
                    - baseline_validation_stats["sharpe_ratio"]
                ),
                "validation_cagr_excess": (
                    selected_validation_stats["cagr"] - baseline_validation_stats["cagr"]
                ),
                "validation_max_drawdown_improvement": (
                    selected_validation_stats["max_drawdown"]
                    - baseline_validation_stats["max_drawdown"]
                ),
                "validation_sharpe_win": (
                    selected_validation_stats["sharpe_ratio"]
                    > baseline_validation_stats["sharpe_ratio"]
                ),
                "validation_cagr_win": (
                    selected_validation_stats["cagr"] > baseline_validation_stats["cagr"]
                ),
            }
        )
        fold_index += 1

    if fold_index == 0:
        raise ValueError("No walk-forward splits were generated for the selected overlay window")

    fold_candidate_rank_df = pd.DataFrame(fold_candidate_rows)
    walkforward_top1_df = pd.DataFrame(top1_rows)
    diagnostic_row: dict[str, Any] = {
        "fold_count": int(fold_index),
        "avg_fold_spearman_sharpe": float(pd.Series(fold_spearman_sharpe_values).mean()),
        "median_fold_spearman_sharpe": float(pd.Series(fold_spearman_sharpe_values).median()),
    }
    for top_k in rank_top_ks:
        values = overlap_ratios_by_top_k[int(top_k)]
        diagnostic_row[f"avg_top{int(top_k)}_overlap_ratio"] = float(pd.Series(values).mean())
        diagnostic_row[f"median_top{int(top_k)}_overlap_ratio"] = float(
            pd.Series(values).median()
        )
    diagnostics_df = pd.DataFrame([diagnostic_row])
    return fold_candidate_rank_df, diagnostics_df, walkforward_top1_df


def _build_fold_candidate_rank_df(
    *,
    candidate_return_panel_df: pd.DataFrame,
    parameter_lookup: dict[str, dict[str, Any]],
    ordered_candidates: Sequence[str],
    fold_index: int,
    discovery_slice: slice,
    validation_slice: slice,
    base_dates: pd.Series,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate_id in ordered_candidates:
        discovery_returns = candidate_return_panel_df[candidate_id].iloc[discovery_slice]
        validation_returns = candidate_return_panel_df[candidate_id].iloc[validation_slice]
        discovery_stats = _compute_return_series_stats(discovery_returns)
        validation_stats = _compute_return_series_stats(validation_returns)
        rows.append(
            {
                "fold_index": fold_index,
                "discovery_start_date": base_dates.iloc[discovery_slice.start].strftime(
                    "%Y-%m-%d"
                ),
                "discovery_end_date": base_dates.iloc[discovery_slice.stop - 1].strftime(
                    "%Y-%m-%d"
                ),
                "validation_start_date": base_dates.iloc[validation_slice.start].strftime(
                    "%Y-%m-%d"
                ),
                "validation_end_date": base_dates.iloc[validation_slice.stop - 1].strftime(
                    "%Y-%m-%d"
                ),
                "candidate_id": candidate_id,
                **parameter_lookup[candidate_id],
                "discovery_sharpe_ratio": discovery_stats["sharpe_ratio"],
                "discovery_cagr": discovery_stats["cagr"],
                "discovery_max_drawdown": discovery_stats["max_drawdown"],
                "validation_sharpe_ratio": validation_stats["sharpe_ratio"],
                "validation_cagr": validation_stats["cagr"],
                "validation_max_drawdown": validation_stats["max_drawdown"],
            }
        )
    fold_candidate_df = pd.DataFrame(rows)
    fold_candidate_df = fold_candidate_df.sort_values(
        ["discovery_sharpe_ratio", "discovery_cagr", "candidate_id"],
        ascending=[False, False, True],
        ignore_index=True,
    )
    fold_candidate_df["discovery_rank_by_sharpe"] = range(1, len(fold_candidate_df) + 1)
    validation_rank_df = fold_candidate_df.sort_values(
        ["validation_sharpe_ratio", "validation_cagr", "candidate_id"],
        ascending=[False, False, True],
        ignore_index=True,
    )
    validation_rank_lookup = {
        str(candidate_id): rank
        for rank, candidate_id in enumerate(
            validation_rank_df["candidate_id"].astype(str),
            start=1,
        )
    }
    fold_candidate_df["validation_rank_by_sharpe"] = (
        fold_candidate_df["candidate_id"].astype(str).map(validation_rank_lookup).astype(int)
    )
    return fold_candidate_df


def _build_walkforward_top1_summary_df(walkforward_top1_df: pd.DataFrame) -> pd.DataFrame:
    if walkforward_top1_df.empty:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "fold_count": int(len(walkforward_top1_df)),
                "validation_sharpe_win_rate": float(
                    walkforward_top1_df["validation_sharpe_win"].astype(float).mean()
                ),
                "validation_cagr_win_rate": float(
                    walkforward_top1_df["validation_cagr_win"].astype(float).mean()
                ),
                "avg_validation_sharpe_ratio_excess": float(
                    walkforward_top1_df["validation_sharpe_ratio_excess"].astype(float).mean()
                ),
                "median_validation_sharpe_ratio_excess": float(
                    walkforward_top1_df["validation_sharpe_ratio_excess"].astype(float).median()
                ),
                "avg_validation_cagr_excess": float(
                    walkforward_top1_df["validation_cagr_excess"].astype(float).mean()
                ),
                "median_validation_cagr_excess": float(
                    walkforward_top1_df["validation_cagr_excess"].astype(float).median()
                ),
                "avg_validation_max_drawdown_improvement": float(
                    walkforward_top1_df["validation_max_drawdown_improvement"]
                    .astype(float)
                    .mean()
                ),
                "median_validation_max_drawdown_improvement": float(
                    walkforward_top1_df["validation_max_drawdown_improvement"]
                    .astype(float)
                    .median()
                ),
                "avg_candidate_validation_sharpe_ratio": float(
                    walkforward_top1_df["candidate_validation_sharpe_ratio"]
                    .astype(float)
                    .mean()
                ),
                "avg_baseline_validation_sharpe_ratio": float(
                    walkforward_top1_df["baseline_validation_sharpe_ratio"]
                    .astype(float)
                    .mean()
                ),
            }
        ]
    )


def _build_top1_selection_frequency_df(
    *,
    walkforward_top1_df: pd.DataFrame,
    candidate_comparison_df: pd.DataFrame,
    fold_count: int,
) -> pd.DataFrame:
    if walkforward_top1_df.empty:
        return pd.DataFrame()
    summary_df = (
        walkforward_top1_df.groupby("candidate_id", as_index=False)
        .agg(
            selection_count=("fold_index", "count"),
            mean_validation_rank=("validation_rank_by_sharpe", "mean"),
        )
        .sort_values(
            ["selection_count", "mean_validation_rank", "candidate_id"],
            ascending=[False, True, True],
            ignore_index=True,
        )
    )
    summary_df["selection_rate"] = summary_df["selection_count"].div(max(fold_count, 1))
    return summary_df.merge(
        candidate_comparison_df[
            [
                "candidate_id",
                *_PARAMETER_COLUMNS,
                "sharpe_ratio_discovery",
                "sharpe_ratio_validation",
                "cagr_validation",
                "max_drawdown_validation",
            ]
        ],
        on="candidate_id",
        how="left",
    )


def _build_research_bundle_summary_markdown(
    result: TopixDownsideReturnStandardDeviationShockConfirmationVoteOverlayResearchResult,
) -> str:
    baseline_validation = _lookup_split_row(result.baseline_metrics_df, "validation")
    best_sharpe_row = _lookup_selection_row(
        result.selection_summary_df,
        "best_discovery_sharpe",
    )
    walkforward_row = (
        result.walkforward_rank_diagnostics_df.iloc[0]
        if not result.walkforward_rank_diagnostics_df.empty
        else None
    )
    top1_summary_row = (
        result.walkforward_top1_summary_df.iloc[0]
        if not result.walkforward_top1_summary_df.empty
        else None
    )
    lines = [
        "# TOPIX Shock Confirmation Vote Overlay",
        "",
        "## Scope",
        "",
        "This bundle fixes one downside return standard deviation stress family plus one TOPIX trend family and one TOPIX100 breadth family, then compares family-level confirmation logic instead of individual exact rules.",
        "",
        "## Fixed Families",
        "",
        f"- Stress windows: `{_format_int_sequence(result.downside_return_standard_deviation_window_days)}`",
        f"- Stress means: `{_format_int_sequence(result.downside_return_standard_deviation_mean_window_days)}`",
        f"- Stress high thresholds: `{_format_float_sequence(result.high_annualized_downside_return_standard_deviation_thresholds)}`",
        f"- Stress low thresholds: `{_format_float_sequence(result.low_annualized_downside_return_standard_deviation_thresholds)}`",
        f"- Reduced exposure ratios: `{_format_float_sequence(result.reduced_exposure_ratios)}`",
        f"- Trend family rules: `{', '.join(result.trend_family_rules)}`",
        f"- Breadth family rules: `{', '.join(result.breadth_family_rules)}`",
        f"- Trend vote thresholds: `{_format_int_sequence(result.trend_vote_thresholds)}`",
        f"- Breadth vote thresholds: `{_format_int_sequence(result.breadth_vote_thresholds)}`",
        f"- Confirmation modes: `{', '.join(result.confirmation_modes)}`",
        f"- Candidate count: `{result.candidate_count}`",
        "",
        "## Baseline",
        "",
    ]
    if baseline_validation is not None:
        lines.extend(
            [
                f"- Validation CAGR: `{_format_percent(float(baseline_validation['cagr']))}`",
                f"- Validation Sharpe: `{_format_ratio(float(baseline_validation['sharpe_ratio']))}`",
                f"- Validation Sortino: `{_format_ratio(float(baseline_validation['sortino_ratio']))}`",
                f"- Validation max drawdown: `{_format_percent(float(baseline_validation['max_drawdown']))}`",
            ]
        )
    if best_sharpe_row is not None:
        lines.extend(
            [
                "",
                "## Best Discovery Sharpe",
                "",
                f"- Candidate: `{best_sharpe_row['candidate_id']}`",
                f"- Params: `std={int(best_sharpe_row['downside_return_standard_deviation_window_days'])}`, `mean={int(best_sharpe_row['downside_return_standard_deviation_mean_window_days'])}`, `high={float(best_sharpe_row['high_annualized_downside_return_standard_deviation_threshold']):.2f}`, `low={float(best_sharpe_row['low_annualized_downside_return_standard_deviation_threshold']):.2f}`, `reduced={float(best_sharpe_row['reduced_exposure_ratio']):.2f}`, `trend_votes>={int(best_sharpe_row['trend_vote_threshold'])}`, `breadth_votes>={int(best_sharpe_row['breadth_vote_threshold'])}`, `mode={best_sharpe_row['confirmation_mode']}`",
                f"- Discovery Sharpe: `{_format_ratio(float(best_sharpe_row['sharpe_ratio_discovery']))}`",
                f"- Validation Sharpe: `{_format_ratio(float(best_sharpe_row['sharpe_ratio_validation']))}`",
                f"- Validation CAGR: `{_format_percent(float(best_sharpe_row['cagr_validation']))}`",
                f"- Validation max drawdown: `{_format_percent(float(best_sharpe_row['max_drawdown_validation']))}`",
            ]
        )
    lines.extend(["", "## Walk-Forward", ""])
    if walkforward_row is not None:
        lines.extend(
            [
                f"- Fold count: `{int(walkforward_row['fold_count'])}`",
                f"- Mean fold Spearman Sharpe: `{_format_ratio(float(walkforward_row['avg_fold_spearman_sharpe']))}`",
                f"- Median fold Spearman Sharpe: `{_format_ratio(float(walkforward_row['median_fold_spearman_sharpe']))}`",
            ]
        )
        for top_k in result.rank_top_ks:
            lines.append(
                f"- Mean top {int(top_k)} overlap ratio: `{_format_ratio(float(walkforward_row[f'avg_top{int(top_k)}_overlap_ratio']))}`"
            )
    if top1_summary_row is not None:
        lines.extend(
            [
                "",
                "## Walk-Forward Top1 Summary",
                "",
                f"- Validation Sharpe win rate: `{_format_ratio(float(top1_summary_row['validation_sharpe_win_rate']))}`",
                f"- Mean validation Sharpe excess: `{_format_ratio(float(top1_summary_row['avg_validation_sharpe_ratio_excess']))}`",
                f"- Mean validation CAGR excess: `{_format_percent(float(top1_summary_row['avg_validation_cagr_excess']))}`",
                f"- Mean validation max drawdown improvement: `{_format_percent(float(top1_summary_row['avg_validation_max_drawdown_improvement']))}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This study compares family-level vote logic, not individual trend-rule and breadth-rule picks.",
            "- The strongest mode can be useful even when exact candidate ranking stays unstable across folds.",
            "- The breadth universe is the practical TOPIX100 proxy used in the current market DB.",
        ]
    )
    return "\n".join(lines)


def _build_candidate_id(
    *,
    stddev_window_days: int,
    mean_window_days: int,
    high_threshold: float,
    low_threshold: float,
    reduced_exposure_ratio: float,
    trend_vote_threshold: int,
    breadth_vote_threshold: int,
    confirmation_mode: str,
) -> str:
    return (
        f"std{stddev_window_days}_mean{mean_window_days}_"
        f"hi{_format_ratio_token(high_threshold)}_"
        f"lo{_format_ratio_token(low_threshold)}_"
        f"reduced{_format_ratio_token(reduced_exposure_ratio)}_"
        f"trendvotes{trend_vote_threshold}_"
        f"breadthvotes{breadth_vote_threshold}_"
        f"mode_{confirmation_mode}"
    )


def _format_ratio_token(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".").replace(".", "p")


def _normalize_confirmation_modes(values: Sequence[str] | None) -> tuple[str, ...]:
    raw_values = tuple(
        DEFAULT_CONFIRMATION_MODES
        if values is None
        else tuple(str(value).strip() for value in values)
    )
    if not raw_values:
        raise ValueError("confirmation_modes must not be empty")
    if any(not value for value in raw_values):
        raise ValueError("confirmation_modes must not contain blank values")
    invalid_values = sorted(set(raw_values) - _VALID_CONFIRMATION_MODES)
    if invalid_values:
        raise ValueError(
            f"confirmation_modes contains unsupported values: {', '.join(invalid_values)}"
        )
    return tuple(dict.fromkeys(raw_values))
