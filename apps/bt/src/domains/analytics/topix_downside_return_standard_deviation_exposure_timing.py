"""
TOPIX downside return-standard-deviation exposure timing research.

This study asks whether scaling a TOPIX long book down when downside return
standard deviation expands can improve risk-adjusted performance relative to a
constant 100% long baseline.

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
    _build_sample_split_labels,
    _normalize_positive_int_sequence,
    _query_topix_daily_frame,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _fetch_date_range,
    _open_analysis_connection,
)

ANNUALIZATION_FACTOR = 252.0
DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS: tuple[int, ...] = (5, 10, 20, 40)
DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS: tuple[int, ...] = (1, 3, 5)
DEFAULT_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS: tuple[float, ...] = (
    0.10,
    0.15,
    0.20,
    0.25,
)
DEFAULT_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS: tuple[float, ...] = (
    0.05,
    0.10,
    0.15,
    0.20,
)
DEFAULT_REDUCED_EXPOSURE_RATIOS: tuple[float, ...] = (0.0, 0.25, 0.50, 0.75)
EXECUTION_TIMING = "signal_at_close_rebalance_next_open"
TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_EXPOSURE_TIMING_EXPERIMENT_ID = (
    "market-behavior/topix-downside-return-standard-deviation-exposure-timing"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "topix_daily_df",
    "baseline_metrics_df",
    "candidate_metrics_df",
    "candidate_comparison_df",
    "selection_summary_df",
    "best_sharpe_daily_df",
)


@dataclass(frozen=True)
class TopixDownsideReturnStandardDeviationExposureTimingResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    common_signal_start_date: str | None
    common_signal_end_date: str | None
    validation_ratio: float
    annualization_factor: float
    execution_timing: str
    downside_return_standard_deviation_window_days: tuple[int, ...]
    downside_return_standard_deviation_mean_window_days: tuple[int, ...]
    high_annualized_downside_return_standard_deviation_thresholds: tuple[float, ...]
    low_annualized_downside_return_standard_deviation_thresholds: tuple[float, ...]
    reduced_exposure_ratios: tuple[float, ...]
    candidate_count: int
    topix_daily_df: pd.DataFrame
    baseline_metrics_df: pd.DataFrame
    candidate_metrics_df: pd.DataFrame
    candidate_comparison_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    best_sharpe_daily_df: pd.DataFrame


def get_topix_downside_return_standard_deviation_exposure_timing_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def run_topix_downside_return_standard_deviation_exposure_timing_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    downside_return_standard_deviation_window_days: Sequence[int] | None = None,
    downside_return_standard_deviation_mean_window_days: Sequence[int] | None = None,
    high_annualized_downside_return_standard_deviation_thresholds: Sequence[float] | None = None,
    low_annualized_downside_return_standard_deviation_thresholds: Sequence[float] | None = None,
    reduced_exposure_ratios: Sequence[float] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
) -> TopixDownsideReturnStandardDeviationExposureTimingResearchResult:
    resolved_stddev_windows = _normalize_positive_int_sequence(
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
    resolved_reduced_exposure_ratios = _normalize_exposure_ratio_sequence(
        reduced_exposure_ratios,
        default=DEFAULT_REDUCED_EXPOSURE_RATIOS,
        name="reduced_exposure_ratios",
    )
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")

    parameter_grid = [
        (
            stddev_window_days,
            mean_window_days,
            high_threshold,
            low_threshold,
            reduced_exposure_ratio,
        )
        for stddev_window_days, mean_window_days, high_threshold, low_threshold, reduced_exposure_ratio in product(
            resolved_stddev_windows,
            resolved_mean_windows,
            resolved_high_thresholds,
            resolved_low_thresholds,
            resolved_reduced_exposure_ratios,
        )
        if low_threshold <= high_threshold
    ]
    if not parameter_grid:
        raise ValueError("No valid parameter combinations remained after low/high threshold filtering")

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

    market_frame_df = _prepare_topix_market_frame(topix_daily_df)
    signal_base_df = _build_common_signal_frame(
        market_frame_df,
        max_downside_return_standard_deviation_window_days=max(resolved_stddev_windows),
        max_downside_return_standard_deviation_mean_window_days=max(resolved_mean_windows),
        validation_ratio=validation_ratio,
    )
    baseline_daily_df = _build_baseline_daily_df(signal_base_df)
    baseline_metrics_df = _build_baseline_metrics_df(baseline_daily_df)

    best_sharpe_daily_df = pd.DataFrame()
    best_sharpe_key: tuple[float, float, float, str] | None = None
    candidate_metric_rows: list[dict[str, Any]] = []
    signal_frame_cache: dict[tuple[int, int], pd.DataFrame] = {}

    for (
        stddev_window_days,
        mean_window_days,
        high_threshold,
        low_threshold,
        reduced_exposure_ratio,
    ) in parameter_grid:
        signal_key = (stddev_window_days, mean_window_days)
        if signal_key not in signal_frame_cache:
            signal_frame_cache[signal_key] = _build_candidate_signal_frame(
                market_frame_df,
                signal_base_df=signal_base_df,
                downside_return_standard_deviation_window_days=stddev_window_days,
                downside_return_standard_deviation_mean_window_days=mean_window_days,
            )
        candidate_signal_df = signal_frame_cache[signal_key]
        candidate_id = _build_candidate_id(
            stddev_window_days=stddev_window_days,
            mean_window_days=mean_window_days,
            high_threshold=high_threshold,
            low_threshold=low_threshold,
            reduced_exposure_ratio=reduced_exposure_ratio,
        )
        candidate_daily_df = _simulate_candidate_daily_df(
            candidate_id=candidate_id,
            candidate_signal_df=candidate_signal_df,
            high_annualized_downside_return_standard_deviation_threshold=high_threshold,
            low_annualized_downside_return_standard_deviation_threshold=low_threshold,
            reduced_exposure_ratio=reduced_exposure_ratio,
        )
        candidate_metric_rows.extend(
            _build_candidate_metric_rows(
                candidate_daily_df,
                baseline_daily_df=baseline_daily_df,
                stddev_window_days=stddev_window_days,
                mean_window_days=mean_window_days,
                high_threshold=high_threshold,
                low_threshold=low_threshold,
                reduced_exposure_ratio=reduced_exposure_ratio,
            )
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
    common_signal_start_date = (
        str(signal_base_df["signal_date"].iloc[0]) if not signal_base_df.empty else None
    )
    common_signal_end_date = (
        str(signal_base_df["signal_date"].iloc[-1]) if not signal_base_df.empty else None
    )

    return TopixDownsideReturnStandardDeviationExposureTimingResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, source_mode),
        source_detail=str(source_detail),
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        common_signal_start_date=common_signal_start_date,
        common_signal_end_date=common_signal_end_date,
        validation_ratio=validation_ratio,
        annualization_factor=ANNUALIZATION_FACTOR,
        execution_timing=EXECUTION_TIMING,
        downside_return_standard_deviation_window_days=resolved_stddev_windows,
        downside_return_standard_deviation_mean_window_days=resolved_mean_windows,
        high_annualized_downside_return_standard_deviation_thresholds=resolved_high_thresholds,
        low_annualized_downside_return_standard_deviation_thresholds=resolved_low_thresholds,
        reduced_exposure_ratios=resolved_reduced_exposure_ratios,
        candidate_count=len(candidate_comparison_df),
        topix_daily_df=market_frame_df,
        baseline_metrics_df=baseline_metrics_df,
        candidate_metrics_df=candidate_metrics_df,
        candidate_comparison_df=candidate_comparison_df,
        selection_summary_df=selection_summary_df,
        best_sharpe_daily_df=best_sharpe_daily_df,
    )


def write_topix_downside_return_standard_deviation_exposure_timing_research_bundle(
    result: TopixDownsideReturnStandardDeviationExposureTimingResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_EXPOSURE_TIMING_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_downside_return_standard_deviation_exposure_timing_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "validation_ratio": result.validation_ratio,
            "downside_return_standard_deviation_window_days": list(result.downside_return_standard_deviation_window_days),
            "downside_return_standard_deviation_mean_window_days": list(result.downside_return_standard_deviation_mean_window_days),
            "high_annualized_downside_return_standard_deviation_thresholds": list(
                result.high_annualized_downside_return_standard_deviation_thresholds
            ),
            "low_annualized_downside_return_standard_deviation_thresholds": list(
                result.low_annualized_downside_return_standard_deviation_thresholds
            ),
            "reduced_exposure_ratios": list(result.reduced_exposure_ratios),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix_downside_return_standard_deviation_exposure_timing_research_bundle(
    bundle_path: str | Path,
) -> TopixDownsideReturnStandardDeviationExposureTimingResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=TopixDownsideReturnStandardDeviationExposureTimingResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix_downside_return_standard_deviation_exposure_timing_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_EXPOSURE_TIMING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_downside_return_standard_deviation_exposure_timing_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_EXPOSURE_TIMING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _prepare_topix_market_frame(topix_daily_df: pd.DataFrame) -> pd.DataFrame:
    market_frame_df = topix_daily_df.copy().reset_index(drop=True)
    market_frame_df["overnight_return"] = market_frame_df["open"].div(
        market_frame_df["close"].shift(1)
    ).sub(1.0)
    market_frame_df["intraday_return"] = market_frame_df["close"].div(
        market_frame_df["open"]
    ).sub(1.0)
    market_frame_df["signal_analysis_eligible"] = False
    market_frame_df["signal_sample_split"] = "excluded"
    market_frame_df["realized_analysis_eligible"] = False
    market_frame_df["realized_sample_split"] = "excluded"
    return market_frame_df


def _build_common_signal_frame(
    market_frame_df: pd.DataFrame,
    *,
    max_downside_return_standard_deviation_window_days: int,
    max_downside_return_standard_deviation_mean_window_days: int,
    validation_ratio: float,
) -> pd.DataFrame:
    signal_start_index = (
        max_downside_return_standard_deviation_window_days
        + max_downside_return_standard_deviation_mean_window_days
        - 1
    )
    signal_stop_index = len(market_frame_df) - 1
    if signal_start_index >= signal_stop_index:
        raise ValueError(
            "Not enough TOPIX rows for the requested downside return standard deviation windows: "
            f"need more than {signal_start_index + 1}, got {len(market_frame_df)}"
        )

    signal_df = (
        market_frame_df.iloc[signal_start_index:signal_stop_index]
        .copy()
        .reset_index(drop=True)
    )
    signal_df["signal_date"] = signal_df["date"]
    signal_df["realized_date"] = market_frame_df["date"].shift(-1).iloc[
        signal_start_index:signal_stop_index
    ].to_numpy()
    signal_df["realized_close_return"] = market_frame_df["close_return"].shift(-1).iloc[
        signal_start_index:signal_stop_index
    ].to_numpy()
    signal_df["realized_overnight_return"] = market_frame_df["overnight_return"].shift(-1).iloc[
        signal_start_index:signal_stop_index
    ].to_numpy()
    signal_df["realized_intraday_return"] = market_frame_df["intraday_return"].shift(-1).iloc[
        signal_start_index:signal_stop_index
    ].to_numpy()
    signal_df["sample_split"] = _build_sample_split_labels(
        len(signal_df),
        validation_ratio=validation_ratio,
    )
    return signal_df[
        [
            "signal_date",
            "realized_date",
            "sample_split",
            "realized_close_return",
            "realized_overnight_return",
            "realized_intraday_return",
        ]
    ].copy()


def _build_candidate_signal_frame(
    market_frame_df: pd.DataFrame,
    *,
    signal_base_df: pd.DataFrame,
    downside_return_standard_deviation_window_days: int,
    downside_return_standard_deviation_mean_window_days: int,
) -> pd.DataFrame:
    indicator_df = market_frame_df[["date", "close_return"]].copy()
    indicator_df["downside_return_standard_deviation"] = indicator_df["close_return"].rolling(
        downside_return_standard_deviation_window_days,
        min_periods=downside_return_standard_deviation_window_days,
    ).apply(_compute_downside_standard_deviation, raw=False)
    indicator_df["annualized_downside_return_standard_deviation"] = (
        indicator_df["downside_return_standard_deviation"] * math.sqrt(ANNUALIZATION_FACTOR)
    )
    indicator_df["annualized_downside_return_standard_deviation_mean"] = indicator_df[
        "annualized_downside_return_standard_deviation"
    ].rolling(
        downside_return_standard_deviation_mean_window_days,
        min_periods=downside_return_standard_deviation_mean_window_days,
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


def _build_baseline_daily_df(signal_base_df: pd.DataFrame) -> pd.DataFrame:
    baseline_daily_df = signal_base_df.copy()
    baseline_daily_df["candidate_id"] = "baseline"
    baseline_daily_df["signal_state"] = "baseline"
    baseline_daily_df["exposure_ratio_before_rebalance"] = 1.0
    baseline_daily_df["target_exposure_ratio"] = 1.0
    baseline_daily_df["exposure_change"] = 0.0
    baseline_daily_df["rebalanced"] = False
    baseline_daily_df["strategy_return"] = baseline_daily_df["realized_close_return"].astype(float)
    baseline_daily_df["baseline_return"] = baseline_daily_df["realized_close_return"].astype(float)
    baseline_daily_df["excess_return"] = 0.0
    baseline_daily_df["strategy_equity_curve"] = (1.0 + baseline_daily_df["strategy_return"]).cumprod()
    baseline_daily_df["baseline_equity_curve"] = baseline_daily_df["strategy_equity_curve"]
    baseline_daily_df["strategy_drawdown"] = _build_drawdown_series(
        baseline_daily_df["strategy_equity_curve"]
    )
    baseline_daily_df["baseline_drawdown"] = baseline_daily_df["strategy_drawdown"]
    return baseline_daily_df


def _simulate_candidate_daily_df(
    *,
    candidate_id: str,
    candidate_signal_df: pd.DataFrame,
    high_annualized_downside_return_standard_deviation_threshold: float,
    low_annualized_downside_return_standard_deviation_threshold: float,
    reduced_exposure_ratio: float,
) -> pd.DataFrame:
    active_exposure_ratio = 1.0
    rows: list[dict[str, Any]] = []

    for row in candidate_signal_df.itertuples(index=False):
        indicator_value = _coerce_numeric_value(row.annualized_downside_return_standard_deviation_mean)
        realized_overnight_return = _coerce_numeric_value(row.realized_overnight_return)
        realized_intraday_return = _coerce_numeric_value(row.realized_intraday_return)
        realized_close_return = _coerce_numeric_value(row.realized_close_return)
        next_exposure_ratio = active_exposure_ratio
        signal_state = "inside_band"
        if indicator_value > high_annualized_downside_return_standard_deviation_threshold:
            next_exposure_ratio = reduced_exposure_ratio
            signal_state = "above_high_threshold"
        elif indicator_value < low_annualized_downside_return_standard_deviation_threshold:
            next_exposure_ratio = 1.0
            signal_state = "below_low_threshold"

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
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidate_id = str(candidate_daily_df["candidate_id"].iloc[0])
    for sample_split in ("full", "discovery", "validation"):
        strategy_split_df = _subset_daily_df_for_split(candidate_daily_df, sample_split)
        baseline_split_df = _subset_daily_df_for_split(baseline_daily_df, sample_split)
        strategy_stats = _compute_return_series_stats(strategy_split_df["strategy_return"])
        baseline_stats = _compute_return_series_stats(baseline_split_df["strategy_return"])
        activity_stats = _compute_activity_stats(strategy_split_df)
        row = {
            "candidate_id": candidate_id,
            "sample_split": sample_split,
            "downside_return_standard_deviation_window_days": stddev_window_days,
            "downside_return_standard_deviation_mean_window_days": mean_window_days,
            "high_annualized_downside_return_standard_deviation_threshold": high_threshold,
            "low_annualized_downside_return_standard_deviation_threshold": low_threshold,
            "reduced_exposure_ratio": reduced_exposure_ratio,
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
        rows.append(row)
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
                "downside_return_standard_deviation_window_days",
                "downside_return_standard_deviation_mean_window_days",
                "high_annualized_downside_return_standard_deviation_threshold",
                "low_annualized_downside_return_standard_deviation_threshold",
                "reduced_exposure_ratio",
                "sample_split",
                *value_columns,
            ]
        ]
        .pivot_table(
            index=[
                "candidate_id",
                "downside_return_standard_deviation_window_days",
                "downside_return_standard_deviation_mean_window_days",
                "high_annualized_downside_return_standard_deviation_threshold",
                "low_annualized_downside_return_standard_deviation_threshold",
                "reduced_exposure_ratio",
            ],
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
    if candidate_comparison_df.empty:
        return None
    ranked_df = candidate_comparison_df[
        candidate_comparison_df[sort_column].notna()
    ].copy()
    if ranked_df.empty:
        return None
    ranked_df = ranked_df.sort_values(
        [sort_column, "cagr_discovery", "sharpe_ratio_validation", "candidate_id"],
        ascending=[False, False, False, True],
        ignore_index=True,
    )
    return ranked_df.iloc[0]


def _annotate_market_frame_with_split_labels(
    market_frame_df: pd.DataFrame,
    *,
    signal_base_df: pd.DataFrame,
) -> pd.DataFrame:
    annotated_df = market_frame_df.copy()
    signal_split_by_date = dict(
        zip(signal_base_df["signal_date"], signal_base_df["sample_split"], strict=True)
    )
    realized_split_by_date = dict(
        zip(signal_base_df["realized_date"], signal_base_df["sample_split"], strict=True)
    )
    annotated_df["signal_analysis_eligible"] = annotated_df["date"].isin(signal_split_by_date)
    annotated_df["signal_sample_split"] = annotated_df["date"].map(signal_split_by_date).fillna(
        "excluded"
    )
    annotated_df["realized_analysis_eligible"] = annotated_df["date"].isin(realized_split_by_date)
    annotated_df["realized_sample_split"] = annotated_df["date"].map(realized_split_by_date).fillna(
        "excluded"
    )
    return annotated_df


def _subset_daily_df_for_split(
    daily_df: pd.DataFrame,
    sample_split: str,
) -> pd.DataFrame:
    if sample_split == "full":
        return daily_df.copy()
    return daily_df[daily_df["sample_split"] == sample_split].copy()


def _compute_activity_stats(daily_df: pd.DataFrame) -> dict[str, Any]:
    if daily_df.empty:
        return {
            "average_exposure_ratio": float("nan"),
            "reduced_state_rate": float("nan"),
            "turnover_total": float("nan"),
            "turnover_mean": float("nan"),
            "switch_count": float("nan"),
        }
    exposure_after = daily_df["target_exposure_ratio"].astype(float)
    exposure_change = daily_df["exposure_change"].abs().astype(float)
    return {
        "average_exposure_ratio": float(exposure_after.mean()),
        "reduced_state_rate": float((exposure_after < 1.0).mean()),
        "turnover_total": float(exposure_change.sum()),
        "turnover_mean": float(exposure_change.mean()),
        "switch_count": int(daily_df["rebalanced"].sum()),
    }


def _compute_return_series_stats(series: pd.Series) -> dict[str, Any]:
    values = series.astype(float).dropna().reset_index(drop=True)
    day_count = int(len(values))
    if day_count == 0:
        return {
            "day_count": 0,
            "avg_daily_return": float("nan"),
            "median_daily_return": float("nan"),
            "daily_standard_deviation": float("nan"),
            "annualized_standard_deviation": float("nan"),
            "downside_standard_deviation": float("nan"),
            "sharpe_ratio": float("nan"),
            "sortino_ratio": float("nan"),
            "max_drawdown": float("nan"),
            "total_return": float("nan"),
            "cagr": float("nan"),
            "calmar_ratio": float("nan"),
            "positive_rate": float("nan"),
            "non_negative_rate": float("nan"),
            "best_day_return": float("nan"),
            "worst_day_return": float("nan"),
        }

    daily_standard_deviation = _safe_standard_deviation(values)
    downside_standard_deviation = _compute_downside_standard_deviation(values)
    annualized_standard_deviation = float(daily_standard_deviation * math.sqrt(ANNUALIZATION_FACTOR))
    equity_curve = (1.0 + values).cumprod()
    drawdown = equity_curve.div(equity_curve.cummax()).sub(1.0)
    total_return = float(equity_curve.iloc[-1] - 1.0)
    cagr = float(equity_curve.iloc[-1] ** (ANNUALIZATION_FACTOR / day_count) - 1.0)
    max_drawdown = float(drawdown.min())
    sharpe_ratio = (
        float(values.mean() / daily_standard_deviation * math.sqrt(ANNUALIZATION_FACTOR))
        if daily_standard_deviation > 0.0
        else float("nan")
    )
    sortino_ratio = (
        float(values.mean() / downside_standard_deviation * math.sqrt(ANNUALIZATION_FACTOR))
        if downside_standard_deviation > 0.0
        else float("nan")
    )
    calmar_ratio = (
        float(cagr / abs(max_drawdown))
        if math.isfinite(cagr) and max_drawdown < 0.0
        else float("nan")
    )
    return {
        "day_count": day_count,
        "avg_daily_return": float(values.mean()),
        "median_daily_return": float(values.median()),
        "daily_standard_deviation": float(daily_standard_deviation),
        "annualized_standard_deviation": annualized_standard_deviation,
        "downside_standard_deviation": float(downside_standard_deviation),
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "max_drawdown": max_drawdown,
        "total_return": total_return,
        "cagr": cagr,
        "calmar_ratio": calmar_ratio,
        "positive_rate": float((values > 0.0).mean()),
        "non_negative_rate": float((values >= 0.0).mean()),
        "best_day_return": float(values.max()),
        "worst_day_return": float(values.min()),
    }


def _safe_standard_deviation(series: pd.Series) -> float:
    if len(series) <= 1:
        return 0.0
    return float(series.std(ddof=1))


def _compute_downside_standard_deviation(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    downside = series.clip(upper=0.0)
    squared = downside.pow(2)
    return float(math.sqrt(float(squared.mean())))


def _build_drawdown_series(equity_curve: pd.Series) -> pd.Series:
    return equity_curve.div(equity_curve.cummax()).sub(1.0)


def _normalize_non_negative_float_sequence(
    values: Sequence[float] | None,
    *,
    default: tuple[float, ...],
    name: str,
) -> tuple[float, ...]:
    raw_values = tuple(default if values is None else tuple(float(value) for value in values))
    if not raw_values:
        raise ValueError(f"{name} must not be empty")
    if any(value < 0.0 for value in raw_values):
        raise ValueError(f"{name} must contain only non-negative numbers")
    return tuple(sorted(set(raw_values)))


def _normalize_exposure_ratio_sequence(
    values: Sequence[float] | None,
    *,
    default: tuple[float, ...],
    name: str,
) -> tuple[float, ...]:
    raw_values = _normalize_non_negative_float_sequence(values, default=default, name=name)
    if any(value > 1.0 for value in raw_values):
        raise ValueError(f"{name} must stay within 0.0 .. 1.0 for this baseline study")
    return raw_values


def _build_candidate_id(
    *,
    stddev_window_days: int,
    mean_window_days: int,
    high_threshold: float,
    low_threshold: float,
    reduced_exposure_ratio: float,
) -> str:
    return (
        f"std{stddev_window_days}_mean{mean_window_days}_"
        f"hi{_format_ratio_token(high_threshold)}_"
        f"lo{_format_ratio_token(low_threshold)}_"
        f"reduced{_format_ratio_token(reduced_exposure_ratio)}"
    )


def _format_ratio_token(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".").replace(".", "p")


def _coerce_sortable_number(value: Any) -> float:
    numeric = float(value)
    if math.isnan(numeric):
        return float("-inf")
    return numeric


def _coerce_numeric_value(value: object) -> float:
    return float(cast(Any, value))


def _format_percent(value: float) -> str:
    if not math.isfinite(value):
        return "n/a"
    return f"{value * 100:+.2f}%"


def _format_ratio(value: float) -> str:
    if not math.isfinite(value):
        return "n/a"
    return f"{value:.2f}"


def _build_research_bundle_summary_markdown(
    result: TopixDownsideReturnStandardDeviationExposureTimingResearchResult,
) -> str:
    baseline_validation = _lookup_split_row(result.baseline_metrics_df, "validation")
    best_sharpe_row = _lookup_selection_row(
        result.selection_summary_df,
        "best_discovery_sharpe",
    )
    best_cagr_row = _lookup_selection_row(
        result.selection_summary_df,
        "best_discovery_cagr",
    )

    lines = [
        "# TOPIX Downside Return Standard Deviation Exposure Timing",
        "",
        "## Scope",
        "",
        "This bundle tests whether a simple TOPIX long book can improve Sharpe or CAGR by reducing exposure when downside return standard deviation expands and restoring full exposure when downside return standard deviation compresses again.",
        "",
        "Execution convention:",
        "",
        "- signal uses information available at date `X` close",
        "- rebalance happens at `X+1 open`",
        "- overnight `X close -> X+1 open` keeps the previous exposure ratio",
        "- intraday `X+1 open -> X+1 close` uses the new exposure ratio",
        "",
        "## Search Grid",
        "",
        f"- Downside return standard deviation windows: `{_format_int_sequence(result.downside_return_standard_deviation_window_days)}`",
        f"- Downside return standard deviation mean windows: `{_format_int_sequence(result.downside_return_standard_deviation_mean_window_days)}`",
        f"- High annualized downside return standard deviation thresholds: `{_format_float_sequence(result.high_annualized_downside_return_standard_deviation_thresholds)}`",
        f"- Low annualized downside return standard deviation thresholds: `{_format_float_sequence(result.low_annualized_downside_return_standard_deviation_thresholds)}`",
        f"- Reduced exposure ratios: `{_format_float_sequence(result.reduced_exposure_ratios)}`",
        f"- Valid parameter combinations: `{result.candidate_count}`",
        f"- Common signal window: `{result.common_signal_start_date}` -> `{result.common_signal_end_date}`",
        f"- Realized comparison window: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
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
    else:
        lines.append("- Validation baseline row was unavailable.")

    lines.extend(["", "## Discovery Leaders", ""])
    if best_sharpe_row is not None:
        lines.extend(
            [
                "### Best Discovery Sharpe",
                "",
                f"- Candidate: `{best_sharpe_row['candidate_id']}`",
                f"- Params: `downside_std={int(best_sharpe_row['downside_return_standard_deviation_window_days'])}`, `mean={int(best_sharpe_row['downside_return_standard_deviation_mean_window_days'])}`, `high={float(best_sharpe_row['high_annualized_downside_return_standard_deviation_threshold']):.2f}`, `low={float(best_sharpe_row['low_annualized_downside_return_standard_deviation_threshold']):.2f}`, `reduced={float(best_sharpe_row['reduced_exposure_ratio']):.2f}`",
                f"- Discovery Sharpe: `{_format_ratio(float(best_sharpe_row['sharpe_ratio_discovery']))}`",
                f"- Validation Sharpe: `{_format_ratio(float(best_sharpe_row['sharpe_ratio_validation']))}`",
                f"- Validation CAGR: `{_format_percent(float(best_sharpe_row['cagr_validation']))}`",
                f"- Validation max drawdown: `{_format_percent(float(best_sharpe_row['max_drawdown_validation']))}`",
                f"- Validation Sharpe improvement vs baseline: `{_format_ratio(float(best_sharpe_row['sharpe_ratio_improvement_validation']))}`",
                f"- Validation max drawdown improvement vs baseline: `{_format_percent(float(best_sharpe_row['max_drawdown_improvement_validation']))}`",
            ]
        )
    if best_cagr_row is not None:
        lines.extend(
            [
                "",
                "### Best Discovery CAGR",
                "",
                f"- Candidate: `{best_cagr_row['candidate_id']}`",
                f"- Discovery CAGR: `{_format_percent(float(best_cagr_row['cagr_discovery']))}`",
                f"- Validation CAGR: `{_format_percent(float(best_cagr_row['cagr_validation']))}`",
                f"- Validation Sharpe: `{_format_ratio(float(best_cagr_row['sharpe_ratio_validation']))}`",
                f"- Validation max drawdown: `{_format_percent(float(best_cagr_row['max_drawdown_validation']))}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This is the rule-based baseline only. No machine learning is used here.",
            "- Thresholds are applied on annualized downside return standard deviation so the grid remains interpretable.",
            "- The strategy is long-only and only reduces exposure; it never exceeds 100% exposure in this baseline study.",
        ]
    )
    return "\n".join(lines)


def _lookup_split_row(metrics_df: pd.DataFrame, sample_split: str) -> pd.Series | None:
    matches = metrics_df[metrics_df["sample_split"] == sample_split]
    if matches.empty:
        return None
    return matches.iloc[0]


def _lookup_selection_row(
    selection_summary_df: pd.DataFrame,
    selection_label: str,
) -> pd.Series | None:
    matches = selection_summary_df[
        selection_summary_df["selection_label"] == selection_label
    ]
    if matches.empty:
        return None
    return matches.iloc[0]


def _format_int_sequence(values: Sequence[int]) -> str:
    return ", ".join(str(value) for value in values)


def _format_float_sequence(values: Sequence[float]) -> str:
    return ", ".join(f"{value:.2f}" for value in values)
