"""
TOPIX downside return-standard-deviation family committee walk-forward study.

This study fixes one downside return-standard-deviation family, ranks family
members inside each discovery block, builds equal-weight committees from the
top-ranked members, and checks whether those committee rules keep beating the
baseline in rolling walk-forward validation blocks.
"""

from __future__ import annotations

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
    _build_candidate_comparison_df,
    _build_candidate_id,
    _build_candidate_metric_rows,
    _build_candidate_signal_frame,
    _build_common_signal_frame,
    _build_baseline_daily_df,
    _compute_return_series_stats,
    _format_float_sequence,
    _format_int_sequence,
    _format_percent,
    _format_ratio,
    _normalize_exposure_ratio_sequence,
    _normalize_non_negative_float_sequence,
    _prepare_topix_market_frame,
    _simulate_candidate_daily_df,
)

DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS: tuple[int, ...] = (5,)
DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS: tuple[int, ...] = (
    1,
    2,
)
DEFAULT_FAMILY_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS: tuple[
    float, ...
] = (0.22, 0.24, 0.25)
DEFAULT_FAMILY_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS: tuple[
    float, ...
] = (0.05, 0.10, 0.15, 0.20, 0.22, 0.24, 0.25)
DEFAULT_FAMILY_REDUCED_EXPOSURE_RATIOS: tuple[float, ...] = (0.0, 0.10)
DEFAULT_COMMITTEE_SIZES: tuple[int, ...] = (1, 3, 5)
DEFAULT_RANK_TOP_KS: tuple[int, ...] = (10, 20, 50)
DEFAULT_DISCOVERY_WINDOW_DAYS = 252 * 3
DEFAULT_VALIDATION_WINDOW_DAYS = 252
DEFAULT_STEP_WINDOW_DAYS = 126
TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_FAMILY_COMMITTEE_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix-downside-return-standard-deviation-family-committee-walkforward"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "baseline_metrics_df",
    "family_candidate_comparison_df",
    "rank_stability_df",
    "walkforward_fold_candidate_rank_df",
    "walkforward_rank_diagnostics_df",
    "walkforward_fold_committee_df",
    "committee_summary_df",
    "candidate_selection_frequency_df",
)
_PARAMETER_COLUMNS: tuple[str, ...] = (
    "downside_return_standard_deviation_window_days",
    "downside_return_standard_deviation_mean_window_days",
    "high_annualized_downside_return_standard_deviation_threshold",
    "low_annualized_downside_return_standard_deviation_threshold",
    "reduced_exposure_ratio",
)


@dataclass(frozen=True)
class TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    fixed_split_validation_ratio: float
    family_downside_return_standard_deviation_window_days: tuple[int, ...]
    family_downside_return_standard_deviation_mean_window_days: tuple[int, ...]
    family_high_annualized_downside_return_standard_deviation_thresholds: tuple[float, ...]
    family_low_annualized_downside_return_standard_deviation_thresholds: tuple[float, ...]
    family_reduced_exposure_ratios: tuple[float, ...]
    committee_sizes: tuple[int, ...]
    rank_top_ks: tuple[int, ...]
    discovery_window_days: int
    validation_window_days: int
    step_window_days: int
    candidate_count: int
    fold_count: int
    baseline_metrics_df: pd.DataFrame
    family_candidate_comparison_df: pd.DataFrame
    rank_stability_df: pd.DataFrame
    walkforward_fold_candidate_rank_df: pd.DataFrame
    walkforward_rank_diagnostics_df: pd.DataFrame
    walkforward_fold_committee_df: pd.DataFrame
    committee_summary_df: pd.DataFrame
    candidate_selection_frequency_df: pd.DataFrame


def get_topix_downside_return_standard_deviation_family_committee_walkforward_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        return _fetch_date_range(ctx.connection, table_name="topix_data")


def run_topix_downside_return_standard_deviation_family_committee_walkforward_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    fixed_split_validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    family_downside_return_standard_deviation_window_days: Sequence[int] | None = None,
    family_downside_return_standard_deviation_mean_window_days: Sequence[int] | None = None,
    family_high_annualized_downside_return_standard_deviation_thresholds: Sequence[float]
    | None = None,
    family_low_annualized_downside_return_standard_deviation_thresholds: Sequence[float]
    | None = None,
    family_reduced_exposure_ratios: Sequence[float] | None = None,
    committee_sizes: Sequence[int] | None = None,
    rank_top_ks: Sequence[int] | None = None,
    discovery_window_days: int = DEFAULT_DISCOVERY_WINDOW_DAYS,
    validation_window_days: int = DEFAULT_VALIDATION_WINDOW_DAYS,
    step_window_days: int = DEFAULT_STEP_WINDOW_DAYS,
) -> TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult:
    if not 0.0 <= fixed_split_validation_ratio < 1.0:
        raise ValueError(
            "fixed_split_validation_ratio must satisfy 0.0 <= fixed_split_validation_ratio < 1.0"
        )
    if discovery_window_days <= 0 or validation_window_days <= 0 or step_window_days <= 0:
        raise ValueError(
            "discovery_window_days, validation_window_days, and step_window_days must be positive"
        )

    resolved_std_windows = _normalize_positive_int_sequence(
        family_downside_return_standard_deviation_window_days,
        default=DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
        name="family_downside_return_standard_deviation_window_days",
    )
    resolved_mean_windows = _normalize_positive_int_sequence(
        family_downside_return_standard_deviation_mean_window_days,
        default=DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS,
        name="family_downside_return_standard_deviation_mean_window_days",
    )
    resolved_high_thresholds = _normalize_non_negative_float_sequence(
        family_high_annualized_downside_return_standard_deviation_thresholds,
        default=DEFAULT_FAMILY_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
        name="family_high_annualized_downside_return_standard_deviation_thresholds",
    )
    resolved_low_thresholds = _normalize_non_negative_float_sequence(
        family_low_annualized_downside_return_standard_deviation_thresholds,
        default=DEFAULT_FAMILY_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
        name="family_low_annualized_downside_return_standard_deviation_thresholds",
    )
    resolved_reduced_exposures = _normalize_exposure_ratio_sequence(
        family_reduced_exposure_ratios,
        default=DEFAULT_FAMILY_REDUCED_EXPOSURE_RATIOS,
        name="family_reduced_exposure_ratios",
    )
    resolved_committee_sizes = _normalize_positive_int_sequence(
        committee_sizes,
        default=DEFAULT_COMMITTEE_SIZES,
        name="committee_sizes",
    )
    resolved_rank_top_ks = _normalize_positive_int_sequence(
        rank_top_ks,
        default=DEFAULT_RANK_TOP_KS,
        name="rank_top_ks",
    )

    family_grid = [
        {
            "candidate_id": _build_candidate_id(
                stddev_window_days=std_window,
                mean_window_days=mean_window,
                high_threshold=high_threshold,
                low_threshold=low_threshold,
                reduced_exposure_ratio=reduced_exposure_ratio,
            ),
            "downside_return_standard_deviation_window_days": std_window,
            "downside_return_standard_deviation_mean_window_days": mean_window,
            "high_annualized_downside_return_standard_deviation_threshold": high_threshold,
            "low_annualized_downside_return_standard_deviation_threshold": low_threshold,
            "reduced_exposure_ratio": reduced_exposure_ratio,
        }
        for std_window, mean_window, high_threshold, low_threshold, reduced_exposure_ratio in product(
            resolved_std_windows,
            resolved_mean_windows,
            resolved_high_thresholds,
            resolved_low_thresholds,
            resolved_reduced_exposures,
        )
        if low_threshold <= high_threshold
    ]
    if not family_grid:
        raise ValueError("No family members remained after low/high threshold filtering")
    if max(resolved_committee_sizes) > len(family_grid):
        raise ValueError("committee_sizes must not exceed the family candidate count")
    if max(resolved_rank_top_ks) > len(family_grid):
        raise ValueError("rank_top_ks must not exceed the family candidate count")

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
        max_downside_return_standard_deviation_window_days=max(resolved_std_windows),
        max_downside_return_standard_deviation_mean_window_days=max(resolved_mean_windows),
        validation_ratio=fixed_split_validation_ratio,
    )
    baseline_daily_df = _build_baseline_daily_df(signal_base_df)
    baseline_metrics_df = _build_baseline_metrics_df(baseline_daily_df)

    candidate_daily_df_by_id: dict[str, pd.DataFrame] = {}
    candidate_metric_rows: list[dict[str, Any]] = []
    signal_frame_cache: dict[tuple[int, int], pd.DataFrame] = {}
    for family_member in family_grid:
        std_window = int(family_member["downside_return_standard_deviation_window_days"])
        mean_window = int(family_member["downside_return_standard_deviation_mean_window_days"])
        high_threshold = float(
            family_member["high_annualized_downside_return_standard_deviation_threshold"]
        )
        low_threshold = float(
            family_member["low_annualized_downside_return_standard_deviation_threshold"]
        )
        reduced_exposure_ratio = float(family_member["reduced_exposure_ratio"])
        signal_key = (std_window, mean_window)
        if signal_key not in signal_frame_cache:
            signal_frame_cache[signal_key] = _build_candidate_signal_frame(
                market_frame_df,
                signal_base_df=signal_base_df,
                downside_return_standard_deviation_window_days=std_window,
                downside_return_standard_deviation_mean_window_days=mean_window,
            )
        candidate_daily_df = _simulate_candidate_daily_df(
            candidate_id=str(family_member["candidate_id"]),
            candidate_signal_df=signal_frame_cache[signal_key],
            high_annualized_downside_return_standard_deviation_threshold=high_threshold,
            low_annualized_downside_return_standard_deviation_threshold=low_threshold,
            reduced_exposure_ratio=reduced_exposure_ratio,
        )
        candidate_daily_df_by_id[str(family_member["candidate_id"])] = candidate_daily_df
        candidate_metric_rows.extend(
            _build_candidate_metric_rows(
                candidate_daily_df,
                baseline_daily_df=baseline_daily_df,
                stddev_window_days=std_window,
                mean_window_days=mean_window,
                high_threshold=high_threshold,
                low_threshold=low_threshold,
                reduced_exposure_ratio=reduced_exposure_ratio,
            )
        )

    family_candidate_comparison_df = _build_candidate_comparison_df(
        pd.DataFrame(candidate_metric_rows)
    )
    rank_stability_df = _build_rank_stability_df(
        family_candidate_comparison_df,
        top_ks=resolved_rank_top_ks,
    )
    (
        walkforward_fold_candidate_rank_df,
        walkforward_rank_diagnostics_df,
        walkforward_fold_committee_df,
    ) = _build_walkforward_outputs(
        baseline_daily_df=baseline_daily_df,
        family_candidate_comparison_df=family_candidate_comparison_df,
        candidate_daily_df_by_id=candidate_daily_df_by_id,
        committee_sizes=resolved_committee_sizes,
        rank_top_ks=resolved_rank_top_ks,
        discovery_window_days=discovery_window_days,
        validation_window_days=validation_window_days,
        step_window_days=step_window_days,
    )
    committee_summary_df = _build_committee_summary_df(walkforward_fold_committee_df)
    candidate_selection_frequency_df = _build_candidate_selection_frequency_df(
        walkforward_fold_committee_df=walkforward_fold_committee_df,
        family_candidate_comparison_df=family_candidate_comparison_df,
        fold_count=int(walkforward_rank_diagnostics_df["fold_count"].iloc[0])
        if not walkforward_rank_diagnostics_df.empty
        else 0,
    )

    analysis_start_date = (
        str(baseline_daily_df["realized_date"].iloc[0]) if not baseline_daily_df.empty else None
    )
    analysis_end_date = (
        str(baseline_daily_df["realized_date"].iloc[-1]) if not baseline_daily_df.empty else None
    )

    return TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        fixed_split_validation_ratio=fixed_split_validation_ratio,
        family_downside_return_standard_deviation_window_days=resolved_std_windows,
        family_downside_return_standard_deviation_mean_window_days=resolved_mean_windows,
        family_high_annualized_downside_return_standard_deviation_thresholds=resolved_high_thresholds,
        family_low_annualized_downside_return_standard_deviation_thresholds=resolved_low_thresholds,
        family_reduced_exposure_ratios=resolved_reduced_exposures,
        committee_sizes=resolved_committee_sizes,
        rank_top_ks=resolved_rank_top_ks,
        discovery_window_days=discovery_window_days,
        validation_window_days=validation_window_days,
        step_window_days=step_window_days,
        candidate_count=len(family_grid),
        fold_count=int(walkforward_rank_diagnostics_df["fold_count"].iloc[0])
        if not walkforward_rank_diagnostics_df.empty
        else 0,
        baseline_metrics_df=baseline_metrics_df,
        family_candidate_comparison_df=family_candidate_comparison_df,
        rank_stability_df=rank_stability_df,
        walkforward_fold_candidate_rank_df=walkforward_fold_candidate_rank_df,
        walkforward_rank_diagnostics_df=walkforward_rank_diagnostics_df,
        walkforward_fold_committee_df=walkforward_fold_committee_df,
        committee_summary_df=committee_summary_df,
        candidate_selection_frequency_df=candidate_selection_frequency_df,
    )


def write_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle(
    result: TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_FAMILY_COMMITTEE_WALKFORWARD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix_downside_return_standard_deviation_family_committee_walkforward_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "fixed_split_validation_ratio": result.fixed_split_validation_ratio,
            "family_downside_return_standard_deviation_window_days": list(
                result.family_downside_return_standard_deviation_window_days
            ),
            "family_downside_return_standard_deviation_mean_window_days": list(
                result.family_downside_return_standard_deviation_mean_window_days
            ),
            "family_high_annualized_downside_return_standard_deviation_thresholds": list(
                result.family_high_annualized_downside_return_standard_deviation_thresholds
            ),
            "family_low_annualized_downside_return_standard_deviation_thresholds": list(
                result.family_low_annualized_downside_return_standard_deviation_thresholds
            ),
            "family_reduced_exposure_ratios": list(result.family_reduced_exposure_ratios),
            "committee_sizes": list(result.committee_sizes),
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


def load_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle(
    bundle_path: str | Path,
) -> TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix_downside_return_standard_deviation_family_committee_walkforward_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_FAMILY_COMMITTEE_WALKFORWARD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix_downside_return_standard_deviation_family_committee_walkforward_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_FAMILY_COMMITTEE_WALKFORWARD_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_baseline_metrics_df(baseline_daily_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sample_split in ("full", "discovery", "validation"):
        split_df = (
            baseline_daily_df.copy()
            if sample_split == "full"
            else baseline_daily_df[baseline_daily_df["sample_split"] == sample_split].copy()
        )
        rows.append(
            {
                "sample_split": sample_split,
                **_compute_return_series_stats(split_df["strategy_return"]),
            }
        )
    return pd.DataFrame(rows)


def _build_rank_stability_df(
    family_candidate_comparison_df: pd.DataFrame,
    *,
    top_ks: Sequence[int],
) -> pd.DataFrame:
    sharpe_discovery = family_candidate_comparison_df["sharpe_ratio_discovery"].astype(float)
    sharpe_validation = family_candidate_comparison_df["sharpe_ratio_validation"].astype(float)
    cagr_discovery = family_candidate_comparison_df["cagr_discovery"].astype(float)
    cagr_validation = family_candidate_comparison_df["cagr_validation"].astype(float)

    row: dict[str, Any] = {
        "candidate_count": int(len(family_candidate_comparison_df)),
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
            family_candidate_comparison_df.nlargest(
                top_k,
                ["sharpe_ratio_discovery", "cagr_discovery"],
            )["candidate_id"].astype(str)
        )
        top_validation = set(
            family_candidate_comparison_df.nlargest(
                top_k,
                ["sharpe_ratio_validation", "cagr_validation"],
            )["candidate_id"].astype(str)
        )
        overlap_count = len(top_discovery & top_validation)
        row[f"top{top_k}_overlap_count"] = overlap_count
        row[f"top{top_k}_overlap_ratio"] = float(overlap_count / top_k)
    return pd.DataFrame([row])


def _build_walkforward_outputs(
    *,
    baseline_daily_df: pd.DataFrame,
    family_candidate_comparison_df: pd.DataFrame,
    candidate_daily_df_by_id: dict[str, pd.DataFrame],
    committee_sizes: Sequence[int],
    rank_top_ks: Sequence[int],
    discovery_window_days: int,
    validation_window_days: int,
    step_window_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not candidate_daily_df_by_id:
        raise ValueError("No family candidate daily frames were available")

    ordered_candidates = family_candidate_comparison_df["candidate_id"].astype(str).tolist()
    base_dates = pd.to_datetime(baseline_daily_df["realized_date"]).reset_index(drop=True)
    if len(base_dates) < discovery_window_days + validation_window_days:
        raise ValueError("No walk-forward splits were generated for the selected family window")

    candidate_return_panel_df = pd.DataFrame({"realized_date": baseline_daily_df["realized_date"]})
    for candidate_id in ordered_candidates:
        candidate_daily_df = candidate_daily_df_by_id[candidate_id]
        candidate_return_panel_df[candidate_id] = candidate_daily_df["strategy_return"].astype(float)
    baseline_returns = baseline_daily_df["strategy_return"].astype(float).reset_index(drop=True)

    parameter_lookup = {
        str(row["candidate_id"]): {
            column: row[column]
            for column in _PARAMETER_COLUMNS
        }
        for _, row in family_candidate_comparison_df.iterrows()
    }

    fold_candidate_rows: list[dict[str, Any]] = []
    fold_committee_rows: list[dict[str, Any]] = []
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
        for committee_size in committee_sizes:
            selected_df = candidate_fold_df.nsmallest(int(committee_size), "discovery_rank_by_sharpe")
            selected_ids = selected_df["candidate_id"].astype(str).tolist()
            discovery_committee_returns = candidate_return_panel_df[selected_ids].iloc[
                discovery_slice
            ].mean(axis=1)
            validation_committee_returns = candidate_return_panel_df[selected_ids].iloc[
                validation_slice
            ].mean(axis=1)
            discovery_committee_stats = _compute_return_series_stats(discovery_committee_returns)
            validation_committee_stats = _compute_return_series_stats(validation_committee_returns)
            fold_committee_rows.append(
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
                    "committee_size": int(committee_size),
                    "selected_candidates": "|".join(selected_ids),
                    "selected_candidate_count": int(len(selected_ids)),
                    "selected_candidate_discovery_sharpe_mean": float(
                        selected_df["discovery_sharpe_ratio"].mean()
                    ),
                    "selected_candidate_validation_sharpe_mean": float(
                        selected_df["validation_sharpe_ratio"].mean()
                    ),
                    "committee_discovery_sharpe_ratio": discovery_committee_stats["sharpe_ratio"],
                    "committee_discovery_cagr": discovery_committee_stats["cagr"],
                    "committee_discovery_max_drawdown": discovery_committee_stats["max_drawdown"],
                    "committee_validation_sharpe_ratio": validation_committee_stats[
                        "sharpe_ratio"
                    ],
                    "committee_validation_cagr": validation_committee_stats["cagr"],
                    "committee_validation_max_drawdown": validation_committee_stats[
                        "max_drawdown"
                    ],
                    "baseline_discovery_sharpe_ratio": baseline_discovery_stats["sharpe_ratio"],
                    "baseline_discovery_cagr": baseline_discovery_stats["cagr"],
                    "baseline_discovery_max_drawdown": baseline_discovery_stats[
                        "max_drawdown"
                    ],
                    "baseline_validation_sharpe_ratio": baseline_validation_stats["sharpe_ratio"],
                    "baseline_validation_cagr": baseline_validation_stats["cagr"],
                    "baseline_validation_max_drawdown": baseline_validation_stats[
                        "max_drawdown"
                    ],
                    "validation_sharpe_ratio_excess": (
                        validation_committee_stats["sharpe_ratio"]
                        - baseline_validation_stats["sharpe_ratio"]
                    ),
                    "validation_cagr_excess": (
                        validation_committee_stats["cagr"] - baseline_validation_stats["cagr"]
                    ),
                    "validation_max_drawdown_improvement": (
                        validation_committee_stats["max_drawdown"]
                        - baseline_validation_stats["max_drawdown"]
                    ),
                    "validation_sharpe_win": (
                        validation_committee_stats["sharpe_ratio"]
                        > baseline_validation_stats["sharpe_ratio"]
                    ),
                    "validation_cagr_win": (
                        validation_committee_stats["cagr"] > baseline_validation_stats["cagr"]
                    ),
                }
            )
        fold_index += 1

    if fold_index == 0:
        raise ValueError("No walk-forward splits were generated for the selected family window")

    walkforward_fold_candidate_rank_df = pd.DataFrame(fold_candidate_rows)
    walkforward_fold_committee_df = pd.DataFrame(fold_committee_rows)
    diagnostic_row: dict[str, Any] = {
        "fold_count": int(fold_index),
        "avg_fold_spearman_sharpe": float(pd.Series(fold_spearman_sharpe_values).mean()),
        "median_fold_spearman_sharpe": float(pd.Series(fold_spearman_sharpe_values).median()),
    }
    for top_k in rank_top_ks:
        values = overlap_ratios_by_top_k[int(top_k)]
        diagnostic_row[f"avg_top{top_k}_overlap_ratio"] = float(pd.Series(values).mean())
        diagnostic_row[f"median_top{top_k}_overlap_ratio"] = float(pd.Series(values).median())
    walkforward_rank_diagnostics_df = pd.DataFrame([diagnostic_row])
    return (
        walkforward_fold_candidate_rank_df,
        walkforward_rank_diagnostics_df,
        walkforward_fold_committee_df,
    )


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
        [
            "discovery_sharpe_ratio",
            "discovery_cagr",
            "candidate_id",
        ],
        ascending=[False, False, True],
        ignore_index=True,
    )
    fold_candidate_df["discovery_rank_by_sharpe"] = range(1, len(fold_candidate_df) + 1)
    validation_rank_df = fold_candidate_df.sort_values(
        [
            "validation_sharpe_ratio",
            "validation_cagr",
            "candidate_id",
        ],
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
        fold_candidate_df["candidate_id"]
        .astype(str)
        .map(validation_rank_lookup)
        .astype(int)
    )
    return fold_candidate_df


def _build_committee_summary_df(
    walkforward_fold_committee_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for committee_size, committee_df in walkforward_fold_committee_df.groupby("committee_size"):
        committee_size_int = int(float(cast(Any, committee_size)))
        rows.append(
            {
                "committee_size": committee_size_int,
                "fold_count": int(len(committee_df)),
                "validation_sharpe_win_rate": float(
                    committee_df["validation_sharpe_win"].astype(float).mean()
                ),
                "validation_cagr_win_rate": float(
                    committee_df["validation_cagr_win"].astype(float).mean()
                ),
                "avg_validation_sharpe_ratio_excess": float(
                    committee_df["validation_sharpe_ratio_excess"].astype(float).mean()
                ),
                "median_validation_sharpe_ratio_excess": float(
                    committee_df["validation_sharpe_ratio_excess"].astype(float).median()
                ),
                "avg_validation_cagr_excess": float(
                    committee_df["validation_cagr_excess"].astype(float).mean()
                ),
                "median_validation_cagr_excess": float(
                    committee_df["validation_cagr_excess"].astype(float).median()
                ),
                "avg_validation_max_drawdown_improvement": float(
                    committee_df["validation_max_drawdown_improvement"].astype(float).mean()
                ),
                "median_validation_max_drawdown_improvement": float(
                    committee_df["validation_max_drawdown_improvement"].astype(float).median()
                ),
                "avg_committee_validation_sharpe_ratio": float(
                    committee_df["committee_validation_sharpe_ratio"].astype(float).mean()
                ),
                "avg_committee_validation_cagr": float(
                    committee_df["committee_validation_cagr"].astype(float).mean()
                ),
                "avg_baseline_validation_sharpe_ratio": float(
                    committee_df["baseline_validation_sharpe_ratio"].astype(float).mean()
                ),
                "avg_baseline_validation_cagr": float(
                    committee_df["baseline_validation_cagr"].astype(float).mean()
                ),
            }
        )
    summary_df = pd.DataFrame(rows)
    return summary_df.sort_values(
        [
            "validation_sharpe_win_rate",
            "avg_validation_sharpe_ratio_excess",
            "committee_size",
        ],
        ascending=[False, False, True],
        ignore_index=True,
    )


def _build_candidate_selection_frequency_df(
    *,
    walkforward_fold_committee_df: pd.DataFrame,
    family_candidate_comparison_df: pd.DataFrame,
    fold_count: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, fold_row in walkforward_fold_committee_df.iterrows():
        selected_candidates = [
            candidate_id
            for candidate_id in str(fold_row["selected_candidates"]).split("|")
            if candidate_id
        ]
        for rank_position, candidate_id in enumerate(selected_candidates, start=1):
            rows.append(
                {
                    "committee_size": int(fold_row["committee_size"]),
                    "fold_index": int(fold_row["fold_index"]),
                    "candidate_id": candidate_id,
                    "selection_rank": rank_position,
                }
            )
    if not rows:
        return pd.DataFrame()

    selection_df = pd.DataFrame(rows)
    summary_df = (
        selection_df.groupby(["committee_size", "candidate_id"], as_index=False)
        .agg(
            selection_count=("fold_index", "count"),
            mean_selection_rank=("selection_rank", "mean"),
        )
        .sort_values(
            ["committee_size", "selection_count", "mean_selection_rank", "candidate_id"],
            ascending=[True, False, True, True],
            ignore_index=True,
        )
    )
    summary_df["selection_rate"] = summary_df["selection_count"].div(max(fold_count, 1))
    return summary_df.merge(
        family_candidate_comparison_df[
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
    result: TopixDownsideReturnStandardDeviationFamilyCommitteeWalkforwardResearchResult,
) -> str:
    baseline_validation = _lookup_split_row(result.baseline_metrics_df, "validation")
    rank_row = result.rank_stability_df.iloc[0] if not result.rank_stability_df.empty else None
    walkforward_row = (
        result.walkforward_rank_diagnostics_df.iloc[0]
        if not result.walkforward_rank_diagnostics_df.empty
        else None
    )
    best_committee_row = (
        result.committee_summary_df.iloc[0] if not result.committee_summary_df.empty else None
    )

    lines = [
        "# TOPIX Downside Return Standard Deviation Family Committee Walk-Forward",
        "",
        "## Scope",
        "",
        "This bundle fixes one downside return standard deviation family, ranks family members inside each discovery block, builds equal-weight committees from the top-ranked members, and checks whether those committee rules keep beating the baseline in rolling walk-forward validation blocks.",
        "",
        "## Fixed Family",
        "",
        f"- Downside return standard deviation windows: `{_format_int_sequence(result.family_downside_return_standard_deviation_window_days)}`",
        f"- Downside return standard deviation mean windows: `{_format_int_sequence(result.family_downside_return_standard_deviation_mean_window_days)}`",
        f"- High annualized downside return standard deviation thresholds: `{_format_float_sequence(result.family_high_annualized_downside_return_standard_deviation_thresholds)}`",
        f"- Low annualized downside return standard deviation thresholds: `{_format_float_sequence(result.family_low_annualized_downside_return_standard_deviation_thresholds)}`",
        f"- Reduced exposure ratios: `{_format_float_sequence(result.family_reduced_exposure_ratios)}`",
        f"- Family candidate count: `{result.candidate_count}`",
        f"- Committee sizes: `{_format_int_sequence(result.committee_sizes)}`",
        "",
        "## Single Split Stability",
        "",
    ]
    if rank_row is not None:
        lines.extend(
            [
                f"- Pearson Sharpe correlation: `{_format_ratio(float(rank_row['pearson_sharpe']))}`",
                f"- Spearman Sharpe correlation: `{_format_ratio(float(rank_row['spearman_sharpe']))}`",
                f"- Pearson CAGR correlation: `{_format_ratio(float(rank_row['pearson_cagr']))}`",
                f"- Spearman CAGR correlation: `{_format_ratio(float(rank_row['spearman_cagr']))}`",
            ]
        )
        for top_k in result.rank_top_ks:
            lines.append(
                f"- Top {int(top_k)} overlap ratio: `{_format_ratio(float(rank_row[f'top{int(top_k)}_overlap_ratio']))}`"
            )
    else:
        lines.append("- Single split rank diagnostics were unavailable.")

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
    else:
        lines.append("- Walk-forward rank diagnostics were unavailable.")

    lines.extend(["", "## Committee Summary", ""])
    if baseline_validation is not None:
        lines.append(
            f"- Baseline validation Sharpe/CAGR/MaxDD: `{_format_ratio(float(baseline_validation['sharpe_ratio']))}` / `{_format_percent(float(baseline_validation['cagr']))}` / `{_format_percent(float(baseline_validation['max_drawdown']))}`"
        )
    if best_committee_row is not None:
        lines.extend(
            [
                f"- Best committee size by win rate: `{int(best_committee_row['committee_size'])}`",
                f"- Sharpe win rate: `{_format_ratio(float(best_committee_row['validation_sharpe_win_rate']))}`",
                f"- Mean validation Sharpe excess: `{_format_ratio(float(best_committee_row['avg_validation_sharpe_ratio_excess']))}`",
                f"- Mean validation CAGR excess: `{_format_percent(float(best_committee_row['avg_validation_cagr_excess']))}`",
                f"- Mean validation MaxDD improvement: `{_format_percent(float(best_committee_row['avg_validation_max_drawdown_improvement']))}`",
            ]
        )

    return "\n".join(lines)


def _lookup_split_row(metrics_df: pd.DataFrame, sample_split: str) -> pd.Series | None:
    matches = metrics_df[metrics_df["sample_split"] == sample_split]
    if matches.empty:
        return None
    return matches.iloc[0]
