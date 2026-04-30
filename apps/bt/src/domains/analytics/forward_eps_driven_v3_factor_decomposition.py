"""V3 factor decomposition for the production forward EPS strategy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    DEFAULT_HOLDOUT_MONTHS,
    DEFAULT_QUANTILE_BUCKET_COUNT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    DEFAULT_STRATEGY_NAME,
    _assign_quantile_bucket,
    _build_trade_metrics,
    _with_value_composite_score,
    run_forward_eps_trade_archetype_decomposition,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)

FORWARD_EPS_DRIVEN_V3_FACTOR_DECOMPOSITION_EXPERIMENT_ID = (
    "strategy-audit/forward-eps-driven-v3-factor-decomposition"
)
DEFAULT_DATASET_NAME = "primeExTopix500"
DEFAULT_SIZE_HAIRCUT = 0.5


@dataclass(frozen=True)
class _FactorSpec:
    name: str
    family: str
    label: str


@dataclass(frozen=True)
class ForwardEpsDrivenV3FactorDecompositionResult:
    db_path: str
    strategy_name: str
    dataset_name: str
    holdout_months: int
    severe_loss_threshold_pct: float
    quantile_bucket_count: int
    size_haircut: float
    analysis_start_date: str
    analysis_end_date: str
    dataset_summary_df: pd.DataFrame
    scenario_summary_df: pd.DataFrame
    market_scope_summary_df: pd.DataFrame
    factor_bucket_summary_df: pd.DataFrame
    factor_contrast_summary_df: pd.DataFrame
    tail_profile_df: pd.DataFrame
    action_candidate_summary_df: pd.DataFrame
    enriched_trade_df: pd.DataFrame


_FACTOR_SPECS: tuple[_FactorSpec, ...] = (
    _FactorSpec("forward_eps_growth_value", "fundamental", "Forward EPS growth"),
    _FactorSpec("forward_eps_growth_margin", "fundamental", "Forward EPS growth margin"),
    _FactorSpec("pbr", "fundamental", "PBR"),
    _FactorSpec("forward_per", "fundamental", "Forward PER"),
    _FactorSpec("market_cap_bil_jpy", "fundamental", "Market cap"),
    _FactorSpec("value_composite_score", "fundamental", "Value composite"),
    _FactorSpec("risk_adjusted_return_value", "technical", "Risk-adjusted return"),
    _FactorSpec("risk_adjusted_return_margin", "technical", "Risk-adjusted return margin"),
    _FactorSpec("volume_ratio_value", "technical", "Volume ratio"),
    _FactorSpec("volume_ratio_margin", "technical", "Volume ratio margin"),
    _FactorSpec("rsi10", "technical", "RSI 10"),
    _FactorSpec("stock_return_20d_pct", "technical", "Stock return 20d"),
    _FactorSpec("stock_return_60d_pct", "technical", "Stock return 60d"),
    _FactorSpec("stock_volatility_20d_pct", "technical", "Stock volatility 20d"),
    _FactorSpec("topix_return_20d_pct", "market_regime", "TOPIX return 20d"),
    _FactorSpec("topix_return_60d_pct", "market_regime", "TOPIX return 60d"),
    _FactorSpec("topix_risk_adjusted_return_60", "market_regime", "TOPIX risk-adjusted return"),
    _FactorSpec("topix_close_vs_sma200_pct", "market_regime", "TOPIX close vs SMA200"),
    _FactorSpec("days_since_disclosed", "freshness", "Days since disclosure"),
)

_TRADE_METRIC_COLUMNS: tuple[str, ...] = (
    "trade_count",
    "avg_trade_return_pct",
    "median_trade_return_pct",
    "win_rate_pct",
    "severe_loss_rate_pct",
    "worst_trade_return_pct",
    "p10_trade_return_pct",
)
_GROUP_COLUMNS: tuple[str, ...] = ("window_label", "market_scope")
_FACTOR_BUCKET_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "factor_family",
    "feature_name",
    "feature_label",
    "bucket_rank",
    "bucket_count",
    "bucket_label",
    "coverage_pct",
    "feature_min",
    "feature_median",
    "feature_max",
    *_TRADE_METRIC_COLUMNS,
)
_FACTOR_CONTRAST_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "factor_family",
    "feature_name",
    "feature_label",
    "low_bucket_label",
    "high_bucket_label",
    "low_trade_count",
    "high_trade_count",
    "low_avg_trade_return_pct",
    "high_avg_trade_return_pct",
    "delta_high_minus_low_avg_trade_return_pct",
    "low_severe_loss_rate_pct",
    "high_severe_loss_rate_pct",
    "delta_high_minus_low_severe_loss_rate_pct",
)
_TAIL_PROFILE_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "tail_cohort",
    "right_tail_threshold_pct",
    *_TRADE_METRIC_COLUMNS,
    *(f"median_{spec.name}" for spec in _FACTOR_SPECS),
)
_ACTION_CANDIDATE_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "candidate_name",
    "action_type",
    "candidate_description",
    "selected_trade_count",
    "selected_coverage_pct",
    "selected_avg_trade_return_pct",
    "selected_median_trade_return_pct",
    "selected_severe_loss_rate_pct",
    "kept_trade_count",
    "kept_avg_trade_return_pct",
    "kept_median_trade_return_pct",
    "kept_severe_loss_rate_pct",
    "kept_worst_trade_return_pct",
    "haircut_size_multiplier",
    "haircut_avg_trade_return_pct",
    "haircut_median_trade_return_pct",
    "haircut_severe_loss_rate_pct",
    "haircut_worst_trade_return_pct",
)


def run_forward_eps_driven_v3_factor_decomposition(
    *,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    dataset_name: str = DEFAULT_DATASET_NAME,
    holdout_months: int = DEFAULT_HOLDOUT_MONTHS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    quantile_bucket_count: int = DEFAULT_QUANTILE_BUCKET_COUNT,
    size_haircut: float = DEFAULT_SIZE_HAIRCUT,
) -> ForwardEpsDrivenV3FactorDecompositionResult:
    if holdout_months <= 0:
        raise ValueError("holdout_months must be greater than 0")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if quantile_bucket_count < 2:
        raise ValueError("quantile_bucket_count must be at least 2")
    if not 0.0 <= size_haircut <= 1.0:
        raise ValueError("size_haircut must satisfy 0.0 <= size_haircut <= 1.0")

    base_result = run_forward_eps_trade_archetype_decomposition(
        strategy_name=strategy_name,
        dataset_name=dataset_name,
        holdout_months=holdout_months,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        quantile_bucket_count=quantile_bucket_count,
    )
    frame = _prepare_factor_frame(base_result.enriched_trade_df)
    factor_bucket_summary_df = _build_factor_bucket_summary_df(
        frame=frame,
        quantile_bucket_count=quantile_bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    factor_contrast_summary_df = _build_factor_contrast_summary_df(
        factor_bucket_summary_df
    )
    tail_profile_df = _build_tail_profile_df(
        frame=frame,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    action_candidate_summary_df = _build_action_candidate_summary_df(
        frame=frame,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        size_haircut=size_haircut,
    )

    return ForwardEpsDrivenV3FactorDecompositionResult(
        db_path="multi://forward-eps-driven-v3-factor-decomposition",
        strategy_name=base_result.strategy_name,
        dataset_name=base_result.dataset_name,
        holdout_months=base_result.holdout_months,
        severe_loss_threshold_pct=base_result.severe_loss_threshold_pct,
        quantile_bucket_count=base_result.quantile_bucket_count,
        size_haircut=size_haircut,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        dataset_summary_df=base_result.dataset_summary_df,
        scenario_summary_df=base_result.scenario_summary_df,
        market_scope_summary_df=base_result.market_scope_summary_df,
        factor_bucket_summary_df=factor_bucket_summary_df,
        factor_contrast_summary_df=factor_contrast_summary_df,
        tail_profile_df=tail_profile_df,
        action_candidate_summary_df=action_candidate_summary_df,
        enriched_trade_df=frame,
    )


def write_forward_eps_driven_v3_factor_decomposition_bundle(
    result: ForwardEpsDrivenV3FactorDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=FORWARD_EPS_DRIVEN_V3_FACTOR_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_forward_eps_driven_v3_factor_decomposition",
        params={
            "strategy_name": result.strategy_name,
            "dataset_name": result.dataset_name,
            "holdout_months": result.holdout_months,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "quantile_bucket_count": result.quantile_bucket_count,
            "size_haircut": result.size_haircut,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "strategy_name": result.strategy_name,
            "dataset_name": result.dataset_name,
            "holdout_months": result.holdout_months,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "quantile_bucket_count": result.quantile_bucket_count,
            "size_haircut": result.size_haircut,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
        },
        result_tables={
            "dataset_summary_df": result.dataset_summary_df,
            "scenario_summary_df": result.scenario_summary_df,
            "market_scope_summary_df": result.market_scope_summary_df,
            "factor_bucket_summary_df": result.factor_bucket_summary_df,
            "factor_contrast_summary_df": result.factor_contrast_summary_df,
            "tail_profile_df": result.tail_profile_df,
            "action_candidate_summary_df": result.action_candidate_summary_df,
            "enriched_trade_df": result.enriched_trade_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_forward_eps_driven_v3_factor_decomposition_bundle(
    bundle_path: str | Path,
) -> ForwardEpsDrivenV3FactorDecompositionResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return ForwardEpsDrivenV3FactorDecompositionResult(
        db_path=str(metadata["db_path"]),
        strategy_name=str(metadata["strategy_name"]),
        dataset_name=str(metadata["dataset_name"]),
        holdout_months=int(metadata["holdout_months"]),
        severe_loss_threshold_pct=float(metadata["severe_loss_threshold_pct"]),
        quantile_bucket_count=int(metadata["quantile_bucket_count"]),
        size_haircut=float(metadata["size_haircut"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        dataset_summary_df=tables["dataset_summary_df"],
        scenario_summary_df=tables["scenario_summary_df"],
        market_scope_summary_df=tables["market_scope_summary_df"],
        factor_bucket_summary_df=tables["factor_bucket_summary_df"],
        factor_contrast_summary_df=tables["factor_contrast_summary_df"],
        tail_profile_df=tables["tail_profile_df"],
        action_candidate_summary_df=tables["action_candidate_summary_df"],
        enriched_trade_df=tables["enriched_trade_df"],
    )


def get_forward_eps_driven_v3_factor_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FORWARD_EPS_DRIVEN_V3_FACTOR_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_forward_eps_driven_v3_factor_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FORWARD_EPS_DRIVEN_V3_FACTOR_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _prepare_factor_frame(enriched_trade_df: pd.DataFrame) -> pd.DataFrame:
    frame = _with_value_composite_score(enriched_trade_df.copy())
    if "market_scope" not in frame.columns:
        frame["market_scope"] = "unknown"
    if "window_label" not in frame.columns:
        frame["window_label"] = "full"
    for spec in _FACTOR_SPECS:
        if spec.name not in frame.columns:
            frame[spec.name] = np.nan
    return frame


def _build_factor_bucket_summary_df(
    *,
    frame: pd.DataFrame,
    quantile_bucket_count: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload, group_frame in _iter_window_market_groups(frame):
        total_trade_count = len(group_frame)
        for spec in _FACTOR_SPECS:
            valid = group_frame.dropna(subset=[spec.name]).copy()
            if valid.empty:
                continue
            bucket_count = min(quantile_bucket_count, len(valid))
            valid["bucket_rank"] = _assign_quantile_bucket(
                pd.to_numeric(valid[spec.name], errors="coerce"),
                bucket_count=bucket_count,
            )
            for bucket_rank, bucket_frame in valid.groupby("bucket_rank", dropna=False):
                bucket_rank_int = _coerce_int(bucket_rank)
                metrics = _build_trade_metrics(
                    bucket_frame,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                values = pd.to_numeric(bucket_frame[spec.name], errors="coerce")
                rows.append(
                    {
                        **payload,
                        "factor_family": spec.family,
                        "feature_name": spec.name,
                        "feature_label": spec.label,
                        "bucket_rank": bucket_rank_int,
                        "bucket_count": bucket_count,
                        "bucket_label": f"Q{bucket_rank_int}/{bucket_count}",
                        "coverage_pct": _coverage(metrics["trade_count"], total_trade_count),
                        "feature_min": _finite_or_nan(values.min()),
                        "feature_median": _finite_or_nan(values.median()),
                        "feature_max": _finite_or_nan(values.max()),
                        **metrics,
                    }
                )
    return _sort_factor_table(_table_with_columns(rows, _FACTOR_BUCKET_COLUMNS))


def _build_factor_contrast_summary_df(
    factor_bucket_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if factor_bucket_summary_df.empty:
        return _table_with_columns([], _FACTOR_CONTRAST_COLUMNS)
    rows: list[dict[str, Any]] = []
    group_columns = ["window_label", "market_scope", "factor_family", "feature_name", "feature_label"]
    for keys, group in factor_bucket_summary_df.groupby(group_columns, dropna=False, sort=False):
        q1 = group[group["bucket_rank"] == group["bucket_rank"].min()]
        qn = group[group["bucket_rank"] == group["bucket_rank"].max()]
        if q1.empty or qn.empty:
            continue
        q1_row = q1.iloc[0]
        qn_row = qn.iloc[0]
        rows.append(
            {
                **dict(zip(group_columns, keys, strict=True)),
                "low_bucket_label": q1_row["bucket_label"],
                "high_bucket_label": qn_row["bucket_label"],
                "low_trade_count": int(q1_row["trade_count"]),
                "high_trade_count": int(qn_row["trade_count"]),
                "low_avg_trade_return_pct": float(q1_row["avg_trade_return_pct"]),
                "high_avg_trade_return_pct": float(qn_row["avg_trade_return_pct"]),
                "delta_high_minus_low_avg_trade_return_pct": float(
                    qn_row["avg_trade_return_pct"] - q1_row["avg_trade_return_pct"]
                ),
                "low_severe_loss_rate_pct": float(q1_row["severe_loss_rate_pct"]),
                "high_severe_loss_rate_pct": float(qn_row["severe_loss_rate_pct"]),
                "delta_high_minus_low_severe_loss_rate_pct": float(
                    qn_row["severe_loss_rate_pct"] - q1_row["severe_loss_rate_pct"]
                ),
            }
        )
    return _sort_factor_table(_table_with_columns(rows, _FACTOR_CONTRAST_COLUMNS))


def _build_tail_profile_df(
    *,
    frame: pd.DataFrame,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload, group_frame in _iter_window_market_groups(frame):
        returns = pd.to_numeric(group_frame["trade_return_pct"], errors="coerce")
        right_tail_threshold = returns.quantile(0.90) if returns.notna().any() else np.nan
        cohorts: tuple[tuple[str, pd.Series], ...] = (
            ("all", pd.Series(True, index=group_frame.index, dtype=bool)),
            ("severe_loss", returns <= severe_loss_threshold_pct),
            ("right_tail_p90", returns >= right_tail_threshold),
            ("non_severe", returns > severe_loss_threshold_pct),
        )
        for cohort_name, mask in cohorts:
            cohort = group_frame[mask.fillna(False)].copy()
            metrics = _build_trade_metrics(
                cohort,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    **payload,
                    "tail_cohort": cohort_name,
                    "right_tail_threshold_pct": _finite_or_nan(right_tail_threshold),
                    **metrics,
                    **_feature_medians(cohort),
                }
            )
    return _sort_factor_table(_table_with_columns(rows, _TAIL_PROFILE_COLUMNS))


def _build_action_candidate_summary_df(
    *,
    frame: pd.DataFrame,
    severe_loss_threshold_pct: float,
    size_haircut: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload, group_frame in _iter_window_market_groups(frame):
        candidates = _action_candidate_masks(group_frame)
        for candidate_name, action_type, description, mask in candidates:
            mask = mask.fillna(False).astype(bool)
            selected = group_frame[mask].copy()
            kept = group_frame[~mask].copy() if action_type in {"exclude", "haircut"} else selected
            selected_metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            kept_metrics = _build_trade_metrics(
                kept,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            haircut_metrics = (
                _haircut_metrics(
                    group_frame,
                    mask=mask,
                    size_haircut=size_haircut,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                if action_type in {"exclude", "haircut"}
                else _empty_haircut_metrics()
            )
            rows.append(
                {
                    **payload,
                    "candidate_name": candidate_name,
                    "action_type": action_type,
                    "candidate_description": description,
                    "selected_trade_count": selected_metrics["trade_count"],
                    "selected_coverage_pct": _coverage(
                        selected_metrics["trade_count"],
                        len(group_frame),
                    ),
                    "selected_avg_trade_return_pct": selected_metrics["avg_trade_return_pct"],
                    "selected_median_trade_return_pct": selected_metrics[
                        "median_trade_return_pct"
                    ],
                    "selected_severe_loss_rate_pct": selected_metrics["severe_loss_rate_pct"],
                    "kept_trade_count": kept_metrics["trade_count"],
                    "kept_avg_trade_return_pct": kept_metrics["avg_trade_return_pct"],
                    "kept_median_trade_return_pct": kept_metrics["median_trade_return_pct"],
                    "kept_severe_loss_rate_pct": kept_metrics["severe_loss_rate_pct"],
                    "kept_worst_trade_return_pct": kept_metrics["worst_trade_return_pct"],
                    "haircut_size_multiplier": size_haircut,
                    **haircut_metrics,
                }
            )
    return _sort_factor_table(_table_with_columns(rows, _ACTION_CANDIDATE_COLUMNS))


def _action_candidate_masks(
    frame: pd.DataFrame,
) -> tuple[tuple[str, Literal["baseline", "keep", "exclude", "haircut"], str, pd.Series], ...]:
    baseline = pd.Series(True, index=frame.index, dtype=bool)
    overheat_v3 = _overheat_overlap_mask(
        frame,
        thresholds=_dynamic_q80_thresholds(frame),
    )
    overheat_old = _overheat_overlap_mask(
        frame,
        thresholds={
            "stock_return_60d_pct": 58.78,
            "stock_return_20d_pct": 33.71,
            "risk_adjusted_return_value": 3.886,
        },
    )
    return (
        ("baseline_all", "baseline", "No action; keep all trades.", baseline),
        (
            "exclude_overheated_v3_q80_overlap_ge2",
            "exclude",
            "Exclude trades where at least two of stock 20d return, stock 60d return, and risk-adjusted return are in the group top quintile.",
            overheat_v3,
        ),
        (
            "haircut_overheated_v3_q80_overlap_ge2",
            "haircut",
            "Apply a trade-level size haircut to the same v3 overheat-overlap trades.",
            overheat_v3,
        ),
        (
            "exclude_overheated_old_threshold_overlap_ge2",
            "exclude",
            "Exclude trades matching the 2026-04-24 overheat thresholds on at least two axes.",
            overheat_old,
        ),
        (
            "keep_low_pbr_q1",
            "keep",
            "Keep the lowest-third PBR trades inside the same window and market scope.",
            _low_quantile_mask(frame, "pbr", q=1.0 / 3.0),
        ),
        (
            "keep_low_forward_per_q1",
            "keep",
            "Keep the lowest-third forward PER trades inside the same window and market scope.",
            _low_quantile_mask(frame, "forward_per", q=1.0 / 3.0),
        ),
        (
            "keep_high_forward_eps_margin_q5",
            "keep",
            "Keep the highest-quintile forward EPS growth margin trades.",
            _high_quantile_mask(frame, "forward_eps_growth_margin", q=0.80),
        ),
        (
            "keep_topix_20d_positive",
            "keep",
            "Keep trades when TOPIX 20-day return is positive.",
            pd.to_numeric(frame["topix_return_20d_pct"], errors="coerce") > 0.0,
        ),
    )


def _haircut_metrics(
    frame: pd.DataFrame,
    *,
    mask: pd.Series,
    size_haircut: float,
    severe_loss_threshold_pct: float,
) -> dict[str, Any]:
    adjusted = frame.copy()
    returns = pd.to_numeric(adjusted["trade_return_pct"], errors="coerce")
    adjusted["trade_return_pct"] = returns.where(~mask, returns * size_haircut)
    metrics = _build_trade_metrics(
        adjusted,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    return {
        "haircut_avg_trade_return_pct": metrics["avg_trade_return_pct"],
        "haircut_median_trade_return_pct": metrics["median_trade_return_pct"],
        "haircut_severe_loss_rate_pct": metrics["severe_loss_rate_pct"],
        "haircut_worst_trade_return_pct": metrics["worst_trade_return_pct"],
    }


def _empty_haircut_metrics() -> dict[str, float]:
    return {
        "haircut_avg_trade_return_pct": np.nan,
        "haircut_median_trade_return_pct": np.nan,
        "haircut_severe_loss_rate_pct": np.nan,
        "haircut_worst_trade_return_pct": np.nan,
    }


def _dynamic_q80_thresholds(frame: pd.DataFrame) -> dict[str, float]:
    return {
        column: _finite_or_nan(pd.to_numeric(frame[column], errors="coerce").quantile(0.80))
        for column in (
            "stock_return_60d_pct",
            "stock_return_20d_pct",
            "risk_adjusted_return_value",
        )
    }


def _overheat_overlap_mask(frame: pd.DataFrame, *, thresholds: dict[str, float]) -> pd.Series:
    count = pd.Series(0, index=frame.index, dtype=int)
    for column, threshold in thresholds.items():
        if pd.isna(threshold):
            continue
        count += (pd.to_numeric(frame[column], errors="coerce") >= threshold).astype(int)
    return count >= 2


def _low_quantile_mask(frame: pd.DataFrame, column: str, *, q: float) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    threshold = values.quantile(q)
    return values <= threshold


def _high_quantile_mask(frame: pd.DataFrame, column: str, *, q: float) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    threshold = values.quantile(q)
    return values >= threshold


def _feature_medians(frame: pd.DataFrame) -> dict[str, float]:
    return {
        f"median_{spec.name}": _finite_or_nan(
            pd.to_numeric(frame[spec.name], errors="coerce").median()
        )
        for spec in _FACTOR_SPECS
        if spec.name in frame.columns
    }


def _iter_window_market_groups(
    frame: pd.DataFrame,
) -> list[tuple[dict[str, Any], pd.DataFrame]]:
    rows: list[tuple[dict[str, Any], pd.DataFrame]] = []
    if frame.empty:
        return rows
    for window_label, window_frame in frame.groupby("window_label", dropna=False, sort=False):
        rows.append(
            (
                {"window_label": str(window_label), "market_scope": "all"},
                window_frame.copy(),
            )
        )
        for market_scope, market_frame in window_frame.groupby(
            "market_scope",
            dropna=False,
            sort=False,
        ):
            rows.append(
                (
                    {
                        "window_label": str(window_label),
                        "market_scope": str(market_scope),
                    },
                    market_frame.copy(),
                )
            )
    return rows


def _coverage(count: int, total: int) -> float:
    return float(count / total * 100.0) if total > 0 else np.nan


def _coerce_int(value: Any) -> int:
    if pd.isna(value):
        return 0
    return int(value)


def _finite_or_nan(value: Any) -> float:
    if pd.isna(value):
        return np.nan
    return float(value)


def _sort_factor_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sort_columns = [
        column
        for column in (
            "window_label",
            "market_scope",
            "factor_family",
            "feature_name",
            "bucket_rank",
            "tail_cohort",
            "action_type",
            "candidate_name",
        )
        if column in frame.columns
    ]
    if not sort_columns:
        return frame.reset_index(drop=True)
    return frame.sort_values(sort_columns, kind="stable").reset_index(drop=True)


def _table_with_columns(
    rows: list[dict[str, Any]],
    columns: tuple[str, ...],
) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(columns))


def _build_summary_markdown(result: ForwardEpsDrivenV3FactorDecompositionResult) -> str:
    scenario = result.scenario_summary_df
    full = _first_row(scenario[scenario["window_label"] == "full"])
    holdout = _first_row(scenario[scenario["window_label"] == f"holdout_{result.holdout_months}m"])
    market_scope = result.market_scope_summary_df
    full_all = _first_row(
        market_scope[
            (market_scope["window_label"] == "full") & (market_scope["market_scope"] == "all")
        ]
    )
    holdout_all = _first_row(
        market_scope[
            (market_scope["window_label"] == f"holdout_{result.holdout_months}m")
            & (market_scope["market_scope"] == "all")
        ]
    )
    action = result.action_candidate_summary_df
    full_overheat = _first_row(
        action[
            (action["window_label"] == "full")
            & (action["market_scope"] == "all")
            & (action["candidate_name"] == "exclude_overheated_v3_q80_overlap_ge2")
        ]
    )
    standard_low_fper = _first_row(
        action[
            (action["window_label"] == "full")
            & (action["market_scope"] == "standard")
            & (action["candidate_name"] == "keep_low_forward_per_q1")
        ]
    )
    lines = [
        "# Forward EPS Driven V3 Factor Decomposition",
        "",
        "## Scope",
        "",
        f"- Strategy: `{result.strategy_name}`",
        f"- Dataset / universe preset: `{result.dataset_name}`",
        f"- Analysis period: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Holdout: `{result.holdout_months}` months",
        f"- Severe loss threshold: `{result.severe_loss_threshold_pct}%`",
        "",
        "## Key Reads",
        "",
        f"- Full-history trades: `{_fmt_int(full.get('trade_count'))}`, avg `{_fmt_pct(full.get('avg_trade_return_pct'))}`, severe loss `{_fmt_pct(full_all.get('severe_loss_rate_pct'))}`.",
        f"- Holdout trades: `{_fmt_int(holdout.get('trade_count'))}`, avg `{_fmt_pct(holdout.get('avg_trade_return_pct'))}`, severe loss `{_fmt_pct(holdout_all.get('severe_loss_rate_pct'))}`.",
        f"- V3 overheat exclude kept avg: `{_fmt_pct(full_overheat.get('kept_avg_trade_return_pct'))}`, kept severe loss `{_fmt_pct(full_overheat.get('kept_severe_loss_rate_pct'))}`.",
        f"- Standard low forward PER full-history selected avg: `{_fmt_pct(standard_low_fper.get('selected_avg_trade_return_pct'))}` with selected trades `{_fmt_int(standard_low_fper.get('selected_trade_count'))}`.",
        "",
        "## Artifact Tables",
        "",
        "- `dataset_summary_df`",
        "- `scenario_summary_df`",
        "- `market_scope_summary_df`",
        "- `factor_bucket_summary_df`",
        "- `factor_contrast_summary_df`",
        "- `tail_profile_df`",
        "- `action_candidate_summary_df`",
        "- `enriched_trade_df`",
    ]
    return "\n".join(lines)


def _build_published_summary(
    result: ForwardEpsDrivenV3FactorDecompositionResult,
) -> dict[str, Any]:
    return {
        "strategyName": result.strategy_name,
        "datasetName": result.dataset_name,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "holdoutMonths": result.holdout_months,
        "tradeCount": int(result.enriched_trade_df.shape[0]),
        "scenarioSummary": result.scenario_summary_df.to_dict("records"),
        "marketScopeSummary": result.market_scope_summary_df.to_dict("records"),
        "topActionCandidates": result.action_candidate_summary_df.head(30).to_dict(
            "records"
        ),
    }


def _first_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return dict(frame.iloc[0].to_dict())


def _fmt_int(value: Any) -> str:
    if pd.isna(value):
        return "n/a"
    return str(int(value))


def _fmt_pct(value: Any) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):+.2f}%"


__all__ = [
    "DEFAULT_DATASET_NAME",
    "DEFAULT_HOLDOUT_MONTHS",
    "DEFAULT_QUANTILE_BUCKET_COUNT",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "DEFAULT_SIZE_HAIRCUT",
    "DEFAULT_STRATEGY_NAME",
    "FORWARD_EPS_DRIVEN_V3_FACTOR_DECOMPOSITION_EXPERIMENT_ID",
    "ForwardEpsDrivenV3FactorDecompositionResult",
    "get_forward_eps_driven_v3_factor_decomposition_bundle_path_for_run_id",
    "get_forward_eps_driven_v3_factor_decomposition_latest_bundle_path",
    "load_forward_eps_driven_v3_factor_decomposition_bundle",
    "run_forward_eps_driven_v3_factor_decomposition",
    "write_forward_eps_driven_v3_factor_decomposition_bundle",
]
