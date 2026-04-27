"""Bad-tail pruning research for falling-knife rebound events."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.analytics.falling_knife_reversal_study import (
    get_falling_knife_reversal_study_latest_bundle_path,
    load_falling_knife_reversal_study_bundle,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    load_research_bundle_info,
    resolve_required_bundle_path,
    write_dataclass_research_bundle,
)

FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID = (
    "market-behavior/falling-knife-bad-tail-pruning"
)
DEFAULT_HORIZON_DAYS = 20
DEFAULT_SEVERE_LOSS_THRESHOLD = -0.10
_RULE_SUMMARY_COLUMNS: tuple[str, ...] = (
    "rule_name",
    "rule_label",
    "horizon_days",
    "baseline_count",
    "kept_count",
    "removed_count",
    "kept_fraction_pct",
    "removed_fraction_pct",
    "baseline_mean_pct",
    "kept_mean_pct",
    "removed_mean_pct",
    "baseline_median_pct",
    "kept_median_pct",
    "removed_median_pct",
    "baseline_p10_pct",
    "kept_p10_pct",
    "removed_p10_pct",
    "baseline_p90_pct",
    "kept_p90_pct",
    "removed_p90_pct",
    "baseline_severe_loss_rate_pct",
    "kept_severe_loss_rate_pct",
    "removed_severe_loss_rate_pct",
    "severe_loss_rate_reduction_pct",
    "mean_return_cost_pct",
    "removed_severe_loss_share_pct",
)
_SEGMENT_SUMMARY_COLUMNS: tuple[str, ...] = (
    "segment_name",
    "segment_value",
    "horizon_days",
    "sample_count",
    "mean_return_pct",
    "median_return_pct",
    "p10_return_pct",
    "p90_return_pct",
    "severe_loss_rate_pct",
)


@dataclass(frozen=True)
class FallingKnifeBadTailPruningResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizon_days: int
    severe_loss_threshold: float
    baseline_count: int
    research_note: str
    rule_summary_df: pd.DataFrame
    segment_summary_df: pd.DataFrame


@dataclass(frozen=True)
class _RuleSpec:
    name: str
    label: str
    predicate: Callable[[pd.DataFrame], pd.Series]


def _empty_df(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _fmt(value: object, digits: int = 2) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _return_column(horizon_days: int) -> str:
    if horizon_days < 1:
        raise ValueError("horizon_days must be a positive integer")
    return f"catch_return_{int(horizon_days)}d"


def _summary_stats(
    frame: pd.DataFrame,
    *,
    return_column: str,
    severe_loss_threshold: float,
) -> dict[str, float | int]:
    returns = pd.to_numeric(frame[return_column], errors="coerce").dropna()
    if returns.empty:
        return {
            "count": 0,
            "mean_pct": math.nan,
            "median_pct": math.nan,
            "p10_pct": math.nan,
            "p90_pct": math.nan,
            "severe_loss_rate_pct": math.nan,
            "severe_loss_count": 0,
        }
    severe_mask = returns <= severe_loss_threshold
    return {
        "count": int(len(returns)),
        "mean_pct": float(returns.mean() * 100.0),
        "median_pct": float(returns.median() * 100.0),
        "p10_pct": float(returns.quantile(0.10) * 100.0),
        "p90_pct": float(returns.quantile(0.90) * 100.0),
        "severe_loss_rate_pct": float(severe_mask.mean() * 100.0),
        "severe_loss_count": int(severe_mask.sum()),
    }


def _build_rule_specs() -> tuple[_RuleSpec, ...]:
    return (
        _RuleSpec(
            "exclude_growth",
            "Exclude Growth market",
            lambda df: df["market_name"].astype(str) == "グロース",
        ),
        _RuleSpec(
            "exclude_growth_or_q5_rar",
            "Exclude Growth or Daily RAR Q5 highest",
            lambda df: (df["market_name"].astype(str) == "グロース")
            | (df["risk_adjusted_bucket"].astype(str) == "Q5_highest"),
        ),
        _RuleSpec(
            "exclude_rar_q5_highest",
            "Exclude Daily RAR Q5 highest",
            lambda df: df["risk_adjusted_bucket"].astype(str) == "Q5_highest",
        ),
        _RuleSpec(
            "exclude_rar_q5_or_unbucketed",
            "Exclude Daily RAR Q5 highest or unbucketed",
            lambda df: df["risk_adjusted_bucket"].astype(str).isin(
                {"Q5_highest", "unbucketed"}
            ),
        ),
        _RuleSpec(
            "exclude_deep_60d_drawdown",
            "Exclude 60d high drawdown condition",
            lambda df: df["deep_60d_drawdown"].astype(bool),
        ),
        _RuleSpec(
            "exclude_deep_20d_drop",
            "Exclude 20d drop condition",
            lambda df: df["deep_20d_drop"].astype(bool),
        ),
        _RuleSpec(
            "exclude_sharp_5d_drop",
            "Exclude sharp 5d drop condition",
            lambda df: df["sharp_5d_drop"].astype(bool),
        ),
        _RuleSpec(
            "exclude_condition_count_ge3",
            "Exclude condition_count >= 3",
            lambda df: pd.to_numeric(df["condition_count"], errors="coerce") >= 3,
        ),
        _RuleSpec(
            "exclude_condition_count_ge4",
            "Exclude condition_count >= 4",
            lambda df: pd.to_numeric(df["condition_count"], errors="coerce") >= 4,
        ),
        _RuleSpec(
            "exclude_growth_and_q5_rar",
            "Exclude Growth with Daily RAR Q5 highest",
            lambda df: (df["market_name"].astype(str) == "グロース")
            & (df["risk_adjusted_bucket"].astype(str) == "Q5_highest"),
        ),
        _RuleSpec(
            "exclude_growth_and_deep_60d_drawdown",
            "Exclude Growth with 60d high drawdown",
            lambda df: (df["market_name"].astype(str) == "グロース")
            & df["deep_60d_drawdown"].astype(bool),
        ),
        _RuleSpec(
            "exclude_q5_rar_and_deep_60d_drawdown",
            "Exclude Daily RAR Q5 highest with 60d high drawdown",
            lambda df: (df["risk_adjusted_bucket"].astype(str) == "Q5_highest")
            & df["deep_60d_drawdown"].astype(bool),
        ),
    )


def _build_rule_summary_df(
    event_df: pd.DataFrame,
    *,
    horizon_days: int,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    return_column = _return_column(horizon_days)
    if event_df.empty or return_column not in event_df.columns:
        return _empty_df(_RULE_SUMMARY_COLUMNS)
    baseline = event_df[pd.to_numeric(event_df[return_column], errors="coerce").notna()]
    baseline_stats = _summary_stats(
        baseline,
        return_column=return_column,
        severe_loss_threshold=severe_loss_threshold,
    )
    baseline_count = int(baseline_stats["count"])
    baseline_severe_count = int(baseline_stats["severe_loss_count"])
    rows: list[dict[str, object]] = []
    for rule in _build_rule_specs():
        remove_mask = rule.predicate(baseline).fillna(False).astype(bool)
        removed = baseline[remove_mask]
        kept = baseline[~remove_mask]
        kept_stats = _summary_stats(
            kept,
            return_column=return_column,
            severe_loss_threshold=severe_loss_threshold,
        )
        removed_stats = _summary_stats(
            removed,
            return_column=return_column,
            severe_loss_threshold=severe_loss_threshold,
        )
        kept_count = int(kept_stats["count"])
        removed_count = int(removed_stats["count"])
        baseline_severe_rate = float(baseline_stats["severe_loss_rate_pct"])
        kept_severe_rate = float(kept_stats["severe_loss_rate_pct"])
        row = {
            "rule_name": rule.name,
            "rule_label": rule.label,
            "horizon_days": horizon_days,
            "baseline_count": baseline_count,
            "kept_count": kept_count,
            "removed_count": removed_count,
            "kept_fraction_pct": (kept_count / baseline_count * 100.0)
            if baseline_count
            else math.nan,
            "removed_fraction_pct": (removed_count / baseline_count * 100.0)
            if baseline_count
            else math.nan,
            "baseline_mean_pct": baseline_stats["mean_pct"],
            "kept_mean_pct": kept_stats["mean_pct"],
            "removed_mean_pct": removed_stats["mean_pct"],
            "baseline_median_pct": baseline_stats["median_pct"],
            "kept_median_pct": kept_stats["median_pct"],
            "removed_median_pct": removed_stats["median_pct"],
            "baseline_p10_pct": baseline_stats["p10_pct"],
            "kept_p10_pct": kept_stats["p10_pct"],
            "removed_p10_pct": removed_stats["p10_pct"],
            "baseline_p90_pct": baseline_stats["p90_pct"],
            "kept_p90_pct": kept_stats["p90_pct"],
            "removed_p90_pct": removed_stats["p90_pct"],
            "baseline_severe_loss_rate_pct": baseline_severe_rate,
            "kept_severe_loss_rate_pct": kept_severe_rate,
            "removed_severe_loss_rate_pct": removed_stats["severe_loss_rate_pct"],
            "severe_loss_rate_reduction_pct": baseline_severe_rate - kept_severe_rate,
            "mean_return_cost_pct": float(baseline_stats["mean_pct"])
            - float(kept_stats["mean_pct"]),
            "removed_severe_loss_share_pct": (
                int(removed_stats["severe_loss_count"]) / baseline_severe_count * 100.0
            )
            if baseline_severe_count
            else math.nan,
        }
        rows.append(row)
    return pd.DataFrame(rows, columns=list(_RULE_SUMMARY_COLUMNS)).sort_values(
        [
            "severe_loss_rate_reduction_pct",
            "mean_return_cost_pct",
            "removed_fraction_pct",
        ],
        ascending=[False, True, True],
        kind="stable",
    ).reset_index(drop=True)


def _build_segment_summary_df(
    event_df: pd.DataFrame,
    *,
    horizon_days: int,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    return_column = _return_column(horizon_days)
    if event_df.empty or return_column not in event_df.columns:
        return _empty_df(_SEGMENT_SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    for segment_name, column in (
        ("market_name", "market_name"),
        ("risk_adjusted_bucket", "risk_adjusted_bucket"),
        ("condition_count", "condition_count"),
    ):
        for value, group in event_df.groupby(column, dropna=False, sort=False):
            stats = _summary_stats(
                group,
                return_column=return_column,
                severe_loss_threshold=severe_loss_threshold,
            )
            rows.append(
                {
                    "segment_name": segment_name,
                    "segment_value": str(value),
                    "horizon_days": horizon_days,
                    "sample_count": stats["count"],
                    "mean_return_pct": stats["mean_pct"],
                    "median_return_pct": stats["median_pct"],
                    "p10_return_pct": stats["p10_pct"],
                    "p90_return_pct": stats["p90_pct"],
                    "severe_loss_rate_pct": stats["severe_loss_rate_pct"],
                }
            )
    return pd.DataFrame(rows, columns=list(_SEGMENT_SUMMARY_COLUMNS)).sort_values(
        ["segment_name", "sample_count"],
        ascending=[True, False],
        kind="stable",
    ).reset_index(drop=True)


def run_falling_knife_bad_tail_pruning(
    input_bundle: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    severe_loss_threshold: float = DEFAULT_SEVERE_LOSS_THRESHOLD,
) -> FallingKnifeBadTailPruningResult:
    bundle_path = resolve_required_bundle_path(
        input_bundle,
        latest_bundle_resolver=lambda: get_falling_knife_reversal_study_latest_bundle_path(
            output_root=output_root
        ),
        missing_message=(
            "No falling-knife reversal study bundle was found. Run "
            "run_falling_knife_reversal_study.py first or pass --input-bundle."
        ),
    )
    input_info = load_research_bundle_info(bundle_path)
    input_result = load_falling_knife_reversal_study_bundle(bundle_path)
    rule_summary_df = _build_rule_summary_df(
        input_result.event_df,
        horizon_days=horizon_days,
        severe_loss_threshold=severe_loss_threshold,
    )
    segment_summary_df = _build_segment_summary_df(
        input_result.event_df,
        horizon_days=horizon_days,
        severe_loss_threshold=severe_loss_threshold,
    )
    baseline_count = (
        int(rule_summary_df["baseline_count"].iloc[0])
        if not rule_summary_df.empty
        else 0
    )
    research_note = (
        "This follow-on study keeps the falling-knife rebound setup fixed and "
        "tests exclusion rules. The target is not to maximize average return, "
        "but to reduce severe-loss frequency while preserving median/right-tail "
        "rebound exposure."
    )
    return FallingKnifeBadTailPruningResult(
        db_path=input_result.db_path,
        input_bundle_path=str(bundle_path),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_result.analysis_start_date,
        analysis_end_date=input_result.analysis_end_date,
        horizon_days=int(horizon_days),
        severe_loss_threshold=float(severe_loss_threshold),
        baseline_count=baseline_count,
        research_note=research_note,
        rule_summary_df=rule_summary_df,
        segment_summary_df=segment_summary_df,
    )


def write_falling_knife_bad_tail_pruning_bundle(
    result: FallingKnifeBadTailPruningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID,
        module=__name__,
        function="run_falling_knife_bad_tail_pruning",
        params={
            "input_bundle": result.input_bundle_path,
            "horizon_days": result.horizon_days,
            "severe_loss_threshold": result.severe_loss_threshold,
        },
        result=result,
        table_field_names=("rule_summary_df", "segment_summary_df"),
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_falling_knife_bad_tail_pruning_bundle(
    bundle_path: str | Path,
) -> FallingKnifeBadTailPruningResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FallingKnifeBadTailPruningResult,
        table_field_names=("rule_summary_df", "segment_summary_df"),
    )


def get_falling_knife_bad_tail_pruning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_falling_knife_bad_tail_pruning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: FallingKnifeBadTailPruningResult,
) -> str:
    top_rows = _top_rows(result.rule_summary_df, limit=12)
    return "\n".join(
        [
            "# Falling Knife Bad-Tail Pruning",
            "",
            "## Snapshot",
            "",
            f"- Input bundle: `{result.input_bundle_path}`",
            f"- Input run id: `{result.input_run_id}`",
            f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
            f"- Horizon: `{result.horizon_days}` sessions",
            f"- Severe loss threshold: `{_fmt(result.severe_loss_threshold * 100.0)}%`",
            f"- Baseline trades: `{result.baseline_count}`",
            "",
            "## Top Rule Candidates",
            "",
            *top_rows,
            "",
            "## Tables",
            "",
            "- `rule_summary_df`",
            "- `segment_summary_df`",
        ]
    )


def _top_rows(frame: pd.DataFrame, *, limit: int) -> list[str]:
    if frame.empty:
        return ["- No rows."]
    rows: list[str] = []
    for row in frame.head(limit).to_dict(orient="records"):
        rows.append(
            "- "
            + ", ".join(
                [
                    f"`{key}`={_fmt(value) if isinstance(value, float) else value}"
                    for key, value in row.items()
                ]
            )
        )
    return rows


def _build_published_summary_payload(
    result: FallingKnifeBadTailPruningResult,
) -> dict[str, Any]:
    return {
        "experimentId": FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID,
        "inputBundlePath": result.input_bundle_path,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "horizonDays": result.horizon_days,
        "severeLossThreshold": result.severe_loss_threshold,
        "baselineCount": result.baseline_count,
        "topRules": result.rule_summary_df.head(20).to_dict(orient="records")
        if not result.rule_summary_df.empty
        else [],
        "note": result.research_note,
    }
