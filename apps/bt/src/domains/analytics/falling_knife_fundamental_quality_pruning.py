"""Fundamental-quality decomposition for falling-knife bad tails."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics.falling_knife_reversal_study import (
    get_falling_knife_reversal_study_latest_bundle_path,
    load_falling_knife_reversal_study_bundle,
)
from src.domains.analytics.readonly_duckdb_support import (
    normalize_code_sql,
    open_readonly_analysis_connection,
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

FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID = (
    "market-behavior/falling-knife-fundamental-quality-pruning"
)
DEFAULT_HORIZON_DAYS = 20
DEFAULT_SEVERE_LOSS_THRESHOLD = -0.10
DEFAULT_MIN_QUALITY_SCORE = 3
_ENRICHED_COLUMNS: tuple[str, ...] = (
    "event_id",
    "signal_date",
    "code",
    "market_name",
    "risk_adjusted_bucket",
    "condition_count",
    "catch_return",
    "disclosed_date",
    "period_type",
    "document_type",
    "eps",
    "forecast_eps",
    "profit",
    "sales",
    "operating_cash_flow",
    "investing_cash_flow",
    "equity",
    "total_assets",
    "cfo_margin_pct",
    "simple_fcf_margin_pct",
    "cfo_to_profit_ratio",
    "equity_ratio_pct",
    "forecast_eps_sign",
    "profit_sign",
    "cfo_sign",
    "fcf_sign",
    "equity_ratio_bucket",
    "quality_score",
    "quality_bucket",
)
_SEGMENT_COLUMNS: tuple[str, ...] = (
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
_RULE_COLUMNS: tuple[str, ...] = (
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


@dataclass(frozen=True)
class FallingKnifeFundamentalQualityPruningResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizon_days: int
    severe_loss_threshold: float
    min_quality_score: int
    baseline_count: int
    statement_coverage_pct: float | None
    research_note: str
    enriched_event_df: pd.DataFrame
    quality_segment_summary_df: pd.DataFrame
    quality_rule_summary_df: pd.DataFrame


@dataclass(frozen=True)
class _RuleSpec:
    name: str
    label: str
    predicate: Callable[[pd.DataFrame], pd.Series]


def _empty_df(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _return_column(horizon_days: int) -> str:
    if horizon_days < 1:
        raise ValueError("horizon_days must be a positive integer")
    return f"catch_return_{int(horizon_days)}d"


def _fmt(value: object, digits: int = 2) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _ratio(numerator: object, denominator: object, *, multiplier: float = 1.0) -> float | None:
    try:
        num = float(numerator)  # type: ignore[arg-type]
        den = float(denominator)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num) or not math.isfinite(den) or math.isclose(den, 0.0):
        return None
    return num / den * multiplier


def _sign_bucket(value: object, *, positive_label: str, non_positive_label: str) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "missing"
    if not math.isfinite(number):
        return "missing"
    return positive_label if number > 0.0 else non_positive_label


def _equity_ratio_bucket(value: object) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "missing"
    if not math.isfinite(number):
        return "missing"
    if number >= 50.0:
        return ">=50%"
    if number >= 30.0:
        return "30-50%"
    if number >= 10.0:
        return "10-30%"
    return "<10%"


def _summary_stats(
    frame: pd.DataFrame,
    *,
    severe_loss_threshold: float,
) -> dict[str, float | int]:
    returns = pd.to_numeric(frame["catch_return"], errors="coerce").dropna()
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


def _query_latest_statement_features(
    db_path: str,
    event_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_df(
            (
                "event_id",
                "disclosed_date",
                "period_type",
                "document_type",
                "eps",
                "forecast_eps",
                "profit",
                "sales",
                "operating_cash_flow",
                "investing_cash_flow",
                "equity",
                "total_assets",
            )
        )
    event_key_df = event_df[["event_id", "code", "signal_date"]].copy()
    event_key_df["event_id"] = pd.to_numeric(event_key_df["event_id"], errors="raise").astype(int)
    event_key_df["code"] = event_key_df["code"].astype(str)
    event_key_df["signal_date"] = event_key_df["signal_date"].astype(str)
    normalized_code_sql = normalize_code_sql("s.code")
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="falling-knife-fundamental-quality-",
    ) as ctx:
        ctx.connection.register("_event_keys", event_key_df)
        try:
            return ctx.connection.execute(
                f"""
                WITH statement_candidates AS (
                    SELECT
                        e.event_id,
                        s.disclosed_date,
                        s.type_of_current_period AS period_type,
                        s.type_of_document AS document_type,
                        CAST(s.earnings_per_share AS DOUBLE) AS eps,
                        CAST(COALESCE(
                            s.next_year_forecast_earnings_per_share,
                            s.forecast_eps
                        ) AS DOUBLE) AS forecast_eps,
                        CAST(s.profit AS DOUBLE) AS profit,
                        CAST(s.sales AS DOUBLE) AS sales,
                        CAST(s.operating_cash_flow AS DOUBLE) AS operating_cash_flow,
                        CAST(s.investing_cash_flow AS DOUBLE) AS investing_cash_flow,
                        CAST(s.equity AS DOUBLE) AS equity,
                        CAST(s.total_assets AS DOUBLE) AS total_assets,
                        ROW_NUMBER() OVER (
                            PARTITION BY e.event_id
                            ORDER BY s.disclosed_date DESC, s.type_of_current_period DESC
                        ) AS row_priority
                    FROM _event_keys e
                    JOIN statements s
                        ON {normalized_code_sql} = e.code
                       AND s.disclosed_date <= e.signal_date
                )
                SELECT
                    event_id,
                    disclosed_date,
                    period_type,
                    document_type,
                    eps,
                    forecast_eps,
                    profit,
                    sales,
                    operating_cash_flow,
                    investing_cash_flow,
                    equity,
                    total_assets
                FROM statement_candidates
                WHERE row_priority = 1
                ORDER BY event_id
                """
            ).fetchdf()
        finally:
            ctx.connection.unregister("_event_keys")


def _build_enriched_event_df(
    event_df: pd.DataFrame,
    *,
    db_path: str,
    horizon_days: int,
    min_quality_score: int,
) -> pd.DataFrame:
    return_column = _return_column(horizon_days)
    if event_df.empty or return_column not in event_df.columns:
        return _empty_df(_ENRICHED_COLUMNS)
    base_df = event_df[
        pd.to_numeric(event_df[return_column], errors="coerce").notna()
    ].copy()
    base_df = base_df.reset_index(drop=True)
    base_df["event_id"] = np.arange(len(base_df), dtype=int)
    base_df["catch_return"] = pd.to_numeric(base_df[return_column], errors="coerce")
    statement_df = _query_latest_statement_features(db_path, base_df)
    enriched = base_df.merge(statement_df, on="event_id", how="left")
    enriched["cfo_margin_pct"] = [
        _ratio(cfo, sales, multiplier=100.0)
        for cfo, sales in zip(
            enriched["operating_cash_flow"],
            enriched["sales"],
            strict=False,
        )
    ]
    enriched["simple_fcf_margin_pct"] = [
        _ratio(
            (0.0 if pd.isna(cfo) else float(cfo))
            + (0.0 if pd.isna(icf) else float(icf)),
            sales,
            multiplier=100.0,
        )
        if not (pd.isna(cfo) and pd.isna(icf))
        else None
        for cfo, icf, sales in zip(
            enriched["operating_cash_flow"],
            enriched["investing_cash_flow"],
            enriched["sales"],
            strict=False,
        )
    ]
    enriched["cfo_to_profit_ratio"] = [
        _ratio(cfo, profit)
        for cfo, profit in zip(
            enriched["operating_cash_flow"],
            enriched["profit"],
            strict=False,
        )
    ]
    enriched["equity_ratio_pct"] = [
        _ratio(equity, assets, multiplier=100.0)
        for equity, assets in zip(
            enriched["equity"],
            enriched["total_assets"],
            strict=False,
        )
    ]
    enriched["forecast_eps_sign"] = [
        _sign_bucket(value, positive_label="forecast_positive", non_positive_label="forecast_non_positive")
        for value in enriched["forecast_eps"]
    ]
    enriched["profit_sign"] = [
        _sign_bucket(value, positive_label="profit_positive", non_positive_label="profit_non_positive")
        for value in enriched["profit"]
    ]
    enriched["cfo_sign"] = [
        _sign_bucket(value, positive_label="cfo_positive", non_positive_label="cfo_non_positive")
        for value in enriched["operating_cash_flow"]
    ]
    enriched["fcf_sign"] = [
        _sign_bucket(value, positive_label="fcf_positive", non_positive_label="fcf_non_positive")
        for value in enriched["simple_fcf_margin_pct"]
    ]
    enriched["equity_ratio_bucket"] = [
        _equity_ratio_bucket(value) for value in enriched["equity_ratio_pct"]
    ]
    quality_flags = pd.DataFrame(
        {
            "forecast": enriched["forecast_eps_sign"] == "forecast_positive",
            "profit": enriched["profit_sign"] == "profit_positive",
            "cfo": enriched["cfo_sign"] == "cfo_positive",
            "fcf": enriched["fcf_sign"] == "fcf_positive",
            "equity": pd.to_numeric(enriched["equity_ratio_pct"], errors="coerce") >= 30.0,
        }
    )
    enriched["quality_score"] = quality_flags.fillna(False).sum(axis=1).astype(int)
    enriched["quality_bucket"] = np.where(
        enriched["disclosed_date"].isna(),
        "missing_statement",
        np.where(
            enriched["quality_score"] >= min_quality_score,
            "high_quality",
            "low_quality",
        ),
    )
    for column in _ENRICHED_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = np.nan
    return enriched[list(_ENRICHED_COLUMNS)].sort_values(
        ["signal_date", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _build_segment_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    horizon_days: int,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    if enriched_event_df.empty:
        return _empty_df(_SEGMENT_COLUMNS)
    rows: list[dict[str, object]] = []
    segment_columns = (
        "market_name",
        "quality_bucket",
        "forecast_eps_sign",
        "profit_sign",
        "cfo_sign",
        "fcf_sign",
        "equity_ratio_bucket",
        "risk_adjusted_bucket",
    )
    for segment_name in segment_columns:
        for value, group in enriched_event_df.groupby(segment_name, dropna=False, sort=False):
            stats = _summary_stats(group, severe_loss_threshold=severe_loss_threshold)
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
    for keys, group in enriched_event_df.groupby(
        ["market_name", "quality_bucket"],
        dropna=False,
        sort=False,
    ):
        market_name, quality_bucket = keys
        stats = _summary_stats(group, severe_loss_threshold=severe_loss_threshold)
        rows.append(
            {
                "segment_name": "market_x_quality",
                "segment_value": f"{market_name}__{quality_bucket}",
                "horizon_days": horizon_days,
                "sample_count": stats["count"],
                "mean_return_pct": stats["mean_pct"],
                "median_return_pct": stats["median_pct"],
                "p10_return_pct": stats["p10_pct"],
                "p90_return_pct": stats["p90_pct"],
                "severe_loss_rate_pct": stats["severe_loss_rate_pct"],
            }
        )
    return pd.DataFrame(rows, columns=list(_SEGMENT_COLUMNS)).sort_values(
        ["segment_name", "sample_count"],
        ascending=[True, False],
        kind="stable",
    ).reset_index(drop=True)


def _build_rule_specs(min_quality_score: int) -> tuple[_RuleSpec, ...]:
    return (
        _RuleSpec(
            "exclude_low_quality",
            f"Exclude quality_score < {min_quality_score}",
            lambda df: df["quality_bucket"].astype(str) == "low_quality",
        ),
        _RuleSpec(
            "exclude_missing_or_low_quality",
            f"Exclude missing statement or quality_score < {min_quality_score}",
            lambda df: df["quality_bucket"].astype(str).isin(
                {"missing_statement", "low_quality"}
            ),
        ),
        _RuleSpec(
            "exclude_growth_low_quality",
            "Exclude Growth low quality",
            lambda df: (df["market_name"].astype(str) == "グロース")
            & (df["quality_bucket"].astype(str) == "low_quality"),
        ),
        _RuleSpec(
            "exclude_growth_missing_or_low_quality",
            "Exclude Growth missing/low quality",
            lambda df: (df["market_name"].astype(str) == "グロース")
            & df["quality_bucket"].astype(str).isin(
                {"missing_statement", "low_quality"}
            ),
        ),
        _RuleSpec(
            "exclude_q5_rar_low_quality",
            "Exclude Daily RAR Q5 highest low quality",
            lambda df: (df["risk_adjusted_bucket"].astype(str) == "Q5_highest")
            & (df["quality_bucket"].astype(str) == "low_quality"),
        ),
        _RuleSpec(
            "exclude_growth_or_q5_rar_low_quality",
            "Exclude Growth or Daily RAR Q5 highest when low quality",
            lambda df: (
                (df["market_name"].astype(str) == "グロース")
                | (df["risk_adjusted_bucket"].astype(str) == "Q5_highest")
            )
            & (df["quality_bucket"].astype(str) == "low_quality"),
        ),
        _RuleSpec(
            "exclude_profit_non_positive",
            "Exclude profit non-positive",
            lambda df: df["profit_sign"].astype(str) == "profit_non_positive",
        ),
        _RuleSpec(
            "exclude_cfo_non_positive",
            "Exclude operating CF non-positive",
            lambda df: df["cfo_sign"].astype(str) == "cfo_non_positive",
        ),
        _RuleSpec(
            "exclude_forecast_non_positive",
            "Exclude forecast EPS non-positive",
            lambda df: df["forecast_eps_sign"].astype(str) == "forecast_non_positive",
        ),
        _RuleSpec(
            "exclude_fcf_non_positive",
            "Exclude simple FCF non-positive",
            lambda df: df["fcf_sign"].astype(str) == "fcf_non_positive",
        ),
    )


def _build_rule_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    horizon_days: int,
    severe_loss_threshold: float,
    min_quality_score: int,
) -> pd.DataFrame:
    if enriched_event_df.empty:
        return _empty_df(_RULE_COLUMNS)
    baseline = enriched_event_df.copy()
    baseline_stats = _summary_stats(
        baseline,
        severe_loss_threshold=severe_loss_threshold,
    )
    baseline_count = int(baseline_stats["count"])
    baseline_severe_count = int(baseline_stats["severe_loss_count"])
    rows: list[dict[str, object]] = []
    for rule in _build_rule_specs(min_quality_score):
        remove_mask = rule.predicate(baseline).fillna(False).astype(bool)
        removed = baseline[remove_mask]
        kept = baseline[~remove_mask]
        kept_stats = _summary_stats(kept, severe_loss_threshold=severe_loss_threshold)
        removed_stats = _summary_stats(removed, severe_loss_threshold=severe_loss_threshold)
        kept_count = int(kept_stats["count"])
        removed_count = int(removed_stats["count"])
        baseline_severe_rate = float(baseline_stats["severe_loss_rate_pct"])
        kept_severe_rate = float(kept_stats["severe_loss_rate_pct"])
        rows.append(
            {
                "rule_name": rule.name,
                "rule_label": rule.label,
                "horizon_days": horizon_days,
                "baseline_count": baseline_count,
                "kept_count": kept_count,
                "removed_count": removed_count,
                "kept_fraction_pct": kept_count / baseline_count * 100.0
                if baseline_count
                else math.nan,
                "removed_fraction_pct": removed_count / baseline_count * 100.0
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
        )
    return pd.DataFrame(rows, columns=list(_RULE_COLUMNS)).sort_values(
        ["severe_loss_rate_reduction_pct", "mean_return_cost_pct"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def run_falling_knife_fundamental_quality_pruning(
    input_bundle: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    severe_loss_threshold: float = DEFAULT_SEVERE_LOSS_THRESHOLD,
    min_quality_score: int = DEFAULT_MIN_QUALITY_SCORE,
) -> FallingKnifeFundamentalQualityPruningResult:
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
    enriched_event_df = _build_enriched_event_df(
        input_result.event_df,
        db_path=input_result.db_path,
        horizon_days=horizon_days,
        min_quality_score=min_quality_score,
    )
    segment_summary_df = _build_segment_summary_df(
        enriched_event_df,
        horizon_days=horizon_days,
        severe_loss_threshold=severe_loss_threshold,
    )
    rule_summary_df = _build_rule_summary_df(
        enriched_event_df,
        horizon_days=horizon_days,
        severe_loss_threshold=severe_loss_threshold,
        min_quality_score=min_quality_score,
    )
    baseline_count = (
        int(rule_summary_df["baseline_count"].iloc[0])
        if not rule_summary_df.empty
        else len(enriched_event_df)
    )
    statement_coverage_pct = (
        float(enriched_event_df["disclosed_date"].notna().mean() * 100.0)
        if not enriched_event_df.empty
        else None
    )
    research_note = (
        "This study decomposes the Growth-market bad-tail proxy into PIT-safe "
        "fundamental quality buckets using the latest statement disclosed on or "
        "before each falling-knife signal date."
    )
    return FallingKnifeFundamentalQualityPruningResult(
        db_path=input_result.db_path,
        input_bundle_path=str(bundle_path),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_result.analysis_start_date,
        analysis_end_date=input_result.analysis_end_date,
        horizon_days=int(horizon_days),
        severe_loss_threshold=float(severe_loss_threshold),
        min_quality_score=int(min_quality_score),
        baseline_count=baseline_count,
        statement_coverage_pct=statement_coverage_pct,
        research_note=research_note,
        enriched_event_df=enriched_event_df,
        quality_segment_summary_df=segment_summary_df,
        quality_rule_summary_df=rule_summary_df,
    )


def write_falling_knife_fundamental_quality_pruning_bundle(
    result: FallingKnifeFundamentalQualityPruningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID,
        module=__name__,
        function="run_falling_knife_fundamental_quality_pruning",
        params={
            "input_bundle": result.input_bundle_path,
            "horizon_days": result.horizon_days,
            "severe_loss_threshold": result.severe_loss_threshold,
            "min_quality_score": result.min_quality_score,
        },
        result=result,
        table_field_names=(
            "enriched_event_df",
            "quality_segment_summary_df",
            "quality_rule_summary_df",
        ),
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_falling_knife_fundamental_quality_pruning_bundle(
    bundle_path: str | Path,
) -> FallingKnifeFundamentalQualityPruningResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FallingKnifeFundamentalQualityPruningResult,
        table_field_names=(
            "enriched_event_df",
            "quality_segment_summary_df",
            "quality_rule_summary_df",
        ),
    )


def get_falling_knife_fundamental_quality_pruning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_falling_knife_fundamental_quality_pruning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: FallingKnifeFundamentalQualityPruningResult,
) -> str:
    return "\n".join(
        [
            "# Falling Knife Fundamental Quality Pruning",
            "",
            "## Snapshot",
            "",
            f"- Input bundle: `{result.input_bundle_path}`",
            f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
            f"- Horizon: `{result.horizon_days}` sessions",
            f"- Severe loss threshold: `{_fmt(result.severe_loss_threshold * 100.0)}%`",
            f"- Baseline trades: `{result.baseline_count}`",
            f"- Statement coverage: `{_fmt(result.statement_coverage_pct)}%`",
            f"- High quality threshold: `quality_score >= {result.min_quality_score}`",
            "",
            "## Top Quality Rules",
            "",
            *_top_rows(result.quality_rule_summary_df, limit=12),
            "",
            "## Tables",
            "",
            "- `enriched_event_df`",
            "- `quality_segment_summary_df`",
            "- `quality_rule_summary_df`",
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
    result: FallingKnifeFundamentalQualityPruningResult,
) -> dict[str, Any]:
    return {
        "experimentId": FALLING_KNIFE_FUNDAMENTAL_QUALITY_PRUNING_EXPERIMENT_ID,
        "inputBundlePath": result.input_bundle_path,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "horizonDays": result.horizon_days,
        "severeLossThreshold": result.severe_loss_threshold,
        "minQualityScore": result.min_quality_score,
        "baselineCount": result.baseline_count,
        "statementCoveragePct": result.statement_coverage_pct,
        "topRules": result.quality_rule_summary_df.head(20).to_dict(orient="records")
        if not result.quality_rule_summary_df.empty
        else [],
        "note": result.research_note,
    }
