"""Fundamental profile of falling-knife events that fail to rebound."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.falling_knife_fundamental_quality_pruning import (
    DEFAULT_HORIZON_DAYS,
    DEFAULT_MIN_QUALITY_SCORE,
    DEFAULT_SEVERE_LOSS_THRESHOLD,
    _build_enriched_event_df,
    _fmt,
)
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

FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID = (
    "market-behavior/falling-knife-non-rebound-fundamental-profile"
)
DEFAULT_REBOUND_THRESHOLD = 0.0

_PROFILE_COLUMNS: tuple[str, ...] = (
    "feature_name",
    "feature_value",
    "horizon_days",
    "sample_count",
    "sample_fraction_pct",
    "mean_return_pct",
    "median_return_pct",
    "p10_return_pct",
    "p90_return_pct",
    "non_rebound_count",
    "non_rebound_rate_pct",
    "baseline_non_rebound_rate_pct",
    "non_rebound_lift_pct_points",
    "severe_loss_rate_pct",
)
_FEATURE_LIFT_COLUMNS: tuple[str, ...] = (
    "feature_name",
    "feature_label",
    "horizon_days",
    "sample_count",
    "sample_fraction_pct",
    "mean_return_pct",
    "median_return_pct",
    "non_rebound_count",
    "non_rebound_rate_pct",
    "baseline_non_rebound_rate_pct",
    "relative_risk",
    "odds_ratio",
    "prevalence_in_non_rebound_pct",
    "prevalence_in_rebound_pct",
    "prevalence_lift_pct_points",
    "severe_loss_rate_pct",
)


@dataclass(frozen=True)
class FallingKnifeNonReboundFundamentalProfileResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizon_days: int
    rebound_threshold: float
    severe_loss_threshold: float
    min_quality_score: int
    baseline_count: int
    rebound_count: int
    non_rebound_count: int
    non_rebound_rate_pct: float | None
    statement_coverage_pct: float | None
    research_note: str
    enriched_event_df: pd.DataFrame
    fundamental_profile_summary_df: pd.DataFrame
    feature_lift_summary_df: pd.DataFrame


def _empty_df(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _return_column(horizon_days: int) -> str:
    if horizon_days < 1:
        raise ValueError("horizon_days must be a positive integer")
    return f"catch_return_{int(horizon_days)}d"


def _positive_ratio(numerator: object, denominator: object) -> float | None:
    try:
        num = float(numerator)  # type: ignore[arg-type]
        den = float(denominator)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num) or not math.isfinite(den) or num <= 0.0 or den <= 0.0:
        return None
    return num / den


def _valuation_bucket(value: object, breakpoints: tuple[float, ...]) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "missing"
    if not math.isfinite(number):
        return "missing"
    previous = 0.0
    for breakpoint in breakpoints:
        if number < breakpoint:
            if math.isclose(previous, 0.0):
                return f"<{breakpoint:g}x"
            return f"{previous:g}-{breakpoint:g}x"
        previous = breakpoint
    return f">={breakpoints[-1]:g}x"


def _query_latest_valuation_features(
    db_path: str,
    event_key_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_key_df.empty:
        return _empty_df(("event_id", "bps", "actual_eps", "valuation_forecast_eps"))
    normalized_code_sql = normalize_code_sql("s.code")
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="falling-knife-non-rebound-valuation-",
    ) as ctx:
        ctx.connection.register("_event_keys", event_key_df)
        try:
            return ctx.connection.execute(
                f"""
                WITH bps_candidates AS (
                    SELECT
                        e.event_id,
                        CAST(s.bps AS DOUBLE) AS bps,
                        ROW_NUMBER() OVER (
                            PARTITION BY e.event_id
                            ORDER BY s.disclosed_date DESC, s.type_of_current_period DESC
                        ) AS row_priority
                    FROM _event_keys e
                    JOIN statements s
                        ON {normalized_code_sql} = e.code
                       AND s.disclosed_date <= e.signal_date
                    WHERE s.type_of_current_period = 'FY'
                      AND s.bps IS NOT NULL
                ),
                actual_eps_candidates AS (
                    SELECT
                        e.event_id,
                        CAST(s.earnings_per_share AS DOUBLE) AS actual_eps,
                        ROW_NUMBER() OVER (
                            PARTITION BY e.event_id
                            ORDER BY s.disclosed_date DESC, s.type_of_current_period DESC
                        ) AS row_priority
                    FROM _event_keys e
                    JOIN statements s
                        ON {normalized_code_sql} = e.code
                       AND s.disclosed_date <= e.signal_date
                    WHERE s.type_of_current_period = 'FY'
                      AND s.earnings_per_share IS NOT NULL
                ),
                forecast_eps_candidates AS (
                    SELECT
                        e.event_id,
                        CAST(COALESCE(
                            s.next_year_forecast_earnings_per_share,
                            s.forecast_eps
                        ) AS DOUBLE) AS valuation_forecast_eps,
                        ROW_NUMBER() OVER (
                            PARTITION BY e.event_id
                            ORDER BY s.disclosed_date DESC, s.type_of_current_period DESC
                        ) AS row_priority
                    FROM _event_keys e
                    JOIN statements s
                        ON {normalized_code_sql} = e.code
                       AND s.disclosed_date <= e.signal_date
                    WHERE COALESCE(
                        s.next_year_forecast_earnings_per_share,
                        s.forecast_eps
                    ) IS NOT NULL
                )
                SELECT
                    e.event_id,
                    b.bps,
                    a.actual_eps,
                    f.valuation_forecast_eps
                FROM _event_keys e
                LEFT JOIN bps_candidates b
                    ON b.event_id = e.event_id
                   AND b.row_priority = 1
                LEFT JOIN actual_eps_candidates a
                    ON a.event_id = e.event_id
                   AND a.row_priority = 1
                LEFT JOIN forecast_eps_candidates f
                    ON f.event_id = e.event_id
                   AND f.row_priority = 1
                ORDER BY e.event_id
                """
            ).fetchdf()
        finally:
            ctx.connection.unregister("_event_keys")


def _add_valuation_features(
    enriched_event_df: pd.DataFrame,
    event_df: pd.DataFrame,
    *,
    db_path: str,
    horizon_days: int,
) -> pd.DataFrame:
    if enriched_event_df.empty:
        return enriched_event_df.copy()
    return_column = _return_column(horizon_days)
    base_event_df = event_df[
        pd.to_numeric(event_df[return_column], errors="coerce").notna()
    ].copy()
    base_event_df = base_event_df.reset_index(drop=True)
    base_event_df["event_id"] = range(len(base_event_df))
    price_df = base_event_df[["event_id", "close"]].copy()
    event_key_df = base_event_df[["event_id", "code", "signal_date"]].copy()
    event_key_df["event_id"] = pd.to_numeric(
        event_key_df["event_id"],
        errors="raise",
    ).astype(int)
    event_key_df["code"] = event_key_df["code"].astype(str)
    event_key_df["signal_date"] = event_key_df["signal_date"].astype(str)
    valuation_df = _query_latest_valuation_features(db_path, event_key_df)
    frame = enriched_event_df.merge(price_df, on="event_id", how="left")
    frame = frame.merge(valuation_df, on="event_id", how="left")
    frame["pbr"] = [
        _positive_ratio(close, bps)
        for close, bps in zip(frame["close"], frame["bps"], strict=False)
    ]
    frame["per"] = [
        _positive_ratio(close, eps)
        for close, eps in zip(frame["close"], frame["actual_eps"], strict=False)
    ]
    frame["forward_per"] = [
        _positive_ratio(close, forecast_eps)
        for close, forecast_eps in zip(
            frame["close"],
            frame["valuation_forecast_eps"],
            strict=False,
        )
    ]
    frame["pbr_bucket"] = [
        _valuation_bucket(value, (0.5, 1.0, 1.5, 3.0)) for value in frame["pbr"]
    ]
    frame["per_bucket"] = [
        _valuation_bucket(value, (10.0, 15.0, 25.0, 40.0)) for value in frame["per"]
    ]
    frame["forward_per_bucket"] = [
        _valuation_bucket(value, (10.0, 15.0, 25.0, 40.0))
        for value in frame["forward_per"]
    ]
    frame["pbr_lt1"] = pd.to_numeric(frame["pbr"], errors="coerce") < 1.0
    frame["pbr_ge3"] = pd.to_numeric(frame["pbr"], errors="coerce") >= 3.0
    frame["per_ge40"] = pd.to_numeric(frame["per"], errors="coerce") >= 40.0
    frame["forward_per_ge40"] = (
        pd.to_numeric(frame["forward_per"], errors="coerce") >= 40.0
    )
    frame["forward_per_lt15"] = (
        pd.to_numeric(frame["forward_per"], errors="coerce") < 15.0
    )
    return frame


def _summary_stats(
    frame: pd.DataFrame,
    *,
    rebound_threshold: float,
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
            "non_rebound_count": 0,
            "non_rebound_rate_pct": math.nan,
            "severe_loss_rate_pct": math.nan,
        }
    non_rebound_mask = returns <= rebound_threshold
    severe_loss_mask = returns <= severe_loss_threshold
    return {
        "count": int(len(returns)),
        "mean_pct": float(returns.mean() * 100.0),
        "median_pct": float(returns.median() * 100.0),
        "p10_pct": float(returns.quantile(0.10) * 100.0),
        "p90_pct": float(returns.quantile(0.90) * 100.0),
        "non_rebound_count": int(non_rebound_mask.sum()),
        "non_rebound_rate_pct": float(non_rebound_mask.mean() * 100.0),
        "severe_loss_rate_pct": float(severe_loss_mask.mean() * 100.0),
    }


def _safe_divide(numerator: float, denominator: float) -> float:
    if math.isclose(denominator, 0.0):
        return math.nan
    return numerator / denominator


def _odds_ratio(
    *,
    exposed_non_rebound: int,
    exposed_rebound: int,
    unexposed_non_rebound: int,
    unexposed_rebound: int,
) -> float:
    # Haldane-Anscombe correction keeps rare feature rows finite and comparable.
    a = exposed_non_rebound + 0.5
    b = exposed_rebound + 0.5
    c = unexposed_non_rebound + 0.5
    d = unexposed_rebound + 0.5
    return float((a * d) / (b * c))


def _add_non_rebound_labels(
    enriched_event_df: pd.DataFrame,
    *,
    rebound_threshold: float,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    if enriched_event_df.empty:
        return enriched_event_df.copy()
    frame = enriched_event_df.copy()
    returns = pd.to_numeric(frame["catch_return"], errors="coerce")
    frame["non_rebound"] = returns <= rebound_threshold
    frame["rebound"] = returns > rebound_threshold
    frame["severe_loss"] = returns <= severe_loss_threshold
    frame["quality_score_bucket"] = [
        "missing_statement"
        if pd.isna(disclosed_date)
        else f"score_{int(score)}"
        for score, disclosed_date in zip(
            frame["quality_score"],
            frame["disclosed_date"],
            strict=False,
        )
    ]
    frame["equity_ratio_lt30"] = (
        pd.to_numeric(frame["equity_ratio_pct"], errors="coerce") < 30.0
    )
    frame["cfo_to_profit_lt1"] = (
        pd.to_numeric(frame["cfo_to_profit_ratio"], errors="coerce") < 1.0
    )
    return frame


def _build_profile_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    horizon_days: int,
    rebound_threshold: float,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    if enriched_event_df.empty:
        return _empty_df(_PROFILE_COLUMNS)
    baseline_stats = _summary_stats(
        enriched_event_df,
        rebound_threshold=rebound_threshold,
        severe_loss_threshold=severe_loss_threshold,
    )
    baseline_count = int(baseline_stats["count"])
    baseline_non_rebound_rate = float(baseline_stats["non_rebound_rate_pct"])
    rows: list[dict[str, object]] = []
    segment_columns = (
        "market_name",
        "quality_bucket",
        "quality_score_bucket",
        "forecast_eps_sign",
        "profit_sign",
        "cfo_sign",
        "fcf_sign",
        "equity_ratio_bucket",
        "pbr_bucket",
        "per_bucket",
        "forward_per_bucket",
        "risk_adjusted_bucket",
    )
    for feature_name in segment_columns:
        for feature_value, group in enriched_event_df.groupby(
            feature_name,
            dropna=False,
            sort=False,
        ):
            stats = _summary_stats(
                group,
                rebound_threshold=rebound_threshold,
                severe_loss_threshold=severe_loss_threshold,
            )
            non_rebound_rate = float(stats["non_rebound_rate_pct"])
            rows.append(
                {
                    "feature_name": feature_name,
                    "feature_value": str(feature_value),
                    "horizon_days": horizon_days,
                    "sample_count": stats["count"],
                    "sample_fraction_pct": (
                        int(stats["count"]) / baseline_count * 100.0
                        if baseline_count
                        else math.nan
                    ),
                    "mean_return_pct": stats["mean_pct"],
                    "median_return_pct": stats["median_pct"],
                    "p10_return_pct": stats["p10_pct"],
                    "p90_return_pct": stats["p90_pct"],
                    "non_rebound_count": stats["non_rebound_count"],
                    "non_rebound_rate_pct": non_rebound_rate,
                    "baseline_non_rebound_rate_pct": baseline_non_rebound_rate,
                    "non_rebound_lift_pct_points": (
                        non_rebound_rate - baseline_non_rebound_rate
                    ),
                    "severe_loss_rate_pct": stats["severe_loss_rate_pct"],
                }
            )
    for keys, group in enriched_event_df.groupby(
        ["market_name", "quality_bucket"],
        dropna=False,
        sort=False,
    ):
        market_name, quality_bucket = keys
        stats = _summary_stats(
            group,
            rebound_threshold=rebound_threshold,
            severe_loss_threshold=severe_loss_threshold,
        )
        non_rebound_rate = float(stats["non_rebound_rate_pct"])
        rows.append(
            {
                "feature_name": "market_x_quality",
                "feature_value": f"{market_name}__{quality_bucket}",
                "horizon_days": horizon_days,
                "sample_count": stats["count"],
                "sample_fraction_pct": (
                    int(stats["count"]) / baseline_count * 100.0
                    if baseline_count
                    else math.nan
                ),
                "mean_return_pct": stats["mean_pct"],
                "median_return_pct": stats["median_pct"],
                "p10_return_pct": stats["p10_pct"],
                "p90_return_pct": stats["p90_pct"],
                "non_rebound_count": stats["non_rebound_count"],
                "non_rebound_rate_pct": non_rebound_rate,
                "baseline_non_rebound_rate_pct": baseline_non_rebound_rate,
                "non_rebound_lift_pct_points": (
                    non_rebound_rate - baseline_non_rebound_rate
                ),
                "severe_loss_rate_pct": stats["severe_loss_rate_pct"],
            }
        )
    return pd.DataFrame(rows, columns=list(_PROFILE_COLUMNS)).sort_values(
        ["non_rebound_lift_pct_points", "sample_count"],
        ascending=[False, False],
        kind="stable",
    ).reset_index(drop=True)


def _feature_specs(min_quality_score: int) -> tuple[tuple[str, str, str], ...]:
    return (
        ("missing_statement", "Missing statement", "quality_bucket == 'missing_statement'"),
        (
            "low_quality",
            f"quality_score < {min_quality_score}",
            "quality_bucket == 'low_quality'",
        ),
        (
            "missing_or_low_quality",
            f"missing statement or quality_score < {min_quality_score}",
            "quality_bucket in ['missing_statement', 'low_quality']",
        ),
        ("forecast_non_positive", "forecast EPS <= 0", "forecast_eps_sign == 'forecast_non_positive'"),
        ("profit_non_positive", "Profit <= 0", "profit_sign == 'profit_non_positive'"),
        ("cfo_non_positive", "OperatingCashFlow <= 0", "cfo_sign == 'cfo_non_positive'"),
        ("fcf_non_positive", "simple FCF margin <= 0", "fcf_sign == 'fcf_non_positive'"),
        ("equity_ratio_lt30", "equity ratio < 30%", "equity_ratio_lt30 == True"),
        ("cfo_to_profit_lt1", "CFO / Profit < 1", "cfo_to_profit_lt1 == True"),
        ("pbr_lt1", "PBR < 1x", "pbr_lt1 == True"),
        ("pbr_ge3", "PBR >= 3x", "pbr_ge3 == True"),
        ("per_ge40", "PER >= 40x", "per_ge40 == True"),
        ("forward_per_ge40", "forward PER >= 40x", "forward_per_ge40 == True"),
        ("forward_per_lt15", "forward PER < 15x", "forward_per_lt15 == True"),
        ("growth_market", "Growth market", "market_name == 'グロース'"),
        (
            "growth_low_quality",
            f"Growth and quality_score < {min_quality_score}",
            "market_name == 'グロース' and quality_bucket == 'low_quality'",
        ),
        ("daily_rar_q5_highest", "Daily RAR Q5 highest", "risk_adjusted_bucket == 'Q5_highest'"),
    )


def _predicate_for_feature(frame: pd.DataFrame, expression: str) -> pd.Series:
    result = frame.eval(expression, engine="python")
    return cast(pd.Series, result).fillna(False).astype(bool)


def _build_feature_lift_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    horizon_days: int,
    rebound_threshold: float,
    severe_loss_threshold: float,
    min_quality_score: int,
) -> pd.DataFrame:
    if enriched_event_df.empty:
        return _empty_df(_FEATURE_LIFT_COLUMNS)
    baseline_stats = _summary_stats(
        enriched_event_df,
        rebound_threshold=rebound_threshold,
        severe_loss_threshold=severe_loss_threshold,
    )
    baseline_count = int(baseline_stats["count"])
    baseline_non_rebound_count = int(baseline_stats["non_rebound_count"])
    baseline_rebound_count = baseline_count - baseline_non_rebound_count
    baseline_non_rebound_rate = float(baseline_stats["non_rebound_rate_pct"])
    rows: list[dict[str, object]] = []
    non_rebound_mask = enriched_event_df["non_rebound"].astype(bool)
    for feature_name, feature_label, expression in _feature_specs(min_quality_score):
        feature_mask = _predicate_for_feature(enriched_event_df, expression)
        exposed = enriched_event_df[feature_mask]
        stats = _summary_stats(
            exposed,
            rebound_threshold=rebound_threshold,
            severe_loss_threshold=severe_loss_threshold,
        )
        sample_count = int(stats["count"])
        exposed_non_rebound = int(stats["non_rebound_count"])
        exposed_rebound = sample_count - exposed_non_rebound
        unexposed_non_rebound = baseline_non_rebound_count - exposed_non_rebound
        unexposed_rebound = baseline_rebound_count - exposed_rebound
        non_rebound_rate = float(stats["non_rebound_rate_pct"])
        prevalence_non_rebound = (
            feature_mask[non_rebound_mask].mean() * 100.0
            if baseline_non_rebound_count
            else math.nan
        )
        prevalence_rebound = (
            feature_mask[~non_rebound_mask].mean() * 100.0
            if baseline_rebound_count
            else math.nan
        )
        rows.append(
            {
                "feature_name": feature_name,
                "feature_label": feature_label,
                "horizon_days": horizon_days,
                "sample_count": sample_count,
                "sample_fraction_pct": (
                    sample_count / baseline_count * 100.0
                    if baseline_count
                    else math.nan
                ),
                "mean_return_pct": stats["mean_pct"],
                "median_return_pct": stats["median_pct"],
                "non_rebound_count": exposed_non_rebound,
                "non_rebound_rate_pct": non_rebound_rate,
                "baseline_non_rebound_rate_pct": baseline_non_rebound_rate,
                "relative_risk": _safe_divide(
                    non_rebound_rate,
                    baseline_non_rebound_rate,
                ),
                "odds_ratio": _odds_ratio(
                    exposed_non_rebound=exposed_non_rebound,
                    exposed_rebound=exposed_rebound,
                    unexposed_non_rebound=unexposed_non_rebound,
                    unexposed_rebound=unexposed_rebound,
                ),
                "prevalence_in_non_rebound_pct": prevalence_non_rebound,
                "prevalence_in_rebound_pct": prevalence_rebound,
                "prevalence_lift_pct_points": (
                    prevalence_non_rebound - prevalence_rebound
                ),
                "severe_loss_rate_pct": stats["severe_loss_rate_pct"],
            }
        )
    return pd.DataFrame(rows, columns=list(_FEATURE_LIFT_COLUMNS)).sort_values(
        ["prevalence_lift_pct_points", "relative_risk", "sample_count"],
        ascending=[False, False, False],
        kind="stable",
    ).reset_index(drop=True)


def run_falling_knife_non_rebound_fundamental_profile(
    input_bundle: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    rebound_threshold: float = DEFAULT_REBOUND_THRESHOLD,
    severe_loss_threshold: float = DEFAULT_SEVERE_LOSS_THRESHOLD,
    min_quality_score: int = DEFAULT_MIN_QUALITY_SCORE,
) -> FallingKnifeNonReboundFundamentalProfileResult:
    bundle_path = resolve_required_bundle_path(
        input_bundle,
        latest_bundle_resolver=lambda: get_falling_knife_reversal_study_latest_bundle_path(
            output_root=output_root,
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
    enriched_event_df = _add_valuation_features(
        enriched_event_df,
        input_result.event_df,
        db_path=input_result.db_path,
        horizon_days=horizon_days,
    )
    enriched_event_df = _add_non_rebound_labels(
        enriched_event_df,
        rebound_threshold=rebound_threshold,
        severe_loss_threshold=severe_loss_threshold,
    )
    profile_summary_df = _build_profile_summary_df(
        enriched_event_df,
        horizon_days=horizon_days,
        rebound_threshold=rebound_threshold,
        severe_loss_threshold=severe_loss_threshold,
    )
    feature_lift_summary_df = _build_feature_lift_summary_df(
        enriched_event_df,
        horizon_days=horizon_days,
        rebound_threshold=rebound_threshold,
        severe_loss_threshold=severe_loss_threshold,
        min_quality_score=min_quality_score,
    )
    baseline_count = len(enriched_event_df)
    non_rebound_count = int(enriched_event_df["non_rebound"].sum()) if baseline_count else 0
    rebound_count = int(enriched_event_df["rebound"].sum()) if baseline_count else 0
    statement_coverage_pct = (
        float(enriched_event_df["disclosed_date"].notna().mean() * 100.0)
        if baseline_count
        else None
    )
    non_rebound_rate_pct = (
        float(non_rebound_count / baseline_count * 100.0) if baseline_count else None
    )
    research_note = (
        "This study treats falling-knife events that do not produce a positive "
        "catch return as the target label and profiles their PIT-safe fundamental "
        "features. It is descriptive feature attribution, not an exclusion-rule "
        "optimization."
    )
    return FallingKnifeNonReboundFundamentalProfileResult(
        db_path=input_result.db_path,
        input_bundle_path=str(bundle_path),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_result.analysis_start_date,
        analysis_end_date=input_result.analysis_end_date,
        horizon_days=int(horizon_days),
        rebound_threshold=float(rebound_threshold),
        severe_loss_threshold=float(severe_loss_threshold),
        min_quality_score=int(min_quality_score),
        baseline_count=baseline_count,
        rebound_count=rebound_count,
        non_rebound_count=non_rebound_count,
        non_rebound_rate_pct=non_rebound_rate_pct,
        statement_coverage_pct=statement_coverage_pct,
        research_note=research_note,
        enriched_event_df=enriched_event_df,
        fundamental_profile_summary_df=profile_summary_df,
        feature_lift_summary_df=feature_lift_summary_df,
    )


def write_falling_knife_non_rebound_fundamental_profile_bundle(
    result: FallingKnifeNonReboundFundamentalProfileResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID,
        module=__name__,
        function="run_falling_knife_non_rebound_fundamental_profile",
        params={
            "input_bundle": result.input_bundle_path,
            "horizon_days": result.horizon_days,
            "rebound_threshold": result.rebound_threshold,
            "severe_loss_threshold": result.severe_loss_threshold,
            "min_quality_score": result.min_quality_score,
        },
        result=result,
        table_field_names=(
            "enriched_event_df",
            "fundamental_profile_summary_df",
            "feature_lift_summary_df",
        ),
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_falling_knife_non_rebound_fundamental_profile_bundle(
    bundle_path: str | Path,
) -> FallingKnifeNonReboundFundamentalProfileResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FallingKnifeNonReboundFundamentalProfileResult,
        table_field_names=(
            "enriched_event_df",
            "fundamental_profile_summary_df",
            "feature_lift_summary_df",
        ),
    )


def get_falling_knife_non_rebound_fundamental_profile_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_falling_knife_non_rebound_fundamental_profile_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
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


def _build_research_bundle_summary_markdown(
    result: FallingKnifeNonReboundFundamentalProfileResult,
) -> str:
    return "\n".join(
        [
            "# Falling Knife Non-Rebound Fundamental Profile",
            "",
            "## Snapshot",
            "",
            f"- Input bundle: `{result.input_bundle_path}`",
            f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
            f"- Horizon: `{result.horizon_days}` sessions",
            f"- Rebound threshold: `>{_fmt(result.rebound_threshold * 100.0)}%`",
            f"- Severe loss threshold: `{_fmt(result.severe_loss_threshold * 100.0)}%`",
            f"- Baseline trades: `{result.baseline_count}`",
            f"- Rebound trades: `{result.rebound_count}`",
            f"- Non-rebound trades: `{result.non_rebound_count}`",
            f"- Non-rebound rate: `{_fmt(result.non_rebound_rate_pct)}%`",
            f"- Statement coverage: `{_fmt(result.statement_coverage_pct)}%`",
            f"- High quality threshold: `quality_score >= {result.min_quality_score}`",
            "",
            "## Top Feature Lift Rows",
            "",
            *_top_rows(result.feature_lift_summary_df, limit=12),
            "",
            "## Top Profile Rows",
            "",
            *_top_rows(result.fundamental_profile_summary_df, limit=12),
            "",
            "## Tables",
            "",
            "- `enriched_event_df`",
            "- `fundamental_profile_summary_df`",
            "- `feature_lift_summary_df`",
        ]
    )


def _build_published_summary_payload(
    result: FallingKnifeNonReboundFundamentalProfileResult,
) -> dict[str, Any]:
    return {
        "experimentId": FALLING_KNIFE_NON_REBOUND_FUNDAMENTAL_PROFILE_EXPERIMENT_ID,
        "inputBundlePath": result.input_bundle_path,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "horizonDays": result.horizon_days,
        "reboundThreshold": result.rebound_threshold,
        "severeLossThreshold": result.severe_loss_threshold,
        "minQualityScore": result.min_quality_score,
        "baselineCount": result.baseline_count,
        "reboundCount": result.rebound_count,
        "nonReboundCount": result.non_rebound_count,
        "nonReboundRatePct": result.non_rebound_rate_pct,
        "statementCoveragePct": result.statement_coverage_pct,
        "topFeatureLift": result.feature_lift_summary_df.head(20).to_dict(
            orient="records",
        )
        if not result.feature_lift_summary_df.empty
        else [],
        "note": result.research_note,
    }
