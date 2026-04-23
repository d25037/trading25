"""Confounder analysis for the annual fundamental holding panel."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    get_annual_first_open_last_close_fundamental_panel_latest_bundle_path,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    load_research_bundle_info,
    load_research_bundle_tables,
    resolve_required_bundle_path,
    write_dataclass_research_bundle,
)

ANNUAL_FUNDAMENTAL_CONFOUNDER_ANALYSIS_EXPERIMENT_ID = (
    "market-behavior/annual-fundamental-confounder-analysis"
)
DEFAULT_WINSOR_LOWER = 0.01
DEFAULT_WINSOR_UPPER = 0.99
DEFAULT_MIN_OBSERVATIONS = 80
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "prepared_panel_df",
    "factor_coverage_df",
    "feature_correlation_df",
    "vif_df",
    "conditional_spread_df",
    "panel_regression_df",
    "fama_macbeth_df",
    "leave_one_year_out_df",
    "incremental_selection_df",
)


@dataclass(frozen=True)
class FactorSpec:
    score_name: str
    label: str
    source_column: str
    prefer_low: bool


@dataclass(frozen=True)
class RegressionModelSpec:
    name: str
    factor_scores: tuple[str, ...]
    fixed_effects: tuple[str, ...]


@dataclass(frozen=True)
class SelectionRuleSpec:
    name: str
    label: str
    required_score_buckets: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class AnnualFundamentalConfounderAnalysisResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    winsor_lower: float
    winsor_upper: float
    min_observations: int
    score_policy: str
    prepared_panel_df: pd.DataFrame
    factor_coverage_df: pd.DataFrame
    feature_correlation_df: pd.DataFrame
    vif_df: pd.DataFrame
    conditional_spread_df: pd.DataFrame
    panel_regression_df: pd.DataFrame
    fama_macbeth_df: pd.DataFrame
    leave_one_year_out_df: pd.DataFrame
    incremental_selection_df: pd.DataFrame


FACTOR_SPECS: tuple[FactorSpec, ...] = (
    FactorSpec("low_pbr_score", "Low PBR", "pbr", True),
    FactorSpec("low_forward_per_score", "Low forward PER", "forward_per", True),
    FactorSpec("low_per_score", "Low PER", "per", True),
    FactorSpec("small_market_cap_score", "Small market cap", "market_cap_bil_jpy", True),
    FactorSpec("low_adv60_score", "Low ADV60", "avg_trading_value_60d_mil_jpy", True),
    FactorSpec(
        "high_forecast_dividend_yield_score",
        "High forecast dividend yield",
        "forecast_dividend_yield_pct",
        False,
    ),
    FactorSpec("high_dividend_yield_score", "High dividend yield", "dividend_yield_pct", False),
    FactorSpec("high_cfo_yield_score", "High CFO yield", "cfo_yield_pct", False),
    FactorSpec(
        "high_forward_eps_to_actual_eps_score",
        "High forward EPS / actual EPS",
        "forward_eps_to_actual_eps",
        False,
    ),
)
_FACTOR_BY_SCORE = {spec.score_name: spec for spec in FACTOR_SPECS}
_CORE_MODEL = RegressionModelSpec(
    name="core_value_size_liquidity",
    factor_scores=(
        "low_pbr_score",
        "low_forward_per_score",
        "small_market_cap_score",
        "low_adv60_score",
    ),
    fixed_effects=("year", "market", "sector_33_name"),
)
_EXTENDED_MODEL = RegressionModelSpec(
    name="extended_value_yield_growth",
    factor_scores=(
        "low_pbr_score",
        "low_forward_per_score",
        "small_market_cap_score",
        "low_adv60_score",
        "high_forecast_dividend_yield_score",
        "high_cfo_yield_score",
        "high_forward_eps_to_actual_eps_score",
    ),
    fixed_effects=("year", "market", "sector_33_name"),
)
REGRESSION_MODELS: tuple[RegressionModelSpec, ...] = (_CORE_MODEL, _EXTENDED_MODEL)
SELECTION_RULES: tuple[SelectionRuleSpec, ...] = (
    SelectionRuleSpec("all_realized", "All realized events", ()),
    SelectionRuleSpec("low_pbr", "Low PBR", (("low_pbr_score", 5),)),
    SelectionRuleSpec(
        "low_pbr_small_cap",
        "Low PBR + small cap",
        (("low_pbr_score", 5), ("small_market_cap_score", 5)),
    ),
    SelectionRuleSpec(
        "low_pbr_small_cap_low_forward_per",
        "Low PBR + small cap + low forward PER",
        (
            ("low_pbr_score", 5),
            ("small_market_cap_score", 5),
            ("low_forward_per_score", 5),
        ),
    ),
    SelectionRuleSpec(
        "low_pbr_small_cap_low_adv60",
        "Low PBR + small cap + low ADV60",
        (
            ("low_pbr_score", 5),
            ("small_market_cap_score", 5),
            ("low_adv60_score", 5),
        ),
    ),
    SelectionRuleSpec(
        "low_pbr_low_forward_per",
        "Low PBR + low forward PER",
        (("low_pbr_score", 5), ("low_forward_per_score", 5)),
    ),
    SelectionRuleSpec(
        "low_pbr_high_forecast_dividend_yield",
        "Low PBR + high forecast dividend yield",
        (("low_pbr_score", 5), ("high_forecast_dividend_yield_score", 5)),
    ),
    SelectionRuleSpec(
        "low_pbr_high_cfo_yield",
        "Low PBR + high CFO yield",
        (("low_pbr_score", 5), ("high_cfo_yield_score", 5)),
    ),
)


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _is_finite_number(value: object) -> bool:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _normal_p_value_from_t(t_stat: float | None) -> float | None:
    if t_stat is None or not math.isfinite(t_stat):
        return None
    return math.erfc(abs(t_stat) / math.sqrt(2.0))


def _market_scope_sort(frame: pd.DataFrame, extra_columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty or "market_scope" not in frame.columns:
        return frame.reset_index(drop=True)
    result = frame.copy()
    result["market_scope"] = pd.Categorical(
        result["market_scope"].astype(str),
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(result["market_scope"])],
        ordered=True,
    )
    return result.sort_values(["market_scope", *extra_columns], kind="stable").reset_index(drop=True)


def _expand_market_scope(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    all_scope = frame.copy()
    all_scope["market_scope"] = "all"
    market_scope = frame.copy()
    market_scope["market_scope"] = market_scope["market"].astype(str)
    return pd.concat([all_scope, market_scope], ignore_index=True)


def _winsorize(series: pd.Series, lower: float, upper: float) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return numeric
    low = float(valid.quantile(lower))
    high = float(valid.quantile(upper))
    return numeric.clip(lower=low, upper=high)


def _score_factor_within_year_market(frame: pd.DataFrame, spec: FactorSpec) -> pd.Series:
    values = pd.to_numeric(frame[spec.source_column], errors="coerce")
    scores = pd.Series(np.nan, index=frame.index, dtype="float64")
    for _, group in frame.groupby(["year", "market"], sort=False):
        group_values = values.loc[group.index]
        valid = group_values.dropna()
        count = len(valid)
        if count == 0:
            continue
        if count == 1:
            ranked = pd.Series(0.5, index=valid.index, dtype="float64")
        else:
            ranked = (valid.rank(method="average") - 1.0) / float(count - 1)
        if spec.prefer_low:
            ranked = 1.0 - ranked
        scores.loc[ranked.index] = ranked.astype(float)
    return scores


def _assign_score_buckets(scoped_df: pd.DataFrame, score_name: str, bucket_count: int = 5) -> pd.Series:
    values = pd.to_numeric(scoped_df[score_name], errors="coerce")
    buckets = pd.Series(np.nan, index=scoped_df.index, dtype="float64")
    for _, group in scoped_df.groupby(["market_scope", "year"], observed=True, sort=False):
        valid = values.loc[group.index].dropna().sort_values(kind="stable")
        count = len(valid)
        if count < bucket_count:
            continue
        ranks = np.arange(count, dtype=float)
        assigned = np.floor(ranks * bucket_count / count).astype(int) + 1
        buckets.loc[valid.index] = assigned.astype(float)
    return buckets


def _prepare_panel_df(
    event_ledger_df: pd.DataFrame,
    *,
    winsor_lower: float,
    winsor_upper: float,
) -> pd.DataFrame:
    required_columns = {
        "status",
        "event_id",
        "year",
        "code",
        "market",
        "sector_33_name",
        "event_return_pct",
    }
    missing = sorted(required_columns - set(event_ledger_df.columns))
    if missing:
        raise ValueError(f"Input event_ledger_df is missing required columns: {missing}")
    realized = event_ledger_df[event_ledger_df["status"].astype(str) == "realized"].copy()
    if realized.empty:
        return _empty_df([])
    realized["year"] = realized["year"].astype(str)
    realized["market"] = realized["market"].astype(str)
    realized["sector_33_name"] = realized["sector_33_name"].fillna("unknown").astype(str)
    realized["event_return_pct"] = pd.to_numeric(realized["event_return_pct"], errors="coerce")
    realized["event_return_winsor_pct"] = _winsorize(
        realized["event_return_pct"],
        winsor_lower,
        winsor_upper,
    )
    for spec in FACTOR_SPECS:
        if spec.source_column not in realized.columns:
            realized[spec.source_column] = np.nan
        realized[spec.score_name] = _score_factor_within_year_market(realized, spec)
    keep_columns = [
        "event_id",
        "year",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "event_return_pct",
        "event_return_winsor_pct",
        *[spec.source_column for spec in FACTOR_SPECS],
        *[spec.score_name for spec in FACTOR_SPECS],
    ]
    for column in keep_columns:
        if column not in realized.columns:
            realized[column] = None
    return realized[keep_columns].reset_index(drop=True)


def _build_factor_coverage_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "factor_name",
        "factor_label",
        "source_column",
        "event_count",
        "non_null_count",
        "coverage_pct",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _expand_market_scope(panel_df)
    records: list[dict[str, Any]] = []
    for (market_scope, factor_name), group in [
        ((market_scope, spec.score_name), group)
        for market_scope, scope_group in scoped.groupby("market_scope", observed=True, sort=False)
        for spec in FACTOR_SPECS
        for group in [scope_group]
    ]:
        spec = _FACTOR_BY_SCORE[factor_name]
        non_null = pd.to_numeric(group[spec.score_name], errors="coerce").notna()
        records.append(
            {
                "market_scope": str(market_scope),
                "factor_name": spec.score_name,
                "factor_label": spec.label,
                "source_column": spec.source_column,
                "event_count": int(len(group)),
                "non_null_count": int(non_null.sum()),
                "coverage_pct": float(non_null.mean() * 100.0) if len(group) else None,
            }
        )
    return _market_scope_sort(pd.DataFrame(records), ["factor_name"])


def _build_feature_correlation_df(panel_df: pd.DataFrame, *, min_observations: int) -> pd.DataFrame:
    columns = [
        "market_scope",
        "factor_x",
        "factor_y",
        "factor_x_label",
        "factor_y_label",
        "observation_count",
        "spearman_corr",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _expand_market_scope(panel_df)
    score_names = [spec.score_name for spec in FACTOR_SPECS]
    records: list[dict[str, Any]] = []
    for market_scope, group in scoped.groupby("market_scope", observed=True, sort=False):
        for index, left in enumerate(score_names):
            for right in score_names[index + 1 :]:
                pair = group[[left, right]].apply(pd.to_numeric, errors="coerce").dropna()
                if len(pair) < min_observations:
                    corr = None
                else:
                    corr_value = pair[left].corr(pair[right], method="spearman")
                    corr = float(corr_value) if pd.notna(corr_value) else None
                records.append(
                    {
                        "market_scope": str(market_scope),
                        "factor_x": left,
                        "factor_y": right,
                        "factor_x_label": _FACTOR_BY_SCORE[left].label,
                        "factor_y_label": _FACTOR_BY_SCORE[right].label,
                        "observation_count": int(len(pair)),
                        "spearman_corr": corr,
                    }
                )
    return _market_scope_sort(pd.DataFrame(records), ["factor_x", "factor_y"])


def _standardize_columns(frame: pd.DataFrame, columns: Sequence[str]) -> tuple[pd.DataFrame, list[str]]:
    standardized = pd.DataFrame(index=frame.index)
    kept: list[str] = []
    for column in columns:
        values = pd.to_numeric(frame[column], errors="coerce").astype(float)
        std = float(values.std(ddof=0))
        if not math.isfinite(std) or math.isclose(std, 0.0, abs_tol=1e-12):
            continue
        mean = float(values.mean())
        standardized[column] = (values - mean) / std
        kept.append(column)
    return standardized, kept


def _ols_fit(
    frame: pd.DataFrame,
    *,
    y_column: str,
    numeric_columns: Sequence[str],
    fixed_effect_columns: Sequence[str],
    min_observations: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = [y_column, *numeric_columns, *fixed_effect_columns]
    data = frame[columns].copy()
    for column in [y_column, *numeric_columns]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=[y_column, *numeric_columns]).copy()
    if len(data) < min_observations:
        return _empty_df([]), {"nobs": int(len(data)), "r_squared": None}
    standardized, kept_numeric = _standardize_columns(data, numeric_columns)
    if not kept_numeric:
        return _empty_df([]), {"nobs": int(len(data)), "r_squared": None}
    x_parts = [standardized[kept_numeric].reset_index(drop=True)]
    for column in fixed_effect_columns:
        if column not in data.columns:
            continue
        dummies = pd.get_dummies(data[column].fillna("unknown").astype(str), prefix=column, drop_first=True)
        if not dummies.empty:
            x_parts.append(dummies.astype(float).reset_index(drop=True))
    x_df = pd.concat(x_parts, axis=1)
    x = np.column_stack([np.ones(len(x_df)), x_df.to_numpy(dtype=float)])
    y = pd.to_numeric(data[y_column], errors="coerce").to_numpy(dtype=float)
    beta = np.linalg.pinv(x) @ y
    fitted = x @ beta
    residual = y - fitted
    ss_res = float(np.sum(residual**2))
    ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
    r_squared = 0.0 if math.isclose(ss_tot, 0.0, abs_tol=1e-12) else 1.0 - ss_res / ss_tot
    x_pinv = np.linalg.pinv(x.T @ x)
    meat = x.T @ ((residual[:, None] ** 2) * x)
    denom = max(1, len(y) - x.shape[1])
    scale = len(y) / denom
    covariance = x_pinv @ meat @ x_pinv * scale
    standard_errors = np.sqrt(np.maximum(np.diag(covariance), 0.0))
    rows: list[dict[str, Any]] = []
    x_columns = ["intercept", *list(x_df.columns)]
    for index, column in enumerate(x_columns):
        if column not in kept_numeric:
            continue
        coefficient = float(beta[index])
        standard_error = float(standard_errors[index]) if math.isfinite(float(standard_errors[index])) else None
        t_stat = (
            coefficient / standard_error
            if standard_error is not None and not math.isclose(standard_error, 0.0, abs_tol=1e-12)
            else None
        )
        rows.append(
            {
                "factor_name": column,
                "factor_label": _FACTOR_BY_SCORE[column].label if column in _FACTOR_BY_SCORE else column,
                "coefficient_pct_per_1sd": coefficient,
                "robust_se": standard_error,
                "t_stat": t_stat,
                "p_value_normal_approx": _normal_p_value_from_t(t_stat),
            }
        )
    summary = {"nobs": int(len(data)), "r_squared": float(r_squared), "feature_count": int(x.shape[1])}
    return pd.DataFrame(rows), summary


def _build_vif_df(panel_df: pd.DataFrame, *, min_observations: int) -> pd.DataFrame:
    columns = [
        "market_scope",
        "factor_name",
        "factor_label",
        "observation_count",
        "r_squared_against_other_factors",
        "vif",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _expand_market_scope(panel_df)
    score_names = [spec.score_name for spec in FACTOR_SPECS]
    records: list[dict[str, Any]] = []
    for market_scope, group in scoped.groupby("market_scope", observed=True, sort=False):
        matrix = group[score_names].apply(pd.to_numeric, errors="coerce").dropna()
        for score_name in score_names:
            if len(matrix) < min_observations or len(score_names) < 2:
                r_squared = None
                vif = None
            else:
                others = [column for column in score_names if column != score_name]
                coef_df, summary = _ols_fit(
                    matrix,
                    y_column=score_name,
                    numeric_columns=others,
                    fixed_effect_columns=(),
                    min_observations=min_observations,
                )
                _ = coef_df
                r_squared = summary.get("r_squared")
                vif = (
                    1.0 / max(1e-12, 1.0 - float(r_squared))
                    if isinstance(r_squared, float) and math.isfinite(r_squared)
                    else None
                )
            records.append(
                {
                    "market_scope": str(market_scope),
                    "factor_name": score_name,
                    "factor_label": _FACTOR_BY_SCORE[score_name].label,
                    "observation_count": int(len(matrix)),
                    "r_squared_against_other_factors": r_squared,
                    "vif": vif,
                }
            )
    return _market_scope_sort(pd.DataFrame(records), ["factor_name"])


def _build_bucketed_scoped_panel(panel_df: pd.DataFrame) -> pd.DataFrame:
    scoped = _expand_market_scope(panel_df)
    for spec in FACTOR_SPECS:
        scoped[f"{spec.score_name}_bucket"] = _assign_score_buckets(scoped, spec.score_name)
    return scoped


def _mean_return(frame: pd.DataFrame) -> float | None:
    values = pd.to_numeric(frame["event_return_winsor_pct"], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def _win_rate(frame: pd.DataFrame) -> float | None:
    values = pd.to_numeric(frame["event_return_winsor_pct"], errors="coerce").dropna()
    return float((values > 0.0).mean() * 100.0) if not values.empty else None


def _build_conditional_spread_df(
    panel_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "target_factor",
        "target_label",
        "confounder_factor",
        "confounder_label",
        "confounder_bucket",
        "confounder_bucket_label",
        "preferred_count",
        "opposite_count",
        "preferred_mean_return_pct",
        "opposite_mean_return_pct",
        "preferred_minus_opposite_mean_return_pct",
        "preferred_win_rate_pct",
        "opposite_win_rate_pct",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _build_bucketed_scoped_panel(panel_df)
    records: list[dict[str, Any]] = []
    for market_scope, scope_group in scoped.groupby("market_scope", observed=True, sort=False):
        for target in FACTOR_SPECS:
            target_bucket = f"{target.score_name}_bucket"
            for confounder in FACTOR_SPECS:
                if target.score_name == confounder.score_name:
                    continue
                confounder_bucket = f"{confounder.score_name}_bucket"
                for bucket_value in (1, 5):
                    conditioned = scope_group[scope_group[confounder_bucket] == float(bucket_value)]
                    preferred = conditioned[conditioned[target_bucket] == 5.0]
                    opposite = conditioned[conditioned[target_bucket] == 1.0]
                    preferred_mean = _mean_return(preferred) if len(preferred) >= min_observations else None
                    opposite_mean = _mean_return(opposite) if len(opposite) >= min_observations else None
                    spread = (
                        preferred_mean - opposite_mean
                        if preferred_mean is not None and opposite_mean is not None
                        else None
                    )
                    records.append(
                        {
                            "market_scope": str(market_scope),
                            "target_factor": target.score_name,
                            "target_label": target.label,
                            "confounder_factor": confounder.score_name,
                            "confounder_label": confounder.label,
                            "confounder_bucket": bucket_value,
                            "confounder_bucket_label": f"Q{bucket_value}",
                            "preferred_count": int(len(preferred)),
                            "opposite_count": int(len(opposite)),
                            "preferred_mean_return_pct": preferred_mean,
                            "opposite_mean_return_pct": opposite_mean,
                            "preferred_minus_opposite_mean_return_pct": spread,
                            "preferred_win_rate_pct": _win_rate(preferred),
                            "opposite_win_rate_pct": _win_rate(opposite),
                        }
                    )
    return _market_scope_sort(
        pd.DataFrame(records),
        ["target_factor", "confounder_factor", "confounder_bucket"],
    )


def _frame_for_market_scope(panel_df: pd.DataFrame, market_scope: str) -> pd.DataFrame:
    if market_scope == "all":
        return panel_df.copy()
    return panel_df[panel_df["market"].astype(str) == market_scope].copy()


def _fixed_effects_for_scope(model: RegressionModelSpec, market_scope: str) -> tuple[str, ...]:
    if market_scope == "all":
        return model.fixed_effects
    return tuple(effect for effect in model.fixed_effects if effect != "market")


def _build_panel_regression_df(panel_df: pd.DataFrame, *, min_observations: int) -> pd.DataFrame:
    columns = [
        "market_scope",
        "model_name",
        "factor_name",
        "factor_label",
        "observation_count",
        "r_squared",
        "coefficient_pct_per_1sd",
        "robust_se",
        "t_stat",
        "p_value_normal_approx",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_frame = _frame_for_market_scope(panel_df, market_scope)
        if scope_frame.empty:
            continue
        for model in REGRESSION_MODELS:
            coef_df, summary = _ols_fit(
                scope_frame,
                y_column="event_return_winsor_pct",
                numeric_columns=model.factor_scores,
                fixed_effect_columns=_fixed_effects_for_scope(model, market_scope),
                min_observations=min_observations,
            )
            for row in coef_df.to_dict(orient="records"):
                records.append(
                    {
                        "market_scope": market_scope,
                        "model_name": model.name,
                        "factor_name": row["factor_name"],
                        "factor_label": row["factor_label"],
                        "observation_count": summary.get("nobs"),
                        "r_squared": summary.get("r_squared"),
                        "coefficient_pct_per_1sd": row["coefficient_pct_per_1sd"],
                        "robust_se": row["robust_se"],
                        "t_stat": row["t_stat"],
                        "p_value_normal_approx": row["p_value_normal_approx"],
                    }
                )
    return _market_scope_sort(pd.DataFrame(records), ["model_name", "factor_name"])


def _build_fama_macbeth_df(panel_df: pd.DataFrame, *, min_observations: int) -> pd.DataFrame:
    columns = [
        "market_scope",
        "model_name",
        "factor_name",
        "factor_label",
        "year_count",
        "mean_coefficient_pct_per_1sd",
        "coefficient_std",
        "t_stat_over_years",
        "positive_year_rate_pct",
        "negative_year_rate_pct",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    model = _EXTENDED_MODEL
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_frame = _frame_for_market_scope(panel_df, market_scope)
        yearly_coefficients: dict[str, list[float]] = {factor: [] for factor in model.factor_scores}
        for _, year_frame in scope_frame.groupby("year", sort=True):
            coef_df, _summary = _ols_fit(
                year_frame,
                y_column="event_return_winsor_pct",
                numeric_columns=model.factor_scores,
                fixed_effect_columns=_fixed_effects_for_scope(model, market_scope),
                min_observations=min_observations,
            )
            for row in coef_df.to_dict(orient="records"):
                value = row.get("coefficient_pct_per_1sd")
                factor_name = str(row.get("factor_name"))
                if factor_name in yearly_coefficients and _is_finite_number(value):
                    yearly_coefficients[factor_name].append(float(cast(float, value)))
        for factor_name, coefficients in yearly_coefficients.items():
            if not coefficients:
                continue
            series = pd.Series(coefficients, dtype="float64")
            std = float(series.std(ddof=1)) if len(series) > 1 else None
            mean = float(series.mean())
            t_stat = (
                mean / (std / math.sqrt(len(series)))
                if std is not None and not math.isclose(std, 0.0, abs_tol=1e-12)
                else None
            )
            records.append(
                {
                    "market_scope": market_scope,
                    "model_name": model.name,
                    "factor_name": factor_name,
                    "factor_label": _FACTOR_BY_SCORE[factor_name].label,
                    "year_count": int(len(series)),
                    "mean_coefficient_pct_per_1sd": mean,
                    "coefficient_std": std,
                    "t_stat_over_years": t_stat,
                    "positive_year_rate_pct": float((series > 0.0).mean() * 100.0),
                    "negative_year_rate_pct": float((series < 0.0).mean() * 100.0),
                }
            )
    return _market_scope_sort(pd.DataFrame(records), ["model_name", "factor_name"])


def _build_leave_one_year_out_df(panel_df: pd.DataFrame, *, min_observations: int) -> pd.DataFrame:
    columns = [
        "market_scope",
        "model_name",
        "excluded_year",
        "factor_name",
        "factor_label",
        "observation_count",
        "coefficient_pct_per_1sd",
        "t_stat",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    model = _EXTENDED_MODEL
    years = sorted(str(year) for year in panel_df["year"].dropna().unique())
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_frame = _frame_for_market_scope(panel_df, market_scope)
        for excluded_year in years:
            train = scope_frame[scope_frame["year"].astype(str) != excluded_year]
            coef_df, summary = _ols_fit(
                train,
                y_column="event_return_winsor_pct",
                numeric_columns=model.factor_scores,
                fixed_effect_columns=_fixed_effects_for_scope(model, market_scope),
                min_observations=min_observations,
            )
            for row in coef_df.to_dict(orient="records"):
                records.append(
                    {
                        "market_scope": market_scope,
                        "model_name": model.name,
                        "excluded_year": excluded_year,
                        "factor_name": row["factor_name"],
                        "factor_label": row["factor_label"],
                        "observation_count": summary.get("nobs"),
                        "coefficient_pct_per_1sd": row["coefficient_pct_per_1sd"],
                        "t_stat": row["t_stat"],
                    }
                )
    return _market_scope_sort(pd.DataFrame(records), ["model_name", "excluded_year", "factor_name"])


def _annual_selection_stats(selection: pd.DataFrame) -> dict[str, Any]:
    annual = (
        selection.groupby("year", sort=True)["event_return_winsor_pct"]
        .mean()
        .dropna()
        .astype(float)
    )
    year_std = float(annual.std(ddof=1)) if len(annual) > 1 else None
    year_mean = float(annual.mean()) if len(annual) else None
    year_t = (
        year_mean / (year_std / math.sqrt(len(annual)))
        if year_mean is not None and year_std is not None and not math.isclose(year_std, 0.0, abs_tol=1e-12)
        else None
    )
    return {
        "year_count": int(len(annual)),
        "annual_mean_return_pct": year_mean,
        "annual_return_std_pct": year_std,
        "year_t_stat": year_t,
        "positive_year_rate_pct": float((annual > 0.0).mean() * 100.0) if len(annual) else None,
        "min_year_return_pct": float(annual.min()) if len(annual) else None,
        "max_year_return_pct": float(annual.max()) if len(annual) else None,
    }


def _build_incremental_selection_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "rule_name",
        "rule_label",
        "event_count",
        "year_count",
        "median_annual_names",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "annual_mean_return_pct",
        "annual_return_std_pct",
        "year_t_stat",
        "positive_year_rate_pct",
        "min_year_return_pct",
        "max_year_return_pct",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _build_bucketed_scoped_panel(panel_df)
    records: list[dict[str, Any]] = []
    for market_scope, scope_group in scoped.groupby("market_scope", observed=True, sort=False):
        for rule in SELECTION_RULES:
            selection = scope_group.copy()
            for score_name, bucket_value in rule.required_score_buckets:
                selection = selection[selection[f"{score_name}_bucket"] == float(bucket_value)]
            annual_counts = selection.groupby("year", sort=True).size()
            returns = pd.to_numeric(selection["event_return_winsor_pct"], errors="coerce").dropna()
            stats = _annual_selection_stats(selection)
            records.append(
                {
                    "market_scope": str(market_scope),
                    "rule_name": rule.name,
                    "rule_label": rule.label,
                    "event_count": int(len(selection)),
                    "year_count": stats["year_count"],
                    "median_annual_names": (
                        float(annual_counts.median()) if not annual_counts.empty else None
                    ),
                    "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                    "median_return_pct": float(returns.median()) if not returns.empty else None,
                    "win_rate_pct": float((returns > 0.0).mean() * 100.0)
                    if not returns.empty
                    else None,
                    **stats,
                }
            )
    return _market_scope_sort(pd.DataFrame(records), ["rule_name"])


def run_annual_fundamental_confounder_analysis(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> AnnualFundamentalConfounderAnalysisResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_observations < 5:
        raise ValueError("min_observations must be >= 5")
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_annual_first_open_last_close_fundamental_panel_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Annual first-open last-close fundamental panel bundle was not found. "
            "Run run_annual_first_open_last_close_fundamental_panel.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    tables = load_research_bundle_tables(resolved_input, table_names=("event_ledger_df",))
    panel_df = _prepare_panel_df(
        tables["event_ledger_df"],
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
    )
    return AnnualFundamentalConfounderAnalysisResult(
        db_path=str(resolved_input),
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_info.analysis_start_date,
        analysis_end_date=input_info.analysis_end_date,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_observations=min_observations,
        score_policy=(
            "factor scores are within year x current-market percentile scores; "
            "higher score means stronger preferred direction"
        ),
        prepared_panel_df=panel_df,
        factor_coverage_df=_build_factor_coverage_df(panel_df),
        feature_correlation_df=_build_feature_correlation_df(
            panel_df,
            min_observations=min_observations,
        ),
        vif_df=_build_vif_df(panel_df, min_observations=min_observations),
        conditional_spread_df=_build_conditional_spread_df(
            panel_df,
            min_observations=min_observations,
        ),
        panel_regression_df=_build_panel_regression_df(
            panel_df,
            min_observations=min_observations,
        ),
        fama_macbeth_df=_build_fama_macbeth_df(panel_df, min_observations=min_observations),
        leave_one_year_out_df=_build_leave_one_year_out_df(
            panel_df,
            min_observations=min_observations,
        ),
        incremental_selection_df=_build_incremental_selection_df(panel_df),
    )


def _fmt(value: object, digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _build_summary_markdown(result: AnnualFundamentalConfounderAnalysisResult) -> str:
    lines = [
        "# Annual Fundamental Confounder Analysis",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Winsorized return bounds: `{result.winsor_lower}` / `{result.winsor_upper}`",
        f"- Minimum observations: `{result.min_observations}`",
        f"- Score policy: {result.score_policy}.",
        "",
        "## Extended Panel Regression",
        "",
    ]
    reg = result.panel_regression_df[
        result.panel_regression_df["model_name"].astype(str) == _EXTENDED_MODEL.name
    ].copy() if not result.panel_regression_df.empty else pd.DataFrame()
    if reg.empty:
        lines.append("- No regression rows were produced.")
    else:
        for row in reg.sort_values(
            ["market_scope", "factor_name"],
            kind="stable",
        ).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['factor_name']}`: "
                f"coef `{_fmt(row['coefficient_pct_per_1sd'])}pp`, "
                f"t `{_fmt(row['t_stat'])}`, "
                f"n `{int(cast(int, row['observation_count']))}`"
            )
    lines.extend(["", "## Incremental Selection Snapshot", ""])
    selection = result.incremental_selection_df.copy()
    if selection.empty:
        lines.append("- No incremental selection rows were produced.")
    else:
        for row in selection[
            selection["market_scope"].astype(str).isin(["all", "standard"])
        ].to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['rule_name']}`: "
                f"events `{int(cast(int, row['event_count']))}`, "
                f"mean `{_fmt(row['mean_return_pct'])}%`, "
                f"annual mean `{_fmt(row['annual_mean_return_pct'])}%`, "
                f"year t `{_fmt(row['year_t_stat'])}`"
            )
    return "\n".join(lines)


def _build_published_summary(result: AnnualFundamentalConfounderAnalysisResult) -> dict[str, Any]:
    return {
        "inputBundlePath": result.input_bundle_path,
        "inputRunId": result.input_run_id,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "winsorLower": result.winsor_lower,
        "winsorUpper": result.winsor_upper,
        "minObservations": result.min_observations,
        "panelRegression": result.panel_regression_df.to_dict(orient="records"),
        "famaMacbeth": result.fama_macbeth_df.to_dict(orient="records"),
        "incrementalSelection": result.incremental_selection_df.to_dict(orient="records"),
    }


def write_annual_fundamental_confounder_analysis_bundle(
    result: AnnualFundamentalConfounderAnalysisResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_FUNDAMENTAL_CONFOUNDER_ANALYSIS_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_fundamental_confounder_analysis",
        params={
            "input_bundle_path": result.input_bundle_path,
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_observations": result.min_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_fundamental_confounder_analysis_bundle(
    bundle_path: str | Path,
) -> AnnualFundamentalConfounderAnalysisResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualFundamentalConfounderAnalysisResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_fundamental_confounder_analysis_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_FUNDAMENTAL_CONFOUNDER_ANALYSIS_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_fundamental_confounder_analysis_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_FUNDAMENTAL_CONFOUNDER_ANALYSIS_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
