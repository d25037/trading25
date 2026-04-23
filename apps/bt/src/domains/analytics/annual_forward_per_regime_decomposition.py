"""Forward PER regime decomposition research for the annual fundamental panel."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    get_annual_first_open_last_close_fundamental_panel_latest_bundle_path,
    _to_nullable_float,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    _annual_selection_stats,
    _expand_market_scope,
    _market_scope_sort,
    _ols_fit,
    _prepare_panel_df,
)
from src.domains.analytics.annual_value_composite_selection import (
    _build_portfolio_summary_df,
    _load_price_df,
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

ANNUAL_FORWARD_PER_REGIME_DECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/annual-forward-per-regime-decomposition"
)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10)
DEFAULT_MIN_TRAIN_OBSERVATIONS = 80
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")
_REGIME_ORDER: tuple[str, ...] = (
    "positive_low",
    "non_positive",
    "positive_other",
    "missing_or_nonfinite",
)
_REGIME_LABELS: dict[str, str] = {
    "positive_low": "Positive low forward PER",
    "non_positive": "Non-positive forward PER",
    "positive_other": "Positive other forward PER",
    "missing_or_nonfinite": "Missing / non-finite forward PER",
}
_FACTOR_LABELS: dict[str, str] = {
    "low_pbr_score": "Low PBR",
    "small_market_cap_score": "Small market cap",
    "low_adv60_score": "Low ADV60",
    "high_forecast_dividend_yield_score": "High forecast dividend yield",
    "high_cfo_yield_score": "High CFO yield",
    "high_forward_eps_to_actual_eps_score": "High forward EPS / actual EPS",
    "positive_low_forward_per_score": "Positive low forward PER",
    "positive_low_forward_per_component_score": "Positive low forward PER component",
    "non_positive_forward_per_score": "Non-positive forward PER",
}
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "prepared_panel_df",
    "regime_coverage_df",
    "regime_return_summary_df",
    "conditional_regime_summary_df",
    "panel_regression_df",
    "selected_event_df",
    "selection_mix_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
    "portfolio_regime_contribution_df",
)


@dataclass(frozen=True)
class StrategySpec:
    name: str
    label: str
    factor_columns: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class LiquidityScenarioSpec:
    name: str
    label: str
    min_adv60_mil_jpy: float | None = None


@dataclass(frozen=True)
class ContextRuleSpec:
    name: str
    label: str
    required_score_buckets: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class RegressionModelSpec:
    name: str
    factor_columns: tuple[str, ...]
    fixed_effects: tuple[str, ...]


@dataclass(frozen=True)
class AnnualForwardPerRegimeDecompositionResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    winsor_lower: float
    winsor_upper: float
    selection_fractions: tuple[float, ...]
    min_train_observations: int
    input_realized_event_count: int
    analysis_event_count: int
    finite_forward_per_event_count: int
    score_policy: str
    prepared_panel_df: pd.DataFrame
    regime_coverage_df: pd.DataFrame
    regime_return_summary_df: pd.DataFrame
    conditional_regime_summary_df: pd.DataFrame
    panel_regression_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_mix_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame
    portfolio_regime_contribution_df: pd.DataFrame


STRATEGY_SPECS: tuple[StrategySpec, ...] = (
    StrategySpec(
        "base_pbr_size_walkforward",
        "Base walk-forward (low PBR + small cap)",
        ("low_pbr_score", "small_market_cap_score"),
        "Walk-forward score using low PBR and small market cap only.",
    ),
    StrategySpec(
        "full_forward_per_walkforward",
        "Full low-forward-PER walk-forward",
        ("low_pbr_score", "small_market_cap_score", "low_forward_per_score"),
        "Walk-forward score using the existing low forward PER factor.",
    ),
    StrategySpec(
        "positive_forward_per_walkforward",
        "Positive low-forward-PER walk-forward",
        ("low_pbr_score", "small_market_cap_score", "positive_low_forward_per_score"),
        "Walk-forward score using only positive low forward PER.",
    ),
    StrategySpec(
        "non_positive_forward_per_walkforward",
        "Non-positive-forward-PER walk-forward",
        ("low_pbr_score", "small_market_cap_score", "non_positive_forward_per_score"),
        "Walk-forward score using a non-positive forward PER tilt.",
    ),
    StrategySpec(
        "decomposed_forward_per_walkforward",
        "Decomposed forward-PER walk-forward",
        (
            "low_pbr_score",
            "small_market_cap_score",
            "positive_low_forward_per_component_score",
            "non_positive_forward_per_score",
        ),
        "Walk-forward score using both positive low forward PER and non-positive forward PER.",
    ),
)
LIQUIDITY_SCENARIOS: tuple[LiquidityScenarioSpec, ...] = (
    LiquidityScenarioSpec("none", "No liquidity floor"),
    LiquidityScenarioSpec("adv10m", "ADV60 >= 10mn JPY", min_adv60_mil_jpy=10.0),
)
CONTEXT_RULES: tuple[ContextRuleSpec, ...] = (
    ContextRuleSpec("all_realized", "All realized events", ()),
    ContextRuleSpec("low_pbr", "Low PBR", (("low_pbr_score", 5),)),
    ContextRuleSpec(
        "low_pbr_small_cap",
        "Low PBR + small cap",
        (("low_pbr_score", 5), ("small_market_cap_score", 5)),
    ),
)
REGRESSION_MODELS: tuple[RegressionModelSpec, ...] = (
    RegressionModelSpec(
        "forward_per_regime_core",
        (
            "low_pbr_score",
            "small_market_cap_score",
            "low_adv60_score",
            "positive_low_forward_per_component_score",
            "non_positive_forward_per_score",
        ),
        ("year", "market", "sector_33_name"),
    ),
    RegressionModelSpec(
        "forward_per_regime_extended",
        (
            "low_pbr_score",
            "small_market_cap_score",
            "low_adv60_score",
            "positive_low_forward_per_component_score",
            "non_positive_forward_per_score",
            "high_forecast_dividend_yield_score",
            "high_cfo_yield_score",
            "high_forward_eps_to_actual_eps_score",
        ),
        ("year", "market", "sector_33_name"),
    ),
)


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_selection_fractions(values: Sequence[float]) -> tuple[float, ...]:
    normalized: list[float] = []
    for value in values:
        fraction = float(value)
        if not (0.0 < fraction <= 1.0):
            raise ValueError("selection fractions must satisfy 0 < fraction <= 1")
        if fraction not in normalized:
            normalized.append(fraction)
    if not normalized:
        raise ValueError("at least one selection fraction is required")
    return tuple(sorted(normalized))


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


def _assign_buckets_within_year_market(
    values: pd.Series,
    years: pd.Series,
    markets: pd.Series,
    *,
    bucket_count: int = 5,
) -> pd.Series:
    buckets = pd.Series(np.nan, index=values.index, dtype="float64")
    helper = pd.DataFrame(
        {
            "value": pd.to_numeric(values, errors="coerce"),
            "year": years.astype(str),
            "market": markets.astype(str),
        },
        index=values.index,
    )
    for _, group in helper.groupby(["year", "market"], sort=False):
        valid = group["value"].dropna().sort_values(kind="stable")
        count = len(valid)
        if count < bucket_count:
            continue
        ranks = np.arange(count, dtype=float)
        buckets.loc[valid.index] = (np.floor(ranks * bucket_count / count).astype(int) + 1).astype(float)
    return buckets


def _score_positive_forward_per_within_year_market(frame: pd.DataFrame) -> pd.Series:
    values = pd.to_numeric(frame["forward_per"], errors="coerce")
    scores = pd.Series(np.nan, index=frame.index, dtype="float64")
    for _, group in frame.groupby(["year", "market"], sort=False):
        group_values = values.loc[group.index]
        valid = group_values[(group_values > 0.0) & group_values.map(math.isfinite)]
        count = len(valid)
        if count == 0:
            continue
        if count == 1:
            ranked = pd.Series(0.5, index=valid.index, dtype="float64")
        else:
            ranked = (valid.rank(method="average") - 1.0) / float(count - 1)
        scores.loc[ranked.index] = (1.0 - ranked).astype(float)
    return scores


def _build_prepared_panel_df(
    event_ledger_df: pd.DataFrame,
    *,
    winsor_lower: float,
    winsor_upper: float,
) -> pd.DataFrame:
    panel = _prepare_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
    )
    if panel.empty:
        return panel
    forward_per = pd.to_numeric(panel["forward_per"], errors="coerce")
    finite_forward_per = forward_per.map(math.isfinite)
    positive_forward_per = finite_forward_per & (forward_per > 0.0)
    non_positive_forward_per = finite_forward_per & (forward_per <= 0.0)
    panel["positive_low_forward_per_score"] = _score_positive_forward_per_within_year_market(panel)
    panel["positive_low_forward_per_bucket"] = _assign_buckets_within_year_market(
        panel["positive_low_forward_per_score"],
        panel["year"],
        panel["market"],
    )
    panel["low_pbr_score_bucket"] = _assign_buckets_within_year_market(
        panel["low_pbr_score"],
        panel["year"],
        panel["market"],
    )
    panel["small_market_cap_score_bucket"] = _assign_buckets_within_year_market(
        panel["small_market_cap_score"],
        panel["year"],
        panel["market"],
    )
    panel["forward_per_is_finite"] = finite_forward_per.astype(bool)
    panel["positive_forward_per_flag"] = positive_forward_per.astype(bool)
    panel["non_positive_forward_per_flag"] = non_positive_forward_per.astype(bool)
    panel["non_positive_forward_per_score"] = np.where(
        finite_forward_per,
        non_positive_forward_per.astype(float),
        np.nan,
    )
    panel["positive_low_forward_per_component_score"] = np.where(
        finite_forward_per,
        pd.to_numeric(panel["positive_low_forward_per_score"], errors="coerce").fillna(0.0),
        np.nan,
    )
    regime = pd.Series("missing_or_nonfinite", index=panel.index, dtype="object")
    regime.loc[non_positive_forward_per] = "non_positive"
    regime.loc[positive_forward_per] = "positive_other"
    regime.loc[
        positive_forward_per
        & (pd.to_numeric(panel["positive_low_forward_per_bucket"], errors="coerce") == 5.0)
    ] = "positive_low"
    panel["forward_per_regime"] = regime
    panel["forward_per_regime_label"] = panel["forward_per_regime"].map(_REGIME_LABELS)
    realized = event_ledger_df[event_ledger_df["status"].astype(str) == "realized"].copy()
    extra_columns = [
        "event_id",
        "entry_date",
        "exit_date",
        "entry_open",
        "exit_close",
        "holding_trading_days",
        "avg_trading_value_60d_mil_jpy",
        "market_cap_bil_jpy",
    ]
    for column in extra_columns:
        if column not in realized.columns:
            realized[column] = None
    extras = realized[extra_columns].drop_duplicates("event_id")
    panel = panel.merge(extras, on="event_id", how="left", suffixes=("", "_event"))
    if "avg_trading_value_60d_mil_jpy_event" in panel.columns:
        panel["avg_trading_value_60d_mil_jpy"] = panel["avg_trading_value_60d_mil_jpy"].combine_first(
            panel["avg_trading_value_60d_mil_jpy_event"]
        )
    if "market_cap_bil_jpy_event" in panel.columns:
        panel["market_cap_bil_jpy"] = panel["market_cap_bil_jpy"].combine_first(
            panel["market_cap_bil_jpy_event"]
        )
    return panel.reset_index(drop=True)


def _build_regime_coverage_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "forward_per_regime",
        "forward_per_regime_label",
        "event_count",
        "share_pct",
        "median_annual_names",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _expand_market_scope(panel_df)
    records: list[dict[str, Any]] = []
    for market_scope, group in scoped.groupby("market_scope", observed=True, sort=False):
        total = len(group)
        for regime_name in _REGIME_ORDER:
            regime_group = group[group["forward_per_regime"].astype(str) == regime_name]
            annual_counts = regime_group.groupby("year", sort=True).size()
            records.append(
                {
                    "market_scope": str(market_scope),
                    "forward_per_regime": regime_name,
                    "forward_per_regime_label": _REGIME_LABELS[regime_name],
                    "event_count": int(len(regime_group)),
                    "share_pct": float(len(regime_group) / total * 100.0) if total else None,
                    "median_annual_names": (
                        float(annual_counts.median()) if not annual_counts.empty else None
                    ),
                }
            )
    result = pd.DataFrame(records)
    result["forward_per_regime"] = pd.Categorical(
        result["forward_per_regime"],
        categories=list(_REGIME_ORDER),
        ordered=True,
    )
    return _market_scope_sort(result, ["forward_per_regime"])


def _build_regime_return_summary_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "forward_per_regime",
        "forward_per_regime_label",
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
    scoped = _expand_market_scope(panel_df)
    records: list[dict[str, Any]] = []
    for market_scope, scope_group in scoped.groupby("market_scope", observed=True, sort=False):
        for regime_name in _REGIME_ORDER:
            regime_group = scope_group[scope_group["forward_per_regime"].astype(str) == regime_name].copy()
            annual_counts = regime_group.groupby("year", sort=True).size()
            returns = pd.to_numeric(regime_group["event_return_winsor_pct"], errors="coerce").dropna()
            stats = _annual_selection_stats(regime_group) if not regime_group.empty else _annual_selection_stats(_empty_df(["year", "event_return_winsor_pct"]))
            records.append(
                {
                    "market_scope": str(market_scope),
                    "forward_per_regime": regime_name,
                    "forward_per_regime_label": _REGIME_LABELS[regime_name],
                    "event_count": int(len(regime_group)),
                    "year_count": stats["year_count"],
                    "median_annual_names": (
                        float(annual_counts.median()) if not annual_counts.empty else None
                    ),
                    "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                    "median_return_pct": float(returns.median()) if not returns.empty else None,
                    "win_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else None,
                    **stats,
                }
            )
    result = pd.DataFrame(records)
    result["forward_per_regime"] = pd.Categorical(
        result["forward_per_regime"],
        categories=list(_REGIME_ORDER),
        ordered=True,
    )
    return _market_scope_sort(result, ["forward_per_regime"])


def _build_conditional_regime_summary_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "context_name",
        "context_label",
        "forward_per_regime",
        "forward_per_regime_label",
        "event_count",
        "year_count",
        "median_annual_names",
        "mean_return_pct",
        "annual_mean_return_pct",
        "year_t_stat",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    scoped = _expand_market_scope(panel_df)
    records: list[dict[str, Any]] = []
    for rule in CONTEXT_RULES:
        selection = scoped.copy()
        for score_name, bucket_value in rule.required_score_buckets:
            selection = selection[pd.to_numeric(selection[f"{score_name}_bucket"], errors="coerce") == float(bucket_value)]
        for (market_scope, regime_name), group in selection.groupby(
            ["market_scope", "forward_per_regime"],
            observed=True,
            sort=False,
        ):
            annual_counts = group.groupby("year", sort=True).size()
            returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
            stats = _annual_selection_stats(group)
            records.append(
                {
                    "market_scope": str(market_scope),
                    "context_name": rule.name,
                    "context_label": rule.label,
                    "forward_per_regime": str(regime_name),
                    "forward_per_regime_label": _REGIME_LABELS[str(regime_name)],
                    "event_count": int(len(group)),
                    "year_count": stats["year_count"],
                    "median_annual_names": (
                        float(annual_counts.median()) if not annual_counts.empty else None
                    ),
                    "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                    "annual_mean_return_pct": stats["annual_mean_return_pct"],
                    "year_t_stat": stats["year_t_stat"],
                }
            )
    result = pd.DataFrame(records)
    if result.empty:
        return _empty_df(columns)
    result["forward_per_regime"] = pd.Categorical(
        result["forward_per_regime"],
        categories=list(_REGIME_ORDER),
        ordered=True,
    )
    return _market_scope_sort(result, ["context_name", "forward_per_regime"])


def _frame_for_market_scope(panel_df: pd.DataFrame, market_scope: str) -> pd.DataFrame:
    if market_scope == "all":
        return panel_df.copy()
    return panel_df[panel_df["market"].astype(str) == market_scope].copy()


def _fixed_effects_for_scope(model: RegressionModelSpec, market_scope: str) -> tuple[str, ...]:
    if market_scope == "all":
        return model.fixed_effects
    return tuple(effect for effect in model.fixed_effects if effect != "market")


def _build_panel_regression_df(
    panel_df: pd.DataFrame,
    *,
    min_train_observations: int,
) -> pd.DataFrame:
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
    ]
    if panel_df.empty:
        return _empty_df(columns)
    regression_panel = panel_df[panel_df["forward_per_is_finite"]].copy()
    if regression_panel.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_frame = _frame_for_market_scope(regression_panel, market_scope)
        if scope_frame.empty:
            continue
        for model in REGRESSION_MODELS:
            coef_df, summary = _ols_fit(
                scope_frame,
                y_column="event_return_winsor_pct",
                numeric_columns=model.factor_columns,
                fixed_effect_columns=_fixed_effects_for_scope(model, market_scope),
                min_observations=min_train_observations,
            )
            for row in coef_df.to_dict(orient="records"):
                factor_name = str(row["factor_name"])
                records.append(
                    {
                        "market_scope": market_scope,
                        "model_name": model.name,
                        "factor_name": factor_name,
                        "factor_label": _FACTOR_LABELS.get(factor_name, factor_name),
                        "observation_count": summary.get("nobs"),
                        "r_squared": summary.get("r_squared"),
                        "coefficient_pct_per_1sd": row["coefficient_pct_per_1sd"],
                        "robust_se": row["robust_se"],
                        "t_stat": row["t_stat"],
                    }
                )
    return _market_scope_sort(pd.DataFrame(records), ["model_name", "factor_name"])


def _fit_positive_weights(
    train_df: pd.DataFrame,
    *,
    factor_columns: Sequence[str],
    min_train_observations: int,
) -> tuple[dict[str, float], str, int]:
    clean = train_df[["event_return_winsor_pct", *factor_columns]].copy()
    for column in ["event_return_winsor_pct", *factor_columns]:
        clean[column] = pd.to_numeric(clean[column], errors="coerce")
    clean = clean.dropna()
    if len(clean) < min_train_observations:
        equal = 1.0 / len(factor_columns)
        return {column: equal for column in factor_columns}, "equal_fallback_insufficient_train", int(len(clean))
    coef_df, _summary = _ols_fit(
        clean,
        y_column="event_return_winsor_pct",
        numeric_columns=factor_columns,
        fixed_effect_columns=(),
        min_observations=min_train_observations,
    )
    raw_weights = {column: 0.0 for column in factor_columns}
    for row in coef_df.to_dict(orient="records"):
        factor = str(row["factor_name"])
        value = row.get("coefficient_pct_per_1sd")
        if factor in raw_weights and value is not None:
            raw_weights[factor] = max(0.0, float(cast(float, value)))
    total = sum(raw_weights.values())
    if total <= 0.0 or not math.isfinite(total):
        equal = 1.0 / len(factor_columns)
        return {column: equal for column in factor_columns}, "equal_fallback_non_positive_weights", int(len(clean))
    return {column: weight / total for column, weight in raw_weights.items()}, "ols_positive_normalized", int(len(clean))


def _build_walkforward_weight_df(
    panel_df: pd.DataFrame,
    *,
    min_train_observations: int,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "strategy_name",
        "strategy_label",
        "target_year",
        "train_start_year",
        "train_end_year",
        "train_observation_count",
        "weight_policy",
        "factor_name",
        "factor_label",
        "weight",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    years = sorted(str(year) for year in panel_df["year"].dropna().unique())
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(panel_df, market_scope)
        if scope_df.empty:
            continue
        for strategy in STRATEGY_SPECS:
            for target_year in years:
                train = scope_df[scope_df["year"].astype(str) < target_year].copy()
                train_years = sorted(str(year) for year in train["year"].dropna().unique())
                weights, policy, train_count = _fit_positive_weights(
                    train,
                    factor_columns=strategy.factor_columns,
                    min_train_observations=min_train_observations,
                )
                for factor_name in strategy.factor_columns:
                    records.append(
                        {
                            "market_scope": market_scope,
                            "strategy_name": strategy.name,
                            "strategy_label": strategy.label,
                            "target_year": target_year,
                            "train_start_year": train_years[0] if train_years else None,
                            "train_end_year": train_years[-1] if train_years else None,
                            "train_observation_count": train_count,
                            "weight_policy": policy,
                            "factor_name": factor_name,
                            "factor_label": _FACTOR_LABELS.get(factor_name, factor_name),
                            "weight": weights[factor_name],
                        }
                    )
    return _market_scope_sort(pd.DataFrame(records), ["strategy_name", "target_year", "factor_name"])


def _walkforward_score_for_scope_year(
    group_df: pd.DataFrame,
    weight_df: pd.DataFrame,
    *,
    market_scope: str,
    strategy: StrategySpec,
    year: str,
) -> pd.Series:
    rows = weight_df[
        (weight_df["market_scope"].astype(str) == market_scope)
        & (weight_df["strategy_name"].astype(str) == strategy.name)
        & (weight_df["target_year"].astype(str) == year)
    ]
    if rows.empty:
        equal = 1.0 / len(strategy.factor_columns)
        weights = {column: equal for column in strategy.factor_columns}
    else:
        weights = {
            str(row["factor_name"]): float(cast(float, row["weight"]))
            for row in rows.to_dict(orient="records")
        }
    score = pd.Series(0.0, index=group_df.index, dtype="float64")
    factor_frame = group_df[list(strategy.factor_columns)].apply(pd.to_numeric, errors="coerce")
    for factor_name in strategy.factor_columns:
        score = score + factor_frame[factor_name] * weights[factor_name]
    score.loc[factor_frame.isna().any(axis=1)] = np.nan
    return score


def _passes_liquidity_scenario(frame: pd.DataFrame, scenario: LiquidityScenarioSpec) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    if scenario.min_adv60_mil_jpy is not None:
        adv = pd.to_numeric(frame["avg_trading_value_60d_mil_jpy"], errors="coerce")
        mask &= adv >= scenario.min_adv60_mil_jpy
    return mask


def _select_top_events_for_group(
    group_df: pd.DataFrame,
    *,
    strategy: StrategySpec,
    score_values: pd.Series,
    scenario: LiquidityScenarioSpec,
    selection_fraction: float,
) -> pd.DataFrame:
    eligible = group_df.copy()
    eligible["composite_score"] = score_values.loc[group_df.index]
    eligible = eligible[_passes_liquidity_scenario(eligible, scenario)]
    eligible = eligible[pd.to_numeric(eligible["composite_score"], errors="coerce").notna()].copy()
    eligible_count = int(len(eligible))
    if eligible_count == 0:
        return _empty_df([])
    selection_count = max(1, int(math.ceil(eligible_count * selection_fraction)))
    ranked = eligible.sort_values(["composite_score", "code"], ascending=[False, True], kind="stable").copy()
    ranked["selection_rank"] = np.arange(len(ranked), dtype=int) + 1
    selected = ranked.head(selection_count).copy()
    selected["eligible_count"] = eligible_count
    selected["selection_count_target"] = selection_count
    selected["score_method"] = strategy.name
    selected["score_method_label"] = strategy.label
    selected["liquidity_scenario"] = scenario.name
    selected["liquidity_scenario_label"] = scenario.label
    selected["selection_fraction"] = selection_fraction
    return selected


def _build_selected_event_df(
    panel_df: pd.DataFrame,
    walkforward_weight_df: pd.DataFrame,
    *,
    selection_fractions: Sequence[float],
) -> pd.DataFrame:
    if panel_df.empty:
        return _empty_df([])
    selected_frames: list[pd.DataFrame] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(panel_df, market_scope)
        if scope_df.empty:
            continue
        for year, year_df in scope_df.groupby("year", sort=True):
            year_value = str(year)
            for strategy in STRATEGY_SPECS:
                score_values = _walkforward_score_for_scope_year(
                    year_df,
                    walkforward_weight_df,
                    market_scope=market_scope,
                    strategy=strategy,
                    year=year_value,
                )
                for scenario in LIQUIDITY_SCENARIOS:
                    for fraction in selection_fractions:
                        selected = _select_top_events_for_group(
                            year_df,
                            strategy=strategy,
                            score_values=score_values,
                            scenario=scenario,
                            selection_fraction=float(fraction),
                        )
                        if selected.empty:
                            continue
                        selected["market_scope"] = market_scope
                        selected_frames.append(selected)
    if not selected_frames:
        return _empty_df([])
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
        "eligible_count",
        "selection_count_target",
        "selection_rank",
        "composite_score",
        "event_id",
        "year",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "entry_date",
        "exit_date",
        "entry_open",
        "exit_close",
        "event_return_pct",
        "event_return_winsor_pct",
        "low_pbr_score",
        "small_market_cap_score",
        "low_forward_per_score",
        "positive_low_forward_per_score",
        "positive_low_forward_per_component_score",
        "non_positive_forward_per_score",
        "forward_per",
        "pbr",
        "market_cap_bil_jpy",
        "avg_trading_value_60d_mil_jpy",
        "forward_per_regime",
        "forward_per_regime_label",
    ]
    result = pd.concat(selected_frames, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    result = result[columns]
    return _market_scope_sort(
        result,
        ["score_method", "liquidity_scenario", "selection_fraction", "year", "selection_rank"],
    )


def _series_mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return float(numeric.mean()) if not numeric.empty else None


def _build_selection_mix_df(selected_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
        "forward_per_regime",
        "forward_per_regime_label",
        "event_count",
        "selection_share_pct",
        "mean_return_pct",
        "annual_mean_return_pct",
        "year_t_stat",
        "mean_forward_per",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    total_counts = (
        selected_event_df.groupby(
            ["market_scope", "score_method", "liquidity_scenario", "selection_fraction"],
            observed=True,
            sort=False,
        )
        .size()
        .to_dict()
    )
    records: list[dict[str, Any]] = []
    for keys, group in selected_event_df.groupby(
        [
            "market_scope",
            "score_method",
            "liquidity_scenario",
            "selection_fraction",
            "forward_per_regime",
        ],
        observed=True,
        sort=False,
    ):
        market_scope, score_method, scenario, fraction, regime = keys
        total = total_counts[(market_scope, score_method, scenario, fraction)]
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
        stats = _annual_selection_stats(group)
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": str(group["score_method_label"].iloc[0]),
                "liquidity_scenario": str(scenario),
                "liquidity_scenario_label": str(group["liquidity_scenario_label"].iloc[0]),
                "selection_fraction": float(cast(float, fraction)),
                "forward_per_regime": str(regime),
                "forward_per_regime_label": _REGIME_LABELS[str(regime)],
                "event_count": int(len(group)),
                "selection_share_pct": float(len(group) / total * 100.0) if total else None,
                "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                "annual_mean_return_pct": stats["annual_mean_return_pct"],
                "year_t_stat": stats["year_t_stat"],
                "mean_forward_per": _series_mean(group["forward_per"]),
            }
        )
    result = pd.DataFrame(records)
    result["forward_per_regime"] = pd.Categorical(
        result["forward_per_regime"],
        categories=list(_REGIME_ORDER),
        ordered=True,
    )
    return _market_scope_sort(
        result,
        ["score_method", "liquidity_scenario", "selection_fraction", "forward_per_regime"],
    )


def _build_portfolio_daily_df(
    selected_event_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "liquidity_scenario",
        "selection_fraction",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if selected_event_df.empty or price_df.empty:
        return _empty_df(columns)
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[str, str, str, float, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for event in selected_event_df.to_dict(orient="records"):
        code = str(event["code"])
        price_frame = price_by_code.get(code)
        if price_frame is None:
            continue
        path_df = price_frame[
            (price_frame["date"].astype(str) >= str(event["entry_date"]))
            & (price_frame["date"].astype(str) <= str(event["exit_date"]))
        ].copy()
        if path_df.empty:
            continue
        entry_open = _to_nullable_float(event.get("entry_open"))
        if entry_open is None or entry_open <= 0:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            key = (
                str(event["market_scope"]),
                str(event["score_method"]),
                str(event["liquidity_scenario"]),
                float(cast(float, event["selection_fraction"])),
                str(date_value),
            )
            aggregate[key][0] += float(daily_return)
            aggregate[key][1] += 1.0
    if not aggregate:
        return _empty_df(columns)
    records = [
        {
            "market_scope": market_scope,
            "score_method": score_method,
            "liquidity_scenario": liquidity_scenario,
            "selection_fraction": selection_fraction,
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for (market_scope, score_method, liquidity_scenario, selection_fraction, date_value), values
        in aggregate.items()
    ]
    daily_df = pd.DataFrame(records).sort_values(
        ["market_scope", "score_method", "liquidity_scenario", "selection_fraction", "date"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    for _, group in daily_df.groupby(
        ["market_scope", "score_method", "liquidity_scenario", "selection_fraction"],
        observed=True,
        sort=False,
    ):
        idx = list(group.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df[columns]


def _build_portfolio_regime_contribution_df(
    selected_event_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
        "forward_per_regime",
        "forward_per_regime_label",
        "active_days_with_regime",
        "avg_regime_active_positions",
        "mean_daily_return_contribution_pct",
        "cumulative_simple_contribution_pct",
        "positive_contribution_days_pct",
    ]
    if selected_event_df.empty or price_df.empty:
        return _empty_df(columns)
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    total_aggregate: dict[tuple[str, str, str, float, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    regime_aggregate: dict[tuple[str, str, str, float, str, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    label_lookup: dict[tuple[str, str, str, float], tuple[str, str]] = {}
    for event in selected_event_df.to_dict(orient="records"):
        key_base = (
            str(event["market_scope"]),
            str(event["score_method"]),
            str(event["liquidity_scenario"]),
            float(cast(float, event["selection_fraction"])),
        )
        label_lookup[key_base] = (
            str(event["score_method_label"]),
            str(event["liquidity_scenario_label"]),
        )
        code = str(event["code"])
        price_frame = price_by_code.get(code)
        if price_frame is None:
            continue
        path_df = price_frame[
            (price_frame["date"].astype(str) >= str(event["entry_date"]))
            & (price_frame["date"].astype(str) <= str(event["exit_date"]))
        ].copy()
        if path_df.empty:
            continue
        entry_open = _to_nullable_float(event.get("entry_open"))
        if entry_open is None or entry_open <= 0:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        regime_name = str(event["forward_per_regime"])
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            total_key = (*key_base, str(date_value))
            total_aggregate[total_key][0] += float(daily_return)
            total_aggregate[total_key][1] += 1.0
            regime_key = (*key_base, str(date_value), regime_name)
            regime_aggregate[regime_key][0] += float(daily_return)
            regime_aggregate[regime_key][1] += 1.0
    if not regime_aggregate:
        return _empty_df(columns)
    daily_records: list[dict[str, Any]] = []
    for (
        market_scope,
        score_method,
        liquidity_scenario,
        selection_fraction,
        date_value,
        regime_name,
    ), regime_values in regime_aggregate.items():
        total_values = total_aggregate[(market_scope, score_method, liquidity_scenario, selection_fraction, date_value)]
        total_active_positions = total_values[1]
        if total_active_positions <= 0:
            continue
        daily_records.append(
            {
                "market_scope": market_scope,
                "score_method": score_method,
                "liquidity_scenario": liquidity_scenario,
                "selection_fraction": selection_fraction,
                "date": date_value,
                "forward_per_regime": regime_name,
                "forward_per_regime_label": _REGIME_LABELS[regime_name],
                "regime_active_positions": int(regime_values[1]),
                "daily_return_contribution": float(regime_values[0] / total_active_positions),
            }
        )
    daily_df = pd.DataFrame(daily_records)
    records: list[dict[str, Any]] = []
    for keys, group in daily_df.groupby(
        [
            "market_scope",
            "score_method",
            "liquidity_scenario",
            "selection_fraction",
            "forward_per_regime",
        ],
        observed=True,
        sort=False,
    ):
        market_scope, score_method, liquidity_scenario, selection_fraction, regime_name = keys
        labels = label_lookup[(market_scope, score_method, liquidity_scenario, selection_fraction)]
        contribution = pd.to_numeric(group["daily_return_contribution"], errors="coerce").dropna()
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": labels[0],
                "liquidity_scenario": str(liquidity_scenario),
                "liquidity_scenario_label": labels[1],
                "selection_fraction": float(cast(float, selection_fraction)),
                "forward_per_regime": str(regime_name),
                "forward_per_regime_label": _REGIME_LABELS[str(regime_name)],
                "active_days_with_regime": int(len(group)),
                "avg_regime_active_positions": _series_mean(group["regime_active_positions"]),
                "mean_daily_return_contribution_pct": (
                    float(contribution.mean() * 100.0) if not contribution.empty else None
                ),
                "cumulative_simple_contribution_pct": (
                    float(contribution.sum() * 100.0) if not contribution.empty else None
                ),
                "positive_contribution_days_pct": (
                    float((contribution > 0.0).mean() * 100.0) if not contribution.empty else None
                ),
            }
        )
    result = pd.DataFrame(records)
    result["forward_per_regime"] = pd.Categorical(
        result["forward_per_regime"],
        categories=list(_REGIME_ORDER),
        ordered=True,
    )
    return _market_scope_sort(
        result,
        ["score_method", "liquidity_scenario", "selection_fraction", "forward_per_regime"],
    )


def run_annual_forward_per_regime_decomposition(
    input_bundle_path: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_train_observations: int = DEFAULT_MIN_TRAIN_OBSERVATIONS,
) -> AnnualForwardPerRegimeDecompositionResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_train_observations < 5:
        raise ValueError("min_train_observations must be >= 5")
    normalized_fractions = _normalize_selection_fractions(selection_fractions)
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
    resolved_db_path = str(Path(db_path).expanduser()) if db_path is not None else input_info.db_path
    tables = load_research_bundle_tables(resolved_input, table_names=("event_ledger_df",))
    realized_count = int((tables["event_ledger_df"]["status"].astype(str) == "realized").sum())
    panel_df = _build_prepared_panel_df(
        tables["event_ledger_df"],
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
    )
    walkforward_weight_df = _build_walkforward_weight_df(
        panel_df,
        min_train_observations=min_train_observations,
    )
    selected_event_df = _build_selected_event_df(
        panel_df,
        walkforward_weight_df,
        selection_fractions=normalized_fractions,
    )
    source_mode, source_detail, price_df = _load_price_df(resolved_db_path, selected_event_df)
    portfolio_daily_df = _build_portfolio_daily_df(selected_event_df, price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, selected_event_df)
    return AnnualForwardPerRegimeDecompositionResult(
        db_path=resolved_db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_info.analysis_start_date,
        analysis_end_date=input_info.analysis_end_date,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        selection_fractions=normalized_fractions,
        min_train_observations=min_train_observations,
        input_realized_event_count=realized_count,
        analysis_event_count=int(len(panel_df)),
        finite_forward_per_event_count=int(panel_df["forward_per_is_finite"].sum()) if not panel_df.empty else 0,
        score_policy=(
            "core factor scores are within year x current-market percentiles; "
            "forward PER is decomposed into positive low-forward-PER and non-positive-forward-PER regimes"
        ),
        prepared_panel_df=panel_df,
        regime_coverage_df=_build_regime_coverage_df(panel_df),
        regime_return_summary_df=_build_regime_return_summary_df(panel_df),
        conditional_regime_summary_df=_build_conditional_regime_summary_df(panel_df),
        panel_regression_df=_build_panel_regression_df(
            panel_df,
            min_train_observations=min_train_observations,
        ),
        selected_event_df=selected_event_df,
        selection_mix_df=_build_selection_mix_df(selected_event_df),
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
        portfolio_regime_contribution_df=_build_portfolio_regime_contribution_df(
            selected_event_df,
            price_df,
        ),
    )


def _build_summary_markdown(result: AnnualForwardPerRegimeDecompositionResult) -> str:
    lines = [
        "# Annual Forward PER Regime Decomposition",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Selection fractions: `{', '.join(_fmt(value, 2) for value in result.selection_fractions)}`",
        f"- Input realized events: `{result.input_realized_event_count}`",
        f"- Analysis events: `{result.analysis_event_count}`",
        f"- Finite forward PER events: `{result.finite_forward_per_event_count}`",
        f"- Score policy: {result.score_policy}.",
        "",
        "## Regime Return Snapshot",
        "",
    ]
    regime_summary = result.regime_return_summary_df.copy()
    if regime_summary.empty:
        lines.append("- No regime rows were produced.")
    else:
        for row in regime_summary[
            regime_summary["market_scope"].astype(str).isin(["all", "standard"])
        ].to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['forward_per_regime']}`: "
                f"events `{int(cast(int, row['event_count']))}`, "
                f"mean `{_fmt(row['mean_return_pct'])}%`, "
                f"annual mean `{_fmt(row['annual_mean_return_pct'])}%`, "
                f"year t `{_fmt(row['year_t_stat'])}`"
            )
    lines.extend(["", "## Standard Portfolio Comparison", ""])
    standard_rows = result.portfolio_summary_df[
        result.portfolio_summary_df["market_scope"].astype(str) == "standard"
    ].copy()
    if standard_rows.empty:
        lines.append("- No standard portfolio rows were produced.")
    else:
        standard_rows = standard_rows.sort_values(
            ["selection_fraction", "liquidity_scenario", "sharpe_ratio"],
            ascending=[True, True, False],
            kind="stable",
        )
        for row in standard_rows.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['score_method']}` / `{row['liquidity_scenario']}` / top "
                f"`{_fmt(float(cast(float, row['selection_fraction'])) * 100.0, 0)}%`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"events `{int(cast(int, row['realized_event_count']))}`"
            )
    return "\n".join(lines)


def _build_published_summary(result: AnnualForwardPerRegimeDecompositionResult) -> dict[str, Any]:
    return {
        "inputBundlePath": result.input_bundle_path,
        "inputRunId": result.input_run_id,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "selectionFractions": list(result.selection_fractions),
        "inputRealizedEventCount": result.input_realized_event_count,
        "analysisEventCount": result.analysis_event_count,
        "finiteForwardPerEventCount": result.finite_forward_per_event_count,
        "regimeCoverage": result.regime_coverage_df.to_dict(orient="records"),
        "regimeReturnSummary": result.regime_return_summary_df.to_dict(orient="records"),
        "panelRegression": result.panel_regression_df.to_dict(orient="records"),
        "portfolioSummary": result.portfolio_summary_df.to_dict(orient="records"),
        "selectionMix": result.selection_mix_df.to_dict(orient="records"),
        "portfolioRegimeContribution": result.portfolio_regime_contribution_df.to_dict(orient="records"),
    }


def write_annual_forward_per_regime_decomposition_bundle(
    result: AnnualForwardPerRegimeDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_FORWARD_PER_REGIME_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_forward_per_regime_decomposition",
        params={
            "input_bundle_path": result.input_bundle_path,
            "selection_fractions": list(result.selection_fractions),
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_train_observations": result.min_train_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_forward_per_regime_decomposition_bundle(
    bundle_path: str | Path,
) -> AnnualForwardPerRegimeDecompositionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualForwardPerRegimeDecompositionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_forward_per_regime_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_FORWARD_PER_REGIME_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_forward_per_regime_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_FORWARD_PER_REGIME_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
