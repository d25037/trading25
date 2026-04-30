"""Composite selection research for the annual fundamental panel."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    get_annual_first_open_last_close_fundamental_panel_latest_bundle_path,
    _open_analysis_connection,
    _query_price_rows,
    _to_nullable_float,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    _ols_fit,
    _prepare_panel_df,
    _normalize_required_positive_columns,
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

ANNUAL_VALUE_COMPOSITE_SELECTION_EXPERIMENT_ID = (
    "market-behavior/annual-value-composite-selection"
)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10, 0.15, 0.20)
DEFAULT_MIN_TRAIN_OBSERVATIONS = 80
_CORE_SCORE_COLUMNS: tuple[str, ...] = (
    "low_pbr_score",
    "small_market_cap_score",
    "low_forward_per_score",
)
FIXED_VALUE_COMPOSITE_SCORE_COLUMN = "fixed_55_25_20_score"
FIXED_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    "small_market_cap_score": 0.55,
    "low_pbr_score": 0.25,
    "low_forward_per_score": 0.20,
}
STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    "small_market_cap_score": 0.35,
    "low_pbr_score": 0.40,
    "low_forward_per_score": 0.25,
}
EQUAL_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    column: 1.0 for column in _CORE_SCORE_COLUMNS
}
VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS: tuple[str, ...] = ("pbr", "forward_per")
_CORE_SCORE_LABELS: dict[str, str] = {
    "low_pbr_score": "Low PBR",
    "small_market_cap_score": "Small market cap",
    "low_forward_per_score": "Low forward PER",
}
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "scored_panel_df",
    "walkforward_weight_df",
    "selected_event_df",
    "selection_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class ScoreMethodSpec:
    name: str
    label: str
    score_column: str | None
    description: str


@dataclass(frozen=True)
class LiquidityScenarioSpec:
    name: str
    label: str
    min_adv60_mil_jpy: float | None = None
    min_market_cap_bil_jpy: float | None = None


@dataclass(frozen=True)
class AnnualValueCompositeSelectionResult:
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
    required_positive_columns: tuple[str, ...]
    input_realized_event_count: int
    scored_event_count: int
    score_policy: str
    scored_panel_df: pd.DataFrame
    walkforward_weight_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


SCORE_METHODS: tuple[ScoreMethodSpec, ...] = (
    ScoreMethodSpec(
        "equal_weight",
        "Equal-weight value composite",
        "equal_weight_score",
        "Average of low PBR, small market cap, and low forward PER percentile scores.",
    ),
    ScoreMethodSpec(
        "bucket_sum",
        "Bucket-sum value composite",
        "bucket_sum_score",
        "Average of the three Q1-Q5 preferred-direction buckets scaled to 0-1.",
    ),
    ScoreMethodSpec(
        "fixed_55_25_20",
        "Fixed 55/25/20 value composite",
        FIXED_VALUE_COMPOSITE_SCORE_COLUMN,
        "55% small market cap, 25% low PBR, and 20% low forward PER.",
    ),
    ScoreMethodSpec(
        "pbr_required_equal_weight",
        "Low-PBR-required equal-weight composite",
        "pbr_required_equal_weight_score",
        "Equal-weight composite after requiring low PBR to be in the preferred Q5 bucket.",
    ),
    ScoreMethodSpec(
        "walkforward_regression_weight",
        "Walk-forward regression-weight composite",
        None,
        "Prior-year positive OLS coefficients over the three core scores, normalized each year.",
    ),
)
LIQUIDITY_SCENARIOS: tuple[LiquidityScenarioSpec, ...] = (
    LiquidityScenarioSpec("none", "No liquidity/capacity floor"),
    LiquidityScenarioSpec("adv10m", "ADV60 >= 10mn JPY", min_adv60_mil_jpy=10.0),
    LiquidityScenarioSpec("adv30m", "ADV60 >= 30mn JPY", min_adv60_mil_jpy=30.0),
    LiquidityScenarioSpec("marketcap10b", "Market cap >= 10bn JPY", min_market_cap_bil_jpy=10.0),
    LiquidityScenarioSpec(
        "adv10m_marketcap10b",
        "ADV60 >= 10mn JPY and market cap >= 10bn JPY",
        min_adv60_mil_jpy=10.0,
        min_market_cap_bil_jpy=10.0,
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


def _normalize_score_group_columns(columns: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_column in columns:
        column = str(raw_column).strip()
        if not column:
            raise ValueError("score group columns must not be empty")
        if column not in normalized:
            normalized.append(column)
    return tuple(normalized)


def _score_factor_within_groups(
    frame: pd.DataFrame,
    source_column: str,
    *,
    group_columns: Sequence[str],
    prefer_low: bool,
) -> pd.Series:
    values = pd.to_numeric(frame[source_column], errors="coerce")
    scores = pd.Series(np.nan, index=frame.index, dtype="float64")
    normalized_groups = _normalize_score_group_columns(group_columns)
    groups = (
        frame.groupby(list(normalized_groups), dropna=False, sort=False)
        if normalized_groups
        else ((None, frame),)
    )
    for _, group in groups:
        valid = values.loc[group.index].dropna()
        count = len(valid)
        if count == 0:
            continue
        if count == 1:
            ranked = pd.Series(0.5, index=valid.index, dtype="float64")
        else:
            ranked = (valid.rank(method="average") - 1.0) / float(count - 1)
        if prefer_low:
            ranked = 1.0 - ranked
        scores.loc[ranked.index] = ranked.astype(float)
    return scores


def build_value_composite_score_frame(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str] = ("year", "market"),
    required_positive_columns: Sequence[str] = (),
    score_column: str = FIXED_VALUE_COMPOSITE_SCORE_COLUMN,
    weights: Mapping[str, float] = FIXED_VALUE_COMPOSITE_WEIGHTS,
) -> pd.DataFrame:
    """Apply the research value-composite factor scores to a panel or snapshot."""

    result = frame.copy()
    for column in ("pbr", "forward_per", "market_cap_bil_jpy", *group_columns):
        if column not in result.columns:
            result[column] = np.nan

    normalized_positive_columns = _normalize_required_positive_columns(required_positive_columns)
    for column in normalized_positive_columns:
        result = result[pd.to_numeric(result[column], errors="coerce") > 0].copy()

    for score_name, source_column in (
        ("low_pbr_score", "pbr"),
        ("small_market_cap_score", "market_cap_bil_jpy"),
        ("low_forward_per_score", "forward_per"),
    ):
        result[score_name] = _score_factor_within_groups(
            result,
            source_column,
            group_columns=group_columns,
            prefer_low=True,
        )

    normalized_weights = {str(column): float(weight) for column, weight in weights.items()}
    missing_weight_columns = sorted(set(normalized_weights) - set(_CORE_SCORE_COLUMNS))
    if missing_weight_columns:
        raise ValueError(f"Unsupported value composite score column(s): {missing_weight_columns}")
    weight_sum = sum(normalized_weights.values())
    if not math.isfinite(weight_sum) or weight_sum <= 0:
        raise ValueError("value composite weights must sum to a positive finite value")

    composite = pd.Series(0.0, index=result.index, dtype="float64")
    for column, raw_weight in normalized_weights.items():
        composite = composite + pd.to_numeric(result[column], errors="coerce") * (raw_weight / weight_sum)
    missing = result[list(normalized_weights)].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    composite.loc[missing] = np.nan
    result[score_column] = composite
    return result.reset_index(drop=True)


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


def _assign_preferred_buckets(frame: pd.DataFrame, score_name: str) -> pd.Series:
    values = pd.to_numeric(frame[score_name], errors="coerce")
    buckets = pd.Series(np.nan, index=frame.index, dtype="float64")
    for _, group in frame.groupby(["year", "market"], sort=False):
        valid = values.loc[group.index].dropna().sort_values(kind="stable")
        count = len(valid)
        if count < 5:
            continue
        ranks = np.arange(count, dtype=float)
        buckets.loc[valid.index] = (np.floor(ranks * 5 / count).astype(int) + 1).astype(float)
    return buckets


def _build_scored_panel_df(
    event_ledger_df: pd.DataFrame,
    *,
    winsor_lower: float,
    winsor_upper: float,
    required_positive_columns: Sequence[str] = (),
) -> pd.DataFrame:
    panel = _prepare_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        required_positive_columns=required_positive_columns,
    )
    if panel.empty:
        return panel
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
    panel = build_value_composite_score_frame(
        panel,
        group_columns=("year", "market"),
        required_positive_columns=(),
    )
    for score_name in _CORE_SCORE_COLUMNS:
        panel[f"{score_name}_bucket"] = _assign_preferred_buckets(panel, score_name)
    panel["equal_weight_score"] = panel[list(_CORE_SCORE_COLUMNS)].mean(axis=1, skipna=False)
    bucket_columns = [f"{score_name}_bucket" for score_name in _CORE_SCORE_COLUMNS]
    panel["bucket_sum_score"] = panel[bucket_columns].mean(axis=1, skipna=False) / 5.0
    panel["pbr_required_equal_weight_score"] = np.where(
        panel["low_pbr_score_bucket"] == 5.0,
        panel["equal_weight_score"],
        np.nan,
    )
    return panel.reset_index(drop=True)


def _frame_for_market_scope(panel_df: pd.DataFrame, market_scope: str) -> pd.DataFrame:
    if market_scope == "all":
        return panel_df.copy()
    return panel_df[panel_df["market"].astype(str) == market_scope].copy()


def _fit_walkforward_weights(
    train_df: pd.DataFrame,
    *,
    min_train_observations: int,
) -> tuple[dict[str, float], str, int]:
    clean = train_df[["event_return_winsor_pct", *_CORE_SCORE_COLUMNS]].copy()
    for column in ["event_return_winsor_pct", *_CORE_SCORE_COLUMNS]:
        clean[column] = pd.to_numeric(clean[column], errors="coerce")
    clean = clean.dropna()
    if len(clean) < min_train_observations:
        equal = 1.0 / len(_CORE_SCORE_COLUMNS)
        return {column: equal for column in _CORE_SCORE_COLUMNS}, "equal_fallback_insufficient_train", int(len(clean))
    coef_df, _summary = _ols_fit(
        clean,
        y_column="event_return_winsor_pct",
        numeric_columns=_CORE_SCORE_COLUMNS,
        fixed_effect_columns=(),
        min_observations=min_train_observations,
    )
    raw_weights = {column: 0.0 for column in _CORE_SCORE_COLUMNS}
    for row in coef_df.to_dict(orient="records"):
        factor = str(row["factor_name"])
        value = row.get("coefficient_pct_per_1sd")
        if factor in raw_weights and value is not None:
            coefficient = float(cast(float, value))
            raw_weights[factor] = max(0.0, coefficient)
    total = sum(raw_weights.values())
    if total <= 0.0 or not math.isfinite(total):
        equal = 1.0 / len(_CORE_SCORE_COLUMNS)
        return {column: equal for column in _CORE_SCORE_COLUMNS}, "equal_fallback_non_positive_weights", int(len(clean))
    return {column: weight / total for column, weight in raw_weights.items()}, "ols_positive_normalized", int(len(clean))


def _build_walkforward_weight_df(
    panel_df: pd.DataFrame,
    *,
    min_train_observations: int,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "target_year",
        "train_start_year",
        "train_end_year",
        "train_observation_count",
        "weight_policy",
        *[f"{column}_weight" for column in _CORE_SCORE_COLUMNS],
    ]
    if panel_df.empty:
        return _empty_df(columns)
    years = sorted(str(year) for year in panel_df["year"].dropna().unique())
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(panel_df, market_scope)
        if scope_df.empty:
            continue
        for target_year in years:
            train = scope_df[scope_df["year"].astype(str) < target_year].copy()
            train_years = sorted(str(year) for year in train["year"].dropna().unique())
            weights, policy, train_count = _fit_walkforward_weights(
                train,
                min_train_observations=min_train_observations,
            )
            records.append(
                {
                    "market_scope": market_scope,
                    "target_year": target_year,
                    "train_start_year": train_years[0] if train_years else None,
                    "train_end_year": train_years[-1] if train_years else None,
                    "train_observation_count": train_count,
                    "weight_policy": policy,
                    **{f"{column}_weight": weights[column] for column in _CORE_SCORE_COLUMNS},
                }
            )
    return _market_scope_sort(pd.DataFrame(records), ["target_year"])


def _walkforward_score_for_scope_year(
    panel_df: pd.DataFrame,
    weight_df: pd.DataFrame,
    *,
    market_scope: str,
    year: str,
) -> pd.Series:
    row = weight_df[
        (weight_df["market_scope"].astype(str) == market_scope)
        & (weight_df["target_year"].astype(str) == year)
    ]
    if row.empty:
        equal = 1.0 / len(_CORE_SCORE_COLUMNS)
        weights = {column: equal for column in _CORE_SCORE_COLUMNS}
    else:
        record = row.iloc[0]
        weights = {
            column: float(cast(float, record[f"{column}_weight"]))
            for column in _CORE_SCORE_COLUMNS
        }
    score = pd.Series(0.0, index=panel_df.index, dtype="float64")
    for column, weight in weights.items():
        score = score + pd.to_numeric(panel_df[column], errors="coerce") * weight
    missing = panel_df[list(_CORE_SCORE_COLUMNS)].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    score.loc[missing] = np.nan
    return score


def _passes_liquidity_scenario(frame: pd.DataFrame, scenario: LiquidityScenarioSpec) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    if scenario.min_adv60_mil_jpy is not None:
        adv = pd.to_numeric(frame["avg_trading_value_60d_mil_jpy"], errors="coerce")
        mask &= adv >= scenario.min_adv60_mil_jpy
    if scenario.min_market_cap_bil_jpy is not None:
        market_cap = pd.to_numeric(frame["market_cap_bil_jpy"], errors="coerce")
        mask &= market_cap >= scenario.min_market_cap_bil_jpy
    return mask


def _select_top_events_for_group(
    group_df: pd.DataFrame,
    *,
    score_method: ScoreMethodSpec,
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
    selected["score_method"] = score_method.name
    selected["score_method_label"] = score_method.label
    selected["liquidity_scenario"] = scenario.name
    selected["liquidity_scenario_label"] = scenario.label
    selected["selection_fraction"] = selection_fraction
    return selected


def _build_selected_event_df(
    scored_panel_df: pd.DataFrame,
    walkforward_weight_df: pd.DataFrame,
    *,
    selection_fractions: Sequence[float],
) -> pd.DataFrame:
    if scored_panel_df.empty:
        return _empty_df([])
    selected_frames: list[pd.DataFrame] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(scored_panel_df, market_scope)
        if scope_df.empty:
            continue
        for year, year_df in scope_df.groupby("year", sort=True):
            year_value = str(year)
            for method in SCORE_METHODS:
                if method.name == "walkforward_regression_weight":
                    score_values = _walkforward_score_for_scope_year(
                        year_df,
                        walkforward_weight_df,
                        market_scope=market_scope,
                        year=year_value,
                    )
                elif method.score_column is not None:
                    score_values = pd.to_numeric(year_df[method.score_column], errors="coerce")
                else:
                    continue
                for scenario in LIQUIDITY_SCENARIOS:
                    for fraction in selection_fractions:
                        selected = _select_top_events_for_group(
                            year_df,
                            score_method=method,
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
        "pbr",
        "market_cap_bil_jpy",
        "forward_per",
        "avg_trading_value_60d_mil_jpy",
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
        if year_mean is not None
        and year_std is not None
        and not math.isclose(year_std, 0.0, abs_tol=1e-12)
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


def _build_selection_summary_df(selected_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
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
        "mean_composite_score",
        "mean_adv60_mil_jpy",
        "median_adv60_mil_jpy",
        "mean_market_cap_bil_jpy",
        "median_market_cap_bil_jpy",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    group_columns = ["market_scope", "score_method", "liquidity_scenario", "selection_fraction"]
    for keys, group in selected_event_df.groupby(group_columns, observed=True, sort=False):
        market_scope, score_method, scenario_name, selection_fraction = keys
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
        annual_counts = group.groupby("year", sort=True).size()
        stats = _annual_selection_stats(group)
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": str(group["score_method_label"].iloc[0]),
                "liquidity_scenario": str(scenario_name),
                "liquidity_scenario_label": str(group["liquidity_scenario_label"].iloc[0]),
                "selection_fraction": float(cast(float, selection_fraction)),
                "event_count": int(len(group)),
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
                "mean_composite_score": _series_mean(group["composite_score"]),
                "mean_adv60_mil_jpy": _series_mean(group["avg_trading_value_60d_mil_jpy"]),
                "median_adv60_mil_jpy": _series_median(group["avg_trading_value_60d_mil_jpy"]),
                "mean_market_cap_bil_jpy": _series_mean(group["market_cap_bil_jpy"]),
                "median_market_cap_bil_jpy": _series_median(group["market_cap_bil_jpy"]),
            }
        )
    return _market_scope_sort(
        pd.DataFrame(records),
        ["score_method", "liquidity_scenario", "selection_fraction"],
    )


def _series_mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return float(numeric.mean()) if not numeric.empty else None


def _series_median(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return float(numeric.median()) if not numeric.empty else None


def _load_price_df(
    db_path: str,
    selected_event_df: pd.DataFrame,
) -> tuple[str, str, pd.DataFrame]:
    if selected_event_df.empty:
        return "unknown", "no selected events", _empty_df([])
    start_year = int(str(selected_event_df["year"].min()))
    start_date = f"{start_year - 1:04d}-01-01"
    end_date = str(selected_event_df["exit_date"].max())
    selected_codes = tuple(sorted(set(selected_event_df["code"].astype(str))))
    with _open_analysis_connection(db_path) as ctx:
        price_df = _query_price_rows(
            ctx.connection,
            codes=selected_codes,
            start_date=start_date,
            end_date=end_date,
        )
        return str(ctx.source_mode), ctx.source_detail, price_df


def _daily_stats(values: pd.Series) -> dict[str, float | None]:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if returns.empty:
        return {
            "annualized_volatility_pct": None,
            "sharpe_ratio": None,
            "sortino_ratio": None,
        }
    volatility = float(returns.std(ddof=1) * math.sqrt(252.0) * 100.0) if len(returns) > 1 else None
    std = float(returns.std(ddof=1)) if len(returns) > 1 else None
    sharpe = (
        float(returns.mean()) / std * math.sqrt(252.0)
        if std is not None and not math.isclose(std, 0.0, abs_tol=1e-12)
        else None
    )
    downside = returns[returns < 0.0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else None
    sortino = (
        float(returns.mean()) / downside_std * math.sqrt(252.0)
        if downside_std is not None and not math.isclose(downside_std, 0.0, abs_tol=1e-12)
        else None
    )
    return {
        "annualized_volatility_pct": volatility,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
    }


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


def _build_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    selected_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
        "realized_event_count",
        "start_date",
        "end_date",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
    ]
    if portfolio_daily_df.empty:
        return _empty_df(columns)
    event_counts = (
        selected_event_df.groupby(
            ["market_scope", "score_method", "liquidity_scenario", "selection_fraction"],
            observed=True,
            sort=False,
        )
        .size()
        .to_dict()
    )
    label_lookup = {
        (
            str(row["market_scope"]),
            str(row["score_method"]),
            str(row["liquidity_scenario"]),
            float(cast(float, row["selection_fraction"])),
        ): (str(row["score_method_label"]), str(row["liquidity_scenario_label"]))
        for row in selected_event_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(
        ["market_scope", "score_method", "liquidity_scenario", "selection_fraction"],
        observed=True,
        sort=False,
    ):
        market_scope, score_method, liquidity_scenario, selection_fraction = keys
        start_date = str(group["date"].iloc[0])
        end_date = str(group["date"].iloc[-1])
        total_return = float(group["portfolio_value"].iloc[-1] - 1.0)
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = None
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value) if math.isfinite(cagr_value) else None
        drawdown = pd.to_numeric(group["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = float(drawdown) if pd.notna(drawdown) else None
        daily_stats = _daily_stats(group["mean_daily_return"])
        label_key = (
            str(market_scope),
            str(score_method),
            str(liquidity_scenario),
            float(cast(float, selection_fraction)),
        )
        labels = label_lookup.get(label_key, (str(score_method), str(liquidity_scenario)))
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": labels[0],
                "liquidity_scenario": str(liquidity_scenario),
                "liquidity_scenario_label": labels[1],
                "selection_fraction": float(cast(float, selection_fraction)),
                "realized_event_count": int(event_counts.get(keys, 0)),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group)),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"]).max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                **daily_stats,
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None and max_drawdown_pct is not None and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    return _market_scope_sort(
        pd.DataFrame(records),
        ["score_method", "liquidity_scenario", "selection_fraction"],
    )


def run_annual_value_composite_selection(
    input_bundle_path: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_train_observations: int = DEFAULT_MIN_TRAIN_OBSERVATIONS,
    required_positive_columns: Sequence[str] = (),
) -> AnnualValueCompositeSelectionResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_train_observations < 5:
        raise ValueError("min_train_observations must be >= 5")
    normalized_fractions = _normalize_selection_fractions(selection_fractions)
    normalized_positive_columns = _normalize_required_positive_columns(required_positive_columns)
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
    scored_panel_df = _build_scored_panel_df(
        tables["event_ledger_df"],
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        required_positive_columns=normalized_positive_columns,
    )
    walkforward_weight_df = _build_walkforward_weight_df(
        scored_panel_df,
        min_train_observations=min_train_observations,
    )
    selected_event_df = _build_selected_event_df(
        scored_panel_df,
        walkforward_weight_df,
        selection_fractions=normalized_fractions,
    )
    selection_summary_df = _build_selection_summary_df(selected_event_df)
    source_mode, source_detail, price_df = _load_price_df(resolved_db_path, selected_event_df)
    portfolio_daily_df = _build_portfolio_daily_df(selected_event_df, price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, selected_event_df)
    return AnnualValueCompositeSelectionResult(
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
        required_positive_columns=normalized_positive_columns,
        input_realized_event_count=realized_count,
        scored_event_count=int(len(scored_panel_df)),
        score_policy=(
            "core scores are within year x current-market percentiles; composite uses "
            "low PBR, small market cap, and low forward PER"
            + (
                f"; required positive columns: {', '.join(normalized_positive_columns)}"
                if normalized_positive_columns
                else ""
            )
        ),
        scored_panel_df=scored_panel_df,
        walkforward_weight_df=walkforward_weight_df,
        selected_event_df=selected_event_df,
        selection_summary_df=selection_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
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


def _build_summary_markdown(result: AnnualValueCompositeSelectionResult) -> str:
    lines = [
        "# Annual Value Composite Selection",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Selection fractions: `{', '.join(_fmt(v, 2) for v in result.selection_fractions)}`",
        f"- Input realized events: `{result.input_realized_event_count}`",
        f"- Scored events: `{result.scored_event_count}`",
        (
            "- Required positive columns: "
            f"`{', '.join(result.required_positive_columns)}`"
            if result.required_positive_columns
            else "- Required positive columns: none"
        ),
        f"- Score policy: {result.score_policy}.",
        "",
        "## Top Portfolio Rows",
        "",
    ]
    summary = result.portfolio_summary_df.copy()
    if summary.empty:
        lines.append("- No portfolio summary rows were produced.")
    else:
        focus = summary[
            (summary["market_scope"].astype(str) == "standard")
            & (summary["liquidity_scenario"].astype(str).isin(["none", "adv10m"]))
            & (summary["selection_fraction"].astype(float).isin([0.10, 0.15]))
        ].copy()
        if focus.empty:
            focus = summary.head(12)
        focus = focus.sort_values("sharpe_ratio", ascending=False, na_position="last").head(12)
        for row in focus.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['score_method']}` / "
                f"`{row['liquidity_scenario']}` / top `{float(row['selection_fraction']) * 100:.0f}%`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"events `{int(cast(int, row['realized_event_count']))}`"
            )
    return "\n".join(lines)


def _build_published_summary(result: AnnualValueCompositeSelectionResult) -> dict[str, Any]:
    return {
        "inputBundlePath": result.input_bundle_path,
        "inputRunId": result.input_run_id,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "requiredPositiveColumns": list(result.required_positive_columns),
        "inputRealizedEventCount": result.input_realized_event_count,
        "scoredEventCount": result.scored_event_count,
        "selectionFractions": list(result.selection_fractions),
        "scorePolicy": result.score_policy,
        "selectionSummary": result.selection_summary_df.to_dict(orient="records"),
        "portfolioSummary": result.portfolio_summary_df.to_dict(orient="records"),
    }


def write_annual_value_composite_selection_bundle(
    result: AnnualValueCompositeSelectionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_VALUE_COMPOSITE_SELECTION_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_value_composite_selection",
        params={
            "input_bundle_path": result.input_bundle_path,
            "db_path": result.db_path,
            "selection_fractions": list(result.selection_fractions),
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_train_observations": result.min_train_observations,
            "required_positive_columns": list(result.required_positive_columns),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_value_composite_selection_bundle(
    bundle_path: str | Path,
) -> AnnualValueCompositeSelectionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualValueCompositeSelectionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_value_composite_selection_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_VALUE_COMPOSITE_SELECTION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_value_composite_selection_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_VALUE_COMPOSITE_SELECTION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
