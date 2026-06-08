"""Large-universe fundamental factor-family research."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    get_annual_first_open_last_close_fundamental_panel_latest_bundle_path,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    POSITIVE_RATIO_ONLY_COLUMNS,
    _normalize_required_positive_columns,
    _ols_fit,
    _winsorize,
)
from src.domains.analytics.annual_large_universe_value_profile import (
    LARGE_UNIVERSE_SPECS,
)
from src.domains.analytics.research_core.factor_scoring import (
    assign_ordered_buckets,
    score_within_groups,
)
from src.domains.analytics.annual_value_composite_selection import (
    LiquidityScenarioSpec,
    ScoreMethodSpec,
    _build_portfolio_daily_df,
    _build_portfolio_summary_df,
    _build_selection_summary_df,
    _empty_df,
    _load_price_df,
    _select_top_events_for_group,
    _series_median,
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

ANNUAL_LARGE_UNIVERSE_FACTOR_FAMILY_EXPERIMENT_ID = (
    "market-behavior/annual-large-universe-factor-family"
)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10)
DEFAULT_MIN_OBSERVATIONS = 60
CORE_VALUE_SCORE_COLUMNS: tuple[str, ...] = (
    "low_forward_per_score",
    "low_pbr_score",
    "small_market_cap_score",
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "factor_scored_panel_df",
    "factor_bucket_summary_df",
    "factor_regression_df",
    "selected_event_df",
    "selection_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
    "profile_summary_df",
)


@dataclass(frozen=True)
class FundamentalFactorSpec:
    score_name: str
    label: str
    source_column: str
    prefer_low: bool
    family: str


@dataclass(frozen=True)
class AnnualLargeUniverseFactorFamilyResult:
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
    required_positive_columns: tuple[str, ...]
    min_observations: int
    input_realized_event_count: int
    factor_scored_event_count: int
    analysis_policy: str
    factor_scored_panel_df: pd.DataFrame
    factor_bucket_summary_df: pd.DataFrame
    factor_regression_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame
    profile_summary_df: pd.DataFrame


FUNDAMENTAL_FACTOR_SPECS: tuple[FundamentalFactorSpec, ...] = (
    FundamentalFactorSpec("low_forward_per_score", "Low forward PER", "forward_per", True, "valuation"),
    FundamentalFactorSpec("low_per_score", "Low PER", "per", True, "valuation"),
    FundamentalFactorSpec("low_pbr_score", "Low PBR", "pbr", True, "valuation"),
    FundamentalFactorSpec("small_market_cap_score", "Small market cap", "market_cap_bil_jpy", True, "size"),
    FundamentalFactorSpec("high_cfo_yield_score", "High CFO yield", "cfo_yield_pct", False, "yield"),
    FundamentalFactorSpec("high_fcf_yield_score", "High FCF yield", "fcf_yield_pct", False, "yield"),
    FundamentalFactorSpec(
        "high_dividend_yield_score",
        "High dividend yield",
        "dividend_yield_pct",
        False,
        "yield",
    ),
    FundamentalFactorSpec(
        "high_forecast_dividend_yield_score",
        "High forecast dividend yield",
        "forecast_dividend_yield_pct",
        False,
        "yield",
    ),
    FundamentalFactorSpec("high_roe_score", "High ROE", "roe_pct", False, "quality"),
    FundamentalFactorSpec("high_roa_score", "High ROA", "roa_pct", False, "quality"),
    FundamentalFactorSpec(
        "high_operating_margin_score",
        "High operating margin",
        "operating_margin_pct",
        False,
        "quality",
    ),
    FundamentalFactorSpec("high_net_margin_score", "High net margin", "net_margin_pct", False, "quality"),
    FundamentalFactorSpec("high_cfo_margin_score", "High CFO margin", "cfo_margin_pct", False, "quality"),
    FundamentalFactorSpec("high_fcf_margin_score", "High FCF margin", "fcf_margin_pct", False, "quality"),
    FundamentalFactorSpec(
        "high_equity_ratio_score",
        "High equity ratio",
        "equity_ratio_pct",
        False,
        "quality",
    ),
    FundamentalFactorSpec(
        "high_cfo_to_net_profit_score",
        "High CFO / net profit",
        "cfo_to_net_profit_ratio",
        False,
        "cash_conversion",
    ),
    FundamentalFactorSpec(
        "high_payout_ratio_score",
        "High payout ratio",
        "payout_ratio_pct",
        False,
        "payout",
    ),
    FundamentalFactorSpec(
        "high_forecast_payout_ratio_score",
        "High forecast payout ratio",
        "forecast_payout_ratio_pct",
        False,
        "payout",
    ),
    FundamentalFactorSpec(
        "high_forward_eps_to_actual_eps_score",
        "High forward EPS / actual EPS",
        "forward_eps_to_actual_eps",
        False,
        "forecast_quality",
    ),
)
_FACTOR_BY_SCORE = {spec.score_name: spec for spec in FUNDAMENTAL_FACTOR_SPECS}
_NO_LIQUIDITY_FLOOR = LiquidityScenarioSpec("none", "No liquidity/capacity floor")


def run_annual_large_universe_factor_family(
    input_bundle_path: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    required_positive_columns: Sequence[str] = POSITIVE_RATIO_ONLY_COLUMNS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> AnnualLargeUniverseFactorFamilyResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_observations < 5:
        raise ValueError("min_observations must be >= 5")
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
    event_ledger_df = tables["event_ledger_df"]
    realized_count = int((event_ledger_df["status"].astype(str) == "realized").sum())
    scored_panel_df = _build_factor_scored_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        required_positive_columns=normalized_positive_columns,
    )
    factor_bucket_summary_df = _build_factor_bucket_summary_df(
        scored_panel_df,
        min_observations=min_observations,
    )
    factor_regression_df = _build_factor_regression_df(
        scored_panel_df,
        min_observations=min_observations,
    )
    selected_event_df = _build_selected_event_df(
        scored_panel_df,
        selection_fractions=normalized_fractions,
    )
    selection_summary_df = _build_selection_summary_df(selected_event_df)
    source_mode, source_detail, price_df = _load_price_df(resolved_db_path, selected_event_df)
    portfolio_daily_df = _build_portfolio_daily_df(selected_event_df, price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, selected_event_df)
    profile_summary_df = _build_profile_summary_df(
        selected_event_df,
        selection_summary_df,
        portfolio_summary_df,
    )
    return AnnualLargeUniverseFactorFamilyResult(
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
        required_positive_columns=normalized_positive_columns,
        min_observations=int(min_observations),
        input_realized_event_count=realized_count,
        factor_scored_event_count=int(len(scored_panel_df)),
        analysis_policy=(
            "entry-date stock_master_daily scale_category defines TOPIX100/TOPIX500; "
            "all factor scores are re-ranked within year x large_universe; regression tests "
            "single-factor and core-plus-candidate effects; portfolio lens compares single "
            "factors and 50/50 low-forward-PER overlays"
        ),
        factor_scored_panel_df=scored_panel_df,
        factor_bucket_summary_df=factor_bucket_summary_df,
        factor_regression_df=factor_regression_df,
        selected_event_df=selected_event_df,
        selection_summary_df=selection_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
        profile_summary_df=profile_summary_df,
    )


def _normalize_selection_fractions(values: Sequence[float]) -> tuple[float, ...]:
    normalized: list[float] = []
    for value in values:
        fraction = float(value)
        if not (0.0 < fraction <= 1.0):
            raise ValueError("selection fractions must satisfy 0 < fraction <= 1")
        rounded = round(fraction, 6)
        if rounded not in {round(existing, 6) for existing in normalized}:
            normalized.append(fraction)
    if not normalized:
        raise ValueError("at least one selection fraction is required")
    return tuple(sorted(normalized))


def _build_factor_scored_panel_df(
    event_ledger_df: pd.DataFrame,
    *,
    winsor_lower: float,
    winsor_upper: float,
    required_positive_columns: Sequence[str],
) -> pd.DataFrame:
    required_columns = {
        "status",
        "event_id",
        "year",
        "code",
        "market",
        "scale_category",
        "sector_33_name",
        "event_return_pct",
    }
    missing = sorted(required_columns - set(event_ledger_df.columns))
    if missing:
        raise ValueError(f"Input event_ledger_df is missing required columns: {missing}")
    realized = event_ledger_df[event_ledger_df["status"].astype(str) == "realized"].copy()
    for column in required_positive_columns:
        if column not in realized.columns:
            realized[column] = np.nan
        realized = realized[pd.to_numeric(realized[column], errors="coerce") > 0].copy()
    if realized.empty:
        return _empty_df([])
    realized["year"] = realized["year"].astype(str)
    realized["scale_category"] = realized["scale_category"].fillna("").astype(str)
    realized["event_return_pct"] = pd.to_numeric(realized["event_return_pct"], errors="coerce")
    realized["event_return_winsor_pct"] = _winsorize(
        realized["event_return_pct"],
        winsor_lower,
        winsor_upper,
    )
    expanded_frames: list[pd.DataFrame] = []
    for spec in LARGE_UNIVERSE_SPECS:
        scope = realized[realized["scale_category"].isin(spec.scale_categories)].copy()
        if scope.empty:
            continue
        scope["large_universe"] = spec.name
        scope["large_universe_label"] = spec.label
        expanded_frames.append(scope)
    if not expanded_frames:
        return _empty_df([])
    expanded = pd.concat(expanded_frames, ignore_index=True)
    for spec in FUNDAMENTAL_FACTOR_SPECS:
        if spec.source_column not in expanded.columns:
            expanded[spec.source_column] = np.nan
        expanded[spec.score_name] = score_within_groups(
            expanded,
            spec.source_column,
            group_columns=("year", "large_universe"),
            prefer_low=spec.prefer_low,
        )
        expanded[f"{spec.score_name}_bucket"] = assign_ordered_buckets(
            expanded,
            spec.score_name,
            group_columns=("year", "large_universe"),
        )
    keep_columns = [
        "large_universe",
        "large_universe_label",
        "event_id",
        "year",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "scale_category",
        "entry_date",
        "exit_date",
        "entry_open",
        "exit_close",
        "event_return_pct",
        "event_return_winsor_pct",
        "avg_trading_value_60d_mil_jpy",
        *[spec.source_column for spec in FUNDAMENTAL_FACTOR_SPECS],
        *[spec.score_name for spec in FUNDAMENTAL_FACTOR_SPECS],
        *[f"{spec.score_name}_bucket" for spec in FUNDAMENTAL_FACTOR_SPECS],
    ]
    for column in keep_columns:
        if column not in expanded.columns:
            expanded[column] = None
    return expanded[keep_columns].sort_values(
        ["large_universe", "year", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _build_factor_bucket_summary_df(
    scored_panel_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "large_universe",
        "factor_name",
        "factor_label",
        "factor_family",
        "bucket",
        "event_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "median_source_value",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for universe_name, universe_df in scored_panel_df.groupby("large_universe", sort=False):
        for spec in FUNDAMENTAL_FACTOR_SPECS:
            bucket_column = f"{spec.score_name}_bucket"
            for bucket in range(1, 6):
                group = universe_df[universe_df[bucket_column] == float(bucket)]
                returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
                has_min = len(returns) >= min_observations
                records.append(
                    {
                        "large_universe": str(universe_name),
                        "factor_name": spec.score_name,
                        "factor_label": spec.label,
                        "factor_family": spec.family,
                        "bucket": bucket,
                        "event_count": int(len(group)),
                        "mean_return_pct": float(returns.mean()) if has_min else None,
                        "median_return_pct": float(returns.median()) if has_min else None,
                        "win_rate_pct": float((returns > 0.0).mean() * 100.0) if has_min else None,
                        "median_source_value": _series_median(group[spec.source_column]),
                    }
                )
    return pd.DataFrame(records, columns=columns)


def _build_factor_regression_df(
    scored_panel_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "large_universe",
        "model_name",
        "factor_name",
        "factor_label",
        "factor_family",
        "observation_count",
        "r_squared",
        "coefficient_pct_per_1sd",
        "robust_se",
        "t_stat",
        "p_value_normal_approx",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for universe_name, group in scored_panel_df.groupby("large_universe", sort=False):
        for spec in FUNDAMENTAL_FACTOR_SPECS:
            for model_name, columns_for_model in _regression_models_for_factor(spec.score_name):
                coef_df, summary = _ols_fit(
                    group,
                    y_column="event_return_winsor_pct",
                    numeric_columns=columns_for_model,
                    fixed_effect_columns=("year", "sector_33_name"),
                    min_observations=min_observations,
                )
                factor_rows = coef_df[coef_df["factor_name"].astype(str) == spec.score_name]
                for row in factor_rows.to_dict(orient="records"):
                    records.append(
                        {
                            "large_universe": str(universe_name),
                            "model_name": model_name,
                            "factor_name": spec.score_name,
                            "factor_label": spec.label,
                            "factor_family": spec.family,
                            "observation_count": summary.get("nobs"),
                            "r_squared": summary.get("r_squared"),
                            "coefficient_pct_per_1sd": row["coefficient_pct_per_1sd"],
                            "robust_se": row["robust_se"],
                            "t_stat": row["t_stat"],
                            "p_value_normal_approx": row["p_value_normal_approx"],
                        }
                    )
    return pd.DataFrame(records, columns=columns)


def _regression_models_for_factor(score_name: str) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if score_name in CORE_VALUE_SCORE_COLUMNS:
        return (
            ("single_factor_year_sector_fe", (score_name,)),
            ("core_value_year_sector_fe", CORE_VALUE_SCORE_COLUMNS),
        )
    return (
        ("single_factor_year_sector_fe", (score_name,)),
        ("core_value_plus_candidate_year_sector_fe", (*CORE_VALUE_SCORE_COLUMNS, score_name)),
    )


def _score_values(frame: pd.DataFrame, weights: Mapping[str, float]) -> pd.Series:
    normalized = _normalize_weights(weights)
    score = pd.Series(0.0, index=frame.index, dtype="float64")
    for column, weight in normalized.items():
        score = score + pd.to_numeric(frame[column], errors="coerce") * weight
    missing = frame[list(normalized)].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    score.loc[missing] = np.nan
    return score


def _normalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    normalized = {str(column): float(weight) for column, weight in weights.items()}
    supported = {spec.score_name for spec in FUNDAMENTAL_FACTOR_SPECS}
    unsupported = sorted(set(normalized) - supported)
    if unsupported:
        raise ValueError(f"Unsupported score column(s): {unsupported}")
    total = sum(normalized.values())
    if not math.isfinite(total) or total <= 0.0:
        raise ValueError("weights must sum to a positive finite value")
    return {column: weight / total for column, weight in normalized.items()}


def _score_method_for_weights(name: str, label: str, weights: Mapping[str, float]) -> ScoreMethodSpec:
    _ = _normalize_weights(weights)
    return ScoreMethodSpec(
        name=name,
        label=label,
        score_column=None,
        description="Large-universe factor-family selection profile.",
    )


def _build_selected_event_df(
    scored_panel_df: pd.DataFrame,
    *,
    selection_fractions: Sequence[float],
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "large_universe",
        "large_universe_label",
        "score_method",
        "score_method_label",
        "factor_name",
        "factor_family",
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
        "scale_category",
        "entry_date",
        "exit_date",
        "entry_open",
        "exit_close",
        "event_return_pct",
        "event_return_winsor_pct",
        "pbr",
        "market_cap_bil_jpy",
        "forward_per",
        "avg_trading_value_60d_mil_jpy",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    selected_frames: list[pd.DataFrame] = []
    for universe_name, universe_df in scored_panel_df.groupby("large_universe", sort=False):
        for year, year_df in universe_df.groupby("year", sort=True):
            _ = year
            for name, label, factor_name, factor_family, weights in _selection_profiles():
                method = _score_method_for_weights(name, label, weights)
                score_values = _score_values(year_df, weights)
                for fraction in selection_fractions:
                    selected = _select_top_events_for_group(
                        year_df,
                        score_method=method,
                        score_values=score_values,
                        scenario=_NO_LIQUIDITY_FLOOR,
                        selection_fraction=float(fraction),
                    )
                    if selected.empty:
                        continue
                    selected["market_scope"] = str(universe_name)
                    selected["large_universe"] = str(universe_name)
                    selected["large_universe_label"] = str(year_df["large_universe_label"].iloc[0])
                    selected["factor_name"] = factor_name
                    selected["factor_family"] = factor_family
                    selected_frames.append(selected)
    if not selected_frames:
        return _empty_df(columns)
    result = pd.concat(selected_frames, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[columns].sort_values(
        ["large_universe", "score_method", "selection_fraction", "year", "selection_rank"],
        kind="stable",
    ).reset_index(drop=True)


def _selection_profiles() -> list[tuple[str, str, str, str, dict[str, float]]]:
    profiles: list[tuple[str, str, str, str, dict[str, float]]] = []
    for spec in FUNDAMENTAL_FACTOR_SPECS:
        profiles.append(
            (
                f"single_{spec.score_name}",
                spec.label,
                spec.score_name,
                spec.family,
                {spec.score_name: 1.0},
            )
        )
        if spec.score_name != "low_forward_per_score":
            profiles.append(
                (
                    f"forward_per_plus_{spec.score_name}",
                    f"Low forward PER 50% + {spec.label} 50%",
                    spec.score_name,
                    spec.family,
                    {"low_forward_per_score": 0.50, spec.score_name: 0.50},
                )
            )
    return profiles


def _build_profile_summary_df(
    selected_event_df: pd.DataFrame,
    selection_summary_df: pd.DataFrame,
    portfolio_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "large_universe",
        "score_method",
        "score_method_label",
        "factor_name",
        "factor_family",
        "selection_fraction",
        "event_count",
        "eligible_median",
        "median_annual_names",
        "mean_return_pct",
        "annual_mean_return_pct",
        "year_t_stat",
        "cagr_pct",
        "sharpe_ratio",
        "max_drawdown_pct",
        "median_pbr",
        "median_market_cap_bil_jpy",
        "median_forward_per",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    selection_lookup = {
        (
            str(row["market_scope"]),
            str(row["score_method"]),
            float(cast(float, row["selection_fraction"])),
        ): row
        for row in selection_summary_df.to_dict(orient="records")
    }
    portfolio_lookup = {
        (
            str(row["market_scope"]),
            str(row["score_method"]),
            float(cast(float, row["selection_fraction"])),
        ): row
        for row in portfolio_summary_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    for keys, group in selected_event_df.groupby(
        ["large_universe", "score_method", "selection_fraction"],
        sort=False,
    ):
        universe_name, score_method, selection_fraction = keys
        first = group.iloc[0]
        lookup_key = (str(universe_name), str(score_method), float(cast(float, selection_fraction)))
        selection = selection_lookup.get(lookup_key, {})
        portfolio = portfolio_lookup.get(lookup_key, {})
        records.append(
            {
                "large_universe": str(universe_name),
                "score_method": str(score_method),
                "score_method_label": str(first["score_method_label"]),
                "factor_name": str(first["factor_name"]),
                "factor_family": str(first["factor_family"]),
                "selection_fraction": float(cast(float, selection_fraction)),
                "event_count": int(len(group)),
                "eligible_median": _series_median(group["eligible_count"]),
                "median_annual_names": selection.get("median_annual_names"),
                "mean_return_pct": selection.get("mean_return_pct"),
                "annual_mean_return_pct": selection.get("annual_mean_return_pct"),
                "year_t_stat": selection.get("year_t_stat"),
                "cagr_pct": portfolio.get("cagr_pct"),
                "sharpe_ratio": portfolio.get("sharpe_ratio"),
                "max_drawdown_pct": portfolio.get("max_drawdown_pct"),
                "median_pbr": _series_median(group["pbr"]),
                "median_market_cap_bil_jpy": _series_median(group["market_cap_bil_jpy"]),
                "median_forward_per": _series_median(group["forward_per"]),
            }
        )
    return pd.DataFrame(records, columns=columns).sort_values(
        ["large_universe", "selection_fraction", "sharpe_ratio"],
        ascending=[True, True, False],
        na_position="last",
        kind="stable",
    ).reset_index(drop=True)


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


def _build_summary_markdown(result: AnnualLargeUniverseFactorFamilyResult) -> str:
    lines = [
        "# Annual Large-Universe Factor Family",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Selection fractions: `{', '.join(_fmt(v, 2) for v in result.selection_fractions)}`",
        f"- Required positive columns: `{', '.join(result.required_positive_columns)}`",
        f"- Input realized events: `{result.input_realized_event_count}`",
        f"- Large-universe scored rows: `{result.factor_scored_event_count}`",
        f"- Analysis policy: {result.analysis_policy}.",
        "",
        "## Core-Plus-Candidate Regression Leaders",
        "",
    ]
    core_plus = result.factor_regression_df[
        result.factor_regression_df["model_name"].astype(str).str.contains("core")
    ].copy()
    if core_plus.empty:
        lines.append("- No regression rows were produced.")
    else:
        leaders = core_plus.sort_values(
            ["large_universe", "coefficient_pct_per_1sd"],
            ascending=[True, False],
            na_position="last",
            kind="stable",
        )
        for row in leaders.groupby("large_universe", sort=False).head(8).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['large_universe']}` / `{row['factor_name']}`: "
                f"coef `{_fmt(row['coefficient_pct_per_1sd'])}pp`, "
                f"t `{_fmt(row['t_stat'])}`, "
                f"n `{int(cast(int, row['observation_count']))}`"
            )
    lines.extend(["", "## Top Portfolio Profiles", ""])
    if result.profile_summary_df.empty:
        lines.append("- No profile summary rows were produced.")
    else:
        focus = result.profile_summary_df.sort_values(
            ["large_universe", "selection_fraction", "sharpe_ratio"],
            ascending=[True, True, False],
            na_position="last",
            kind="stable",
        )
        for row in focus.groupby(["large_universe", "selection_fraction"], sort=False).head(8).to_dict(
            orient="records"
        ):
            lines.append(
                "- "
                f"`{row['large_universe']}` / top `{float(row['selection_fraction']) * 100:.0f}%` / "
                f"`{row['score_method']}`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"events `{int(cast(int, row['event_count']))}`"
            )
    return "\n".join(lines)


def write_annual_large_universe_factor_family_bundle(
    result: AnnualLargeUniverseFactorFamilyResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_LARGE_UNIVERSE_FACTOR_FAMILY_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_large_universe_factor_family",
        params={
            "input_bundle_path": result.input_bundle_path,
            "db_path": result.db_path,
            "selection_fractions": list(result.selection_fractions),
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "required_positive_columns": list(result.required_positive_columns),
            "min_observations": result.min_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_large_universe_factor_family_bundle(
    bundle_path: str | Path,
) -> AnnualLargeUniverseFactorFamilyResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualLargeUniverseFactorFamilyResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_large_universe_factor_family_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_LARGE_UNIVERSE_FACTOR_FAMILY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_large_universe_factor_family_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_LARGE_UNIVERSE_FACTOR_FAMILY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
