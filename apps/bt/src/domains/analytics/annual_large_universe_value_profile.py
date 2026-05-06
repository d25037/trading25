"""Large-universe value profile research for annual value composites."""

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
from src.domains.analytics.value_composite_scoring import (
    EQUAL_VALUE_COMPOSITE_WEIGHTS,
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    VALUE_COMPOSITE_CORE_SCORE_COLUMNS,
    build_value_composite_score_frame,
)

ANNUAL_LARGE_UNIVERSE_VALUE_PROFILE_EXPERIMENT_ID = (
    "market-behavior/annual-large-universe-value-profile"
)
TOPIX100_SCALE_CATEGORIES: tuple[str, ...] = ("TOPIX Core30", "TOPIX Large70")
TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10)
DEFAULT_MIN_OBSERVATIONS = 60
_CORE_SCORE_COLUMNS = VALUE_COMPOSITE_CORE_SCORE_COLUMNS
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "large_universe_scored_panel_df",
    "factor_regression_df",
    "factor_bucket_summary_df",
    "selected_event_df",
    "selection_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
    "profile_summary_df",
    "universe_coverage_df",
)
_FACTOR_SOURCE_COLUMNS: dict[str, str] = {
    "low_pbr_score": "pbr",
    "small_market_cap_score": "market_cap_bil_jpy",
    "low_forward_per_score": "forward_per",
}
_FACTOR_LABELS: dict[str, str] = {
    "low_pbr_score": "Low PBR",
    "small_market_cap_score": "Small market cap",
    "low_forward_per_score": "Low forward PER",
}


@dataclass(frozen=True)
class LargeUniverseSpec:
    name: str
    label: str
    scale_categories: tuple[str, ...]


@dataclass(frozen=True)
class WeightProfileSpec:
    name: str
    label: str
    weights: Mapping[str, float]


@dataclass(frozen=True)
class AnnualLargeUniverseValueProfileResult:
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
    large_universe_event_count: int
    analysis_policy: str
    large_universe_scored_panel_df: pd.DataFrame
    factor_regression_df: pd.DataFrame
    factor_bucket_summary_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame
    profile_summary_df: pd.DataFrame
    universe_coverage_df: pd.DataFrame


LARGE_UNIVERSE_SPECS: tuple[LargeUniverseSpec, ...] = (
    LargeUniverseSpec("topix100", "TOPIX100", TOPIX100_SCALE_CATEGORIES),
    LargeUniverseSpec("topix500", "TOPIX500", TOPIX500_SCALE_CATEGORIES),
)
DEFAULT_WEIGHT_PROFILES: tuple[WeightProfileSpec, ...] = (
    WeightProfileSpec(
        "equal_weight",
        "Equal value composite",
        EQUAL_VALUE_COMPOSITE_WEIGHTS,
    ),
    WeightProfileSpec(
        "prime_size_tilt",
        "Prime size/forward-PER tilt",
        PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    ),
    WeightProfileSpec(
        "standard_pbr_tilt",
        "Standard PBR tilt",
        STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    ),
    WeightProfileSpec(
        "small_forward_per_50_50",
        "Small cap 50% / low forward PER 50%",
        {"small_market_cap_score": 0.50, "low_forward_per_score": 0.50},
    ),
    WeightProfileSpec(
        "pbr_forward_per_50_50",
        "Low PBR 50% / low forward PER 50%",
        {"low_pbr_score": 0.50, "low_forward_per_score": 0.50},
    ),
    WeightProfileSpec(
        "small_only",
        "Small cap only",
        {"small_market_cap_score": 1.0},
    ),
    WeightProfileSpec(
        "low_pbr_only",
        "Low PBR only",
        {"low_pbr_score": 1.0},
    ),
    WeightProfileSpec(
        "low_forward_per_only",
        "Low forward PER only",
        {"low_forward_per_score": 1.0},
    ),
)
_NO_LIQUIDITY_FLOOR = LiquidityScenarioSpec("none", "No liquidity/capacity floor")


def run_annual_large_universe_value_profile(
    input_bundle_path: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    required_positive_columns: Sequence[str] = POSITIVE_RATIO_ONLY_COLUMNS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> AnnualLargeUniverseValueProfileResult:
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
    scored_panel_df = _build_large_universe_scored_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        required_positive_columns=normalized_positive_columns,
    )
    factor_regression_df = _build_factor_regression_df(
        scored_panel_df,
        min_observations=min_observations,
    )
    factor_bucket_summary_df = _build_factor_bucket_summary_df(
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
    universe_coverage_df = _build_universe_coverage_df(scored_panel_df)
    return AnnualLargeUniverseValueProfileResult(
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
        large_universe_event_count=int(len(scored_panel_df)),
        analysis_policy=(
            "entry-date stock_master_daily scale_category is used as the PIT universe source; "
            "TOPIX100 is Core30+Large70 and TOPIX500 is Core30+Large70+Mid400; "
            "value scores are re-ranked within year x large_universe instead of reusing "
            "Prime/Standard ranks"
        ),
        large_universe_scored_panel_df=scored_panel_df,
        factor_regression_df=factor_regression_df,
        factor_bucket_summary_df=factor_bucket_summary_df,
        selected_event_df=selected_event_df,
        selection_summary_df=selection_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
        profile_summary_df=profile_summary_df,
        universe_coverage_df=universe_coverage_df,
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


def _build_large_universe_scored_panel_df(
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
    expanded = build_value_composite_score_frame(
        expanded,
        group_columns=("year", "large_universe"),
        required_positive_columns=(),
    )
    for score_name in _CORE_SCORE_COLUMNS:
        expanded[f"{score_name}_bucket"] = _assign_preferred_buckets(
            expanded,
            score_name,
            group_columns=("year", "large_universe"),
        )
    expanded["equal_weight_score"] = expanded[list(_CORE_SCORE_COLUMNS)].mean(
        axis=1,
        skipna=False,
    )
    bucket_columns = [f"{score_name}_bucket" for score_name in _CORE_SCORE_COLUMNS]
    expanded["bucket_sum_score"] = expanded[bucket_columns].mean(axis=1, skipna=False) / 5.0
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
        "pbr",
        "market_cap_bil_jpy",
        "forward_per",
        "avg_trading_value_60d_mil_jpy",
        "equal_weight_score",
        "bucket_sum_score",
        *_CORE_SCORE_COLUMNS,
        *bucket_columns,
    ]
    for column in keep_columns:
        if column not in expanded.columns:
            expanded[column] = None
    return expanded[keep_columns].sort_values(
        ["large_universe", "year", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _assign_preferred_buckets(
    frame: pd.DataFrame,
    score_name: str,
    *,
    group_columns: Sequence[str],
) -> pd.Series:
    values = pd.to_numeric(frame[score_name], errors="coerce")
    buckets = pd.Series(np.nan, index=frame.index, dtype="float64")
    for _, group in frame.groupby(list(group_columns), sort=False):
        valid = values.loc[group.index].dropna().sort_values(kind="stable")
        count = len(valid)
        if count < 5:
            continue
        ranks = np.arange(count, dtype=float)
        buckets.loc[valid.index] = (np.floor(ranks * 5 / count).astype(int) + 1).astype(float)
    return buckets


def _normalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    normalized = {str(column): float(weight) for column, weight in weights.items()}
    unsupported = sorted(set(normalized) - set(_CORE_SCORE_COLUMNS))
    if unsupported:
        raise ValueError(f"Unsupported score column(s): {unsupported}")
    total = sum(normalized.values())
    if not math.isfinite(total) or total <= 0.0:
        raise ValueError("weights must sum to a positive finite value")
    return {column: weight / total for column, weight in normalized.items()}


def _score_values(frame: pd.DataFrame, weights: Mapping[str, float]) -> pd.Series:
    normalized = _normalize_weights(weights)
    score = pd.Series(0.0, index=frame.index, dtype="float64")
    for column, weight in normalized.items():
        score = score + pd.to_numeric(frame[column], errors="coerce") * weight
    missing = frame[list(normalized)].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    score.loc[missing] = np.nan
    return score


def _score_method_for_profile(profile: WeightProfileSpec) -> ScoreMethodSpec:
    weights = _normalize_weights(profile.weights)
    return ScoreMethodSpec(
        name=profile.name,
        label=profile.label,
        score_column=None,
        description=(
            f"Large-universe fixed score: PBR {weights.get('low_pbr_score', 0.0) * 100:.1f}%, "
            f"size {weights.get('small_market_cap_score', 0.0) * 100:.1f}%, "
            f"forward PER {weights.get('low_forward_per_score', 0.0) * 100:.1f}%."
        ),
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
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
        "eligible_count",
        "selection_count_target",
        "selection_rank",
        "composite_score",
        "low_pbr_weight",
        "small_market_cap_weight",
        "low_forward_per_weight",
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
        "low_pbr_score",
        "small_market_cap_score",
        "low_forward_per_score",
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
            for profile in DEFAULT_WEIGHT_PROFILES:
                weights = _normalize_weights(profile.weights)
                method = _score_method_for_profile(profile)
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
                    selected["low_pbr_weight"] = weights.get("low_pbr_score", 0.0)
                    selected["small_market_cap_weight"] = weights.get("small_market_cap_score", 0.0)
                    selected["low_forward_per_weight"] = weights.get(
                        "low_forward_per_score",
                        0.0,
                    )
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
        coef_df, summary = _ols_fit(
            group,
            y_column="event_return_winsor_pct",
            numeric_columns=_CORE_SCORE_COLUMNS,
            fixed_effect_columns=("year", "sector_33_name"),
            min_observations=min_observations,
        )
        for row in coef_df.to_dict(orient="records"):
            records.append(
                {
                    "large_universe": str(universe_name),
                    "model_name": "core_value_scores_year_sector_fe",
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
    return pd.DataFrame(records, columns=columns)


def _build_factor_bucket_summary_df(
    scored_panel_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "large_universe",
        "factor_name",
        "factor_label",
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
        for factor_name in _CORE_SCORE_COLUMNS:
            bucket_column = f"{factor_name}_bucket"
            source_column = _FACTOR_SOURCE_COLUMNS[factor_name]
            for bucket in range(1, 6):
                group = universe_df[universe_df[bucket_column] == float(bucket)]
                returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
                has_min = len(returns) >= min_observations
                records.append(
                    {
                        "large_universe": str(universe_name),
                        "factor_name": factor_name,
                        "factor_label": _FACTOR_LABELS[factor_name],
                        "bucket": bucket,
                        "event_count": int(len(group)),
                        "mean_return_pct": float(returns.mean()) if has_min else None,
                        "median_return_pct": float(returns.median()) if has_min else None,
                        "win_rate_pct": float((returns > 0.0).mean() * 100.0) if has_min else None,
                        "median_source_value": _series_median(group[source_column]),
                    }
                )
    return pd.DataFrame(records, columns=columns)


def _build_profile_summary_df(
    selected_event_df: pd.DataFrame,
    selection_summary_df: pd.DataFrame,
    portfolio_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "large_universe",
        "score_method",
        "score_method_label",
        "selection_fraction",
        "low_pbr_weight",
        "small_market_cap_weight",
        "low_forward_per_weight",
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
        "median_low_pbr_score",
        "median_small_market_cap_score",
        "median_low_forward_per_score",
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
                "selection_fraction": float(cast(float, selection_fraction)),
                "low_pbr_weight": float(cast(float, first["low_pbr_weight"])),
                "small_market_cap_weight": float(cast(float, first["small_market_cap_weight"])),
                "low_forward_per_weight": float(cast(float, first["low_forward_per_weight"])),
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
                "median_low_pbr_score": _series_median(group["low_pbr_score"]),
                "median_small_market_cap_score": _series_median(group["small_market_cap_score"]),
                "median_low_forward_per_score": _series_median(group["low_forward_per_score"]),
            }
        )
    return pd.DataFrame(records, columns=columns).sort_values(
        ["large_universe", "selection_fraction", "sharpe_ratio"],
        ascending=[True, True, False],
        na_position="last",
        kind="stable",
    ).reset_index(drop=True)


def _build_universe_coverage_df(scored_panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "large_universe",
        "year",
        "event_count",
        "unique_code_count",
        "core30_count",
        "large70_count",
        "mid400_count",
        "prime_count",
        "standard_count",
        "median_market_cap_bil_jpy",
        "median_adv60_mil_jpy",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for (universe_name, year), group in scored_panel_df.groupby(["large_universe", "year"], sort=False):
        scale = group["scale_category"].astype(str)
        market = group["market"].astype(str)
        records.append(
            {
                "large_universe": str(universe_name),
                "year": str(year),
                "event_count": int(len(group)),
                "unique_code_count": int(group["code"].astype(str).nunique()),
                "core30_count": int((scale == "TOPIX Core30").sum()),
                "large70_count": int((scale == "TOPIX Large70").sum()),
                "mid400_count": int((scale == "TOPIX Mid400").sum()),
                "prime_count": int((market == "prime").sum()),
                "standard_count": int((market == "standard").sum()),
                "median_market_cap_bil_jpy": _series_median(group["market_cap_bil_jpy"]),
                "median_adv60_mil_jpy": _series_median(group["avg_trading_value_60d_mil_jpy"]),
            }
        )
    return pd.DataFrame(records, columns=columns)


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


def _build_summary_markdown(result: AnnualLargeUniverseValueProfileResult) -> str:
    lines = [
        "# Annual Large-Universe Value Profile",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Selection fractions: `{', '.join(_fmt(v, 2) for v in result.selection_fractions)}`",
        f"- Required positive columns: `{', '.join(result.required_positive_columns)}`",
        f"- Input realized events: `{result.input_realized_event_count}`",
        f"- Large-universe event rows: `{result.large_universe_event_count}`",
        f"- Analysis policy: {result.analysis_policy}.",
        "",
        "## Factor Regression",
        "",
    ]
    if result.factor_regression_df.empty:
        lines.append("- No factor regression rows were produced.")
    else:
        for row in result.factor_regression_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['large_universe']}` / `{row['factor_name']}`: "
                f"coef `{_fmt(row['coefficient_pct_per_1sd'])}pp`, "
                f"t `{_fmt(row['t_stat'])}`, "
                f"n `{int(cast(int, row['observation_count']))}`"
            )
    lines.extend(["", "## Top Portfolio Profiles", ""])
    focus = result.profile_summary_df[
        result.profile_summary_df["selection_fraction"].astype(float).isin([0.05, 0.10])
    ].copy()
    if focus.empty:
        lines.append("- No profile summary rows were produced.")
    else:
        focus = focus.sort_values(
            ["large_universe", "selection_fraction", "sharpe_ratio"],
            ascending=[True, True, False],
            na_position="last",
            kind="stable",
        )
        for row in focus.groupby(["large_universe", "selection_fraction"], sort=False).head(4).to_dict(
            orient="records"
        ):
            lines.append(
                "- "
                f"`{row['large_universe']}` / top `{float(row['selection_fraction']) * 100:.0f}%` / "
                f"`{row['score_method']}`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"weights PBR/size/fPER "
                f"`{_fmt(row['low_pbr_weight'] * 100, 1)} / "
                f"{_fmt(row['small_market_cap_weight'] * 100, 1)} / "
                f"{_fmt(row['low_forward_per_weight'] * 100, 1)}`"
            )
    return "\n".join(lines)


def write_annual_large_universe_value_profile_bundle(
    result: AnnualLargeUniverseValueProfileResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_LARGE_UNIVERSE_VALUE_PROFILE_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_large_universe_value_profile",
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


def load_annual_large_universe_value_profile_bundle(
    bundle_path: str | Path,
) -> AnnualLargeUniverseValueProfileResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualLargeUniverseValueProfileResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_large_universe_value_profile_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_LARGE_UNIVERSE_VALUE_PROFILE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_large_universe_value_profile_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_LARGE_UNIVERSE_VALUE_PROFILE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
