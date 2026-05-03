"""Prime top-slice PBR absorption research."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_prime_value_technical_risk_decomposition import (
    DEFAULT_MARKET_SCOPE,
)
from src.domains.analytics.annual_value_composite_selection import (
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    LiquidityScenarioSpec,
    ScoreMethodSpec,
    get_annual_value_composite_selection_latest_bundle_path,
    load_annual_value_composite_selection_bundle,
    _build_portfolio_daily_df,
    _build_portfolio_summary_df,
    _build_selection_summary_df,
    _empty_df,
    _load_price_df,
    _select_top_events_for_group,
    _series_median,
)
from src.domains.analytics.annual_value_technical_feature_importance import (
    DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
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

ANNUAL_PRIME_VALUE_PBR_ABSORPTION_EXPERIMENT_ID = (
    "market-behavior/annual-prime-value-pbr-absorption"
)
DEFAULT_SELECTION_FRACTION = 0.05
DEFAULT_PBR_WEIGHTS: tuple[float, ...] = (0.0, 0.05, 0.10, 0.20, 1.0 / 3.0)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "weighted_selected_event_df",
    "weight_sensitivity_summary_df",
    "selection_overlap_df",
    "pbr_swap_profile_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)
_CORE_SCORE_COLUMNS: tuple[str, ...] = (
    "low_pbr_score",
    "small_market_cap_score",
    "low_forward_per_score",
)


@dataclass(frozen=True)
class AnnualPrimeValuePbrAbsorptionResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_scope: str
    selection_fraction: float
    liquidity_scenario: str
    pbr_weights: tuple[float, ...]
    baseline_pbr_weight: float
    selected_event_count: int
    analysis_policy: str
    weighted_selected_event_df: pd.DataFrame
    weight_sensitivity_summary_df: pd.DataFrame
    selection_overlap_df: pd.DataFrame
    pbr_swap_profile_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def run_annual_prime_value_pbr_absorption(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    market_scope: str = DEFAULT_MARKET_SCOPE,
    selection_fraction: float = DEFAULT_SELECTION_FRACTION,
    liquidity_scenario: str = DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    pbr_weights: Sequence[float] = DEFAULT_PBR_WEIGHTS,
    baseline_pbr_weight: float = 0.05,
) -> AnnualPrimeValuePbrAbsorptionResult:
    normalized_fraction = float(selection_fraction)
    if not (0.0 < normalized_fraction <= 1.0):
        raise ValueError("selection_fraction must satisfy 0 < fraction <= 1")
    normalized_pbr_weights = _normalize_pbr_weights(pbr_weights)
    if round(float(baseline_pbr_weight), 6) not in {round(value, 6) for value in normalized_pbr_weights}:
        raise ValueError("baseline_pbr_weight must be included in pbr_weights")
    scenario = _resolve_liquidity_scenario(liquidity_scenario)
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_annual_value_composite_selection_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Annual value-composite selection bundle was not found. "
            "Run run_annual_value_composite_selection.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    value_result = load_annual_value_composite_selection_bundle(resolved_input)
    selected_event_df = _build_weighted_selected_event_df(
        value_result.scored_panel_df,
        market_scope=market_scope,
        selection_fraction=normalized_fraction,
        scenario=scenario,
        pbr_weights=normalized_pbr_weights,
    )
    selection_summary_df = _build_selection_summary_df(selected_event_df)
    source_mode, source_detail, price_df = _load_price_df(value_result.db_path, selected_event_df)
    portfolio_daily_df = _build_portfolio_daily_df(selected_event_df, price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, selected_event_df)
    weight_sensitivity_summary_df = _build_weight_sensitivity_summary_df(
        selected_event_df,
        selection_summary_df,
        portfolio_summary_df,
    )
    selection_overlap_df = _build_selection_overlap_df(
        selected_event_df,
        baseline_pbr_weight=float(baseline_pbr_weight),
    )
    pbr_swap_profile_df = _build_pbr_swap_profile_df(
        selected_event_df,
        baseline_pbr_weight=float(baseline_pbr_weight),
    )
    return AnnualPrimeValuePbrAbsorptionResult(
        db_path=value_result.db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=value_result.analysis_start_date,
        analysis_end_date=value_result.analysis_end_date,
        market_scope=str(market_scope),
        selection_fraction=normalized_fraction,
        liquidity_scenario=scenario.name,
        pbr_weights=normalized_pbr_weights,
        baseline_pbr_weight=float(baseline_pbr_weight),
        selected_event_count=int(len(selected_event_df)),
        analysis_policy=(
            "within the existing v3 annual-value-composite scored panel, Prime annual top-slice "
            "selection is re-run with fixed PBR weights; the remaining weight is split between "
            "small market cap and low forward PER using the current prime_size_tilt non-PBR ratio; "
            "portfolio lens reuses equal-weight annual open-to-close daily close paths"
        ),
        weighted_selected_event_df=selected_event_df,
        weight_sensitivity_summary_df=weight_sensitivity_summary_df,
        selection_overlap_df=selection_overlap_df,
        pbr_swap_profile_df=pbr_swap_profile_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _normalize_pbr_weights(values: Sequence[float]) -> tuple[float, ...]:
    normalized: list[float] = []
    for value in values:
        weight = float(value)
        if not (0.0 <= weight < 1.0):
            raise ValueError("pbr_weights must satisfy 0 <= weight < 1")
        rounded = round(weight, 6)
        if rounded not in {round(existing, 6) for existing in normalized}:
            normalized.append(weight)
    if not normalized:
        raise ValueError("pbr_weights must not be empty")
    return tuple(sorted(normalized))


def _resolve_liquidity_scenario(name: str) -> LiquidityScenarioSpec:
    # This research defaults to no floor. Keep a local resolver to avoid adding a
    # public dependency surface to annual_value_composite_selection.
    scenarios: tuple[LiquidityScenarioSpec, ...] = (
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
    for scenario in scenarios:
        if scenario.name == str(name):
            return scenario
    raise ValueError(f"Unknown liquidity scenario: {name}")


def _weights_for_pbr_weight(pbr_weight: float) -> dict[str, float]:
    non_pbr_total = (
        PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS["small_market_cap_score"]
        + PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS["low_forward_per_score"]
    )
    remaining = 1.0 - float(pbr_weight)
    return {
        "low_pbr_score": float(pbr_weight),
        "small_market_cap_score": remaining
        * PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS["small_market_cap_score"]
        / non_pbr_total,
        "low_forward_per_score": remaining
        * PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS["low_forward_per_score"]
        / non_pbr_total,
    }


def _score_method_for_pbr_weight(pbr_weight: float) -> ScoreMethodSpec:
    weights = _weights_for_pbr_weight(pbr_weight)
    return ScoreMethodSpec(
        name=_score_method_name(pbr_weight),
        label=(
            f"PBR {weights['low_pbr_score'] * 100:.1f}% / "
            f"size {weights['small_market_cap_score'] * 100:.1f}% / "
            f"forward PER {weights['low_forward_per_score'] * 100:.1f}%"
        ),
        score_column=None,
        description="Prime PBR absorption sensitivity score.",
    )


def _score_method_name(pbr_weight: float) -> str:
    return f"pbr_weight_{int(round(float(pbr_weight) * 100)):02d}"


def _score_values(panel_df: pd.DataFrame, pbr_weight: float) -> pd.Series:
    weights = _weights_for_pbr_weight(pbr_weight)
    score = pd.Series(0.0, index=panel_df.index, dtype="float64")
    for column, weight in weights.items():
        score = score + pd.to_numeric(panel_df[column], errors="coerce") * weight
    missing = panel_df[list(_CORE_SCORE_COLUMNS)].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    score.loc[missing] = np.nan
    return score


def _build_weighted_selected_event_df(
    scored_panel_df: pd.DataFrame,
    *,
    market_scope: str,
    selection_fraction: float,
    scenario: LiquidityScenarioSpec,
    pbr_weights: Sequence[float],
) -> pd.DataFrame:
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
        "pbr_weight",
        "small_market_cap_weight",
        "low_forward_per_weight",
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
    if scored_panel_df.empty:
        return _empty_df(columns)
    scope_df = scored_panel_df[scored_panel_df["market"].astype(str) == str(market_scope)].copy()
    if scope_df.empty:
        return _empty_df(columns)
    selected_frames: list[pd.DataFrame] = []
    for year, year_df in scope_df.groupby("year", sort=True):
        _ = year
        for pbr_weight in pbr_weights:
            method = _score_method_for_pbr_weight(float(pbr_weight))
            selected = _select_top_events_for_group(
                year_df,
                score_method=method,
                score_values=_score_values(year_df, float(pbr_weight)),
                scenario=scenario,
                selection_fraction=float(selection_fraction),
            )
            if selected.empty:
                continue
            weights = _weights_for_pbr_weight(float(pbr_weight))
            selected["market_scope"] = str(market_scope)
            selected["pbr_weight"] = weights["low_pbr_score"]
            selected["small_market_cap_weight"] = weights["small_market_cap_score"]
            selected["low_forward_per_weight"] = weights["low_forward_per_score"]
            selected_frames.append(selected)
    if not selected_frames:
        return _empty_df(columns)
    result = pd.concat(selected_frames, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[columns].sort_values(
        ["market_scope", "pbr_weight", "year", "selection_rank"],
        kind="stable",
    ).reset_index(drop=True)


def _build_weight_sensitivity_summary_df(
    selected_event_df: pd.DataFrame,
    selection_summary_df: pd.DataFrame,
    portfolio_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "score_method",
        "pbr_weight",
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
    records: list[dict[str, Any]] = []
    summary_lookup = {
        str(row["score_method"]): row for row in selection_summary_df.to_dict(orient="records")
    }
    portfolio_lookup = {
        str(row["score_method"]): row for row in portfolio_summary_df.to_dict(orient="records")
    }
    for score_method, group in selected_event_df.groupby("score_method", sort=False):
        first = group.iloc[0]
        selection = summary_lookup.get(str(score_method), {})
        portfolio = portfolio_lookup.get(str(score_method), {})
        records.append(
            {
                "score_method": str(score_method),
                "pbr_weight": float(cast(float, first["pbr_weight"])),
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
    return pd.DataFrame(records, columns=columns).sort_values("pbr_weight", kind="stable").reset_index(drop=True)


def _build_selection_overlap_df(
    selected_event_df: pd.DataFrame,
    *,
    baseline_pbr_weight: float,
) -> pd.DataFrame:
    columns = [
        "comparison",
        "score_method",
        "baseline_score_method",
        "year",
        "baseline_count",
        "variant_count",
        "intersection_count",
        "added_count",
        "dropped_count",
        "overlap_pct_of_baseline",
        "jaccard_pct",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    baseline_method = _score_method_name(baseline_pbr_weight)
    baseline = selected_event_df[selected_event_df["score_method"].astype(str) == baseline_method].copy()
    if baseline.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    years = sorted(str(year) for year in selected_event_df["year"].dropna().unique())
    for score_method, variant in selected_event_df.groupby("score_method", sort=False):
        score_method_str = str(score_method)
        for year in years:
            base_ids = set(
                baseline[baseline["year"].astype(str) == year]["event_id"].astype(str)
            )
            variant_ids = set(
                variant[variant["year"].astype(str) == year]["event_id"].astype(str)
            )
            records.append(
                _overlap_record(
                    comparison="year",
                    score_method=score_method_str,
                    baseline_method=baseline_method,
                    year=year,
                    baseline_ids=base_ids,
                    variant_ids=variant_ids,
                )
            )
        records.append(
            _overlap_record(
                comparison="all_years",
                score_method=score_method_str,
                baseline_method=baseline_method,
                year="all",
                baseline_ids=set(baseline["event_id"].astype(str)),
                variant_ids=set(variant["event_id"].astype(str)),
            )
        )
    return pd.DataFrame(records, columns=columns)


def _overlap_record(
    *,
    comparison: str,
    score_method: str,
    baseline_method: str,
    year: str,
    baseline_ids: set[str],
    variant_ids: set[str],
) -> dict[str, Any]:
    intersection = baseline_ids & variant_ids
    union = baseline_ids | variant_ids
    return {
        "comparison": comparison,
        "score_method": score_method,
        "baseline_score_method": baseline_method,
        "year": year,
        "baseline_count": int(len(baseline_ids)),
        "variant_count": int(len(variant_ids)),
        "intersection_count": int(len(intersection)),
        "added_count": int(len(variant_ids - baseline_ids)),
        "dropped_count": int(len(baseline_ids - variant_ids)),
        "overlap_pct_of_baseline": (
            float(len(intersection) / len(baseline_ids) * 100.0) if baseline_ids else np.nan
        ),
        "jaccard_pct": float(len(intersection) / len(union) * 100.0) if union else np.nan,
    }


def _build_pbr_swap_profile_df(
    selected_event_df: pd.DataFrame,
    *,
    baseline_pbr_weight: float,
) -> pd.DataFrame:
    columns = [
        "score_method",
        "baseline_score_method",
        "direction",
        "event_count",
        "year_count",
        "mean_return_pct",
        "median_return_pct",
        "p10_return_pct",
        "win_rate_pct",
        "median_pbr",
        "median_market_cap_bil_jpy",
        "median_forward_per",
        "median_low_pbr_score",
        "median_small_market_cap_score",
        "median_low_forward_per_score",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    baseline_method = _score_method_name(baseline_pbr_weight)
    baseline = selected_event_df[selected_event_df["score_method"].astype(str) == baseline_method].copy()
    if baseline.empty:
        return _empty_df(columns)
    baseline_ids = set(baseline["event_id"].astype(str))
    records: list[dict[str, Any]] = []
    for score_method, variant in selected_event_df.groupby("score_method", sort=False):
        score_method_str = str(score_method)
        variant_ids = set(variant["event_id"].astype(str))
        frames = (
            ("kept", variant[variant["event_id"].astype(str).isin(baseline_ids & variant_ids)]),
            ("added_by_variant", variant[variant["event_id"].astype(str).isin(variant_ids - baseline_ids)]),
            ("dropped_from_baseline", baseline[baseline["event_id"].astype(str).isin(baseline_ids - variant_ids)]),
        )
        for direction, frame in frames:
            records.append(
                _swap_profile_record(
                    score_method=score_method_str,
                    baseline_method=baseline_method,
                    direction=direction,
                    frame=frame,
                )
            )
    return pd.DataFrame(records, columns=columns)


def _swap_profile_record(
    *,
    score_method: str,
    baseline_method: str,
    direction: str,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    returns = pd.to_numeric(frame["event_return_winsor_pct"], errors="coerce").dropna()
    return {
        "score_method": score_method,
        "baseline_score_method": baseline_method,
        "direction": direction,
        "event_count": int(len(frame)),
        "year_count": int(frame["year"].nunique()) if "year" in frame.columns else 0,
        "mean_return_pct": float(returns.mean()) if not returns.empty else np.nan,
        "median_return_pct": float(returns.median()) if not returns.empty else np.nan,
        "p10_return_pct": float(returns.quantile(0.10)) if not returns.empty else np.nan,
        "win_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else np.nan,
        "median_pbr": _series_median(frame["pbr"]) if "pbr" in frame.columns else None,
        "median_market_cap_bil_jpy": _series_median(frame["market_cap_bil_jpy"])
        if "market_cap_bil_jpy" in frame.columns
        else None,
        "median_forward_per": _series_median(frame["forward_per"]) if "forward_per" in frame.columns else None,
        "median_low_pbr_score": _series_median(frame["low_pbr_score"])
        if "low_pbr_score" in frame.columns
        else None,
        "median_small_market_cap_score": _series_median(frame["small_market_cap_score"])
        if "small_market_cap_score" in frame.columns
        else None,
        "median_low_forward_per_score": _series_median(frame["low_forward_per_score"])
        if "low_forward_per_score" in frame.columns
        else None,
    }


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


def _build_summary_markdown(result: AnnualPrimeValuePbrAbsorptionResult) -> str:
    lines = [
        "# Annual Prime Value PBR Absorption",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Market scope: `{result.market_scope}`",
        f"- Selection fraction: `{result.selection_fraction:.2f}`",
        f"- Liquidity scenario: `{result.liquidity_scenario}`",
        f"- PBR weights: `{', '.join(_fmt(value, 3) for value in result.pbr_weights)}`",
        f"- Baseline PBR weight: `{_fmt(result.baseline_pbr_weight, 3)}`",
        f"- Selected event rows: `{result.selected_event_count}`",
        f"- Analysis policy: {result.analysis_policy}.",
        "",
        "## Weight Sensitivity",
        "",
    ]
    if result.weight_sensitivity_summary_df.empty:
        lines.append("- No sensitivity rows were produced.")
    else:
        for row in result.weight_sensitivity_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"PBR `{_fmt(row['pbr_weight'] * 100, 1)}%`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"annual mean `{_fmt(row['annual_mean_return_pct'])}%`, "
                f"median PBR `{_fmt(row['median_pbr'])}`, "
                f"median mcap `{_fmt(row['median_market_cap_bil_jpy'])}bn`, "
                f"median forward PER `{_fmt(row['median_forward_per'])}`"
            )
    lines.extend(["", "## Overlap vs Baseline", ""])
    overlap = result.selection_overlap_df[
        result.selection_overlap_df["comparison"].astype(str) == "all_years"
    ].copy()
    if overlap.empty:
        lines.append("- No overlap rows were produced.")
    else:
        for row in overlap.sort_values("score_method").to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['score_method']}` vs `{row['baseline_score_method']}`: "
                f"overlap `{_fmt(row['overlap_pct_of_baseline'])}%`, "
                f"added `{int(cast(int, row['added_count']))}`, "
                f"dropped `{int(cast(int, row['dropped_count']))}`"
            )
    return "\n".join(lines)


def write_annual_prime_value_pbr_absorption_bundle(
    result: AnnualPrimeValuePbrAbsorptionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_PRIME_VALUE_PBR_ABSORPTION_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_prime_value_pbr_absorption",
        params={
            "input_bundle_path": result.input_bundle_path,
            "market_scope": result.market_scope,
            "selection_fraction": result.selection_fraction,
            "liquidity_scenario": result.liquidity_scenario,
            "pbr_weights": list(result.pbr_weights),
            "baseline_pbr_weight": result.baseline_pbr_weight,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_prime_value_pbr_absorption_bundle(
    bundle_path: str | Path,
) -> AnnualPrimeValuePbrAbsorptionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualPrimeValuePbrAbsorptionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_prime_value_pbr_absorption_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_PRIME_VALUE_PBR_ABSORPTION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_prime_value_pbr_absorption_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_PRIME_VALUE_PBR_ABSORPTION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
