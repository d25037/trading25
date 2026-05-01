"""Sector-relative value composite research for the annual fundamental panel."""

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
    _normalize_required_positive_columns,
    _prepare_panel_df,
)
from src.domains.analytics.annual_value_composite_selection import (
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    _build_portfolio_daily_df,
    _build_portfolio_summary_df,
    _build_selection_summary_df,
    _frame_for_market_scope,
    _load_price_df,
    _market_scope_sort,
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

ANNUAL_SECTOR_RELATIVE_VALUE_COMPOSITE_EXPERIMENT_ID = (
    "market-behavior/annual-sector-relative-value-composite"
)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10, 0.15)
DEFAULT_MIN_SECTOR_OBSERVATIONS = 5
VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS: tuple[str, ...] = ("pbr", "forward_per")
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")
_CORE_COMPONENTS: tuple[str, ...] = (
    "small_market_cap_score",
    "low_pbr_score",
    "low_forward_per_score",
)
_EQUAL_WEIGHTS: dict[str, float] = {component: 1.0 for component in _CORE_COMPONENTS}
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "scored_panel_df",
    "score_coverage_df",
    "selected_event_df",
    "selection_summary_df",
    "sector_exposure_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class ScoreMethodSpec:
    name: str
    label: str
    pbr_score_column: str
    forward_per_score_column: str
    weights: Mapping[str, float]
    description: str


@dataclass(frozen=True)
class LiquidityScenarioSpec:
    name: str
    label: str
    min_adv60_mil_jpy: float | None = None
    min_market_cap_bil_jpy: float | None = None


@dataclass(frozen=True)
class AnnualSectorRelativeValueCompositeResult:
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
    min_sector_observations: int
    required_positive_columns: tuple[str, ...]
    input_realized_event_count: int
    scored_event_count: int
    score_policy: str
    scored_panel_df: pd.DataFrame
    score_coverage_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    sector_exposure_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


SCORE_METHODS: tuple[ScoreMethodSpec, ...] = (
    ScoreMethodSpec(
        "equal_raw",
        "Equal raw value",
        "low_pbr_score",
        "low_forward_per_score",
        _EQUAL_WEIGHTS,
        "Equal-weight raw low PBR, raw small cap, and raw low forward PER.",
    ),
    ScoreMethodSpec(
        "equal_sector_relative",
        "Equal sector-relative value",
        "sector_low_pbr_score",
        "sector_low_forward_per_score",
        _EQUAL_WEIGHTS,
        "Equal-weight raw small cap plus sector-relative low PBR and low forward PER.",
    ),
    ScoreMethodSpec(
        "equal_sector_relative_pbr",
        "Equal sector-relative PBR",
        "sector_low_pbr_score",
        "low_forward_per_score",
        _EQUAL_WEIGHTS,
        "Equal-weight raw small cap and low forward PER, with only PBR sector-relative.",
    ),
    ScoreMethodSpec(
        "equal_sector_relative_forward_per",
        "Equal sector-relative forward PER",
        "low_pbr_score",
        "sector_low_forward_per_score",
        _EQUAL_WEIGHTS,
        "Equal-weight raw small cap and low PBR, with only forward PER sector-relative.",
    ),
    ScoreMethodSpec(
        "equal_hybrid_valuation",
        "Equal hybrid valuation",
        "hybrid_low_pbr_score",
        "hybrid_low_forward_per_score",
        _EQUAL_WEIGHTS,
        "Equal-weight raw small cap plus 50/50 raw and sector-relative valuation scores.",
    ),
    ScoreMethodSpec(
        "standard_pbr_tilt_raw",
        "Standard PBR tilt raw",
        "low_pbr_score",
        "low_forward_per_score",
        STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
        "Standard PBR tilt weights using raw valuation percentiles.",
    ),
    ScoreMethodSpec(
        "standard_pbr_tilt_sector_relative",
        "Standard PBR tilt sector-relative",
        "sector_low_pbr_score",
        "sector_low_forward_per_score",
        STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
        "Standard PBR tilt weights using sector-relative valuation percentiles.",
    ),
    ScoreMethodSpec(
        "standard_pbr_tilt_hybrid",
        "Standard PBR tilt hybrid",
        "hybrid_low_pbr_score",
        "hybrid_low_forward_per_score",
        STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
        "Standard PBR tilt weights using 50/50 raw and sector-relative valuation scores.",
    ),
    ScoreMethodSpec(
        "prime_size_tilt_raw",
        "Prime size tilt raw",
        "low_pbr_score",
        "low_forward_per_score",
        PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
        "Prime size tilt weights using raw valuation percentiles.",
    ),
    ScoreMethodSpec(
        "prime_size_tilt_sector_relative",
        "Prime size tilt sector-relative",
        "sector_low_pbr_score",
        "sector_low_forward_per_score",
        PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
        "Prime size tilt weights using sector-relative valuation percentiles.",
    ),
    ScoreMethodSpec(
        "prime_size_tilt_hybrid",
        "Prime size tilt hybrid",
        "hybrid_low_pbr_score",
        "hybrid_low_forward_per_score",
        PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
        "Prime size tilt weights using 50/50 raw and sector-relative valuation scores.",
    ),
)
LIQUIDITY_SCENARIOS: tuple[LiquidityScenarioSpec, ...] = (
    LiquidityScenarioSpec("none", "No liquidity/capacity floor"),
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


def _score_factor_within_year_market_sector(
    frame: pd.DataFrame,
    source_column: str,
    *,
    min_sector_observations: int,
    prefer_low: bool,
) -> pd.Series:
    values = pd.to_numeric(frame[source_column], errors="coerce")
    scores = pd.Series(np.nan, index=frame.index, dtype="float64")
    group_columns = ["year", "market", "sector_33_name"]
    for _, group in frame.groupby(group_columns, dropna=False, sort=False):
        valid = values.loc[group.index].dropna()
        count = len(valid)
        if count < min_sector_observations:
            continue
        if count == 1:
            ranked = pd.Series(0.5, index=valid.index, dtype="float64")
        else:
            ranked = (valid.rank(method="average") - 1.0) / float(count - 1)
        if prefer_low:
            ranked = 1.0 - ranked
        scores.loc[ranked.index] = ranked.astype(float)
    return scores


def _weighted_score(
    frame: pd.DataFrame,
    *,
    size_score_column: str,
    pbr_score_column: str,
    forward_per_score_column: str,
    weights: Mapping[str, float],
) -> pd.Series:
    component_by_key = {
        "small_market_cap_score": size_score_column,
        "low_pbr_score": pbr_score_column,
        "low_forward_per_score": forward_per_score_column,
    }
    normalized_weights = {str(key): float(value) for key, value in weights.items()}
    unsupported = sorted(set(normalized_weights) - set(component_by_key))
    if unsupported:
        raise ValueError(f"Unsupported value composite weight key(s): {unsupported}")
    weight_sum = sum(normalized_weights.values())
    if not math.isfinite(weight_sum) or weight_sum <= 0:
        raise ValueError("value composite weights must sum to a positive finite value")
    score = pd.Series(0.0, index=frame.index, dtype="float64")
    used_columns: list[str] = []
    for key, raw_weight in normalized_weights.items():
        column = component_by_key[key]
        used_columns.append(column)
        score = score + pd.to_numeric(frame[column], errors="coerce") * (raw_weight / weight_sum)
    missing = frame[used_columns].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    score.loc[missing] = np.nan
    return score


def _build_scored_panel_df(
    event_ledger_df: pd.DataFrame,
    *,
    winsor_lower: float,
    winsor_upper: float,
    min_sector_observations: int,
    required_positive_columns: Sequence[str],
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
    panel = panel.merge(
        realized[extra_columns].drop_duplicates("event_id"),
        on="event_id",
        how="left",
        suffixes=("", "_event"),
    )
    for column in ("avg_trading_value_60d_mil_jpy", "market_cap_bil_jpy"):
        event_column = f"{column}_event"
        if event_column in panel.columns:
            panel[column] = panel[column].combine_first(panel[event_column])
            panel = panel.drop(columns=[event_column])
    panel["sector_low_pbr_score"] = _score_factor_within_year_market_sector(
        panel,
        "pbr",
        min_sector_observations=min_sector_observations,
        prefer_low=True,
    )
    panel["sector_low_forward_per_score"] = _score_factor_within_year_market_sector(
        panel,
        "forward_per",
        min_sector_observations=min_sector_observations,
        prefer_low=True,
    )
    panel["hybrid_low_pbr_score"] = (
        panel[["low_pbr_score", "sector_low_pbr_score"]]
        .apply(pd.to_numeric, errors="coerce")
        .mean(axis=1, skipna=False)
    )
    panel["hybrid_low_forward_per_score"] = (
        panel[["low_forward_per_score", "sector_low_forward_per_score"]]
        .apply(pd.to_numeric, errors="coerce")
        .mean(axis=1, skipna=False)
    )
    for method in SCORE_METHODS:
        panel[f"{method.name}_score"] = _weighted_score(
            panel,
            size_score_column="small_market_cap_score",
            pbr_score_column=method.pbr_score_column,
            forward_per_score_column=method.forward_per_score_column,
            weights=method.weights,
        )
    return panel.reset_index(drop=True)


def _build_score_coverage_df(scored_panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "event_count",
        "score_non_null_count",
        "score_coverage_pct",
        "sector_pbr_non_null_count",
        "sector_pbr_coverage_pct",
        "sector_forward_per_non_null_count",
        "sector_forward_per_coverage_pct",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(scored_panel_df, market_scope)
        if scope_df.empty:
            continue
        sector_pbr = pd.to_numeric(scope_df["sector_low_pbr_score"], errors="coerce").notna()
        sector_forward_per = pd.to_numeric(
            scope_df["sector_low_forward_per_score"],
            errors="coerce",
        ).notna()
        for method in SCORE_METHODS:
            score = pd.to_numeric(scope_df[f"{method.name}_score"], errors="coerce").notna()
            records.append(
                {
                    "market_scope": market_scope,
                    "score_method": method.name,
                    "score_method_label": method.label,
                    "event_count": int(len(scope_df)),
                    "score_non_null_count": int(score.sum()),
                    "score_coverage_pct": float(score.mean() * 100.0) if len(scope_df) else None,
                    "sector_pbr_non_null_count": int(sector_pbr.sum()),
                    "sector_pbr_coverage_pct": (
                        float(sector_pbr.mean() * 100.0) if len(scope_df) else None
                    ),
                    "sector_forward_per_non_null_count": int(sector_forward_per.sum()),
                    "sector_forward_per_coverage_pct": (
                        float(sector_forward_per.mean() * 100.0) if len(scope_df) else None
                    ),
                }
            )
    return _market_scope_sort(pd.DataFrame(records), ["score_method"])


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
    *,
    selection_fractions: Sequence[float],
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
        "sector_low_pbr_score",
        "hybrid_low_pbr_score",
        "small_market_cap_score",
        "low_forward_per_score",
        "sector_low_forward_per_score",
        "hybrid_low_forward_per_score",
        "pbr",
        "market_cap_bil_jpy",
        "forward_per",
        "avg_trading_value_60d_mil_jpy",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    selected_frames: list[pd.DataFrame] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(scored_panel_df, market_scope)
        if scope_df.empty:
            continue
        for _year, year_df in scope_df.groupby("year", sort=True):
            for method in SCORE_METHODS:
                score_values = pd.to_numeric(year_df[f"{method.name}_score"], errors="coerce")
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
        return _empty_df(columns)
    result = pd.concat(selected_frames, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    result = result[columns]
    return _market_scope_sort(
        result,
        ["score_method", "liquidity_scenario", "selection_fraction", "year", "selection_rank"],
    )


def _build_sector_exposure_df(selected_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "selection_fraction",
        "year",
        "sector_33_name",
        "selected_count",
        "selected_share_pct",
        "mean_return_pct",
        "mean_composite_score",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    group_columns = ["market_scope", "score_method", "selection_fraction", "year"]
    totals = selected_event_df.groupby(group_columns, observed=True, sort=False).size()
    for keys, group in selected_event_df.groupby(
        [*group_columns, "sector_33_name"],
        observed=True,
        sort=False,
    ):
        market_scope, score_method, selection_fraction, year, sector_name = keys
        total_key = (market_scope, score_method, selection_fraction, year)
        selected_count = int(len(group))
        total_count = int(totals.get(total_key, selected_count))
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
        score = pd.to_numeric(group["composite_score"], errors="coerce").dropna()
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": str(group["score_method_label"].iloc[0]),
                "selection_fraction": float(cast(float, selection_fraction)),
                "year": str(year),
                "sector_33_name": str(sector_name),
                "selected_count": selected_count,
                "selected_share_pct": (
                    float(selected_count / total_count * 100.0) if total_count else None
                ),
                "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                "mean_composite_score": float(score.mean()) if not score.empty else None,
            }
        )
    result = pd.DataFrame(records)
    result["market_scope"] = pd.Categorical(
        result["market_scope"].astype(str),
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(result["market_scope"])],
        ordered=True,
    )
    return result.sort_values(
        [
            "market_scope",
            "score_method",
            "selection_fraction",
            "year",
            "selected_count",
            "sector_33_name",
        ],
        ascending=[True, True, True, True, False, True],
        kind="stable",
    ).reset_index(drop=True)


def run_annual_sector_relative_value_composite(
    input_bundle_path: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_sector_observations: int = DEFAULT_MIN_SECTOR_OBSERVATIONS,
    required_positive_columns: Sequence[str] = VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
) -> AnnualSectorRelativeValueCompositeResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_sector_observations < 2:
        raise ValueError("min_sector_observations must be >= 2")
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
    scored_panel_df = _build_scored_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_sector_observations=min_sector_observations,
        required_positive_columns=normalized_positive_columns,
    )
    score_coverage_df = _build_score_coverage_df(scored_panel_df)
    selected_event_df = _build_selected_event_df(
        scored_panel_df,
        selection_fractions=normalized_fractions,
    )
    selection_summary_df = _build_selection_summary_df(selected_event_df)
    sector_exposure_df = _build_sector_exposure_df(selected_event_df)
    source_mode, source_detail, price_df = _load_price_df(resolved_db_path, selected_event_df)
    portfolio_daily_df = _build_portfolio_daily_df(selected_event_df, price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, selected_event_df)
    score_policy = (
        "raw scores are within year x current-market percentiles; sector-relative valuation "
        "scores are within year x current-market x sector_33_name percentiles after as-of "
        f"panel construction, requiring at least {min_sector_observations} finite names per "
        "sector group; small-cap score remains raw"
        + (
            f"; required positive columns: {', '.join(normalized_positive_columns)}"
            if normalized_positive_columns
            else ""
        )
    )
    return AnnualSectorRelativeValueCompositeResult(
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
        min_sector_observations=min_sector_observations,
        required_positive_columns=normalized_positive_columns,
        input_realized_event_count=realized_count,
        scored_event_count=int(len(scored_panel_df)),
        score_policy=score_policy,
        scored_panel_df=scored_panel_df,
        score_coverage_df=score_coverage_df,
        selected_event_df=selected_event_df,
        selection_summary_df=selection_summary_df,
        sector_exposure_df=sector_exposure_df,
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


def _build_summary_markdown(result: AnnualSectorRelativeValueCompositeResult) -> str:
    lines = [
        "# Annual Sector-Relative Value Composite",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Selection fractions: `{', '.join(_fmt(v, 2) for v in result.selection_fractions)}`",
        f"- Min sector observations: `{result.min_sector_observations}`",
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
        "## Focus Portfolio Rows",
        "",
    ]
    summary = result.portfolio_summary_df.copy()
    if summary.empty:
        lines.append("- No portfolio summary rows were produced.")
    else:
        focus = summary[
            (summary["market_scope"].astype(str).isin(["prime", "standard"]))
            & (summary["selection_fraction"].astype(float).isin([0.10, 0.15]))
        ].copy()
        if focus.empty:
            focus = summary.head(16)
        focus = focus.sort_values("sharpe_ratio", ascending=False, na_position="last").head(16)
        for row in focus.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['score_method']}` / "
                f"top `{float(row['selection_fraction']) * 100:.0f}%`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"events `{int(cast(int, row['realized_event_count']))}`"
            )
    lines.extend(["", "## Coverage Rows", ""])
    coverage = result.score_coverage_df.copy()
    if coverage.empty:
        lines.append("- No score coverage rows were produced.")
    else:
        for row in coverage[
            coverage["market_scope"].astype(str).isin(["prime", "standard"])
        ].head(16).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['score_method']}`: "
                f"score coverage `{_fmt(row['score_coverage_pct'])}%`, "
                f"sector PBR `{_fmt(row['sector_pbr_coverage_pct'])}%`, "
                f"sector forward PER `{_fmt(row['sector_forward_per_coverage_pct'])}%`"
            )
    return "\n".join(lines)


def _build_published_summary(result: AnnualSectorRelativeValueCompositeResult) -> dict[str, Any]:
    return {
        "inputBundlePath": result.input_bundle_path,
        "inputRunId": result.input_run_id,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "requiredPositiveColumns": list(result.required_positive_columns),
        "inputRealizedEventCount": result.input_realized_event_count,
        "scoredEventCount": result.scored_event_count,
        "selectionFractions": list(result.selection_fractions),
        "minSectorObservations": result.min_sector_observations,
        "scorePolicy": result.score_policy,
        "scoreCoverage": result.score_coverage_df.to_dict(orient="records"),
        "selectionSummary": result.selection_summary_df.to_dict(orient="records"),
        "portfolioSummary": result.portfolio_summary_df.to_dict(orient="records"),
    }


def write_annual_sector_relative_value_composite_bundle(
    result: AnnualSectorRelativeValueCompositeResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_SECTOR_RELATIVE_VALUE_COMPOSITE_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_sector_relative_value_composite",
        params={
            "input_bundle_path": result.input_bundle_path,
            "db_path": result.db_path,
            "selection_fractions": list(result.selection_fractions),
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_sector_observations": result.min_sector_observations,
            "required_positive_columns": list(result.required_positive_columns),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        # bundle-structured-fallback: keep machine-readable tables for old bundle consumers.
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_sector_relative_value_composite_bundle(
    bundle_path: str | Path,
) -> AnnualSectorRelativeValueCompositeResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualSectorRelativeValueCompositeResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_sector_relative_value_composite_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_SECTOR_RELATIVE_VALUE_COMPOSITE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_sector_relative_value_composite_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_SECTOR_RELATIVE_VALUE_COMPOSITE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
