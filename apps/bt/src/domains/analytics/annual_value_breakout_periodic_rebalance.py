"""N-month value rebalance portfolio lens with N-day breakout overlays."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    DEFAULT_ADV_WINDOW,
    DEFAULT_MARKETS,
    SourceMode,
    _fetch_date_range,
    _market_query_codes,
    _open_analysis_connection,
    _query_price_rows,
    _validate_adv_window,
    _normalize_selected_markets,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    _normalize_required_positive_columns,
)
from src.domains.analytics.annual_value_composite_selection import (
    DEFAULT_MIN_TRAIN_OBSERVATIONS,
    EQUAL_VALUE_COMPOSITE_WEIGHTS,
    LIQUIDITY_SCENARIOS,
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    ScoreMethodSpec,
    STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    _daily_stats,
    _frame_for_market_scope,
    _market_scope_sort,
    _series_mean,
    _walkforward_score_for_scope_year,
)
from src.domains.analytics.annual_value_periodic_rebalance import (
    DEFAULT_REBALANCE_MONTHS,
    DEFAULT_SELECTION_COUNTS,
    PERIODIC_SCORE_METHODS,
    AnnualValuePeriodicRebalanceResult,
    _apply_periodic_score_columns,
    _build_periodic_event_ledger_fast,
    _build_rebalance_calendar_df,
    _build_scored_panel_df,
    _build_walkforward_weight_df,
    _empty_df,
    _fmt,
    _last_trading_date_before,
    _normalize_rebalance_months,
    _normalize_selection_counts,
    _query_adjustment_event_rows,
    _query_entry_stock_master,
    _query_statement_rows,
    _query_trading_dates,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.research_core import build_event_portfolio_daily_df

ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID = (
    "market-behavior/annual-value-breakout-periodic-rebalance"
)
DEFAULT_BREAKOUT_WINDOWS: tuple[int, ...] = (20, 60, 120, 252)
DEFAULT_BREAKOUT_LOOKBACK_SESSIONS: tuple[int, ...] = (0, 5, 20)
DEFAULT_BREAKOUT_SCORE_BOOST = 0.10
_CORE_FACTOR_COLUMNS: tuple[str, ...] = (
    "low_pbr_score",
    "small_market_cap_score",
    "low_forward_per_score",
)
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")
_BREAKOUT_GROUP_COLUMNS: tuple[str, ...] = (
    "market_scope",
    "score_method",
    "liquidity_scenario",
    "breakout_policy",
    "breakout_window",
    "breakout_lookback_sessions",
    "rebalance_months",
    "selection_count",
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "rebalance_calendar_df",
    "event_ledger_df",
    "scored_panel_df",
    "score_method_params_df",
    "breakout_feature_df",
    "breakout_scored_panel_df",
    "walkforward_weight_df",
    "selected_event_df",
    "selection_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


class BreakoutSelectionSpec(NamedTuple):
    policy: str
    window: int
    lookback_sessions: int
    label: str


class FactorWeightSpec(NamedTuple):
    method_name: str
    score_column: str
    pbr_weight: float
    size_weight: float
    forward_per_weight: float
    source: str


@dataclass(frozen=True)
class AnnualValueBreakoutPeriodicRebalanceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_markets: tuple[str, ...]
    rebalance_months: tuple[int, ...]
    selection_counts: tuple[int, ...]
    score_methods: tuple[str, ...]
    liquidity_scenarios: tuple[str, ...]
    breakout_policies: tuple[str, ...]
    factor_weight_step: float | None
    factor_weights: tuple[tuple[float, float, float], ...]
    max_portfolio_configs: int | None
    skip_portfolio_curves: bool
    breakout_windows: tuple[int, ...]
    breakout_lookback_sessions: tuple[int, ...]
    breakout_score_boost: float
    winsor_lower: float
    winsor_upper: float
    min_train_observations: int
    adv_window: int
    required_positive_columns: tuple[str, ...]
    current_market_snapshot_only: bool
    score_policy: str
    rebalance_calendar_df: pd.DataFrame
    event_ledger_df: pd.DataFrame
    scored_panel_df: pd.DataFrame
    score_method_params_df: pd.DataFrame
    breakout_feature_df: pd.DataFrame
    breakout_scored_panel_df: pd.DataFrame
    walkforward_weight_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def _normalize_positive_int_sequence(
    values: Sequence[int],
    *,
    label: str,
) -> tuple[int, ...]:
    normalized: list[int] = []
    for raw_value in values:
        value = int(raw_value)
        if value < 1:
            raise ValueError(f"{label} values must be positive")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError(f"at least one {label} value is required")
    return tuple(sorted(normalized))


def _normalize_nonnegative_int_sequence(
    values: Sequence[int],
    *,
    label: str,
) -> tuple[int, ...]:
    normalized: list[int] = []
    for raw_value in values:
        value = int(raw_value)
        if value < 0:
            raise ValueError(f"{label} values must be >= 0")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError(f"at least one {label} value is required")
    return tuple(sorted(normalized))


def _normalize_factor_weights(
    values: Sequence[tuple[float, float, float]],
) -> tuple[tuple[float, float, float], ...]:
    normalized: list[tuple[float, float, float]] = []
    for raw_weights in values:
        if len(raw_weights) != 3:
            raise ValueError("factor weight entries must have exactly three values")
        weights = tuple(float(value) for value in raw_weights)
        if any(value < 0.0 for value in weights):
            raise ValueError("factor weights must be non-negative")
        total = sum(weights)
        if not math.isclose(total, 1.0, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError("factor weights must sum to 1.0")
        rounded = cast(tuple[float, float, float], tuple(round(value, 10) for value in weights))
        if rounded not in normalized:
            normalized.append(rounded)
    return tuple(normalized)


def _build_factor_weight_grid(step: float | None) -> tuple[tuple[float, float, float], ...]:
    if step is None:
        return ()
    value = float(step)
    if value <= 0.0 or value > 1.0:
        raise ValueError("factor_weight_step must satisfy 0 < step <= 1")
    buckets = round(1.0 / value)
    if buckets < 1 or not math.isclose(buckets * value, 1.0, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError("factor_weight_step must evenly divide 1.0")
    weights: list[tuple[float, float, float]] = []
    for pbr_bucket in range(buckets + 1):
        for size_bucket in range(buckets - pbr_bucket + 1):
            forward_per_bucket = buckets - pbr_bucket - size_bucket
            weights.append(
                (
                    round(pbr_bucket / buckets, 10),
                    round(size_bucket / buckets, 10),
                    round(forward_per_bucket / buckets, 10),
                )
            )
    return tuple(weights)


def _weight_name(value: float) -> str:
    return f"{int(round(value * 100)):03d}"


def _factor_weight_method_name(weights: tuple[float, float, float]) -> str:
    return (
        "factor_w_"
        f"pbr{_weight_name(weights[0])}_"
        f"size{_weight_name(weights[1])}_"
        f"fper{_weight_name(weights[2])}"
    )


def _factor_score_column(method_name: str) -> str:
    return f"{method_name}_score"


def _build_factor_weight_specs(
    *,
    explicit_weights: Sequence[tuple[float, float, float]],
    factor_weight_step: float | None,
) -> tuple[FactorWeightSpec, ...]:
    weights: list[tuple[float, float, float]] = []
    sources: dict[tuple[float, float, float], str] = {}
    for weight in _build_factor_weight_grid(factor_weight_step):
        weights.append(weight)
        sources[weight] = "grid"
    for weight in _normalize_factor_weights(explicit_weights):
        if weight not in weights:
            weights.append(weight)
        sources[weight] = "explicit"
    specs: list[FactorWeightSpec] = []
    for weight in weights:
        method_name = _factor_weight_method_name(weight)
        specs.append(
            FactorWeightSpec(
                method_name,
                _factor_score_column(method_name),
                weight[0],
                weight[1],
                weight[2],
                sources[weight],
            )
        )
    return tuple(specs)


def _weights_from_mapping(mapping: dict[str, float]) -> tuple[float | None, float | None, float | None]:
    raw = (
        mapping.get("low_pbr_score"),
        mapping.get("small_market_cap_score"),
        mapping.get("low_forward_per_score"),
    )
    if any(value is None for value in raw):
        return raw
    total = sum(float(cast(float, value)) for value in raw)
    if total <= 0:
        return raw
    return (
        float(cast(float, raw[0])) / total,
        float(cast(float, raw[1])) / total,
        float(cast(float, raw[2])) / total,
    )


def _build_score_method_specs_and_params(
    *,
    factor_weight_specs: Sequence[FactorWeightSpec],
) -> tuple[tuple[ScoreMethodSpec, ...], pd.DataFrame]:
    specs = list(PERIODIC_SCORE_METHODS)
    rows: list[dict[str, Any]] = []
    known_weights = {
        "equal_weight": _weights_from_mapping(EQUAL_VALUE_COMPOSITE_WEIGHTS),
        "standard_pbr_tilt": _weights_from_mapping(STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS),
        "prime_size_tilt": _weights_from_mapping(PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS),
        "walkforward_regression_weight": (None, None, None),
    }
    for method in PERIODIC_SCORE_METHODS:
        weights = known_weights.get(method.name, (None, None, None))
        rows.append(
            {
                "score_method": method.name,
                "score_column": method.score_column,
                "source": "built_in",
                "pbr_weight": weights[0],
                "size_weight": weights[1],
                "forward_per_weight": weights[2],
                "description": method.description,
            }
        )
    for spec in factor_weight_specs:
        label = (
            "Factor grid "
            f"PBR {spec.pbr_weight:.2f}, size {spec.size_weight:.2f}, "
            f"forward PER {spec.forward_per_weight:.2f}"
        )
        specs.append(
            ScoreMethodSpec(
                spec.method_name,
                label,
                spec.score_column,
                "User-searchable linear weight over low PBR, small market cap, and low forward PER scores.",
            )
        )
        rows.append(
            {
                "score_method": spec.method_name,
                "score_column": spec.score_column,
                "source": spec.source,
                "pbr_weight": spec.pbr_weight,
                "size_weight": spec.size_weight,
                "forward_per_weight": spec.forward_per_weight,
                "description": label,
            }
        )
    return tuple(specs), pd.DataFrame(rows)


def _apply_factor_weight_score_columns(
    scored_panel_df: pd.DataFrame,
    factor_weight_specs: Sequence[FactorWeightSpec],
) -> pd.DataFrame:
    if scored_panel_df.empty or not factor_weight_specs:
        return scored_panel_df.copy()
    result = scored_panel_df.copy()
    core = result[list(_CORE_FACTOR_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    missing = core.isna().any(axis=1)
    for spec in factor_weight_specs:
        score = (
            core["low_pbr_score"] * spec.pbr_weight
            + core["small_market_cap_score"] * spec.size_weight
            + core["low_forward_per_score"] * spec.forward_per_weight
        )
        score.loc[missing] = np.nan
        result[spec.score_column] = score
    return result.reset_index(drop=True)


def _breakout_specs(
    *,
    breakout_windows: Sequence[int],
    lookback_sessions: Sequence[int],
    breakout_score_boost: float,
) -> tuple[BreakoutSelectionSpec, ...]:
    specs: list[BreakoutSelectionSpec] = [
        BreakoutSelectionSpec(
            "value_only",
            0,
            -1,
            "Value-only baseline",
        )
    ]
    for window in breakout_windows:
        specs.append(
            BreakoutSelectionSpec(
                "breakout_signal",
                int(window),
                0,
                f"{int(window)}d breakout on signal date",
            )
        )
        for lookback in lookback_sessions:
            if int(lookback) <= 0:
                continue
            specs.append(
                BreakoutSelectionSpec(
                    "breakout_recent",
                    int(window),
                    int(lookback),
                    f"{int(window)}d breakout within {int(lookback)} sessions",
                )
            )
            if breakout_score_boost > 0:
                specs.append(
                    BreakoutSelectionSpec(
                        "breakout_additive",
                        int(window),
                        int(lookback),
                        f"Value score + recent {int(window)}d breakout boost",
                    )
                )
    return tuple(specs)


def _empty_result(
    *,
    db_path: str,
    source_mode: SourceMode,
    source_detail: str,
    available_start_date: str | None,
    available_end_date: str | None,
    selected_markets: tuple[str, ...],
    rebalance_months: tuple[int, ...],
    selection_counts: tuple[int, ...],
    score_methods: tuple[str, ...],
    liquidity_scenarios: tuple[str, ...],
    breakout_policies: tuple[str, ...],
    factor_weight_step: float | None,
    factor_weights: tuple[tuple[float, float, float], ...],
    max_portfolio_configs: int | None,
    skip_portfolio_curves: bool,
    breakout_windows: tuple[int, ...],
    score_method_params_df: pd.DataFrame,
    breakout_lookback_sessions: tuple[int, ...],
    breakout_score_boost: float,
    winsor_lower: float,
    winsor_upper: float,
    min_train_observations: int,
    adv_window: int,
    required_positive_columns: tuple[str, ...],
    current_market_snapshot_only: bool,
    score_policy: str,
) -> AnnualValueBreakoutPeriodicRebalanceResult:
    empty = _empty_df([])
    return AnnualValueBreakoutPeriodicRebalanceResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=None,
        analysis_end_date=None,
        selected_markets=selected_markets,
        rebalance_months=rebalance_months,
        selection_counts=selection_counts,
        score_methods=score_methods,
        liquidity_scenarios=liquidity_scenarios,
        breakout_policies=breakout_policies,
        factor_weight_step=factor_weight_step,
        factor_weights=factor_weights,
        max_portfolio_configs=max_portfolio_configs,
        skip_portfolio_curves=skip_portfolio_curves,
        breakout_windows=breakout_windows,
        breakout_lookback_sessions=breakout_lookback_sessions,
        breakout_score_boost=breakout_score_boost,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_train_observations=min_train_observations,
        adv_window=adv_window,
        required_positive_columns=required_positive_columns,
        current_market_snapshot_only=current_market_snapshot_only,
        score_policy=score_policy,
        rebalance_calendar_df=empty.copy(),
        event_ledger_df=empty.copy(),
        scored_panel_df=empty.copy(),
        score_method_params_df=score_method_params_df,
        breakout_feature_df=empty.copy(),
        breakout_scored_panel_df=empty.copy(),
        walkforward_weight_df=empty.copy(),
        selected_event_df=empty.copy(),
        selection_summary_df=empty.copy(),
        portfolio_daily_df=empty.copy(),
        portfolio_summary_df=empty.copy(),
    )


def _build_price_breakout_frame(
    price_df: pd.DataFrame,
    *,
    breakout_windows: tuple[int, ...],
) -> pd.DataFrame:
    columns = [
        "code",
        "date",
        "signal_close",
        "signal_high",
        "signal_low",
        "signal_volume",
        "signal_trading_value_mil_jpy",
        "signal_trading_value_ratio_20d",
        "signal_return_20d",
        *[f"prior_high_{window}d" for window in breakout_windows],
        *[f"new_high_{window}d" for window in breakout_windows],
        *[f"days_since_new_high_{window}d" for window in breakout_windows],
        *[f"close_to_prior_high_{window}d_pct" for window in breakout_windows],
    ]
    if price_df.empty:
        return _empty_df(columns)
    frames: list[pd.DataFrame] = []
    for code, group in price_df.groupby("code", sort=False):
        frame = group.sort_values("date", kind="stable").reset_index(drop=True).copy()
        frame["code"] = str(code)
        high = pd.to_numeric(frame["high"], errors="coerce")
        close = pd.to_numeric(frame["close"], errors="coerce")
        volume = pd.to_numeric(frame["volume"], errors="coerce")
        trading_value = close * volume
        frame["signal_close"] = close
        frame["signal_high"] = high
        frame["signal_low"] = pd.to_numeric(frame["low"], errors="coerce")
        frame["signal_volume"] = volume
        frame["signal_trading_value_mil_jpy"] = trading_value / 1_000_000.0
        frame["signal_trading_value_ratio_20d"] = trading_value / trading_value.shift(1).rolling(
            20,
            min_periods=1,
        ).mean()
        frame["signal_return_20d"] = close / close.shift(20) - 1.0
        for window in breakout_windows:
            prior_high = high.shift(1).rolling(int(window), min_periods=1).max()
            is_new_high = high > prior_high
            frame[f"prior_high_{window}d"] = prior_high
            frame[f"new_high_{window}d"] = is_new_high.fillna(False)
            frame[f"close_to_prior_high_{window}d_pct"] = (close / prior_high - 1.0) * 100.0
            days_since: list[int | None] = []
            last_new_high_idx: int | None = None
            for idx, flag in enumerate(frame[f"new_high_{window}d"].astype(bool).tolist()):
                if flag:
                    last_new_high_idx = idx
                days_since.append(None if last_new_high_idx is None else idx - last_new_high_idx)
            frame[f"days_since_new_high_{window}d"] = days_since
        frames.append(frame[columns])
    if not frames:
        return _empty_df(columns)
    result = pd.concat(
        [frame.dropna(axis=1, how="all") for frame in frames],
        ignore_index=True,
        sort=False,
    )
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[columns]


def _build_breakout_feature_df(
    scored_panel_df: pd.DataFrame,
    price_df: pd.DataFrame,
    trading_dates: Sequence[str],
    *,
    breakout_windows: tuple[int, ...],
) -> pd.DataFrame:
    base_columns = [
        "event_id",
        "code",
        "entry_date",
        "signal_date",
        "has_signal_session",
    ]
    price_columns = [
        "signal_close",
        "signal_high",
        "signal_low",
        "signal_volume",
        "signal_trading_value_mil_jpy",
        "signal_trading_value_ratio_20d",
        "signal_return_20d",
        *[f"prior_high_{window}d" for window in breakout_windows],
        *[f"new_high_{window}d" for window in breakout_windows],
        *[f"days_since_new_high_{window}d" for window in breakout_windows],
        *[f"close_to_prior_high_{window}d_pct" for window in breakout_windows],
    ]
    columns = [*base_columns, *price_columns]
    if scored_panel_df.empty:
        return _empty_df(columns)
    event_dates = (
        scored_panel_df[["event_id", "code", "entry_date"]]
        .drop_duplicates("event_id")
        .copy()
    )
    signal_date_by_entry = {
        str(entry_date): _last_trading_date_before(trading_dates, str(entry_date))
        for entry_date in event_dates["entry_date"].astype(str).unique()
    }
    event_dates["signal_date"] = event_dates["entry_date"].astype(str).map(signal_date_by_entry)
    price_breakout_df = _build_price_breakout_frame(
        price_df,
        breakout_windows=breakout_windows,
    )
    if price_breakout_df.empty:
        event_dates["has_signal_session"] = False
        for column in price_columns:
            event_dates[column] = None
        return event_dates[columns]
    feature_df = event_dates.merge(
        price_breakout_df,
        left_on=["code", "signal_date"],
        right_on=["code", "date"],
        how="left",
    ).drop(columns=["date"], errors="ignore")
    feature_df["has_signal_session"] = feature_df["signal_close"].notna()
    for window in breakout_windows:
        feature_df[f"new_high_{window}d"] = feature_df[f"new_high_{window}d"].eq(True)
    for column in columns:
        if column not in feature_df.columns:
            feature_df[column] = None
    return feature_df[columns].sort_values(["entry_date", "code"], kind="stable").reset_index(drop=True)


def _merge_breakout_features(
    scored_panel_df: pd.DataFrame,
    breakout_feature_df: pd.DataFrame,
    *,
    breakout_windows: tuple[int, ...],
) -> pd.DataFrame:
    if scored_panel_df.empty:
        return scored_panel_df.copy()
    merged = scored_panel_df.merge(
        breakout_feature_df.drop(columns=["code", "entry_date"], errors="ignore"),
        on="event_id",
        how="left",
    )
    for window in breakout_windows:
        column = f"new_high_{window}d"
        if column in merged.columns:
            merged[column] = merged[column].eq(True)
    return merged.reset_index(drop=True)


def _score_values_for_method(
    period_df: pd.DataFrame,
    walkforward_weight_df: pd.DataFrame,
    *,
    method: ScoreMethodSpec,
    market_scope: str,
    year: str,
) -> pd.Series | None:
    if method.name == "walkforward_regression_weight":
        return _walkforward_score_for_scope_year(
            period_df,
            walkforward_weight_df,
            market_scope=market_scope,
            year=year,
        )
    if method.score_column is None:
        return None
    return pd.to_numeric(period_df[method.score_column], errors="coerce")


def _breakout_eligibility_and_score(
    eligible: pd.DataFrame,
    base_score: pd.Series,
    *,
    spec: BreakoutSelectionSpec,
    breakout_score_boost: float,
) -> tuple[pd.Series, pd.Series]:
    score = pd.to_numeric(base_score, errors="coerce").copy()
    if spec.policy == "value_only":
        return pd.Series(True, index=eligible.index), score
    new_high_column = f"new_high_{spec.window}d"
    days_column = f"days_since_new_high_{spec.window}d"
    if spec.policy == "breakout_signal":
        return eligible[new_high_column].astype(bool), score
    days_since = pd.to_numeric(eligible[days_column], errors="coerce")
    recent_mask = days_since.notna() & (days_since <= int(spec.lookback_sessions))
    if spec.policy == "breakout_recent":
        return recent_mask, score
    if spec.policy == "breakout_additive":
        denominator = max(int(spec.lookback_sessions), 1)
        recency = ((denominator - days_since.clip(lower=0, upper=denominator)) / denominator).fillna(0.0)
        return pd.Series(True, index=eligible.index), score + recency * float(breakout_score_boost)
    raise ValueError(f"unsupported breakout policy: {spec.policy}")


def _select_top_events_for_breakout_spec(
    group_df: pd.DataFrame,
    *,
    score_method: ScoreMethodSpec,
    score_values: pd.Series,
    selection_count: int,
    liquidity_scenario: Any,
    spec: BreakoutSelectionSpec,
    breakout_score_boost: float,
) -> pd.DataFrame:
    eligible = group_df.copy()
    eligible["value_composite_score"] = pd.to_numeric(score_values.loc[group_df.index], errors="coerce")
    mask = pd.Series(True, index=eligible.index)
    if liquidity_scenario.min_adv60_mil_jpy is not None:
        mask &= (
            pd.to_numeric(eligible["avg_trading_value_60d_mil_jpy"], errors="coerce")
            >= liquidity_scenario.min_adv60_mil_jpy
        )
    if liquidity_scenario.min_market_cap_bil_jpy is not None:
        mask &= (
            pd.to_numeric(eligible["market_cap_bil_jpy"], errors="coerce")
            >= liquidity_scenario.min_market_cap_bil_jpy
        )
    eligible = eligible[mask].copy()
    eligible = eligible[pd.to_numeric(eligible["value_composite_score"], errors="coerce").notna()].copy()
    if eligible.empty:
        return _empty_df([])
    breakout_mask, breakout_score = _breakout_eligibility_and_score(
        eligible,
        eligible["value_composite_score"],
        spec=spec,
        breakout_score_boost=breakout_score_boost,
    )
    eligible["composite_score"] = breakout_score
    eligible = eligible[breakout_mask].copy()
    if eligible.empty:
        return _empty_df([])
    ranked = eligible.sort_values(
        ["composite_score", "value_composite_score", "code"],
        ascending=[False, False, True],
        kind="stable",
    ).copy()
    ranked["selection_rank"] = np.arange(len(ranked), dtype=int) + 1
    selected = ranked.head(min(selection_count, len(ranked))).copy()
    selected["eligible_count"] = int(len(eligible))
    selected["selection_count"] = int(selection_count)
    selected["score_method"] = score_method.name
    selected["score_method_label"] = score_method.label
    selected["liquidity_scenario"] = liquidity_scenario.name
    selected["liquidity_scenario_label"] = liquidity_scenario.label
    selected["breakout_policy"] = spec.policy
    selected["breakout_policy_label"] = spec.label
    selected["breakout_window"] = int(spec.window)
    selected["breakout_lookback_sessions"] = int(spec.lookback_sessions)
    return selected


def _selected_event_columns(breakout_windows: Sequence[int]) -> list[str]:
    return [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "breakout_policy",
        "breakout_policy_label",
        "breakout_window",
        "breakout_lookback_sessions",
        "selection_count",
        "eligible_count",
        "selection_rank",
        "composite_score",
        "value_composite_score",
        "event_id",
        "year",
        "rebalance_period",
        "rebalance_months",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "entry_date",
        "signal_date",
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
        "signal_trading_value_mil_jpy",
        "signal_trading_value_ratio_20d",
        "signal_return_20d",
        *[f"new_high_{window}d" for window in breakout_windows],
        *[f"days_since_new_high_{window}d" for window in breakout_windows],
        *[f"close_to_prior_high_{window}d_pct" for window in breakout_windows],
    ]


def _build_selected_event_df(
    breakout_scored_panel_df: pd.DataFrame,
    walkforward_weight_df: pd.DataFrame,
    *,
    selection_counts: Sequence[int],
    score_method_specs: Sequence[ScoreMethodSpec],
    breakout_specs: Sequence[BreakoutSelectionSpec],
    breakout_windows: Sequence[int],
    breakout_score_boost: float,
    score_methods: Sequence[str],
    liquidity_scenarios: Sequence[str],
) -> pd.DataFrame:
    columns = _selected_event_columns(breakout_windows)
    if breakout_scored_panel_df.empty:
        return _empty_df(columns)
    frames: list[pd.DataFrame] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(breakout_scored_panel_df, market_scope)
        if scope_df.empty:
            continue
        for period, period_df in scope_df.groupby("year", sort=True):
            period_value = str(period)
            for method in score_method_specs:
                if method.name not in score_methods:
                    continue
                score_values = _score_values_for_method(
                    period_df,
                    walkforward_weight_df,
                    method=method,
                    market_scope=market_scope,
                    year=period_value,
                )
                if score_values is None:
                    continue
                for liquidity_scenario in LIQUIDITY_SCENARIOS:
                    if liquidity_scenario.name not in liquidity_scenarios:
                        continue
                    for spec in breakout_specs:
                        for count in selection_counts:
                            selected = _select_top_events_for_breakout_spec(
                                period_df,
                                score_method=method,
                                score_values=score_values,
                                selection_count=int(count),
                                liquidity_scenario=liquidity_scenario,
                                spec=spec,
                                breakout_score_boost=breakout_score_boost,
                            )
                            if selected.empty:
                                continue
                            selected["market_scope"] = market_scope
                            selected["rebalance_period"] = period_value
                            frames.append(selected)
    if not frames:
        return _empty_df(columns)
    result = pd.concat(frames, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    result = result[columns]
    return _market_scope_sort(
        result,
        [
            "score_method",
            "liquidity_scenario",
            "breakout_policy",
            "breakout_window",
            "breakout_lookback_sessions",
            "rebalance_months",
            "selection_count",
            "year",
            "selection_rank",
        ],
    )


def _build_selection_summary_df(selected_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        *_BREAKOUT_GROUP_COLUMNS,
        "score_method_label",
        "liquidity_scenario_label",
        "breakout_policy_label",
        "event_count",
        "period_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "mean_value_composite_score",
        "mean_composite_score",
        "mean_adv60_mil_jpy",
        "mean_market_cap_bil_jpy",
        "mean_signal_trading_value_ratio_20d",
        "mean_signal_return_20d_pct",
        "mean_days_since_breakout",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in selected_event_df.groupby(list(_BREAKOUT_GROUP_COLUMNS), observed=True, sort=False):
        key_dict = dict(zip(_BREAKOUT_GROUP_COLUMNS, keys, strict=True))
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
        window = int(cast(int, key_dict["breakout_window"]))
        days_column = f"days_since_new_high_{window}d"
        mean_signal_return_20d = _series_mean(group["signal_return_20d"])
        records.append(
            {
                **key_dict,
                "score_method_label": str(group["score_method_label"].iloc[0]),
                "liquidity_scenario_label": str(group["liquidity_scenario_label"].iloc[0]),
                "breakout_policy_label": str(group["breakout_policy_label"].iloc[0]),
                "event_count": int(len(group)),
                "period_count": int(group["year"].nunique()),
                "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                "median_return_pct": float(returns.median()) if not returns.empty else None,
                "win_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else None,
                "mean_value_composite_score": _series_mean(group["value_composite_score"]),
                "mean_composite_score": _series_mean(group["composite_score"]),
                "mean_adv60_mil_jpy": _series_mean(group["avg_trading_value_60d_mil_jpy"]),
                "mean_market_cap_bil_jpy": _series_mean(group["market_cap_bil_jpy"]),
                "mean_signal_trading_value_ratio_20d": _series_mean(
                    group["signal_trading_value_ratio_20d"]
                ),
                "mean_signal_return_20d_pct": (
                    mean_signal_return_20d * 100.0 if mean_signal_return_20d is not None else None
                ),
                "mean_days_since_breakout": _series_mean(group[days_column])
                if days_column in group.columns
                else None,
            }
        )
    return _market_scope_sort(
        pd.DataFrame(records),
        [
            "score_method",
            "liquidity_scenario",
            "breakout_policy",
            "breakout_window",
            "breakout_lookback_sessions",
            "rebalance_months",
            "selection_count",
        ],
    )


def _build_portfolio_daily_df(
    selected_event_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    return build_event_portfolio_daily_df(
        selected_event_df,
        price_df,
        group_columns=_BREAKOUT_GROUP_COLUMNS,
    )


def _build_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    selected_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        *_BREAKOUT_GROUP_COLUMNS,
        "score_method_label",
        "liquidity_scenario_label",
        "breakout_policy_label",
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
    event_counts = selected_event_df.groupby(list(_BREAKOUT_GROUP_COLUMNS), observed=True, sort=False).size().to_dict()
    label_lookup = {
        tuple(row[column] for column in _BREAKOUT_GROUP_COLUMNS): (
            str(row["score_method_label"]),
            str(row["liquidity_scenario_label"]),
            str(row["breakout_policy_label"]),
        )
        for row in selected_event_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(list(_BREAKOUT_GROUP_COLUMNS), observed=True, sort=False):
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
        labels = label_lookup.get(tuple(keys), ("", "", ""))
        records.append(
            {
                **dict(zip(_BREAKOUT_GROUP_COLUMNS, keys, strict=True)),
                "score_method_label": labels[0],
                "liquidity_scenario_label": labels[1],
                "breakout_policy_label": labels[2],
                "realized_event_count": int(event_counts.get(tuple(keys), 0)),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group)),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"]).max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                **_daily_stats(group["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None and max_drawdown_pct is not None and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    return _market_scope_sort(
        pd.DataFrame(records),
        [
            "score_method",
            "liquidity_scenario",
            "breakout_policy",
            "breakout_window",
            "breakout_lookback_sessions",
            "rebalance_months",
            "selection_count",
        ],
    )


def _filter_selected_events_for_portfolio(
    selected_event_df: pd.DataFrame,
    selection_summary_df: pd.DataFrame,
    *,
    max_portfolio_configs: int | None,
) -> pd.DataFrame:
    if (
        max_portfolio_configs is None
        or max_portfolio_configs <= 0
        or selected_event_df.empty
        or selection_summary_df.empty
    ):
        return selected_event_df
    sorted_summary = selection_summary_df.sort_values(
        ["mean_return_pct", "win_rate_pct", "event_count"],
        ascending=[False, False, False],
        na_position="last",
        kind="stable",
    )
    top_configs = sorted_summary.head(int(max_portfolio_configs))
    baseline_configs = selection_summary_df[
        selection_summary_df["breakout_policy"].astype(str) == "value_only"
    ]
    keep_keys = (
        pd.concat([baseline_configs, top_configs], ignore_index=True)
        [list(_BREAKOUT_GROUP_COLUMNS)]
        .drop_duplicates()
    )
    return selected_event_df.merge(keep_keys, on=list(_BREAKOUT_GROUP_COLUMNS), how="inner")


def _build_base_value_panel(
    *,
    resolved_db_path: str,
    normalized_markets: tuple[str, ...],
    normalized_months: tuple[int, ...],
    normalized_counts: tuple[int, ...],
    start_year: int | None,
    end_year: int | None,
    winsor_lower: float,
    winsor_upper: float,
    min_train_observations: int,
    adv_window: int,
    required_positive_columns: tuple[str, ...],
    include_incomplete_last_period: bool,
) -> tuple[AnnualValuePeriodicRebalanceResult, pd.DataFrame]:
    market_codes = _market_query_codes(normalized_markets)
    calendars: list[pd.DataFrame] = []
    with _open_analysis_connection(resolved_db_path) as ctx:
        conn = ctx.connection
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _fetch_date_range(conn, table_name="stock_data")
        trading_dates = _query_trading_dates(conn, start_year=start_year, end_year=end_year)
        for months in normalized_months:
            calendars.append(
                _build_rebalance_calendar_df(
                    trading_dates,
                    rebalance_months=months,
                    include_incomplete_last_period=include_incomplete_last_period,
                )
            )
        rebalance_calendar_df = pd.concat(calendars, ignore_index=True) if calendars else _empty_df([])
        if rebalance_calendar_df.empty:
            base_empty = AnnualValuePeriodicRebalanceResult(
                db_path=resolved_db_path,
                source_mode=source_mode,
                source_detail=source_detail,
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                analysis_start_date=None,
                analysis_end_date=None,
                selected_markets=normalized_markets,
                rebalance_months=normalized_months,
                selection_counts=normalized_counts,
                winsor_lower=winsor_lower,
                winsor_upper=winsor_upper,
                min_train_observations=min_train_observations,
                adv_window=adv_window,
                required_positive_columns=required_positive_columns,
                current_market_snapshot_only=False,
                score_policy="no rebalance periods available",
                rebalance_calendar_df=_empty_df([]),
                event_ledger_df=_empty_df([]),
                scored_panel_df=_empty_df([]),
                walkforward_weight_df=_empty_df([]),
                selected_event_df=_empty_df([]),
                selection_summary_df=_empty_df([]),
                portfolio_daily_df=_empty_df([]),
                portfolio_summary_df=_empty_df([]),
            )
            return base_empty, _empty_df([])
        stock_frames: list[pd.DataFrame] = []
        pit_stock_master_flags: list[bool] = []
        for _, calendar_part in rebalance_calendar_df.groupby("rebalance_months", sort=False):
            stock_part, uses_pit_stock_master_part = _query_entry_stock_master(
                conn,
                calendar_df=calendar_part.reset_index(drop=True),
                market_codes=market_codes,
            )
            if not stock_part.empty:
                stock_frames.append(stock_part)
            pit_stock_master_flags.append(uses_pit_stock_master_part)
        stock_df = pd.concat(stock_frames, ignore_index=True) if stock_frames else _empty_df([])
        uses_pit_stock_master = all(pit_stock_master_flags) if pit_stock_master_flags else False
        allowed_codes = tuple(sorted(set(stock_df["code"].astype(str)))) if not stock_df.empty else ()
        statement_df = _query_statement_rows(conn, codes=allowed_codes)
        price_start_year = int(str(rebalance_calendar_df["entry_date"].min())[:4]) - 2
        price_df = _query_price_rows(
            conn,
            codes=allowed_codes,
            start_date=f"{price_start_year:04d}-01-01",
            end_date=str(rebalance_calendar_df["exit_date"].max()),
        )
        adjustment_event_df = _query_adjustment_event_rows(
            conn,
            codes=allowed_codes,
            end_date=str(rebalance_calendar_df["exit_date"].max()),
        )
    event_ledger_df = _build_periodic_event_ledger_fast(
        stock_df=stock_df,
        statement_df=statement_df,
        price_df=price_df,
        adjustment_event_df=adjustment_event_df,
        adv_window=adv_window,
    )
    period_metadata = rebalance_calendar_df[
        ["year", "calendar_year", "start_month", "rebalance_months"]
    ].drop_duplicates("year")
    if not event_ledger_df.empty:
        event_ledger_df = event_ledger_df.merge(period_metadata, on="year", how="left")
    scored_panel_df = _build_scored_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        required_positive_columns=required_positive_columns,
    )
    if not scored_panel_df.empty and not event_ledger_df.empty:
        scored_panel_df = scored_panel_df.merge(
            event_ledger_df[["event_id", "calendar_year", "start_month", "rebalance_months"]]
            .drop_duplicates("event_id"),
            on="event_id",
            how="left",
        )
    scored_panel_df = _apply_periodic_score_columns(scored_panel_df)
    walkforward_weight_df = _build_walkforward_weight_df(
        scored_panel_df,
        min_train_observations=min_train_observations,
    )
    realized_df = event_ledger_df[event_ledger_df["status"].astype(str) == "realized"].copy()
    base_result = AnnualValuePeriodicRebalanceResult(
        db_path=resolved_db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=str(realized_df["entry_date"].min()) if not realized_df.empty else None,
        analysis_end_date=str(realized_df["exit_date"].max()) if not realized_df.empty else None,
        selected_markets=normalized_markets,
        rebalance_months=normalized_months,
        selection_counts=normalized_counts,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_train_observations=min_train_observations,
        adv_window=adv_window,
        required_positive_columns=required_positive_columns,
        current_market_snapshot_only=not uses_pit_stock_master,
        score_policy=(
            "full liquidation and replacement at each N-month period; value scores are "
            "PIT as of each entry date; breakout features use the prior trading session"
        ),
        rebalance_calendar_df=rebalance_calendar_df,
        event_ledger_df=event_ledger_df,
        scored_panel_df=scored_panel_df,
        walkforward_weight_df=walkforward_weight_df,
        selected_event_df=_empty_df([]),
        selection_summary_df=_empty_df([]),
        portfolio_daily_df=_empty_df([]),
        portfolio_summary_df=_empty_df([]),
    )
    return base_result, price_df


def run_annual_value_breakout_periodic_rebalance(
    db_path: str | Path,
    *,
    markets: Sequence[str] = DEFAULT_MARKETS,
    rebalance_months: Sequence[int] = DEFAULT_REBALANCE_MONTHS,
    selection_counts: Sequence[int] = DEFAULT_SELECTION_COUNTS,
    score_methods: Sequence[str] | None = None,
    liquidity_scenarios: Sequence[str] | None = None,
    breakout_policies: Sequence[str] | None = None,
    factor_weight_step: float | None = None,
    factor_weights: Sequence[tuple[float, float, float]] = (),
    max_portfolio_configs: int | None = None,
    skip_portfolio_curves: bool = False,
    breakout_windows: Sequence[int] = DEFAULT_BREAKOUT_WINDOWS,
    breakout_lookback_sessions: Sequence[int] = DEFAULT_BREAKOUT_LOOKBACK_SESSIONS,
    breakout_score_boost: float = DEFAULT_BREAKOUT_SCORE_BOOST,
    start_year: int | None = None,
    end_year: int | None = None,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_train_observations: int = DEFAULT_MIN_TRAIN_OBSERVATIONS,
    adv_window: int = DEFAULT_ADV_WINDOW,
    required_positive_columns: Sequence[str] = (),
    include_incomplete_last_period: bool = False,
) -> AnnualValueBreakoutPeriodicRebalanceResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_train_observations < 5:
        raise ValueError("min_train_observations must be >= 5")
    if breakout_score_boost < 0:
        raise ValueError("breakout_score_boost must be non-negative")
    if max_portfolio_configs is not None and max_portfolio_configs < 1:
        raise ValueError("max_portfolio_configs must be positive when provided")
    resolved_db_path = str(Path(db_path).expanduser())
    normalized_markets = _normalize_selected_markets(markets)
    normalized_months = _normalize_rebalance_months(rebalance_months)
    normalized_counts = _normalize_selection_counts(selection_counts)
    normalized_breakout_windows = _normalize_positive_int_sequence(
        breakout_windows,
        label="breakout window",
    )
    normalized_lookbacks = _normalize_nonnegative_int_sequence(
        breakout_lookback_sessions,
        label="breakout lookback session",
    )
    normalized_adv_window = _validate_adv_window(adv_window)
    normalized_positive_columns = _normalize_required_positive_columns(required_positive_columns)
    normalized_factor_weights = _normalize_factor_weights(factor_weights)
    factor_weight_specs = _build_factor_weight_specs(
        explicit_weights=normalized_factor_weights,
        factor_weight_step=factor_weight_step,
    )
    score_method_specs, score_method_params_df = _build_score_method_specs_and_params(
        factor_weight_specs=factor_weight_specs,
    )
    known_score_methods = {method.name for method in score_method_specs}
    normalized_score_methods = tuple(score_methods or sorted(known_score_methods))
    unknown_score_methods = set(normalized_score_methods) - known_score_methods
    if unknown_score_methods:
        raise ValueError(f"unknown score methods: {sorted(unknown_score_methods)}")
    known_liquidity_scenarios = {scenario.name for scenario in LIQUIDITY_SCENARIOS}
    normalized_liquidity_scenarios = tuple(liquidity_scenarios or sorted(known_liquidity_scenarios))
    unknown_liquidity_scenarios = set(normalized_liquidity_scenarios) - known_liquidity_scenarios
    if unknown_liquidity_scenarios:
        raise ValueError(f"unknown liquidity scenarios: {sorted(unknown_liquidity_scenarios)}")
    known_breakout_policies = {
        spec.policy
        for spec in _breakout_specs(
            breakout_windows=normalized_breakout_windows,
            lookback_sessions=normalized_lookbacks,
            breakout_score_boost=float(breakout_score_boost),
        )
    }
    normalized_breakout_policies = tuple(breakout_policies or sorted(known_breakout_policies))
    unknown_breakout_policies = set(normalized_breakout_policies) - known_breakout_policies
    if unknown_breakout_policies:
        raise ValueError(f"unknown breakout policies: {sorted(unknown_breakout_policies)}")
    base_result, price_df = _build_base_value_panel(
        resolved_db_path=resolved_db_path,
        normalized_markets=normalized_markets,
        normalized_months=normalized_months,
        normalized_counts=normalized_counts,
        start_year=start_year,
        end_year=end_year,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_train_observations=min_train_observations,
        adv_window=normalized_adv_window,
        required_positive_columns=normalized_positive_columns,
        include_incomplete_last_period=include_incomplete_last_period,
    )
    if base_result.scored_panel_df.empty:
        return _empty_result(
            db_path=resolved_db_path,
            source_mode=base_result.source_mode,
            source_detail=base_result.source_detail,
            available_start_date=base_result.available_start_date,
            available_end_date=base_result.available_end_date,
            selected_markets=normalized_markets,
            rebalance_months=normalized_months,
            selection_counts=normalized_counts,
            score_methods=normalized_score_methods,
            liquidity_scenarios=normalized_liquidity_scenarios,
            breakout_policies=normalized_breakout_policies,
            factor_weight_step=factor_weight_step,
            factor_weights=normalized_factor_weights,
            max_portfolio_configs=max_portfolio_configs,
            skip_portfolio_curves=skip_portfolio_curves,
            breakout_windows=normalized_breakout_windows,
            score_method_params_df=score_method_params_df,
            breakout_lookback_sessions=normalized_lookbacks,
            breakout_score_boost=float(breakout_score_boost),
            winsor_lower=winsor_lower,
            winsor_upper=winsor_upper,
            min_train_observations=min_train_observations,
            adv_window=normalized_adv_window,
            required_positive_columns=normalized_positive_columns,
            current_market_snapshot_only=base_result.current_market_snapshot_only,
            score_policy=base_result.score_policy,
        )
    trading_dates = (
        sorted(price_df["date"].astype(str).unique().tolist())
        if not price_df.empty
        else []
    )
    scored_panel_df = _apply_factor_weight_score_columns(
        base_result.scored_panel_df,
        factor_weight_specs,
    )
    breakout_feature_df = _build_breakout_feature_df(
        scored_panel_df,
        price_df,
        trading_dates,
        breakout_windows=normalized_breakout_windows,
    )
    breakout_scored_panel_df = _merge_breakout_features(
        scored_panel_df,
        breakout_feature_df,
        breakout_windows=normalized_breakout_windows,
    )
    selected_event_df = _build_selected_event_df(
        breakout_scored_panel_df,
        base_result.walkforward_weight_df,
        selection_counts=normalized_counts,
        score_method_specs=score_method_specs,
        breakout_specs=tuple(
            spec
            for spec in _breakout_specs(
                breakout_windows=normalized_breakout_windows,
                lookback_sessions=normalized_lookbacks,
                breakout_score_boost=float(breakout_score_boost),
            )
            if spec.policy in normalized_breakout_policies
        ),
        breakout_windows=normalized_breakout_windows,
        breakout_score_boost=float(breakout_score_boost),
        score_methods=normalized_score_methods,
        liquidity_scenarios=normalized_liquidity_scenarios,
    )
    selection_summary_df = _build_selection_summary_df(selected_event_df)
    if skip_portfolio_curves:
        portfolio_daily_df = _build_portfolio_daily_df(_empty_df([]), price_df)
        portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, _empty_df([]))
    else:
        portfolio_selected_event_df = _filter_selected_events_for_portfolio(
            selected_event_df,
            selection_summary_df,
            max_portfolio_configs=max_portfolio_configs,
        )
        portfolio_daily_df = _build_portfolio_daily_df(portfolio_selected_event_df, price_df)
        portfolio_summary_df = _build_portfolio_summary_df(
            portfolio_daily_df,
            portfolio_selected_event_df,
        )
    return AnnualValueBreakoutPeriodicRebalanceResult(
        db_path=resolved_db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        selected_markets=normalized_markets,
        rebalance_months=normalized_months,
        selection_counts=normalized_counts,
        score_methods=normalized_score_methods,
        liquidity_scenarios=normalized_liquidity_scenarios,
        breakout_policies=normalized_breakout_policies,
        factor_weight_step=factor_weight_step,
        factor_weights=normalized_factor_weights,
        max_portfolio_configs=max_portfolio_configs,
        skip_portfolio_curves=skip_portfolio_curves,
        breakout_windows=normalized_breakout_windows,
        breakout_lookback_sessions=normalized_lookbacks,
        breakout_score_boost=float(breakout_score_boost),
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_train_observations=min_train_observations,
        adv_window=normalized_adv_window,
        required_positive_columns=normalized_positive_columns,
        current_market_snapshot_only=base_result.current_market_snapshot_only,
        score_policy=base_result.score_policy,
        rebalance_calendar_df=base_result.rebalance_calendar_df,
        event_ledger_df=base_result.event_ledger_df,
        scored_panel_df=scored_panel_df,
        score_method_params_df=score_method_params_df,
        breakout_feature_df=breakout_feature_df,
        breakout_scored_panel_df=breakout_scored_panel_df,
        walkforward_weight_df=base_result.walkforward_weight_df,
        selected_event_df=selected_event_df,
        selection_summary_df=selection_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _build_summary_markdown(result: AnnualValueBreakoutPeriodicRebalanceResult) -> str:
    lines = [
        "# Annual Value Breakout Periodic Rebalance",
        "",
        "## Setup",
        "",
        f"- DB path: `{result.db_path}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Rebalance months: `{', '.join(str(value) for value in result.rebalance_months)}`",
        f"- Selection counts: `{', '.join(str(value) for value in result.selection_counts)}`",
        f"- Score methods: `{', '.join(result.score_methods)}`",
        f"- Liquidity scenarios: `{', '.join(result.liquidity_scenarios)}`",
        f"- Breakout policies: `{', '.join(result.breakout_policies)}`",
        f"- Factor weight step: `{result.factor_weight_step}`",
        f"- Explicit factor weights: `{len(result.factor_weights)}`",
        f"- Max portfolio configs: `{result.max_portfolio_configs or 'all'}`",
        f"- Skip portfolio curves: `{result.skip_portfolio_curves}`",
        f"- Breakout windows: `{', '.join(str(value) for value in result.breakout_windows)}`",
        f"- Breakout lookbacks: `{', '.join(str(value) for value in result.breakout_lookback_sessions)}`",
        f"- Breakout score boost: `{result.breakout_score_boost}`",
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
            (summary["market_scope"].astype(str).isin(["prime", "standard"]))
            & (summary["liquidity_scenario"].astype(str).isin(["none", "adv10m"]))
        ].copy()
        if focus.empty:
            focus = summary.copy()
        focus = focus.sort_values("sharpe_ratio", ascending=False, na_position="last").head(20)
        for row in focus.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['score_method']}` / "
                f"`{row['liquidity_scenario']}` / `{row['breakout_policy']}` / "
                f"`{int(cast(int, row['breakout_window']))}d` / "
                f"`{int(cast(int, row['breakout_lookback_sessions']))}s` / "
                f"`{int(cast(int, row['rebalance_months']))}m` / "
                f"top `{int(cast(int, row['selection_count']))}`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"events `{int(cast(int, row['realized_event_count']))}`"
            )
    return "\n".join(lines)


def _build_published_summary(result: AnnualValueBreakoutPeriodicRebalanceResult) -> dict[str, Any]:
    return {
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "rebalanceMonths": list(result.rebalance_months),
        "selectionCounts": list(result.selection_counts),
        "scoreMethods": list(result.score_methods),
        "liquidityScenarios": list(result.liquidity_scenarios),
        "breakoutPolicies": list(result.breakout_policies),
        "factorWeightStep": result.factor_weight_step,
        "factorWeights": [list(weights) for weights in result.factor_weights],
        "maxPortfolioConfigs": result.max_portfolio_configs,
        "skipPortfolioCurves": result.skip_portfolio_curves,
        "breakoutWindows": list(result.breakout_windows),
        "breakoutLookbackSessions": list(result.breakout_lookback_sessions),
        "breakoutScoreBoost": result.breakout_score_boost,
        "scorePolicy": result.score_policy,
        "selectionSummary": result.selection_summary_df.to_dict(orient="records"),
        "portfolioSummary": result.portfolio_summary_df.to_dict(orient="records"),
    }


def write_annual_value_breakout_periodic_rebalance_bundle(
    result: AnnualValueBreakoutPeriodicRebalanceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_value_breakout_periodic_rebalance",
        params={
            "db_path": result.db_path,
            "markets": list(result.selected_markets),
            "rebalance_months": list(result.rebalance_months),
            "selection_counts": list(result.selection_counts),
            "score_methods": list(result.score_methods),
            "liquidity_scenarios": list(result.liquidity_scenarios),
            "breakout_policies": list(result.breakout_policies),
            "factor_weight_step": result.factor_weight_step,
            "factor_weights": [list(weights) for weights in result.factor_weights],
            "max_portfolio_configs": result.max_portfolio_configs,
            "skip_portfolio_curves": result.skip_portfolio_curves,
            "breakout_windows": list(result.breakout_windows),
            "breakout_lookback_sessions": list(result.breakout_lookback_sessions),
            "breakout_score_boost": result.breakout_score_boost,
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_train_observations": result.min_train_observations,
            "adv_window": result.adv_window,
            "required_positive_columns": list(result.required_positive_columns),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        # bundle-structured-fallback: legacy bundle consumers still read summary.json.
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_value_breakout_periodic_rebalance_bundle(
    bundle_path: str | Path,
) -> AnnualValueBreakoutPeriodicRebalanceResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualValueBreakoutPeriodicRebalanceResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_value_breakout_periodic_rebalance_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_value_breakout_periodic_rebalance_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
