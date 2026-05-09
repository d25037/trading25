"""Overheat-filter overlay research for annual value + breakout selections."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import duckdb
import numpy as np
import pandas as pd

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
from src.domains.strategy.indicators import compute_risk_adjusted_return, compute_rsi

VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID = "market-behavior/value-breakout-overheat-filter"
ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID = (
    "market-behavior/annual-value-breakout-periodic-rebalance"
)
DEFAULT_MARKET_SCOPE = "standard"
DEFAULT_SCORE_METHOD = "prime_size_tilt"
DEFAULT_LIQUIDITY_SCENARIO = "adv10m"
DEFAULT_BREAKOUT_POLICY = "breakout_additive"
DEFAULT_BREAKOUT_WINDOW = 120
DEFAULT_BREAKOUT_LOOKBACK_SESSIONS = 20
DEFAULT_REBALANCE_MONTHS = 3
DEFAULT_SELECTION_COUNT = 10
DEFAULT_HOLDOUT_MONTHS = 6
DEFAULT_THRESHOLD_QUANTILE = 0.80
DEFAULT_SIZE_HAIRCUT = 0.5
DEFAULT_RISK_RATIO_TYPE: Literal["sharpe", "sortino"] = "sharpe"
_HORIZONS: tuple[int, ...] = (10, 20, 60)
_WARMUP_CALENDAR_DAYS = 420
_GROUP_COLUMNS: tuple[str, ...] = (
    "market_scope",
    "score_method",
    "liquidity_scenario",
    "breakout_policy",
    "breakout_window",
    "breakout_lookback_sessions",
    "rebalance_months",
    "selection_count",
)
_PORTFOLIO_GROUP_COLUMNS: tuple[str, ...] = (*_GROUP_COLUMNS, "rule_name", "variant_name")
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_selected_event_df",
    "threshold_summary_df",
    "overheat_rule_event_df",
    "overheat_rule_summary_df",
    "portfolio_event_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class _TechnicalFeatureSpec:
    name: str
    family: str
    label: str
    horizon_days: int


@dataclass(frozen=True)
class OverheatRuleSpec:
    name: str
    label: str
    description: str
    feature_names: tuple[str, ...]
    min_count: int


@dataclass(frozen=True)
class ValueBreakoutOverheatFilterResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_scope: str
    score_method: str
    liquidity_scenario: str
    breakout_policy: str
    breakout_window: int
    breakout_lookback_sessions: int
    rebalance_months: int
    selection_count: int
    holdout_months: int
    threshold_quantile: float
    size_haircut: float
    risk_ratio_type: str
    selected_event_count: int
    technical_feature_count: int
    technical_policy: str
    enriched_selected_event_df: pd.DataFrame
    threshold_summary_df: pd.DataFrame
    overheat_rule_event_df: pd.DataFrame
    overheat_rule_summary_df: pd.DataFrame
    portfolio_event_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


_FEATURE_SPECS: tuple[_TechnicalFeatureSpec, ...] = tuple(
    spec
    for horizon in _HORIZONS
    for spec in (
        _TechnicalFeatureSpec(f"rsi_{horizon}", "rsi", f"RSI {horizon}", horizon),
        _TechnicalFeatureSpec(
            f"runup_{horizon}d_pct",
            "runup",
            f"Run-up {horizon}d",
            horizon,
        ),
        _TechnicalFeatureSpec(
            f"risk_adjusted_return_{horizon}d",
            "risk_adjusted_return",
            f"Risk-adjusted return {horizon}d",
            horizon,
        ),
    )
)

OVERHEAT_RULES: tuple[OverheatRuleSpec, ...] = (
    OverheatRuleSpec(
        "short_climax_10d_q80_overlap_ge2",
        "Short climax 10d",
        "At least two of RSI10, 10d run-up, and 10d risk-adjusted return exceed train Q80.",
        ("rsi_10", "runup_10d_pct", "risk_adjusted_return_10d"),
        2,
    ),
    OverheatRuleSpec(
        "trend_maturity_60d_q80_overlap_ge2",
        "Trend maturity 60d",
        "At least two of RSI60, 60d run-up, and 60d risk-adjusted return exceed train Q80.",
        ("rsi_60", "runup_60d_pct", "risk_adjusted_return_60d"),
        2,
    ),
    OverheatRuleSpec(
        "legacy_20_60_runup_rar60_q80_overlap_ge2",
        "Legacy 20/60 run-up + RAR60",
        "Closest v3 equivalent of the prior 20d/60d run-up plus RAR60 overlap.",
        ("runup_20d_pct", "runup_60d_pct", "risk_adjusted_return_60d"),
        2,
    ),
    OverheatRuleSpec(
        "overheat_runup_rar_cross_horizon_q80_overlap_ge3",
        "Cross-horizon run-up/RAR",
        "At least three run-up or risk-adjusted-return horizons exceed train Q80.",
        tuple(
            feature
            for horizon in _HORIZONS
            for feature in (f"runup_{horizon}d_pct", f"risk_adjusted_return_{horizon}d")
        ),
        3,
    ),
)
_RULE_LABELS = {rule.name: rule.label for rule in OVERHEAT_RULES}


def run_value_breakout_overheat_filter(
    input_bundle_path: str | Path | None = None,
    *,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    market_scope: str = DEFAULT_MARKET_SCOPE,
    score_method: str = DEFAULT_SCORE_METHOD,
    liquidity_scenario: str = DEFAULT_LIQUIDITY_SCENARIO,
    breakout_policy: str = DEFAULT_BREAKOUT_POLICY,
    breakout_window: int = DEFAULT_BREAKOUT_WINDOW,
    breakout_lookback_sessions: int = DEFAULT_BREAKOUT_LOOKBACK_SESSIONS,
    rebalance_months: int = DEFAULT_REBALANCE_MONTHS,
    selection_count: int = DEFAULT_SELECTION_COUNT,
    holdout_months: int = DEFAULT_HOLDOUT_MONTHS,
    threshold_quantile: float = DEFAULT_THRESHOLD_QUANTILE,
    size_haircut: float = DEFAULT_SIZE_HAIRCUT,
    risk_ratio_type: Literal["sharpe", "sortino"] = DEFAULT_RISK_RATIO_TYPE,
) -> ValueBreakoutOverheatFilterResult:
    if holdout_months < 0:
        raise ValueError("holdout_months must be >= 0")
    if not 0.0 < threshold_quantile < 1.0:
        raise ValueError("threshold_quantile must satisfy 0 < q < 1")
    if not 0.0 < size_haircut <= 1.0:
        raise ValueError("size_haircut must satisfy 0 < size_haircut <= 1")

    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_annual_value_breakout_periodic_rebalance_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Annual value+breakout periodic rebalance bundle was not found. "
            "Run run_annual_value_breakout_periodic_rebalance.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    tables = load_research_bundle_tables(resolved_input, table_names=("selected_event_df",))
    selected_event_df = _filter_selected_event_df(
        tables["selected_event_df"],
        market_scope=market_scope,
        score_method=score_method,
        liquidity_scenario=liquidity_scenario,
        breakout_policy=breakout_policy,
        breakout_window=breakout_window,
        breakout_lookback_sessions=breakout_lookback_sessions,
        rebalance_months=rebalance_months,
        selection_count=selection_count,
    )
    resolved_db_path = str(Path(db_path).expanduser() if db_path is not None else input_info.db_path)
    stock_price_df = _query_stock_price_rows(resolved_db_path, selected_event_df)
    enriched = _add_technical_features(
        selected_event_df,
        stock_price_df=stock_price_df,
        risk_ratio_type=risk_ratio_type,
    )
    threshold_summary_df = _build_threshold_summary_df(
        enriched,
        holdout_months=holdout_months,
        threshold_quantile=threshold_quantile,
    )
    thresholds = _threshold_lookup(threshold_summary_df)
    overheat_rule_event_df = _build_overheat_rule_event_df(enriched, thresholds=thresholds)
    overheat_rule_summary_df = _build_overheat_rule_summary_df(overheat_rule_event_df)
    portfolio_event_df = _build_portfolio_event_df(
        overheat_rule_event_df,
        size_haircut=size_haircut,
    )
    portfolio_daily_df = _build_portfolio_daily_df(
        portfolio_event_df,
        stock_price_df=stock_price_df,
    )
    portfolio_summary_df = _build_portfolio_summary_df(
        portfolio_daily_df,
        portfolio_event_df,
    )
    technical_feature_count = int(
        enriched[[spec.name for spec in _FEATURE_SPECS]].notna().any(axis=1).sum()
        if not enriched.empty
        else 0
    )
    return ValueBreakoutOverheatFilterResult(
        db_path=resolved_db_path,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=_min_date(selected_event_df, "entry_date"),
        analysis_end_date=_max_date(selected_event_df, "exit_date"),
        market_scope=market_scope,
        score_method=score_method,
        liquidity_scenario=liquidity_scenario,
        breakout_policy=breakout_policy,
        breakout_window=breakout_window,
        breakout_lookback_sessions=breakout_lookback_sessions,
        rebalance_months=rebalance_months,
        selection_count=selection_count,
        holdout_months=holdout_months,
        threshold_quantile=threshold_quantile,
        size_haircut=size_haircut,
        risk_ratio_type=risk_ratio_type,
        selected_event_count=int(len(selected_event_df)),
        technical_feature_count=technical_feature_count,
        technical_policy=(
            "features use signal_date when present, otherwise the latest trading session "
            "strictly before entry_date; Q80 thresholds are calibrated per market_scope on "
            f"the pre-holdout sample ({holdout_months} months, fallback to all rows if empty); "
            "portfolio variants are baseline, hard exclude without refill, and size haircut"
        ),
        enriched_selected_event_df=enriched,
        threshold_summary_df=threshold_summary_df,
        overheat_rule_event_df=overheat_rule_event_df,
        overheat_rule_summary_df=overheat_rule_summary_df,
        portfolio_event_df=portfolio_event_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _filter_selected_event_df(
    frame: pd.DataFrame,
    *,
    market_scope: str,
    score_method: str,
    liquidity_scenario: str,
    breakout_policy: str,
    breakout_window: int,
    breakout_lookback_sessions: int,
    rebalance_months: int,
    selection_count: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    mask = (
        frame["market_scope"].astype(str).eq(market_scope)
        & frame["score_method"].astype(str).eq(score_method)
        & frame["liquidity_scenario"].astype(str).eq(liquidity_scenario)
        & frame["breakout_policy"].astype(str).eq(breakout_policy)
        & pd.to_numeric(frame["breakout_window"], errors="coerce").eq(breakout_window)
        & pd.to_numeric(frame["breakout_lookback_sessions"], errors="coerce").eq(
            breakout_lookback_sessions
        )
        & pd.to_numeric(frame["rebalance_months"], errors="coerce").eq(rebalance_months)
        & pd.to_numeric(frame["selection_count"], errors="coerce").eq(selection_count)
    )
    result = frame[mask].copy().reset_index(drop=True)
    for column in ("entry_date", "signal_date", "exit_date"):
        if column in result.columns:
            result[column] = result[column].astype(str)
    return result


def _query_stock_price_rows(db_path: str, selected_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["code", "date", "open", "high", "low", "close", "volume"]
    if selected_event_df.empty:
        return _empty_df(columns)
    codes = tuple(sorted(set(selected_event_df["code"].astype(str))))
    entry_dates = pd.to_datetime(selected_event_df["entry_date"], errors="coerce").dropna()
    exit_dates = pd.to_datetime(selected_event_df["exit_date"], errors="coerce").dropna()
    if entry_dates.empty or exit_dates.empty:
        return _empty_df(columns)
    start_date = (entry_dates.min() - pd.Timedelta(days=_WARMUP_CALENDAR_DAYS)).strftime("%Y-%m-%d")
    end_date = exit_dates.max().strftime("%Y-%m-%d")
    conn = duckdb.connect(str(Path(db_path).expanduser()), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in codes)
        return conn.execute(
            f"""
            SELECT code, date, open, high, low, close, volume
            FROM stock_data
            WHERE code IN ({placeholders})
              AND date >= ?
              AND date <= ?
              AND close IS NOT NULL
            ORDER BY code, date
            """,
            [*codes, start_date, end_date],
        ).fetchdf()
    finally:
        conn.close()


def _add_technical_features(
    selected_event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
    risk_ratio_type: Literal["sharpe", "sortino"],
) -> pd.DataFrame:
    result = selected_event_df.copy().reset_index(drop=True)
    if result.empty:
        return _ensure_feature_columns(result)
    feature_frames = {
        str(code): _build_symbol_feature_df(frame, risk_ratio_type=risk_ratio_type)
        for code, frame in stock_price_df.groupby("code", sort=False)
    }
    rows: list[dict[str, Any]] = []
    for event in result.to_dict(orient="records"):
        event_payload = cast(dict[str, Any], event)
        code = str(event_payload.get("code", ""))
        feature_df = feature_frames.get(code)
        rows.append(_lookup_feature_row(feature_df, event_payload))
    return _ensure_feature_columns(pd.concat([result, pd.DataFrame(rows)], axis=1))


def _build_symbol_feature_df(
    frame: pd.DataFrame,
    *,
    risk_ratio_type: Literal["sharpe", "sortino"],
) -> pd.DataFrame:
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna(subset=["date"]).sort_values("date", kind="stable")
    result = result.drop_duplicates("date", keep="last").reset_index(drop=True)
    close = pd.to_numeric(result["close"], errors="coerce").astype(float)
    feature_df = pd.DataFrame({"date": result["date"], "close": close})
    for horizon in _HORIZONS:
        feature_df[f"rsi_{horizon}"] = compute_rsi(close, horizon)
        feature_df[f"runup_{horizon}d_pct"] = (close / close.shift(horizon) - 1.0) * 100.0
        feature_df[f"risk_adjusted_return_{horizon}d"] = compute_risk_adjusted_return(
            close=close,
            lookback_period=horizon,
            ratio_type=risk_ratio_type,
        )
    return feature_df


def _lookup_feature_row(
    feature_df: pd.DataFrame | None,
    event: dict[str, Any],
) -> dict[str, Any]:
    if feature_df is None or feature_df.empty:
        return _missing_feature_row()
    signal_date = _coerce_timestamp(event.get("signal_date"))
    entry_date = _coerce_timestamp(event.get("entry_date"))
    dates = pd.DatetimeIndex(pd.to_datetime(feature_df["date"], errors="coerce"))
    if signal_date is not None:
        position = dates.searchsorted(signal_date, side="right") - 1
    elif entry_date is not None:
        position = dates.searchsorted(entry_date, side="left") - 1
    else:
        position = -1
    if position < 0:
        return _missing_feature_row()
    row = feature_df.iloc[int(position)]
    feature_date = cast(pd.Timestamp, row["date"])
    lag_base = entry_date or signal_date
    return {
        "technical_feature_date": feature_date.strftime("%Y-%m-%d"),
        "technical_feature_lag_days": float((lag_base - feature_date).days)
        if lag_base is not None
        else np.nan,
        **{spec.name: _float_or_nan(row.get(spec.name)) for spec in _FEATURE_SPECS},
    }


def _missing_feature_row() -> dict[str, Any]:
    return {
        "technical_feature_date": None,
        "technical_feature_lag_days": np.nan,
        **{spec.name: np.nan for spec in _FEATURE_SPECS},
    }


def _ensure_feature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in ("technical_feature_date", "technical_feature_lag_days"):
        if column not in result.columns:
            result[column] = np.nan
    for spec in _FEATURE_SPECS:
        if spec.name not in result.columns:
            result[spec.name] = np.nan
    return result


def _build_threshold_summary_df(
    enriched: pd.DataFrame,
    *,
    holdout_months: int,
    threshold_quantile: float,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "feature_family",
        "feature_name",
        "feature_label",
        "horizon_days",
        "threshold_quantile",
        "threshold_value",
        "calibration_event_count",
        "calibration_end_date",
    ]
    if enriched.empty:
        return _empty_df(columns)
    enriched = enriched.copy()
    signal_source = (
        enriched["signal_date"]
        if "signal_date" in enriched.columns
        else pd.Series(index=enriched.index, dtype=object)
    )
    signal_dates = pd.to_datetime(signal_source, errors="coerce")
    if signal_dates.notna().any() and holdout_months > 0:
        holdout_start = cast(pd.Timestamp, signal_dates.max()) - pd.DateOffset(months=holdout_months)
        calibration = enriched[signal_dates < holdout_start].copy()
        calibration_end_date = holdout_start.strftime("%Y-%m-%d")
    else:
        calibration = enriched.copy()
        calibration_end_date = None
    if calibration.empty:
        calibration = enriched.copy()
    records: list[dict[str, Any]] = []
    for market_scope, group in calibration.groupby("market_scope", observed=True, sort=False):
        for spec in _FEATURE_SPECS:
            values = pd.to_numeric(group[spec.name], errors="coerce").dropna()
            threshold = float(values.quantile(threshold_quantile)) if not values.empty else np.nan
            records.append(
                {
                    "market_scope": str(market_scope),
                    "feature_family": spec.family,
                    "feature_name": spec.name,
                    "feature_label": spec.label,
                    "horizon_days": spec.horizon_days,
                    "threshold_quantile": threshold_quantile,
                    "threshold_value": threshold,
                    "calibration_event_count": int(len(values)),
                    "calibration_end_date": calibration_end_date,
                }
            )
    return pd.DataFrame(records)[columns]


def _threshold_lookup(threshold_summary_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    return {
        (str(row["market_scope"]), str(row["feature_name"])): _float_or_nan(
            row.get("threshold_value")
        )
        for row in threshold_summary_df.to_dict(orient="records")
    }


def _build_overheat_rule_event_df(
    enriched: pd.DataFrame,
    *,
    thresholds: dict[tuple[str, str], float],
) -> pd.DataFrame:
    if enriched.empty:
        return _empty_df([])
    frames: list[pd.DataFrame] = []
    for rule in OVERHEAT_RULES:
        frame = enriched.copy()
        overlap_count = _overlap_count(frame, rule, thresholds=thresholds)
        frame["rule_name"] = rule.name
        frame["rule_label"] = rule.label
        frame["rule_description"] = rule.description
        frame["overheat_feature_count"] = overlap_count
        frame["is_overheat"] = overlap_count >= rule.min_count
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _overlap_count(
    frame: pd.DataFrame,
    rule: OverheatRuleSpec,
    *,
    thresholds: dict[tuple[str, str], float],
) -> pd.Series:
    count = pd.Series(0, index=frame.index, dtype=int)
    for feature_name in rule.feature_names:
        feature_values = pd.to_numeric(frame[feature_name], errors="coerce")
        feature_thresholds = frame["market_scope"].astype(str).map(
            lambda scope: thresholds.get((scope, feature_name), np.nan)
        )
        count += (feature_values >= feature_thresholds).fillna(False).astype(int)
    return count


def _build_overheat_rule_summary_df(overheat_rule_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        *_GROUP_COLUMNS,
        "rule_name",
        "rule_label",
        "event_count",
        "overheat_event_count",
        "overheat_event_pct",
        "base_mean_return_pct",
        "overheat_mean_return_pct",
        "kept_mean_return_pct",
        "overheat_win_rate_pct",
        "kept_win_rate_pct",
        "overheat_worst_return_pct",
        "kept_worst_return_pct",
    ]
    if overheat_rule_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in overheat_rule_event_df.groupby(
        [*_GROUP_COLUMNS, "rule_name"],
        observed=True,
        sort=False,
    ):
        overheat = group[group["is_overheat"].fillna(False).astype(bool)]
        kept = group[~group["is_overheat"].fillna(False).astype(bool)]
        returns = _return_series(group)
        overheat_returns = _return_series(overheat)
        kept_returns = _return_series(kept)
        records.append(
            {
                **dict(zip([*_GROUP_COLUMNS, "rule_name"], keys, strict=True)),
                "rule_label": _RULE_LABELS.get(str(keys[-1]), str(keys[-1])),
                "event_count": int(len(group)),
                "overheat_event_count": int(len(overheat)),
                "overheat_event_pct": float(len(overheat) / len(group) * 100.0)
                if len(group)
                else None,
                "base_mean_return_pct": _mean_or_none(returns),
                "overheat_mean_return_pct": _mean_or_none(overheat_returns),
                "kept_mean_return_pct": _mean_or_none(kept_returns),
                "overheat_win_rate_pct": _win_rate_or_none(overheat_returns),
                "kept_win_rate_pct": _win_rate_or_none(kept_returns),
                "overheat_worst_return_pct": _min_or_none(overheat_returns),
                "kept_worst_return_pct": _min_or_none(kept_returns),
            }
        )
    return pd.DataFrame(records)[columns]


def _build_portfolio_event_df(
    overheat_rule_event_df: pd.DataFrame,
    *,
    size_haircut: float,
) -> pd.DataFrame:
    if overheat_rule_event_df.empty:
        return _empty_df([])
    frames: list[pd.DataFrame] = []
    baseline = overheat_rule_event_df.drop_duplicates("event_id", keep="first").copy()
    baseline["rule_name"] = "base"
    baseline["rule_label"] = "Base"
    baseline["variant_name"] = "base"
    baseline["position_weight"] = 1.0
    baseline["is_kept"] = True
    frames.append(baseline)
    for _, rule_group in overheat_rule_event_df.groupby("rule_name", sort=False):
        exclude = rule_group[~rule_group["is_overheat"].fillna(False).astype(bool)].copy()
        exclude["variant_name"] = "exclude_no_refill"
        exclude["position_weight"] = 1.0
        exclude["is_kept"] = True
        frames.append(exclude)

        haircut = rule_group.copy()
        haircut["variant_name"] = "haircut_0_5"
        haircut["position_weight"] = np.where(
            haircut["is_overheat"].fillna(False).astype(bool),
            size_haircut,
            1.0,
        )
        haircut["is_kept"] = True
        frames.append(haircut)
    return pd.concat(frames, ignore_index=True)


def _build_portfolio_daily_df(
    portfolio_event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        *_PORTFOLIO_GROUP_COLUMNS,
        "date",
        "active_positions",
        "active_weight",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if portfolio_event_df.empty or stock_price_df.empty:
        return _empty_df(columns)
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in stock_price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[Any, ...], list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0])
    for event in portfolio_event_df.to_dict(orient="records"):
        weight = _float_or_nan(event.get("position_weight"))
        if not math.isfinite(weight) or weight <= 0.0:
            continue
        price_frame = price_by_code.get(str(event["code"]))
        if price_frame is None:
            continue
        path_df = price_frame[
            (price_frame["date"].astype(str) >= str(event["entry_date"]))
            & (price_frame["date"].astype(str) <= str(event["exit_date"]))
        ].copy()
        if path_df.empty:
            continue
        entry_open_value = _float_or_nan(event.get("entry_open"))
        if not math.isfinite(entry_open_value) or entry_open_value <= 0.0:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open_value], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        group_key = tuple(event[column] for column in _PORTFOLIO_GROUP_COLUMNS)
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            bucket = aggregate[(*group_key, str(date_value))]
            bucket[0] += float(daily_return) * weight
            bucket[1] += weight
            bucket[2] += 1.0
    if not aggregate:
        return _empty_df(columns)
    records = [
        {
            **dict(zip(_PORTFOLIO_GROUP_COLUMNS, key[:-1], strict=True)),
            "date": key[-1],
            "active_positions": int(values[2]),
            "active_weight": float(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for key, values in aggregate.items()
        if values[1] > 0.0
    ]
    daily_df = pd.DataFrame(records).sort_values(
        [*_PORTFOLIO_GROUP_COLUMNS, "date"],
        kind="stable",
    )
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    for _, group in daily_df.groupby(list(_PORTFOLIO_GROUP_COLUMNS), observed=True, sort=False):
        idx = list(group.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df.reset_index(drop=True)[columns]


def _build_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    portfolio_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        *_PORTFOLIO_GROUP_COLUMNS,
        "rule_label",
        "event_count",
        "overheat_event_count",
        "start_date",
        "end_date",
        "active_days",
        "avg_active_positions",
        "avg_active_weight",
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
    event_counts = portfolio_event_df.groupby(list(_PORTFOLIO_GROUP_COLUMNS), observed=True, sort=False).size().to_dict()
    overheat_counts = (
        portfolio_event_df[portfolio_event_df["is_overheat"].fillna(False).astype(bool)]
        .groupby(list(_PORTFOLIO_GROUP_COLUMNS), observed=True, sort=False)
        .size()
        .to_dict()
    )
    rule_labels = {
        tuple(row[column] for column in _PORTFOLIO_GROUP_COLUMNS): str(row.get("rule_label", ""))
        for row in portfolio_event_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(list(_PORTFOLIO_GROUP_COLUMNS), observed=True, sort=False):
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
        records.append(
            {
                **dict(zip(_PORTFOLIO_GROUP_COLUMNS, keys, strict=True)),
                "rule_label": rule_labels.get(tuple(keys), ""),
                "event_count": int(event_counts.get(tuple(keys), 0)),
                "overheat_event_count": int(overheat_counts.get(tuple(keys), 0)),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group)),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "avg_active_weight": _series_mean(group["active_weight"]),
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
    return pd.DataFrame(records)[columns]


def _daily_stats(daily_returns: pd.Series) -> dict[str, float | None]:
    returns = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if returns.empty:
        return {
            "annualized_volatility_pct": None,
            "sharpe_ratio": None,
            "sortino_ratio": None,
        }
    std = float(returns.std(ddof=1))
    mean = float(returns.mean())
    downside = returns[returns < 0.0]
    downside_std = float(downside.std(ddof=1)) if len(downside) >= 2 else math.nan
    return {
        "annualized_volatility_pct": std * math.sqrt(252.0) * 100.0 if math.isfinite(std) else None,
        "sharpe_ratio": mean / std * math.sqrt(252.0)
        if math.isfinite(std) and std > 0.0
        else None,
        "sortino_ratio": mean / downside_std * math.sqrt(252.0)
        if math.isfinite(downside_std) and downside_std > 0.0
        else None,
    }


def _return_series(frame: pd.DataFrame) -> pd.Series:
    column = "event_return_winsor_pct" if "event_return_winsor_pct" in frame.columns else "event_return_pct"
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _mean_or_none(series: pd.Series) -> float | None:
    return float(series.mean()) if not series.empty else None


def _min_or_none(series: pd.Series) -> float | None:
    return float(series.min()) if not series.empty else None


def _win_rate_or_none(series: pd.Series) -> float | None:
    return float((series > 0.0).mean() * 100.0) if not series.empty else None


def _series_mean(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def _empty_df(columns: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _coerce_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(ts):
        return None
    return cast(pd.Timestamp, ts)


def _float_or_nan(value: Any) -> float:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return float("nan")
    return number if math.isfinite(number) else float("nan")


def _min_date(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    return values.min().strftime("%Y-%m-%d") if not values.empty else None


def _max_date(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    return values.max().strftime("%Y-%m-%d") if not values.empty else None


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


def _build_summary_markdown(result: ValueBreakoutOverheatFilterResult) -> str:
    lines = [
        "# Value Breakout Overheat Filter",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Target: `{result.market_scope}` / `{result.score_method}` / `{result.liquidity_scenario}` / "
        f"`{result.breakout_policy}` / `{result.breakout_window}d` / `{result.rebalance_months}m` / "
        f"top `{result.selection_count}`",
        f"- Selected event rows: `{result.selected_event_count}`",
        f"- Rows with technical features: `{result.technical_feature_count}`",
        f"- Technical policy: {result.technical_policy}.",
        "",
        "## Portfolio Rows",
        "",
    ]
    summary = result.portfolio_summary_df.copy()
    if summary.empty:
        lines.append("- No portfolio summary rows were produced.")
        return "\n".join(lines)
    focus = summary.sort_values(
        ["rule_name", "variant_name"],
        ascending=[True, True],
        kind="stable",
    )
    for row in focus.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row['rule_name']}` / `{row['variant_name']}`: "
            f"CAGR `{_fmt(row['cagr_pct'])}%`, "
            f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
            f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
            f"events `{int(cast(int, row['event_count']))}`, "
            f"overheat `{int(cast(int, row['overheat_event_count']))}`"
        )
    return "\n".join(lines)


def write_value_breakout_overheat_filter_bundle(
    result: ValueBreakoutOverheatFilterResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID,
        module=__name__,
        function="run_value_breakout_overheat_filter",
        params={
            "input_bundle_path": result.input_bundle_path,
            "db_path": result.db_path,
            "market_scope": result.market_scope,
            "score_method": result.score_method,
            "liquidity_scenario": result.liquidity_scenario,
            "breakout_policy": result.breakout_policy,
            "breakout_window": result.breakout_window,
            "breakout_lookback_sessions": result.breakout_lookback_sessions,
            "rebalance_months": result.rebalance_months,
            "selection_count": result.selection_count,
            "holdout_months": result.holdout_months,
            "threshold_quantile": result.threshold_quantile,
            "size_haircut": result.size_haircut,
            "risk_ratio_type": result.risk_ratio_type,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_value_breakout_overheat_filter_bundle(
    bundle_path: str | Path,
) -> ValueBreakoutOverheatFilterResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=ValueBreakoutOverheatFilterResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_value_breakout_overheat_filter_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_value_breakout_overheat_filter_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def get_annual_value_breakout_periodic_rebalance_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_VALUE_BREAKOUT_PERIODIC_REBALANCE_EXPERIMENT_ID,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "OVERHEAT_RULES",
    "VALUE_BREAKOUT_OVERHEAT_FILTER_EXPERIMENT_ID",
    "ValueBreakoutOverheatFilterResult",
    "get_value_breakout_overheat_filter_bundle_path_for_run_id",
    "get_value_breakout_overheat_filter_latest_bundle_path",
    "load_value_breakout_overheat_filter_bundle",
    "run_value_breakout_overheat_filter",
    "write_value_breakout_overheat_filter_bundle",
)
