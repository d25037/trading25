"""Technical feature importance research for annual value-composite selections."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_value_composite_selection import (
    get_annual_value_composite_selection_latest_bundle_path,
    load_annual_value_composite_selection_bundle,
    _annual_selection_stats,
    _empty_df,
    _series_mean,
    _series_median,
)
from src.domains.analytics.annual_value_composite_technical_filter import (
    _load_technical_price_frames,
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
from src.domains.strategy.indicators import compute_risk_adjusted_return, compute_rsi

ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID = (
    "market-behavior/annual-value-technical-feature-importance"
)
DEFAULT_BUCKET_COUNT = 5
DEFAULT_WARMUP_SMA_WINDOW = 252
DEFAULT_FOCUS_MARKET_SCOPE = "standard"
DEFAULT_FOCUS_SELECTION_FRACTION = 0.10
DEFAULT_FOCUS_LIQUIDITY_SCENARIO = "none"
DEFAULT_FOCUS_SCORE_METHODS: tuple[str, ...] = (
    "equal_weight",
    "walkforward_regression_weight",
)

_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_event_df",
    "feature_bucket_summary_df",
    "feature_importance_df",
    "conditional_importance_df",
    "walkforward_overlay_df",
)


@dataclass(frozen=True)
class TechnicalFeatureSpec:
    key: str
    family: str
    label: str
    preferred_high: bool | None


@dataclass(frozen=True)
class AnnualValueTechnicalFeatureImportanceResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    bucket_count: int
    focus_market_scope: str
    focus_selection_fraction: float
    focus_liquidity_scenario: str
    focus_score_methods: tuple[str, ...]
    selected_event_count: int
    technical_feature_count: int
    feature_policy: str
    enriched_event_df: pd.DataFrame
    feature_bucket_summary_df: pd.DataFrame
    feature_importance_df: pd.DataFrame
    conditional_importance_df: pd.DataFrame
    walkforward_overlay_df: pd.DataFrame


TECHNICAL_FEATURES: tuple[TechnicalFeatureSpec, ...] = (
    TechnicalFeatureSpec("price_to_sma50", "trend", "Price / SMA50", None),
    TechnicalFeatureSpec("price_to_sma100", "trend", "Price / SMA100", None),
    TechnicalFeatureSpec("price_to_sma250", "trend", "Price / SMA250", None),
    TechnicalFeatureSpec("sma50_slope_20d_pct", "trend", "SMA50 slope 20d", True),
    TechnicalFeatureSpec("sma100_slope_20d_pct", "trend", "SMA100 slope 20d", True),
    TechnicalFeatureSpec("sma250_slope_20d_pct", "trend", "SMA250 slope 20d", True),
    TechnicalFeatureSpec("return_20d_pct", "momentum", "Return 20d", None),
    TechnicalFeatureSpec("return_60d_pct", "momentum", "Return 60d", None),
    TechnicalFeatureSpec("return_120d_pct", "momentum", "Return 120d", None),
    TechnicalFeatureSpec("return_252d_pct", "momentum", "Return 252d", None),
    TechnicalFeatureSpec("drawdown_from_252d_high_pct", "reversal", "Drawdown from 252d high", False),
    TechnicalFeatureSpec("rebound_from_252d_low_pct", "reversal", "Rebound from 252d low", None),
    TechnicalFeatureSpec("volatility_20d_pct", "volatility", "Volatility 20d", False),
    TechnicalFeatureSpec("volatility_60d_pct", "volatility", "Volatility 60d", False),
    TechnicalFeatureSpec("downside_volatility_60d_pct", "volatility", "Downside volatility 60d", False),
    TechnicalFeatureSpec("risk_adjusted_return_20d", "risk_adjusted", "Risk-adjusted return 20d", None),
    TechnicalFeatureSpec("risk_adjusted_return_60d", "risk_adjusted", "Risk-adjusted return 60d", None),
    TechnicalFeatureSpec("risk_adjusted_return_120d", "risk_adjusted", "Risk-adjusted return 120d", None),
    TechnicalFeatureSpec("rsi_14", "oscillator", "RSI 14", None),
    TechnicalFeatureSpec("volume_ratio_20_60", "volume", "Volume SMA20/SMA60", None),
    TechnicalFeatureSpec("trading_value_ratio_20_60", "volume", "Trading value SMA20/SMA60", None),
    TechnicalFeatureSpec("topix_price_to_sma250", "market_regime", "TOPIX Price / SMA250", None),
    TechnicalFeatureSpec("topix_return_252d_pct", "market_regime", "TOPIX return 252d", None),
    TechnicalFeatureSpec("topix_drawdown_from_252d_high_pct", "market_regime", "TOPIX drawdown from 252d high", False),
    TechnicalFeatureSpec("topix_volatility_60d_pct", "market_regime", "TOPIX volatility 60d", False),
)
_FEATURE_BY_KEY = {feature.key: feature for feature in TECHNICAL_FEATURES}


def run_annual_value_technical_feature_importance(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    focus_market_scope: str = DEFAULT_FOCUS_MARKET_SCOPE,
    focus_selection_fraction: float = DEFAULT_FOCUS_SELECTION_FRACTION,
    focus_liquidity_scenario: str = DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    focus_score_methods: Sequence[str] = DEFAULT_FOCUS_SCORE_METHODS,
) -> AnnualValueTechnicalFeatureImportanceResult:
    if bucket_count < 2:
        raise ValueError("bucket_count must be >= 2")
    normalized_score_methods = _normalize_score_methods(focus_score_methods)
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
    selected_event_df = _focus_selected_events(
        value_result.selected_event_df,
        market_scope=focus_market_scope,
        selection_fraction=focus_selection_fraction,
        liquidity_scenario=focus_liquidity_scenario,
        score_methods=normalized_score_methods,
    )
    source_mode, source_detail, stock_price_df, topix_price_df = _load_technical_price_frames(
        value_result.db_path,
        selected_event_df,
        sma_window=DEFAULT_WARMUP_SMA_WINDOW,
    )
    enriched_event_df = _build_enriched_event_df(
        selected_event_df,
        stock_price_df=stock_price_df,
        topix_price_df=topix_price_df,
    )
    feature_bucket_summary_df = _build_feature_bucket_summary_df(
        enriched_event_df,
        bucket_count=bucket_count,
    )
    conditional_importance_df = _build_conditional_importance_df(enriched_event_df)
    feature_importance_df = _build_feature_importance_df(
        feature_bucket_summary_df,
        conditional_importance_df,
    )
    walkforward_overlay_df = _build_walkforward_overlay_df(
        enriched_event_df,
        bucket_count=bucket_count,
    )
    feature_count = int(enriched_event_df["price_to_sma250"].notna().sum()) if not enriched_event_df.empty else 0
    return AnnualValueTechnicalFeatureImportanceResult(
        db_path=value_result.db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=value_result.analysis_start_date,
        analysis_end_date=value_result.analysis_end_date,
        bucket_count=bucket_count,
        focus_market_scope=focus_market_scope,
        focus_selection_fraction=float(focus_selection_fraction),
        focus_liquidity_scenario=focus_liquidity_scenario,
        focus_score_methods=normalized_score_methods,
        selected_event_count=int(len(selected_event_df)),
        technical_feature_count=feature_count,
        feature_policy=(
            "technical features use the latest trading session strictly before entry_date; "
            "2017 DB-left-boundary and short-history rows are kept but classified separately; "
            "fixed_55_25_20 is intentionally excluded from the focus score methods"
        ),
        enriched_event_df=enriched_event_df,
        feature_bucket_summary_df=feature_bucket_summary_df,
        feature_importance_df=feature_importance_df,
        conditional_importance_df=conditional_importance_df,
        walkforward_overlay_df=walkforward_overlay_df,
    )


def _normalize_score_methods(values: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_value in values:
        value = str(raw_value).strip()
        if not value:
            raise ValueError("focus score methods must not be empty")
        if value == "fixed_55_25_20":
            continue
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("at least one non-fixed focus score method is required")
    return tuple(normalized)


def _focus_selected_events(
    selected_event_df: pd.DataFrame,
    *,
    market_scope: str,
    selection_fraction: float,
    liquidity_scenario: str,
    score_methods: Sequence[str],
) -> pd.DataFrame:
    if selected_event_df.empty:
        return selected_event_df.copy()
    fraction = pd.to_numeric(selected_event_df["selection_fraction"], errors="coerce").round(6)
    return selected_event_df[
        (selected_event_df["market_scope"].astype(str) == str(market_scope))
        & (selected_event_df["liquidity_scenario"].astype(str) == str(liquidity_scenario))
        & (selected_event_df["score_method"].astype(str).isin(tuple(score_methods)))
        & (fraction == round(float(selection_fraction), 6))
    ].copy()


def _build_enriched_event_df(
    selected_event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
    topix_price_df: pd.DataFrame,
) -> pd.DataFrame:
    if selected_event_df.empty:
        return _ensure_feature_columns(selected_event_df.copy())
    stock_feature_frames = {
        str(code): _build_symbol_feature_frame(frame)
        for code, frame in stock_price_df.groupby("code", sort=False)
    }
    topix_feature_frame = _build_symbol_feature_frame(topix_price_df)
    rows: list[dict[str, Any]] = []
    first_price_dates = {
        str(code): _first_date(frame) for code, frame in stock_price_df.groupby("code", sort=False)
    }
    prior_row_counts = _prior_row_counts(selected_event_df, stock_price_df)
    for event in selected_event_df.to_dict(orient="records"):
        payload: dict[str, Any] = {str(key): value for key, value in event.items()}
        code = str(payload.get("code", ""))
        entry_date = _coerce_timestamp(payload.get("entry_date"))
        stock_features = _lookup_previous_feature_row(
            stock_feature_frames.get(code),
            entry_date=entry_date,
            prefix="",
        )
        topix_features = _lookup_previous_feature_row(
            topix_feature_frame,
            entry_date=entry_date,
            prefix="topix_",
        )
        prior_rows = int(prior_row_counts.get((str(payload.get("event_id")), code), 0))
        first_price_date = first_price_dates.get(code)
        payload.update(stock_features)
        payload.update(topix_features)
        payload["first_price_date"] = first_price_date
        payload["prior_close_rows"] = prior_rows
        payload["history_class"] = _classify_history(
            year=str(payload.get("year")),
            first_price_date=first_price_date,
            prior_close_rows=prior_rows,
        )
        rows.append(payload)
    result = pd.DataFrame(rows)
    return _ensure_feature_columns(result)


def _prior_row_counts(
    selected_event_df: pd.DataFrame,
    stock_price_df: pd.DataFrame,
) -> dict[tuple[str, str], int]:
    price_by_code = {
        str(code): pd.to_datetime(frame["date"], errors="coerce").dropna().sort_values()
        for code, frame in stock_price_df.groupby("code", sort=False)
    }
    counts: dict[tuple[str, str], int] = {}
    for event in selected_event_df.to_dict(orient="records"):
        code = str(event.get("code", ""))
        entry_date = _coerce_timestamp(event.get("entry_date"))
        dates = price_by_code.get(code)
        count = 0 if dates is None or entry_date is None else int((dates < entry_date).sum())
        counts[(str(event.get("event_id")), code)] = count
    return counts


def _build_symbol_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "close",
        *[feature.key for feature in TECHNICAL_FEATURES if not feature.key.startswith("topix_")],
    ]
    if frame.empty:
        return _empty_df(columns)
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    for column in ("high", "low", "volume"):
        if column not in result.columns:
            result[column] = np.nan
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(subset=["date"]).sort_values("date", kind="stable")
    result = result.drop_duplicates("date", keep="last").reset_index(drop=True)
    close = result["close"].astype(float)
    volume = result["volume"].astype(float)
    returns = close.pct_change()
    for window in (50, 100, 250):
        sma = close.rolling(window, min_periods=window).mean()
        result[f"price_to_sma{window}"] = close / sma
        result[f"sma{window}_slope_20d_pct"] = (sma / sma.shift(20) - 1.0) * 100.0
    for window in (20, 60, 120, 252):
        result[f"return_{window}d_pct"] = (close / close.shift(window) - 1.0) * 100.0
    high_252 = close.rolling(252, min_periods=252).max()
    low_252 = close.rolling(252, min_periods=252).min()
    result["drawdown_from_252d_high_pct"] = (close / high_252 - 1.0) * 100.0
    result["rebound_from_252d_low_pct"] = (close / low_252 - 1.0) * 100.0
    result["volatility_20d_pct"] = returns.rolling(20, min_periods=20).std() * math.sqrt(252.0) * 100.0
    result["volatility_60d_pct"] = returns.rolling(60, min_periods=60).std() * math.sqrt(252.0) * 100.0
    downside = returns.where(returns < 0)
    result["downside_volatility_60d_pct"] = downside.rolling(60, min_periods=2).std() * math.sqrt(252.0) * 100.0
    for window in (20, 60, 120):
        result[f"risk_adjusted_return_{window}d"] = compute_risk_adjusted_return(
            close,
            lookback_period=window,
            ratio_type="sortino",
        )
    result["rsi_14"] = compute_rsi(close, 14)
    volume_sma20 = volume.rolling(20, min_periods=20).mean()
    volume_sma60 = volume.rolling(60, min_periods=60).mean()
    result["volume_ratio_20_60"] = volume_sma20 / volume_sma60.replace(0, np.nan)
    trading_value = close * volume
    tv_sma20 = trading_value.rolling(20, min_periods=20).mean()
    tv_sma60 = trading_value.rolling(60, min_periods=60).mean()
    result["trading_value_ratio_20_60"] = tv_sma20 / tv_sma60.replace(0, np.nan)
    return result[columns]


def _lookup_previous_feature_row(
    feature_df: pd.DataFrame | None,
    *,
    entry_date: pd.Timestamp | None,
    prefix: str,
) -> dict[str, Any]:
    keys = _feature_keys_for_prefix(prefix)
    if feature_df is None or feature_df.empty or entry_date is None:
        return {f"{prefix}{key}": np.nan for key in keys} | {
            f"{prefix}feature_date": None,
            f"{prefix}feature_lag_days": np.nan,
        }
    dates = pd.DatetimeIndex(pd.to_datetime(feature_df["date"], errors="coerce"))
    position = dates.searchsorted(entry_date, side="left") - 1
    if position < 0:
        return {f"{prefix}{key}": np.nan for key in keys} | {
            f"{prefix}feature_date": None,
            f"{prefix}feature_lag_days": np.nan,
        }
    row = feature_df.iloc[int(position)]
    feature_date = cast(pd.Timestamp, row["date"])
    payload = {
        f"{prefix}feature_date": feature_date.strftime("%Y-%m-%d"),
        f"{prefix}feature_lag_days": float((entry_date - feature_date).days),
    }
    for key in keys:
        payload[f"{prefix}{key}"] = _float_or_nan(row.get(key))
    return payload


def _feature_keys_for_prefix(prefix: str) -> tuple[str, ...]:
    if prefix == "topix_":
        return (
            "price_to_sma250",
            "return_252d_pct",
            "drawdown_from_252d_high_pct",
            "volatility_60d_pct",
        )
    return tuple(feature.key for feature in TECHNICAL_FEATURES if not feature.key.startswith("topix_"))


def _ensure_feature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for feature in TECHNICAL_FEATURES:
        if feature.key not in result.columns:
            result[feature.key] = np.nan
    for column in (
        "feature_date",
        "feature_lag_days",
        "topix_feature_date",
        "topix_feature_lag_days",
        "first_price_date",
        "prior_close_rows",
        "history_class",
    ):
        if column not in result.columns:
            result[column] = np.nan
    return result


def _build_feature_bucket_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "score_method",
        "feature_key",
        "feature_family",
        "feature_label",
        "bucket_rank",
        "bucket_count",
        "event_count",
        "year_count",
        "coverage_pct",
        "feature_min",
        "feature_median",
        "feature_max",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "p10_return_pct",
        "worst_return_pct",
        "annual_mean_return_pct",
        "positive_year_rate_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for score_method, score_df in enriched_event_df.groupby("score_method", sort=False):
        total_count = len(score_df)
        for spec in TECHNICAL_FEATURES:
            valid = score_df[pd.to_numeric(score_df[spec.key], errors="coerce").notna()].copy()
            if valid.empty:
                continue
            valid["bucket_rank"] = _yearly_bucket_rank(valid, spec.key, bucket_count=bucket_count)
            for bucket_rank, bucket_df in valid.groupby("bucket_rank", sort=True):
                returns = pd.to_numeric(bucket_df["event_return_winsor_pct"], errors="coerce").dropna()
                values = pd.to_numeric(bucket_df[spec.key], errors="coerce").dropna()
                annual_stats = _annual_selection_stats(bucket_df)
                records.append(
                    {
                        "score_method": str(score_method),
                        "feature_key": spec.key,
                        "feature_family": spec.family,
                        "feature_label": spec.label,
                        "bucket_rank": int(cast(int, bucket_rank)),
                        "bucket_count": int(bucket_count),
                        "event_count": int(len(bucket_df)),
                        "year_count": int(bucket_df["year"].nunique()),
                        "coverage_pct": float(len(valid) / total_count * 100.0) if total_count else None,
                        "feature_min": float(values.min()) if not values.empty else None,
                        "feature_median": float(values.median()) if not values.empty else None,
                        "feature_max": float(values.max()) if not values.empty else None,
                        "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                        "median_return_pct": float(returns.median()) if not returns.empty else None,
                        "win_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else None,
                        "p10_return_pct": float(returns.quantile(0.10)) if not returns.empty else None,
                        "worst_return_pct": float(returns.min()) if not returns.empty else None,
                        "annual_mean_return_pct": annual_stats["annual_mean_return_pct"],
                        "positive_year_rate_pct": annual_stats["positive_year_rate_pct"],
                    }
                )
    return pd.DataFrame(records, columns=columns)


def _build_conditional_importance_df(enriched_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "score_method",
        "feature_key",
        "feature_family",
        "feature_label",
        "year_count",
        "mean_year_corr",
        "median_year_corr",
        "mean_abs_year_corr",
        "positive_corr_year_rate_pct",
        "mean_residual_corr",
        "median_residual_corr",
        "mean_abs_residual_corr",
        "positive_residual_corr_year_rate_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    residual_df = _add_value_score_residuals(enriched_event_df)
    records: list[dict[str, Any]] = []
    for score_method, score_df in residual_df.groupby("score_method", sort=False):
        for spec in TECHNICAL_FEATURES:
            corr_rows: list[dict[str, float]] = []
            for _, year_df in score_df.groupby("year", sort=True):
                values = pd.to_numeric(year_df[spec.key], errors="coerce")
                returns = pd.to_numeric(year_df["event_return_winsor_pct"], errors="coerce")
                residuals = pd.to_numeric(year_df["value_score_residual_return_pct"], errors="coerce")
                corr = _safe_corr(values, returns)
                residual_corr = _safe_corr(values, residuals)
                if corr is not None or residual_corr is not None:
                    corr_rows.append(
                        {
                            "corr": float(corr) if corr is not None else np.nan,
                            "residual_corr": float(residual_corr) if residual_corr is not None else np.nan,
                        }
                    )
            if not corr_rows:
                continue
            corr_df = pd.DataFrame(corr_rows)
            corr_values = pd.to_numeric(corr_df["corr"], errors="coerce").dropna()
            residual_values = pd.to_numeric(corr_df["residual_corr"], errors="coerce").dropna()
            records.append(
                {
                    "score_method": str(score_method),
                    "feature_key": spec.key,
                    "feature_family": spec.family,
                    "feature_label": spec.label,
                    "year_count": int(len(corr_rows)),
                    "mean_year_corr": _series_mean(corr_values),
                    "median_year_corr": _series_median(corr_values),
                    "mean_abs_year_corr": _series_mean(corr_values.abs()),
                    "positive_corr_year_rate_pct": _positive_rate(corr_values),
                    "mean_residual_corr": _series_mean(residual_values),
                    "median_residual_corr": _series_median(residual_values),
                    "mean_abs_residual_corr": _series_mean(residual_values.abs()),
                    "positive_residual_corr_year_rate_pct": _positive_rate(residual_values),
                }
            )
    return pd.DataFrame(records, columns=columns)


def _build_feature_importance_df(
    feature_bucket_summary_df: pd.DataFrame,
    conditional_importance_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "score_method",
        "feature_key",
        "feature_family",
        "feature_label",
        "coverage_pct",
        "low_bucket_mean_return_pct",
        "high_bucket_mean_return_pct",
        "high_minus_low_mean_return_pct",
        "low_bucket_p10_return_pct",
        "high_bucket_p10_return_pct",
        "high_minus_low_p10_return_pct",
        "best_bucket_rank",
        "best_bucket_mean_return_pct",
        "worst_bucket_rank",
        "worst_bucket_mean_return_pct",
        "best_minus_worst_mean_return_pct",
        "mean_abs_year_corr",
        "mean_abs_residual_corr",
        "importance_score",
        "direction_hint",
    ]
    if feature_bucket_summary_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in feature_bucket_summary_df.groupby(["score_method", "feature_key"], sort=False):
        score_method, feature_key = keys
        spec = _FEATURE_BY_KEY[str(feature_key)]
        low = group[group["bucket_rank"].astype(int) == 1]
        high = group[group["bucket_rank"].astype(int) == group["bucket_count"].astype(int).max()]
        best = group.sort_values("mean_return_pct", ascending=False, na_position="last").head(1)
        worst = group.sort_values("mean_return_pct", ascending=True, na_position="last").head(1)
        conditional = conditional_importance_df[
            (conditional_importance_df["score_method"].astype(str) == str(score_method))
            & (conditional_importance_df["feature_key"].astype(str) == str(feature_key))
        ]
        high_minus_low = _row_value(high, "mean_return_pct") - _row_value(low, "mean_return_pct")
        high_minus_low_p10 = _row_value(high, "p10_return_pct") - _row_value(low, "p10_return_pct")
        best_minus_worst = _row_value(best, "mean_return_pct") - _row_value(worst, "mean_return_pct")
        mean_abs_corr = _row_value(conditional, "mean_abs_year_corr")
        mean_abs_resid = _row_value(conditional, "mean_abs_residual_corr")
        residual_component = mean_abs_resid if math.isfinite(mean_abs_resid) else 0.0
        importance_score = (
            abs(high_minus_low) * 0.25
            + abs(high_minus_low_p10) * 0.25
            + abs(best_minus_worst) * 0.35
            + residual_component * 100.0 * 0.15
        )
        records.append(
            {
                "score_method": str(score_method),
                "feature_key": spec.key,
                "feature_family": spec.family,
                "feature_label": spec.label,
                "coverage_pct": _row_value(group.head(1), "coverage_pct"),
                "low_bucket_mean_return_pct": _row_value(low, "mean_return_pct"),
                "high_bucket_mean_return_pct": _row_value(high, "mean_return_pct"),
                "high_minus_low_mean_return_pct": high_minus_low,
                "low_bucket_p10_return_pct": _row_value(low, "p10_return_pct"),
                "high_bucket_p10_return_pct": _row_value(high, "p10_return_pct"),
                "high_minus_low_p10_return_pct": high_minus_low_p10,
                "best_bucket_rank": _row_value(best, "bucket_rank"),
                "best_bucket_mean_return_pct": _row_value(best, "mean_return_pct"),
                "worst_bucket_rank": _row_value(worst, "bucket_rank"),
                "worst_bucket_mean_return_pct": _row_value(worst, "mean_return_pct"),
                "best_minus_worst_mean_return_pct": best_minus_worst,
                "mean_abs_year_corr": mean_abs_corr,
                "mean_abs_residual_corr": mean_abs_resid,
                "importance_score": importance_score,
                "direction_hint": _direction_hint(high_minus_low, spec),
            }
        )
    result = pd.DataFrame(records, columns=columns)
    return result.sort_values(["score_method", "importance_score"], ascending=[True, False]).reset_index(drop=True)


def _build_walkforward_overlay_df(
    enriched_event_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "score_method",
        "feature_key",
        "feature_family",
        "feature_label",
        "selected_bucket_side",
        "target_year_count",
        "baseline_event_count",
        "selected_event_count",
        "kept_event_pct",
        "baseline_mean_return_pct",
        "selected_mean_return_pct",
        "delta_mean_return_pct",
        "baseline_p10_return_pct",
        "selected_p10_return_pct",
        "delta_p10_return_pct",
        "baseline_worst_return_pct",
        "selected_worst_return_pct",
        "delta_worst_return_pct",
        "selected_positive_year_rate_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for score_method, score_df in enriched_event_df.groupby("score_method", sort=False):
        years = sorted(str(year) for year in score_df["year"].dropna().unique())
        for spec in TECHNICAL_FEATURES:
            selected_frames: list[pd.DataFrame] = []
            target_years: list[str] = []
            sides: list[str] = []
            for target_year in years:
                train = score_df[score_df["year"].astype(str) < target_year].copy()
                target = score_df[score_df["year"].astype(str) == target_year].copy()
                train_valid = train[pd.to_numeric(train[spec.key], errors="coerce").notna()].copy()
                target_valid = target[pd.to_numeric(target[spec.key], errors="coerce").notna()].copy()
                if len(train_valid) < bucket_count * 2 or len(target_valid) < bucket_count:
                    continue
                train_valid["bucket_rank"] = _yearly_bucket_rank(
                    train_valid,
                    spec.key,
                    bucket_count=bucket_count,
                )
                bucket_returns = train_valid.groupby("bucket_rank")["event_return_winsor_pct"].mean()
                if bucket_returns.empty:
                    continue
                best_bucket = int(cast(int, bucket_returns.sort_values(ascending=False).index[0]))
                side = "high" if best_bucket > math.ceil(bucket_count / 2) else "low"
                target_valid["bucket_rank"] = _yearly_bucket_rank(
                    target_valid,
                    spec.key,
                    bucket_count=bucket_count,
                )
                selected = target_valid[target_valid["bucket_rank"].astype(int) == best_bucket].copy()
                if selected.empty:
                    continue
                selected_frames.append(selected)
                target_years.append(target_year)
                sides.append(side)
            if not selected_frames:
                continue
            selected_df = pd.concat(selected_frames, ignore_index=True)
            baseline_df = score_df[score_df["year"].astype(str).isin(target_years)].copy()
            baseline_returns = pd.to_numeric(baseline_df["event_return_winsor_pct"], errors="coerce").dropna()
            selected_returns = pd.to_numeric(selected_df["event_return_winsor_pct"], errors="coerce").dropna()
            selected_stats = _annual_selection_stats(selected_df)
            baseline_mean = _series_mean(baseline_returns)
            selected_mean = _series_mean(selected_returns)
            baseline_p10 = _quantile(baseline_returns, 0.10)
            selected_p10 = _quantile(selected_returns, 0.10)
            records.append(
                {
                    "score_method": str(score_method),
                    "feature_key": spec.key,
                    "feature_family": spec.family,
                    "feature_label": spec.label,
                    "selected_bucket_side": _most_common(sides),
                    "target_year_count": int(len(set(target_years))),
                    "baseline_event_count": int(len(baseline_df)),
                    "selected_event_count": int(len(selected_df)),
                    "kept_event_pct": float(len(selected_df) / len(baseline_df) * 100.0) if len(baseline_df) else None,
                    "baseline_mean_return_pct": baseline_mean,
                    "selected_mean_return_pct": selected_mean,
                    "delta_mean_return_pct": _subtract_or_nan(selected_mean, baseline_mean),
                    "baseline_p10_return_pct": baseline_p10,
                    "selected_p10_return_pct": selected_p10,
                    "delta_p10_return_pct": _subtract_or_nan(selected_p10, baseline_p10),
                    "baseline_worst_return_pct": float(baseline_returns.min()) if not baseline_returns.empty else None,
                    "selected_worst_return_pct": float(selected_returns.min()) if not selected_returns.empty else None,
                    "delta_worst_return_pct": (
                        float(selected_returns.min() - baseline_returns.min())
                        if not selected_returns.empty and not baseline_returns.empty
                        else None
                    ),
                    "selected_positive_year_rate_pct": selected_stats["positive_year_rate_pct"],
                }
            )
    result = pd.DataFrame(records, columns=columns)
    return result.sort_values(["score_method", "delta_mean_return_pct"], ascending=[True, False]).reset_index(drop=True)


def _subtract_or_nan(left: float | None, right: float | None) -> float:
    if left is None or right is None:
        return float("nan")
    if not math.isfinite(left) or not math.isfinite(right):
        return float("nan")
    return float(left - right)


def _yearly_bucket_rank(frame: pd.DataFrame, column: str, *, bucket_count: int) -> pd.Series:
    ranks = pd.Series(np.nan, index=frame.index, dtype="float64")
    values = pd.to_numeric(frame[column], errors="coerce")
    for _, group in frame.groupby("year", sort=False):
        valid = values.loc[group.index].dropna().sort_values(kind="stable")
        count = len(valid)
        if count == 0:
            continue
        resolved_bucket_count = min(bucket_count, count)
        bucket = (np.floor(np.arange(count, dtype=float) * resolved_bucket_count / count) + 1).astype(int)
        ranks.loc[valid.index] = bucket
    return ranks


def _add_value_score_residuals(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["value_score_residual_return_pct"] = np.nan
    for _, group in result.groupby(["score_method", "year"], sort=False):
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce")
        score = pd.to_numeric(group["composite_score"], errors="coerce")
        valid = returns.notna() & score.notna()
        if int(valid.sum()) < 3:
            result.loc[group.index, "value_score_residual_return_pct"] = returns - returns.mean()
            continue
        x = score[valid].to_numpy(dtype=float)
        y = returns[valid].to_numpy(dtype=float)
        slope, intercept = np.polyfit(x, y, deg=1)
        fitted = pd.Series(np.nan, index=group.index, dtype="float64")
        fitted.loc[score[valid].index] = intercept + slope * score[valid]
        result.loc[group.index, "value_score_residual_return_pct"] = returns - fitted
    return result


def _classify_history(
    *,
    year: str,
    first_price_date: str | None,
    prior_close_rows: int,
) -> str:
    if prior_close_rows >= 250:
        return "has_250_prior_closes"
    if year == "2017" and first_price_date is not None and first_price_date <= "2016-05-02":
        return "db_left_boundary_2017"
    return "short_history_lt250_prior_closes"


def _first_date(frame: pd.DataFrame) -> str | None:
    dates = frame["date"].astype(str).sort_values()
    return str(dates.iloc[0]) if not dates.empty else None


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


def _safe_corr(values: pd.Series, returns: pd.Series) -> float | None:
    clean = pd.DataFrame({"value": values, "return": returns}).dropna()
    if len(clean) < 3:
        return None
    if clean["value"].nunique() < 2 or clean["return"].nunique() < 2:
        return None
    corr = clean["value"].corr(clean["return"])
    return float(corr) if pd.notna(corr) and math.isfinite(float(corr)) else None


def _positive_rate(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float((clean > 0.0).mean() * 100.0) if not clean.empty else None


def _row_value(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return float("nan")
    value = frame.iloc[0][column]
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return float("nan")
    return number if math.isfinite(number) else float("nan")


def _direction_hint(high_minus_low: float, spec: TechnicalFeatureSpec) -> str:
    if not math.isfinite(high_minus_low) or abs(high_minus_low) < 1e-9:
        return "flat"
    direction = "high_better" if high_minus_low > 0 else "low_better"
    if spec.preferred_high is True:
        return f"{direction}_expected_high"
    if spec.preferred_high is False:
        return f"{direction}_expected_low"
    return direction


def _quantile(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.quantile(q)) if not clean.empty else float("nan")


def _most_common(values: Sequence[str]) -> str | None:
    if not values:
        return None
    counts = pd.Series(list(values)).value_counts()
    return str(counts.index[0])


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


def _build_summary_markdown(result: AnnualValueTechnicalFeatureImportanceResult) -> str:
    lines = [
        "# Annual Value Technical Feature Importance",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Focus: `{result.focus_market_scope}` / `{result.focus_liquidity_scenario}` / top `{result.focus_selection_fraction * 100:.0f}%`",
        f"- Score methods: `{', '.join(result.focus_score_methods)}`",
        f"- Selected event rows: `{result.selected_event_count}`",
        f"- Rows with SMA250 features: `{result.technical_feature_count}`",
        f"- Feature policy: {result.feature_policy}.",
        "",
        "## Top Feature Importance Rows",
        "",
    ]
    if result.feature_importance_df.empty:
        lines.append("- No feature importance rows were produced.")
    else:
        focus = result.feature_importance_df.head(16)
        for row in focus.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['score_method']}` / `{row['feature_key']}`: "
                f"importance `{_fmt(row['importance_score'])}`, "
                f"best-worst mean `{_fmt(row['best_minus_worst_mean_return_pct'])}pp`, "
                f"high-low mean `{_fmt(row['high_minus_low_mean_return_pct'])}pp`, "
                f"residual |corr| `{_fmt(row['mean_abs_residual_corr'], 3)}`"
            )
    lines.extend(["", "## Walk-forward Overlay Rows", ""])
    if result.walkforward_overlay_df.empty:
        lines.append("- No walk-forward overlay rows were produced.")
    else:
        for row in result.walkforward_overlay_df.head(12).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['score_method']}` / `{row['feature_key']}`: "
                f"delta mean `{_fmt(row['delta_mean_return_pct'])}pp`, "
                f"delta p10 `{_fmt(row['delta_p10_return_pct'])}pp`, "
                f"kept `{_fmt(row['kept_event_pct'])}%`, "
                f"years `{int(cast(int, row['target_year_count']))}`"
            )
    return "\n".join(lines)


def write_annual_value_technical_feature_importance_bundle(
    result: AnnualValueTechnicalFeatureImportanceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_value_technical_feature_importance",
        params={
            "input_bundle_path": result.input_bundle_path,
            "bucket_count": result.bucket_count,
            "focus_market_scope": result.focus_market_scope,
            "focus_selection_fraction": result.focus_selection_fraction,
            "focus_liquidity_scenario": result.focus_liquidity_scenario,
            "focus_score_methods": list(result.focus_score_methods),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_value_technical_feature_importance_bundle(
    bundle_path: str | Path,
) -> AnnualValueTechnicalFeatureImportanceResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualValueTechnicalFeatureImportanceResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_value_technical_feature_importance_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_value_technical_feature_importance_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "ANNUAL_VALUE_TECHNICAL_FEATURE_IMPORTANCE_EXPERIMENT_ID",
    "AnnualValueTechnicalFeatureImportanceResult",
    "TECHNICAL_FEATURES",
    "get_annual_value_technical_feature_importance_bundle_path_for_run_id",
    "get_annual_value_technical_feature_importance_latest_bundle_path",
    "load_annual_value_technical_feature_importance_bundle",
    "run_annual_value_technical_feature_importance",
    "write_annual_value_technical_feature_importance_bundle",
)
