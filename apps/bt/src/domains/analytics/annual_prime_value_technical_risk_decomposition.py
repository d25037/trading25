"""Prime value technical-risk decomposition research."""

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
    _empty_df,
    _series_mean,
)
from src.domains.analytics.annual_value_composite_technical_filter import (
    _load_technical_price_frames,
)
from src.domains.analytics.annual_value_technical_feature_importance import (
    DEFAULT_BUCKET_COUNT,
    DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    DEFAULT_FOCUS_SCORE_METHODS,
    DEFAULT_WARMUP_SMA_WINDOW,
    _build_enriched_event_df,
    _normalize_score_methods,
    _yearly_bucket_rank,
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

ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/annual-prime-value-technical-risk-decomposition"
)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10)
DEFAULT_MARKET_SCOPE = "prime"
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_event_df",
    "risk_bucket_summary_df",
    "risk_spread_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)

_RISK_FEATURE_LABELS: dict[str, str] = {
    "volatility_20d_pct": "Stock volatility 20d",
    "volatility_60d_pct": "Stock volatility 60d",
    "downside_volatility_60d_pct": "Stock downside volatility 60d",
    "beta_252d": "TOPIX beta 252d",
    "correlation_topix_252d": "TOPIX correlation 252d",
    "idiosyncratic_volatility_252d_pct": "Idiosyncratic volatility 252d",
    "topix_volatility_60d_pct": "TOPIX volatility 60d",
}
_RISK_FEATURES: tuple[str, ...] = tuple(_RISK_FEATURE_LABELS)


@dataclass(frozen=True)
class AnnualPrimeValueTechnicalRiskDecompositionResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_scope: str
    selection_fractions: tuple[float, ...]
    liquidity_scenario: str
    score_methods: tuple[str, ...]
    bucket_count: int
    selected_event_count: int
    risk_feature_count: int
    risk_policy: str
    enriched_event_df: pd.DataFrame
    risk_bucket_summary_df: pd.DataFrame
    risk_spread_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def run_annual_prime_value_technical_risk_decomposition(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    market_scope: str = DEFAULT_MARKET_SCOPE,
    liquidity_scenario: str = DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    score_methods: Sequence[str] = DEFAULT_FOCUS_SCORE_METHODS,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
) -> AnnualPrimeValueTechnicalRiskDecompositionResult:
    if bucket_count < 2:
        raise ValueError("bucket_count must be >= 2")
    normalized_score_methods = _normalize_score_methods(score_methods)
    normalized_fractions = tuple(float(value) for value in selection_fractions)
    if not normalized_fractions:
        raise ValueError("selection_fractions must not be empty")
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
    selected_event_df = _focus_prime_events(
        value_result.selected_event_df,
        market_scope=market_scope,
        selection_fractions=normalized_fractions,
        liquidity_scenario=liquidity_scenario,
        score_methods=normalized_score_methods,
    )
    source_mode, source_detail, stock_price_df, topix_price_df = _load_technical_price_frames(
        value_result.db_path,
        selected_event_df,
        sma_window=DEFAULT_WARMUP_SMA_WINDOW,
    )
    enriched = _build_enriched_event_df(
        selected_event_df,
        stock_price_df=stock_price_df,
        topix_price_df=topix_price_df,
    )
    enriched = _add_market_risk_features(
        enriched,
        stock_price_df=stock_price_df,
        topix_price_df=topix_price_df,
    )
    risk_bucket_summary_df = _build_risk_bucket_summary_df(enriched, bucket_count=bucket_count)
    risk_spread_df = _build_risk_spread_df(risk_bucket_summary_df)
    portfolio_event_df = _build_portfolio_event_df(enriched, bucket_count=bucket_count)
    portfolio_daily_df = _build_portfolio_daily_df(portfolio_event_df, stock_price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, portfolio_event_df)
    risk_feature_count = int(pd.to_numeric(enriched["beta_252d"], errors="coerce").notna().sum())
    return AnnualPrimeValueTechnicalRiskDecompositionResult(
        db_path=value_result.db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=value_result.analysis_start_date,
        analysis_end_date=value_result.analysis_end_date,
        market_scope=str(market_scope),
        selection_fractions=normalized_fractions,
        liquidity_scenario=str(liquidity_scenario),
        score_methods=normalized_score_methods,
        bucket_count=int(bucket_count),
        selected_event_count=int(len(selected_event_df)),
        risk_feature_count=risk_feature_count,
        risk_policy=(
            "technical and market-risk features use only daily returns strictly before entry_date; "
            "beta-adjusted return subtracts pre-entry TOPIX beta times same-holding-period TOPIX return; "
            "portfolio lens uses equal-weight daily close path from annual entry open to exit close; "
            "fixed_55_25_20 is intentionally excluded"
        ),
        enriched_event_df=enriched,
        risk_bucket_summary_df=risk_bucket_summary_df,
        risk_spread_df=risk_spread_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _focus_prime_events(
    selected_event_df: pd.DataFrame,
    *,
    market_scope: str,
    selection_fractions: Sequence[float],
    liquidity_scenario: str,
    score_methods: Sequence[str],
) -> pd.DataFrame:
    if selected_event_df.empty:
        return selected_event_df.copy()
    fractions = {round(float(value), 6) for value in selection_fractions}
    fraction = pd.to_numeric(selected_event_df["selection_fraction"], errors="coerce").round(6)
    return selected_event_df[
        (selected_event_df["market_scope"].astype(str) == str(market_scope))
        & (selected_event_df["liquidity_scenario"].astype(str) == str(liquidity_scenario))
        & (selected_event_df["score_method"].astype(str).isin(tuple(score_methods)))
        & (fraction.isin(fractions))
    ].copy()


def _add_market_risk_features(
    event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
    topix_price_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_df.empty:
        return _ensure_risk_columns(event_df.copy())
    topix = _prepare_price_frame(topix_price_df)
    topix_returns = _daily_return_frame(topix)
    stock_returns_by_code = {
        str(code): _daily_return_frame(_prepare_price_frame(frame))
        for code, frame in stock_price_df.groupby("code", sort=False)
    }
    stock_close_by_code = {
        str(code): _prepare_price_frame(frame) for code, frame in stock_price_df.groupby("code", sort=False)
    }
    topix_close = topix.set_index("date")["close"].sort_index() if not topix.empty else pd.Series(dtype="float64")
    rows: list[dict[str, Any]] = []
    for event in event_df.to_dict(orient="records"):
        payload: dict[str, Any] = {str(key): value for key, value in event.items()}
        code = str(payload.get("code", ""))
        entry_date = _coerce_timestamp(payload.get("entry_date"))
        exit_date = _coerce_timestamp(payload.get("exit_date"))
        stock_returns = stock_returns_by_code.get(code, _empty_return_frame())
        risk_60 = _risk_metrics_before_entry(stock_returns, topix_returns, entry_date=entry_date, window=60)
        risk_252 = _risk_metrics_before_entry(stock_returns, topix_returns, entry_date=entry_date, window=252)
        payload.update({f"{key}_60d": value for key, value in risk_60.items()})
        payload.update({f"{key}_252d": value for key, value in risk_252.items()})
        topix_event_return = _period_return_pct(topix_close, entry_date=entry_date, exit_date=exit_date)
        payload["topix_event_return_pct"] = topix_event_return
        event_return = _float_or_nan(payload.get("event_return_winsor_pct"))
        beta = _float_or_nan(payload.get("beta_252d"))
        payload["beta_adjusted_event_return_pct"] = (
            event_return - beta * topix_event_return
            if math.isfinite(event_return) and math.isfinite(beta) and math.isfinite(topix_event_return)
            else np.nan
        )
        close = stock_close_by_code.get(code)
        payload["stock_event_return_pct"] = _period_return_pct(
            close.set_index("date")["close"].sort_index() if close is not None and not close.empty else pd.Series(dtype="float64"),
            entry_date=entry_date,
            exit_date=exit_date,
        )
        rows.append(payload)
    return _ensure_risk_columns(pd.DataFrame(rows))


def _prepare_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_df(["date", "close"])
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna(subset=["date", "close"]).sort_values("date", kind="stable")
    return result.drop_duplicates("date", keep="last").reset_index(drop=True)


def _daily_return_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_return_frame()
    result = frame[["date", "close"]].copy()
    result["ret"] = pd.to_numeric(result["close"], errors="coerce").pct_change()
    return result.dropna(subset=["date", "ret"])[["date", "ret"]]


def _empty_return_frame() -> pd.DataFrame:
    return _empty_df(["date", "ret"])


def _risk_metrics_before_entry(
    stock_returns: pd.DataFrame,
    topix_returns: pd.DataFrame,
    *,
    entry_date: pd.Timestamp | None,
    window: int,
) -> dict[str, float]:
    empty = {
        "beta": np.nan,
        "correlation_topix": np.nan,
        "r_squared_topix": np.nan,
        "total_volatility_pct": np.nan,
        "idiosyncratic_volatility_pct": np.nan,
        "return": np.nan,
    }
    if entry_date is None or stock_returns.empty or topix_returns.empty:
        return empty
    stock = stock_returns[stock_returns["date"] < entry_date].tail(window)
    topix = topix_returns[topix_returns["date"] < entry_date].tail(window)
    aligned = pd.merge(stock, topix, on="date", how="inner", suffixes=("_stock", "_topix")).dropna()
    if len(aligned) < max(20, min(window, 60)):
        return empty
    y = pd.to_numeric(aligned["ret_stock"], errors="coerce").astype(float)
    x = pd.to_numeric(aligned["ret_topix"], errors="coerce").astype(float)
    clean = pd.DataFrame({"stock": y, "topix": x}).dropna()
    if len(clean) < max(20, min(window, 60)):
        return empty
    stock_values = clean["stock"].to_numpy(dtype=float)
    topix_values = clean["topix"].to_numpy(dtype=float)
    var_x = float(np.var(topix_values))
    beta = float(np.cov(stock_values, topix_values, ddof=0)[0, 1] / var_x) if var_x > 0 else np.nan
    corr = (
        float(np.corrcoef(stock_values, topix_values)[0, 1])
        if len(np.unique(stock_values)) > 1 and len(np.unique(topix_values)) > 1
        else np.nan
    )
    residual = stock_values - beta * topix_values if math.isfinite(beta) else stock_values - float(np.mean(stock_values))
    total_vol = float(np.std(stock_values, ddof=1) * math.sqrt(252.0) * 100.0)
    idio_vol = float(np.std(residual, ddof=1) * math.sqrt(252.0) * 100.0)
    period_return = float(np.prod(1.0 + stock_values) - 1.0) * 100.0
    return {
        "beta": beta,
        "correlation_topix": corr,
        "r_squared_topix": corr * corr if math.isfinite(corr) else np.nan,
        "total_volatility_pct": total_vol,
        "idiosyncratic_volatility_pct": idio_vol,
        "return": period_return,
    }


def _period_return_pct(
    close: pd.Series,
    *,
    entry_date: pd.Timestamp | None,
    exit_date: pd.Timestamp | None,
) -> float:
    if entry_date is None or exit_date is None or close.empty:
        return float("nan")
    dates = pd.DatetimeIndex(close.index)
    entry_position = dates.searchsorted(entry_date, side="left") - 1
    exit_position = dates.searchsorted(exit_date, side="right") - 1
    if entry_position < 0 or exit_position < 0 or exit_position <= entry_position:
        return float("nan")
    entry_close = float(close.iloc[int(entry_position)])
    exit_close = float(close.iloc[int(exit_position)])
    if entry_close <= 0 or not math.isfinite(entry_close) or not math.isfinite(exit_close):
        return float("nan")
    return (exit_close / entry_close - 1.0) * 100.0


def _ensure_risk_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for suffix in ("60d", "252d"):
        for key in (
            "beta",
            "correlation_topix",
            "r_squared_topix",
            "total_volatility_pct",
            "idiosyncratic_volatility_pct",
            "return",
        ):
            column = f"{key}_{suffix}"
            if column not in result.columns:
                result[column] = np.nan
    for column in ("topix_event_return_pct", "beta_adjusted_event_return_pct", "stock_event_return_pct"):
        if column not in result.columns:
            result[column] = np.nan
    return result


def _build_risk_bucket_summary_df(enriched_event_df: pd.DataFrame, *, bucket_count: int) -> pd.DataFrame:
    columns = [
        "selection_fraction",
        "score_method",
        "feature_key",
        "feature_label",
        "bucket_rank",
        "bucket_count",
        "event_count",
        "year_count",
        "coverage_pct",
        "feature_median",
        "mean_return_pct",
        "p10_return_pct",
        "worst_return_pct",
        "mean_beta_adjusted_return_pct",
        "p10_beta_adjusted_return_pct",
        "mean_beta_252d",
        "mean_correlation_topix_252d",
        "mean_total_volatility_252d_pct",
        "mean_idiosyncratic_volatility_252d_pct",
        "median_market_cap_bil_jpy",
        "median_adv60_mil_jpy",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in enriched_event_df.groupby(["selection_fraction", "score_method"], sort=False):
        selection_fraction, score_method = keys
        total_count = len(group)
        for feature_key in _RISK_FEATURES:
            if feature_key not in group.columns:
                continue
            valid = group[pd.to_numeric(group[feature_key], errors="coerce").notna()].copy()
            if valid.empty:
                continue
            valid["bucket_rank"] = _yearly_bucket_rank(valid, feature_key, bucket_count=bucket_count)
            for bucket_rank, bucket_df in valid.groupby("bucket_rank", sort=True):
                returns = pd.to_numeric(bucket_df["event_return_winsor_pct"], errors="coerce").dropna()
                beta_adj = pd.to_numeric(bucket_df["beta_adjusted_event_return_pct"], errors="coerce").dropna()
                records.append(
                    {
                        "selection_fraction": float(selection_fraction),
                        "score_method": str(score_method),
                        "feature_key": feature_key,
                        "feature_label": _RISK_FEATURE_LABELS[feature_key],
                        "bucket_rank": int(cast(int, bucket_rank)),
                        "bucket_count": int(bucket_count),
                        "event_count": int(len(bucket_df)),
                        "year_count": int(bucket_df["year"].nunique()),
                        "coverage_pct": float(len(valid) / total_count * 100.0) if total_count else None,
                        "feature_median": _median(bucket_df[feature_key]),
                        "mean_return_pct": _series_mean(returns),
                        "p10_return_pct": _quantile(returns, 0.10),
                        "worst_return_pct": float(returns.min()) if not returns.empty else None,
                        "mean_beta_adjusted_return_pct": _series_mean(beta_adj),
                        "p10_beta_adjusted_return_pct": _quantile(beta_adj, 0.10),
                        "mean_beta_252d": _mean(bucket_df["beta_252d"]),
                        "mean_correlation_topix_252d": _mean(bucket_df["correlation_topix_252d"]),
                        "mean_total_volatility_252d_pct": _mean(bucket_df["total_volatility_pct_252d"]),
                        "mean_idiosyncratic_volatility_252d_pct": _mean(bucket_df["idiosyncratic_volatility_pct_252d"]),
                        "median_market_cap_bil_jpy": _median(bucket_df["market_cap_bil_jpy"]),
                        "median_adv60_mil_jpy": _median(bucket_df["avg_trading_value_60d_mil_jpy"]),
                    }
                )
    return pd.DataFrame(records, columns=columns)


def _build_risk_spread_df(risk_bucket_summary_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "selection_fraction",
        "score_method",
        "feature_key",
        "feature_label",
        "low_bucket_rank",
        "high_bucket_rank",
        "low_event_count",
        "high_event_count",
        "high_minus_low_mean_return_pct",
        "high_minus_low_p10_return_pct",
        "high_minus_low_beta_adjusted_mean_pct",
        "high_minus_low_beta_252d",
        "high_minus_low_correlation_topix_252d",
        "high_minus_low_total_volatility_252d_pct",
        "high_minus_low_idiosyncratic_volatility_252d_pct",
        "high_minus_low_market_cap_bil_jpy",
        "high_minus_low_adv60_mil_jpy",
        "risk_interpretation",
    ]
    if risk_bucket_summary_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    group_cols = ["selection_fraction", "score_method", "feature_key"]
    for keys, group in risk_bucket_summary_df.groupby(group_cols, sort=False):
        selection_fraction, score_method, feature_key = keys
        low = group[group["bucket_rank"].astype(int) == 1].head(1)
        high_rank = int(group["bucket_count"].astype(int).max())
        high = group[group["bucket_rank"].astype(int) == high_rank].head(1)
        if low.empty or high.empty:
            continue
        mean_spread = _row_value(high, "mean_return_pct") - _row_value(low, "mean_return_pct")
        beta_adj_spread = _row_value(high, "mean_beta_adjusted_return_pct") - _row_value(
            low, "mean_beta_adjusted_return_pct"
        )
        p10_spread = _row_value(high, "p10_return_pct") - _row_value(low, "p10_return_pct")
        idio_spread = _row_value(high, "mean_idiosyncratic_volatility_252d_pct") - _row_value(
            low, "mean_idiosyncratic_volatility_252d_pct"
        )
        records.append(
            {
                "selection_fraction": float(selection_fraction),
                "score_method": str(score_method),
                "feature_key": str(feature_key),
                "feature_label": _RISK_FEATURE_LABELS[str(feature_key)],
                "low_bucket_rank": 1,
                "high_bucket_rank": high_rank,
                "low_event_count": int(_row_value(low, "event_count")),
                "high_event_count": int(_row_value(high, "event_count")),
                "high_minus_low_mean_return_pct": mean_spread,
                "high_minus_low_p10_return_pct": p10_spread,
                "high_minus_low_beta_adjusted_mean_pct": beta_adj_spread,
                "high_minus_low_beta_252d": _row_value(high, "mean_beta_252d") - _row_value(low, "mean_beta_252d"),
                "high_minus_low_correlation_topix_252d": _row_value(high, "mean_correlation_topix_252d")
                - _row_value(low, "mean_correlation_topix_252d"),
                "high_minus_low_total_volatility_252d_pct": _row_value(high, "mean_total_volatility_252d_pct")
                - _row_value(low, "mean_total_volatility_252d_pct"),
                "high_minus_low_idiosyncratic_volatility_252d_pct": idio_spread,
                "high_minus_low_market_cap_bil_jpy": _row_value(high, "median_market_cap_bil_jpy")
                - _row_value(low, "median_market_cap_bil_jpy"),
                "high_minus_low_adv60_mil_jpy": _row_value(high, "median_adv60_mil_jpy")
                - _row_value(low, "median_adv60_mil_jpy"),
                "risk_interpretation": _risk_interpretation(mean_spread, beta_adj_spread, p10_spread, idio_spread),
            }
        )
    return pd.DataFrame(records, columns=columns)


def _risk_interpretation(
    mean_spread: float,
    beta_adj_spread: float,
    p10_spread: float,
    idio_spread: float,
) -> str:
    if mean_spread > 0 and beta_adj_spread > 0 and p10_spread >= 0:
        return "alpha_like"
    if mean_spread > 0 and p10_spread < 0 and idio_spread > 0:
        return "high_vol_right_tail_with_left_tail_cost"
    if mean_spread > 0 and beta_adj_spread <= 0:
        return "market_beta_exposure"
    if mean_spread <= 0 and p10_spread >= 0:
        return "defensive_but_mean_drag"
    return "weak_or_unstable"


def _build_portfolio_event_df(enriched_event_df: pd.DataFrame, *, bucket_count: int) -> pd.DataFrame:
    columns = [
        *list(enriched_event_df.columns),
        "portfolio_feature_key",
        "portfolio_feature_label",
        "portfolio_variant",
        "portfolio_bucket_rank",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in enriched_event_df.groupby(["selection_fraction", "score_method"], sort=False):
        _selection_fraction, _score_method = keys
        for feature_key in _RISK_FEATURES:
            if feature_key not in group.columns:
                continue
            valid = group[pd.to_numeric(group[feature_key], errors="coerce").notna()].copy()
            if valid.empty:
                continue
            valid["portfolio_bucket_rank"] = _yearly_bucket_rank(valid, feature_key, bucket_count=bucket_count)
            for event in valid.to_dict(orient="records"):
                baseline: dict[str, Any] = {str(key): value for key, value in event.items()}
                baseline["portfolio_feature_key"] = feature_key
                baseline["portfolio_feature_label"] = _RISK_FEATURE_LABELS[feature_key]
                baseline["portfolio_variant"] = "baseline"
                baseline["portfolio_bucket_rank"] = 0
                records.append(baseline)
            low = valid[valid["portfolio_bucket_rank"].astype(int) == 1]
            high = valid[valid["portfolio_bucket_rank"].astype(int) == bucket_count]
            for variant, frame in (("low_bucket", low), ("high_bucket", high)):
                for event in frame.to_dict(orient="records"):
                    payload: dict[str, Any] = {str(key): value for key, value in event.items()}
                    payload["portfolio_feature_key"] = feature_key
                    payload["portfolio_feature_label"] = _RISK_FEATURE_LABELS[feature_key]
                    payload["portfolio_variant"] = variant
                    records.append(payload)
    return pd.DataFrame(records, columns=columns)


def _build_portfolio_daily_df(portfolio_event_df: pd.DataFrame, stock_price_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "selection_fraction",
        "score_method",
        "portfolio_feature_key",
        "portfolio_feature_label",
        "portfolio_variant",
        "date",
        "active_positions",
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
    aggregate: dict[tuple[float, str, str, str, str, str], list[float]] = {}
    for event in portfolio_event_df.to_dict(orient="records"):
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
        entry_open = _float_or_nan(event.get("entry_open"))
        if not math.isfinite(entry_open) or entry_open <= 0:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            key = (
                float(cast(float, event["selection_fraction"])),
                str(event["score_method"]),
                str(event["portfolio_feature_key"]),
                str(event["portfolio_feature_label"]),
                str(event["portfolio_variant"]),
                str(date_value),
            )
            values = aggregate.setdefault(key, [0.0, 0.0])
            values[0] += float(daily_return)
            values[1] += 1.0
    if not aggregate:
        return _empty_df(columns)
    records = [
        {
            "selection_fraction": selection_fraction,
            "score_method": score_method,
            "portfolio_feature_key": feature_key,
            "portfolio_feature_label": feature_label,
            "portfolio_variant": variant,
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for (
            selection_fraction,
            score_method,
            feature_key,
            feature_label,
            variant,
            date_value,
        ), values in aggregate.items()
    ]
    daily_df = pd.DataFrame(records).sort_values(
        ["selection_fraction", "score_method", "portfolio_feature_key", "portfolio_variant", "date"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    group_cols = ["selection_fraction", "score_method", "portfolio_feature_key", "portfolio_variant"]
    for _, group in daily_df.groupby(group_cols, observed=True, sort=False):
        idx = list(group.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df[columns]


def _build_portfolio_summary_df(portfolio_daily_df: pd.DataFrame, portfolio_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "selection_fraction",
        "score_method",
        "portfolio_feature_key",
        "portfolio_feature_label",
        "portfolio_variant",
        "event_count",
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
        "worst_year",
        "worst_year_return_pct",
        "best_year",
        "best_year_return_pct",
        "worst_trade_return_pct",
        "p10_trade_return_pct",
    ]
    if portfolio_daily_df.empty:
        return _empty_df(columns)
    event_counts = portfolio_event_df.groupby(
        ["selection_fraction", "score_method", "portfolio_feature_key", "portfolio_variant"],
        observed=True,
        sort=False,
    ).size()
    trade_stats = (
        portfolio_event_df.groupby(
            ["selection_fraction", "score_method", "portfolio_feature_key", "portfolio_variant"],
            observed=True,
            sort=False,
        )["event_return_winsor_pct"]
        .agg(
            worst_trade_return_pct="min",
            p10_trade_return_pct=lambda s: pd.to_numeric(s, errors="coerce").quantile(0.10),
        )
    )
    records: list[dict[str, Any]] = []
    group_cols = ["selection_fraction", "score_method", "portfolio_feature_key", "portfolio_variant"]
    for keys, group in portfolio_daily_df.groupby(group_cols, observed=True, sort=False):
        selection_fraction, score_method, feature_key, variant = keys
        start_date = str(group["date"].iloc[0])
        end_date = str(group["date"].iloc[-1])
        total_return = float(group["portfolio_value"].iloc[-1] - 1.0)
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = np.nan
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value * 100.0) if math.isfinite(cagr_value) else np.nan
        max_drawdown = _float_or_nan(pd.to_numeric(group["drawdown_pct"], errors="coerce").min())
        daily_stats = _daily_stats(group["mean_daily_return"])
        yearly = _portfolio_year_returns(group)
        worst_year = str(yearly["year"].iloc[0]) if not yearly.empty else None
        worst_year_return = float(yearly["year_return_pct"].iloc[0]) if not yearly.empty else np.nan
        best_year_df = yearly.sort_values("year_return_pct", ascending=False).head(1) if not yearly.empty else yearly
        best_year = str(best_year_df["year"].iloc[0]) if not best_year_df.empty else None
        best_year_return = float(best_year_df["year_return_pct"].iloc[0]) if not best_year_df.empty else np.nan
        label = str(group["portfolio_feature_label"].iloc[0])
        count_key = (selection_fraction, score_method, feature_key, variant)
        stats_row = trade_stats.loc[count_key] if count_key in trade_stats.index else None
        records.append(
            {
                "selection_fraction": float(selection_fraction),
                "score_method": str(score_method),
                "portfolio_feature_key": str(feature_key),
                "portfolio_feature_label": label,
                "portfolio_variant": str(variant),
                "event_count": int(event_counts.get(count_key, 0)),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group)),
                "avg_active_positions": _mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"], errors="coerce").max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr,
                "max_drawdown_pct": max_drawdown,
                "annualized_volatility_pct": daily_stats["annualized_volatility_pct"],
                "sharpe_ratio": daily_stats["sharpe_ratio"],
                "sortino_ratio": daily_stats["sortino_ratio"],
                "calmar_ratio": cagr / abs(max_drawdown) if math.isfinite(cagr) and max_drawdown < 0 else np.nan,
                "worst_year": worst_year,
                "worst_year_return_pct": worst_year_return,
                "best_year": best_year,
                "best_year_return_pct": best_year_return,
                "worst_trade_return_pct": float(stats_row["worst_trade_return_pct"])
                if stats_row is not None
                else np.nan,
                "p10_trade_return_pct": float(stats_row["p10_trade_return_pct"]) if stats_row is not None else np.nan,
            }
        )
    return pd.DataFrame(records, columns=columns)


def _daily_stats(daily_returns: pd.Series) -> dict[str, float]:
    returns = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if returns.empty:
        return {
            "annualized_volatility_pct": np.nan,
            "sharpe_ratio": np.nan,
            "sortino_ratio": np.nan,
        }
    mean = float(returns.mean())
    std = float(returns.std(ddof=1)) if len(returns) > 1 else np.nan
    downside = returns[returns < 0.0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else np.nan
    volatility = std * math.sqrt(252.0) * 100.0 if math.isfinite(std) else np.nan
    sharpe = mean / std * math.sqrt(252.0) if math.isfinite(std) and std > 0 else np.nan
    sortino = mean / downside_std * math.sqrt(252.0) if math.isfinite(downside_std) and downside_std > 0 else np.nan
    return {
        "annualized_volatility_pct": volatility,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
    }


def _portfolio_year_returns(group: pd.DataFrame) -> pd.DataFrame:
    work = group.copy()
    work["year"] = pd.to_datetime(work["date"], errors="coerce").dt.year.astype("Int64")
    records: list[dict[str, Any]] = []
    for year, year_df in work.dropna(subset=["year"]).groupby("year", sort=True):
        returns = pd.to_numeric(year_df["mean_daily_return"], errors="coerce").dropna()
        if returns.empty:
            continue
        year_return = float(np.prod(1.0 + returns.to_numpy(dtype=float)) - 1.0) * 100.0
        records.append(
            {
                "year": int(cast(Any, year)),
                "year_return_pct": year_return,
            }
        )
    if not records:
        return _empty_df(["year", "year_return_pct"])
    return pd.DataFrame(records).sort_values("year_return_pct", ascending=True).reset_index(drop=True)


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


def _mean(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.mean()) if not clean.empty else float("nan")


def _median(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.median()) if not clean.empty else float("nan")


def _quantile(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.quantile(q)) if not clean.empty else float("nan")


def _row_value(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return float("nan")
    return _float_or_nan(frame.iloc[0][column])


def _fmt(value: object, digits: int = 2) -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _build_summary_markdown(result: AnnualPrimeValueTechnicalRiskDecompositionResult) -> str:
    lines = [
        "# Annual Prime Value Technical Risk Decomposition",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Market scope: `{result.market_scope}`",
        f"- Selection fractions: `{', '.join(str(value) for value in result.selection_fractions)}`",
        f"- Score methods: `{', '.join(result.score_methods)}`",
        f"- Selected event rows: `{result.selected_event_count}`",
        f"- Rows with beta_252d: `{result.risk_feature_count}`",
        f"- Risk policy: {result.risk_policy}.",
        "",
        "## High-Low Risk Spreads",
        "",
    ]
    if result.risk_spread_df.empty:
        lines.append("- No risk spread rows were produced.")
    else:
        focus = result.risk_spread_df.sort_values(
            ["selection_fraction", "score_method", "high_minus_low_mean_return_pct"],
            ascending=[True, True, False],
        ).head(24)
        for row in focus.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{_fmt(row['selection_fraction'], 2)}` / `{row['score_method']}` / `{row['feature_key']}`: "
                f"mean spread `{_fmt(row['high_minus_low_mean_return_pct'])}pp`, "
                f"p10 spread `{_fmt(row['high_minus_low_p10_return_pct'])}pp`, "
                f"beta-adjusted mean spread `{_fmt(row['high_minus_low_beta_adjusted_mean_pct'])}pp`, "
                f"idio vol spread `{_fmt(row['high_minus_low_idiosyncratic_volatility_252d_pct'])}pp`, "
                f"`{row['risk_interpretation']}`"
            )
    lines.extend(
        [
            "",
            "## Daily Portfolio Lens",
            "",
        ]
    )
    if result.portfolio_summary_df.empty:
        lines.append("- No portfolio summary rows were produced.")
    else:
        focus_features = ("volatility_20d_pct", "volatility_60d_pct")
        focus = result.portfolio_summary_df[
            result.portfolio_summary_df["portfolio_feature_key"].astype(str).isin(focus_features)
            & result.portfolio_summary_df["portfolio_variant"].astype(str).isin(("baseline", "high_bucket"))
        ].copy()
        baseline = focus[focus["portfolio_variant"].astype(str) == "baseline"]
        high = focus[focus["portfolio_variant"].astype(str) == "high_bucket"]
        for row in high.sort_values(["selection_fraction", "score_method", "portfolio_feature_key"]).to_dict(
            orient="records",
        ):
            key = (
                row["selection_fraction"],
                row["score_method"],
                row["portfolio_feature_key"],
            )
            base = baseline[
                (baseline["selection_fraction"] == key[0])
                & (baseline["score_method"] == key[1])
                & (baseline["portfolio_feature_key"] == key[2])
            ].head(1)
            if base.empty:
                continue
            base_row = base.iloc[0]
            lines.append(
                "- "
                f"`{_fmt(row['selection_fraction'], 2)}` / `{row['score_method']}` / "
                f"`{row['portfolio_feature_key']}` high bucket: "
                f"CAGR `{_fmt(base_row['cagr_pct'])}%` -> `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(base_row['sharpe_ratio'])}` -> `{_fmt(row['sharpe_ratio'])}`, "
                f"MaxDD `{_fmt(base_row['max_drawdown_pct'])}%` -> `{_fmt(row['max_drawdown_pct'])}%`, "
                f"worst year `{base_row['worst_year']} {_fmt(base_row['worst_year_return_pct'])}%` -> "
                f"`{row['worst_year']} {_fmt(row['worst_year_return_pct'])}%`, "
                f"worst trade `{_fmt(base_row['worst_trade_return_pct'])}%` -> "
                f"`{_fmt(row['worst_trade_return_pct'])}%`"
            )
    return "\n".join(lines)


def write_annual_prime_value_technical_risk_decomposition_bundle(
    result: AnnualPrimeValueTechnicalRiskDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_prime_value_technical_risk_decomposition",
        params={
            "input_bundle_path": result.input_bundle_path,
            "selection_fractions": list(result.selection_fractions),
            "market_scope": result.market_scope,
            "liquidity_scenario": result.liquidity_scenario,
            "score_methods": list(result.score_methods),
            "bucket_count": result.bucket_count,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_prime_value_technical_risk_decomposition_bundle(
    bundle_path: str | Path,
) -> AnnualPrimeValueTechnicalRiskDecompositionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualPrimeValueTechnicalRiskDecompositionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_prime_value_technical_risk_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_prime_value_technical_risk_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "ANNUAL_PRIME_VALUE_TECHNICAL_RISK_DECOMPOSITION_EXPERIMENT_ID",
    "AnnualPrimeValueTechnicalRiskDecompositionResult",
    "get_annual_prime_value_technical_risk_decomposition_bundle_path_for_run_id",
    "get_annual_prime_value_technical_risk_decomposition_latest_bundle_path",
    "load_annual_prime_value_technical_risk_decomposition_bundle",
    "run_annual_prime_value_technical_risk_decomposition",
    "write_annual_prime_value_technical_risk_decomposition_bundle",
)
