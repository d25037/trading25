"""Trade-archetype decomposition for the current range-break production strategy."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast

import numpy as np
import pandas as pd

from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    _assign_quantile_bucket,
    _build_market_scope_summary_df,
    _build_topix_feature_df,
    _build_trade_metrics,
    _build_value_feature_row,
    _build_value_feature_bucket_summary_df,
    _build_value_overlay_summary_df,
    _coerce_float,
    _coerce_numeric_series,
    _coerce_timestamp,
    _extract_row,
    _fmt_timestamp,
    _infer_market_scope_from_dataset_name,
    _is_market_universe_preset,
    _load_dataset_stock_metadata,
    _load_market_stock_metadata_for_trades,
    _numeric_median,
    _resolve_last_index_value,
    _resolve_previous_index_value,
    _safe_ge,
    _safe_gt,
    _safe_le,
    _sort_market_scope_summary,
)
from src.domains.analytics.production_strategy_single_name_trade_quality import (
    run_production_strategy_single_name_trade_quality,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.backtest.core.runner import BacktestRunner
from src.domains.strategy.indicators import (
    compute_bollinger_bands,
    compute_risk_adjusted_return,
    compute_rsi,
    compute_trading_value_ma,
)
from src.domains.strategy.indicators.calculations import compute_volume_mas
from src.domains.strategy.signals.beta import vectorbt_rolling_beta
from src.domains.strategy.signals.registry import _select_forward_forecast_eps_column
from src.infrastructure.data_access.loaders import (
    load_statements_data,
    load_stock_data,
    load_topix_data,
)
from src.infrastructure.data_access.mode import data_access_mode_context
from src.shared.models.signals import SignalParams

RANGE_BREAK_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID = (
    "strategy-audit/range-break-trade-archetype-decomposition"
)
DEFAULT_STRATEGY_NAME = "production/range_break_v15"
DEFAULT_DATASET_NAME = "primeExTopix500"
DEFAULT_HOLDOUT_MONTHS = 6
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_QUANTILE_BUCKET_COUNT = 5
_WARMUP_CALENDAR_DAYS = 420
_FEATURE_COLUMNS = (
    "breakout_close_vs_200d_high_pct",
    "breakout_20d_runup_pct",
    "breakout_60d_runup_pct",
    "breakout_120d_runup_pct",
    "distance_to_upper_band_pct",
    "bollinger_band_width_pct",
    "rsi10",
    "volume_ratio_value",
    "volume_ratio_margin",
    "rolling_beta_50",
    "beta_distance_to_min",
    "beta_distance_to_max",
    "trading_value_ma_15_oku",
    "trading_value_distance_to_min_pct",
    "trading_value_distance_to_max_pct",
    "stock_volatility_20d_pct",
    "risk_adjusted_return_60",
    "topix_return_20d_pct",
    "topix_return_60d_pct",
    "topix_risk_adjusted_return_60",
    "topix_close_vs_sma200_pct",
)


@dataclass(frozen=True)
class RangeBreakTradeArchetypeDecompositionResult:
    db_path: str
    strategy_name: str
    dataset_name: str
    holdout_months: int
    severe_loss_threshold_pct: float
    quantile_bucket_count: int
    analysis_start_date: str
    analysis_end_date: str
    dataset_summary_df: pd.DataFrame
    scenario_summary_df: pd.DataFrame
    trade_ledger_df: pd.DataFrame
    enriched_trade_df: pd.DataFrame
    market_scope_summary_df: pd.DataFrame
    feature_bucket_summary_df: pd.DataFrame
    overlay_candidate_summary_df: pd.DataFrame
    value_feature_bucket_summary_df: pd.DataFrame
    value_overlay_candidate_summary_df: pd.DataFrame
    return_bucket_summary_df: pd.DataFrame


@dataclass(frozen=True)
class _OverlayCandidate:
    name: str
    family: str
    description: str
    predicate: Callable[[pd.DataFrame], pd.Series]


def run_range_break_trade_archetype_decomposition(
    *,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    dataset_name: str = DEFAULT_DATASET_NAME,
    holdout_months: int = DEFAULT_HOLDOUT_MONTHS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    quantile_bucket_count: int = DEFAULT_QUANTILE_BUCKET_COUNT,
) -> RangeBreakTradeArchetypeDecompositionResult:
    if holdout_months <= 0:
        raise ValueError("holdout_months must be greater than 0")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if quantile_bucket_count < 2:
        raise ValueError("quantile_bucket_count must be at least 2")

    trade_quality_result = run_production_strategy_single_name_trade_quality(
        strategy_names=(strategy_name,),
        dataset_names=(dataset_name,),
        holdout_months=holdout_months,
    )
    trade_ledger_df = trade_quality_result.trade_ledger_df.copy()
    if trade_ledger_df.empty:
        raise RuntimeError("range-break trade decomposition produced no trades")

    parameters = BacktestRunner().build_parameters_for_strategy(strategy_name)
    enriched_trade_df = _build_enriched_trade_df(
        trade_ledger_df=trade_ledger_df,
        dataset_name=dataset_name,
        parameters=parameters,
    )
    market_scope_summary_df = _build_market_scope_summary_df(
        enriched_trade_df=enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    feature_bucket_summary_df = _build_feature_bucket_summary_df(
        enriched_trade_df=enriched_trade_df,
        quantile_bucket_count=quantile_bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    overlay_candidate_summary_df = _build_overlay_candidate_summary_df(
        enriched_trade_df=enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    value_feature_bucket_summary_df = _build_value_feature_bucket_summary_df(
        enriched_trade_df=enriched_trade_df,
        quantile_bucket_count=quantile_bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    value_overlay_candidate_summary_df = _build_value_overlay_summary_df(
        enriched_trade_df=enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    return_bucket_summary_df = _build_return_bucket_summary_df(
        enriched_trade_df=enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )

    return RangeBreakTradeArchetypeDecompositionResult(
        db_path="multi://range-break-trade-archetype-decomposition",
        strategy_name=strategy_name,
        dataset_name=dataset_name,
        holdout_months=holdout_months,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        quantile_bucket_count=quantile_bucket_count,
        analysis_start_date=trade_quality_result.analysis_start_date,
        analysis_end_date=trade_quality_result.analysis_end_date,
        dataset_summary_df=trade_quality_result.dataset_summary_df.copy(),
        scenario_summary_df=trade_quality_result.scenario_summary_df.copy(),
        trade_ledger_df=trade_ledger_df,
        enriched_trade_df=enriched_trade_df,
        market_scope_summary_df=market_scope_summary_df,
        feature_bucket_summary_df=feature_bucket_summary_df,
        overlay_candidate_summary_df=overlay_candidate_summary_df,
        value_feature_bucket_summary_df=value_feature_bucket_summary_df,
        value_overlay_candidate_summary_df=value_overlay_candidate_summary_df,
        return_bucket_summary_df=return_bucket_summary_df,
    )


def write_range_break_trade_archetype_decomposition_bundle(
    result: RangeBreakTradeArchetypeDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANGE_BREAK_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_range_break_trade_archetype_decomposition",
        params={
            "strategy_name": result.strategy_name,
            "dataset_name": result.dataset_name,
            "holdout_months": result.holdout_months,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "quantile_bucket_count": result.quantile_bucket_count,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "strategy_name": result.strategy_name,
            "dataset_name": result.dataset_name,
            "holdout_months": result.holdout_months,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "quantile_bucket_count": result.quantile_bucket_count,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
        },
        result_tables={
            "dataset_summary_df": result.dataset_summary_df,
            "scenario_summary_df": result.scenario_summary_df,
            "trade_ledger_df": result.trade_ledger_df,
            "enriched_trade_df": result.enriched_trade_df,
            "market_scope_summary_df": result.market_scope_summary_df,
            "feature_bucket_summary_df": result.feature_bucket_summary_df,
            "overlay_candidate_summary_df": result.overlay_candidate_summary_df,
            "value_feature_bucket_summary_df": result.value_feature_bucket_summary_df,
            "value_overlay_candidate_summary_df": result.value_overlay_candidate_summary_df,
            "return_bucket_summary_df": result.return_bucket_summary_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_range_break_trade_archetype_decomposition_bundle(
    bundle_path: str | Path,
) -> RangeBreakTradeArchetypeDecompositionResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return RangeBreakTradeArchetypeDecompositionResult(
        db_path=str(metadata["db_path"]),
        strategy_name=str(metadata["strategy_name"]),
        dataset_name=str(metadata["dataset_name"]),
        holdout_months=int(metadata["holdout_months"]),
        severe_loss_threshold_pct=float(metadata["severe_loss_threshold_pct"]),
        quantile_bucket_count=int(metadata["quantile_bucket_count"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        dataset_summary_df=tables["dataset_summary_df"],
        scenario_summary_df=tables["scenario_summary_df"],
        trade_ledger_df=tables["trade_ledger_df"],
        enriched_trade_df=tables["enriched_trade_df"],
        market_scope_summary_df=tables["market_scope_summary_df"],
        feature_bucket_summary_df=tables["feature_bucket_summary_df"],
        overlay_candidate_summary_df=tables["overlay_candidate_summary_df"],
        value_feature_bucket_summary_df=tables["value_feature_bucket_summary_df"],
        value_overlay_candidate_summary_df=tables["value_overlay_candidate_summary_df"],
        return_bucket_summary_df=tables["return_bucket_summary_df"],
    )


def get_range_break_trade_archetype_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        RANGE_BREAK_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_range_break_trade_archetype_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        RANGE_BREAK_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_enriched_trade_df(
    *,
    trade_ledger_df: pd.DataFrame,
    dataset_name: str,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    enriched = trade_ledger_df.copy()
    enriched["entry_date"] = pd.to_datetime(enriched["entry_timestamp"]).dt.normalize()
    enriched["trade_positive"] = pd.to_numeric(
        enriched["trade_return_pct"],
        errors="coerce",
    ) > 0

    entry_dates = pd.to_datetime(enriched["entry_date"], errors="coerce").dropna()
    if entry_dates.empty:
        raise RuntimeError("range-break decomposition has no valid entry dates")
    load_start_date = (
        entry_dates.min() - pd.Timedelta(days=_WARMUP_CALENDAR_DAYS)
    ).strftime("%Y-%m-%d")
    load_end_date = entry_dates.max().strftime("%Y-%m-%d")

    if _is_market_universe_preset(dataset_name):
        stock_metadata_by_entry = _load_market_stock_metadata_for_trades(
            dataset_name=dataset_name,
            trade_df=enriched,
        )
        stock_metadata_by_code: dict[str, dict[str, str]] = {}
    else:
        stock_metadata_by_entry = {}
        stock_metadata_by_code = _load_dataset_stock_metadata(dataset_name)

    rows: list[dict[str, Any]] = []
    with data_access_mode_context("direct"):
        topix_feature_df = _build_topix_feature_df(
            dataset_name=dataset_name,
            start_date=load_start_date,
            end_date=load_end_date,
        )
        topix_close = _load_topix_close(
            dataset_name=dataset_name,
            start_date=load_start_date,
            end_date=load_end_date,
        )
        symbol_feature_cache: dict[str, dict[str, Any]] = {}
        for trade in enriched.itertuples(index=False):
            symbol = str(trade.symbol)
            symbol_features = symbol_feature_cache.get(symbol)
            if symbol_features is None:
                symbol_features = _build_symbol_feature_payload(
                    dataset_name=dataset_name,
                    stock_code=symbol,
                    start_date=load_start_date,
                    end_date=load_end_date,
                    parameters=parameters,
                    topix_close=topix_close,
                )
                symbol_feature_cache[symbol] = symbol_features

            entry_date = _coerce_timestamp(trade.entry_date)
            if entry_date is None:
                continue
            market_feature_date = _resolve_previous_index_value(
                cast(pd.DatetimeIndex, symbol_features["market_index"]),
                entry_date,
            )
            fundamental_feature_date = _resolve_last_index_value(
                cast(pd.DatetimeIndex, symbol_features["fundamental_index"]),
                entry_date,
            )
            market_row = _extract_row(
                cast(pd.DataFrame, symbol_features["market_features"]),
                market_feature_date,
            )
            topix_row = _extract_row(topix_feature_df, market_feature_date)
            fundamental_row = _extract_row(
                cast(pd.DataFrame, symbol_features["fundamental_features"]),
                fundamental_feature_date,
            )
            entry_date_key = entry_date.strftime("%Y-%m-%d")
            stock_metadata = stock_metadata_by_entry.get(
                (symbol, entry_date_key),
                stock_metadata_by_code.get(symbol, {}),
            )
            entry_price = _coerce_float(getattr(trade, "avg_entry_price", None))
            rows.append(
                {
                    **trade._asdict(),
                    "entry_date": entry_date.strftime("%Y-%m-%d"),
                    "market_code": stock_metadata.get("market_code"),
                    "market_name": stock_metadata.get("market_name"),
                    "market_scope": stock_metadata.get(
                        "market_scope",
                        _infer_market_scope_from_dataset_name(dataset_name),
                    ),
                    "market_feature_date": _fmt_timestamp(market_feature_date),
                    "fundamental_feature_date": _fmt_timestamp(fundamental_feature_date),
                    "market_feature_lag_days": (
                        float((entry_date - market_feature_date).days)
                        if market_feature_date is not None
                        else np.nan
                    ),
                    **_pick_market_feature_columns(market_row),
                    **_pick_topix_feature_columns(topix_row),
                    **_build_value_feature_row(
                        fundamental_row=fundamental_row,
                        entry_price=entry_price,
                    ),
                }
            )

    return pd.DataFrame(rows)


def _build_symbol_feature_payload(
    *,
    dataset_name: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    parameters: dict[str, Any],
    topix_close: pd.Series,
) -> dict[str, Any]:
    stock_df = load_stock_data(
        dataset_name,
        stock_code,
        start_date=start_date,
        end_date=end_date,
    )
    stock_index = cast(pd.DatetimeIndex, stock_df.index)
    statements_df = _load_optional_statements_data(
        dataset_name=dataset_name,
        stock_code=stock_code,
        stock_index=stock_index,
        start_date=start_date,
        end_date=end_date,
    )
    close = pd.to_numeric(stock_df["Close"], errors="coerce").astype(float)
    volume = pd.to_numeric(stock_df["Volume"], errors="coerce").astype(float)
    high = pd.to_numeric(stock_df.get("High", close), errors="coerce").astype(float)

    entry_params = parameters.get("entry_filter_params", {})
    period_extrema = entry_params.get("period_extrema_break", {})
    period = int(period_extrema.get("period", 200))
    volume_params = entry_params.get("volume_ratio_above", {})
    bollinger_params = entry_params.get("bollinger_position", {})
    beta_params = entry_params.get("beta", {})
    trading_value_params = entry_params.get("trading_value_range", {})

    high_basis = high if str(period_extrema.get("direction", "high")) == "high" else close
    period_high = high_basis.rolling(period, min_periods=period).max()
    previous_period_high = period_high.shift(1)
    short_ma, long_ma = compute_volume_mas(
        volume,
        short_period=int(volume_params.get("short_period", 50)),
        long_period=int(volume_params.get("long_period", 150)),
        ma_type=str(volume_params.get("ma_type", "sma")),
    )
    volume_ratio_value = short_ma / long_ma.where(long_ma != 0, np.nan)
    volume_threshold = _coerce_float(volume_params.get("ratio_threshold"))
    bollinger = compute_bollinger_bands(
        close,
        window=int(bollinger_params.get("window", 50)),
        alpha=float(bollinger_params.get("alpha", 2.0)),
    )
    trading_value_period = int(trading_value_params.get("period", 15))
    trading_value_ma = compute_trading_value_ma(close, volume, trading_value_period)
    trading_value_min = _coerce_float(trading_value_params.get("min_threshold"))
    trading_value_max = _coerce_float(trading_value_params.get("max_threshold"))
    rolling_beta = vectorbt_rolling_beta(
        close,
        topix_close.reindex(close.index).ffill(),
        window=int(beta_params.get("lookback_period", 50)),
    )
    beta_min = _coerce_float(beta_params.get("min_beta"))
    beta_max = _coerce_float(beta_params.get("max_beta"))

    market_features = pd.DataFrame(index=stock_index)
    market_features["breakout_close_vs_200d_high_pct"] = (
        close / previous_period_high - 1.0
    ) * 100.0
    market_features["breakout_20d_runup_pct"] = (close / close.shift(20) - 1.0) * 100.0
    market_features["breakout_60d_runup_pct"] = (close / close.shift(60) - 1.0) * 100.0
    market_features["breakout_120d_runup_pct"] = (
        close / close.shift(120) - 1.0
    ) * 100.0
    market_features["distance_to_upper_band_pct"] = (close / bollinger.upper - 1.0) * 100.0
    market_features["bollinger_band_width_pct"] = (
        (bollinger.upper - bollinger.lower) / bollinger.middle
    ) * 100.0
    market_features["rsi10"] = compute_rsi(
        close,
        int(entry_params.get("rsi_threshold", {}).get("period", 10)),
    )
    market_features["volume_ratio_value"] = volume_ratio_value
    market_features["volume_ratio_margin"] = (
        volume_ratio_value - volume_threshold if volume_threshold is not None else np.nan
    )
    market_features["rolling_beta_50"] = rolling_beta
    market_features["beta_distance_to_min"] = (
        rolling_beta - beta_min if beta_min is not None else np.nan
    )
    market_features["beta_distance_to_max"] = (
        beta_max - rolling_beta if beta_max is not None else np.nan
    )
    market_features["trading_value_ma_15_oku"] = trading_value_ma
    market_features["trading_value_distance_to_min_pct"] = (
        (trading_value_ma / trading_value_min - 1.0) * 100.0
        if trading_value_min is not None and trading_value_min > 0
        else np.nan
    )
    market_features["trading_value_distance_to_max_pct"] = (
        (trading_value_ma / trading_value_max - 1.0) * 100.0
        if trading_value_max is not None and trading_value_max > 0
        else np.nan
    )
    market_features["stock_volatility_20d_pct"] = (
        close.pct_change().rolling(20, min_periods=20).std() * 100.0
    )
    market_features["risk_adjusted_return_60"] = compute_risk_adjusted_return(
        close=close,
        lookback_period=60,
        ratio_type="sharpe",
    )

    fundamental_features = _build_optional_fundamental_features(
        statements_df=statements_df,
        stock_index=stock_index,
        parameters=parameters,
    )

    return {
        "market_features": market_features,
        "fundamental_features": fundamental_features,
        "market_index": market_features.index,
        "fundamental_index": fundamental_features.index,
    }


def _load_optional_statements_data(
    *,
    dataset_name: str,
    stock_code: str,
    stock_index: pd.DatetimeIndex,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    try:
        return load_statements_data(
            dataset_name,
            stock_code,
            stock_index,
            start_date=start_date,
            end_date=end_date,
            period_type="FY",
            actual_only=True,
            include_forecast_revision=True,
        )
    except ValueError as exc:
        if "No statements data found" not in str(exc):
            raise
        return pd.DataFrame(index=stock_index)


def _build_optional_fundamental_features(
    *,
    statements_df: pd.DataFrame,
    stock_index: pd.DatetimeIndex,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    fundamental_features = pd.DataFrame(index=stock_index)
    if statements_df.empty:
        fundamental_features["forward_forecast_eps"] = np.nan
        fundamental_features["adjusted_bps"] = np.nan
        fundamental_features["raw_eps"] = np.nan
        fundamental_features["adjusted_eps"] = np.nan
        fundamental_features["shares_outstanding"] = np.nan
        return fundamental_features

    signal_params = SignalParams.model_validate(parameters.get("entry_filter_params", {}))
    forecast_col = _select_forward_forecast_eps_column(
        signal_params,
        {"statements_data": statements_df},
    )
    fundamental_features["forward_forecast_eps"] = pd.to_numeric(
        statements_df[forecast_col],
        errors="coerce",
    )
    fundamental_features["adjusted_bps"] = _coerce_numeric_series(
        statements_df,
        "AdjustedBPS",
        "BPS",
    )
    fundamental_features["raw_eps"] = _coerce_numeric_series(statements_df, "EPS")
    fundamental_features["adjusted_eps"] = _coerce_numeric_series(
        statements_df,
        "AdjustedEPS",
    )
    fundamental_features["shares_outstanding"] = _coerce_numeric_series(
        statements_df,
        "SharesOutstanding",
    )
    return fundamental_features


def _load_topix_close(
    *,
    dataset_name: str,
    start_date: str,
    end_date: str,
) -> pd.Series:
    topix_df = load_topix_data(dataset_name, start_date=start_date, end_date=end_date)
    return pd.to_numeric(topix_df["Close"], errors="coerce").astype(float)


def _build_feature_bucket_summary_df(
    *,
    enriched_trade_df: pd.DataFrame,
    quantile_bucket_count: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for window_label, window_frame in enriched_trade_df.groupby("window_label", dropna=False):
        total_trade_count = len(window_frame)
        for feature_name in _FEATURE_COLUMNS:
            valid = window_frame.dropna(subset=[feature_name]).copy()
            if valid.empty:
                continue
            bucket_count = min(quantile_bucket_count, len(valid))
            valid["bucket_rank"] = _assign_quantile_bucket(
                pd.to_numeric(valid[feature_name], errors="coerce"),
                bucket_count=bucket_count,
            )
            for bucket_rank, bucket_frame in valid.groupby("bucket_rank", dropna=False):
                metrics = _build_trade_metrics(
                    bucket_frame,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                bucket_rank_int = int(float(cast(Any, bucket_rank)))
                rows.append(
                    {
                        "window_label": window_label,
                        "feature_name": feature_name,
                        "bucket_rank": bucket_rank_int,
                        "bucket_count": bucket_count,
                        "bucket_label": f"Q{bucket_rank_int}/{bucket_count}",
                        "coverage_pct": (
                            (metrics["trade_count"] / total_trade_count) * 100.0
                            if total_trade_count > 0
                            else np.nan
                        ),
                        "feature_min": float(
                            pd.to_numeric(bucket_frame[feature_name], errors="coerce").min()
                        ),
                        "feature_median": float(
                            pd.to_numeric(
                                bucket_frame[feature_name],
                                errors="coerce",
                            ).median()
                        ),
                        "feature_max": float(
                            pd.to_numeric(bucket_frame[feature_name], errors="coerce").max()
                        ),
                        **metrics,
                    }
                )
    return pd.DataFrame(rows)


def _build_overlay_candidate_summary_df(
    *,
    enriched_trade_df: pd.DataFrame,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame in _iter_overlay_groups(enriched_trade_df):
        total_trade_count = len(group_frame)
        thresholds = _build_group_thresholds(group_frame)
        for candidate in _overlay_candidates(thresholds):
            selected_mask = candidate.predicate(group_frame).fillna(False).astype(bool)
            selected = group_frame[selected_mask].copy()
            metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    **group_payload,
                    "candidate_name": candidate.name,
                    "candidate_family": candidate.family,
                    "candidate_description": candidate.description,
                    "trade_count": metrics["trade_count"],
                    "coverage_pct": (
                        (metrics["trade_count"] / total_trade_count) * 100.0
                        if total_trade_count > 0
                        else np.nan
                    ),
                    **metrics,
                }
            )

    summary_df = _sort_market_scope_summary(pd.DataFrame(rows))
    if summary_df.empty:
        return summary_df
    baseline_df = summary_df[summary_df["candidate_name"] == "baseline_all"][
        [
            "dataset_name",
            "window_label",
            "market_scope",
            "avg_trade_return_pct",
            "win_rate_pct",
            "severe_loss_rate_pct",
            "worst_trade_return_pct",
        ]
    ].rename(
        columns={
            "avg_trade_return_pct": "baseline_avg_trade_return_pct",
            "win_rate_pct": "baseline_win_rate_pct",
            "severe_loss_rate_pct": "baseline_severe_loss_rate_pct",
            "worst_trade_return_pct": "baseline_worst_trade_return_pct",
        }
    )
    summary_df = summary_df.merge(
        baseline_df,
        on=["dataset_name", "window_label", "market_scope"],
        how="left",
    )
    summary_df["delta_avg_trade_return_pct"] = (
        summary_df["avg_trade_return_pct"] - summary_df["baseline_avg_trade_return_pct"]
    )
    summary_df["delta_win_rate_pct"] = (
        summary_df["win_rate_pct"] - summary_df["baseline_win_rate_pct"]
    )
    summary_df["delta_severe_loss_rate_pct"] = (
        summary_df["severe_loss_rate_pct"] - summary_df["baseline_severe_loss_rate_pct"]
    )
    return _sort_market_scope_summary(summary_df)


def _iter_overlay_groups(
    frame: pd.DataFrame,
) -> list[tuple[dict[str, Any], pd.DataFrame]]:
    if frame.empty:
        return []
    rows: list[tuple[dict[str, Any], pd.DataFrame]] = []
    for window_label, base_group in frame.groupby("window_label", dropna=False, sort=False):
        rows.append(
            (
                {
                    "dataset_name": frame["dataset_name"].iloc[0]
                    if "dataset_name" in frame.columns
                    else "unknown",
                    "window_label": window_label,
                    "market_scope": "all",
                },
                base_group.copy(),
            )
        )
        for market_scope, market_group in base_group.groupby(
            "market_scope",
            dropna=False,
            sort=False,
        ):
            rows.append(
                (
                    {
                        "dataset_name": frame["dataset_name"].iloc[0]
                        if "dataset_name" in frame.columns
                        else "unknown",
                        "window_label": window_label,
                        "market_scope": str(market_scope),
                    },
                    market_group.copy(),
                )
            )
    return rows


def _build_group_thresholds(frame: pd.DataFrame) -> dict[str, float]:
    return {
        "breakout_60d_runup_q80": _quantile(frame, "breakout_60d_runup_pct", 0.80),
        "breakout_120d_runup_q80": _quantile(frame, "breakout_120d_runup_pct", 0.80),
        "rsi_q80": _quantile(frame, "rsi10", 0.80),
        "volume_ratio_q80": _quantile(frame, "volume_ratio_value", 0.80),
        "topix_60d_q50": _quantile(frame, "topix_return_60d_pct", 0.50),
        "trading_value_q20": _quantile(frame, "trading_value_ma_15_oku", 0.20),
        "beta_q80": _quantile(frame, "rolling_beta_50", 0.80),
    }


def _overlay_candidates(thresholds: dict[str, float]) -> tuple[_OverlayCandidate, ...]:
    return (
        _OverlayCandidate(
            name="baseline_all",
            family="baseline",
            description="No additional overlay; keep every range-break trade.",
            predicate=lambda df: pd.Series(True, index=df.index, dtype=bool),
        ),
        _OverlayCandidate(
            name="breakout_not_extended_60d",
            family="breakout_maturity",
            description="Keep trades below the within-group Q80 60-day run-up.",
            predicate=lambda df: _safe_le(
                df,
                "breakout_60d_runup_pct",
                thresholds["breakout_60d_runup_q80"],
            ),
        ),
        _OverlayCandidate(
            name="breakout_not_extended_120d",
            family="breakout_maturity",
            description="Keep trades below the within-group Q80 120-day run-up.",
            predicate=lambda df: _safe_le(
                df,
                "breakout_120d_runup_pct",
                thresholds["breakout_120d_runup_q80"],
            ),
        ),
        _OverlayCandidate(
            name="not_rsi_q80",
            family="overheat",
            description="Keep trades below the within-group Q80 RSI.",
            predicate=lambda df: _safe_le(df, "rsi10", thresholds["rsi_q80"]),
        ),
        _OverlayCandidate(
            name="volume_not_extreme_q80",
            family="volume",
            description="Keep trades below the within-group Q80 volume ratio.",
            predicate=lambda df: _safe_le(
                df,
                "volume_ratio_value",
                thresholds["volume_ratio_q80"],
            ),
        ),
        _OverlayCandidate(
            name="topix_60d_positive",
            family="market",
            description="Keep trades when TOPIX 60-day return is positive.",
            predicate=lambda df: _safe_gt(df, "topix_return_60d_pct", 0.0),
        ),
        _OverlayCandidate(
            name="topix_above_sma200",
            family="market",
            description="Keep trades when TOPIX is above its 200-day average.",
            predicate=lambda df: _safe_gt(df, "topix_close_vs_sma200_pct", 0.0),
        ),
        _OverlayCandidate(
            name="topix_supportive_combo",
            family="market",
            description="Keep trades when TOPIX 60-day return is positive and above SMA200.",
            predicate=lambda df: _safe_gt(df, "topix_return_60d_pct", 0.0)
            & _safe_gt(df, "topix_close_vs_sma200_pct", 0.0),
        ),
        _OverlayCandidate(
            name="not_low_liquidity_q20",
            family="liquidity",
            description="Keep trades above the within-group Q20 trading-value average.",
            predicate=lambda df: _safe_ge(
                df,
                "trading_value_ma_15_oku",
                thresholds["trading_value_q20"],
            ),
        ),
        _OverlayCandidate(
            name="not_high_beta_q80",
            family="beta",
            description="Keep trades below the within-group Q80 rolling beta.",
            predicate=lambda df: _safe_le(df, "rolling_beta_50", thresholds["beta_q80"]),
        ),
        _OverlayCandidate(
            name="overheat_overlap_ge2_excluded",
            family="bad_tail",
            description="Keep trades that do not have 2+ overheat flags among 60d run-up, 120d run-up, RSI, and volume ratio.",
            predicate=lambda df: _overheat_overlap_count(df, thresholds) < 2,
        ),
        _OverlayCandidate(
            name="supportive_topix_and_no_overheat_overlap",
            family="combo",
            description="Keep trades in supportive TOPIX regimes while excluding 2+ overheat overlaps.",
            predicate=lambda df: _safe_gt(df, "topix_return_60d_pct", 0.0)
            & _safe_gt(df, "topix_close_vs_sma200_pct", 0.0)
            & (_overheat_overlap_count(df, thresholds) < 2),
        ),
    )


def _overheat_overlap_count(frame: pd.DataFrame, thresholds: dict[str, float]) -> pd.Series:
    flags = [
        _safe_ge(frame, "breakout_60d_runup_pct", thresholds["breakout_60d_runup_q80"]),
        _safe_ge(frame, "breakout_120d_runup_pct", thresholds["breakout_120d_runup_q80"]),
        _safe_ge(frame, "rsi10", thresholds["rsi_q80"]),
        _safe_ge(frame, "volume_ratio_value", thresholds["volume_ratio_q80"]),
    ]
    return pd.concat(flags, axis=1).sum(axis=1)


def _build_return_bucket_summary_df(
    *,
    enriched_trade_df: pd.DataFrame,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame in _iter_overlay_groups(enriched_trade_df):
        frame = group_frame.copy()
        returns = pd.to_numeric(frame["trade_return_pct"], errors="coerce")
        q20 = returns.quantile(0.20)
        q80 = returns.quantile(0.80)
        buckets = (
            ("low_return_q20", returns <= q20),
            ("middle_return_q20_q80", (returns > q20) & (returns < q80)),
            ("high_return_q80", returns >= q80),
        )
        for bucket_name, mask in buckets:
            selected = frame[mask.fillna(False)].copy()
            metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    **group_payload,
                    "return_bucket": bucket_name,
                    **metrics,
                    "median_breakout_60d_runup_pct": _numeric_median(
                        selected,
                        "breakout_60d_runup_pct",
                    ),
                    "median_rsi10": _numeric_median(selected, "rsi10"),
                    "median_volume_ratio_value": _numeric_median(
                        selected,
                        "volume_ratio_value",
                    ),
                    "median_rolling_beta_50": _numeric_median(
                        selected,
                        "rolling_beta_50",
                    ),
                    "median_topix_return_60d_pct": _numeric_median(
                        selected,
                        "topix_return_60d_pct",
                    ),
                    "median_trading_value_ma_15_oku": _numeric_median(
                        selected,
                        "trading_value_ma_15_oku",
                    ),
                    "median_pbr": _numeric_median(selected, "pbr"),
                    "median_forward_per": _numeric_median(selected, "forward_per"),
                    "median_market_cap_bil_jpy": _numeric_median(
                        selected,
                        "market_cap_bil_jpy",
                    ),
                }
            )
    return _sort_market_scope_summary(pd.DataFrame(rows))


def _pick_market_feature_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {column: np.nan for column in _FEATURE_COLUMNS if not column.startswith("topix_")}
    return {
        key: row.get(key, np.nan)
        for key in _FEATURE_COLUMNS
        if not key.startswith("topix_")
    }


def _pick_topix_feature_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    keys = (
        "topix_return_20d_pct",
        "topix_return_60d_pct",
        "topix_risk_adjusted_return_60",
        "topix_close_vs_sma200_pct",
    )
    if row is None:
        return {key: np.nan for key in keys}
    return {key: row.get(key, np.nan) for key in keys}


def _quantile(frame: pd.DataFrame, column: str, q: float) -> float:
    value = pd.to_numeric(frame[column], errors="coerce").quantile(q)
    return float(value) if pd.notna(value) else math.nan


def _build_summary_markdown(result: RangeBreakTradeArchetypeDecompositionResult) -> str:
    scenario = _format_markdown_table(
        result.scenario_summary_df[
            [
                "window_label",
                "trade_count",
                "avg_trade_return_pct",
                "median_trade_return_pct",
            ]
        ]
    )
    return "\n".join(
        [
            "# Range Break Trade Archetype Decomposition",
            "",
            "## Scope",
            "",
            f"- Strategy: `{result.strategy_name}`",
            f"- Dataset: `{result.dataset_name}`",
            f"- Holdout window: `{result.holdout_months}` months",
            f"- Severe loss threshold: `{result.severe_loss_threshold_pct:.1f}%`",
            "- Market-derived features are evaluated at the previous trading session close to stay PIT-safe.",
            "",
            "## Scenario Summary",
            "",
            scenario,
            "",
            "## Artifact Tables",
            "",
            "- `dataset_summary_df`",
            "- `scenario_summary_df`",
            "- `trade_ledger_df`",
            "- `enriched_trade_df`",
            "- `market_scope_summary_df`",
            "- `feature_bucket_summary_df`",
            "- `overlay_candidate_summary_df`",
            "- `value_feature_bucket_summary_df`",
            "- `value_overlay_candidate_summary_df`",
            "- `return_bucket_summary_df`",
        ]
    )


def _format_markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in frame.to_dict(orient="records"):
        values = [_format_markdown_cell(row.get(column)) for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _format_markdown_cell(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _build_published_summary(result: RangeBreakTradeArchetypeDecompositionResult) -> dict[str, Any]:
    return {
        "strategyName": result.strategy_name,
        "datasetName": result.dataset_name,
        "holdoutMonths": result.holdout_months,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "tradeCount": int(len(result.trade_ledger_df)),
    }


__all__ = [
    "DEFAULT_DATASET_NAME",
    "DEFAULT_HOLDOUT_MONTHS",
    "DEFAULT_QUANTILE_BUCKET_COUNT",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "DEFAULT_STRATEGY_NAME",
    "RANGE_BREAK_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID",
    "RangeBreakTradeArchetypeDecompositionResult",
    "get_range_break_trade_archetype_decomposition_bundle_path_for_run_id",
    "get_range_break_trade_archetype_decomposition_latest_bundle_path",
    "load_range_break_trade_archetype_decomposition_bundle",
    "run_range_break_trade_archetype_decomposition",
    "write_range_break_trade_archetype_decomposition_bundle",
]
