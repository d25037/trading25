"""Trade-archetype decomposition for the current forward EPS production strategy."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, cast

import duckdb
import numpy as np
import pandas as pd

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
from src.shared.utils.market_code_alias import normalize_market_scope
from src.domains.backtest.core.runner import BacktestRunner
from src.domains.strategy.indicators import (
    compute_risk_adjusted_return,
    compute_rsi,
)
from src.domains.strategy.indicators.calculations import compute_volume_mas
from src.domains.strategy.signals.registry import (
    _select_forward_base_eps_column,
    _select_forward_forecast_eps_column,
)
from src.infrastructure.db.market.universe_resolver import UNIVERSE_PRESET_NAMES
from src.infrastructure.data_access.loaders import (
    load_statements_data,
    load_stock_data,
    load_topix_data,
)
from src.infrastructure.data_access.mode import data_access_mode_context
from src.shared.models.signals import SignalParams
from src.shared.paths import get_data_dir

RatioType = Literal["sharpe", "sortino"]
FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID = (
    "strategy-audit/forward-eps-trade-archetype-decomposition"
)
DEFAULT_STRATEGY_NAME = "production/forward_eps_driven"
DEFAULT_DATASET_NAME = "primeExTopix500_20260325"
DEFAULT_HOLDOUT_MONTHS = 6
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_QUANTILE_BUCKET_COUNT = 5
_WARMUP_CALENDAR_DAYS = 420
_FEATURE_COLUMNS = (
    "forward_eps_growth_value",
    "forward_eps_growth_margin",
    "risk_adjusted_return_value",
    "risk_adjusted_return_margin",
    "volume_ratio_value",
    "volume_ratio_margin",
    "rsi10",
    "stock_return_20d_pct",
    "stock_return_60d_pct",
    "stock_volatility_20d_pct",
    "days_since_disclosed",
    "topix_return_20d_pct",
    "topix_return_60d_pct",
    "topix_risk_adjusted_return_60",
    "topix_close_vs_sma200_pct",
)
_VALUE_FEATURE_COLUMNS = (
    "pbr",
    "forward_per",
    "market_cap_bil_jpy",
    "value_composite_score",
)
_VALUE_RAW_COLUMNS = (
    "pbr",
    "forward_per",
    "market_cap_bil_jpy",
)
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth", "unknown")


@dataclass(frozen=True)
class ForwardEpsTradeArchetypeDecompositionResult:
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
    value_feature_bucket_summary_df: pd.DataFrame
    overlay_candidate_summary_df: pd.DataFrame
    value_overlay_candidate_summary_df: pd.DataFrame


@dataclass(frozen=True)
class _OverlayCandidate:
    name: str
    family: str
    description: str
    predicate: Callable[[pd.DataFrame], pd.Series]


def run_forward_eps_trade_archetype_decomposition(
    *,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    dataset_name: str = DEFAULT_DATASET_NAME,
    holdout_months: int = DEFAULT_HOLDOUT_MONTHS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    quantile_bucket_count: int = DEFAULT_QUANTILE_BUCKET_COUNT,
) -> ForwardEpsTradeArchetypeDecompositionResult:
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
        raise RuntimeError("trade archetype decomposition produced no trades")

    runner = BacktestRunner()
    parameters = runner.build_parameters_for_strategy(strategy_name)
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
    value_feature_bucket_summary_df = _build_value_feature_bucket_summary_df(
        enriched_trade_df=enriched_trade_df,
        quantile_bucket_count=quantile_bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    overlay_candidate_summary_df = _build_overlay_candidate_summary_df(
        enriched_trade_df=enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    value_overlay_candidate_summary_df = _build_value_overlay_summary_df(
        enriched_trade_df=enriched_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )

    return ForwardEpsTradeArchetypeDecompositionResult(
        db_path="multi://forward-eps-trade-archetype-decomposition",
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
        value_feature_bucket_summary_df=value_feature_bucket_summary_df,
        overlay_candidate_summary_df=overlay_candidate_summary_df,
        value_overlay_candidate_summary_df=value_overlay_candidate_summary_df,
    )


def write_forward_eps_trade_archetype_decomposition_bundle(
    result: ForwardEpsTradeArchetypeDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_forward_eps_trade_archetype_decomposition",
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
            "value_feature_bucket_summary_df": result.value_feature_bucket_summary_df,
            "overlay_candidate_summary_df": result.overlay_candidate_summary_df,
            "value_overlay_candidate_summary_df": result.value_overlay_candidate_summary_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_forward_eps_trade_archetype_decomposition_bundle(
    bundle_path: str | Path,
) -> ForwardEpsTradeArchetypeDecompositionResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return ForwardEpsTradeArchetypeDecompositionResult(
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
        market_scope_summary_df=tables.get("market_scope_summary_df", pd.DataFrame()),
        feature_bucket_summary_df=tables["feature_bucket_summary_df"],
        value_feature_bucket_summary_df=tables.get(
            "value_feature_bucket_summary_df",
            pd.DataFrame(),
        ),
        overlay_candidate_summary_df=tables["overlay_candidate_summary_df"],
        value_overlay_candidate_summary_df=tables.get(
            "value_overlay_candidate_summary_df",
            pd.DataFrame(),
        ),
    )


def get_forward_eps_trade_archetype_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_forward_eps_trade_archetype_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID,
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
        enriched["trade_return_pct"], errors="coerce"
    ) > 0

    entry_dates = pd.to_datetime(enriched["entry_date"], errors="coerce").dropna()
    if entry_dates.empty:
        raise RuntimeError("trade archetype decomposition has no valid entry dates")
    load_start_date = (
        entry_dates.min() - pd.Timedelta(days=_WARMUP_CALENDAR_DAYS)
    ).strftime("%Y-%m-%d")
    load_end_date = entry_dates.max().strftime("%Y-%m-%d")

    signal_params = SignalParams.model_validate(parameters.get("entry_filter_params", {}))
    symbol_feature_cache: dict[str, dict[str, Any]] = {}
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
        for trade in enriched.itertuples(index=False):
            symbol = str(trade.symbol)
            symbol_features = symbol_feature_cache.get(symbol)
            if symbol_features is None:
                symbol_features = _build_symbol_feature_payload(
                    dataset_name=dataset_name,
                    stock_code=symbol,
                    start_date=load_start_date,
                    end_date=load_end_date,
                    signal_params=signal_params,
                    parameters=parameters,
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
            disclosed_date = _coerce_timestamp(
                None if fundamental_row is None else fundamental_row.get("disclosed_date")
            )
            entry_price = _coerce_float(getattr(trade, "avg_entry_price", None))
            value_features = _build_value_feature_row(
                fundamental_row=fundamental_row,
                entry_price=entry_price,
            )
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
                    "days_since_disclosed": (
                        float((entry_date.normalize() - disclosed_date.normalize()).days)
                        if disclosed_date is not None
                        else np.nan
                    ),
                    "same_day_disclosed": bool(
                        disclosed_date is not None
                        and disclosed_date.normalize() == entry_date.normalize()
                    ),
                    "fresh_disclosure_3d": bool(
                        disclosed_date is not None
                        and (entry_date.normalize() - disclosed_date.normalize()).days <= 3
                    ),
                    "fresh_disclosure_5d": bool(
                        disclosed_date is not None
                        and (entry_date.normalize() - disclosed_date.normalize()).days <= 5
                    ),
                    **_pick_market_feature_columns(market_row),
                    **_pick_topix_feature_columns(topix_row),
                    **_pick_fundamental_feature_columns(fundamental_row),
                    **value_features,
                }
            )

    enriched_trade_df = pd.DataFrame(rows)
    if enriched_trade_df.empty:
        return enriched_trade_df
    return _with_value_composite_score(enriched_trade_df)


def _build_symbol_feature_payload(
    *,
    dataset_name: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    signal_params: SignalParams,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    stock_df = load_stock_data(
        dataset_name,
        stock_code,
        start_date=start_date,
        end_date=end_date,
    )
    stock_index = cast(pd.DatetimeIndex, stock_df.index)
    statements_df = load_statements_data(
        dataset_name,
        stock_code,
        stock_index,
        start_date=start_date,
        end_date=end_date,
        period_type="FY",
        actual_only=True,
        include_forecast_revision=True,
    )

    close = pd.to_numeric(stock_df["Close"], errors="coerce")
    volume = pd.to_numeric(stock_df["Volume"], errors="coerce")

    risk_ratio_type = _extract_risk_adjusted_ratio_type(parameters)
    risk_lookback_period = _extract_risk_adjusted_lookback_period(parameters)
    risk_threshold = _extract_risk_adjusted_return_threshold(parameters)
    volume_threshold = _extract_volume_ratio_above_threshold(parameters)
    volume_short_period = _extract_volume_ratio_above_short_period(parameters)
    volume_long_period = _extract_volume_ratio_above_long_period(parameters)
    volume_ma_type = _extract_volume_ratio_ma_type(parameters)
    rsi_period = _extract_rsi_period(parameters)

    short_ma, long_ma = compute_volume_mas(
        volume.astype(float),
        short_period=volume_short_period,
        long_period=volume_long_period,
        ma_type=volume_ma_type,
    )
    volume_ratio_value = short_ma / long_ma.where(long_ma != 0, np.nan)
    risk_adjusted_return_value = compute_risk_adjusted_return(
        close=close.astype(float),
        lookback_period=risk_lookback_period,
        ratio_type=risk_ratio_type,
    )
    rsi10 = compute_rsi(close.astype(float), rsi_period)

    market_features = pd.DataFrame(index=stock_index)
    market_features["risk_adjusted_return_value"] = risk_adjusted_return_value
    market_features["risk_adjusted_return_margin"] = (
        risk_adjusted_return_value - risk_threshold
        if risk_threshold is not None
        else np.nan
    )
    market_features["volume_ratio_value"] = volume_ratio_value
    market_features["volume_ratio_margin"] = (
        volume_ratio_value - volume_threshold
        if volume_threshold is not None
        else np.nan
    )
    market_features["rsi10"] = rsi10
    market_features["stock_return_20d_pct"] = (
        close.astype(float) / close.astype(float).shift(20) - 1.0
    ) * 100.0
    market_features["stock_return_60d_pct"] = (
        close.astype(float) / close.astype(float).shift(60) - 1.0
    ) * 100.0
    market_features["stock_volatility_20d_pct"] = (
        close.astype(float)
        .pct_change()
        .rolling(20, min_periods=20)
        .std()
        * 100.0
    )
    market_features["avg_trading_value_60d_mil_jpy"] = (
        close.astype(float) * volume.astype(float)
    ).rolling(60, min_periods=20).mean() / 1_000_000.0

    base_col = _select_forward_base_eps_column(
        signal_params,
        {"statements_data": statements_df},
    )
    forecast_col = _select_forward_forecast_eps_column(
        signal_params,
        {"statements_data": statements_df},
    )
    base_eps = pd.to_numeric(statements_df[base_col], errors="coerce")
    forecast_eps = pd.to_numeric(statements_df[forecast_col], errors="coerce")
    forward_eps_growth_value = (forecast_eps - base_eps) / base_eps.where(base_eps > 0)
    forward_eps_threshold = _extract_forward_eps_growth_threshold(parameters)

    fundamental_features = pd.DataFrame(index=stock_index)
    fundamental_features["forward_eps_growth_value"] = forward_eps_growth_value
    fundamental_features["forward_eps_growth_margin"] = (
        forward_eps_growth_value - forward_eps_threshold
        if forward_eps_threshold is not None
        else np.nan
    )
    fundamental_features["forward_base_eps"] = base_eps
    fundamental_features["forward_forecast_eps"] = forecast_eps
    fundamental_features["adjusted_bps"] = _coerce_numeric_series(
        statements_df,
        "AdjustedBPS",
        "BPS",
    )
    fundamental_features["raw_eps"] = _coerce_numeric_series(statements_df, "EPS")
    fundamental_features["adjusted_eps"] = _coerce_numeric_series(statements_df, "AdjustedEPS")
    fundamental_features["shares_outstanding"] = _coerce_numeric_series(
        statements_df,
        "SharesOutstanding",
    )
    fundamental_features["disclosed_date"] = pd.to_datetime(
        statements_df["DisclosedDate"], errors="coerce"
    )

    return {
        "market_features": market_features,
        "fundamental_features": fundamental_features,
        "market_index": market_features.index,
        "fundamental_index": fundamental_features.index,
    }


def _build_topix_feature_df(
    *,
    dataset_name: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    topix_df = load_topix_data(
        dataset_name,
        start_date=start_date,
        end_date=end_date,
    )
    close = pd.to_numeric(topix_df["Close"], errors="coerce").astype(float)
    sma200 = close.rolling(200, min_periods=200).mean()
    feature_df = pd.DataFrame(index=cast(pd.DatetimeIndex, topix_df.index))
    feature_df["topix_return_20d_pct"] = (close / close.shift(20) - 1.0) * 100.0
    feature_df["topix_return_60d_pct"] = (close / close.shift(60) - 1.0) * 100.0
    feature_df["topix_risk_adjusted_return_60"] = compute_risk_adjusted_return(
        close=close,
        lookback_period=60,
        ratio_type="sharpe",
    )
    feature_df["topix_close_vs_sma200_pct"] = (close / sma200 - 1.0) * 100.0
    feature_df["topix_rsi10"] = compute_rsi(close, 10)
    return feature_df


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
                bucket_rank_int = _coerce_int(bucket_rank)
                metrics = _build_trade_metrics(
                    bucket_frame,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                rows.append(
                    {
                        "window_label": window_label,
                        "feature_name": feature_name,
                        "bucket_rank": bucket_rank_int,
                        "bucket_count": bucket_count,
                        "bucket_label": f"Q{bucket_rank_int}/{bucket_count}",
                        "trade_count": metrics["trade_count"],
                        "coverage_pct": (
                            (metrics["trade_count"] / total_trade_count) * 100.0
                            if total_trade_count > 0
                            else np.nan
                        ),
                        "feature_min": float(
                            pd.to_numeric(bucket_frame[feature_name], errors="coerce").min()
                        ),
                        "feature_median": float(
                            pd.to_numeric(bucket_frame[feature_name], errors="coerce").median()
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
    for window_label, window_frame in enriched_trade_df.groupby("window_label", dropna=False):
        total_trade_count = len(window_frame)
        for candidate in _overlay_candidates():
            selected_mask = candidate.predicate(window_frame).fillna(False).astype(bool)
            selected = window_frame[selected_mask].copy()
            metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    "window_label": window_label,
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

    summary_df = pd.DataFrame(rows)
    baseline_df = summary_df[summary_df["candidate_name"] == "baseline_all"][
        [
            "window_label",
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
        on="window_label",
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
    return summary_df


def _build_market_scope_summary_df(
    *,
    enriched_trade_df: pd.DataFrame,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frame = _prepare_value_frame(enriched_trade_df)
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame in _iter_market_scope_groups(frame):
        metrics = _build_trade_metrics(
            group_frame,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        rows.append(
            {
                **group_payload,
                **metrics,
                "traded_symbol_count": int(group_frame["symbol"].nunique())
                if "symbol" in group_frame.columns
                else 0,
                "median_pbr": _numeric_median(group_frame, "pbr"),
                "median_forward_per": _numeric_median(group_frame, "forward_per"),
                "median_market_cap_bil_jpy": _numeric_median(
                    group_frame,
                    "market_cap_bil_jpy",
                ),
                "median_forward_eps_growth_value": _numeric_median(
                    group_frame,
                    "forward_eps_growth_value",
                ),
                "median_avg_trading_value_60d_mil_jpy": _numeric_median(
                    group_frame,
                    "avg_trading_value_60d_mil_jpy",
                ),
            }
        )
    return _sort_market_scope_summary(pd.DataFrame(rows))


def _build_value_feature_bucket_summary_df(
    *,
    enriched_trade_df: pd.DataFrame,
    quantile_bucket_count: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frame = _prepare_value_frame(enriched_trade_df)
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame_raw in _iter_market_scope_groups(frame):
        group_frame = _with_group_value_composite_score(group_frame_raw)
        total_trade_count = len(group_frame)
        for feature_name in _VALUE_FEATURE_COLUMNS:
            if feature_name not in group_frame.columns:
                continue
            valid = group_frame.dropna(subset=[feature_name]).copy()
            if valid.empty:
                continue
            bucket_count = min(quantile_bucket_count, len(valid))
            valid["bucket_rank"] = _assign_value_quantile_bucket(
                pd.to_numeric(valid[feature_name], errors="coerce"),
                bucket_count=bucket_count,
            )
            for bucket_rank, bucket_frame in valid.groupby("bucket_rank", dropna=False):
                bucket_rank_int = _coerce_int(bucket_rank)
                metrics = _build_trade_metrics(
                    bucket_frame,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                rows.append(
                    {
                        **group_payload,
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
                            pd.to_numeric(bucket_frame[feature_name], errors="coerce").median()
                        ),
                        "feature_max": float(
                            pd.to_numeric(bucket_frame[feature_name], errors="coerce").max()
                        ),
                        **metrics,
                    }
                )
    return _sort_market_scope_summary(pd.DataFrame(rows))


def _build_value_overlay_summary_df(
    *,
    enriched_trade_df: pd.DataFrame,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frame = _prepare_value_frame(enriched_trade_df)
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame_raw in _iter_market_scope_groups(frame):
        group_frame = _with_group_value_composite_score(group_frame_raw)
        total_trade_count = len(group_frame)
        predicates = _build_value_overlay_predicates(group_frame)
        for candidate_name, candidate_description, selected_mask in predicates:
            selected = group_frame[selected_mask.fillna(False).astype(bool)].copy()
            metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    **group_payload,
                    "candidate_name": candidate_name,
                    "candidate_family": "value",
                    "candidate_description": candidate_description,
                    "trade_count": metrics["trade_count"],
                    "coverage_pct": (
                        (metrics["trade_count"] / total_trade_count) * 100.0
                        if total_trade_count > 0
                        else np.nan
                    ),
                    "median_pbr": _numeric_median(selected, "pbr"),
                    "median_forward_per": _numeric_median(selected, "forward_per"),
                    "median_market_cap_bil_jpy": _numeric_median(
                        selected,
                        "market_cap_bil_jpy",
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


def _build_value_overlay_predicates(
    frame: pd.DataFrame,
) -> tuple[tuple[str, str, pd.Series], ...]:
    low_pbr = _value_low_quantile_mask(frame, "pbr")
    low_forward_per = _value_low_quantile_mask(frame, "forward_per")
    small_market_cap = _value_low_quantile_mask(frame, "market_cap_bil_jpy")
    value_composite = _value_low_quantile_mask(frame, "value_composite_score")
    baseline = pd.Series(True, index=frame.index, dtype=bool)
    return (
        (
            "baseline_all",
            "No additional value overlay; keep every forward_eps_driven trade.",
            baseline,
        ),
        (
            "low_pbr_q1",
            "Keep the lowest-third PBR trades inside the same dataset/window/market scope.",
            low_pbr,
        ),
        (
            "low_forward_per_q1",
            "Keep the lowest-third forward PER trades inside the same dataset/window/market scope.",
            low_forward_per,
        ),
        (
            "small_market_cap_q1",
            "Keep the smallest-third market-cap trades inside the same dataset/window/market scope.",
            small_market_cap,
        ),
        (
            "value_composite_q1",
            "Keep the lowest-third composite of PBR, forward PER, and market cap.",
            value_composite,
        ),
        (
            "value_core_low_pbr_low_fper_small_cap",
            "Keep only trades that are simultaneously low PBR, low forward PER, and small cap.",
            low_pbr & low_forward_per & small_market_cap,
        ),
    )


def _prepare_value_frame(enriched_trade_df: pd.DataFrame) -> pd.DataFrame:
    frame = enriched_trade_df.copy()
    if frame.empty:
        return frame
    if "dataset_name" not in frame.columns:
        frame["dataset_name"] = "unknown"
    if "window_label" not in frame.columns:
        frame["window_label"] = "full"
    if "market_scope" not in frame.columns:
        frame["market_scope"] = "unknown"
    for column in _VALUE_RAW_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan
    return _with_value_composite_score(frame)


def _with_value_composite_score(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    group_columns = [
        column
        for column in ("dataset_name", "window_label", "market_scope")
        if column in frame.columns
    ]
    if not group_columns:
        return _with_group_value_composite_score(frame)
    groups = [
        _with_group_value_composite_score(group)
        for _, group in frame.groupby(group_columns, dropna=False, sort=False)
    ]
    if not groups:
        return frame.copy()
    return pd.concat(groups, ignore_index=True)


def _with_group_value_composite_score(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()
    score_parts: list[pd.Series] = []
    for column in _VALUE_RAW_COLUMNS:
        if column not in scored.columns:
            continue
        values = pd.to_numeric(scored[column], errors="coerce")
        score_parts.append(values.rank(method="average", pct=True))
    if score_parts:
        scored["value_composite_score"] = pd.concat(score_parts, axis=1).mean(axis=1)
    else:
        scored["value_composite_score"] = np.nan
    return scored


def _iter_market_scope_groups(
    frame: pd.DataFrame,
) -> list[tuple[dict[str, Any], pd.DataFrame]]:
    if frame.empty:
        return []
    group_columns = ["dataset_name", "window_label"]
    rows: list[tuple[dict[str, Any], pd.DataFrame]] = []
    for keys, base_group in frame.groupby(group_columns, dropna=False, sort=False):
        dataset_name, window_label = cast(tuple[Any, Any], keys)
        base_payload = {
            "dataset_name": dataset_name,
            "window_label": window_label,
            "market_scope": "all",
        }
        rows.append((base_payload, base_group.copy()))
        for market_scope, market_group in base_group.groupby(
            "market_scope",
            dropna=False,
            sort=False,
        ):
            rows.append(
                (
                    {
                        "dataset_name": dataset_name,
                        "window_label": window_label,
                        "market_scope": str(market_scope),
                    },
                    market_group.copy(),
                )
            )
    return rows


def _value_low_quantile_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    values = _get_numeric_column(frame, column)
    valid = values.dropna()
    if valid.empty:
        return pd.Series(False, index=frame.index, dtype=bool)
    threshold = valid.quantile(1.0 / 3.0)
    return values <= threshold


def _numeric_median(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return np.nan
    value = pd.to_numeric(frame[column], errors="coerce").median()
    return float(value) if pd.notna(value) else np.nan


def _sort_market_scope_summary(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sorted_frame = frame.copy()
    if "market_scope" in sorted_frame.columns:
        sorted_frame["_market_scope_order"] = sorted_frame["market_scope"].map(
            {scope: idx for idx, scope in enumerate(_MARKET_SCOPE_ORDER)}
        ).fillna(len(_MARKET_SCOPE_ORDER))
    sort_columns = [
        column
        for column in (
            "dataset_name",
            "window_label",
            "_market_scope_order",
            "market_scope",
            "feature_name",
            "candidate_name",
            "bucket_rank",
        )
        if column in sorted_frame.columns
    ]
    if sort_columns:
        sorted_frame = sorted_frame.sort_values(sort_columns, kind="stable")
    return sorted_frame.drop(columns=["_market_scope_order"], errors="ignore").reset_index(
        drop=True
    )


def _overlay_candidates() -> tuple[_OverlayCandidate, ...]:
    return (
        _OverlayCandidate(
            name="baseline_all",
            family="baseline",
            description="No additional overlay; keep every trade.",
            predicate=lambda df: pd.Series(True, index=df.index, dtype=bool),
        ),
        _OverlayCandidate(
            name="fresh_disclosure_3d",
            family="freshness",
            description="Only keep trades within 3 calendar days of the latest disclosure.",
            predicate=lambda df: _safe_le(df, "days_since_disclosed", 3),
        ),
        _OverlayCandidate(
            name="fresh_disclosure_5d",
            family="freshness",
            description="Only keep trades within 5 calendar days of the latest disclosure.",
            predicate=lambda df: _safe_le(df, "days_since_disclosed", 5),
        ),
        _OverlayCandidate(
            name="topix_20d_positive",
            family="market",
            description="Only keep trades when TOPIX 20-day return is positive.",
            predicate=lambda df: _safe_gt(df, "topix_return_20d_pct", 0.0),
        ),
        _OverlayCandidate(
            name="topix_60d_positive",
            family="market",
            description="Only keep trades when TOPIX 60-day return is positive.",
            predicate=lambda df: _safe_gt(df, "topix_return_60d_pct", 0.0),
        ),
        _OverlayCandidate(
            name="topix_supportive_combo",
            family="market",
            description="Only keep trades when TOPIX is above its 200DMA and 60-day return is positive.",
            predicate=lambda df: _safe_gt(df, "topix_return_60d_pct", 0.0)
            & _safe_gt(df, "topix_close_vs_sma200_pct", 0.0),
        ),
        _OverlayCandidate(
            name="feps_margin_ge_0_10",
            family="signal_strength",
            description="Only keep trades with forward EPS growth margin >= 0.10.",
            predicate=lambda df: _safe_ge(df, "forward_eps_growth_margin", 0.10),
        ),
        _OverlayCandidate(
            name="risk_margin_ge_0_30",
            family="signal_strength",
            description="Only keep trades with risk-adjusted-return margin >= 0.30.",
            predicate=lambda df: _safe_ge(df, "risk_adjusted_return_margin", 0.30),
        ),
        _OverlayCandidate(
            name="volume_margin_ge_0_20",
            family="signal_strength",
            description="Only keep trades with volume-ratio margin >= 0.20.",
            predicate=lambda df: _safe_ge(df, "volume_ratio_margin", 0.20),
        ),
        _OverlayCandidate(
            name="fresh_3d_and_topix_supportive",
            family="combo",
            description="Keep only fresh disclosures inside a supportive TOPIX regime.",
            predicate=lambda df: _safe_le(df, "days_since_disclosed", 3)
            & _safe_gt(df, "topix_return_60d_pct", 0.0)
            & _safe_gt(df, "topix_close_vs_sma200_pct", 0.0),
        ),
        _OverlayCandidate(
            name="fresh_3d_and_risk_margin_ge_0_30",
            family="combo",
            description="Keep only fresh disclosures with strong stock-level risk-adjusted-return margin.",
            predicate=lambda df: _safe_le(df, "days_since_disclosed", 3)
            & _safe_ge(df, "risk_adjusted_return_margin", 0.30),
        ),
    )


def _build_trade_metrics(
    frame: pd.DataFrame,
    *,
    severe_loss_threshold_pct: float,
) -> dict[str, Any]:
    trade_returns = pd.to_numeric(frame["trade_return_pct"], errors="coerce")
    trade_returns = trade_returns.dropna()
    trade_count = int(len(trade_returns))
    if trade_count == 0:
        return {
            "trade_count": 0,
            "avg_trade_return_pct": np.nan,
            "median_trade_return_pct": np.nan,
            "win_rate_pct": np.nan,
            "severe_loss_rate_pct": np.nan,
            "worst_trade_return_pct": np.nan,
            "p10_trade_return_pct": np.nan,
        }
    severe_loss_rate = float((trade_returns <= severe_loss_threshold_pct).mean() * 100.0)
    return {
        "trade_count": trade_count,
        "avg_trade_return_pct": float(trade_returns.mean()),
        "median_trade_return_pct": float(trade_returns.median()),
        "win_rate_pct": float((trade_returns > 0).mean() * 100.0),
        "severe_loss_rate_pct": severe_loss_rate,
        "worst_trade_return_pct": float(trade_returns.min()),
        "p10_trade_return_pct": float(trade_returns.quantile(0.10)),
    }


def _assign_quantile_bucket(series: pd.Series, *, bucket_count: int) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    rank_pct = valid.rank(method="first", pct=True)
    bucket = (rank_pct * bucket_count).apply(np.ceil).clip(1, bucket_count)
    return cast(pd.Series, bucket.astype(int))


def _assign_value_quantile_bucket(series: pd.Series, *, bucket_count: int) -> pd.Series:
    valid = pd.to_numeric(series, errors="coerce")
    ranks = valid.rank(method="first", ascending=True)
    valid_count = int(valid.notna().sum())
    if valid_count == 0:
        return pd.Series(np.nan, index=series.index)
    bucket = (((ranks - 1) * bucket_count / valid_count).apply(np.floor) + 1).clip(
        1,
        bucket_count,
    )
    return cast(pd.Series, bucket.astype(int))


def _resolve_previous_index_value(
    index: pd.DatetimeIndex,
    target: pd.Timestamp,
) -> pd.Timestamp | None:
    position = index.searchsorted(target, side="left") - 1
    if position < 0:
        return None
    return cast(pd.Timestamp, index[position])


def _resolve_last_index_value(
    index: pd.DatetimeIndex,
    target: pd.Timestamp,
) -> pd.Timestamp | None:
    position = index.searchsorted(target, side="right") - 1
    if position < 0:
        return None
    return cast(pd.Timestamp, index[position])


def _extract_row(frame: pd.DataFrame, ts: pd.Timestamp | None) -> dict[str, Any] | None:
    if ts is None:
        return None
    if ts not in frame.index:
        return None
    row = frame.loc[ts]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return cast(dict[str, Any], row.to_dict())


def _pick_market_feature_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            "risk_adjusted_return_value": np.nan,
            "risk_adjusted_return_margin": np.nan,
            "volume_ratio_value": np.nan,
            "volume_ratio_margin": np.nan,
            "rsi10": np.nan,
            "stock_return_20d_pct": np.nan,
            "stock_return_60d_pct": np.nan,
            "stock_volatility_20d_pct": np.nan,
            "avg_trading_value_60d_mil_jpy": np.nan,
        }
    return {
        key: row.get(key, np.nan)
        for key in (
            "risk_adjusted_return_value",
            "risk_adjusted_return_margin",
            "volume_ratio_value",
            "volume_ratio_margin",
            "rsi10",
            "stock_return_20d_pct",
            "stock_return_60d_pct",
            "stock_volatility_20d_pct",
            "avg_trading_value_60d_mil_jpy",
        )
    }


def _pick_topix_feature_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            "topix_return_20d_pct": np.nan,
            "topix_return_60d_pct": np.nan,
            "topix_risk_adjusted_return_60": np.nan,
            "topix_close_vs_sma200_pct": np.nan,
        }
    return {
        key: row.get(key, np.nan)
        for key in (
            "topix_return_20d_pct",
            "topix_return_60d_pct",
            "topix_risk_adjusted_return_60",
            "topix_close_vs_sma200_pct",
        )
    }


def _pick_fundamental_feature_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {
            "forward_eps_growth_value": np.nan,
            "forward_eps_growth_margin": np.nan,
        }
    return {
        key: row.get(key, np.nan)
        for key in (
            "forward_eps_growth_value",
            "forward_eps_growth_margin",
        )
    }


def _build_value_feature_row(
    *,
    fundamental_row: dict[str, Any] | None,
    entry_price: float | None,
) -> dict[str, Any]:
    if fundamental_row is None or entry_price is None:
        return {
            "pbr": np.nan,
            "forward_per": np.nan,
            "market_cap_bil_jpy": np.nan,
            "adjusted_share_baseline": np.nan,
        }

    adjusted_bps = _coerce_float(fundamental_row.get("adjusted_bps"))
    forward_forecast_eps = _coerce_float(fundamental_row.get("forward_forecast_eps"))
    adjusted_share_baseline = _infer_adjusted_share_baseline(fundamental_row)
    market_cap = (
        entry_price * adjusted_share_baseline
        if adjusted_share_baseline is not None and entry_price > 0
        else np.nan
    )
    return {
        "pbr": _safe_ratio(entry_price, adjusted_bps),
        "forward_per": _safe_ratio(entry_price, forward_forecast_eps),
        "market_cap_bil_jpy": market_cap / 1_000_000_000.0
        if pd.notna(market_cap)
        else np.nan,
        "adjusted_share_baseline": adjusted_share_baseline
        if adjusted_share_baseline is not None
        else np.nan,
    }


def _infer_adjusted_share_baseline(row: dict[str, Any]) -> float | None:
    shares = _coerce_float(row.get("shares_outstanding"))
    raw_eps = _coerce_float(row.get("raw_eps"))
    adjusted_eps = _coerce_float(row.get("adjusted_eps"))
    if (
        shares is not None
        and shares > 0
        and raw_eps is not None
        and adjusted_eps is not None
        and adjusted_eps != 0
    ):
        baseline = raw_eps * shares / adjusted_eps
        if math.isfinite(baseline) and baseline > 0:
            return float(baseline)
    return shares if shares is not None and shares > 0 else None


def _safe_ratio(numerator: float | None, denominator: float | None) -> float:
    if numerator is None or denominator is None or denominator <= 0:
        return np.nan
    return numerator / denominator


def _coerce_numeric_series(df: pd.DataFrame, *columns: str) -> pd.Series:
    for column in columns:
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.nan, index=df.index)


def _load_dataset_stock_metadata(dataset_name: str) -> dict[str, dict[str, str]]:
    duckdb_path = get_data_dir() / "datasets" / dataset_name / "dataset.duckdb"
    if not duckdb_path.exists():
        return {}
    try:
        conn = duckdb.connect(str(duckdb_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT code, market_code, market_name
                FROM stocks
                """
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return {}

    metadata: dict[str, dict[str, str]] = {}
    for code, market_code, market_name in rows:
        normalized_code = str(code)
        normalized_market_code = str(market_code or "")
        normalized_market_name = str(market_name or "")
        metadata[normalized_code] = {
            "market_code": normalized_market_code,
            "market_name": normalized_market_name,
            "market_scope": _normalize_market_scope(
                normalized_market_code,
                normalized_market_name,
            ),
        }
    return metadata


def _is_market_universe_preset(dataset_name: str) -> bool:
    return dataset_name in UNIVERSE_PRESET_NAMES


def _market_duckdb_path() -> Path:
    return get_data_dir() / "market-timeseries" / "market.duckdb"


def _load_market_stock_metadata_for_trades(
    *,
    dataset_name: str,
    trade_df: pd.DataFrame,
) -> dict[tuple[str, str], dict[str, str]]:
    if trade_df.empty:
        return {}
    keys = (
        trade_df[["symbol", "entry_date"]]
        .assign(
            code=lambda frame: frame["symbol"].astype(str),
            entry_date_key=lambda frame: pd.to_datetime(
                frame["entry_date"],
                errors="coerce",
            ).dt.strftime("%Y-%m-%d"),
        )[["code", "entry_date_key"]]
        .dropna()
        .drop_duplicates()
    )
    if keys.empty:
        return {}
    duckdb_path = _market_duckdb_path()
    if not duckdb_path.exists():
        raise FileNotFoundError(f"market.duckdb was not found: {duckdb_path}")
    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        conn.register("__trade_entry_keys", keys)
        rows = conn.execute(
            """
            SELECT
                k.code,
                k.entry_date_key,
                m.market_code,
                m.market_name,
                m.scale_category
            FROM __trade_entry_keys k
            LEFT JOIN stock_master_daily m
              ON m.code = k.code
             AND m.date = k.entry_date_key
            ORDER BY k.code, k.entry_date_key
            """
        ).fetchdf()

    metadata_by_entry: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows.to_dict("records"):
        market_scope = normalize_market_scope(
            row.get("market_code"),
            market_name=row.get("market_name"),
            default=_infer_market_scope_from_dataset_name(dataset_name),
        )
        metadata_by_entry[(str(row["code"]), str(row["entry_date_key"]))] = {
            "market_code": str(row.get("market_code") or ""),
            "market_name": str(row.get("market_name") or ""),
            "market_scope": str(market_scope or "unknown"),
            "scale_category": str(row.get("scale_category") or ""),
        }
    return metadata_by_entry


def _normalize_market_scope(market_code: str | None, market_name: str | None) -> str:
    return str(
        normalize_market_scope(
            market_code,
            market_name=market_name,
            default="unknown",
        )
    )


def _infer_market_scope_from_dataset_name(dataset_name: str) -> str:
    lowered = dataset_name.lower()
    if "standard" in lowered:
        return "standard"
    if "growth" in lowered:
        return "growth"
    if "prime" in lowered or "topix" in lowered:
        return "prime"
    return "unknown"


def _extract_forward_eps_growth_threshold(parameters: dict[str, Any]) -> float | None:
    threshold = (
        parameters.get("entry_filter_params", {})
        .get("fundamental", {})
        .get("forward_eps_growth", {})
        .get("threshold")
    )
    return float(threshold) if threshold is not None else None


def _extract_risk_adjusted_return_threshold(parameters: dict[str, Any]) -> float | None:
    threshold = (
        parameters.get("entry_filter_params", {})
        .get("risk_adjusted_return", {})
        .get("threshold")
    )
    return float(threshold) if threshold is not None else None


def _extract_risk_adjusted_ratio_type(parameters: dict[str, Any]) -> RatioType:
    ratio_type = (
        parameters.get("entry_filter_params", {})
        .get("risk_adjusted_return", {})
        .get("ratio_type", "sharpe")
    )
    return cast(RatioType, ratio_type if ratio_type in ("sharpe", "sortino") else "sharpe")


def _extract_risk_adjusted_lookback_period(parameters: dict[str, Any]) -> int:
    lookback = (
        parameters.get("entry_filter_params", {})
        .get("risk_adjusted_return", {})
        .get("lookback_period", 60)
    )
    return int(lookback)


def _extract_volume_ratio_above_threshold(parameters: dict[str, Any]) -> float | None:
    threshold = (
        parameters.get("entry_filter_params", {})
        .get("volume_ratio_above", {})
        .get("ratio_threshold")
    )
    return float(threshold) if threshold is not None else None


def _extract_volume_ratio_above_short_period(parameters: dict[str, Any]) -> int:
    return int(
        parameters.get("entry_filter_params", {})
        .get("volume_ratio_above", {})
        .get("short_period", 50)
    )


def _extract_volume_ratio_above_long_period(parameters: dict[str, Any]) -> int:
    return int(
        parameters.get("entry_filter_params", {})
        .get("volume_ratio_above", {})
        .get("long_period", 150)
    )


def _extract_volume_ratio_ma_type(parameters: dict[str, Any]) -> str:
    return str(
        parameters.get("entry_filter_params", {})
        .get("volume_ratio_above", {})
        .get("ma_type", "sma")
    )


def _extract_rsi_period(parameters: dict[str, Any]) -> int:
    return int(
        parameters.get("entry_filter_params", {})
        .get("rsi_threshold", {})
        .get("period", 10)
    )


def _safe_gt(frame: pd.DataFrame, column: str, threshold: float) -> pd.Series:
    values = _get_numeric_column(frame, column)
    return values > threshold


def _safe_ge(frame: pd.DataFrame, column: str, threshold: float) -> pd.Series:
    values = _get_numeric_column(frame, column)
    return values >= threshold


def _safe_le(frame: pd.DataFrame, column: str, threshold: float) -> pd.Series:
    values = _get_numeric_column(frame, column)
    return values <= threshold


def _get_numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return cast(pd.Series, pd.to_numeric(frame[column], errors="coerce"))


def _coerce_int(value: Any) -> int:
    return int(float(value))


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _coerce_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return cast(pd.Timestamp, ts)


def _fmt_timestamp(ts: pd.Timestamp | None) -> str | None:
    return None if ts is None else ts.strftime("%Y-%m-%d")


def _build_summary_markdown(
    result: ForwardEpsTradeArchetypeDecompositionResult,
) -> str:
    overlay_df = result.overlay_candidate_summary_df.copy()
    overlay_df = overlay_df[overlay_df["candidate_name"] != "baseline_all"]
    value_overlay_df = result.value_overlay_candidate_summary_df.copy()
    value_overlay_df = value_overlay_df[
        value_overlay_df["candidate_name"] != "baseline_all"
    ] if not value_overlay_df.empty else value_overlay_df

    holdout_note = _pick_overlay_note(
        overlay_df=overlay_df,
        window_label=f"holdout_{result.holdout_months}m",
    )
    full_note = _pick_overlay_note(
        overlay_df=overlay_df,
        window_label="full",
    )
    fresh_coverage = _safe_overlay_coverage(
        overlay_df=result.overlay_candidate_summary_df,
        candidate_name="fresh_disclosure_3d",
        window_label="full",
    )
    prime_value_holdout_note = _pick_value_overlay_note(
        overlay_df=value_overlay_df,
        market_scope="prime",
        window_label=f"holdout_{result.holdout_months}m",
    )
    standard_value_holdout_note = _pick_value_overlay_note(
        overlay_df=value_overlay_df,
        market_scope="standard",
        window_label=f"holdout_{result.holdout_months}m",
    )
    market_scope_lines = _build_market_scope_summary_lines(result.market_scope_summary_df)

    return "\n".join(
        [
            "# Forward EPS Trade Archetype Decomposition",
            "",
            "## Scope",
            "",
            f"- Strategy: `{result.strategy_name}`",
            f"- Dataset: `{result.dataset_name}`",
            f"- Holdout window: `{result.holdout_months}` months",
            f"- Severe loss threshold: `{result.severe_loss_threshold_pct:.1f}%`",
            f"- Quantile bucket count: `{result.quantile_bucket_count}`",
            "- Market-derived features are evaluated at the previous trading session close to stay PIT-safe.",
            "",
            "## Key Reads",
            "",
            f"- Fresh disclosure (<=3d) share in full-history trades: `{fresh_coverage}`",
            f"- Best high-coverage holdout overlay: {holdout_note}",
            f"- Best high-coverage full-history overlay: {full_note}",
            f"- Prime holdout value overlay: {prime_value_holdout_note}",
            f"- Standard holdout value overlay: {standard_value_holdout_note}",
            *market_scope_lines,
            "",
            "## Artifact Tables",
            "",
            "- `dataset_summary_df`",
            "- `scenario_summary_df`",
            "- `trade_ledger_df`",
            "- `enriched_trade_df`",
            "- `market_scope_summary_df`",
            "- `feature_bucket_summary_df`",
            "- `value_feature_bucket_summary_df`",
            "- `overlay_candidate_summary_df`",
            "- `value_overlay_candidate_summary_df`",
        ]
    )


def _pick_overlay_note(*, overlay_df: pd.DataFrame, window_label: str) -> str:
    window_df = overlay_df[
        (overlay_df["window_label"] == window_label)
        & (overlay_df["coverage_pct"] >= 40.0)
        & (overlay_df["trade_count"] >= 5)
    ].copy()
    if window_df.empty:
        return "`No overlay candidate cleared the minimum coverage filter.`"
    window_df = window_df.sort_values(
        ["delta_avg_trade_return_pct", "delta_severe_loss_rate_pct"],
        ascending=[False, True],
    )
    best = window_df.iloc[0]
    return (
        f"`{best['candidate_name']}` "
        f"(coverage `{best['coverage_pct']:.1f}%`, "
        f"avg trade `{best['avg_trade_return_pct']:.2f}%`, "
        f"severe-loss rate `{best['severe_loss_rate_pct']:.1f}%`)"
    )


def _safe_overlay_coverage(
    *,
    overlay_df: pd.DataFrame,
    candidate_name: str,
    window_label: str,
) -> str:
    matched = overlay_df[
        (overlay_df["candidate_name"] == candidate_name)
        & (overlay_df["window_label"] == window_label)
    ]
    if matched.empty:
        return "N/A"
    return f"{matched.iloc[0]['coverage_pct']:.1f}%"


def _pick_value_overlay_note(
    *,
    overlay_df: pd.DataFrame,
    market_scope: str,
    window_label: str,
) -> str:
    if overlay_df.empty:
        return "`No value overlay rows were produced.`"
    window_df = overlay_df[
        (overlay_df["window_label"] == window_label)
        & (overlay_df["market_scope"] == market_scope)
        & (overlay_df["trade_count"] >= 1)
    ].copy()
    if window_df.empty:
        return "`No trades for this market scope in the selected dataset/window.`"
    window_df = window_df.sort_values(
        ["delta_avg_trade_return_pct", "avg_trade_return_pct"],
        ascending=[False, False],
    )
    best = window_df.iloc[0]
    return (
        f"`{best['candidate_name']}` "
        f"(trades `{int(best['trade_count'])}`, "
        f"coverage `{best['coverage_pct']:.1f}%`, "
        f"avg trade `{best['avg_trade_return_pct']:.2f}%`, "
        f"delta `{best['delta_avg_trade_return_pct']:.2f}%`)"
    )


def _build_market_scope_summary_lines(summary_df: pd.DataFrame) -> list[str]:
    if summary_df.empty:
        return ["- Market scope summary: `No market-scope rows were produced.`"]
    full_rows = summary_df[summary_df["window_label"] == "full"].copy()
    if full_rows.empty:
        return []
    lines: list[str] = []
    for market_scope in ("prime", "standard"):
        matched = full_rows[full_rows["market_scope"] == market_scope]
        if matched.empty:
            lines.append(
                f"- `{market_scope}` full-history trade coverage: `0 trades` "
                "(not available in the selected dataset)."
            )
            continue
        row = matched.iloc[0]
        lines.append(
            (
                f"- `{market_scope}` full-history trades: `{int(row['trade_count'])}`, "
                f"avg `{row['avg_trade_return_pct']:.2f}%`, "
                f"median PBR `{row['median_pbr']:.2f}`, "
                f"median forward PER `{row['median_forward_per']:.2f}`, "
                f"median mcap `{row['median_market_cap_bil_jpy']:.1f}bn JPY`."
            )
        )
    return lines


def _build_published_summary(
    result: ForwardEpsTradeArchetypeDecompositionResult,
) -> dict[str, Any]:
    published: dict[str, Any] = {
        "strategyName": result.strategy_name,
        "datasetName": result.dataset_name,
        "holdoutMonths": result.holdout_months,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "tradeCount": int(len(result.trade_ledger_df)),
    }
    if not result.market_scope_summary_df.empty:
        published["marketScopeSummary"] = result.market_scope_summary_df[
            result.market_scope_summary_df["window_label"].isin(
                ["full", f"holdout_{result.holdout_months}m"]
            )
        ].to_dict(orient="records")
    if not result.value_overlay_candidate_summary_df.empty:
        value_core = result.value_overlay_candidate_summary_df[
            result.value_overlay_candidate_summary_df["candidate_name"]
            == "value_core_low_pbr_low_fper_small_cap"
        ]
        published["valueCoreSummary"] = value_core.to_dict(orient="records")
    return published


__all__ = [
    "DEFAULT_DATASET_NAME",
    "DEFAULT_HOLDOUT_MONTHS",
    "DEFAULT_QUANTILE_BUCKET_COUNT",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "DEFAULT_STRATEGY_NAME",
    "FORWARD_EPS_TRADE_ARCHETYPE_DECOMPOSITION_EXPERIMENT_ID",
    "ForwardEpsTradeArchetypeDecompositionResult",
    "get_forward_eps_trade_archetype_decomposition_bundle_path_for_run_id",
    "get_forward_eps_trade_archetype_decomposition_latest_bundle_path",
    "load_forward_eps_trade_archetype_decomposition_bundle",
    "run_forward_eps_trade_archetype_decomposition",
    "write_forward_eps_trade_archetype_decomposition_bundle",
]
