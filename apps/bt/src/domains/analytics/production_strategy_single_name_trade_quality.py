"""Runner-first single-name trade-quality audit for production strategies."""

from __future__ import annotations

import calendar
import copy
import json
import math
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.window_warmup import (
    estimate_strategy_indicator_warmup_calendar_days,
    resolve_window_load_start_date,
)
from src.domains.backtest.core.runner import BacktestRunner
from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.infrastructure.data_access.loaders import get_stock_list
from src.infrastructure.data_access.mode import data_access_mode_context
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.shared.paths import get_data_dir

PRODUCTION_STRATEGY_SINGLE_NAME_TRADE_QUALITY_EXPERIMENT_ID = (
    "strategy-audit/production-strategy-single-name-trade-quality"
)
DEFAULT_STRATEGY_NAMES = (
    "production/forward_eps_driven",
    "production/range_break_v15",
)
DEFAULT_DATASET_NAMES = (
    "primeExTopix500_20260325",
    "topix500_20260325",
)
_SCENARIO_SUMMARY_COLUMNS = (
    "status",
    "error",
    "strategy_name",
    "strategy_basename",
    "dataset_name",
    "dataset_preset",
    "window_label",
    "window_start_date",
    "window_end_date",
    "runtime_seconds",
    "trade_count",
    "traded_symbol_count",
    "trades_per_symbol",
    "win_rate_pct",
    "avg_trade_return_pct",
    "median_trade_return_pct",
    "avg_win_return_pct",
    "avg_loss_return_pct",
    "best_trade_return_pct",
    "worst_trade_return_pct",
    "p05_trade_return_pct",
    "p95_trade_return_pct",
    "profit_factor",
    "expectancy_pnl",
    "total_pnl",
    "avg_holding_days",
    "median_holding_days",
)
_TRADE_LEDGER_COLUMNS = (
    "strategy_name",
    "strategy_basename",
    "dataset_name",
    "dataset_preset",
    "window_label",
    "window_start_date",
    "window_end_date",
    "symbol",
    "entry_timestamp",
    "exit_timestamp",
    "holding_days",
    "size",
    "avg_entry_price",
    "avg_exit_price",
    "pnl",
    "trade_return_pct",
    "direction",
    "status",
    "position_id",
)
_PER_SYMBOL_SUMMARY_COLUMNS = (
    "strategy_name",
    "strategy_basename",
    "dataset_name",
    "dataset_preset",
    "window_label",
    "symbol",
    "trade_count",
    "win_rate_pct",
    "avg_trade_return_pct",
    "median_trade_return_pct",
    "avg_holding_days",
    "total_pnl",
    "best_trade_return_pct",
    "worst_trade_return_pct",
    "first_entry_timestamp",
    "last_exit_timestamp",
)


@dataclass(frozen=True)
class ProductionStrategySingleNameTradeQualityResult:
    db_path: str
    strategy_names: tuple[str, ...]
    dataset_names: tuple[str, ...]
    holdout_months: int
    holdout_start_date: str
    holdout_end_date: str
    analysis_start_date: str
    analysis_end_date: str
    dataset_summary_df: pd.DataFrame
    scenario_summary_df: pd.DataFrame
    trade_ledger_df: pd.DataFrame
    per_symbol_summary_df: pd.DataFrame


def run_production_strategy_single_name_trade_quality(
    *,
    strategy_names: Sequence[str] = DEFAULT_STRATEGY_NAMES,
    dataset_names: Sequence[str] = DEFAULT_DATASET_NAMES,
    holdout_months: int = 6,
) -> ProductionStrategySingleNameTradeQualityResult:
    if holdout_months <= 0:
        raise ValueError("holdout_months must be greater than 0")
    if not strategy_names:
        raise ValueError("strategy_names must not be empty")
    if not dataset_names:
        raise ValueError("dataset_names must not be empty")

    dataset_infos = [
        _load_dataset_summary(dataset_name, holdout_months=holdout_months)
        for dataset_name in dataset_names
    ]
    dataset_summary_df = pd.DataFrame(dataset_infos)

    runner = BacktestRunner()
    base_parameters_by_strategy = {
        strategy_name: runner.build_parameters_for_strategy(strategy_name)
        for strategy_name in strategy_names
    }

    scenario_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []

    with data_access_mode_context("direct"):
        resolved_stock_codes = {
            dataset_name: get_stock_list(dataset_name)
            for dataset_name in dataset_names
        }
        for dataset_info in dataset_infos:
            windows = _build_analysis_windows(
                dataset_info=dataset_info,
                holdout_months=holdout_months,
            )
            stock_codes = resolved_stock_codes[dataset_info["dataset_name"]]
            for strategy_name in strategy_names:
                base_parameters = base_parameters_by_strategy[strategy_name]
                for window in windows:
                    scenario_row, scenario_trade_rows = _run_single_name_scenario(
                        base_parameters=base_parameters,
                        strategy_name=strategy_name,
                        dataset_info=dataset_info,
                        stock_codes=stock_codes,
                        window_label=window["window_label"],
                        window_start_date=window["window_start_date"],
                        window_end_date=window["window_end_date"],
                    )
                    scenario_rows.append(scenario_row)
                    trade_rows.extend(scenario_trade_rows)

    scenario_summary_df = pd.DataFrame(
        scenario_rows,
        columns=_SCENARIO_SUMMARY_COLUMNS,
    )
    if scenario_summary_df.empty:
        raise RuntimeError("single-name trade-quality audit produced no scenario rows")

    trade_ledger_df = pd.DataFrame(
        trade_rows,
        columns=_TRADE_LEDGER_COLUMNS,
    )
    per_symbol_summary_df = _build_per_symbol_summary_df(trade_ledger_df)

    analysis_start_date = str(dataset_summary_df["dataset_start_date"].min())
    analysis_end_date = str(dataset_summary_df["dataset_end_date"].max())
    holdout_start_date = str(dataset_summary_df["holdout_start_date"].min())
    holdout_end_date = str(dataset_summary_df["holdout_end_date"].max())

    return ProductionStrategySingleNameTradeQualityResult(
        db_path="multi://single-name-trade-quality",
        strategy_names=tuple(strategy_names),
        dataset_names=tuple(dataset_names),
        holdout_months=holdout_months,
        holdout_start_date=holdout_start_date,
        holdout_end_date=holdout_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        dataset_summary_df=dataset_summary_df,
        scenario_summary_df=scenario_summary_df,
        trade_ledger_df=trade_ledger_df,
        per_symbol_summary_df=per_symbol_summary_df,
    )


def write_production_strategy_single_name_trade_quality_bundle(
    result: ProductionStrategySingleNameTradeQualityResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=PRODUCTION_STRATEGY_SINGLE_NAME_TRADE_QUALITY_EXPERIMENT_ID,
        module=__name__,
        function="run_production_strategy_single_name_trade_quality",
        params={
            "strategy_names": list(result.strategy_names),
            "dataset_names": list(result.dataset_names),
            "holdout_months": result.holdout_months,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "strategy_names": list(result.strategy_names),
            "dataset_names": list(result.dataset_names),
            "holdout_months": result.holdout_months,
            "holdout_start_date": result.holdout_start_date,
            "holdout_end_date": result.holdout_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
        },
        result_tables={
            "dataset_summary_df": result.dataset_summary_df,
            "scenario_summary_df": result.scenario_summary_df,
            "trade_ledger_df": result.trade_ledger_df,
            "per_symbol_summary_df": result.per_symbol_summary_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_production_strategy_single_name_trade_quality_bundle(
    bundle_path: str | Path,
) -> ProductionStrategySingleNameTradeQualityResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(
        bundle_path,
        table_names=(
            "dataset_summary_df",
            "scenario_summary_df",
            "trade_ledger_df",
            "per_symbol_summary_df",
        ),
    )
    metadata = dict(info.result_metadata)
    return ProductionStrategySingleNameTradeQualityResult(
        db_path=str(metadata["db_path"]),
        strategy_names=tuple(metadata["strategy_names"]),
        dataset_names=tuple(metadata["dataset_names"]),
        holdout_months=int(metadata["holdout_months"]),
        holdout_start_date=str(metadata["holdout_start_date"]),
        holdout_end_date=str(metadata["holdout_end_date"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        dataset_summary_df=tables["dataset_summary_df"],
        scenario_summary_df=tables["scenario_summary_df"],
        trade_ledger_df=tables["trade_ledger_df"],
        per_symbol_summary_df=tables["per_symbol_summary_df"],
    )


def get_production_strategy_single_name_trade_quality_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        PRODUCTION_STRATEGY_SINGLE_NAME_TRADE_QUALITY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_production_strategy_single_name_trade_quality_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        PRODUCTION_STRATEGY_SINGLE_NAME_TRADE_QUALITY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _run_single_name_scenario(
    *,
    base_parameters: dict[str, Any],
    strategy_name: str,
    dataset_info: dict[str, Any],
    stock_codes: list[str],
    window_label: str,
    window_start_date: str,
    window_end_date: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start = time.perf_counter()
    scenario_trade_rows: list[dict[str, Any]] = []
    try:
        warmup_calendar_days = estimate_strategy_indicator_warmup_calendar_days(
            base_parameters
        )
        load_start_date = resolve_window_load_start_date(
            dataset_start_date=dataset_info["dataset_start_date"],
            window_start_date=window_start_date,
            warmup_calendar_days=warmup_calendar_days,
        )
        scenario_parameters = copy.deepcopy(base_parameters)
        shared_config_payload = dict(scenario_parameters.get("shared_config", {}))
        shared_config_payload.update(
            {
                "dataset": dataset_info["dataset_name"],
                "start_date": load_start_date,
                "end_date": window_end_date,
                "group_by": True,
                "cash_sharing": True,
                "printlog": False,
                "stock_codes": stock_codes,
            }
        )
        shared_config = SharedConfig.model_validate(
            shared_config_payload,
            context={"resolve_stock_codes": False},
        )
        entry_signal_params = SignalParams.model_validate(
            scenario_parameters.get("entry_filter_params", {})
        )
        exit_signal_params = SignalParams.model_validate(
            scenario_parameters.get("exit_trigger_params", {})
        )
        strategy = YamlConfigurableStrategy(
            shared_config=shared_config,
            entry_filter_params=entry_signal_params,
            exit_trigger_params=exit_signal_params,
        )
        strategy.run_multi_backtest()
        portfolio = _build_windowed_individual_portfolio(
            strategy=strategy,
            window_start_date=window_start_date,
            window_end_date=window_end_date,
        )
        runtime_seconds = time.perf_counter() - start
        scenario_trade_rows = _extract_trade_ledger_rows(
            portfolio=portfolio,
            strategy_name=strategy_name,
            dataset_info=dataset_info,
            window_label=window_label,
            window_start_date=window_start_date,
            window_end_date=window_end_date,
        )
        return (
            _build_scenario_summary_row(
                strategy_name=strategy_name,
                dataset_info=dataset_info,
                window_label=window_label,
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                runtime_seconds=runtime_seconds,
                trade_rows=scenario_trade_rows,
                error=None,
            ),
            scenario_trade_rows,
        )
    except Exception as exc:
        runtime_seconds = time.perf_counter() - start
        return (
            _build_scenario_summary_row(
                strategy_name=strategy_name,
                dataset_info=dataset_info,
                window_label=window_label,
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                runtime_seconds=runtime_seconds,
                trade_rows=[],
                error=str(exc),
            ),
            [],
        )


def _build_windowed_individual_portfolio(
    *,
    strategy: YamlConfigurableStrategy,
    window_start_date: str,
    window_end_date: str,
) -> Any:
    cached = strategy._get_grouped_portfolio_inputs_cache()
    if cached is None:
        raise RuntimeError("grouped portfolio inputs cache was not populated")

    open_data, close_data, all_entries, all_exits = cached
    open_slice = open_data.loc[window_start_date:window_end_date]
    close_slice = close_data.loc[window_start_date:window_end_date]
    entries_slice = all_entries.loc[window_start_date:window_end_date]
    exits_slice = all_exits.loc[window_start_date:window_end_date]

    data_dict: dict[str, pd.DataFrame] = {}
    entries_dict: dict[str, pd.Series] = {}
    exits_dict: dict[str, pd.Series] = {}

    for stock_code in strategy.stock_codes:
        if stock_code not in close_slice.columns:
            continue
        data_dict[stock_code] = pd.DataFrame(
            {
                "Open": open_slice[stock_code],
                "Close": close_slice[stock_code],
            }
        )
        entries_dict[stock_code] = entries_slice[stock_code]
        exits_dict[stock_code] = exits_slice[stock_code]

    if not data_dict:
        raise RuntimeError("windowed individual portfolio inputs produced no data")

    return strategy._create_individual_portfolios(
        data_dict,
        entries_dict,
        exits_dict,
    )


def _extract_trade_ledger_rows(
    *,
    portfolio: Any,
    strategy_name: str,
    dataset_info: dict[str, Any],
    window_label: str,
    window_start_date: str,
    window_end_date: str,
) -> list[dict[str, Any]]:
    trades = getattr(portfolio, "trades", None)
    trade_records = getattr(trades, "records_readable", None)
    if not isinstance(trade_records, pd.DataFrame) or trade_records.empty:
        return []

    frame = trade_records.copy()
    entry_timestamp_values = (
        frame["Entry Timestamp"]
        if "Entry Timestamp" in frame.columns
        else pd.Series(pd.NaT, index=frame.index)
    )
    exit_timestamp_values = (
        frame["Exit Timestamp"]
        if "Exit Timestamp" in frame.columns
        else pd.Series(pd.NaT, index=frame.index)
    )
    entry_timestamp = pd.to_datetime(entry_timestamp_values, errors="coerce")
    exit_timestamp = pd.to_datetime(exit_timestamp_values, errors="coerce")

    rows: list[dict[str, Any]] = []
    strategy_basename = strategy_name.split("/")[-1]
    for idx in range(len(frame)):
        entry_value = entry_timestamp.iloc[idx]
        exit_value = exit_timestamp.iloc[idx]
        holding_days = None
        if pd.notna(entry_value) and pd.notna(exit_value):
            holding_days = _coerce_float(
                (pd.Timestamp(exit_value) - pd.Timestamp(entry_value)).total_seconds()
                / 86400.0
            )
        rows.append(
            {
                "strategy_name": strategy_name,
                "strategy_basename": strategy_basename,
                "dataset_name": dataset_info["dataset_name"],
                "dataset_preset": dataset_info["dataset_preset"],
                "window_label": window_label,
                "window_start_date": window_start_date,
                "window_end_date": window_end_date,
                "symbol": str(frame.iloc[idx].get("Column", "")),
                "entry_timestamp": _coerce_timestamp(entry_value),
                "exit_timestamp": _coerce_timestamp(exit_value),
                "holding_days": holding_days,
                "size": _coerce_float(frame.iloc[idx].get("Size")),
                "avg_entry_price": _coerce_float(frame.iloc[idx].get("Avg Entry Price")),
                "avg_exit_price": _coerce_float(frame.iloc[idx].get("Avg Exit Price")),
                "pnl": _coerce_float(frame.iloc[idx].get("PnL")),
                "trade_return_pct": _scale_return_pct(frame.iloc[idx].get("Return")),
                "direction": _coerce_str(frame.iloc[idx].get("Direction")),
                "status": _coerce_str(frame.iloc[idx].get("Status")),
                "position_id": _coerce_int(frame.iloc[idx].get("Position Id")),
            }
        )
    return rows


def _build_scenario_summary_row(
    *,
    strategy_name: str,
    dataset_info: dict[str, Any],
    window_label: str,
    window_start_date: str,
    window_end_date: str,
    runtime_seconds: float,
    trade_rows: list[dict[str, Any]],
    error: str | None,
) -> dict[str, Any]:
    strategy_basename = strategy_name.split("/")[-1]
    row: dict[str, Any] = {
        "status": "failed" if error else "ok",
        "error": error,
        "strategy_name": strategy_name,
        "strategy_basename": strategy_basename,
        "dataset_name": dataset_info["dataset_name"],
        "dataset_preset": dataset_info["dataset_preset"],
        "window_label": window_label,
        "window_start_date": window_start_date,
        "window_end_date": window_end_date,
        "runtime_seconds": runtime_seconds,
        "trade_count": 0,
        "traded_symbol_count": 0,
        "trades_per_symbol": None,
        "win_rate_pct": None,
        "avg_trade_return_pct": None,
        "median_trade_return_pct": None,
        "avg_win_return_pct": None,
        "avg_loss_return_pct": None,
        "best_trade_return_pct": None,
        "worst_trade_return_pct": None,
        "p05_trade_return_pct": None,
        "p95_trade_return_pct": None,
        "profit_factor": None,
        "expectancy_pnl": None,
        "total_pnl": None,
        "avg_holding_days": None,
        "median_holding_days": None,
    }
    if error or not trade_rows:
        return row

    frame = pd.DataFrame(trade_rows)
    returns = pd.to_numeric(frame["trade_return_pct"], errors="coerce").dropna()
    pnl = pd.to_numeric(frame["pnl"], errors="coerce").dropna()
    holding_days = pd.to_numeric(frame["holding_days"], errors="coerce").dropna()
    positive_returns = returns[returns > 0]
    negative_returns = returns[returns < 0]
    positive_pnl = pnl[pnl > 0]
    negative_pnl = pnl[pnl < 0]
    traded_symbol_count = int(frame["symbol"].astype(str).nunique())

    row.update(
        {
            "trade_count": int(len(frame)),
            "traded_symbol_count": traded_symbol_count,
            "trades_per_symbol": (
                len(frame) / traded_symbol_count if traded_symbol_count > 0 else None
            ),
            "win_rate_pct": (
                float((returns > 0).mean() * 100.0) if not returns.empty else None
            ),
            "avg_trade_return_pct": _series_stat(returns, "mean"),
            "median_trade_return_pct": _series_stat(returns, "median"),
            "avg_win_return_pct": _series_stat(positive_returns, "mean"),
            "avg_loss_return_pct": _series_stat(negative_returns, "mean"),
            "best_trade_return_pct": _series_stat(returns, "max"),
            "worst_trade_return_pct": _series_stat(returns, "min"),
            "p05_trade_return_pct": _series_quantile(returns, 0.05),
            "p95_trade_return_pct": _series_quantile(returns, 0.95),
            "profit_factor": _calculate_profit_factor(positive_pnl, negative_pnl),
            "expectancy_pnl": _series_stat(pnl, "mean"),
            "total_pnl": _series_stat(pnl, "sum"),
            "avg_holding_days": _series_stat(holding_days, "mean"),
            "median_holding_days": _series_stat(holding_days, "median"),
        }
    )
    return row


def _build_per_symbol_summary_df(trade_ledger_df: pd.DataFrame) -> pd.DataFrame:
    if trade_ledger_df.empty:
        return pd.DataFrame(columns=_PER_SYMBOL_SUMMARY_COLUMNS)

    frame = trade_ledger_df.copy()
    grouped = frame.groupby(
        [
            "strategy_name",
            "strategy_basename",
            "dataset_name",
            "dataset_preset",
            "window_label",
            "symbol",
        ],
        as_index=False,
    )

    rows: list[dict[str, Any]] = []
    for keys, group in grouped:
        (
            strategy_name,
            strategy_basename,
            dataset_name,
            dataset_preset,
            window_label,
            symbol,
        ) = keys
        returns = pd.to_numeric(group["trade_return_pct"], errors="coerce").dropna()
        pnl = pd.to_numeric(group["pnl"], errors="coerce").dropna()
        holding_days = pd.to_numeric(group["holding_days"], errors="coerce").dropna()
        entry_timestamp = pd.to_datetime(group["entry_timestamp"], errors="coerce")
        exit_timestamp = pd.to_datetime(group["exit_timestamp"], errors="coerce")
        rows.append(
            {
                "strategy_name": strategy_name,
                "strategy_basename": strategy_basename,
                "dataset_name": dataset_name,
                "dataset_preset": dataset_preset,
                "window_label": window_label,
                "symbol": symbol,
                "trade_count": int(len(group)),
                "win_rate_pct": (
                    float((returns > 0).mean() * 100.0) if not returns.empty else None
                ),
                "avg_trade_return_pct": _series_stat(returns, "mean"),
                "median_trade_return_pct": _series_stat(returns, "median"),
                "avg_holding_days": _series_stat(holding_days, "mean"),
                "total_pnl": _series_stat(pnl, "sum"),
                "best_trade_return_pct": _series_stat(returns, "max"),
                "worst_trade_return_pct": _series_stat(returns, "min"),
                "first_entry_timestamp": _coerce_timestamp(entry_timestamp.min()),
                "last_exit_timestamp": _coerce_timestamp(exit_timestamp.max()),
            }
        )
    return pd.DataFrame(rows, columns=_PER_SYMBOL_SUMMARY_COLUMNS).sort_values(
        [
            "dataset_name",
            "window_label",
            "strategy_name",
            "trade_count",
            "avg_trade_return_pct",
        ],
        ascending=[True, True, True, False, False],
        kind="stable",
    ).reset_index(drop=True)


def _load_dataset_summary(dataset_name: str, *, holdout_months: int) -> dict[str, Any]:
    manifest_path = get_data_dir() / "datasets" / dataset_name / "manifest.v2.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Dataset manifest was not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    date_range = payload.get("dateRange", {})
    dataset_start = str(date_range["min"])
    dataset_end = str(date_range["max"])
    holdout_start_date = _subtract_months_iso(dataset_end, holdout_months)
    train_end_date = _day_before_iso(holdout_start_date)
    if train_end_date < dataset_start:
        train_end_date = None
    return {
        "dataset_name": dataset_name,
        "dataset_preset": payload.get("dataset", {}).get("preset", ""),
        "stocks": payload.get("counts", {}).get("stocks"),
        "stock_data_rows": payload.get("counts", {}).get("stock_data"),
        "dataset_start_date": dataset_start,
        "dataset_end_date": dataset_end,
        "train_end_date": train_end_date,
        "holdout_start_date": holdout_start_date,
        "holdout_end_date": dataset_end,
    }


def _build_analysis_windows(
    *,
    dataset_info: dict[str, Any],
    holdout_months: int,
) -> tuple[dict[str, str], ...]:
    windows: list[dict[str, str]] = []
    train_end_date = dataset_info.get("train_end_date")
    if train_end_date:
        windows.append(
            {
                "window_label": "train_pre_holdout",
                "window_start_date": dataset_info["dataset_start_date"],
                "window_end_date": str(train_end_date),
            }
        )
    windows.append(
        {
            "window_label": f"holdout_{holdout_months}m",
            "window_start_date": dataset_info["holdout_start_date"],
            "window_end_date": dataset_info["holdout_end_date"],
        }
    )
    windows.append(
        {
            "window_label": "full",
            "window_start_date": dataset_info["dataset_start_date"],
            "window_end_date": dataset_info["dataset_end_date"],
        }
    )
    return tuple(windows)


def _subtract_months_iso(iso_date: str, months: int) -> str:
    parsed = date.fromisoformat(iso_date)
    year = parsed.year
    month = parsed.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(parsed.day, calendar.monthrange(year, month)[1])
    return date(year, month, day).isoformat()


def _day_before_iso(iso_date: str) -> str:
    return (date.fromisoformat(iso_date) - timedelta(days=1)).isoformat()


def _series_stat(series: pd.Series, method_name: str) -> float | None:
    if series.empty:
        return None
    method = getattr(series, method_name)
    return _coerce_float(method())


def _series_quantile(series: pd.Series, q: float) -> float | None:
    if series.empty:
        return None
    return _coerce_float(series.quantile(q))


def _calculate_profit_factor(
    positive_pnl: pd.Series,
    negative_pnl: pd.Series,
) -> float | None:
    gain = _coerce_float(positive_pnl.sum()) if not positive_pnl.empty else 0.0
    loss = _coerce_float(negative_pnl.sum()) if not negative_pnl.empty else 0.0
    if gain is None or loss is None:
        return None
    if loss is None or loss == 0.0:
        if gain > 0:
            return None
        return 0.0
    return gain / abs(loss)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _coerce_int(value: Any) -> int | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _coerce_timestamp(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _scale_return_pct(value: Any) -> float | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return parsed * 100.0


def _build_summary_markdown(
    result: ProductionStrategySingleNameTradeQualityResult,
) -> str:
    lines = [
        "# Production Strategy Single-Name Trade Quality",
        "",
        "## Scope",
        "",
        f"- Strategies: `{', '.join(result.strategy_names)}`",
        f"- Datasets: `{', '.join(result.dataset_names)}`",
        (
            f"- Holdout window: `{result.holdout_start_date} -> "
            f"{result.holdout_end_date}` ({result.holdout_months} months)"
        ),
        "- Execution shape: `group_by=False`, `cash_sharing=False`, no Kelly rerun.",
        "",
        "## Key Reads",
        "",
    ]
    lines.extend(_build_key_read_lines(result))
    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `dataset_summary_df`",
            "- `scenario_summary_df`",
            "- `trade_ledger_df`",
            "- `per_symbol_summary_df`",
        ]
    )
    return "\n".join(lines)


def _build_key_read_lines(
    result: ProductionStrategySingleNameTradeQualityResult,
) -> list[str]:
    scenario_df = result.scenario_summary_df
    if scenario_df.empty:
        return ["- No scenario rows were produced."]

    ok_df = scenario_df[scenario_df["status"] == "ok"].copy()
    if ok_df.empty:
        return ["- All scenarios failed."]

    lines: list[str] = []
    for dataset_name in result.dataset_names:
        for strategy_basename in ("forward_eps_driven", "range_break_v15"):
            holdout_row = _select_scenario_row(
                ok_df,
                strategy_basename=strategy_basename,
                dataset_name=dataset_name,
                window_label=f"holdout_{result.holdout_months}m",
            )
            if holdout_row is None:
                continue
            lines.append(
                (
                    f"- `{dataset_name}` holdout / `{strategy_basename}`: "
                    f"`{int(holdout_row['trade_count'])}` trades across "
                    f"`{int(holdout_row['traded_symbol_count'])}` symbols, "
                    f"avg trade return `{_fmt_pct(holdout_row['avg_trade_return_pct'])}`, "
                    f"win rate `{_fmt_pct(holdout_row['win_rate_pct'])}`, "
                    f"runtime `{_fmt_seconds(holdout_row['runtime_seconds'])}`."
                )
            )

    slowest = ok_df.sort_values("runtime_seconds", ascending=False).iloc[0]
    lines.append(
        (
            f"- Slowest scenario: `{slowest['strategy_name']}` on "
            f"`{slowest['dataset_name']}` / `{slowest['window_label']}` "
            f"at `{_fmt_seconds(slowest['runtime_seconds'])}`."
        )
    )

    best_holdout = ok_df[
        ok_df["window_label"] == f"holdout_{result.holdout_months}m"
    ].sort_values("avg_trade_return_pct", ascending=False)
    if not best_holdout.empty:
        best = best_holdout.iloc[0]
        lines.append(
            (
                f"- Best holdout trade expectancy: `{best['strategy_name']}` on "
                f"`{best['dataset_name']}` with avg trade return "
                f"`{_fmt_pct(best['avg_trade_return_pct'])}`."
            )
        )
    return lines


def _build_published_summary(
    result: ProductionStrategySingleNameTradeQualityResult,
) -> dict[str, Any]:
    scenario_df = result.scenario_summary_df
    published: dict[str, Any] = {
        "strategyNames": list(result.strategy_names),
        "datasetNames": list(result.dataset_names),
        "holdoutMonths": result.holdout_months,
        "holdoutStartDate": result.holdout_start_date,
        "holdoutEndDate": result.holdout_end_date,
        "scenarioCount": int(len(scenario_df)),
        "successfulScenarioCount": int((scenario_df["status"] == "ok").sum()),
    }
    ok_df = scenario_df[scenario_df["status"] == "ok"].copy()
    if ok_df.empty:
        return published

    slowest = ok_df.sort_values("runtime_seconds", ascending=False).iloc[0]
    published["slowestScenario"] = {
        "strategyName": _json_scalar(slowest["strategy_name"]),
        "datasetName": _json_scalar(slowest["dataset_name"]),
        "windowLabel": _json_scalar(slowest["window_label"]),
        "runtimeSeconds": _json_scalar(slowest["runtime_seconds"]),
    }

    holdout_df = ok_df[
        ok_df["window_label"] == f"holdout_{result.holdout_months}m"
    ].sort_values("avg_trade_return_pct", ascending=False)
    if not holdout_df.empty:
        best = holdout_df.iloc[0]
        published["bestHoldoutTradeQuality"] = {
            "strategyName": _json_scalar(best["strategy_name"]),
            "datasetName": _json_scalar(best["dataset_name"]),
            "avgTradeReturnPct": _json_scalar(best["avg_trade_return_pct"]),
            "winRatePct": _json_scalar(best["win_rate_pct"]),
            "tradeCount": _json_scalar(best["trade_count"]),
        }
    return published


def _select_scenario_row(
    scenario_df: pd.DataFrame,
    *,
    strategy_basename: str,
    dataset_name: str,
    window_label: str,
) -> dict[str, Any] | None:
    filtered = scenario_df[
        (scenario_df["strategy_basename"] == strategy_basename)
        & (scenario_df["dataset_name"] == dataset_name)
        & (scenario_df["window_label"] == window_label)
    ]
    if filtered.empty:
        return None
    return filtered.iloc[0].to_dict()


def _fmt_pct(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "N/A"
    return f"{parsed:.2f}%"


def _fmt_seconds(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "N/A"
    return f"{parsed:.2f}s"


def _json_scalar(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


__all__ = [
    "DEFAULT_DATASET_NAMES",
    "DEFAULT_STRATEGY_NAMES",
    "PRODUCTION_STRATEGY_SINGLE_NAME_TRADE_QUALITY_EXPERIMENT_ID",
    "ProductionStrategySingleNameTradeQualityResult",
    "get_production_strategy_single_name_trade_quality_bundle_path_for_run_id",
    "get_production_strategy_single_name_trade_quality_latest_bundle_path",
    "load_production_strategy_single_name_trade_quality_bundle",
    "run_production_strategy_single_name_trade_quality",
    "write_production_strategy_single_name_trade_quality_bundle",
]
