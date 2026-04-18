"""Runner-first robustness audit for production backtest strategies."""

from __future__ import annotations

import calendar
import json
import math
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
from src.domains.backtest.core.simulation import BacktestSimulationResult
from src.domains.backtest.core.report_payload import build_backtest_report_payload
from src.domains.backtest.core.runner import BacktestRunner
from src.infrastructure.data_access.loaders import get_stock_list
from src.infrastructure.db.market.dataset_snapshot_reader import DatasetSnapshotReader
from src.domains.strategy.runtime.file_operations import load_yaml_file
from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.infrastructure.data_access.mode import data_access_mode_context
from src.shared.paths import get_data_dir

PRODUCTION_STRATEGY_ROBUSTNESS_AUDIT_EXPERIMENT_ID = (
    "strategy-audit/production-strategy-robustness"
)
DEFAULT_STRATEGY_NAMES = (
    "production/forward_eps_driven",
    "production/range_break_v15",
)
DEFAULT_REFERENCE_STRATEGY = "buy_and_hold"
DEFAULT_DATASET_NAMES = (
    "primeExTopix500_20260325",
    "topix500_20260325",
)
_EQUAL_WEIGHT_BENCHMARK_COLUMNS = (
    "dataset_name",
    "dataset_preset",
    "window_label",
    "equal_weight_constituent_count",
    "equal_weight_total_return_pct",
    "equal_weight_median_stock_total_return_pct",
    "equal_weight_winner_ratio_pct",
    "equal_weight_earliest_first_date",
    "equal_weight_latest_last_date",
)


@dataclass(frozen=True)
class ProductionStrategyRobustnessAuditResult:
    db_path: str
    strategy_names: tuple[str, ...]
    dataset_names: tuple[str, ...]
    holdout_months: int
    holdout_start_date: str
    holdout_end_date: str
    analysis_start_date: str
    analysis_end_date: str
    dataset_summary_df: pd.DataFrame
    equal_weight_benchmark_df: pd.DataFrame
    scenario_metrics_df: pd.DataFrame
    comparison_df: pd.DataFrame
    sizing_lift_df: pd.DataFrame


@dataclass(frozen=True)
class _WindowedSimulationState:
    strategy: YamlConfigurableStrategy
    initial_portfolio: Any
    all_entries: pd.DataFrame


_WINDOW_SHARED_CONFIG_KEYS = {
    "dataset",
    "start_date",
    "end_date",
    "group_by",
    "cash_sharing",
    "printlog",
    "stock_codes",
}
_KELLY_ONLY_SHARED_CONFIG_KEYS = {
    "kelly_fraction",
    "min_allocation",
    "max_allocation",
}
_DEFAULT_KELLY_FRACTION = 1.0
_DEFAULT_MIN_ALLOCATION = 0.01
_DEFAULT_MAX_ALLOCATION = 0.5


def run_production_strategy_robustness_audit(
    *,
    strategy_names: Sequence[str] = DEFAULT_STRATEGY_NAMES,
    dataset_names: Sequence[str] = DEFAULT_DATASET_NAMES,
    holdout_months: int = 6,
    include_reference_buy_and_hold: bool = True,
) -> ProductionStrategyRobustnessAuditResult:
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
    equal_weight_rows: list[dict[str, Any]] = []
    simulation_state_cache: dict[str, _WindowedSimulationState] = {}

    runner = BacktestRunner()
    all_strategy_names = list(dict.fromkeys(strategy_names))
    if include_reference_buy_and_hold:
        all_strategy_names.append(DEFAULT_REFERENCE_STRATEGY)

    rows: list[dict[str, Any]] = []
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
            equal_weight_rows.extend(
                _compute_equal_weight_benchmark_rows(
                    dataset_info=dataset_info,
                    windows=windows,
                )
            )
            stock_codes = resolved_stock_codes[dataset_info["dataset_name"]]
            for strategy_name in all_strategy_names:
                for window in windows:
                    rows.extend(
                        _run_single_scenario(
                            runner=runner,
                            strategy_name=strategy_name,
                            dataset_info=dataset_info,
                            stock_codes=stock_codes,
                            simulation_state_cache=simulation_state_cache,
                            window_label=window["window_label"],
                            window_start_date=window["window_start_date"],
                            window_end_date=window["window_end_date"],
                        )
                    )

    equal_weight_benchmark_df = pd.DataFrame(
        equal_weight_rows,
        columns=_EQUAL_WEIGHT_BENCHMARK_COLUMNS,
    )
    scenario_metrics_df = pd.DataFrame(rows)
    if scenario_metrics_df.empty:
        raise RuntimeError("robustness audit produced no scenario metrics")

    comparison_df = _build_comparison_df(
        scenario_metrics_df,
        equal_weight_benchmark_df,
    )
    sizing_lift_df = _build_sizing_lift_df(scenario_metrics_df)

    analysis_start_date = str(dataset_summary_df["dataset_start_date"].min())
    analysis_end_date = str(dataset_summary_df["dataset_end_date"].max())
    holdout_start_date = str(dataset_summary_df["holdout_start_date"].min())
    holdout_end_date = str(dataset_summary_df["holdout_end_date"].max())

    return ProductionStrategyRobustnessAuditResult(
        db_path="multi://backtest-simulation",
        strategy_names=tuple(strategy_names),
        dataset_names=tuple(dataset_names),
        holdout_months=holdout_months,
        holdout_start_date=holdout_start_date,
        holdout_end_date=holdout_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        dataset_summary_df=dataset_summary_df,
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        scenario_metrics_df=scenario_metrics_df,
        comparison_df=comparison_df,
        sizing_lift_df=sizing_lift_df,
    )


def write_production_strategy_robustness_audit_bundle(
    result: ProductionStrategyRobustnessAuditResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=PRODUCTION_STRATEGY_ROBUSTNESS_AUDIT_EXPERIMENT_ID,
        module=__name__,
        function="run_production_strategy_robustness_audit",
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
            "equal_weight_benchmark_df": result.equal_weight_benchmark_df,
            "scenario_metrics_df": result.scenario_metrics_df,
            "comparison_df": result.comparison_df,
            "sizing_lift_df": result.sizing_lift_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_production_strategy_robustness_audit_bundle(
    bundle_path: str | Path,
) -> ProductionStrategyRobustnessAuditResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(
        bundle_path,
        table_names=(
            "dataset_summary_df",
            "equal_weight_benchmark_df",
            "scenario_metrics_df",
            "comparison_df",
            "sizing_lift_df",
        ),
    )
    metadata = dict(info.result_metadata)
    return ProductionStrategyRobustnessAuditResult(
        db_path=str(metadata["db_path"]),
        strategy_names=tuple(metadata["strategy_names"]),
        dataset_names=tuple(metadata["dataset_names"]),
        holdout_months=int(metadata["holdout_months"]),
        holdout_start_date=str(metadata["holdout_start_date"]),
        holdout_end_date=str(metadata["holdout_end_date"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        dataset_summary_df=tables["dataset_summary_df"],
        equal_weight_benchmark_df=tables["equal_weight_benchmark_df"],
        scenario_metrics_df=tables["scenario_metrics_df"],
        comparison_df=tables["comparison_df"],
        sizing_lift_df=tables["sizing_lift_df"],
    )


def get_production_strategy_robustness_audit_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        PRODUCTION_STRATEGY_ROBUSTNESS_AUDIT_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_production_strategy_robustness_audit_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        PRODUCTION_STRATEGY_ROBUSTNESS_AUDIT_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _run_single_scenario(
    *,
    runner: BacktestRunner,
    strategy_name: str,
    dataset_info: dict[str, Any],
    stock_codes: list[str],
    simulation_state_cache: dict[str, _WindowedSimulationState],
    window_label: str,
    window_start_date: str,
    window_end_date: str,
) -> list[dict[str, Any]]:
    try:
        parameters = _build_parameters_for_scenario(
            runner=runner,
            strategy_name=strategy_name,
            config_override={
                "shared_config": {
                    "dataset": dataset_info["dataset_name"],
                    "end_date": window_end_date,
                }
            },
        )
        simulation_state = _get_or_prepare_windowed_simulation_state(
            parameters=parameters,
            dataset_info=dataset_info,
            stock_codes=stock_codes,
            simulation_state_cache=simulation_state_cache,
            window_start_date=window_start_date,
            window_end_date=window_end_date,
        )
        kelly_fraction, min_allocation, max_allocation = _resolve_kelly_config(
            parameters
        )
        simulation_result = _build_windowed_simulation_result(
            simulation_state=simulation_state,
            kelly_fraction=kelly_fraction,
            min_allocation=min_allocation,
            max_allocation=max_allocation,
        )
        payload = build_backtest_report_payload(simulation_result)
    except Exception as exc:
        return [
            _failed_metric_row(
                strategy_name=strategy_name,
                dataset_info=dataset_info,
                window_label=window_label,
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                portfolio_kind=portfolio_kind,
                error=str(exc),
            )
            for portfolio_kind in ("initial", "kelly")
        ]

    rows: list[dict[str, Any]] = []
    for portfolio_kind in ("initial", "kelly"):
        portfolio_payload = payload.get(f"{portfolio_kind}_portfolio")
        rows.append(
            _build_metric_row(
                strategy_name=strategy_name,
                dataset_info=dataset_info,
                window_label=window_label,
                window_start_date=window_start_date,
                window_end_date=window_end_date,
                portfolio_kind=portfolio_kind,
                portfolio_payload=portfolio_payload,
                kelly_fraction=parameters.get("shared_config", {}).get("kelly_fraction"),
            )
        )
    return rows


def _resolve_kelly_config(parameters: dict[str, Any]) -> tuple[float, float, float]:
    shared_config_payload = parameters.get("shared_config", {})
    return (
        float(shared_config_payload.get("kelly_fraction", _DEFAULT_KELLY_FRACTION)),
        float(shared_config_payload.get("min_allocation", _DEFAULT_MIN_ALLOCATION)),
        float(shared_config_payload.get("max_allocation", _DEFAULT_MAX_ALLOCATION)),
    )


def _build_windowed_simulation_cache_key(
    *,
    parameters: dict[str, Any],
    dataset_name: str,
    stock_codes: Sequence[str],
    window_start_date: str,
    window_end_date: str,
) -> str:
    shared_config_payload = {
        key: value
        for key, value in dict(parameters.get("shared_config", {})).items()
        if key not in _WINDOW_SHARED_CONFIG_KEYS
        and key not in _KELLY_ONLY_SHARED_CONFIG_KEYS
    }
    payload = {
        "dataset_name": dataset_name,
        "stock_codes": list(stock_codes),
        "window_start_date": window_start_date,
        "window_end_date": window_end_date,
        "shared_config": shared_config_payload,
        "entry_filter_params": parameters.get("entry_filter_params", {}),
        "exit_trigger_params": parameters.get("exit_trigger_params", {}),
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def _get_or_prepare_windowed_simulation_state(
    *,
    parameters: dict[str, Any],
    dataset_info: dict[str, Any],
    stock_codes: list[str],
    simulation_state_cache: dict[str, _WindowedSimulationState],
    window_start_date: str,
    window_end_date: str,
) -> _WindowedSimulationState:
    cache_key = _build_windowed_simulation_cache_key(
        parameters=parameters,
        dataset_name=dataset_info["dataset_name"],
        stock_codes=stock_codes,
        window_start_date=window_start_date,
        window_end_date=window_end_date,
    )
    cached = simulation_state_cache.get(cache_key)
    if cached is not None:
        return cached

    state = _prepare_windowed_simulation_state(
        parameters=parameters,
        dataset_info=dataset_info,
        stock_codes=stock_codes,
        window_start_date=window_start_date,
        window_end_date=window_end_date,
    )
    simulation_state_cache[cache_key] = state
    return state


def _prepare_windowed_simulation_state(
    *,
    parameters: dict[str, Any],
    dataset_info: dict[str, Any],
    stock_codes: list[str],
    window_start_date: str,
    window_end_date: str,
) -> _WindowedSimulationState:
    warmup_calendar_days = estimate_strategy_indicator_warmup_calendar_days(parameters)
    load_start_date = resolve_window_load_start_date(
        dataset_start_date=dataset_info["dataset_start_date"],
        window_start_date=window_start_date,
        warmup_calendar_days=warmup_calendar_days,
    )

    shared_config_payload = dict(parameters.get("shared_config", {}))
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
    strategy = YamlConfigurableStrategy(
        shared_config=shared_config,
        entry_filter_params=SignalParams.model_validate(
            parameters.get("entry_filter_params", {})
        ),
        exit_trigger_params=SignalParams.model_validate(
            parameters.get("exit_trigger_params", {})
        ),
    )

    strategy.run_multi_backtest()
    open_slice, close_slice, entries_slice, exits_slice = _slice_grouped_inputs_for_window(
        strategy=strategy,
        window_start_date=window_start_date,
        window_end_date=window_end_date,
    )
    strategy._set_grouped_portfolio_inputs_cache(
        open_slice,
        close_slice,
        entries_slice,
        exits_slice,
    )
    initial_allocation_pct = 1.0 / len(strategy.stock_codes)
    initial_portfolio = strategy.run_multi_backtest_from_cached_signals(
        initial_allocation_pct
    )

    return _WindowedSimulationState(
        strategy=strategy,
        initial_portfolio=initial_portfolio,
        all_entries=entries_slice,
    )


def _build_windowed_simulation_result(
    *,
    simulation_state: _WindowedSimulationState,
    kelly_fraction: float,
    min_allocation: float,
    max_allocation: float,
) -> BacktestSimulationResult:
    strategy = simulation_state.strategy
    strategy.combined_portfolio = simulation_state.initial_portfolio
    optimized_allocation, _stats = strategy.optimize_allocation_kelly(
        simulation_state.initial_portfolio,
        kelly_fraction=kelly_fraction,
        min_allocation=min_allocation,
        max_allocation=max_allocation,
    )
    kelly_portfolio = strategy.run_multi_backtest_from_cached_signals(
        optimized_allocation
    )

    return BacktestSimulationResult(
        initial_portfolio=simulation_state.initial_portfolio,
        kelly_portfolio=kelly_portfolio,
        allocation_info=None,
        all_entries=simulation_state.all_entries,
        summary_metrics=None,
        metrics_payload={},
    )


def _slice_grouped_inputs_for_window(
    *,
    strategy: YamlConfigurableStrategy,
    window_start_date: str,
    window_end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cached = strategy._get_grouped_portfolio_inputs_cache()
    if cached is None:
        raise RuntimeError("grouped portfolio inputs cache was not populated")

    open_data, close_data, all_entries, all_exits = cached
    return (
        open_data.loc[window_start_date:window_end_date],
        close_data.loc[window_start_date:window_end_date],
        all_entries.loc[window_start_date:window_end_date],
        all_exits.loc[window_start_date:window_end_date],
    )


def _build_metric_row(
    *,
    strategy_name: str,
    dataset_info: dict[str, Any],
    window_label: str,
    window_start_date: str,
    window_end_date: str,
    portfolio_kind: str,
    portfolio_payload: dict[str, Any] | None,
    kelly_fraction: Any,
) -> dict[str, Any]:
    final_stats = _stats_records_to_dict(
        (portfolio_payload or {}).get("final_stats")
    )
    risk_metrics = (portfolio_payload or {}).get("risk_metrics") or {}
    cagr_pct = _compute_cagr_pct_from_portfolio_payload(portfolio_payload)
    strategy_basename = strategy_name.split("/")[-1]
    is_reference = strategy_name == DEFAULT_REFERENCE_STRATEGY
    return {
        "status": "ok",
        "error": None,
        "strategy_name": strategy_name,
        "strategy_basename": strategy_basename,
        "is_reference": is_reference,
        "dataset_name": dataset_info["dataset_name"],
        "dataset_preset": dataset_info["dataset_preset"],
        "window_label": window_label,
        "window_start_date": window_start_date,
        "window_end_date": window_end_date,
        "portfolio_kind": portfolio_kind,
        "kelly_fraction": _coerce_float(kelly_fraction),
        "total_return_pct": _coerce_float(final_stats.get("Total Return [%]")),
        "cagr_pct": cagr_pct,
        "benchmark_return_pct": _coerce_float(final_stats.get("Benchmark Return [%]")),
        "max_gross_exposure_pct": _coerce_float(
            final_stats.get("Max Gross Exposure [%]")
        ),
        "max_drawdown_pct": _coerce_float(final_stats.get("Max Drawdown [%]")),
        "win_rate_pct": _coerce_float(final_stats.get("Win Rate [%]")),
        "total_trades": _coerce_int(final_stats.get("Total Trades")),
        "sharpe_ratio": _coerce_float(risk_metrics.get("sharpe_ratio")),
        "sortino_ratio": _coerce_float(risk_metrics.get("sortino_ratio")),
        "calmar_ratio": _coerce_float(risk_metrics.get("calmar_ratio")),
        "annualized_volatility": _coerce_float(
            risk_metrics.get("annualized_volatility")
        ),
        "omega_ratio": _coerce_float(risk_metrics.get("omega_ratio")),
    }


def _failed_metric_row(
    *,
    strategy_name: str,
    dataset_info: dict[str, Any],
    window_label: str,
    window_start_date: str,
    window_end_date: str,
    portfolio_kind: str,
    error: str,
) -> dict[str, Any]:
    strategy_basename = strategy_name.split("/")[-1]
    return {
        "status": "failed",
        "error": error,
        "strategy_name": strategy_name,
        "strategy_basename": strategy_basename,
        "is_reference": strategy_name == DEFAULT_REFERENCE_STRATEGY,
        "dataset_name": dataset_info["dataset_name"],
        "dataset_preset": dataset_info["dataset_preset"],
        "window_label": window_label,
        "window_start_date": window_start_date,
        "window_end_date": window_end_date,
        "portfolio_kind": portfolio_kind,
        "kelly_fraction": None,
        "total_return_pct": None,
        "cagr_pct": None,
        "benchmark_return_pct": None,
        "max_gross_exposure_pct": None,
        "max_drawdown_pct": None,
        "win_rate_pct": None,
        "total_trades": None,
        "sharpe_ratio": None,
        "sortino_ratio": None,
        "calmar_ratio": None,
        "annualized_volatility": None,
        "omega_ratio": None,
    }


def _build_comparison_df(
    scenario_metrics_df: pd.DataFrame,
    equal_weight_benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    ok_df = scenario_metrics_df[scenario_metrics_df["status"] == "ok"].copy()
    if ok_df.empty:
        return scenario_metrics_df.iloc[0:0].copy()

    merged = ok_df.merge(
        equal_weight_benchmark_df,
        on=["dataset_name", "dataset_preset", "window_label"],
        how="left",
    )

    reference_df = ok_df[ok_df["is_reference"]].copy()
    reference_df = reference_df[
        [
            "dataset_name",
            "window_label",
            "portfolio_kind",
            "total_return_pct",
            "sharpe_ratio",
            "max_drawdown_pct",
            "total_trades",
        ]
    ].rename(
        columns={
            "total_return_pct": "buy_and_hold_total_return_pct",
            "sharpe_ratio": "buy_and_hold_sharpe_ratio",
            "max_drawdown_pct": "buy_and_hold_max_drawdown_pct",
            "total_trades": "buy_and_hold_total_trades",
        }
    )
    merged = merged.merge(
        reference_df,
        on=["dataset_name", "window_label", "portfolio_kind"],
        how="left",
    )
    merged["excess_return_vs_benchmark_pct"] = (
        merged["total_return_pct"] - merged["benchmark_return_pct"]
    )
    merged["excess_return_vs_equal_weight_pct"] = (
        merged["total_return_pct"] - merged["equal_weight_total_return_pct"]
    )
    merged["beat_equal_weight"] = merged["excess_return_vs_equal_weight_pct"] > 0
    merged["excess_return_vs_buy_and_hold_pct"] = (
        merged["total_return_pct"] - merged["buy_and_hold_total_return_pct"]
    )
    merged["beat_buy_and_hold"] = (
        merged["excess_return_vs_buy_and_hold_pct"] > 0
    )
    return merged.sort_values(
        ["dataset_name", "window_label", "portfolio_kind", "is_reference", "strategy_name"]
    ).reset_index(drop=True)


def _build_sizing_lift_df(scenario_metrics_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = scenario_metrics_df[
        (scenario_metrics_df["status"] == "ok")
        & (~scenario_metrics_df["is_reference"])
    ].copy()
    if ok_df.empty:
        return scenario_metrics_df.iloc[0:0].copy()

    initial_df = ok_df[ok_df["portfolio_kind"] == "initial"].copy()
    kelly_df = ok_df[ok_df["portfolio_kind"] == "kelly"].copy()
    initial_df = initial_df.rename(
        columns={
            "total_return_pct": "initial_total_return_pct",
            "cagr_pct": "initial_cagr_pct",
            "benchmark_return_pct": "initial_benchmark_return_pct",
            "max_gross_exposure_pct": "initial_max_gross_exposure_pct",
            "max_drawdown_pct": "initial_max_drawdown_pct",
            "sharpe_ratio": "initial_sharpe_ratio",
        }
    )
    kelly_df = kelly_df.rename(
        columns={
            "total_return_pct": "kelly_total_return_pct",
            "cagr_pct": "kelly_cagr_pct",
            "benchmark_return_pct": "kelly_benchmark_return_pct",
            "max_gross_exposure_pct": "kelly_max_gross_exposure_pct",
            "max_drawdown_pct": "kelly_max_drawdown_pct",
            "sharpe_ratio": "kelly_sharpe_ratio",
        }
    )
    merged = kelly_df.merge(
        initial_df[
            [
                "strategy_name",
                "dataset_name",
                "window_label",
                "initial_total_return_pct",
                "initial_cagr_pct",
                "initial_benchmark_return_pct",
                "initial_max_gross_exposure_pct",
                "initial_max_drawdown_pct",
                "initial_sharpe_ratio",
            ]
        ],
        on=["strategy_name", "dataset_name", "window_label"],
        how="inner",
    )
    merged["return_lift_pct"] = (
        merged["kelly_total_return_pct"] - merged["initial_total_return_pct"]
    )
    merged["cagr_lift_pct"] = (
        merged["kelly_cagr_pct"] - merged["initial_cagr_pct"]
    )
    merged["gross_exposure_lift_pct"] = (
        merged["kelly_max_gross_exposure_pct"]
        - merged["initial_max_gross_exposure_pct"]
    )
    merged["drawdown_lift_pct"] = (
        merged["kelly_max_drawdown_pct"] - merged["initial_max_drawdown_pct"]
    )
    return merged.sort_values(
        ["dataset_name", "window_label", "strategy_name"]
    ).reset_index(drop=True)


def _compute_cagr_pct_from_portfolio_payload(
    portfolio_payload: dict[str, Any] | None,
) -> float | None:
    if not isinstance(portfolio_payload, dict):
        return None
    value_payload = portfolio_payload.get("value_series")
    if not isinstance(value_payload, dict):
        return None
    index_values = value_payload.get("index")
    raw_values = value_payload.get("values")
    if not isinstance(index_values, list) or not isinstance(raw_values, list):
        return None
    if len(index_values) != len(raw_values) or len(raw_values) < 2:
        return None

    value_series = pd.to_numeric(pd.Series(raw_values), errors="coerce")
    if value_series.isna().all():
        return None
    index = pd.to_datetime(index_values, errors="coerce")
    if index.isna().all():
        return None

    equity_curve = pd.Series(value_series.to_numpy(dtype=float), index=index).dropna()
    if len(equity_curve) < 2:
        return None

    starting_value = float(equity_curve.iloc[0])
    ending_value = float(equity_curve.iloc[-1])
    if (
        not math.isfinite(starting_value)
        or not math.isfinite(ending_value)
        or starting_value <= 0.0
        or ending_value <= 0.0
    ):
        return None

    normalized = equity_curve / starting_value
    trading_day_count = len(normalized) - 1
    if trading_day_count <= 0:
        return None
    cagr = float(normalized.iloc[-1] ** (252.0 / trading_day_count) - 1.0)
    if not math.isfinite(cagr):
        return None
    return cagr * 100.0


def _load_dataset_summary(dataset_name: str, *, holdout_months: int) -> dict[str, Any]:
    manifest_path = (
        get_data_dir() / "datasets" / dataset_name / "manifest.v2.json"
    )
    if not manifest_path.exists():
        raise FileNotFoundError(f"Dataset manifest was not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    date_range = payload.get("dateRange", {})
    dataset_start = str(date_range["min"])
    dataset_max = str(date_range["max"])
    holdout_start_date = _subtract_months_iso(dataset_max, holdout_months)
    train_end_date = _day_before_iso(holdout_start_date)
    if train_end_date < dataset_start:
        train_end_date = None
    return {
        "dataset_name": dataset_name,
        "dataset_preset": payload.get("dataset", {}).get("preset", ""),
        "stocks": payload.get("counts", {}).get("stocks"),
        "stock_data_rows": payload.get("counts", {}).get("stock_data"),
        "dataset_start_date": dataset_start,
        "dataset_end_date": dataset_max,
        "train_end_date": train_end_date,
        "holdout_start_date": holdout_start_date,
        "holdout_end_date": dataset_max,
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


def _compute_equal_weight_benchmark_rows(
    *,
    dataset_info: dict[str, Any],
    windows: Sequence[dict[str, str]],
) -> list[dict[str, Any]]:
    snapshot_dir = get_data_dir() / "datasets" / dataset_info["dataset_name"]
    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        rows: list[dict[str, Any]] = []
        for window in windows:
            benchmark = _compute_equal_weight_benchmark(
                reader,
                window_start_date=window["window_start_date"],
                window_end_date=window["window_end_date"],
            )
            rows.append(
                {
                    "dataset_name": dataset_info["dataset_name"],
                    "dataset_preset": dataset_info["dataset_preset"],
                    "window_label": window["window_label"],
                    **benchmark,
                }
            )
        return rows
    finally:
        reader.close()


def _compute_equal_weight_benchmark(
    reader: DatasetSnapshotReader,
    *,
    window_start_date: str,
    window_end_date: str,
) -> dict[str, Any]:
    row = reader.query_one(
        """
        WITH filtered AS (
            SELECT code, date, close
            FROM stock_data
            WHERE date >= ?
              AND date <= ?
              AND close IS NOT NULL
              AND close > 0
        ),
        per_code AS (
            SELECT
                code,
                arg_min(close, date) AS first_close,
                arg_max(close, date) AS last_close,
                min(date) AS first_date,
                max(date) AS last_date,
                count(*) AS observations
            FROM filtered
            GROUP BY code
        )
        SELECT
            count(*) AS constituent_count,
            avg((last_close / first_close - 1.0) * 100.0) AS equal_weight_total_return_pct,
            median((last_close / first_close - 1.0) * 100.0) AS median_stock_total_return_pct,
            avg(CASE WHEN last_close > first_close THEN 1.0 ELSE 0.0 END) * 100.0 AS winner_ratio_pct,
            min(first_date) AS earliest_first_date,
            max(last_date) AS latest_last_date
        FROM per_code
        WHERE observations >= 2
          AND first_close IS NOT NULL
          AND last_close IS NOT NULL
          AND first_close > 0
        """,
        (window_start_date, window_end_date),
    )
    if row is None:
        return {
            "equal_weight_constituent_count": 0,
            "equal_weight_total_return_pct": None,
            "equal_weight_median_stock_total_return_pct": None,
            "equal_weight_winner_ratio_pct": None,
            "equal_weight_earliest_first_date": None,
            "equal_weight_latest_last_date": None,
        }
    return {
        "equal_weight_constituent_count": _coerce_int(row["constituent_count"]),
        "equal_weight_total_return_pct": _coerce_float(
            row["equal_weight_total_return_pct"]
        ),
        "equal_weight_median_stock_total_return_pct": _coerce_float(
            row["median_stock_total_return_pct"]
        ),
        "equal_weight_winner_ratio_pct": _coerce_float(row["winner_ratio_pct"]),
        "equal_weight_earliest_first_date": _coerce_iso_value(
            row["earliest_first_date"]
        ),
        "equal_weight_latest_last_date": _coerce_iso_value(row["latest_last_date"]),
    }


def _build_parameters_for_scenario(
    *,
    runner: BacktestRunner,
    strategy_name: str,
    config_override: dict[str, Any],
) -> dict[str, Any]:
    if strategy_name != DEFAULT_REFERENCE_STRATEGY:
        return runner.build_parameters_for_strategy(
            strategy_name,
            config_override=config_override,
        )

    reference_config_path = (
        Path(__file__).resolve().parents[3]
        / "config"
        / "strategies"
        / "reference"
        / "buy_and_hold.yaml"
    )
    strategy_config = load_yaml_file(reference_config_path)
    return runner._build_parameters(  # noqa: SLF001
        strategy_config,
        config_override=config_override,
        strategy_name=strategy_name,
    )


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


def _stats_records_to_dict(records: Any) -> dict[str, Any]:
    if not isinstance(records, list):
        return {}
    metrics: dict[str, Any] = {}
    for row in records:
        if not isinstance(row, dict):
            continue
        metric = row.get("metric")
        if metric is None:
            continue
        metrics[str(metric)] = row.get("value")
    return metrics


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _coerce_int(value: Any) -> int | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _coerce_iso_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _build_summary_markdown(
    result: ProductionStrategyRobustnessAuditResult,
) -> str:
    lines = [
        "# Production Strategy Robustness Audit",
        "",
        "## Scope",
        "",
        f"- Strategies: `{', '.join(result.strategy_names)}`",
        f"- Datasets: `{', '.join(result.dataset_names)}`",
        (
            f"- Holdout window: `{result.holdout_start_date} -> "
            f"{result.holdout_end_date}` ({result.holdout_months} months)"
        ),
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
        "- `equal_weight_benchmark_df`",
        "- `scenario_metrics_df`",
        "- `comparison_df`",
        "- `sizing_lift_df`",
        ]
    )
    return "\n".join(lines)


def _build_key_read_lines(
    result: ProductionStrategyRobustnessAuditResult,
) -> list[str]:
    comparison_df = result.comparison_df
    sizing_lift_df = result.sizing_lift_df
    if comparison_df.empty:
        return ["- No successful audit rows were produced."]

    lines: list[str] = []
    for dataset_name in result.dataset_names:
        for strategy_basename in ("forward_eps_driven", "range_break_v15"):
            holdout_kelly = _select_metric_row(
                comparison_df,
                strategy_basename=strategy_basename,
                dataset_name=dataset_name,
                window_label=f"holdout_{result.holdout_months}m",
                portfolio_kind="kelly",
            )
            if holdout_kelly is None:
                continue
            holdout_initial = _select_metric_row(
                comparison_df,
                strategy_basename=strategy_basename,
                dataset_name=dataset_name,
                window_label=f"holdout_{result.holdout_months}m",
                portfolio_kind="initial",
            )
            lines.append(
                (
                    f"- `{dataset_name}` holdout / `{strategy_basename}`: "
                    f"kelly total return `{_fmt_pct(holdout_kelly['total_return_pct'])}` "
                    f"(vs equal-weight `{_fmt_pct(holdout_kelly['equal_weight_total_return_pct'])}`, "
                    f"vs benchmark `{_fmt_pct(holdout_kelly['benchmark_return_pct'])}`), "
                    f"initial total return `{_fmt_pct((holdout_initial or {}).get('total_return_pct'))}`."
                )
            )

    if not sizing_lift_df.empty:
        biggest_lift = sizing_lift_df.sort_values(
            "return_lift_pct", ascending=False
        ).iloc[0]
        lines.append(
            (
                f"- Largest Kelly lift: `{biggest_lift['strategy_name']}` on "
                f"`{biggest_lift['dataset_name']}` / `{biggest_lift['window_label']}` "
                f"with return lift `{_fmt_pct(biggest_lift['return_lift_pct'])}` and "
                f"gross-exposure lift `{_fmt_pct(biggest_lift['gross_exposure_lift_pct'])}`."
            )
        )

    raw_underperformance = comparison_df[
        (comparison_df["portfolio_kind"] == "initial")
        & (~comparison_df["is_reference"])
        & (comparison_df["excess_return_vs_benchmark_pct"] < 0)
    ]
    if not raw_underperformance.empty:
        lines.append(
            (
                f"- Initial portfolios lagged benchmark in `{len(raw_underperformance)}` "
                "scenario rows. For these sparse-entry strategies that mostly reflects "
                "idle-capital drag before Kelly concentration rather than automatically "
                "invalidating the underlying filter."
            )
        )
    return lines


def _build_published_summary(
    result: ProductionStrategyRobustnessAuditResult,
) -> dict[str, Any]:
    comparison_df = result.comparison_df
    sizing_lift_df = result.sizing_lift_df
    published: dict[str, Any] = {
        "strategyNames": list(result.strategy_names),
        "datasetNames": list(result.dataset_names),
        "holdoutMonths": result.holdout_months,
        "holdoutStartDate": result.holdout_start_date,
        "holdoutEndDate": result.holdout_end_date,
        "scenarioCount": int(len(result.scenario_metrics_df)),
        "successfulScenarioCount": int(
            (result.scenario_metrics_df["status"] == "ok").sum()
        ),
    }
    if not comparison_df.empty:
        best_holdout = comparison_df[
            (comparison_df["window_label"] == f"holdout_{result.holdout_months}m")
            & (comparison_df["portfolio_kind"] == "kelly")
            & (~comparison_df["is_reference"])
        ].sort_values("total_return_pct", ascending=False)
        if not best_holdout.empty:
            row = best_holdout.iloc[0]
            published["bestHoldoutKelly"] = {
                "strategyName": row["strategy_name"],
                "datasetName": row["dataset_name"],
                "totalReturnPct": row["total_return_pct"],
                "benchmarkReturnPct": row["benchmark_return_pct"],
                "equalWeightTotalReturnPct": row["equal_weight_total_return_pct"],
                "buyAndHoldTotalReturnPct": row["buy_and_hold_total_return_pct"],
            }
    if not sizing_lift_df.empty:
        row = sizing_lift_df.sort_values("return_lift_pct", ascending=False).iloc[0]
        published["largestSizingLift"] = {
            "strategyName": row["strategy_name"],
            "datasetName": row["dataset_name"],
            "windowLabel": row["window_label"],
            "returnLiftPct": row["return_lift_pct"],
            "grossExposureLiftPct": row["gross_exposure_lift_pct"],
        }
    return published


def _select_metric_row(
    comparison_df: pd.DataFrame,
    *,
    strategy_basename: str,
    dataset_name: str,
    window_label: str,
    portfolio_kind: str,
) -> dict[str, Any] | None:
    filtered = comparison_df[
        (comparison_df["strategy_basename"] == strategy_basename)
        & (comparison_df["dataset_name"] == dataset_name)
        & (comparison_df["window_label"] == window_label)
        & (comparison_df["portfolio_kind"] == portfolio_kind)
    ]
    if filtered.empty:
        return None
    return filtered.iloc[0].to_dict()


def _fmt_pct(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "N/A"
    return f"{parsed:.2f}%"


__all__ = [
    "DEFAULT_DATASET_NAMES",
    "DEFAULT_REFERENCE_STRATEGY",
    "DEFAULT_STRATEGY_NAMES",
    "PRODUCTION_STRATEGY_ROBUSTNESS_AUDIT_EXPERIMENT_ID",
    "ProductionStrategyRobustnessAuditResult",
    "get_production_strategy_robustness_audit_bundle_path_for_run_id",
    "get_production_strategy_robustness_audit_latest_bundle_path",
    "load_production_strategy_robustness_audit_bundle",
    "run_production_strategy_robustness_audit",
    "write_production_strategy_robustness_audit_bundle",
]
