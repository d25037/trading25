"""Focused rolling-window threshold study for forward_eps_driven."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from src.domains.analytics.production_strategy_robustness_audit import (
    _build_metric_row,
    _build_parameters_for_scenario,
    _compute_equal_weight_benchmark_rows,
    _failed_metric_row,
    _load_dataset_summary,
    _resolve_kelly_config,
    _subtract_months_iso,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.backtest.core.report_payload import build_backtest_report_payload
from src.domains.backtest.core.runner import BacktestRunner
from src.domains.backtest.core.simulation import BacktestSimulationResult
from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.infrastructure.data_access.loaders import get_stock_list
from src.infrastructure.data_access.mode import data_access_mode_context
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams

FORWARD_EPS_THRESHOLD_WINDOW_STUDY_EXPERIMENT_ID = (
    "strategy-audit/forward-eps-threshold-window-study"
)
DEFAULT_DATASET_NAME = "primeExTopix500_20260325"
DEFAULT_BASELINE_STRATEGY_NAME = "experimental/robustness/forward_eps_driven"
DEFAULT_STRATEGY_NAMES = (
    DEFAULT_BASELINE_STRATEGY_NAME,
    "experimental/robustness/forward_eps_driven_forward_eps_0_35",
    "experimental/robustness/forward_eps_driven_forward_eps_0_4",
    "experimental/robustness/forward_eps_driven_forward_eps_0_45",
    "experimental/robustness/forward_eps_driven_forward_eps_0_5",
)


@dataclass(frozen=True)
class ForwardEpsThresholdWindowStudyResult:
    db_path: str
    strategy_names: tuple[str, ...]
    dataset_name: str
    baseline_strategy_name: str
    rolling_months: int
    rolling_step_months: int
    analysis_start_date: str
    analysis_end_date: str
    dataset_summary_df: pd.DataFrame
    equal_weight_benchmark_df: pd.DataFrame
    window_metrics_df: pd.DataFrame
    comparison_df: pd.DataFrame
    rolling_summary_df: pd.DataFrame


@dataclass(frozen=True)
class _FullHistorySimulationState:
    strategy: YamlConfigurableStrategy
    full_open_data: pd.DataFrame
    full_close_data: pd.DataFrame
    full_entries: pd.DataFrame
    full_exits: pd.DataFrame


def run_forward_eps_threshold_window_study(
    *,
    strategy_names: Sequence[str] = DEFAULT_STRATEGY_NAMES,
    dataset_name: str = DEFAULT_DATASET_NAME,
    baseline_strategy_name: str = DEFAULT_BASELINE_STRATEGY_NAME,
    rolling_months: int = 6,
    rolling_step_months: int = 1,
) -> ForwardEpsThresholdWindowStudyResult:
    if rolling_months <= 0:
        raise ValueError("rolling_months must be greater than 0")
    if rolling_step_months <= 0:
        raise ValueError("rolling_step_months must be greater than 0")
    if not strategy_names:
        raise ValueError("strategy_names must not be empty")
    if baseline_strategy_name not in strategy_names:
        raise ValueError("baseline_strategy_name must be included in strategy_names")

    dataset_info = _load_dataset_summary(dataset_name, holdout_months=rolling_months)
    windows = _build_rolling_analysis_windows(
        dataset_info=dataset_info,
        rolling_months=rolling_months,
        rolling_step_months=rolling_step_months,
    )
    window_type_by_label = {
        window["window_label"]: window["window_type"] for window in windows
    }
    dataset_summary_df = pd.DataFrame(
        [
            {
                **dataset_info,
                "rolling_months": rolling_months,
                "rolling_step_months": rolling_step_months,
                "rolling_window_count": sum(
                    1 for window in windows if window["window_type"] == "rolling"
                ),
            }
        ]
    )

    equal_weight_benchmark_df = pd.DataFrame(
        _compute_equal_weight_benchmark_rows(
            dataset_info=dataset_info,
            windows=windows,
        )
    )
    if not equal_weight_benchmark_df.empty:
        equal_weight_benchmark_df["window_type"] = equal_weight_benchmark_df[
            "window_label"
        ].map(window_type_by_label)

    runner = BacktestRunner()
    parameters_by_strategy = {
        strategy_name: _build_parameters_for_scenario(
            runner=runner,
            strategy_name=strategy_name,
            config_override={
                "shared_config": {
                    "dataset": dataset_name,
                    "end_date": dataset_info["dataset_end_date"],
                }
            },
        )
        for strategy_name in strategy_names
    }

    metric_rows: list[dict[str, Any]] = []
    with data_access_mode_context("direct"):
        stock_codes = get_stock_list(dataset_name)
        shared_multi_data_dict, shared_benchmark_data = _prepare_shared_data_cache(
            parameters=parameters_by_strategy[baseline_strategy_name],
            dataset_info=dataset_info,
            stock_codes=stock_codes,
        )
        state_by_strategy = {
            strategy_name: _prepare_full_history_simulation_state(
                parameters=parameters_by_strategy[strategy_name],
                dataset_info=dataset_info,
                stock_codes=stock_codes,
                shared_multi_data_dict=shared_multi_data_dict,
                shared_benchmark_data=shared_benchmark_data,
            )
            for strategy_name in strategy_names
        }
        for strategy_name in strategy_names:
            state = state_by_strategy[strategy_name]
            parameters = parameters_by_strategy[strategy_name]
            threshold = _extract_forward_eps_growth_threshold(parameters)
            risk_adjusted_return_threshold = _extract_risk_adjusted_return_threshold(
                parameters
            )
            volume_ratio_above_threshold = _extract_volume_ratio_above_threshold(
                parameters
            )
            volume_ratio_above_short_period = _extract_volume_ratio_above_short_period(
                parameters
            )
            volume_ratio_above_long_period = _extract_volume_ratio_above_long_period(
                parameters
            )
            kelly_fraction, min_allocation, max_allocation = _resolve_kelly_config(
                parameters
            )
            for window in windows:
                metric_rows.extend(
                    _evaluate_window(
                        state=state,
                        strategy_name=strategy_name,
                        dataset_info=dataset_info,
                        window=window,
                        threshold=threshold,
                        risk_adjusted_return_threshold=risk_adjusted_return_threshold,
                        volume_ratio_above_threshold=volume_ratio_above_threshold,
                        volume_ratio_above_short_period=(
                            volume_ratio_above_short_period
                        ),
                        volume_ratio_above_long_period=volume_ratio_above_long_period,
                        kelly_fraction=kelly_fraction,
                        min_allocation=min_allocation,
                        max_allocation=max_allocation,
                    )
                )

    window_metrics_df = pd.DataFrame(metric_rows)
    if window_metrics_df.empty:
        raise RuntimeError("forward eps threshold study produced no metric rows")

    comparison_df = _build_threshold_comparison_df(
        window_metrics_df=window_metrics_df,
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        baseline_strategy_name=baseline_strategy_name,
    )
    rolling_summary_df = _build_rolling_summary_df(comparison_df)

    return ForwardEpsThresholdWindowStudyResult(
        db_path="multi://forward-eps-threshold-window-study",
        strategy_names=tuple(strategy_names),
        dataset_name=dataset_name,
        baseline_strategy_name=baseline_strategy_name,
        rolling_months=rolling_months,
        rolling_step_months=rolling_step_months,
        analysis_start_date=dataset_info["dataset_start_date"],
        analysis_end_date=dataset_info["dataset_end_date"],
        dataset_summary_df=dataset_summary_df,
        equal_weight_benchmark_df=equal_weight_benchmark_df,
        window_metrics_df=window_metrics_df,
        comparison_df=comparison_df,
        rolling_summary_df=rolling_summary_df,
    )


def write_forward_eps_threshold_window_study_bundle(
    result: ForwardEpsThresholdWindowStudyResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=FORWARD_EPS_THRESHOLD_WINDOW_STUDY_EXPERIMENT_ID,
        module=__name__,
        function="run_forward_eps_threshold_window_study",
        params={
            "strategy_names": list(result.strategy_names),
            "dataset_name": result.dataset_name,
            "baseline_strategy_name": result.baseline_strategy_name,
            "rolling_months": result.rolling_months,
            "rolling_step_months": result.rolling_step_months,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "strategy_names": list(result.strategy_names),
            "dataset_name": result.dataset_name,
            "baseline_strategy_name": result.baseline_strategy_name,
            "rolling_months": result.rolling_months,
            "rolling_step_months": result.rolling_step_months,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
        },
        result_tables={
            "dataset_summary_df": result.dataset_summary_df,
            "equal_weight_benchmark_df": result.equal_weight_benchmark_df,
            "window_metrics_df": result.window_metrics_df,
            "comparison_df": result.comparison_df,
            "rolling_summary_df": result.rolling_summary_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_forward_eps_threshold_window_study_bundle(
    bundle_path: str | Path,
) -> ForwardEpsThresholdWindowStudyResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(
        bundle_path,
        table_names=(
            "dataset_summary_df",
            "equal_weight_benchmark_df",
            "window_metrics_df",
            "comparison_df",
            "rolling_summary_df",
        ),
    )
    metadata = dict(info.result_metadata)
    return ForwardEpsThresholdWindowStudyResult(
        db_path=str(metadata["db_path"]),
        strategy_names=tuple(metadata["strategy_names"]),
        dataset_name=str(metadata["dataset_name"]),
        baseline_strategy_name=str(metadata["baseline_strategy_name"]),
        rolling_months=int(metadata["rolling_months"]),
        rolling_step_months=int(metadata["rolling_step_months"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        dataset_summary_df=tables["dataset_summary_df"],
        equal_weight_benchmark_df=tables["equal_weight_benchmark_df"],
        window_metrics_df=tables["window_metrics_df"],
        comparison_df=tables["comparison_df"],
        rolling_summary_df=tables["rolling_summary_df"],
    )


def get_forward_eps_threshold_window_study_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FORWARD_EPS_THRESHOLD_WINDOW_STUDY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_forward_eps_threshold_window_study_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FORWARD_EPS_THRESHOLD_WINDOW_STUDY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_rolling_analysis_windows(
    *,
    dataset_info: dict[str, Any],
    rolling_months: int,
    rolling_step_months: int,
) -> tuple[dict[str, str], ...]:
    windows: list[dict[str, str]] = [
        {
            "window_label": "full",
            "window_start_date": dataset_info["dataset_start_date"],
            "window_end_date": dataset_info["dataset_end_date"],
            "window_type": "full",
        }
    ]
    rolling_windows: list[dict[str, str]] = []
    current_end_date = dataset_info["dataset_end_date"]
    dataset_start_date = dataset_info["dataset_start_date"]

    while True:
        current_start_date = _subtract_months_iso(current_end_date, rolling_months)
        if current_start_date < dataset_start_date:
            break
        rolling_windows.append(
            {
                "window_label": (
                    f"rolling_{rolling_months}m_"
                    f"{current_start_date}_{current_end_date}"
                ),
                "window_start_date": current_start_date,
                "window_end_date": current_end_date,
                "window_type": "rolling",
            }
        )
        next_end_date = _subtract_months_iso(current_end_date, rolling_step_months)
        if next_end_date >= current_end_date:
            break
        current_end_date = next_end_date

    rolling_windows.reverse()
    windows.extend(rolling_windows)
    return tuple(windows)


def _prepare_full_history_simulation_state(
    *,
    parameters: dict[str, Any],
    dataset_info: dict[str, Any],
    stock_codes: list[str],
    shared_multi_data_dict: dict[str, dict[str, pd.DataFrame]] | None = None,
    shared_benchmark_data: pd.DataFrame | None = None,
) -> _FullHistorySimulationState:
    shared_config_payload = dict(parameters.get("shared_config", {}))
    shared_config_payload.update(
        {
            "dataset": dataset_info["dataset_name"],
            "start_date": dataset_info["dataset_start_date"],
            "end_date": dataset_info["dataset_end_date"],
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
    if shared_multi_data_dict is not None:
        strategy.multi_data_dict = shared_multi_data_dict
    if shared_benchmark_data is not None:
        strategy.benchmark_data = shared_benchmark_data
    strategy.run_multi_backtest()
    cached = strategy._get_grouped_portfolio_inputs_cache()
    if cached is None:
        raise RuntimeError("grouped portfolio inputs cache was not populated")
    open_data, close_data, all_entries, all_exits = cached
    return _FullHistorySimulationState(
        strategy=strategy,
        full_open_data=open_data,
        full_close_data=close_data,
        full_entries=all_entries,
        full_exits=all_exits,
    )


def _prepare_shared_data_cache(
    *,
    parameters: dict[str, Any],
    dataset_info: dict[str, Any],
    stock_codes: list[str],
) -> tuple[dict[str, dict[str, pd.DataFrame]], pd.DataFrame | None]:
    """Load shared universe data once and reuse it across nearby strategy variants."""
    shared_config_payload = dict(parameters.get("shared_config", {}))
    shared_config_payload.update(
        {
            "dataset": dataset_info["dataset_name"],
            "start_date": dataset_info["dataset_start_date"],
            "end_date": dataset_info["dataset_end_date"],
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
    shared_multi_data_dict = strategy.load_multi_data()
    shared_benchmark_data = None
    if strategy._should_load_benchmark():
        shared_benchmark_data = strategy.load_benchmark_data()
    return shared_multi_data_dict, shared_benchmark_data


def _evaluate_window(
    *,
    state: _FullHistorySimulationState,
    strategy_name: str,
    dataset_info: dict[str, Any],
    window: dict[str, str],
    threshold: float | None,
    risk_adjusted_return_threshold: float | None,
    volume_ratio_above_threshold: float | None,
    volume_ratio_above_short_period: int | None,
    volume_ratio_above_long_period: int | None,
    kelly_fraction: float,
    min_allocation: float,
    max_allocation: float,
) -> list[dict[str, Any]]:
    try:
        strategy = state.strategy
        open_slice = state.full_open_data.loc[
            window["window_start_date"] : window["window_end_date"]
        ]
        close_slice = state.full_close_data.loc[
            window["window_start_date"] : window["window_end_date"]
        ]
        entries_slice = state.full_entries.loc[
            window["window_start_date"] : window["window_end_date"]
        ]
        exits_slice = state.full_exits.loc[
            window["window_start_date"] : window["window_end_date"]
        ]
        if open_slice.empty or close_slice.empty:
            raise RuntimeError("window slice produced no market data")

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
        strategy.combined_portfolio = initial_portfolio
        optimized_allocation, _stats = strategy.optimize_allocation_kelly(
            initial_portfolio,
            kelly_fraction=kelly_fraction,
            min_allocation=min_allocation,
            max_allocation=max_allocation,
        )
        kelly_portfolio = strategy.run_multi_backtest_from_cached_signals(
            optimized_allocation
        )
        simulation_result = BacktestSimulationResult(
            initial_portfolio=initial_portfolio,
            kelly_portfolio=kelly_portfolio,
            allocation_info=None,
            all_entries=entries_slice,
            summary_metrics=None,
            metrics_payload={},
        )
        payload = build_backtest_report_payload(simulation_result)
    except Exception as exc:
        return [
            _decorate_metric_row(
                _failed_metric_row(
                    strategy_name=strategy_name,
                    dataset_info=dataset_info,
                    window_label=window["window_label"],
                    window_start_date=window["window_start_date"],
                    window_end_date=window["window_end_date"],
                    portfolio_kind=portfolio_kind,
                    error=str(exc),
                ),
                window_type=window["window_type"],
                threshold=threshold,
                risk_adjusted_return_threshold=risk_adjusted_return_threshold,
                volume_ratio_above_threshold=volume_ratio_above_threshold,
                volume_ratio_above_short_period=volume_ratio_above_short_period,
                volume_ratio_above_long_period=volume_ratio_above_long_period,
                is_baseline=False,
            )
            for portfolio_kind in ("initial", "kelly")
        ]

    return [
        _decorate_metric_row(
            _build_metric_row(
                strategy_name=strategy_name,
                dataset_info=dataset_info,
                window_label=window["window_label"],
                window_start_date=window["window_start_date"],
                window_end_date=window["window_end_date"],
                portfolio_kind=portfolio_kind,
                portfolio_payload=payload.get(f"{portfolio_kind}_portfolio"),
                kelly_fraction=kelly_fraction,
            ),
            window_type=window["window_type"],
            threshold=threshold,
            risk_adjusted_return_threshold=risk_adjusted_return_threshold,
            volume_ratio_above_threshold=volume_ratio_above_threshold,
            volume_ratio_above_short_period=volume_ratio_above_short_period,
            volume_ratio_above_long_period=volume_ratio_above_long_period,
            is_baseline=False,
        )
        for portfolio_kind in ("initial", "kelly")
    ]


def _decorate_metric_row(
    row: dict[str, Any],
    *,
    window_type: str,
    threshold: float | None,
    risk_adjusted_return_threshold: float | None,
    volume_ratio_above_threshold: float | None,
    volume_ratio_above_short_period: int | None,
    volume_ratio_above_long_period: int | None,
    is_baseline: bool,
) -> dict[str, Any]:
    decorated = dict(row)
    decorated["window_type"] = window_type
    decorated["forward_eps_growth_threshold"] = threshold
    decorated["risk_adjusted_return_threshold"] = risk_adjusted_return_threshold
    decorated["volume_ratio_above_threshold"] = volume_ratio_above_threshold
    decorated["volume_ratio_above_short_period"] = volume_ratio_above_short_period
    decorated["volume_ratio_above_long_period"] = volume_ratio_above_long_period
    decorated["is_baseline_threshold"] = is_baseline
    return decorated


def _extract_forward_eps_growth_threshold(parameters: dict[str, Any]) -> float | None:
    fundamental = (
        parameters.get("entry_filter_params", {})
        .get("fundamental", {})
        .get("forward_eps_growth", {})
    )
    threshold = fundamental.get("threshold")
    try:
        return float(threshold) if threshold is not None else None
    except (TypeError, ValueError):
        return None


def _extract_risk_adjusted_return_threshold(parameters: dict[str, Any]) -> float | None:
    risk_adjusted_return = parameters.get("entry_filter_params", {}).get(
        "risk_adjusted_return", {}
    )
    threshold = risk_adjusted_return.get("threshold")
    try:
        return float(threshold) if threshold is not None else None
    except (TypeError, ValueError):
        return None


def _extract_volume_ratio_above_threshold(parameters: dict[str, Any]) -> float | None:
    volume_ratio_above = parameters.get("entry_filter_params", {}).get(
        "volume_ratio_above", {}
    )
    threshold = volume_ratio_above.get("ratio_threshold")
    try:
        return float(threshold) if threshold is not None else None
    except (TypeError, ValueError):
        return None


def _extract_volume_ratio_above_short_period(
    parameters: dict[str, Any],
) -> int | None:
    volume_ratio_above = parameters.get("entry_filter_params", {}).get(
        "volume_ratio_above", {}
    )
    short_period = volume_ratio_above.get("short_period")
    try:
        return int(short_period) if short_period is not None else None
    except (TypeError, ValueError):
        return None


def _extract_volume_ratio_above_long_period(
    parameters: dict[str, Any],
) -> int | None:
    volume_ratio_above = parameters.get("entry_filter_params", {}).get(
        "volume_ratio_above", {}
    )
    long_period = volume_ratio_above.get("long_period")
    try:
        return int(long_period) if long_period is not None else None
    except (TypeError, ValueError):
        return None


def _build_threshold_comparison_df(
    *,
    window_metrics_df: pd.DataFrame,
    equal_weight_benchmark_df: pd.DataFrame,
    baseline_strategy_name: str,
) -> pd.DataFrame:
    ok_df = window_metrics_df[window_metrics_df["status"] == "ok"].copy()
    if ok_df.empty:
        return window_metrics_df.iloc[0:0].copy()

    merged = ok_df.merge(
        equal_weight_benchmark_df,
        on=["dataset_name", "dataset_preset", "window_label", "window_type"],
        how="left",
    )
    merged["excess_return_vs_equal_weight_pct"] = (
        merged["total_return_pct"] - merged["equal_weight_total_return_pct"]
    )
    merged["beat_equal_weight"] = merged["excess_return_vs_equal_weight_pct"] > 0

    baseline_df = merged[
        merged["strategy_name"] == baseline_strategy_name
    ].copy()
    baseline_df = baseline_df[
        [
            "dataset_name",
            "window_label",
            "window_type",
            "portfolio_kind",
            "total_return_pct",
            "max_drawdown_pct",
            "total_trades",
            "sharpe_ratio",
        ]
    ].rename(
        columns={
            "total_return_pct": "original_total_return_pct",
            "max_drawdown_pct": "original_max_drawdown_pct",
            "total_trades": "original_total_trades",
            "sharpe_ratio": "original_sharpe_ratio",
        }
    )
    merged = merged.merge(
        baseline_df,
        on=["dataset_name", "window_label", "window_type", "portfolio_kind"],
        how="left",
    )
    merged["excess_return_vs_original_pct"] = (
        merged["total_return_pct"] - merged["original_total_return_pct"]
    )
    merged["drawdown_delta_vs_original_pct"] = (
        merged["max_drawdown_pct"] - merged["original_max_drawdown_pct"]
    )
    merged["beat_original_return"] = merged["excess_return_vs_original_pct"] > 0
    merged["improve_original_drawdown"] = (
        merged["drawdown_delta_vs_original_pct"] < 0
    )
    merged["joint_improvement_vs_original"] = (
        merged["beat_original_return"] & merged["improve_original_drawdown"]
    )
    sort_columns = [
        column
        for column in (
            "window_type",
            "window_end_date",
            "portfolio_kind",
            "strategy_name",
        )
        if column in merged.columns
    ]
    if sort_columns:
        merged = merged.sort_values(sort_columns)
    return merged.reset_index(drop=True)


def _build_rolling_summary_df(comparison_df: pd.DataFrame) -> pd.DataFrame:
    rolling_df = comparison_df[comparison_df["window_type"] == "rolling"].copy()
    if rolling_df.empty:
        return comparison_df.iloc[0:0].copy()

    for column in (
        "risk_adjusted_return_threshold",
        "volume_ratio_above_threshold",
        "volume_ratio_above_short_period",
        "volume_ratio_above_long_period",
    ):
        if column not in rolling_df.columns:
            rolling_df[column] = None

    rows: list[dict[str, Any]] = []
    grouped = rolling_df.groupby(
        [
            "strategy_name",
            "strategy_basename",
            "portfolio_kind",
            "forward_eps_growth_threshold",
            "risk_adjusted_return_threshold",
            "volume_ratio_above_threshold",
            "volume_ratio_above_short_period",
            "volume_ratio_above_long_period",
        ],
        dropna=False,
    )
    for keys, group in grouped:
        (
            strategy_name,
            strategy_basename,
            portfolio_kind,
            threshold,
            risk_adjusted_return_threshold,
            volume_ratio_above_threshold,
            volume_ratio_above_short_period,
            volume_ratio_above_long_period,
        ) = keys
        total_return = pd.to_numeric(group["total_return_pct"], errors="coerce")
        max_drawdown = pd.to_numeric(group["max_drawdown_pct"], errors="coerce")
        sharpe = pd.to_numeric(group["sharpe_ratio"], errors="coerce")
        total_trades = pd.to_numeric(group["total_trades"], errors="coerce")
        excess_equal_weight = pd.to_numeric(
            group["excess_return_vs_equal_weight_pct"], errors="coerce"
        )
        excess_original = pd.to_numeric(
            group["excess_return_vs_original_pct"], errors="coerce"
        )
        drawdown_delta = pd.to_numeric(
            group["drawdown_delta_vs_original_pct"], errors="coerce"
        )
        rows.append(
            {
                "strategy_name": strategy_name,
                "strategy_basename": strategy_basename,
                "portfolio_kind": portfolio_kind,
                "forward_eps_growth_threshold": threshold,
                "risk_adjusted_return_threshold": risk_adjusted_return_threshold,
                "volume_ratio_above_threshold": volume_ratio_above_threshold,
                "volume_ratio_above_short_period": volume_ratio_above_short_period,
                "volume_ratio_above_long_period": volume_ratio_above_long_period,
                "rolling_window_count": int(len(group)),
                "avg_total_return_pct": _series_stat(total_return, "mean"),
                "median_total_return_pct": _series_stat(total_return, "median"),
                "positive_return_window_ratio_pct": _bool_ratio_pct(
                    total_return > 0
                ),
                "beat_equal_weight_window_ratio_pct": _bool_ratio_pct(
                    group["beat_equal_weight"]
                ),
                "beat_original_return_window_ratio_pct": _bool_ratio_pct(
                    group["beat_original_return"]
                ),
                "improve_original_drawdown_window_ratio_pct": _bool_ratio_pct(
                    group["improve_original_drawdown"]
                ),
                "joint_improvement_window_ratio_pct": _bool_ratio_pct(
                    group["joint_improvement_vs_original"]
                ),
                "avg_excess_return_vs_equal_weight_pct": _series_stat(
                    excess_equal_weight, "mean"
                ),
                "avg_excess_return_vs_original_pct": _series_stat(
                    excess_original, "mean"
                ),
                "avg_sharpe_ratio": _series_stat(sharpe, "mean"),
                "avg_max_drawdown_pct": _series_stat(max_drawdown, "mean"),
                "median_max_drawdown_pct": _series_stat(max_drawdown, "median"),
                "worst_max_drawdown_pct": _series_stat(max_drawdown, "max"),
                "avg_drawdown_delta_vs_original_pct": _series_stat(
                    drawdown_delta, "mean"
                ),
                "worst_drawdown_delta_vs_original_pct": _series_stat(
                    drawdown_delta, "max"
                ),
                "avg_total_trades": _series_stat(total_trades, "mean"),
            }
        )
    return pd.DataFrame(rows).sort_values(
        [
            "portfolio_kind",
            "forward_eps_growth_threshold",
            "risk_adjusted_return_threshold",
            "volume_ratio_above_threshold",
            "volume_ratio_above_short_period",
            "volume_ratio_above_long_period",
            "strategy_name",
        ]
    )


def _series_stat(series: pd.Series, stat: str) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(getattr(numeric, stat)())


def _bool_ratio_pct(series: pd.Series) -> float | None:
    normalized = series.dropna()
    if normalized.empty:
        return None
    return float(normalized.astype(bool).mean() * 100.0)


def _build_summary_markdown(
    result: ForwardEpsThresholdWindowStudyResult,
) -> str:
    lines = [
        "# Forward EPS Threshold Window Study",
        "",
        "## Scope",
        "",
        f"- Strategies: `{', '.join(result.strategy_names)}`",
        f"- Dataset: `{result.dataset_name}`",
        f"- Baseline strategy: `{result.baseline_strategy_name}`",
        (
            f"- Rolling windows: `{result.rolling_months}m` stepped every "
            f"`{result.rolling_step_months}m` across `{result.analysis_start_date} -> "
            f"{result.analysis_end_date}`"
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
            "- `window_metrics_df`",
            "- `comparison_df`",
            "- `rolling_summary_df`",
        ]
    )
    return "\n".join(lines)


def _build_key_read_lines(
    result: ForwardEpsThresholdWindowStudyResult,
) -> list[str]:
    comparison_df = result.comparison_df
    rolling_summary_df = result.rolling_summary_df
    if comparison_df.empty or rolling_summary_df.empty:
        return ["- No successful study rows were produced."]

    lines: list[str] = []
    full_kelly = comparison_df[
        (comparison_df["window_label"] == "full")
        & (comparison_df["portfolio_kind"] == "kelly")
    ]
    baseline_full = full_kelly[
        full_kelly["strategy_name"] == result.baseline_strategy_name
    ]
    best_full = full_kelly.sort_values("total_return_pct", ascending=False)
    if not baseline_full.empty and not best_full.empty:
        baseline_row = baseline_full.iloc[0]
        best_row = best_full.iloc[0]
        lines.append(
            (
                "- Full Kelly / baseline vs best variant: "
                f"return `{_fmt_pct(baseline_row['total_return_pct'])} -> "
                f"{_fmt_pct(best_row['total_return_pct'])}`, "
                f"max DD `{_fmt_pct(baseline_row['max_drawdown_pct'])} -> "
                f"{_fmt_pct(best_row['max_drawdown_pct'])}`, "
                f"Sharpe `{_fmt_num(baseline_row['sharpe_ratio'])} -> "
                f"{_fmt_num(best_row['sharpe_ratio'])}` "
                f"for `{_format_variant(best_row)}`."
            )
        )

    rolling_kelly = rolling_summary_df[
        rolling_summary_df["portfolio_kind"] == "kelly"
    ]
    baseline_summary = rolling_kelly[
        rolling_kelly["strategy_name"] == result.baseline_strategy_name
    ]
    best_avg_return = rolling_kelly.sort_values(
        "avg_total_return_pct", ascending=False
    )
    if not baseline_summary.empty and not best_avg_return.empty:
        baseline_row = baseline_summary.iloc[0]
        best_row = best_avg_return.iloc[0]
        lines.append(
            (
                f"- Rolling {result.rolling_months}m Kelly / baseline vs best avg-return variant: "
                f"avg return `{_fmt_pct(baseline_row['avg_total_return_pct'])} -> "
                f"{_fmt_pct(best_row['avg_total_return_pct'])}`, "
                f"median return `{_fmt_pct(baseline_row['median_total_return_pct'])} -> "
                f"{_fmt_pct(best_row['median_total_return_pct'])}`, "
                f"avg max DD `{_fmt_pct(baseline_row['avg_max_drawdown_pct'])} -> "
                f"{_fmt_pct(best_row['avg_max_drawdown_pct'])}`, "
                f"worst max DD `{_fmt_pct(baseline_row['worst_max_drawdown_pct'])} -> "
                f"{_fmt_pct(best_row['worst_max_drawdown_pct'])}`, "
                f"beat-original return windows `{_fmt_pct(best_row['beat_original_return_window_ratio_pct'])}`, "
                f"joint return+DD improvement `{_fmt_pct(best_row['joint_improvement_window_ratio_pct'])}` "
                f"for `{_format_variant(best_row)}`."
            )
        )

    lowest_avg_drawdown = rolling_kelly.sort_values(
        "avg_max_drawdown_pct", ascending=True
    ).iloc[0]
    best_joint = rolling_kelly.sort_values(
        "joint_improvement_window_ratio_pct", ascending=False
    ).iloc[0]
    lines.append(
        (
            f"- Best rolling avg return variant: "
            f"`{_format_variant(best_avg_return.iloc[0])}` "
            f"with avg return `{_fmt_pct(best_avg_return.iloc[0]['avg_total_return_pct'])}`, "
            f"avg max DD `{_fmt_pct(best_avg_return.iloc[0]['avg_max_drawdown_pct'])}`, "
            f"beat-equal-weight `{_fmt_pct(best_avg_return.iloc[0]['beat_equal_weight_window_ratio_pct'])}`."
        )
    )
    lines.append(
        (
            f"- Lowest rolling avg max DD variant: "
            f"`{_format_variant(lowest_avg_drawdown)}` "
            f"with avg max DD `{_fmt_pct(lowest_avg_drawdown['avg_max_drawdown_pct'])}` "
            f"and avg return `{_fmt_pct(lowest_avg_drawdown['avg_total_return_pct'])}`."
        )
    )
    lines.append(
        (
            f"- Highest joint return+DD improvement variant: "
            f"`{_format_variant(best_joint)}` "
            f"with joint-improvement windows `{_fmt_pct(best_joint['joint_improvement_window_ratio_pct'])}`, "
            f"return-only windows `{_fmt_pct(best_joint['beat_original_return_window_ratio_pct'])}`, "
            f"DD-only windows `{_fmt_pct(best_joint['improve_original_drawdown_window_ratio_pct'])}`."
        )
    )
    return lines


def _build_published_summary(
    result: ForwardEpsThresholdWindowStudyResult,
) -> dict[str, Any]:
    published: dict[str, Any] = {
        "strategyNames": list(result.strategy_names),
        "datasetName": result.dataset_name,
        "baselineStrategyName": result.baseline_strategy_name,
        "rollingMonths": result.rolling_months,
        "rollingStepMonths": result.rolling_step_months,
        "scenarioCount": int(len(result.window_metrics_df)),
        "successfulScenarioCount": int(
            (result.window_metrics_df["status"] == "ok").sum()
        ),
    }
    rolling_kelly = result.rolling_summary_df[
        result.rolling_summary_df["portfolio_kind"] == "kelly"
    ]
    if not rolling_kelly.empty:
        best_avg_return = rolling_kelly.sort_values(
            "avg_total_return_pct", ascending=False
        ).iloc[0]
        published["bestRollingKellyAverageReturn"] = {
            "strategyName": _to_native(best_avg_return["strategy_name"]),
            "threshold": _to_native(best_avg_return["forward_eps_growth_threshold"]),
            "riskAdjustedReturnThreshold": _to_native(
                best_avg_return["risk_adjusted_return_threshold"]
            ),
            "volumeRatioAboveThreshold": _to_native(
                best_avg_return["volume_ratio_above_threshold"]
            ),
            "volumeRatioAboveShortPeriod": _to_native(
                best_avg_return["volume_ratio_above_short_period"]
            ),
            "volumeRatioAboveLongPeriod": _to_native(
                best_avg_return["volume_ratio_above_long_period"]
            ),
            "avgTotalReturnPct": _to_native(best_avg_return["avg_total_return_pct"]),
            "avgMaxDrawdownPct": _to_native(
                best_avg_return["avg_max_drawdown_pct"]
            ),
            "beatEqualWeightWindowRatioPct": _to_native(
                best_avg_return["beat_equal_weight_window_ratio_pct"]
            ),
        }
    return published


def _fmt_pct(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "n/a"


def _fmt_num(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return "n/a"


def _fmt_threshold(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "n/a"


def _to_native(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def _fmt_int(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return "n/a"
        return str(int(value))
    except (TypeError, ValueError):
        return "n/a"


def _format_variant(row: Any) -> str:
    return (
        f"feps={_fmt_threshold(row['forward_eps_growth_threshold'])}, "
        f"risk={_fmt_threshold(row.get('risk_adjusted_return_threshold'))}, "
        "volume="
        f"{_fmt_int(row.get('volume_ratio_above_short_period'))}/"
        f"{_fmt_int(row.get('volume_ratio_above_long_period'))}@"
        f"{_fmt_threshold(row.get('volume_ratio_above_threshold'))}"
    )
