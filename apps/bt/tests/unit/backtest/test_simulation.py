"""Tests for pure backtest simulation execution."""

from __future__ import annotations

from src.domains.backtest.contracts import CanonicalExecutionMetrics
from src.domains.backtest.core import simulation as simulation_module
from src.domains.backtest.core.simulation import BacktestSimulationExecutor


def test_simulation_executor_uses_allocation_info(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _execute_strategy_with_config(shared_config, entry_filter_params, exit_trigger_params):
        captured["shared_config"] = shared_config
        captured["entry_filter_params"] = entry_filter_params
        captured["exit_trigger_params"] = exit_trigger_params
        return {
            "initial_portfolio": "initial",
            "kelly_portfolio": "kelly",
            "allocation_info": {"allocation": 0.25},
            "all_entries": "entries",
        }

    monkeypatch.setattr(
        simulation_module.StrategyFactory,
        "execute_strategy_with_config",
        _execute_strategy_with_config,
    )
    monkeypatch.setattr(
        simulation_module,
        "canonical_metrics_from_portfolio",
        lambda portfolio: CanonicalExecutionMetrics(total_return=4.0) if portfolio == "kelly" else None,
    )
    monkeypatch.setattr(
        simulation_module,
        "build_metrics_payload",
        lambda *, portfolio, allocation_info: {
            "portfolio": portfolio,
            "allocation_info": allocation_info,
        },
    )

    executor = BacktestSimulationExecutor()
    result = executor.execute(
        {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {"foo": {"enabled": True}},
            "exit_trigger_params": {"bar": {"enabled": True}},
        }
    )

    assert captured == {
        "shared_config": {"dataset": "sample"},
        "entry_filter_params": {"foo": {"enabled": True}},
        "exit_trigger_params": {"bar": {"enabled": True}},
    }
    assert result.initial_portfolio == "initial"
    assert result.kelly_portfolio == "kelly"
    assert result.allocation_info == {"allocation": 0.25}
    assert result.summary_metrics is not None
    assert result.summary_metrics.total_return == 4.0
    assert result.metrics_payload == {
        "portfolio": "kelly",
        "allocation_info": {"allocation": 0.25},
    }


def test_simulation_executor_falls_back_to_max_concurrent(monkeypatch) -> None:
    monkeypatch.setattr(
        simulation_module.StrategyFactory,
        "execute_strategy_with_config",
        lambda *args, **kwargs: {  # noqa: ARG005
            "initial_portfolio": None,
            "kelly_portfolio": "kelly",
            "max_concurrent": 7,
            "all_entries": None,
        },
    )
    monkeypatch.setattr(
        simulation_module,
        "canonical_metrics_from_portfolio",
        lambda _portfolio: None,
    )
    monkeypatch.setattr(
        simulation_module,
        "build_metrics_payload",
        lambda *, portfolio, allocation_info: {  # noqa: ARG005
            "optimal_allocation": allocation_info,
        },
    )

    result = BacktestSimulationExecutor().execute({})

    assert result.allocation_info == 7
    assert result.summary_metrics is None
    assert result.metrics_payload == {"optimal_allocation": 7}
