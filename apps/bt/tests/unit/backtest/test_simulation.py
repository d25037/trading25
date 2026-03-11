"""Tests for extracted backtest simulation stage."""

from __future__ import annotations

from types import SimpleNamespace

from src.domains.backtest.core.simulation import (
    BacktestSimulationExecutor,
    BacktestSimulationResult,
)


class _MetricValue:
    def __init__(self, value: float) -> None:
        self._value = value

    def mean(self) -> float:
        return self._value


class _Portfolio:
    def stats(self):
        return {
            "Total Return [%]": 12.3,
            "Max Drawdown [%]": -4.5,
            "Sharpe Ratio": 1.6,
            "Sortino Ratio": 2.1,
            "Calmar Ratio": 1.8,
            "Win Rate [%]": 57.0,
            "Profit Factor": 1.9,
            "Total Trades": 11,
        }

    def total_return(self):
        return _MetricValue(12.3)

    def sharpe_ratio(self):
        return _MetricValue(1.6)

    def sortino_ratio(self):
        return _MetricValue(2.1)

    def calmar_ratio(self):
        return _MetricValue(1.8)

    def max_drawdown(self):
        return _MetricValue(-4.5)

    @property
    def trades(self):
        return SimpleNamespace(
            count=lambda: 11,
            win_rate=lambda: _MetricValue(57.0),
        )


def test_simulation_executor_builds_metrics_and_fallback_allocation(monkeypatch) -> None:
    executor = BacktestSimulationExecutor()
    initial_portfolio = _Portfolio()
    kelly_portfolio = _Portfolio()

    monkeypatch.setattr(
        "src.domains.backtest.core.simulation.StrategyFactory.execute_strategy_with_config",
        lambda *_args, **_kwargs: {
            "initial_portfolio": initial_portfolio,
            "kelly_portfolio": kelly_portfolio,
            "max_concurrent": 3,
            "all_entries": None,
        },
    )

    result = executor.execute(
        {
            "shared_config": {"dataset": "sample"},
            "entry_filter_params": {},
            "exit_trigger_params": {},
        }
    )

    assert isinstance(result, BacktestSimulationResult)
    assert result.initial_portfolio is initial_portfolio
    assert result.kelly_portfolio is kelly_portfolio
    assert result.allocation_info == 3
    assert result.summary_metrics is not None
    assert result.summary_metrics.trade_count == 11
    assert result.metrics_payload["profit_factor"] == 1.9
    assert result.metrics_payload["trade_count"] == 11
