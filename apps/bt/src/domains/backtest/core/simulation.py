"""Pure simulation stage for backtest runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.domains.backtest.contracts import CanonicalExecutionMetrics
from src.domains.backtest.core.artifacts import build_metrics_payload
from src.domains.backtest.vectorbt_adapter import canonical_metrics_from_portfolio
from src.domains.strategy.core.factory import StrategyFactory


@dataclass(slots=True)
class BacktestSimulationResult:
    """Result of the simulation stage before any report rendering."""

    initial_portfolio: Any
    kelly_portfolio: Any
    allocation_info: Any
    all_entries: Any
    summary_metrics: CanonicalExecutionMetrics | None
    metrics_payload: dict[str, Any]


class BacktestSimulationExecutor:
    """Execute the core backtest simulation without producing presentation artifacts."""

    def execute(self, parameters: dict[str, Any]) -> BacktestSimulationResult:
        result = StrategyFactory.execute_strategy_with_config(
            parameters.get("shared_config", {}),
            parameters.get("entry_filter_params"),
            parameters.get("exit_trigger_params"),
        )

        initial_portfolio = result.get("initial_portfolio")
        kelly_portfolio = result.get("kelly_portfolio")
        allocation_info = result.get("allocation_info", result.get("max_concurrent"))
        all_entries = result.get("all_entries")
        summary_metrics = canonical_metrics_from_portfolio(kelly_portfolio)
        metrics_payload = build_metrics_payload(
            portfolio=kelly_portfolio,
            allocation_info=allocation_info,
        )

        return BacktestSimulationResult(
            initial_portfolio=initial_portfolio,
            kelly_portfolio=kelly_portfolio,
            allocation_info=allocation_info,
            all_entries=all_entries,
            summary_metrics=summary_metrics,
            metrics_payload=metrics_payload,
        )
