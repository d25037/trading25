"""Backtest execution boundary for Phase 4C."""

from src.lib.backtest_core.marimo_executor import MarimoExecutor
from src.lib.backtest_core.runner import BacktestResult, BacktestRunner
from src.lib.backtest_core.signal_attribution import SignalAttributionAnalyzer
from src.lib.backtest_core.walkforward import WalkForwardSplit, generate_walkforward_splits

__all__ = [
    "BacktestResult",
    "BacktestRunner",
    "MarimoExecutor",
    "SignalAttributionAnalyzer",
    "WalkForwardSplit",
    "generate_walkforward_splits",
]
