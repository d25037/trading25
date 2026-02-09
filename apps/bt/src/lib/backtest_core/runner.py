"""Compatibility wrapper for backtest runner."""

from typing import Any

from src.backtest.runner import BacktestResult


class BacktestRunner:  # pragma: no cover - thin delegation wrapper
    """Delegate to `src.backtest.runner.BacktestRunner` at runtime."""

    def __new__(cls, *args: Any, **kwargs: Any):
        from src.backtest.runner import BacktestRunner as LegacyBacktestRunner

        return LegacyBacktestRunner(*args, **kwargs)

__all__ = ["BacktestResult", "BacktestRunner"]
