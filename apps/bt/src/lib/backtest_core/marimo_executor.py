"""Compatibility wrapper for Marimo executor."""

from typing import Any


class MarimoExecutor:  # pragma: no cover - thin delegation wrapper
    """Delegate to `src.backtest.marimo_executor.MarimoExecutor` at runtime."""

    def __new__(cls, *args: Any, **kwargs: Any):
        from src.backtest.marimo_executor import MarimoExecutor as LegacyMarimoExecutor

        return LegacyMarimoExecutor(*args, **kwargs)


__all__ = ["MarimoExecutor"]
