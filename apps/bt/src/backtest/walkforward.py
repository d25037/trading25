"""Compatibility facade for Phase 4C."""

from src.lib.backtest_core.walkforward import (
    WalkForwardSplit,
    generate_walkforward_splits,
)

__all__ = [
    "WalkForwardSplit",
    "generate_walkforward_splits",
]
