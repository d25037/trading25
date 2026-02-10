"""Compatibility facade for Phase 4C."""

from src.lib.strategy_runtime.path_resolver import (
    StrategyMetadata,
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)

__all__ = [
    "StrategyMetadata",
    "infer_strategy_path",
    "get_available_strategies",
    "get_strategy_metadata",
    "validate_path_within_strategies",
]
