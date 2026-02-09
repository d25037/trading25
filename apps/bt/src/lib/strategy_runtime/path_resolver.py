"""Compatibility wrapper for strategy path resolver."""

from src.strategy_config.path_resolver import (
    StrategyMetadata,
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)

__all__ = [
    "StrategyMetadata",
    "get_available_strategies",
    "get_strategy_metadata",
    "infer_strategy_path",
    "validate_path_within_strategies",
]

