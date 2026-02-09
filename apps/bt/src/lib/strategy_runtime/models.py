"""Compatibility wrapper for strategy config models."""

from src.strategy_config.models import (
    ExecutionConfig,
    StrategyConfig,
    try_validate_strategy_config_dict,
    validate_strategy_config_dict,
)

__all__ = [
    "ExecutionConfig",
    "StrategyConfig",
    "validate_strategy_config_dict",
    "try_validate_strategy_config_dict",
]

