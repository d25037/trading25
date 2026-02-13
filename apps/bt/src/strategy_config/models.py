"""Compatibility facade for Phase 4C."""

from src.lib.strategy_runtime.models import (
    ExecutionConfig,
    StrategyConfig,
    StrategyConfigStrictValidationError,
    try_validate_strategy_config_dict,
    try_validate_strategy_config_dict_strict,
    validate_strategy_config_dict,
    validate_strategy_config_dict_strict,
)

__all__ = [
    "ExecutionConfig",
    "StrategyConfig",
    "StrategyConfigStrictValidationError",
    "validate_strategy_config_dict",
    "try_validate_strategy_config_dict",
    "validate_strategy_config_dict_strict",
    "try_validate_strategy_config_dict_strict",
]
