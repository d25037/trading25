"""Compatibility wrapper for strategy config validation."""

from src.strategy_config.validator import (
    DANGEROUS_PATH_PATTERNS,
    MAX_STRATEGY_NAME_LENGTH,
    is_editable_category,
    validate_strategy_config,
    validate_strategy_name,
)

__all__ = [
    "DANGEROUS_PATH_PATTERNS",
    "MAX_STRATEGY_NAME_LENGTH",
    "is_editable_category",
    "validate_strategy_config",
    "validate_strategy_name",
]

