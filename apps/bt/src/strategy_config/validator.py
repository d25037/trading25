"""Compatibility facade for Phase 4C."""

from typing import Any

from loguru import logger

from src.lib.strategy_runtime.models import try_validate_strategy_config_dict
from src.lib.strategy_runtime.validator import (
    DANGEROUS_PATH_PATTERNS,
    MAX_STRATEGY_NAME_LENGTH,
    is_editable_category,
    validate_strategy_name,
)

__all__ = [
    "MAX_STRATEGY_NAME_LENGTH",
    "DANGEROUS_PATH_PATTERNS",
    "validate_strategy_name",
    "validate_strategy_config",
    "is_editable_category",
    "try_validate_strategy_config_dict",
]


def validate_strategy_config(config: dict[str, Any]) -> bool:
    """
    Compatibility wrapper.

    Keep this function in the legacy module so monkeypatch targets like
    `src.strategy_config.validator.try_validate_strategy_config_dict` remain valid.
    """
    is_valid, error = try_validate_strategy_config_dict(config)
    if not is_valid:
        logger.error(f"戦略設定バリデーションエラー: {error}")
        return False

    entry_filter_params = config.get("entry_filter_params", {})
    expected_filter_types = [
        "volume",
        "trend",
        "fundamental",
        "volatility",
        "relative_performance",
        "margin",
    ]

    for filter_type in expected_filter_types:
        if filter_type not in entry_filter_params:
            logger.warning(
                f"フィルター設定が不足（デフォルト値を使用）: entry_filter_params.{filter_type}"
            )

    logger.info("戦略設定の妥当性チェック成功")
    return True
