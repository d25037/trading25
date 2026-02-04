"""
戦略設定バリデーション

戦略名・設定の検証ロジック
"""

import re
from typing import Any

from loguru import logger

from .models import try_validate_strategy_config_dict


# 戦略名の最大長
MAX_STRATEGY_NAME_LENGTH = 100

# 危険なパストラバーサルパターン
DANGEROUS_PATH_PATTERNS = ("..", "\\", "~", "//")


def validate_strategy_name(strategy_name: str) -> None:
    """
    戦略名の安全性を検証（パストラバーサル攻撃対策）

    Args:
        strategy_name: 戦略名（カテゴリ/戦略名 形式も許可）

    Raises:
        ValueError: 不正な戦略名の場合
    """
    if not re.match(r"^[a-zA-Z0-9_/-]+$", strategy_name):
        raise ValueError(
            f"無効な戦略名: {strategy_name} (英数字、アンダースコア、ハイフン、スラッシュのみ許可)"
        )

    if any(p in strategy_name for p in DANGEROUS_PATH_PATTERNS):
        raise ValueError(f"不正な文字が含まれています: {strategy_name}")

    if len(strategy_name) > MAX_STRATEGY_NAME_LENGTH:
        raise ValueError(
            f"戦略名が長すぎます: {strategy_name} (最大{MAX_STRATEGY_NAME_LENGTH}文字)"
        )


def validate_strategy_config(config: dict[str, Any]) -> bool:
    """
    戦略設定の妥当性をチェック

    Args:
        config: 戦略設定

    Returns:
        妥当性チェック結果
    """
    is_valid, error = try_validate_strategy_config_dict(config)
    if not is_valid:
        logger.error(f"戦略設定バリデーションエラー: {error}")
        return False

    # entry_filter_params の構造チェック（基本的な構造の存在確認）
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


def is_editable_category(category: str) -> bool:
    """
    カテゴリが編集可能かチェック

    Args:
        category: カテゴリ名

    Returns:
        bool: experimental カテゴリの場合のみ True
    """
    return category == "experimental"
