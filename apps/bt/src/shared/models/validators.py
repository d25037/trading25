"""
共通バリデータファクトリ

Pydanticモデルで使用する共通バリデータのファクトリ関数
"""

from typing import Any, Callable


def _format_choices_message(choices: list[Any], field_name: str) -> str:
    """選択肢のエラーメッセージをフォーマット"""
    choices_str = "または".join(f"'{c}'" for c in choices)
    return f"{field_name}は{choices_str}のみ指定可能です"


def create_choice_validator(
    valid_values: list[str],
    field_name: str,
) -> Callable[[type, str], str]:
    """
    選択肢バリデータのファクトリ関数

    Args:
        valid_values: 有効な値のリスト
        field_name: フィールド名（エラーメッセージ用）

    Returns:
        Pydantic field_validator として使用可能なクラスメソッド

    Usage:
        class MyModel(BaseModel):
            direction: str = Field(default="up")

            @field_validator("direction")
            @classmethod
            def validate_direction(cls, v):
                return create_choice_validator(
                    ["up", "down"], "direction"
                )(cls, v)

    Note:
        - 現在のシグナルモデルは既に各クラスで個別にバリデータを持っている
        - 新規モデル作成時にこのファクトリを使用することで重複を削減できる
        - 既存コードのリファクタリングは影響範囲が大きいため、段階的に適用推奨
    """
    error_msg = _format_choices_message(valid_values, field_name)

    def validator(cls: type, v: str) -> str:
        if v not in valid_values:
            raise ValueError(error_msg)
        return v

    return validator


def create_range_validator(
    min_val: float | None = None,
    max_val: float | None = None,
    field_name: str = "value",
) -> Callable[[type, float], float]:
    """
    範囲バリデータのファクトリ関数

    Args:
        min_val: 最小値（None で下限なし）
        max_val: 最大値（None で上限なし）
        field_name: フィールド名（エラーメッセージ用）

    Returns:
        Pydantic field_validator として使用可能なクラスメソッド
    """

    def validator(cls: type, v: float) -> float:
        if min_val is not None and v < min_val:
            raise ValueError(f"{field_name}は{min_val}以上である必要があります")
        if max_val is not None and v > max_val:
            raise ValueError(f"{field_name}は{max_val}以下である必要があります")
        return v

    return validator


# 一般的なバリデータセット（再利用可能）
DIRECTION_CHOICES = {
    "above_below": ["above", "below"],
    "surge_drop": ["surge", "drop"],
    "high_low": ["high", "low"],
    "golden_dead": ["golden", "dead"],
    "upward_downward": ["upward", "downward"],
    "break_maintained": ["break", "maintained"],
}

INDICATOR_CHOICES = {
    "ma_type": ["sma", "ema"],
    "baseline_type": ["sma", "ema"],
    "price_column": ["high", "low", "close"],
    "ratio_type": ["sharpe", "sortino"],
    "crossover_type": ["sma", "rsi", "macd", "ema"],
}


def validate_in_choices(value: Any, choices: list[Any], field_name: str) -> Any:
    """
    値が選択肢に含まれるかを検証するシンプルな関数

    Args:
        value: 検証する値
        choices: 有効な選択肢のリスト
        field_name: フィールド名（エラーメッセージ用）

    Returns:
        検証後の値

    Raises:
        ValueError: 値が選択肢に含まれない場合

    Usage:
        @field_validator("direction")
        @classmethod
        def validate_direction(cls, v):
            return validate_in_choices(v, ["above", "below"], "direction")
    """
    if value not in choices:
        raise ValueError(_format_choices_message(choices, field_name))
    return value
