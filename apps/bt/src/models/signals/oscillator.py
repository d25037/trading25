"""
RSI系オシレーターシグナルパラメータ
"""

from pydantic import Field, ValidationInfo, field_validator

from .base import BaseSignalParams, _validate_condition_above_below, _validate_period_order


class RSIThresholdSignalParams(BaseSignalParams):
    """RSI閾値シグナルパラメータ（買われすぎ・売られすぎ判定）"""
    period: int = Field(default=14, gt=0, le=100, description="RSI計算期間")
    threshold: float = Field(default=30.0, gt=0, lt=100, description="RSI閾値（0-100）")
    condition: str = Field(
        default="below",
        description="閾値条件（below=閾値より下（売られすぎ）、above=閾値より上（買われすぎ））",
    )

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: str) -> str:
        return _validate_condition_above_below(v)


class RSISpreadSignalParams(BaseSignalParams):
    """RSIスプレッドシグナルパラメータ（短期RSIと長期RSIの差分判定）"""
    fast_period: int = Field(default=9, gt=0, le=100, description="短期RSI期間")
    slow_period: int = Field(default=14, gt=0, le=100, description="長期RSI期間")
    threshold: float = Field(
        default=10.0, gt=0, lt=100, description="スプレッド閾値（0-100）"
    )
    condition: str = Field(
        default="above",
        description="閾値条件（above=短期RSI>長期RSI+閾値（強気乖離）、below=短期RSI<長期RSI-閾値（弱気乖離））",
    )

    @field_validator("slow_period")
    @classmethod
    def validate_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v, info, "fast_period", "長期RSI期間は短期RSI期間より大きい必要があります"
        )

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: str) -> str:
        return _validate_condition_above_below(v)
