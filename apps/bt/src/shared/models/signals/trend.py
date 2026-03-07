"""
トレンド・リトレースメントシグナルパラメータ
"""

from pydantic import Field, field_validator

from .base import BaseSignalParams


class TrendSignalParams(BaseSignalParams):
    """
    トレンドシグナルパラメータ

    [未使用] SignalRegistryに未登録。baseline_cross または crossover の使用を推奨。
    """
    ema_window: int = Field(default=200, gt=0, le=500, description="EMA期間")


class _BaseRetracementSignalParams(BaseSignalParams):
    """リトレースメント系シグナル共通パラメータ"""

    lookback_period: int = Field(default=20, gt=0, le=500, description="最高値計算期間")
    retracement_level: float = Field(
        default=0.382,
        gt=0.0,
        lt=1.0,
        description="下落率（0.236/0.382/0.5/0.618/0.786等のフィボナッチレベル）",
    )
    direction: str = Field(
        default="below",
        description="判定方向（below=レベル下側、above=レベル上側）",
    )
    price_column: str = Field(
        default="close", description="判定価格カラム（close/low）"
    )

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, v: str) -> str:
        if v not in ["below", "above"]:
            raise ValueError("directionは'below'または'above'のみ指定可能です")
        return v

    @field_validator("price_column")
    @classmethod
    def validate_price_column(cls, v: str) -> str:
        if v not in ["close", "low"]:
            raise ValueError("price_columnは'close'または'low'のみ指定可能です")
        return v


class RetracementPositionSignalParams(_BaseRetracementSignalParams):
    """リトレースメント水準に対する位置シグナルパラメータ"""


class RetracementCrossSignalParams(_BaseRetracementSignalParams):
    """リトレースメント水準クロスシグナルパラメータ"""

    lookback_days: int = Field(
        default=1, gt=0, le=100, description="クロス検出期間（1=その日のみ、N=直近N日以内）"
    )
