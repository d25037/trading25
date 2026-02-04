"""
トレンド・リトレースメントシグナルパラメータ
"""

from pydantic import Field

from .base import BaseSignalParams


class TrendSignalParams(BaseSignalParams):
    """
    トレンドシグナルパラメータ

    [未使用] SignalRegistryに未登録。ma_breakout または crossover の使用を推奨。
    """
    ema_window: int = Field(default=200, gt=0, le=500, description="EMA期間")


class RetracementSignalParams(BaseSignalParams):
    """リトレースメントシグナルパラメータ（フィボナッチ下落率ベース）"""
    lookback_period: int = Field(default=20, gt=0, le=500, description="最高値計算期間")
    retracement_level: float = Field(
        default=0.382,
        gt=0.0,
        lt=1.0,
        description="下落率（0.236/0.382/0.5/0.618/0.786等のフィボナッチレベル）",
    )
    direction: str = Field(
        default="break",
        description="ブレイク方向（break=レベル下抜け（押し目）、recovery=レベル上抜け（反発））",
    )
    price_column: str = Field(
        default="close", description="判定価格カラム（close/low）"
    )
