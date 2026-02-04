"""
ボラティリティ・ATR・ボリンジャーバンドシグナルパラメータ
"""

from pydantic import Field, field_validator

from .base import BaseSignalParams


class VolatilitySignalParams(BaseSignalParams):
    """
    ボラティリティシグナルパラメータ

    [未使用] SignalRegistryに未登録。bollinger_bands の使用を推奨。
    """
    lookback_period: int = Field(
        default=200, gt=0, le=500, description="ボラティリティ計算期間"
    )
    threshold_multiplier: float = Field(
        default=1.0,
        gt=0.1,
        le=5.0,
        description="ベンチマークボラティリティに対する倍率閾値",
    )


class ATRSupportBreakParams(BaseSignalParams):
    """ATRサポートラインブレイクシグナルパラメータ"""
    direction: str = Field(
        default="break",
        description="ブレイク方向（break=サポート割れ、recovery=サポート回復）",
    )
    lookback_period: int = Field(
        default=20,
        gt=0,
        le=500,
        description="サポートライン・ATR両方の計算期間（統一期間）",
    )
    atr_multiplier: float = Field(default=2.0, gt=0, le=10.0, description="ATR倍率")
    price_column: str = Field(
        default="close", description="判定価格カラム（close/low）"
    )


class BollingerBandsSignalParams(BaseSignalParams):
    """ボリンジャーバンドシグナルパラメータ（汎用・シンプル設計）"""
    window: int = Field(default=20, gt=0, le=500, description="ボリンジャーバンド期間")
    alpha: float = Field(
        default=2.0, gt=0, le=5.0, description="標準偏差倍率（2.0 = 2σ）"
    )
    position: str = Field(
        default="below_upper",
        description="判定位置（below_upper/above_lower/above_middle/below_middle/above_upper/below_lower）",
    )

    @field_validator("position")
    @classmethod
    def validate_position(cls, v):
        valid_positions = [
            "below_upper",
            "above_lower",
            "above_middle",
            "below_middle",
            "above_upper",
            "below_lower",
        ]
        if v not in valid_positions:
            raise ValueError(f"positionは{valid_positions}のいずれかのみ指定可能です")
        return v
