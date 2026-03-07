"""
ボラティリティ・ATR・ボリンジャーバンドシグナルパラメータ
"""

from pydantic import Field, field_validator

from .base import BaseSignalParams


class VolatilityPercentileSignalParams(BaseSignalParams):
    """ボラティリティパーセンタイルシグナルパラメータ"""

    window: int = Field(
        default=20,
        gt=0,
        le=500,
        description="現在ボラティリティの計算期間",
    )
    lookback: int = Field(
        default=252,
        gt=1,
        le=1000,
        description="パーセンタイル比較期間",
    )
    percentile: float = Field(
        default=50.0,
        ge=0.0,
        le=100.0,
        description="現在ボラティリティが下回るべきパーセンタイル閾値（0-100）",
    )


class _BaseATRSupportSignalParams(BaseSignalParams):
    """ATRサポートライン系シグナル共通パラメータ"""

    direction: str = Field(
        default="below",
        description="判定方向（below=サポートライン下側、above=サポートライン上側）",
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


class ATRSupportPositionParams(_BaseATRSupportSignalParams):
    """ATRサポートライン位置シグナルパラメータ"""


class ATRSupportCrossParams(_BaseATRSupportSignalParams):
    """ATRサポートラインクロスシグナルパラメータ"""

    lookback_days: int = Field(
        default=1, gt=0, le=100, description="クロス検出期間（1=その日のみ、N=直近N日以内）"
    )


class _BaseBollingerSignalParams(BaseSignalParams):
    """ボリンジャーバンド系シグナル共通パラメータ"""

    window: int = Field(default=20, gt=0, le=500, description="ボリンジャーバンド期間")
    alpha: float = Field(
        default=2.0, gt=0, le=5.0, description="標準偏差倍率（2.0 = 2σ）"
    )
    level: str = Field(
        default="upper",
        description="判定対象バンド（upper/middle/lower）",
    )
    direction: str = Field(
        default="below",
        description="判定方向（below=対象バンド以下、above=対象バンド以上）",
    )

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        valid_levels = ["upper", "middle", "lower"]
        if v not in valid_levels:
            raise ValueError(f"levelは{valid_levels}のいずれかのみ指定可能です")
        return v

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, v: str) -> str:
        if v not in ["above", "below"]:
            raise ValueError("directionは'above'または'below'のみ指定可能です")
        return v


class BollingerPositionSignalParams(_BaseBollingerSignalParams):
    """ボリンジャーバンド位置シグナルパラメータ"""


class BollingerCrossSignalParams(_BaseBollingerSignalParams):
    """ボリンジャーバンドクロスシグナルパラメータ"""

    lookback_days: int = Field(
        default=1, gt=0, le=100, description="クロス検出期間（1=その日のみ、N=直近N日以内）"
    )
