"""
マクロ・市場環境シグナルパラメータ（β値、信用残高、指数関連）
"""

from typing import Literal

from pydantic import Field, ValidationInfo, field_validator, model_validator

from .base import BaseSignalParams, _validate_period_order


class BetaSignalParams(BaseSignalParams):
    """β値シグナルパラメータ"""
    min_beta: float = Field(default=0.5, ge=-2.0, le=5.0, description="β値下限閾値")
    max_beta: float = Field(default=1.5, ge=-2.0, le=5.0, description="β値上限閾値")
    lookback_period: int = Field(default=200, gt=0, le=500, description="β値計算期間")

    @field_validator("max_beta")
    @classmethod
    def validate_beta_range(cls, v, info):
        if (
            hasattr(info, "data")
            and "min_beta" in info.data
            and v <= info.data["min_beta"]
        ):
            raise ValueError("β値上限は下限より大きい必要があります")
        return v


class MarginSignalParams(BaseSignalParams):
    """信用残高シグナルパラメータ"""
    lookback_period: int = Field(
        default=150, gt=0, le=500, description="信用残高参照期間"
    )
    percentile_threshold: float = Field(
        default=0.2,
        gt=0,
        lt=1.0,
        description="パーセンタイル閾値（0.2 = 下位20%で信用残高が少ない）",
    )


class IndexDailyChangeSignalParams(BaseSignalParams):
    """指数前日比シグナルパラメータ（市場環境フィルター）"""
    max_daily_change_pct: float = Field(
        default=1.0,
        ge=-10.0,
        le=10.0,
        description="前日比閾値（%単位、例: 1.0 = +1.0%）",
    )
    direction: str = Field(
        default="below",
        description="判定方向（below=閾値以下でTrue、above=閾値超でTrue）",
    )


class IndexMACDHistogramSignalParams(BaseSignalParams):
    """INDEXヒストグラムシグナルパラメータ（市場モメンタムフィルター）"""
    fast_period: int = Field(default=12, gt=0, le=500, description="MACD高速EMA期間")
    slow_period: int = Field(default=26, gt=0, le=1000, description="MACD低速EMA期間")
    signal_period: int = Field(default=9, gt=0, le=100, description="MACDシグナル期間")
    direction: str = Field(
        default="positive",
        description="判定方向（positive=histogram > 0、negative=histogram < 0）",
    )

    @field_validator("slow_period")
    @classmethod
    def validate_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v, info, "fast_period", "slow_periodはfast_periodより大きい必要があります"
        )


class IndexOpenGapRegimeSignalParams(BaseSignalParams):
    """指数寄り付きギャップを same-day レジームとして判定するシグナル."""

    gap_threshold_1_pct: float = Field(
        default=1.0,
        gt=0.0,
        le=10.0,
        description="第1閾値（%単位、例: 1.0 = 1%）",
    )
    gap_threshold_2_pct: float = Field(
        default=2.0,
        gt=0.0,
        le=20.0,
        description="第2閾値（%単位、例: 2.0 = 2%）",
    )
    regime: Literal[
        "down_large",
        "down_medium",
        "flat",
        "up_medium",
        "up_large",
    ] = Field(
        default="down_medium",
        description="指数寄り付きギャップの判定レジーム",
    )

    @field_validator("gap_threshold_2_pct")
    @classmethod
    def validate_gap_threshold_order(cls, v: float, info: ValidationInfo) -> float:
        if hasattr(info, "data") and "gap_threshold_1_pct" in info.data:
            if v <= info.data["gap_threshold_1_pct"]:
                raise ValueError(
                    "gap_threshold_2_pctはgap_threshold_1_pctより大きい必要があります"
                )
        return v


class UniverseRankBucketSignalParams(BaseSignalParams):
    """指数/ユニバース内の価格順位バケット判定シグナル."""

    price_sma_period: int = Field(
        default=50,
        gt=0,
        le=500,
        description="価格 / SMA 乖離の基準SMA期間（research default は 50）",
    )
    volume_short_period: int = Field(
        default=20,
        gt=0,
        le=500,
        description="出来高短期SMA期間",
    )
    volume_long_period: int = Field(
        default=80,
        gt=0,
        le=1000,
        description="出来高長期SMA期間",
    )
    price_bucket: Literal["q1", "q10", "q456", "other"] = Field(
        default="q1",
        description="価格順位バケット",
    )
    volume_bucket: Literal["any", "high", "low"] = Field(
        default="any",
        description="出来高バケット",
    )
    min_constituents: int = Field(
        default=10,
        ge=2,
        le=5000,
        description="判定に必要な最小構成銘柄数",
    )

    @field_validator("volume_long_period")
    @classmethod
    def validate_volume_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v,
            info,
            "volume_short_period",
            "volume_long_periodはvolume_short_periodより大きい必要があります",
        )

    @model_validator(mode="after")
    def validate_volume_bucket_selection(self) -> "UniverseRankBucketSignalParams":
        if self.price_bucket == "other" and self.volume_bucket != "any":
            raise ValueError(
                "price_bucket='other' の場合、volume_bucket は 'any' のみ指定可能です"
            )
        return self
