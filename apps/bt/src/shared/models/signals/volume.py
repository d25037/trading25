"""
出来高・売買代金シグナルパラメータ
"""

from pydantic import Field, ValidationInfo, field_validator

from .base import BaseSignalParams, _validate_period_order


class _BaseVolumeRatioSignalParams(BaseSignalParams):
    """出来高比率シグナル共通パラメータ"""

    ratio_threshold: float = Field(
        default=1.5,
        gt=0.0,
        le=10.0,
        description="出来高比率閾値（短期MA / 長期MA）",
    )
    short_period: int = Field(
        default=20, gt=0, le=300, description="出来高短期移動平均期間"
    )
    long_period: int = Field(
        default=100, gt=0, le=800, description="出来高長期移動平均期間"
    )
    ma_type: str = Field(default="sma", description="移動平均タイプ（sma/ema）")

    @field_validator("long_period")
    @classmethod
    def validate_volume_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v, info, "short_period", "出来高長期期間は出来高短期期間より大きい必要があります"
        )

    @field_validator("ma_type")
    @classmethod
    def validate_ma_type(cls, v: str) -> str:
        if v not in ["sma", "ema"]:
            raise ValueError("ma_typeは'sma'または'ema'のみ指定可能です")
        return v


class VolumeRatioAboveSignalParams(_BaseVolumeRatioSignalParams):
    """出来高比率上抜けシグナルパラメータ"""

    ratio_threshold: float = Field(
        default=1.5,
        gt=0.0,
        le=10.0,
        description="短期出来高MAが長期出来高MAの何倍を上回るとTrueにするか",
    )


class VolumeRatioBelowSignalParams(_BaseVolumeRatioSignalParams):
    """出来高比率下抜けシグナルパラメータ"""

    ratio_threshold: float = Field(
        default=0.7,
        gt=0.0,
        le=10.0,
        description="短期出来高MAが長期出来高MAの何倍を下回るとTrueにするか",
    )


class TradingValueSignalParams(BaseSignalParams):
    """売買代金シグナルパラメータ（X日平均売買代金が閾値以上/以下を判定）"""
    direction: str = Field(
        default="above", description="売買代金判定方向（above=閾値以上、below=閾値以下）"
    )
    period: int = Field(
        default=20, gt=0, le=200, description="売買代金移動平均期間（日数）"
    )
    threshold_value: float = Field(
        default=1.0,
        ge=0.0,
        le=10000.0,
        description="売買代金閾値（億円単位）",
    )

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, v):
        if v not in ["above", "below"]:
            raise ValueError("directionは'above'または'below'のみ指定可能です")
        return v


class TradingValueRangeSignalParams(BaseSignalParams):
    """売買代金範囲シグナルパラメータ（X日平均売買代金が範囲内を判定）"""
    period: int = Field(
        default=20, gt=0, le=200, description="売買代金移動平均期間（日数）"
    )
    min_threshold: float = Field(
        default=0.5,
        ge=0.0,
        le=10000.0,
        description="最小閾値（億円単位）",
    )
    max_threshold: float = Field(
        default=100.0,
        ge=0.0,
        le=10000.0,
        description="最大閾値（億円単位）",
    )

    @field_validator("max_threshold")
    @classmethod
    def validate_threshold_range(cls, v, info):
        """最大閾値が最小閾値より大きいことを検証"""
        if "min_threshold" in info.data and v <= info.data["min_threshold"]:
            raise ValueError("最大閾値は最小閾値より大きい必要があります")
        return v
