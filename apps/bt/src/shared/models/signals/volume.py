"""
出来高・売買代金シグナルパラメータ
"""

from pydantic import Field, ValidationInfo, field_validator

from .base import (
    BaseSignalParams,
    _validate_condition_above_below,
    _validate_period_order,
)


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
    ma_type: str = Field(default="sma", description="移動平均タイプ（sma/ema/median）")

    @field_validator("long_period")
    @classmethod
    def validate_volume_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v, info, "short_period", "出来高長期期間は出来高短期期間より大きい必要があります"
        )

    @field_validator("ma_type")
    @classmethod
    def validate_ma_type(cls, v: str) -> str:
        if v not in ["sma", "ema", "median"]:
            raise ValueError("ma_typeは'sma'、'ema'、'median'のみ指定可能です")
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


class TradingValueEmaRatioAboveSignalParams(BaseSignalParams):
    """短期EMA売買代金がADVを上回る freshness シグナル"""

    ratio_threshold: float = Field(
        default=1.0,
        gt=0.0,
        le=10.0,
        description="EMA売買代金がADVの何倍以上でTrueにするか",
    )
    ema_period: int = Field(
        default=3,
        gt=0,
        le=30,
        description="売買代金EMA期間",
    )
    baseline_period: int = Field(
        default=20,
        gt=0,
        le=250,
        description="ADV（単純平均売買代金）期間",
    )

    @field_validator("baseline_period")
    @classmethod
    def validate_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v,
            info,
            "ema_period",
            "ADV期間はEMA期間より大きい必要があります",
        )


class TradingValueEmaRatioBelowSignalParams(BaseSignalParams):
    """短期EMA売買代金がADVを下回る stale-volume シグナル"""

    ratio_threshold: float = Field(
        default=0.9,
        gt=0.0,
        le=10.0,
        description="EMA売買代金がADVの何倍未満でTrueにするか",
    )
    ema_period: int = Field(
        default=3,
        gt=0,
        le=30,
        description="売買代金EMA期間",
    )
    baseline_period: int = Field(
        default=20,
        gt=0,
        le=250,
        description="ADV（単純平均売買代金）期間",
    )

    @field_validator("baseline_period")
    @classmethod
    def validate_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v,
            info,
            "ema_period",
            "ADV期間はEMA期間より大きい必要があります",
        )


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


class CMFThresholdSignalParams(BaseSignalParams):
    """Chaikin Money Flow 閾値シグナルパラメータ"""

    period: int = Field(default=20, gt=0, le=500, description="CMF計算期間")
    threshold: float = Field(
        default=0.05,
        ge=-1.0,
        le=1.0,
        description="CMF閾値（-1.0〜1.0）",
    )
    condition: str = Field(
        default="above",
        description="閾値判定方向（above=閾値以上、below=閾値未満）",
    )

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: str) -> str:
        return _validate_condition_above_below(v)


class ChaikinOscillatorSignalParams(BaseSignalParams):
    """Chaikin oscillator 閾値シグナルパラメータ"""

    fast_period: int = Field(
        default=3,
        gt=0,
        le=500,
        description="ADL短期EMA期間",
    )
    slow_period: int = Field(
        default=10,
        gt=0,
        le=500,
        description="ADL長期EMA期間",
    )
    threshold: float = Field(
        default=0.0,
        description="Chaikin oscillator閾値",
    )
    condition: str = Field(
        default="above",
        description="閾値判定方向（above=閾値以上、below=閾値未満）",
    )

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: str) -> str:
        return _validate_condition_above_below(v)

    @field_validator("slow_period")
    @classmethod
    def validate_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v,
            info,
            "fast_period",
            "Chaikin oscillator長期期間は短期期間より大きい必要があります",
        )


class OBVFlowScoreSignalParams(BaseSignalParams):
    """OBV flow score 閾値シグナルパラメータ"""

    lookback_period: int = Field(
        default=20,
        gt=0,
        le=500,
        description="OBV変化を出来高で正規化する期間",
    )
    threshold: float = Field(
        default=0.05,
        ge=-1.0,
        le=1.0,
        description="OBV flow score閾値（-1.0〜1.0）",
    )
    condition: str = Field(
        default="above",
        description="閾値判定方向（above=閾値以上、below=閾値未満）",
    )

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: str) -> str:
        return _validate_condition_above_below(v)


class AccumulationPressureSignalParams(BaseSignalParams):
    """CMF/Chaikin/OBV の投票式 accumulation pressure シグナルパラメータ"""

    cmf_period: int = Field(default=20, gt=0, le=500, description="CMF計算期間")
    chaikin_fast_period: int = Field(
        default=3,
        gt=0,
        le=500,
        description="Chaikin oscillator短期EMA期間",
    )
    chaikin_slow_period: int = Field(
        default=10,
        gt=0,
        le=500,
        description="Chaikin oscillator長期EMA期間",
    )
    obv_lookback_period: int = Field(
        default=20,
        gt=0,
        le=500,
        description="OBV flow score計算期間",
    )
    cmf_threshold: float = Field(
        default=0.05,
        ge=-1.0,
        le=1.0,
        description="CMFの買い集めproxy閾値",
    )
    chaikin_oscillator_threshold: float = Field(
        default=0.0,
        description="Chaikin oscillatorの買い集めproxy閾値",
    )
    obv_score_threshold: float = Field(
        default=0.05,
        ge=-1.0,
        le=1.0,
        description="OBV flow scoreの買い集めproxy閾値",
    )
    min_votes: int = Field(
        default=2,
        ge=1,
        le=3,
        description="CMF/Chaikin/OBVのうち必要な成立数",
    )

    @field_validator("chaikin_slow_period")
    @classmethod
    def validate_chaikin_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v,
            info,
            "chaikin_fast_period",
            "Chaikin oscillator長期期間は短期期間より大きい必要があります",
        )
