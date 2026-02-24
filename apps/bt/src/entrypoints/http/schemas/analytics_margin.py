"""
Margin Analytics Response Schemas

Hono hono-openapi-baseline.json と完全互換のマージン分析レスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- Margin Pressure Indicators ---


class MarginLongPressureData(BaseModel):
    """マージンロングプレッシャーデータ"""

    date: str
    pressure: float = Field(description="(LongVol - ShrtVol) / N-day avg volume")
    longVol: float = Field(ge=0)
    shortVol: float = Field(ge=0)
    avgVolume: float = Field(ge=0)


class MarginFlowPressureData(BaseModel):
    """マージンフロープレッシャーデータ"""

    date: str
    flowPressure: float = Field(description="Delta(LongVol - ShrtVol) / N-day avg volume")
    currentNetMargin: float
    previousNetMargin: float | None = None
    avgVolume: float = Field(ge=0)


class MarginTurnoverDaysData(BaseModel):
    """マージン回転日数データ"""

    date: str
    turnoverDays: float = Field(ge=0, description="LongVol / N-day avg volume")
    longVol: float = Field(ge=0)
    avgVolume: float = Field(ge=0)


class MarginPressureIndicatorsResponse(BaseModel):
    """マージンプレッシャー指標レスポンス"""

    symbol: str = Field(min_length=4, max_length=4)
    averagePeriod: int = Field(gt=0, description="Rolling average period in days")
    longPressure: list[MarginLongPressureData]
    flowPressure: list[MarginFlowPressureData]
    turnoverDays: list[MarginTurnoverDaysData]
    lastUpdated: str


# --- Margin Volume Ratio ---


class MarginVolumeRatioData(BaseModel):
    """マージン出来高比率データ"""

    date: str
    ratio: float = Field(ge=0)
    weeklyAvgVolume: float = Field(ge=0)
    marginVolume: float = Field(ge=0)


class MarginVolumeRatioResponse(BaseModel):
    """マージン出来高比率レスポンス"""

    symbol: str = Field(min_length=4, max_length=4)
    longRatio: list[MarginVolumeRatioData]
    shortRatio: list[MarginVolumeRatioData]
    lastUpdated: str
