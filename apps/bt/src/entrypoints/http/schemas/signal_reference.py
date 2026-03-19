"""
シグナルリファレンス レスポンススキーマ
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from src.domains.strategy.runtime.compiler import (
    CompiledSignalAvailability,
    CompiledSignalScope,
)

SignalFieldTypeValue = Literal["boolean", "number", "string", "select"]
SignalExecutionSemantics = Literal[
    "standard",
    "next_session_round_trip",
    "current_session_round_trip",
    "overnight_round_trip",
]


class FieldConstraints(BaseModel):
    """フィールド制約情報"""

    gt: float | None = None
    ge: float | None = None
    lt: float | None = None
    le: float | None = None


class SignalFieldSchema(BaseModel):
    """シグナルフィールド定義"""

    name: str
    label: str | None = Field(default=None, description="Display label")
    type: SignalFieldTypeValue
    description: str
    default: bool | int | float | str | None = None
    options: list[str] | None = None
    constraints: FieldConstraints | None = None
    unit: str | None = Field(default=None, description="Display unit")
    placeholder: str | None = Field(default=None, description="Suggested placeholder")


class SignalChartCapability(BaseModel):
    """Chart overlay capability metadata."""

    supported: bool = True
    supported_modes: list[str] = Field(default_factory=list)
    supports_relative_mode: bool = True
    requires_benchmark: bool = False
    requires_sector_data: bool = False
    requires_margin_data: bool = False
    requires_statements_data: bool = False


class SignalReferenceSchema(BaseModel):
    """シグナル定義"""

    key: str = Field(description="param_keyベースの安定スラッグ")
    signal_type: str = Field(description="chart/signal API で使用する signal type")
    name: str
    category: str
    description: str
    summary: str | None = Field(default=None, description="Short authoring summary")
    when_to_use: list[str] = Field(default_factory=list)
    pitfalls: list[str] = Field(default_factory=list)
    examples: list[str] = Field(default_factory=list)
    usage_hint: str = Field(description="entry_purpose + exit_purposeから自動合成")
    fields: list[SignalFieldSchema]
    yaml_snippet: str
    exit_disabled: bool = False
    data_requirements: list[str] = Field(default_factory=list)
    availability_profiles: list["SignalAvailabilityProfile"] = Field(default_factory=list)
    chart: SignalChartCapability = Field(default_factory=SignalChartCapability)


class SignalAvailabilityProfile(BaseModel):
    """Compiled availability profile for a signal under one execution semantic."""

    scope: CompiledSignalScope
    execution_semantics: SignalExecutionSemantics
    availability: CompiledSignalAvailability


class SignalCategorySchema(BaseModel):
    """シグナルカテゴリ定義"""

    key: str
    label: str


class SignalReferenceResponse(BaseModel):
    """シグナルリファレンス レスポンス"""

    signals: list[SignalReferenceSchema]
    categories: list[SignalCategorySchema]
    total: int
