"""
シグナルリファレンス レスポンススキーマ
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

SignalFieldTypeValue = Literal["boolean", "number", "string", "select"]


class FieldConstraints(BaseModel):
    """フィールド制約情報"""

    gt: float | None = None
    ge: float | None = None
    lt: float | None = None
    le: float | None = None


class SignalFieldSchema(BaseModel):
    """シグナルフィールド定義"""

    name: str
    type: SignalFieldTypeValue
    description: str
    default: bool | int | float | str | None = None
    options: list[str] | None = None
    constraints: FieldConstraints | None = None


class SignalReferenceSchema(BaseModel):
    """シグナル定義"""

    key: str = Field(description="param_keyベースの安定スラッグ")
    name: str
    category: str
    description: str
    usage_hint: str = Field(description="entry_purpose + exit_purposeから自動合成")
    fields: list[SignalFieldSchema]
    yaml_snippet: str
    exit_disabled: bool = False
    data_requirements: list[str] = Field(default_factory=list)


class SignalCategorySchema(BaseModel):
    """シグナルカテゴリ定義"""

    key: str
    label: str


class SignalReferenceResponse(BaseModel):
    """シグナルリファレンス レスポンス"""

    signals: list[SignalReferenceSchema]
    categories: list[SignalCategorySchema]
    total: int
