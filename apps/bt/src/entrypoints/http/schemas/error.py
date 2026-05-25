"""
Unified Error Response Schema

FastAPI 全エンドポイントで使う統一エラーレスポンス定義。
"""

from typing import Literal

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    """バリデーションエラー詳細"""

    field: str = Field(description="エラーフィールド名")
    message: str = Field(description="エラーメッセージ")


class ErrorResponse(BaseModel):
    """統一エラーレスポンス"""

    status: Literal["error"] = Field(default="error", description="ステータス")
    error: str = Field(description="HTTP ステータステキスト（例: 'Not Found'）")
    message: str = Field(description="詳細エラーメッセージ")
    details: list[ErrorDetail] | None = Field(default=None, description="バリデーションエラー詳細")
    timestamp: str = Field(description="ISO 8601 タイムスタンプ")
    correlationId: str = Field(description="リクエスト追跡用 UUID")
