"""
Common API Schemas

ジョブステータスとジョブレスポンスの共通基底クラス
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """ジョブステータス"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BaseJobResponse(BaseModel):
    """ジョブレスポンス基底クラス"""

    job_id: str = Field(description="ジョブID")
    status: JobStatus = Field(description="ジョブステータス")
    progress: float | None = Field(default=None, description="進捗（0.0 - 1.0）")
    message: str | None = Field(default=None, description="ステータスメッセージ")
    created_at: datetime = Field(description="作成日時")
    started_at: datetime | None = Field(default=None, description="開始日時")
    completed_at: datetime | None = Field(default=None, description="完了日時")
    error: str | None = Field(default=None, description="エラーメッセージ")

    model_config = {"use_enum_values": True}


class SSEJobEvent(BaseModel):
    """SSEジョブイベント"""

    job_id: str = Field(description="ジョブID")
    status: str = Field(description="ジョブステータス")
    progress: float | None = Field(default=None, description="進捗（0.0 - 1.0）")
    message: str | None = Field(default=None, description="ステータスメッセージ")
    data: dict[str, Any] | None = Field(default=None, description="追加データ")
