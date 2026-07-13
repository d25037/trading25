"""Application-owned job lifecycle contracts."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobProgress(BaseModel):
    stage: str = Field(description="Current stage name")
    current: int = Field(description="Current step number")
    total: int = Field(description="Total steps")
    percentage: float = Field(description="Progress percentage 0-100")
    message: str = Field(description="Human-readable progress message")


class JobEvent(BaseModel):
    job_id: str = Field(description="ジョブID")
    status: str = Field(description="ジョブステータス")
    progress: float | None = Field(default=None, description="進捗（0.0 - 1.0）")
    message: str | None = Field(default=None, description="ステータスメッセージ")
    data: dict[str, Any] | None = Field(default=None, description="追加データ")
