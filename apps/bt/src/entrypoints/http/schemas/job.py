"""
共通ジョブスキーマ

GenericJobManager で使用される共通型。
"""

from __future__ import annotations

from enum import Enum

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


class CancelJobResponse(BaseModel):
    success: bool
    jobId: str
    message: str
