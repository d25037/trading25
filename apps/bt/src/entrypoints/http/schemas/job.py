"""
共通ジョブスキーマ

GenericJobManager で使用される共通型。
"""

from __future__ import annotations

from pydantic import BaseModel


class CancelJobResponse(BaseModel):
    success: bool
    jobId: str
    message: str
