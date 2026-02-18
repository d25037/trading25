"""
Screening Job Schemas

非同期 Screening ジョブ向けのリクエスト/レスポンスモデル。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from src.server.schemas.common import BaseJobResponse
from src.server.schemas.screening import ScreeningSortBy, SortOrder


class ScreeningJobRequest(BaseModel):
    """Screening ジョブ作成リクエスト"""

    markets: str = Field(default="prime")
    strategies: str | None = Field(default=None)
    recentDays: int = Field(default=10, ge=1, le=90)
    date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    sortBy: ScreeningSortBy = Field(default="matchedDate")
    order: SortOrder = Field(default="desc")
    limit: int | None = Field(default=None, ge=1)

    model_config = {"extra": "forbid"}


class ScreeningJobResponse(BaseJobResponse):
    """Screening ジョブレスポンス"""

    markets: str = Field(description="市場コードフィルタ")
    strategies: str | None = Field(default=None, description="対象戦略")
    recentDays: int = Field(description="判定対象の直近日数")
    referenceDate: str | None = Field(default=None, description="基準日（任意）")
    sortBy: ScreeningSortBy = Field(description="並び順の基準")
    order: SortOrder = Field(description="並び順")
    limit: int | None = Field(default=None, description="結果件数上限")


class ScreeningJobPayload(BaseModel):
    """JobInfo.raw_result へ保持する payload"""

    response: dict[str, Any]
