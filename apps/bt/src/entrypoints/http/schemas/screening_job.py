"""
Screening Job Schemas

非同期 Screening ジョブ向けのリクエスト/レスポンスモデル。
"""

from __future__ import annotations

from pydantic import Field

from src.domains.analytics import screening_results
from src.domains.strategy.runtime import screening_profile
from src.entrypoints.http.schemas.common import BaseJobResponse


class ScreeningJobResponse(BaseJobResponse):
    """Screening ジョブレスポンス"""

    entry_decidability: screening_profile.EntryDecidability = Field(
        default="pre_open_decidable",
        description="entry decidability classification",
    )
    markets: str = Field(description="市場コードフィルタ")
    scopeLabel: str | None = Field(default=None, description="表示用スコープラベル")
    strategies: str | None = Field(default=None, description="対象戦略")
    recentDays: int = Field(description="判定対象の直近日数")
    referenceDate: str | None = Field(default=None, description="基準日（任意）")
    sortBy: screening_results.ScreeningSortBy = Field(description="並び順の基準")
    order: screening_results.SortOrder = Field(description="並び順")
    limit: int | None = Field(default=None, description="結果件数上限")
