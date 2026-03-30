"""
Optimization API Schemas
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from src.domains.backtest.contracts import EnginePolicy, FastCandidateSummary, VerificationSummary
from src.entrypoints.http.schemas.common import BaseJobResponse


class OptimizationRequest(BaseModel):
    """最適化リクエスト"""

    strategy_name: str = Field(
        ...,
        description="戦略名",
        min_length=1,
    )
    engine_policy: EnginePolicy = Field(
        default_factory=EnginePolicy,
        description="Fast path / verification execution policy",
    )


class OptimizationJobResponse(BaseJobResponse):
    """最適化ジョブレスポンス"""

    best_score: float | None = Field(default=None, description="最良スコア")
    best_params: dict[str, Any] | None = Field(default=None, description="最良スコア時のパラメータ")
    worst_score: float | None = Field(default=None, description="最悪スコア")
    worst_params: dict[str, Any] | None = Field(default=None, description="最悪スコア時のパラメータ")
    total_combinations: int | None = Field(default=None, description="パラメータ組み合わせ総数")
    html_path: str | None = Field(default=None, description="結果HTMLパス")
    fast_candidates: list[FastCandidateSummary] | None = Field(
        default=None,
        description="Fast-path ranked candidates",
    )
    verification: VerificationSummary | None = Field(
        default=None,
        description="Verification summary for top-ranked candidates",
    )


class OptimizationHtmlFileInfo(BaseModel):
    """最適化結果HTMLファイル情報"""

    strategy_name: str = Field(description="戦略名")
    filename: str = Field(description="ファイル名")
    dataset_name: str = Field(description="データセット名")
    created_at: datetime = Field(description="作成日時")
    size_bytes: int = Field(description="ファイルサイズ（バイト）")


class OptimizationHtmlFileListResponse(BaseModel):
    """最適化結果HTMLファイル一覧レスポンス"""

    files: list[OptimizationHtmlFileInfo] = Field(description="HTMLファイル一覧")
    total: int = Field(description="総ファイル数")


class OptimizationHtmlFileContentResponse(BaseModel):
    """最適化結果HTMLファイルコンテンツレスポンス"""

    strategy_name: str = Field(description="戦略名")
    filename: str = Field(description="ファイル名")
    html_content: str = Field(description="HTMLコンテンツ（base64エンコード）")
