"""
Optimization API Schemas
"""

from datetime import datetime

from pydantic import BaseModel, Field

from src.server.schemas.common import BaseJobResponse


class OptimizationRequest(BaseModel):
    """最適化リクエスト"""

    strategy_name: str = Field(
        ...,
        description="戦略名",
        min_length=1,
    )


class OptimizationJobResponse(BaseJobResponse):
    """最適化ジョブレスポンス"""

    best_score: float | None = Field(default=None, description="最良スコア")
    total_combinations: int | None = Field(default=None, description="パラメータ組み合わせ総数")
    notebook_path: str | None = Field(default=None, description="結果Notebookパス")


class OptimizationGridConfig(BaseModel):
    """Grid設定"""

    strategy_name: str = Field(description="戦略名")
    content: str = Field(description="YAML文字列")
    param_count: int = Field(description="パラメータ数")
    combinations: int = Field(description="組み合わせ数")


class OptimizationGridListResponse(BaseModel):
    """Grid設定一覧レスポンス"""

    configs: list[OptimizationGridConfig] = Field(description="Grid設定一覧")
    total: int = Field(description="総設定数")


class OptimizationGridSaveRequest(BaseModel):
    """Grid設定保存リクエスト"""

    content: str = Field(description="YAML文字列")


class OptimizationGridSaveResponse(BaseModel):
    """Grid設定保存レスポンス"""

    success: bool = Field(description="保存成功フラグ")
    strategy_name: str = Field(description="戦略名")
    param_count: int = Field(description="パラメータ数")
    combinations: int = Field(description="組み合わせ数")


class OptimizationGridDeleteResponse(BaseModel):
    """Grid設定削除レスポンス"""

    success: bool = Field(description="削除成功フラグ")
    strategy_name: str = Field(description="戦略名")


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
