"""
Backtest API Schemas
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from src.server.schemas.common import BaseJobResponse, JobStatus

# JobStatus を後方互換性のため再エクスポート
__all__ = ["JobStatus"]


class BacktestRequest(BaseModel):
    """バックテスト実行リクエスト"""

    strategy_name: str = Field(
        ...,
        description="戦略名（例: 'range_break_v5', 'production/range_break_v5'）",
        min_length=1,
    )
    strategy_config_override: dict[str, Any] | None = Field(
        default=None,
        description="戦略設定のオーバーライド（オプション）",
    )


class BacktestResultSummary(BaseModel):
    """バックテスト結果サマリー"""

    total_return: float = Field(description="トータルリターン (%)")
    sharpe_ratio: float = Field(description="シャープレシオ")
    calmar_ratio: float = Field(description="カルマーレシオ")
    max_drawdown: float = Field(description="最大ドローダウン (%)")
    win_rate: float = Field(description="勝率 (%)")
    trade_count: int = Field(description="取引回数")
    html_path: str | None = Field(default=None, description="結果HTMLファイルのパス")


class BacktestJobResponse(BaseJobResponse):
    """バックテストジョブレスポンス"""

    result: BacktestResultSummary | None = Field(
        default=None, description="結果サマリー（完了時のみ）"
    )


class BacktestResultResponse(BaseModel):
    """バックテスト結果レスポンス（詳細）"""

    job_id: str = Field(description="ジョブID")
    strategy_name: str = Field(description="戦略名")
    dataset_name: str = Field(description="データセット名")
    summary: BacktestResultSummary = Field(description="結果サマリー")
    execution_time: float = Field(description="実行時間（秒）")
    html_content: str | None = Field(
        default=None, description="HTMLコンテンツ（base64エンコード）"
    )
    created_at: datetime = Field(description="作成日時")


class HtmlFileInfo(BaseModel):
    """HTMLファイル情報"""

    strategy_name: str = Field(description="戦略名")
    filename: str = Field(description="ファイル名")
    dataset_name: str = Field(description="データセット名")
    created_at: datetime = Field(description="作成日時")
    size_bytes: int = Field(description="ファイルサイズ（バイト）")


class HtmlFileListResponse(BaseModel):
    """HTMLファイル一覧レスポンス"""

    files: list[HtmlFileInfo] = Field(description="HTMLファイル一覧")
    total: int = Field(description="総ファイル数")


class HtmlFileMetrics(BaseModel):
    """HTMLファイルから抽出したメトリクス"""

    total_return: float | None = None
    max_drawdown: float | None = None
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    calmar_ratio: float | None = None
    win_rate: float | None = None
    profit_factor: float | None = None
    total_trades: int | None = None


class HtmlFileContentResponse(BaseModel):
    """HTMLファイルコンテンツレスポンス"""

    strategy_name: str = Field(description="戦略名")
    filename: str = Field(description="ファイル名")
    html_content: str = Field(description="HTMLコンテンツ（base64エンコード）")
    metrics: HtmlFileMetrics | None = Field(default=None, description="メトリクス")


class HtmlFileRenameRequest(BaseModel):
    """HTMLファイルリネームリクエスト"""

    new_filename: str = Field(
        description="新しいファイル名（.html拡張子必須）",
        min_length=1,
        max_length=200,
    )


class HtmlFileRenameResponse(BaseModel):
    """HTMLファイルリネームレスポンス"""

    success: bool = Field(description="リネーム成功フラグ")
    strategy_name: str = Field(description="戦略名")
    old_filename: str = Field(description="変更前のファイル名")
    new_filename: str = Field(description="変更後のファイル名")


class HtmlFileDeleteResponse(BaseModel):
    """HTMLファイル削除レスポンス"""

    success: bool = Field(description="削除成功フラグ")
    strategy_name: str = Field(description="戦略名")
    filename: str = Field(description="削除されたファイル名")
