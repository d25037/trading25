"""
Backtest API Schemas
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from src.application.contracts import backtest as backtest_contracts
from src.domains.backtest.contracts import (
    ArtifactIndex,
    CanonicalExecutionResult,
    EngineFamily,
    RunSpec,
)
from src.entrypoints.http.schemas.common import BaseJobResponse


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
    engine_family: EngineFamily = Field(
        ...,
        description="実行エンジン。verification run では nautilus を明示指定する。",
    )


class SignalAttributionRequest(BaseModel):
    """シグナル寄与分析リクエスト"""

    strategy_name: str = Field(
        ...,
        description="戦略名（例: 'range_break_v5', 'production/range_break_v5'）",
        min_length=1,
    )
    strategy_config_override: dict[str, Any] | None = Field(
        default=None,
        description="戦略設定のオーバーライド（オプション）",
    )
    shapley_top_n: int = Field(
        default=5,
        ge=1,
        description="Shapley計算対象にする上位シグナル数",
    )
    shapley_permutations: int = Field(
        default=128,
        ge=1,
        description="Shapley近似時の順列サンプル数",
    )
    random_seed: int | None = Field(
        default=None,
        description="Shapley近似の乱数シード",
    )


class BacktestJobResponse(BaseJobResponse):
    """バックテストジョブレスポンス"""

    result: backtest_contracts.BacktestResultSummary | None = Field(
        default=None, description="結果サマリー（完了時のみ）"
    )


class SignalAttributionJobResponse(BaseJobResponse):
    """シグナル寄与分析ジョブレスポンス"""

    result_data: backtest_contracts.SignalAttributionResult | None = Field(
        default=None,
        description="寄与分析結果（完了時のみ）",
    )


class BacktestResultResponse(BaseModel):
    """バックテスト結果レスポンス（詳細）"""

    job_id: str = Field(description="ジョブID")
    strategy_name: str = Field(description="戦略名")
    dataset_name: str = Field(description="データセット名")
    summary: backtest_contracts.BacktestResultSummary = Field(description="結果サマリー")
    execution_time: float = Field(description="実行時間（秒）")
    html_content: str | None = Field(
        default=None, description="HTMLコンテンツ（base64エンコード）"
    )
    created_at: datetime = Field(description="作成日時")
    run_spec: RunSpec | None = Field(
        default=None,
        description="Engine-neutral execution input contract",
    )
    canonical_result: CanonicalExecutionResult | None = Field(
        default=None,
        description="Engine-neutral execution result",
    )
    artifact_index: ArtifactIndex | None = Field(
        default=None,
        description="Resolved artifact index",
    )


class SignalAttributionResultResponse(BaseModel):
    """シグナル寄与分析結果レスポンス（詳細）"""

    job_id: str = Field(description="ジョブID")
    strategy_name: str = Field(description="戦略名")
    result: backtest_contracts.SignalAttributionResult = Field(description="寄与分析結果")
    created_at: datetime = Field(description="作成日時")
    canonical_result: CanonicalExecutionResult | None = Field(
        default=None,
        description="Engine-neutral execution result",
    )
    artifact_index: ArtifactIndex | None = Field(
        default=None,
        description="Resolved artifact index",
    )


class AttributionArtifactInfo(BaseModel):
    """保存済み attribution JSON ファイル情報"""

    strategy_name: str = Field(description="戦略名（ディレクトリ相対パス）")
    filename: str = Field(description="ファイル名")
    created_at: datetime = Field(description="作成日時")
    size_bytes: int = Field(description="ファイルサイズ（バイト）")
    job_id: str | None = Field(default=None, description="ファイル名から推定したジョブID")


class AttributionArtifactListResponse(BaseModel):
    """保存済み attribution JSON ファイル一覧レスポンス"""

    files: list[AttributionArtifactInfo] = Field(description="attribution JSON ファイル一覧")
    total: int = Field(description="総ファイル数")


class AttributionArtifactContentResponse(BaseModel):
    """保存済み attribution JSON ファイル内容レスポンス"""

    strategy_name: str = Field(description="戦略名（ディレクトリ相対パス）")
    filename: str = Field(description="ファイル名")
    artifact: dict[str, Any] = Field(description="保存済み attribution JSON データ")


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
