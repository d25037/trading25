"""
Backtest API Schemas
"""

from datetime import datetime
from typing import Any, Literal

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


class BacktestResultSummary(BaseModel):
    """バックテスト結果サマリー"""

    total_return: float = Field(description="トータルリターン (%)")
    sharpe_ratio: float = Field(description="シャープレシオ")
    sortino_ratio: float | None = Field(default=None, description="ソルティノレシオ")
    calmar_ratio: float = Field(description="カルマーレシオ")
    max_drawdown: float = Field(description="最大ドローダウン (%)")
    win_rate: float = Field(description="勝率 (%)")
    trade_count: int = Field(description="取引回数")
    html_path: str | None = Field(default=None, description="結果HTMLファイルのパス")


class SignalAttributionMetrics(BaseModel):
    """シグナル寄与分析で使用するメトリクス"""

    total_return: float = Field(description="トータルリターン")
    sharpe_ratio: float = Field(description="シャープレシオ")


class SignalAttributionLooResult(BaseModel):
    """LOO（1シグナル無効化）結果"""

    status: Literal["ok", "error"] = Field(description="計算ステータス")
    variant_metrics: SignalAttributionMetrics | None = Field(
        default=None,
        description="当該シグナル無効化時のメトリクス",
    )
    delta_total_return: float | None = Field(
        default=None,
        description="baseline - variant の total_return 差分",
    )
    delta_sharpe_ratio: float | None = Field(
        default=None,
        description="baseline - variant の sharpe_ratio 差分",
    )
    error: str | None = Field(default=None, description="エラー詳細")


class SignalAttributionShapleyResult(BaseModel):
    """Shapley寄与結果"""

    status: Literal["ok", "error"] = Field(description="計算ステータス")
    total_return: float | None = Field(default=None, description="total_returnへのShapley寄与")
    sharpe_ratio: float | None = Field(default=None, description="sharpe_ratioへのShapley寄与")
    method: str = Field(description="計算方式（exact/permutation/error）")
    sample_size: int | None = Field(default=None, description="計算に使ったサンプル数")
    error: str | None = Field(default=None, description="エラー詳細")


class SignalAttributionSignalResult(BaseModel):
    """シグナル単位の寄与結果"""

    signal_id: str = Field(description="シグナル識別子（entry.<param_key> / exit.<param_key>）")
    scope: Literal["entry", "exit"] = Field(description="シグナルの適用スコープ")
    param_key: str = Field(description="SignalParams上のparam_key")
    signal_name: str = Field(description="表示用シグナル名")
    loo: SignalAttributionLooResult = Field(description="LOO寄与結果")
    shapley: SignalAttributionShapleyResult | None = Field(
        default=None,
        description="Shapley寄与結果（topN対象外はnull）",
    )


class SignalAttributionTopNScore(BaseModel):
    """TopN選定時のスコア"""

    signal_id: str = Field(description="シグナル識別子")
    score: float = Field(description="LOO絶対値正規化の合成スコア")


class SignalAttributionTopNSelection(BaseModel):
    """Shapley対象のTopN選定情報"""

    top_n_requested: int = Field(description="要求されたTopN")
    top_n_effective: int = Field(description="実際に選定されたTopN")
    selected_signal_ids: list[str] = Field(description="Shapley計算対象のsignal_id一覧")
    scores: list[SignalAttributionTopNScore] = Field(default_factory=list, description="上位シグナルの選定スコア")


class SignalAttributionTiming(BaseModel):
    """処理時間情報"""

    total_seconds: float = Field(description="総処理時間（秒）")
    baseline_seconds: float = Field(description="baseline計算時間（秒）")
    loo_seconds: float = Field(description="LOO計算時間（秒）")
    shapley_seconds: float = Field(description="Shapley計算時間（秒）")


class SignalAttributionShapleyMeta(BaseModel):
    """Shapley計算メタ情報"""

    method: str | None = Field(default=None, description="計算方式（exact/permutation/error）")
    sample_size: int | None = Field(default=None, description="近似時のサンプル数")
    error: str | None = Field(default=None, description="エラー詳細")
    evaluations: int | None = Field(default=None, description="評価実行回数")


class SignalAttributionResult(BaseModel):
    """シグナル寄与分析結果"""

    baseline_metrics: SignalAttributionMetrics = Field(description="ベースラインのメトリクス")
    signals: list[SignalAttributionSignalResult] = Field(description="シグナル別寄与結果")
    top_n_selection: SignalAttributionTopNSelection = Field(description="Shapley対象TopNの選定情報")
    timing: SignalAttributionTiming = Field(description="処理時間情報")
    shapley: SignalAttributionShapleyMeta = Field(description="Shapley計算メタ情報")


class BacktestJobResponse(BaseJobResponse):
    """バックテストジョブレスポンス"""

    result: BacktestResultSummary | None = Field(
        default=None, description="結果サマリー（完了時のみ）"
    )


class SignalAttributionJobResponse(BaseJobResponse):
    """シグナル寄与分析ジョブレスポンス"""

    result_data: SignalAttributionResult | None = Field(
        default=None,
        description="寄与分析結果（完了時のみ）",
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


class SignalAttributionResultResponse(BaseModel):
    """シグナル寄与分析結果レスポンス（詳細）"""

    job_id: str = Field(description="ジョブID")
    strategy_name: str = Field(description="戦略名")
    result: SignalAttributionResult = Field(description="寄与分析結果")
    created_at: datetime = Field(description="作成日時")


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
