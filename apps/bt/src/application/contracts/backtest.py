"""Application-owned backtest result contracts."""

from typing import Literal

from pydantic import BaseModel, Field


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
