"""
エージェント用Pydanticモデル定義

戦略生成・最適化・改善に必要なデータ構造を型安全に管理
"""

from typing import Any, Literal

from pydantic import BaseModel, Field

SignalCategory = Literal[
    "breakout",
    "trend",
    "oscillator",
    "volatility",
    "volume",
    "macro",
    "fundamental",
    "sector",
]


class SignalConstraints(BaseModel):
    """シグナル制約定義"""

    # シグナル名
    name: str

    # 必須データ要件（例: "benchmark_data", "statements_data"）
    required_data: list[str] = Field(default_factory=list)

    # 相互排他シグナル（同時有効化不可）
    mutually_exclusive: list[str] = Field(default_factory=list)

    # 推奨組み合わせシグナル
    recommended_with: list[str] = Field(default_factory=list)

    # Entryのみ/Exitのみ/両用
    usage: str = Field(default="both", description="entry/exit/both")

    # シグナルカテゴリ（制約指定用）
    category: SignalCategory = Field(
        default="breakout",
        description="signal category",
    )


class GeneratorConfig(BaseModel):
    """戦略生成設定"""

    # 生成する戦略数
    n_strategies: int = Field(default=100, ge=1, le=10000)

    # Entryシグナル数の範囲
    entry_signal_min: int = Field(default=2, ge=1, le=10)
    entry_signal_max: int = Field(default=5, ge=1, le=10)

    # Exitシグナル数の範囲
    exit_signal_min: int = Field(default=2, ge=1, le=10)
    exit_signal_max: int = Field(default=4, ge=1, le=10)

    # 乱数シード（再現性用）
    seed: int | None = None

    # 除外シグナル（生成から除外）
    exclude_signals: list[str] = Field(default_factory=list)

    # 必須シグナル（必ず含める）
    required_signals: list[str] = Field(default_factory=list)

    # Entryフィルターのみ生成（Exitは空）
    entry_filter_only: bool = Field(default=False)

    # 許可カテゴリ（空なら全カテゴリ許可）
    allowed_categories: list[SignalCategory] = Field(default_factory=list)


class EvolutionConfig(BaseModel):
    """遺伝的アルゴリズム設定"""

    # 個体数
    population_size: int = Field(default=50, ge=10, le=500)

    # 世代数
    generations: int = Field(default=20, ge=1, le=100)

    # 突然変異率
    mutation_rate: float = Field(default=0.1, ge=0.0, le=1.0)

    # 交叉率
    crossover_rate: float = Field(default=0.7, ge=0.0, le=1.0)

    # エリート保存率（上位X%を次世代にそのまま継承）
    elite_ratio: float = Field(default=0.1, ge=0.0, le=0.5)

    # トーナメント選択サイズ
    tournament_size: int = Field(default=3, ge=2, le=10)

    # 並列ワーカー数（-1で全CPU）
    n_jobs: int = Field(default=-1, ge=-1)

    # 評価タイムアウト（秒）
    timeout_seconds: int = Field(default=600, ge=60, le=3600)

    # Entryフィルターのみ最適化（Exitパラメータは変更しない）
    entry_filter_only: bool = Field(default=False)

    # 最適化対象として許可するシグナルカテゴリ（空なら全カテゴリ）
    allowed_categories: list[SignalCategory] = Field(default_factory=list)


class StrategyCandidate(BaseModel):
    """戦略候補"""

    # 戦略ID（一意識別子）
    strategy_id: str

    # Entryシグナル設定
    entry_filter_params: dict[str, Any]

    # Exitシグナル設定
    exit_trigger_params: dict[str, Any]

    # 共有設定（オプション）
    shared_config: dict[str, Any] = Field(default_factory=dict)

    # メタデータ（生成方法、世代番号等）
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvaluationResult(BaseModel):
    """評価結果"""

    # 戦略候補
    candidate: StrategyCandidate

    # 複合スコア（正規化済み）
    score: float

    # 個別メトリクス
    sharpe_ratio: float = 0.0
    calmar_ratio: float = 0.0
    total_return: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    trade_count: int = 0

    # 評価成功フラグ
    success: bool = True

    # エラーメッセージ（失敗時）
    error_message: str | None = None


class WeaknessReport(BaseModel):
    """弱点分析レポート"""

    # 戦略名
    strategy_name: str

    # 最大ドローダウン情報
    max_drawdown: float = 0.0
    max_drawdown_start: str | None = None
    max_drawdown_end: str | None = None
    max_drawdown_duration_days: int = 0

    # 負けトレードパターン
    losing_trade_patterns: list[dict[str, Any]] = Field(default_factory=list)

    # 市場環境別パフォーマンス
    performance_by_market_condition: dict[str, float] = Field(default_factory=dict)

    # 改善提案
    suggested_improvements: list[str] = Field(default_factory=list)


class Improvement(BaseModel):
    """改善提案"""

    # 改善タイプ（add_signal, remove_signal, adjust_param）
    improvement_type: str

    # 対象（entry/exit）
    target: str

    # シグナル名
    signal_name: str

    # 変更内容
    changes: dict[str, Any] = Field(default_factory=dict)

    # 改善理由
    reason: str

    # 期待効果
    expected_impact: str


class OptunaConfig(BaseModel):
    """Optuna最適化設定"""

    # 試行回数
    n_trials: int = Field(default=100, ge=10, le=1000)

    # 並列ワーカー数（-1で全CPU）
    n_jobs: int = Field(default=-1, ge=-1)

    # 評価タイムアウト（秒）
    timeout_seconds: int = Field(default=600, ge=60, le=3600)

    # 枝刈り有効化
    pruning: bool = Field(default=True)

    # サンプラー（tpe, random, cmaes）
    sampler: str = Field(default="tpe")

    # Study名（永続化用）
    study_name: str | None = None

    # SQLite保存パス（永続化用）
    storage_path: str | None = None

    # Entryフィルターのみ最適化（Exitパラメータは変更しない）
    entry_filter_only: bool = Field(default=False)

    # 最適化対象として許可するシグナルカテゴリ（空なら全カテゴリ）
    allowed_categories: list[SignalCategory] = Field(default_factory=list)
