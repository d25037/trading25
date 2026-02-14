"""
Lab API Schemas

戦略自動生成・進化・最適化・改善のリクエスト/レスポンスモデル
"""

from typing import Any, Literal

from pydantic import BaseModel, Field

from src.server.schemas.common import BaseJobResponse

LabSignalCategory = Literal[
    "breakout",
    "trend",
    "oscillator",
    "volatility",
    "volume",
    "macro",
    "fundamental",
    "sector",
]


# ============================================
# Request Models
# ============================================


class LabGenerateRequest(BaseModel):
    """戦略自動生成リクエスト"""

    count: int = Field(default=100, ge=1, le=10000, description="生成する戦略数")
    top: int = Field(default=10, ge=1, le=100, description="評価する上位戦略数")
    seed: int | None = Field(default=None, description="乱数シード（再現性用）")
    save: bool = Field(default=True, description="結果をYAMLに保存")
    direction: Literal["longonly", "shortonly", "both"] = Field(
        default="longonly", description="売買方向"
    )
    timeframe: Literal["daily", "weekly"] = Field(default="daily", description="タイムフレーム")
    dataset: str = Field(default="primeExTopix500", description="データセット名")
    entry_filter_only: bool = Field(
        default=False,
        description="Entryフィルターのみ生成（Exitシグナルを生成しない）",
    )
    allowed_categories: list[LabSignalCategory] | None = Field(
        default=None,
        description="許可するシグナルカテゴリ（未指定時は全カテゴリ）",
    )


class LabEvolveRequest(BaseModel):
    """GA進化リクエスト"""

    strategy_name: str = Field(..., min_length=1, description="ベース戦略名")
    generations: int = Field(default=20, ge=1, le=100, description="世代数")
    population: int = Field(default=50, ge=10, le=500, description="個体数")
    structure_mode: Literal["params_only", "random_add"] = Field(
        default="params_only",
        description="探索パターン（params_only=既存シグナルのパラメータのみ、random_add=新しいシグナルを追加して探索）",
    )
    random_add_entry_signals: int = Field(
        default=1,
        ge=0,
        le=10,
        description="random_add時に追加するentryシグナル数（ベースに対する追加分）",
    )
    random_add_exit_signals: int = Field(
        default=1,
        ge=0,
        le=10,
        description="random_add時に追加するexitシグナル数（ベースに対する追加分）",
    )
    seed: int | None = Field(default=None, description="乱数シード（再現性用）")
    save: bool = Field(default=True, description="結果をYAMLに保存")
    entry_filter_only: bool = Field(
        default=False,
        description="Entryフィルターのみ最適化（Exitパラメータは変更しない）",
    )
    allowed_categories: list[LabSignalCategory] | None = Field(
        default=None,
        description="最適化対象として許可するカテゴリ（未指定時は全カテゴリ）",
    )


class LabOptimizeRequest(BaseModel):
    """Optuna最適化リクエスト"""

    strategy_name: str = Field(..., min_length=1, description="ベース戦略名")
    trials: int = Field(default=100, ge=10, le=1000, description="試行回数")
    sampler: Literal["tpe", "random", "cmaes"] = Field(default="tpe", description="サンプラー")
    structure_mode: Literal["params_only", "random_add"] = Field(
        default="params_only",
        description="探索パターン（params_only=既存シグナルのパラメータのみ、random_add=新しいシグナルを追加して探索）",
    )
    random_add_entry_signals: int = Field(
        default=1,
        ge=0,
        le=10,
        description="random_add時に追加するentryシグナル数（ベースに対する追加分）",
    )
    random_add_exit_signals: int = Field(
        default=1,
        ge=0,
        le=10,
        description="random_add時に追加するexitシグナル数（ベースに対する追加分）",
    )
    seed: int | None = Field(default=None, description="乱数シード（再現性用）")
    save: bool = Field(default=True, description="結果をYAMLに保存")
    entry_filter_only: bool = Field(
        default=False,
        description="Entryフィルターのみ最適化（Exitパラメータは変更しない）",
    )
    allowed_categories: list[LabSignalCategory] | None = Field(
        default=None,
        description="最適化対象として許可するカテゴリ（未指定時は全カテゴリ）",
    )
    scoring_weights: dict[str, float] | None = Field(
        default=None, description="スコアリング重み"
    )


class LabImproveRequest(BaseModel):
    """戦略改善リクエスト"""

    strategy_name: str = Field(..., min_length=1, description="改善対象の戦略名")
    auto_apply: bool = Field(default=True, description="改善を自動適用")
    entry_filter_only: bool = Field(
        default=False,
        description="Entryフィルターの改善のみを許可",
    )
    allowed_categories: list[LabSignalCategory] | None = Field(
        default=None,
        description="改善対象として許可するカテゴリ（未指定時は全カテゴリ）",
    )


# ============================================
# Result Item Models
# ============================================


class GenerateResultItem(BaseModel):
    """生成結果アイテム"""

    strategy_id: str = Field(description="戦略ID")
    score: float = Field(description="評価スコア")
    sharpe_ratio: float = Field(default=0.0, description="シャープレシオ")
    calmar_ratio: float = Field(default=0.0, description="カルマーレシオ")
    total_return: float = Field(default=0.0, description="トータルリターン")
    max_drawdown: float = Field(default=0.0, description="最大ドローダウン")
    win_rate: float = Field(default=0.0, description="勝率")
    trade_count: int = Field(default=0, description="取引回数")
    entry_signals: list[str] = Field(default_factory=list, description="エントリーシグナル")
    exit_signals: list[str] = Field(default_factory=list, description="エグジットシグナル")


class EvolutionHistoryItem(BaseModel):
    """進化履歴アイテム"""

    generation: int = Field(description="世代番号")
    best_score: float = Field(description="最良スコア")
    avg_score: float = Field(description="平均スコア")
    worst_score: float = Field(description="最悪スコア")


class OptimizeTrialItem(BaseModel):
    """最適化トライアルアイテム"""

    trial: int = Field(description="トライアル番号")
    score: float = Field(description="スコア")
    params: dict[str, Any] = Field(description="パラメータ")


class ImprovementItem(BaseModel):
    """改善アイテム"""

    improvement_type: str = Field(description="改善タイプ")
    target: str = Field(description="対象 (entry/exit)")
    signal_name: str = Field(description="シグナル名")
    changes: dict[str, Any] = Field(default_factory=dict, description="変更内容")
    reason: str = Field(description="理由")
    expected_impact: str = Field(description="期待される効果")


# ============================================
# Result Models (discriminated union)
# ============================================


class LabGenerateResult(BaseModel):
    """戦略自動生成結果"""

    lab_type: Literal["generate"] = "generate"
    results: list[GenerateResultItem] = Field(description="生成結果リスト")
    total_generated: int = Field(description="生成総数")
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")


class LabEvolveResult(BaseModel):
    """GA進化結果"""

    lab_type: Literal["evolve"] = "evolve"
    best_strategy_id: str = Field(description="最良戦略ID")
    best_score: float = Field(description="最良スコア")
    history: list[EvolutionHistoryItem] = Field(description="進化履歴")
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")
    saved_history_path: str | None = Field(default=None, description="履歴保存先パス")


class LabOptimizeResult(BaseModel):
    """Optuna最適化結果"""

    lab_type: Literal["optimize"] = "optimize"
    best_score: float = Field(description="最良スコア")
    best_params: dict[str, Any] = Field(description="最良パラメータ")
    total_trials: int = Field(description="総トライアル数")
    history: list[OptimizeTrialItem] = Field(description="トライアル履歴")
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")
    saved_history_path: str | None = Field(default=None, description="履歴保存先パス")


class LabImproveResult(BaseModel):
    """戦略改善結果"""

    lab_type: Literal["improve"] = "improve"
    strategy_name: str = Field(description="戦略名")
    max_drawdown: float = Field(default=0.0, description="最大ドローダウン")
    max_drawdown_duration_days: int = Field(default=0, description="最大DD期間（日）")
    suggested_improvements: list[str] = Field(
        default_factory=list, description="改善提案サマリー"
    )
    improvements: list[ImprovementItem] = Field(
        default_factory=list, description="具体的改善リスト"
    )
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")


# ============================================
# Response Model
# ============================================

LabResultData = LabGenerateResult | LabEvolveResult | LabOptimizeResult | LabImproveResult


class LabJobResponse(BaseJobResponse):
    """Lab APIジョブレスポンス"""

    lab_type: Literal["generate", "evolve", "optimize", "improve"] | None = Field(
        default=None, description="Lab処理タイプ"
    )
    strategy_name: str | None = Field(default=None, description="戦略名")
    result_data: LabResultData | None = Field(
        default=None,
        description="Lab結果データ（完了時のみ）",
        discriminator="lab_type",
    )
