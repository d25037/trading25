"""Application-owned Lab result contracts."""

from typing import Any, Literal

from pydantic import BaseModel, Field

from src.domains.backtest.contracts import (
    FastCandidateSummary,
    VerificationSummary,
)


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


class LabGenerateResult(BaseModel):
    """戦略自動生成結果"""

    lab_type: Literal["generate"] = "generate"
    results: list[GenerateResultItem] = Field(description="生成結果リスト")
    total_generated: int = Field(description="生成総数")
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")
    verification: VerificationSummary | None = Field(
        default=None,
        description="Verification summary for top-ranked candidates",
    )


class LabEvolveResult(BaseModel):
    """GA進化結果"""

    lab_type: Literal["evolve"] = "evolve"
    best_strategy_id: str = Field(description="最良戦略ID")
    best_score: float = Field(description="最良スコア")
    history: list[EvolutionHistoryItem] = Field(description="進化履歴")
    fast_candidates: list[FastCandidateSummary] = Field(
        default_factory=list,
        description="Fast-path ranked candidates",
    )
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")
    saved_history_path: str | None = Field(default=None, description="履歴保存先パス")
    verification: VerificationSummary | None = Field(
        default=None,
        description="Verification summary for top-ranked candidates",
    )


class LabOptimizeResult(BaseModel):
    """Optuna最適化結果"""

    lab_type: Literal["optimize"] = "optimize"
    best_score: float = Field(description="最良スコア")
    best_params: dict[str, Any] = Field(description="最良パラメータ")
    total_trials: int = Field(description="総トライアル数")
    history: list[OptimizeTrialItem] = Field(description="トライアル履歴")
    fast_candidates: list[FastCandidateSummary] = Field(
        default_factory=list,
        description="Fast-path ranked candidates",
    )
    saved_strategy_path: str | None = Field(default=None, description="保存先パス")
    saved_history_path: str | None = Field(default=None, description="履歴保存先パス")
    verification: VerificationSummary | None = Field(
        default=None,
        description="Verification summary for top-ranked candidates",
    )


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


LabResultData = LabGenerateResult | LabEvolveResult | LabOptimizeResult | LabImproveResult
