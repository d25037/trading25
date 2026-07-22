"""
Lab API Schemas

戦略自動生成・進化・最適化・改善のリクエスト/レスポンスモデル
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.application.contracts import lab as lab_contracts
from src.entrypoints.http.schemas.common import BaseJobResponse

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
LabTargetScope = Literal["entry_filter_only", "exit_trigger_only", "both"]


# ============================================
# Request Models
# ============================================


class LabGenerateRequest(BaseModel):
    """戦略自動生成リクエスト"""

    model_config = ConfigDict(extra="forbid")

    count: int = Field(default=100, ge=1, le=10000, description="生成する戦略数")
    top: int = Field(default=10, ge=1, le=100, description="評価する上位戦略数")
    seed: int | None = Field(default=None, description="乱数シード（再現性用）")
    save: bool = Field(default=True, description="結果をYAMLに保存")
    direction: Literal["longonly", "shortonly", "both"] = Field(
        default="longonly", description="売買方向"
    )
    timeframe: Literal["daily", "weekly"] = Field(default="daily", description="タイムフレーム")
    universe_preset: str | None = Field(
        default=None,
        description=(
            "market.duckdb universe preset for generated-strategy evaluation "
            "(prime/standard/growth/topix100/primeExTopix500)."
        ),
    )
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

    model_config = ConfigDict(extra="forbid")

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
        description="（互換性用）true の場合 target_scope=entry_filter_only と同義",
    )
    target_scope: LabTargetScope = Field(
        default="both",
        description="最適化対象 (entry_filter_only/exit_trigger_only/both)",
    )
    allowed_categories: list[LabSignalCategory] | None = Field(
        default=None,
        description="最適化対象として許可するカテゴリ（未指定時は全カテゴリ）",
    )
    @model_validator(mode="after")
    def _normalize_target_scope(self) -> "LabEvolveRequest":
        if self.target_scope == "exit_trigger_only" and self.entry_filter_only:
            raise ValueError(
                "entry_filter_only=true と target_scope=exit_trigger_only は同時指定できません"
            )
        if self.target_scope == "both" and self.entry_filter_only:
            self.target_scope = "entry_filter_only"
        self.entry_filter_only = self.target_scope == "entry_filter_only"
        return self


class LabOptimizeRequest(BaseModel):
    """Optuna最適化リクエスト"""

    model_config = ConfigDict(extra="forbid")

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
        description="（互換性用）true の場合 target_scope=entry_filter_only と同義",
    )
    target_scope: LabTargetScope = Field(
        default="both",
        description="最適化対象 (entry_filter_only/exit_trigger_only/both)",
    )
    allowed_categories: list[LabSignalCategory] | None = Field(
        default=None,
        description="最適化対象として許可するカテゴリ（未指定時は全カテゴリ）",
    )
    scoring_weights: dict[str, float] | None = Field(
        default=None, description="スコアリング重み"
    )
    @model_validator(mode="after")
    def _normalize_target_scope(self) -> "LabOptimizeRequest":
        if self.target_scope == "exit_trigger_only" and self.entry_filter_only:
            raise ValueError(
                "entry_filter_only=true と target_scope=exit_trigger_only は同時指定できません"
            )
        if self.target_scope == "both" and self.entry_filter_only:
            self.target_scope = "entry_filter_only"
        self.entry_filter_only = self.target_scope == "entry_filter_only"
        return self


class LabOptimizeRecommendationResponse(BaseModel):
    """Optuna試行回数の推奨値"""

    strategy_name: str = Field(description="戦略名")
    target_scope: LabTargetScope = Field(description="最適化対象")
    allowed_categories: list[LabSignalCategory] = Field(
        default_factory=list,
        description="カテゴリ制約（未指定は空配列）",
    )
    dimension_count: int = Field(description="探索次元数")
    minimum_trials: int = Field(description="最小推奨試行数")
    recommended_trials: int = Field(description="標準推奨試行数")
    high_quality_trials: int = Field(description="高品質探索向け推奨試行数")
    formula: str = Field(description="推奨値算出式")


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


class LabJobResponse(BaseJobResponse):
    """Lab APIジョブレスポンス"""

    lab_type: Literal["generate", "evolve", "optimize", "improve"] | None = Field(
        default=None, description="Lab処理タイプ"
    )
    strategy_name: str | None = Field(default=None, description="戦略名")
    result_data: lab_contracts.LabResultData | None = Field(
        default=None,
        description="Lab結果データ（完了時のみ）",
        discriminator="lab_type",
    )
