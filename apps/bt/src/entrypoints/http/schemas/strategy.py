"""
Strategy API Schemas
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from src.domains.strategy.runtime.compiler import CompiledStrategyIR
from src.entrypoints.http.schemas.screening import (
    EntryDecidability,
    ScreeningSupport,
)


class StrategyMetadataResponse(BaseModel):
    """戦略メタデータ"""

    name: str = Field(description="戦略名（カテゴリ/名前）")
    category: str = Field(description="カテゴリ（production, experimental, etc.）")
    display_name: str | None = Field(default=None, description="表示名")
    description: str | None = Field(default=None, description="説明")
    last_modified: datetime | None = Field(default=None, description="最終更新日時")
    screening_support: ScreeningSupport = Field(
        default="unsupported",
        description="screening support classification for analysis UI",
    )
    entry_decidability: EntryDecidability | None = Field(
        default=None,
        description="whether entry can be decided before the execution session opens",
    )
    screening_error: str | None = Field(
        default=None,
        description="validation error when screening availability cannot be resolved",
    )
    dataset_name: str | None = Field(
        default=None,
        description="resolved dataset snapshot name",
    )
    dataset_preset: str | None = Field(
        default=None,
        description="dataset preset recorded in manifest",
    )
    screening_default_markets: list[str] | None = Field(
        default=None,
        description="default screening markets inferred from strategy dataset preset",
    )


class StrategyListResponse(BaseModel):
    """戦略一覧レスポンス"""

    strategies: list[StrategyMetadataResponse] = Field(description="戦略一覧")
    total: int = Field(description="総数")


class StrategyDetailResponse(BaseModel):
    """戦略詳細レスポンス"""

    name: str = Field(description="戦略名")
    category: str = Field(description="カテゴリ")
    display_name: str | None = Field(default=None, description="表示名")
    description: str | None = Field(default=None, description="説明")
    config: dict[str, Any] = Field(description="戦略設定（YAML）")
    execution_info: dict[str, Any] = Field(description="実行情報")


class OptimizationDiagnosticResponse(BaseModel):
    """Optimization spec diagnostics."""

    path: str = Field(description="Dot-separated path")
    message: str = Field(description="Diagnostic message")


class StrategyOptimizationStateResponse(BaseModel):
    """Strategy-linked optimization state."""

    strategy_name: str = Field(description="戦略名")
    persisted: bool = Field(description="戦略YAMLに保存済みか")
    source: Literal["saved", "draft"] = Field(description="Response source")
    optimization: dict[str, Any] | None = Field(
        default=None,
        description="Structured optimization block",
    )
    yaml_content: str = Field(description="Optimization block YAML")
    valid: bool = Field(description="Spec validation result")
    ready_to_run: bool = Field(description="Whether optimization can run immediately")
    param_count: int = Field(description="Parameter leaf count")
    combinations: int = Field(description="Cartesian product size")
    errors: list[OptimizationDiagnosticResponse] = Field(
        default_factory=list,
        description="Blocking validation issues",
    )
    warnings: list[OptimizationDiagnosticResponse] = Field(
        default_factory=list,
        description="Non-blocking validation issues",
    )
    drift: list[OptimizationDiagnosticResponse] = Field(
        default_factory=list,
        description="Strategy/spec drift diagnostics",
    )


class StrategyOptimizationSaveRequest(BaseModel):
    """Optimization block save request."""

    yaml_content: str = Field(description="Optimization block YAML")


class StrategyOptimizationSaveResponse(StrategyOptimizationStateResponse):
    """Strategy optimization save response."""

    success: bool = Field(description="保存成功フラグ")


class StrategyOptimizationDeleteResponse(BaseModel):
    """Strategy optimization delete response."""

    success: bool = Field(description="削除成功フラグ")
    strategy_name: str = Field(description="戦略名")


class StrategyValidationRequest(BaseModel):
    """戦略設定検証リクエスト"""

    config: dict[str, Any] = Field(description="検証する戦略設定")


class StrategyValidationResponse(BaseModel):
    """戦略設定検証レスポンス"""

    valid: bool = Field(description="検証結果")
    errors: list[str] = Field(default_factory=list, description="エラーメッセージ一覧")
    warnings: list[str] = Field(default_factory=list, description="警告メッセージ一覧")
    compiled_strategy: CompiledStrategyIR | None = Field(
        default=None,
        description="Shadow-compiled strategy IR when validation succeeds",
    )


# ============================================
# Strategy CRUD Schemas
# ============================================


class StrategyUpdateRequest(BaseModel):
    """戦略設定更新リクエスト"""

    config: dict[str, Any] = Field(description="更新する戦略設定")


class StrategyUpdateResponse(BaseModel):
    """戦略設定更新レスポンス"""

    success: bool = Field(description="更新成功フラグ")
    strategy_name: str = Field(description="戦略名")
    path: str = Field(description="保存先パス")


class StrategyDeleteResponse(BaseModel):
    """戦略削除レスポンス"""

    success: bool = Field(description="削除成功フラグ")
    strategy_name: str = Field(description="削除した戦略名")


class StrategyDuplicateRequest(BaseModel):
    """戦略複製リクエスト"""

    new_name: str = Field(description="新しい戦略名（カテゴリなし）")


class StrategyDuplicateResponse(BaseModel):
    """戦略複製レスポンス"""

    success: bool = Field(description="複製成功フラグ")
    new_strategy_name: str = Field(description="新しい戦略名")
    path: str = Field(description="保存先パス")


class StrategyRenameRequest(BaseModel):
    """戦略リネームリクエスト"""

    new_name: str = Field(
        description="新しい戦略名（カテゴリなし）",
        min_length=1,
        max_length=100,
    )


class StrategyRenameResponse(BaseModel):
    """戦略リネームレスポンス"""

    success: bool = Field(description="リネーム成功フラグ")
    old_name: str = Field(description="変更前の戦略名")
    new_name: str = Field(description="変更後の戦略名")
    new_path: str = Field(description="新しいファイルパス")


class StrategyMoveRequest(BaseModel):
    """戦略カテゴリ移動リクエスト"""

    target_category: Literal["production", "experimental", "legacy"] = Field(
        description="移動先カテゴリ"
    )


class StrategyMoveResponse(BaseModel):
    """戦略カテゴリ移動レスポンス"""

    success: bool = Field(description="移動成功フラグ")
    old_strategy_name: str = Field(description="移動前の戦略名")
    new_strategy_name: str = Field(description="移動後の戦略名")
    target_category: str = Field(description="移動先カテゴリ")
    new_path: str = Field(description="新しいファイルパス")


# ============================================
# Default Config Schemas
# ============================================


class DefaultConfigResponse(BaseModel):
    """デフォルト設定レスポンス（raw YAML文字列）"""

    content: str = Field(description="default.yamlの内容（YAML文字列）")


class DefaultConfigUpdateRequest(BaseModel):
    """デフォルト設定更新リクエスト"""

    content: str = Field(description="更新するYAML文字列")


class DefaultConfigUpdateResponse(BaseModel):
    """デフォルト設定更新レスポンス"""

    success: bool = Field(description="更新成功フラグ")
