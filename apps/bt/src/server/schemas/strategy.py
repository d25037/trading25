"""
Strategy API Schemas
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StrategyMetadataResponse(BaseModel):
    """戦略メタデータ"""

    name: str = Field(description="戦略名（カテゴリ/名前）")
    category: str = Field(description="カテゴリ（production, experimental, etc.）")
    display_name: str | None = Field(default=None, description="表示名")
    description: str | None = Field(default=None, description="説明")
    last_modified: datetime | None = Field(default=None, description="最終更新日時")


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


class StrategyValidationRequest(BaseModel):
    """戦略設定検証リクエスト"""

    config: dict[str, Any] = Field(description="検証する戦略設定")


class StrategyValidationResponse(BaseModel):
    """戦略設定検証レスポンス"""

    valid: bool = Field(description="検証結果")
    errors: list[str] = Field(default_factory=list, description="エラーメッセージ一覧")
    warnings: list[str] = Field(default_factory=list, description="警告メッセージ一覧")


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
