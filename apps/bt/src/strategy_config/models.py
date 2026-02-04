"""
Strategy config schema

YAML戦略設定の構造バリデーションを提供する。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, ValidationError

from src.models.config import SharedConfig
from src.models.signals import SignalParams


class ExecutionConfig(BaseModel):
    """実行設定"""

    template_notebook: str = Field(
        default="notebooks/templates/marimo/strategy_analysis.py",
        description="Marimoテンプレートパス",
    )
    output_directory: str | None = Field(
        default=None, description="出力ディレクトリ（Noneでデフォルト）"
    )
    create_output_dir: bool = Field(default=True, description="出力ディレクトリ作成")


class StrategyConfig(BaseModel):
    """戦略YAML設定の統合スキーマ"""

    display_name: str | None = Field(default=None, description="表示名")
    description: str | None = Field(default=None, description="説明")
    execution: ExecutionConfig | None = Field(default=None, description="実行設定")
    shared_config: SharedConfig | None = Field(default=None, description="共通設定")
    entry_filter_params: SignalParams = Field(description="エントリーフィルター")
    exit_trigger_params: SignalParams | None = Field(
        default=None, description="エグジットトリガー"
    )

    model_config = {"extra": "allow"}


def validate_strategy_config_dict(config: dict[str, Any]) -> StrategyConfig:
    """辞書をStrategyConfigとして検証（stock_codes解決は行わない）"""
    return StrategyConfig.model_validate(
        config, context={"resolve_stock_codes": False}
    )


def try_validate_strategy_config_dict(config: dict[str, Any]) -> tuple[bool, str | None]:
    """戦略設定を検証し、結果とエラーメッセージを返す"""
    try:
        validate_strategy_config_dict(config)
        return True, None
    except ValidationError as e:
        return False, str(e)
