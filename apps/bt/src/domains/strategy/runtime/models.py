"""
Strategy config schema

YAML戦略設定の構造バリデーションを提供する。
"""

from __future__ import annotations

from typing import Any, get_args, get_origin

from pydantic import BaseModel, Field, ValidationError

from src.shared.models.config import SharedConfig, StrategyOptimizationConfig
from src.shared.models.signals import SignalParams


class ExecutionConfig(BaseModel):
    """実行設定"""

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
    optimization: StrategyOptimizationConfig | None = Field(
        default=None,
        description="戦略と連動した optimization 仕様",
    )
    entry_filter_params: SignalParams = Field(description="エントリーフィルター")
    exit_trigger_params: SignalParams | None = Field(
        default=None, description="エグジットトリガー"
    )

    model_config = {"extra": "allow"}


class StrategyConfigStrictValidationError(ValueError):
    """厳密バリデーションエラー（未知キー検出を含む）"""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


def _format_pydantic_error(error: ValidationError) -> list[str]:
    """ValidationErrorを可読なエラーメッセージへ整形"""
    messages: list[str] = []

    for item in error.errors():
        loc = item.get("loc", ())
        path = ".".join(str(p) for p in loc if p != "__root__")
        msg = str(item.get("msg", "Invalid value"))
        if path == "shared_config.dataset" and item.get("type") == "extra_forbidden":
            messages.append(
                "shared_config.dataset is no longer supported; "
                "use shared_config.universe_preset for PIT market universes, "
                "or shared_config.dataset_snapshot with data_source='dataset_snapshot' "
                "and static_universe=true for archived reproducibility."
            )
            continue
        messages.append(f"{path}: {msg}" if path else msg)

    return messages


def _extract_base_model(annotation: Any) -> type[BaseModel] | None:
    """フィールド型からネストされたBaseModel型を抽出"""
    origin = get_origin(annotation)

    if origin is None:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return annotation
        return None

    for arg in get_args(annotation):
        if arg is type(None):
            continue
        nested = _extract_base_model(arg)
        if nested is not None:
            return nested

    return None


def _collect_unknown_paths(
    data: Any,
    model_cls: type[BaseModel],
    path: str = "",
) -> list[str]:
    """辞書データを再帰走査して未知キーのパスを収集"""
    if not isinstance(data, dict):
        return []

    errors: list[str] = []
    fields = model_cls.model_fields

    for key, value in data.items():
        current_path = f"{path}.{key}" if path else key
        field_info = fields.get(key)
        if field_info is None:
            if current_path == "shared_config.dataset":
                continue
            errors.append(current_path)
            continue

        nested_model = _extract_base_model(field_info.annotation)
        if nested_model is not None and isinstance(value, dict):
            errors.extend(_collect_unknown_paths(value, nested_model, current_path))

    return errors


def _dedupe(messages: list[str]) -> list[str]:
    """順序を保持して重複を除去"""
    seen: set[str] = set()
    deduped: list[str] = []

    for message in messages:
        if message in seen:
            continue
        seen.add(message)
        deduped.append(message)

    return deduped


def resolve_execution_semantics(shared_config: SharedConfig) -> str:
    return shared_config.execution_policy.mode.value


def _validate_round_trip_rules(
    config: dict[str, Any],
    validated: StrategyConfig | None,
) -> list[str]:
    if validated is None or validated.shared_config is None:
        return []
    mode_name = resolve_execution_semantics(validated.shared_config)
    if mode_name == "standard":
        return []

    exit_trigger_params = config.get("exit_trigger_params")
    if exit_trigger_params not in (None, {}):
        return [
            "exit_trigger_params must be empty when "
            f"shared_config.execution_policy.mode is '{mode_name}'"
        ]
    return []


def _is_all_stock_codes(value: Any) -> bool:
    return value is None or value == ["all"]


def _validate_shared_config_data_scope(
    shared_config: Any,
    *,
    require_scope_for_all_codes: bool,
) -> list[str]:
    if not isinstance(shared_config, dict):
        return []

    errors: list[str] = []
    if "dataset" in shared_config:
        errors.append(
            "shared_config.dataset is no longer supported; "
            "use shared_config.universe_preset for PIT market universes, "
            "or shared_config.dataset_snapshot with data_source='dataset_snapshot' "
            "and static_universe=true for archived reproducibility."
        )

    data_source = shared_config.get("data_source", "market")
    if data_source not in ("market", "dataset_snapshot"):
        errors.append("shared_config.data_source must be one of: market, dataset_snapshot")
        return errors

    universe_preset = shared_config.get("universe_preset")
    dataset_snapshot = shared_config.get("dataset_snapshot")
    static_universe = shared_config.get("static_universe") is True
    stock_codes = shared_config.get("stock_codes")

    if data_source == "market":
        if isinstance(dataset_snapshot, str) and dataset_snapshot.strip():
            errors.append(
                "shared_config.dataset_snapshot is only allowed when "
                "shared_config.data_source is 'dataset_snapshot'"
            )
        if (
            require_scope_for_all_codes
            and _is_all_stock_codes(stock_codes)
            and not (isinstance(universe_preset, str) and universe_preset.strip())
        ):
            errors.append(
                "shared_config.universe_preset is required for market-backed "
                "backtest YAML when stock_codes is ['all'] or omitted."
            )
    else:
        if not (isinstance(dataset_snapshot, str) and dataset_snapshot.strip()):
            errors.append(
                "shared_config.dataset_snapshot is required when "
                "shared_config.data_source is 'dataset_snapshot'"
            )
        if not static_universe:
            errors.append(
                "shared_config.static_universe must be true when "
                "shared_config.data_source is 'dataset_snapshot'"
            )

    return errors


def _build_strict_validation_errors(config: dict[str, Any]) -> tuple[StrategyConfig | None, list[str]]:
    """型検証と未知キー検出をまとめて実行"""
    validated: StrategyConfig | None = None
    errors: list[str] = []

    try:
        validated = validate_strategy_config_dict(config)
    except ValidationError as e:
        errors.extend(_format_pydantic_error(e))

    errors.extend(_validate_round_trip_rules(config, validated))
    errors.extend(
        _validate_shared_config_data_scope(
            config.get("shared_config"),
            require_scope_for_all_codes=False,
        )
    )

    unknown_paths = _collect_unknown_paths(config, StrategyConfig)
    errors.extend(
        f"{path} is not a valid parameter name"
        for path in unknown_paths
    )

    return validated, _dedupe(errors)


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


def validate_strategy_config_dict_strict(config: dict[str, Any]) -> StrategyConfig:
    """辞書を厳密検証（未知キーを許容しない）"""
    validated, errors = _build_strict_validation_errors(config)
    if errors:
        raise StrategyConfigStrictValidationError(errors)

    if validated is None:
        raise StrategyConfigStrictValidationError(["Strategy config validation failed"])

    return validated


def try_validate_strategy_config_dict_strict(config: dict[str, Any]) -> tuple[bool, list[str]]:
    """厳密検証の結果とエラー一覧を返す"""
    validated, errors = _build_strict_validation_errors(config)
    if errors or validated is None:
        if not errors:
            return False, ["Strategy config validation failed"]
        return False, errors

    return True, []


def validate_backtest_shared_config_data_scope(shared_config: dict[str, Any]) -> None:
    """Validate merged shared_config for executable backtest/lab runs."""
    errors = _validate_shared_config_data_scope(
        shared_config,
        require_scope_for_all_codes=True,
    )
    if errors:
        raise StrategyConfigStrictValidationError(errors)
