"""
Metadata-driven strategy authoring helpers.
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import Enum
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from src.domains.strategy.runtime.models import ExecutionConfig
from src.entrypoints.http.schemas.signal_reference import (
    FieldConstraints,
    SignalCategorySchema,
)
from src.entrypoints.http.schemas.strategy_authoring import (
    AuthoringFieldGroupSchema,
    AuthoringFieldProvenance,
    AuthoringFieldSchema,
    DefaultConfigEditorContextResponse,
    StrategyEditorContextResponse,
    StrategyEditorReferenceResponse,
)
from src.shared.models.config import (
    ParameterOptimizationConfig,
    SharedConfig,
)


_KNOWN_STRATEGY_TOP_LEVEL_KEYS = {
    "display_name",
    "description",
    "shared_config",
    "execution",
    "entry_filter_params",
    "exit_trigger_params",
}

_SHARED_CONFIG_GROUPS = [
    AuthoringFieldGroupSchema(
        key="data",
        label="Data",
        description="Dataset and market-data loading settings.",
    ),
    AuthoringFieldGroupSchema(
        key="execution",
        label="Execution",
        description="Execution semantics and benchmark behavior.",
    ),
    AuthoringFieldGroupSchema(
        key="portfolio",
        label="Portfolio",
        description="Capital, position sizing, and portfolio construction settings.",
    ),
    AuthoringFieldGroupSchema(
        key="optimization",
        label="Optimization",
        description="Grid/random search defaults and scoring weights.",
    ),
    AuthoringFieldGroupSchema(
        key="walk_forward",
        label="Walk Forward",
        description="Train/test window settings for walk-forward analysis.",
    ),
]

_EXECUTION_GROUPS = [
    AuthoringFieldGroupSchema(
        key="execution",
        label="Execution",
        description="Notebook template and output location defaults.",
    )
]

_BASIC_FIELDS = [
    AuthoringFieldSchema(
        path="display_name",
        section="strategy",
        group="basics",
        label="Display Name",
        type="string",
        widget="text",
        description="Human-friendly name shown in the UI.",
        summary="Optional display label for the strategy.",
        placeholder="Forward EPS Driven",
        examples=["Forward EPS Driven"],
    ),
    AuthoringFieldSchema(
        path="description",
        section="strategy",
        group="basics",
        label="Description",
        type="string",
        widget="textarea",
        description="Strategy summary shown in catalog/detail views.",
        summary="Explain the strategy intent, timing, and notable constraints.",
        placeholder="Pre-open factor strategy using forward EPS revisions and liquidity filters.",
    ),
]

_SHARED_FIELD_OVERRIDES: dict[str, dict[str, Any]] = {
    "initial_cash": {
        "group": "portfolio",
        "label": "Initial Cash",
        "summary": "Starting capital for the backtest.",
        "unit": "JPY",
    },
    "fees": {
        "group": "portfolio",
        "label": "Fees",
        "summary": "Per-trade proportional fee.",
        "placeholder": "0.001",
    },
    "slippage": {
        "group": "portfolio",
        "label": "Slippage",
        "summary": "Per-trade slippage cost.",
    },
    "spread": {
        "group": "portfolio",
        "label": "Spread",
        "summary": "Per-trade bid/ask spread cost.",
    },
    "borrow_fee": {
        "group": "portfolio",
        "label": "Borrow Fee",
        "summary": "Per-trade short borrow fee.",
    },
    "max_concurrent_positions": {
        "group": "portfolio",
        "label": "Max New Positions / Day",
        "summary": "Cap the number of fresh entries per day.",
    },
    "max_exposure": {
        "group": "portfolio",
        "label": "Max Exposure",
        "summary": "Cap one position's exposure between 0 and 1.",
    },
    "start_date": {
        "group": "data",
        "label": "Start Date",
        "summary": "Optional inclusive backtest start date.",
        "placeholder": "YYYY-MM-DD",
    },
    "end_date": {
        "group": "data",
        "label": "End Date",
        "summary": "Optional inclusive backtest end date.",
        "placeholder": "YYYY-MM-DD",
    },
    "dataset": {
        "group": "data",
        "label": "Dataset",
        "widget": "combobox",
        "summary": "Dataset snapshot name resolved by the runtime.",
        "placeholder": "Override in XDG default config or strategy YAML",
    },
    "include_margin_data": {
        "group": "data",
        "label": "Include Margin Data",
        "summary": "Load local margin-interest data when signals require it.",
    },
    "include_statements_data": {
        "group": "data",
        "label": "Include Statements Data",
        "summary": "Load local statements data when signals require it.",
    },
    "relative_mode": {
        "group": "execution",
        "label": "Relative Mode",
        "summary": "Run signals against price relatives to the selected benchmark.",
    },
    "benchmark_table": {
        "group": "execution",
        "label": "Benchmark",
        "widget": "combobox",
        "summary": "Benchmark code/table used for relative mode and market-aware signals.",
        "placeholder": "topix",
        "examples": ["topix", "N225_UNDERPX"],
    },
    "group_by": {
        "group": "portfolio",
        "label": "Group Positions",
        "summary": "Aggregate instruments into one portfolio in vectorbt.",
    },
    "cash_sharing": {
        "group": "portfolio",
        "label": "Cash Sharing",
        "summary": "Allow all instruments to share one cash pool.",
    },
    "printlog": {
        "group": "portfolio",
        "label": "Verbose Logs",
        "summary": "Emit strategy logs during execution.",
    },
    "stock_codes": {
        "group": "data",
        "label": "Stock Codes",
        "widget": "string_list",
        "summary": "Use ['all'] for the whole dataset or specify custom codes.",
        "placeholder": "7203, 6758, 9984",
    },
    "direction": {
        "group": "portfolio",
        "label": "Direction",
        "summary": "Trade direction passed to the engine.",
    },
    "kelly_fraction": {
        "group": "portfolio",
        "label": "Kelly Fraction",
        "summary": "Sizing multiplier for Kelly-based allocation.",
    },
    "min_allocation": {
        "group": "portfolio",
        "label": "Min Allocation",
        "summary": "Minimum per-position allocation.",
    },
    "max_allocation": {
        "group": "portfolio",
        "label": "Max Allocation",
        "summary": "Maximum per-position allocation.",
    },
    "timeframe": {
        "group": "data",
        "label": "Timeframe",
        "summary": "Data aggregation used by the strategy runtime.",
    },
    "execution_policy.mode": {
        "group": "execution",
        "label": "Execution Policy",
        "summary": "Controls when signals become actionable and when trades execute.",
        "examples": [
            "standard",
            "next_session_round_trip",
            "current_session_round_trip",
            "overnight_round_trip",
        ],
    },
    "parameter_optimization.enabled": {
        "group": "optimization",
        "label": "Optimization Enabled",
        "summary": "Enable optimizer defaults from shared config.",
    },
    "parameter_optimization.method": {
        "group": "optimization",
        "label": "Optimization Method",
        "summary": "Default search method for parameter optimization.",
    },
    "parameter_optimization.n_trials": {
        "group": "optimization",
        "label": "Optimization Trials",
        "summary": "Trial count for random-search style optimization.",
    },
    "parameter_optimization.n_jobs": {
        "group": "optimization",
        "label": "Optimization Jobs",
        "summary": "Parallel worker count for optimization.",
    },
    "parameter_optimization.scoring_weights.sharpe_ratio": {
        "group": "optimization",
        "label": "Sharpe Weight",
        "summary": "Weight applied to Sharpe ratio in composite scoring.",
    },
    "parameter_optimization.scoring_weights.calmar_ratio": {
        "group": "optimization",
        "label": "Calmar Weight",
        "summary": "Weight applied to Calmar ratio in composite scoring.",
    },
    "parameter_optimization.scoring_weights.total_return": {
        "group": "optimization",
        "label": "Total Return Weight",
        "summary": "Weight applied to total return in composite scoring.",
    },
    "walk_forward.enabled": {
        "group": "walk_forward",
        "label": "Walk Forward Enabled",
        "summary": "Enable walk-forward evaluation.",
    },
    "walk_forward.train_window": {
        "group": "walk_forward",
        "label": "Train Window",
        "summary": "Number of bars in each training window.",
    },
    "walk_forward.test_window": {
        "group": "walk_forward",
        "label": "Test Window",
        "summary": "Number of bars in each validation window.",
    },
    "walk_forward.step": {
        "group": "walk_forward",
        "label": "Walk Forward Step",
        "summary": "Optional shift between windows. Blank means use the test window.",
    },
    "walk_forward.max_splits": {
        "group": "walk_forward",
        "label": "Max Splits",
        "summary": "Optional cap on walk-forward window count.",
    },
}

_EXECUTION_FIELD_OVERRIDES: dict[str, dict[str, Any]] = {
    "template_notebook": {
        "group": "execution",
        "label": "Template Notebook",
        "summary": "Notebook template used for HTML report generation.",
        "placeholder": "notebooks/templates/strategy_analysis.py",
    },
    "output_directory": {
        "group": "execution",
        "label": "Output Directory",
        "summary": "Optional output directory override for generated artifacts.",
        "placeholder": "/path/to/output",
    },
    "create_output_dir": {
        "group": "execution",
        "label": "Create Output Directory",
        "summary": "Create the output directory automatically when missing.",
    },
}

_SIGNAL_CATEGORY_LABELS = {
    "breakout": "ブレイクアウト",
    "trend": "トレンド",
    "volume": "出来高",
    "oscillator": "オシレーター",
    "volatility": "ボラティリティ",
    "macro": "マクロ",
    "fundamental": "ファンダメンタル",
    "sector": "セクター",
}


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is Union:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _is_base_model_subclass(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _humanize_name(value: str) -> str:
    return value.replace("_", " ").strip().title()


def _resolve_default_value(field_info: FieldInfo) -> Any:
    if not field_info.is_required() and field_info.default is not PydanticUndefined:
        return field_info.default
    if field_info.default_factory is not None:
        return field_info.default_factory()
    return None


def _extract_constraints_from_json_schema(
    model_class: type[BaseModel],
) -> dict[str, FieldConstraints]:
    schema = model_class.model_json_schema()
    properties = schema.get("properties", {})
    result: dict[str, FieldConstraints] = {}

    for field_name, field_schema in properties.items():
        constraints: dict[str, float] = {}
        if "exclusiveMinimum" in field_schema:
            constraints["gt"] = field_schema["exclusiveMinimum"]
        if "minimum" in field_schema:
            constraints["ge"] = field_schema["minimum"]
        if "exclusiveMaximum" in field_schema:
            constraints["lt"] = field_schema["exclusiveMaximum"]
        if "maximum" in field_schema:
            constraints["le"] = field_schema["maximum"]
        if constraints:
            result[field_name] = FieldConstraints(**constraints)

    return result


def _resolve_options(annotation: Any) -> list[str] | None:
    annotation = _unwrap_optional(annotation)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return [str(member.value) for member in annotation]

    args = get_args(annotation)
    if args and all(isinstance(arg, str) for arg in args):
        return list(args)
    return None


def _resolve_field_type(annotation: Any, widget_override: str | None = None) -> str:
    if widget_override == "string_list":
        return "string_list"

    annotation = _unwrap_optional(annotation)
    if annotation is bool:
        return "boolean"
    if annotation in (int, float):
        return "number"
    options = _resolve_options(annotation)
    if options:
        return "select"
    origin = get_origin(annotation)
    if origin in (list, tuple):
        return "string_list"
    return "string"


def _resolve_widget(field_type: str, override: str | None) -> str:
    if override is not None:
        return override
    if field_type == "boolean":
        return "switch"
    if field_type == "number":
        return "number"
    if field_type == "select":
        return "select"
    if field_type == "string_list":
        return "string_list"
    return "text"


def _build_field_schema(
    *,
    path: str,
    section: str,
    field_info: FieldInfo,
    constraints_map: dict[str, FieldConstraints],
    overrides: dict[str, dict[str, Any]],
) -> AuthoringFieldSchema:
    override = overrides.get(path, {})
    field_type = _resolve_field_type(field_info.annotation, override.get("widget"))
    widget = _resolve_widget(field_type, override.get("widget"))
    options = override.get("options") or _resolve_options(field_info.annotation)
    default_value = override.get("default", _resolve_default_value(field_info))

    return AuthoringFieldSchema(
        path=path,
        section=section,  # type: ignore[arg-type]
        group=override.get("group"),
        label=override.get("label", _humanize_name(path.split(".")[-1])),
        type=field_type,  # type: ignore[arg-type]
        widget=widget,  # type: ignore[arg-type]
        description=override.get("description", field_info.description or ""),
        summary=override.get("summary"),
        default=default_value,
        options=options,
        constraints=constraints_map.get(path.split(".")[-1]),
        placeholder=override.get("placeholder"),
        unit=override.get("unit"),
        examples=override.get("examples", []),
        required=field_info.is_required(),
        advanced_only=override.get("advanced_only", False),
    )


def _build_scoring_weight_fields() -> list[AuthoringFieldSchema]:
    defaults = ParameterOptimizationConfig().scoring_weights
    fields: list[AuthoringFieldSchema] = []
    for key, value in defaults.items():
        path = f"parameter_optimization.scoring_weights.{key}"
        override = _SHARED_FIELD_OVERRIDES.get(path, {})
        fields.append(
            AuthoringFieldSchema(
                path=path,
                section="shared_config",
                group=override.get("group", "optimization"),
                label=override.get("label", _humanize_name(key)),
                type="number",
                widget="number",
                description=override.get(
                    "description",
                    f"Composite scoring weight for {key}.",
                ),
                summary=override.get("summary"),
                default=value,
                placeholder=override.get("placeholder"),
                unit=override.get("unit"),
                examples=override.get("examples", []),
            )
        )
    return fields


def _build_model_fields(
    *,
    model_class: type[BaseModel],
    section: str,
    prefix: str = "",
    overrides: dict[str, dict[str, Any]],
) -> list[AuthoringFieldSchema]:
    fields: list[AuthoringFieldSchema] = []
    constraints_map = _extract_constraints_from_json_schema(model_class)

    for name, field_info in model_class.model_fields.items():
        path = f"{prefix}.{name}" if prefix else name
        annotation = _unwrap_optional(field_info.annotation)

        if path == "parameter_optimization.scoring_weights":
            fields.extend(_build_scoring_weight_fields())
            continue

        if _is_base_model_subclass(annotation):
            fields.extend(
                _build_model_fields(
                    model_class=annotation,
                    section=section,
                    prefix=path,
                    overrides=overrides,
                )
            )
            continue

        fields.append(
            _build_field_schema(
                path=path,
                section=section,
                field_info=field_info,
                constraints_map=constraints_map,
                overrides=overrides,
            )
        )

    return fields


def build_strategy_editor_reference() -> StrategyEditorReferenceResponse:
    """Build editor metadata for strategy/default config authoring."""
    shared_config_fields = _build_model_fields(
        model_class=SharedConfig,
        section="shared_config",
        overrides=_SHARED_FIELD_OVERRIDES,
    )
    execution_fields = _build_model_fields(
        model_class=ExecutionConfig,
        section="execution",
        overrides=_EXECUTION_FIELD_OVERRIDES,
    )
    signal_categories = [
        SignalCategorySchema(key=key, label=label)
        for key, label in _SIGNAL_CATEGORY_LABELS.items()
    ]

    return StrategyEditorReferenceResponse(
        basics=list(_BASIC_FIELDS),
        shared_config_fields=shared_config_fields,
        execution_fields=execution_fields,
        shared_config_groups=list(_SHARED_CONFIG_GROUPS),
        execution_groups=list(_EXECUTION_GROUPS),
        signal_categories=signal_categories,
    )


def _validate_model_dump(model_class: type[BaseModel], payload: dict[str, Any]) -> dict[str, Any]:
    if model_class is SharedConfig:
        validated = model_class.model_validate(
            payload,
            context={"resolve_stock_codes": False},
        )
    else:
        validated = model_class.model_validate(payload)
    return validated.model_dump(mode="json")


def _path_exists(payload: dict[str, Any], path: str) -> bool:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def _resolve_field_provenance(
    raw_payload: dict[str, Any],
    fields: Iterable[AuthoringFieldSchema],
) -> list[AuthoringFieldProvenance]:
    provenance: list[AuthoringFieldProvenance] = []
    for field in fields:
        overridden = _path_exists(raw_payload, field.path)
        provenance.append(
            AuthoringFieldProvenance(
                path=field.path,
                source="strategy" if overridden else "default",
                overridden=overridden,
            )
        )
    return provenance


def _resolve_default_shared_payload(default_config: dict[str, Any]) -> dict[str, Any]:
    parameters = default_config.get("parameters", {})
    shared_config = parameters.get("shared_config", {})
    if not isinstance(shared_config, dict):
        shared_config = {}
    return shared_config


def _resolve_default_execution_payload(default_config: dict[str, Any]) -> dict[str, Any]:
    execution = default_config.get("execution", {})
    if not isinstance(execution, dict):
        execution = {}
    return execution


def build_strategy_editor_context(
    *,
    strategy_name: str,
    category: str,
    raw_config: dict[str, Any],
    default_config: dict[str, Any],
    merged_shared_config: dict[str, Any],
    merged_execution_config: dict[str, Any],
) -> StrategyEditorContextResponse:
    """Build editor context for one strategy."""
    reference = build_strategy_editor_reference()
    raw_shared_config = raw_config.get("shared_config", {})
    raw_execution = raw_config.get("execution", {})
    if not isinstance(raw_shared_config, dict):
        raw_shared_config = {}
    if not isinstance(raw_execution, dict):
        raw_execution = {}

    default_shared_config = _validate_model_dump(
        SharedConfig,
        _resolve_default_shared_payload(default_config),
    )
    default_execution = _validate_model_dump(
        ExecutionConfig,
        _resolve_default_execution_payload(default_config),
    )

    unknown_top_level_keys = sorted(
        key for key in raw_config.keys() if key not in _KNOWN_STRATEGY_TOP_LEVEL_KEYS
    )

    return StrategyEditorContextResponse(
        strategy_name=strategy_name,
        category=category,
        raw_config=raw_config,
        default_shared_config=default_shared_config,
        default_execution=default_execution,
        effective_shared_config=_validate_model_dump(SharedConfig, merged_shared_config),
        effective_execution=_validate_model_dump(ExecutionConfig, merged_execution_config),
        shared_config_provenance=_resolve_field_provenance(
            raw_shared_config,
            reference.shared_config_fields,
        ),
        execution_provenance=_resolve_field_provenance(
            raw_execution,
            reference.execution_fields,
        ),
        unknown_top_level_keys=unknown_top_level_keys,
    )


def build_default_config_editor_context(
    *,
    raw_yaml: str,
    raw_document: dict[str, Any],
) -> DefaultConfigEditorContextResponse:
    """Build editor context for default.yaml."""
    default_section = raw_document.get("default", {})
    if not isinstance(default_section, dict):
        default_section = {}

    raw_execution = default_section.get("execution", {})
    if not isinstance(raw_execution, dict):
        raw_execution = {}

    parameters = default_section.get("parameters", {})
    if not isinstance(parameters, dict):
        parameters = {}

    raw_shared_config = parameters.get("shared_config", {})
    if not isinstance(raw_shared_config, dict):
        raw_shared_config = {}

    advanced_only_paths: list[str] = []
    for key in default_section.keys():
        if key not in {"execution", "parameters"}:
            advanced_only_paths.append(f"default.{key}")
    for key in parameters.keys():
        if key != "shared_config":
            advanced_only_paths.append(f"default.parameters.{key}")

    return DefaultConfigEditorContextResponse(
        raw_yaml=raw_yaml,
        raw_document=raw_document,
        raw_execution=raw_execution,
        raw_shared_config=raw_shared_config,
        effective_execution=_validate_model_dump(ExecutionConfig, raw_execution),
        effective_shared_config=_validate_model_dump(SharedConfig, raw_shared_config),
        advanced_only_paths=advanced_only_paths,
    )
