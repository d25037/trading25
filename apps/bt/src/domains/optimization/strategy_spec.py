"""
Strategy-linked optimization spec helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from io import StringIO
from typing import Any

from pydantic import BaseModel
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from src.domains.optimization.grid_loader import generate_combinations
from src.domains.optimization.grid_validation import (
    GridValidationIssue,
    validate_parameter_ranges,
)
from src.shared.models.config import StrategyOptimizationConfig
from src.shared.models.signals import SignalParams

_STRUCTURAL_PARAM_NAMES = {
    "direction",
    "condition",
    "state",
    "type",
    "ma_type",
    "price_column",
    "period_type",
    "use_adjusted",
}
_SECTION_NAMES = ("entry_filter_params", "exit_trigger_params")
_FLOAT_EPSILON = 1e-6


@dataclass
class StrategyOptimizationAnalysis:
    optimization: dict[str, Any] | None
    yaml_content: str
    valid: bool
    ready_to_run: bool
    param_count: int
    combinations: int
    errors: list[GridValidationIssue] = field(default_factory=list)
    warnings: list[GridValidationIssue] = field(default_factory=list)
    drift: list[GridValidationIssue] = field(default_factory=list)


def _issue(path: str, message: str) -> GridValidationIssue:
    return GridValidationIssue(path=path, message=message)


def _create_yaml() -> YAML:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False
    yaml.indent(mapping=2, sequence=2, offset=2)
    return yaml


def dump_optimization_yaml(optimization: dict[str, Any] | None) -> str:
    if optimization is None:
        return ""

    buffer = StringIO()
    _create_yaml().dump(optimization, buffer)
    return buffer.getvalue()


def parse_optimization_yaml(content: str) -> tuple[dict[str, Any] | None, list[GridValidationIssue]]:
    yaml = _create_yaml()
    try:
        document = yaml.load(content)
    except YAMLError as exc:
        return None, [_issue("$", f"YAML parse error: {exc}")]

    if document is None:
        return None, [_issue("$", "Optimization YAML must be a mapping.")]
    if not isinstance(document, dict):
        return None, [_issue("$", "Optimization YAML root must be a mapping.")]

    return dict(document), []


def analyze_saved_strategy_optimization(
    strategy_config: dict[str, Any],
) -> StrategyOptimizationAnalysis:
    raw_optimization = strategy_config.get("optimization")
    return analyze_strategy_optimization(strategy_config, raw_optimization)


def analyze_strategy_optimization(
    strategy_config: dict[str, Any],
    optimization: Any,
) -> StrategyOptimizationAnalysis:
    errors: list[GridValidationIssue] = []
    warnings: list[GridValidationIssue] = []
    drift: list[GridValidationIssue] = []
    normalized_optimization: dict[str, Any] | None = None

    if optimization is None:
        return StrategyOptimizationAnalysis(
            optimization=None,
            yaml_content="",
            valid=True,
            ready_to_run=False,
            param_count=0,
            combinations=0,
        )

    if not isinstance(optimization, dict):
        return StrategyOptimizationAnalysis(
            optimization=None,
            yaml_content="",
            valid=False,
            ready_to_run=False,
            param_count=0,
            combinations=0,
            errors=[_issue("optimization", "Optimization block must be a mapping.")],
        )

    try:
        normalized_optimization = StrategyOptimizationConfig.model_validate(
            optimization
        ).model_dump(mode="json")
    except Exception as exc:
        errors.append(_issue("optimization", str(exc)))
        normalized_optimization = dict(optimization)

    parameter_ranges = (
        normalized_optimization.get("parameter_ranges", {})
        if isinstance(normalized_optimization, dict)
        else {}
    )
    base_validation = validate_parameter_ranges(parameter_ranges)
    errors.extend(base_validation.errors)
    warnings.extend(base_validation.warnings)

    if not errors and isinstance(parameter_ranges, dict):
        errors.extend(
            _validate_parameter_ranges_against_strategy(
                strategy_config=strategy_config,
                parameter_ranges=parameter_ranges,
                drift=drift,
            )
        )
        drift.extend(
            _collect_missing_range_drift(
                strategy_config=strategy_config,
                parameter_ranges=parameter_ranges,
            )
        )

    valid = len(errors) == 0
    ready_to_run = valid and base_validation.ready_to_run

    return StrategyOptimizationAnalysis(
        optimization=normalized_optimization,
        yaml_content=dump_optimization_yaml(normalized_optimization),
        valid=valid,
        ready_to_run=ready_to_run,
        param_count=base_validation.param_count,
        combinations=base_validation.combinations,
        errors=_dedupe_issues(errors),
        warnings=_dedupe_issues(warnings),
        drift=_dedupe_issues(drift),
    )


def generate_strategy_optimization_draft(
    strategy_config: dict[str, Any],
) -> StrategyOptimizationAnalysis:
    warnings: list[GridValidationIssue] = []
    parameter_ranges: dict[str, dict[str, Any]] = {}

    for section_name, section_params in _build_section_signal_params(strategy_config).items():
        draft_section: dict[str, Any] = {}
        for signal_name in section_params.__class__.model_fields:
            current_signal_value = getattr(section_params, signal_name)
            signal_draft = _build_signal_draft(
                signal_name=signal_name,
                signal_value=current_signal_value,
                warnings=warnings,
                path_prefix=f"optimization.parameter_ranges.{section_name}.{signal_name}",
            )
            if signal_draft:
                draft_section[signal_name] = signal_draft
        if draft_section:
            parameter_ranges[section_name] = draft_section

    optimization = {
        "description": "Generated from the current strategy configuration.",
        "parameter_ranges": parameter_ranges,
    }
    analysis = analyze_strategy_optimization(strategy_config, optimization)
    analysis.warnings = _dedupe_issues([*analysis.warnings, *warnings])
    analysis.yaml_content = dump_optimization_yaml(optimization)
    analysis.optimization = optimization
    return analysis


def _build_section_signal_params(strategy_config: dict[str, Any]) -> dict[str, SignalParams]:
    return {
        "entry_filter_params": SignalParams(
            **(strategy_config.get("entry_filter_params") or {})
        ),
        "exit_trigger_params": SignalParams(
            **(strategy_config.get("exit_trigger_params") or {})
        ),
    }


def _has_enabled_signal(signal_value: Any) -> bool:
    if isinstance(signal_value, BaseModel):
        if hasattr(signal_value, "enabled") and bool(getattr(signal_value, "enabled")):
            return True
        for field_name in signal_value.__class__.model_fields:
            nested_value = getattr(signal_value, field_name)
            if _has_enabled_signal(nested_value):
                return True

    return False


def _build_signal_draft(
    *,
    signal_name: str,
    signal_value: Any,
    warnings: list[GridValidationIssue],
    path_prefix: str,
) -> dict[str, Any]:
    if not isinstance(signal_value, BaseModel):
        return {}
    if not _has_enabled_signal(signal_value):
        return {}

    draft = _build_model_draft(signal_value, path_prefix=path_prefix)
    if draft:
        return draft

    warnings.append(
        _issue(
            path_prefix,
            f"No numeric optimization candidates were generated for enabled signal '{signal_name}'.",
        )
    )
    return {}


def _build_model_draft(model_value: BaseModel, *, path_prefix: str) -> dict[str, Any]:
    draft: dict[str, Any] = {}
    for field_name in model_value.__class__.model_fields:
        current_value = getattr(model_value, field_name)
        field_path = f"{path_prefix}.{field_name}"
        if isinstance(current_value, BaseModel):
            if hasattr(current_value, "enabled") and not bool(getattr(current_value, "enabled")):
                continue
            nested = _build_model_draft(current_value, path_prefix=field_path)
            if nested:
                draft[field_name] = nested
            continue

        if field_name in _STRUCTURAL_PARAM_NAMES or isinstance(current_value, bool):
            continue
        if not isinstance(current_value, (int, float)):
            continue

        candidates = _generate_numeric_candidates(
            current_value,
            model_value.__class__,
            field_name,
        )
        if candidates:
            draft[field_name] = candidates

    return draft


@lru_cache(maxsize=256)
def _field_schema(model_class: type[BaseModel], field_name: str) -> dict[str, Any]:
    schema = model_class.model_json_schema()
    properties = schema.get("properties", {})
    property_schema = properties.get(field_name, {})
    return property_schema if isinstance(property_schema, dict) else {}


def _generate_numeric_candidates(
    value: int | float,
    model_class: type[BaseModel],
    field_name: str,
) -> list[int | float]:
    schema = _field_schema(model_class, field_name)
    if isinstance(value, bool):
        return []
    if isinstance(value, int):
        int_candidates = _generate_int_candidates(int(value), schema)
        return [*int_candidates]
    float_candidates = _generate_float_candidates(float(value), schema)
    return [*float_candidates]


def _generate_int_candidates(value: int, schema: dict[str, Any]) -> list[int]:
    delta = max(1, min(50, int(round(max(abs(value), 1) * 0.2))))
    raw = [value - delta, value, value + delta]
    candidates = [
        _clamp_int(candidate, schema)
        for candidate in raw
    ]
    return sorted(dict.fromkeys(candidates))


def _generate_float_candidates(value: float, schema: dict[str, Any]) -> list[float]:
    delta = max(0.05, abs(value) * 0.2)
    raw = [value - delta, value, value + delta]
    candidates = [
        round(_clamp_float(candidate, schema), 6)
        for candidate in raw
    ]
    return sorted(dict.fromkeys(candidates))


def _clamp_int(value: int, schema: dict[str, Any]) -> int:
    if "exclusiveMinimum" in schema:
        value = max(value, int(schema["exclusiveMinimum"]) + 1)
    elif "minimum" in schema:
        value = max(value, int(schema["minimum"]))

    if "exclusiveMaximum" in schema:
        value = min(value, int(schema["exclusiveMaximum"]) - 1)
    elif "maximum" in schema:
        value = min(value, int(schema["maximum"]))

    return value


def _clamp_float(value: float, schema: dict[str, Any]) -> float:
    if "exclusiveMinimum" in schema:
        value = max(value, float(schema["exclusiveMinimum"]) + _FLOAT_EPSILON)
    elif "minimum" in schema:
        value = max(value, float(schema["minimum"]))

    if "exclusiveMaximum" in schema:
        value = min(value, float(schema["exclusiveMaximum"]) - _FLOAT_EPSILON)
    elif "maximum" in schema:
        value = min(value, float(schema["maximum"]))

    return value


def _validate_parameter_ranges_against_strategy(
    *,
    strategy_config: dict[str, Any],
    parameter_ranges: dict[str, Any],
    drift: list[GridValidationIssue],
) -> list[GridValidationIssue]:
    errors: list[GridValidationIssue] = []
    sections = _build_section_signal_params(strategy_config)
    covered_paths: dict[str, set[str]] = {
        section_name: set() for section_name in _SECTION_NAMES
    }

    for section_name, signals in parameter_ranges.items():
        section_path = f"optimization.parameter_ranges.{section_name}"
        section_params = sections.get(section_name)
        if section_params is None:
            errors.append(
                _issue(
                    section_path,
                    "Section must be entry_filter_params or exit_trigger_params.",
                )
            )
            continue
        if not isinstance(signals, dict):
            continue

        for signal_name, signal_params in signals.items():
            signal_path = f"{section_path}.{signal_name}"
            if signal_name not in section_params.__class__.model_fields:
                errors.append(
                    _issue(
                        signal_path,
                        f"Signal '{signal_name}' does not exist in the current strategy model.",
                    )
                )
                continue

            current_signal = getattr(section_params, signal_name)
            if not _has_enabled_signal(current_signal):
                errors.append(
                    _issue(
                        signal_path,
                        f"Signal '{signal_name}' is disabled in the current strategy.",
                    )
                )
                continue

            if isinstance(signal_params, dict):
                _validate_signal_node(
                    signal_name=signal_name,
                    node=signal_params,
                    model_value=current_signal,
                    path_prefix=signal_path,
                    errors=errors,
                    drift=drift,
                    covered_paths=covered_paths[section_name],
                )

    return errors


def _validate_signal_node(
    *,
    signal_name: str,
    node: dict[str, Any],
    model_value: BaseModel,
    path_prefix: str,
    errors: list[GridValidationIssue],
    drift: list[GridValidationIssue],
    covered_paths: set[str],
) -> None:
    for key, value in node.items():
        current_path = f"{path_prefix}.{key}"
        field_info = model_value.__class__.model_fields.get(key)
        if field_info is None:
            errors.append(
                _issue(
                    current_path,
                    f"Parameter '{key}' does not exist on the current strategy signal.",
                )
            )
            continue

        current_value = getattr(model_value, key)
        if isinstance(value, dict):
            if not isinstance(current_value, BaseModel):
                errors.append(
                    _issue(
                        current_path,
                        "Parameter path must resolve to a nested object in the current strategy.",
                    )
                )
                continue
            if hasattr(current_value, "enabled") and not bool(getattr(current_value, "enabled")):
                errors.append(
                    _issue(
                        current_path,
                        "Nested signal is disabled in the current strategy.",
                    )
                )
                continue
            _validate_signal_node(
                signal_name=signal_name,
                node=value,
                model_value=current_value,
                path_prefix=current_path,
                errors=errors,
                drift=drift,
                covered_paths=covered_paths,
            )
            continue

        if isinstance(current_value, BaseModel):
            errors.append(
                _issue(
                    current_path,
                    "Parameter path must resolve to a concrete leaf value.",
                )
            )
            continue

        covered_paths.add(_coverage_token(signal_name, path_prefix, current_path))
        if isinstance(value, list) and not any(candidate == current_value for candidate in value):
            drift.append(
                _issue(
                    current_path,
                    f"Current strategy value {current_value!r} is missing from optimization candidates.",
                )
            )


def _coverage_token(signal_name: str, path_prefix: str, current_path: str) -> str:
    if signal_name != "fundamental":
        return signal_name
    relative = current_path.removeprefix(f"{path_prefix}.")
    top_level = relative.split(".", 1)[0]
    return f"fundamental.{top_level}"


def _collect_missing_range_drift(
    *,
    strategy_config: dict[str, Any],
    parameter_ranges: dict[str, Any],
) -> list[GridValidationIssue]:
    drift: list[GridValidationIssue] = []
    covered: dict[str, set[str]] = {section_name: set() for section_name in _SECTION_NAMES}

    for section_name, signals in parameter_ranges.items():
        if not isinstance(signals, dict):
            continue
        for signal_name, signal_params in signals.items():
            if signal_name != "fundamental":
                covered.setdefault(section_name, set()).add(signal_name)
                continue
            if not isinstance(signal_params, dict):
                continue
            for top_level in signal_params.keys():
                covered.setdefault(section_name, set()).add(f"fundamental.{top_level}")

    for section_name, section_params in _build_section_signal_params(strategy_config).items():
        section_covered = covered.setdefault(section_name, set())
        for signal_name in section_params.__class__.model_fields:
            current_signal = getattr(section_params, signal_name)
            if signal_name == "fundamental":
                for child_name in current_signal.__class__.model_fields:
                    child_value = getattr(current_signal, child_name)
                    if not isinstance(child_value, BaseModel):
                        continue
                    if hasattr(child_value, "enabled") and bool(getattr(child_value, "enabled")):
                        token = f"fundamental.{child_name}"
                        if token not in section_covered:
                            drift.append(
                                _issue(
                                    f"optimization.parameter_ranges.{section_name}.fundamental.{child_name}",
                                    f"Enabled signal '{section_name}.fundamental.{child_name}' has no optimization ranges.",
                                )
                            )
                continue

            if _has_enabled_signal(current_signal) and signal_name not in section_covered:
                drift.append(
                    _issue(
                        f"optimization.parameter_ranges.{section_name}.{signal_name}",
                        f"Enabled signal '{section_name}.{signal_name}' has no optimization ranges.",
                    )
                )

    return drift


def _dedupe_issues(issues: list[GridValidationIssue]) -> list[GridValidationIssue]:
    seen: set[tuple[str, str]] = set()
    deduped: list[GridValidationIssue] = []
    for issue in issues:
        key = (issue.path, issue.message)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped


def calculate_total_combinations(parameter_ranges: dict[str, Any]) -> int:
    return len(generate_combinations(parameter_ranges))
