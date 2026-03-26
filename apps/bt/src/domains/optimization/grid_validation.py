"""
Optimization grid YAML validation helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from operator import mul
from collections.abc import Mapping
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError


@dataclass(frozen=True)
class GridValidationIssue:
    path: str
    message: str


@dataclass
class GridValidationResult:
    valid: bool
    ready_to_run: bool
    param_count: int
    combinations: int
    errors: list[GridValidationIssue] = field(default_factory=list)
    warnings: list[GridValidationIssue] = field(default_factory=list)


def _issue(path: str, message: str) -> GridValidationIssue:
    return GridValidationIssue(path=path, message=message)


def _join_path(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


def _validate_signal_params(
    node: Mapping[str, Any],
    path: str,
    *,
    errors: list[GridValidationIssue],
    warnings: list[GridValidationIssue],
    leaf_counts: list[int],
) -> None:
    for raw_key, value in node.items():
        key = str(raw_key)
        current_path = _join_path(path, key)
        if value is None:
            warnings.append(
                _issue(
                    current_path,
                    "Parameter is null and will be ignored. Use a candidate list such as [10, 20, 30].",
                )
            )
            continue
        if isinstance(value, Mapping):
            _validate_signal_params(
                value,
                current_path,
                errors=errors,
                warnings=warnings,
                leaf_counts=leaf_counts,
            )
            continue
        if isinstance(value, list):
            leaf_counts.append(len(value))
            if len(value) == 0:
                warnings.append(
                    _issue(
                        current_path,
                        "Candidate list is empty. Optimization cannot run until at least one value is provided.",
                    )
                )
            continue
        errors.append(
            _issue(
                current_path,
                "Parameter must be a candidate list such as [10, 20, 30], not a scalar value.",
            )
        )


def validate_parameter_ranges(parameter_ranges: Any) -> GridValidationResult:
    errors: list[GridValidationIssue] = []
    warnings: list[GridValidationIssue] = []
    leaf_counts: list[int] = []

    if parameter_ranges is None:
        warnings.append(
            _issue(
                "parameter_ranges",
                'Missing "parameter_ranges" key. Add sections such as entry_filter_params / exit_trigger_params.',
            )
        )
        return GridValidationResult(
            valid=True,
            ready_to_run=False,
            param_count=0,
            combinations=0,
            warnings=warnings,
        )

    if not isinstance(parameter_ranges, Mapping):
        errors.append(
            _issue(
                "parameter_ranges",
                '"parameter_ranges" must be a mapping of sections to signal definitions.',
            )
        )
        return GridValidationResult(
            valid=False,
            ready_to_run=False,
            param_count=0,
            combinations=0,
            errors=errors,
            warnings=warnings,
        )

    for raw_section_name, signals in parameter_ranges.items():
        section_name = str(raw_section_name)
        section_path = _join_path("parameter_ranges", section_name)
        if signals is None:
            continue
        if not isinstance(signals, Mapping):
            errors.append(
                _issue(
                    section_path,
                    "Section must be a mapping of signal names to parameter maps.",
                )
            )
            continue

        for raw_signal_name, params in signals.items():
            signal_name = str(raw_signal_name)
            signal_path = _join_path(section_path, signal_name)
            if params is None:
                continue
            if not isinstance(params, Mapping):
                errors.append(
                    _issue(
                        signal_path,
                        "Signal must be a mapping of parameter names to candidate lists.",
                    )
                )
                continue
            _validate_signal_params(
                params,
                signal_path,
                errors=errors,
                warnings=warnings,
                leaf_counts=leaf_counts,
            )

    param_count = len(leaf_counts)
    combinations = reduce(mul, leaf_counts, 1) if leaf_counts else 0

    if param_count == 0:
        warnings.append(
            _issue(
                "parameter_ranges",
                'No parameter arrays found under "parameter_ranges". Add list values such as period: [10, 20, 30].',
            )
        )

    return GridValidationResult(
        valid=not errors,
        ready_to_run=(not errors and param_count > 0 and combinations > 0),
        param_count=param_count,
        combinations=combinations,
        errors=errors,
        warnings=warnings,
    )


def validate_grid_document(document: Any) -> GridValidationResult:
    if document is None:
        return validate_parameter_ranges(None)

    if not isinstance(document, Mapping):
        return GridValidationResult(
            valid=False,
            ready_to_run=False,
            param_count=0,
            combinations=0,
            errors=[
                _issue(
                    "$",
                    "Grid YAML root must be a mapping.",
                )
            ],
        )

    parameter_ranges = document.get("parameter_ranges")
    return validate_parameter_ranges(parameter_ranges)


def validate_grid_yaml_content(content: str) -> GridValidationResult:
    yaml = YAML()
    try:
        document = yaml.load(content)
    except YAMLError as exc:
        return GridValidationResult(
            valid=False,
            ready_to_run=False,
            param_count=0,
            combinations=0,
            errors=[_issue("$", f"YAML parse error: {exc}")],
        )
    return validate_grid_document(document)


def format_grid_validation_issues(issues: list[GridValidationIssue], *, limit: int = 3) -> str:
    if not issues:
        return "unknown grid validation error"
    rendered = [f"{issue.path}: {issue.message}" for issue in issues[:limit]]
    if len(issues) > limit:
        rendered.append(f"... and {len(issues) - limit} more")
    return "; ".join(rendered)
