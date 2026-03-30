"""
optimization parameter range utilities

戦略内 optimization.parameter_ranges の組み合わせ生成を提供します。
"""

import itertools
from typing import Any

from ruamel.yaml import YAML

from src.shared.paths import get_default_config_path

from .grid_validation import (
    format_grid_validation_issues,
    validate_parameter_ranges,
)


def load_default_config() -> dict:
    """
    default.yamlから設定読み込み

    Returns:
        dict: {
            "parameter_optimization": {...},
            "shared_config": {...}
        }
    """
    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    with get_default_config_path().open(encoding="utf-8") as f:
        config = ruamel_yaml.load(f)

    return {
        "parameter_optimization": config["default"]["parameters"]["shared_config"][
            "parameter_optimization"
        ],
        "shared_config": config["default"]["parameters"]["shared_config"],
    }


def flatten_params(
    obj: dict[str, Any], prefix: str = ""
) -> list[tuple[str, list[Any]]]:
    """
    再帰的にネストされたパラメータを平坦化

    4階層以上のネスト（例: entry_filter_params.fundamental.per.threshold）に対応

    Args:
        obj: 対象の辞書
        prefix: キーのプレフィックス

    Returns:
        list[tuple[str, list[Any]]]: (フルキー, 値リスト) のタプルリスト

    Example:
        >>> params = {"per": {"threshold": [10, 15, 20]}}
        >>> flatten_params(params, "entry_filter_params.fundamental")
        [("entry_filter_params.fundamental.per.threshold", [10, 15, 20])]
    """
    result: list[tuple[str, list[Any]]] = []
    for key, value in obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.extend(flatten_params(value, full_key))
        elif isinstance(value, list):
            result.append((full_key, value))
    return result


def generate_combinations(parameter_ranges: dict[str, Any]) -> list[dict[str, Any]]:
    """
    パラメータ組み合わせ生成（デカルト積）

    Args:
        parameter_ranges: グリッドYAMLのparameter_ranges

    Returns:
        list[dict[str, Any]]: パラメータ組み合わせリスト
    """
    validation = validate_parameter_ranges(parameter_ranges)
    if validation.errors:
        raise ValueError(format_grid_validation_issues(validation.errors))
    if not validation.ready_to_run:
        return []

    param_names = []
    param_values_list = []

    for section, signals in parameter_ranges.items():
        if signals is None:
            continue
        for signal_name, params in signals.items():
            if params is None:
                continue
            prefix = f"{section}.{signal_name}"
            flattened = flatten_params(params, prefix)
            for full_key, values in flattened:
                param_names.append(full_key)
                param_values_list.append(values)

    if not param_values_list:
        return []

    return [dict(zip(param_names, combination)) for combination in itertools.product(*param_values_list)]
