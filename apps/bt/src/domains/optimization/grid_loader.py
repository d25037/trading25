"""
グリッドYAML読み込み・パラメータ組み合わせ生成

最適化エンジンで使用するグリッド設定の読み込みとパラメータ組み合わせ生成を提供します。
"""

import itertools
import os
from typing import Any

from ruamel.yaml import YAML


def find_grid_config_path(strategy_basename: str, grid_config_path: str | None = None) -> str:
    """
    グリッド設定ファイルのパスを解決

    Args:
        strategy_basename: 戦略ベース名（カテゴリなし）
        grid_config_path: 明示指定パス（Noneで自動探索）

    Returns:
        str: グリッド設定ファイルパス

    Raises:
        FileNotFoundError: ファイルが見つからない場合
    """
    if grid_config_path is None:
        from src.shared.paths import get_all_optimization_grid_dirs

        grid_filename = f"{strategy_basename}_grid.yaml"

        for search_dir in get_all_optimization_grid_dirs():
            candidate = search_dir / grid_filename
            if candidate.exists():
                grid_config_path = str(candidate)
                break

        if grid_config_path is None:
            grid_config_path = f"config/optimization/{strategy_basename}_grid.yaml"

    if not os.path.exists(grid_config_path):
        raise FileNotFoundError(
            f"Grid config not found: {grid_config_path}\n"
            f"Expected: config/optimization/{strategy_basename}_grid.yaml or "
            f"~/.local/share/trading25/optimization/{strategy_basename}_grid.yaml"
        )

    return grid_config_path


def load_grid_config(grid_config_path: str) -> dict[str, Any]:
    """
    グリッドYAML設定を読み込み

    Args:
        grid_config_path: グリッド設定ファイルパス

    Returns:
        dict: グリッド設定辞書
    """
    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    with open(grid_config_path) as f:
        return ruamel_yaml.load(f)


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
    with open("config/default.yaml") as f:
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

    return [
        dict(zip(param_names, combination))
        for combination in itertools.product(*param_values_list)
    ]
