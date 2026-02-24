"""
パラメータ抽出

設定からパラメータを抽出するヘルパー関数
"""

from pathlib import Path
from typing import Any

from loguru import logger


def _get_dict_value(config: dict[str, Any], key: str) -> dict[str, Any]:
    """configから辞書型の値を安全に取得"""
    value = config.get(key, {})
    return value if isinstance(value, dict) else {}


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """辞書をディープマージ（overrideが優先）"""
    merged = base.copy()
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def get_execution_config(
    strategy_config: dict[str, Any], default_config: dict[str, Any]
) -> dict[str, Any]:
    """
    実行設定を取得

    Args:
        strategy_config: 戦略設定
        default_config: デフォルト設定

    Returns:
        実行設定辞書
    """
    execution_config = _get_dict_value(default_config, "execution").copy()
    execution_config.update(_get_dict_value(strategy_config, "execution"))
    return execution_config


def get_template_notebook_path(execution_config: dict[str, Any]) -> Path:
    """テンプレートNotebookのパスを取得"""
    template_path = execution_config.get(
        "template_notebook", "notebooks/templates/strategy_analysis_template.ipynb"
    )
    return Path(template_path)


def get_output_directory(execution_config: dict[str, Any]) -> Path:
    """出力ディレクトリのパスを取得"""
    output_dir = execution_config.get("output_directory")
    if output_dir:
        return Path(output_dir)
    from src.shared.paths import get_backtest_results_dir
    return get_backtest_results_dir()


def extract_entry_filter_params(config: dict[str, Any]) -> dict[str, Any]:
    """設定からエントリーフィルターパラメータを抽出"""
    return _get_dict_value(config, "entry_filter_params")


def extract_exit_trigger_params(config: dict[str, Any]) -> dict[str, Any]:
    """設定からエグジットトリガーパラメータを抽出"""
    return _get_dict_value(config, "exit_trigger_params")


def merge_shared_config(
    strategy_config: dict[str, Any], default_config: dict[str, Any]
) -> dict[str, Any]:
    """
    デフォルト設定と戦略固有の shared_config をマージ

    Args:
        strategy_config: 戦略設定（戦略YAMLから読み込んだ辞書）
        default_config: デフォルト設定

    Returns:
        マージされた shared_config 辞書

    Note:
        - デフォルト設定の shared_config をベースにする
        - 戦略設定に shared_config があれば、それでディープマージする
        - 戦略設定で指定しないパラメータはデフォルト値を維持
    """
    default_parameters = _get_dict_value(default_config, "parameters")
    default_shared = _get_dict_value(default_parameters, "shared_config")

    if not default_shared and "parameters" in default_config:
        logger.warning("デフォルト設定の shared_config が辞書ではありません")

    merged_config = default_shared.copy()
    strategy_shared = _get_dict_value(strategy_config, "shared_config")

    if strategy_shared:
        logger.info(f"戦略固有の shared_config をマージ: {list(strategy_shared.keys())}")
        merged_config = _deep_merge_dict(merged_config, strategy_shared)

    return merged_config
