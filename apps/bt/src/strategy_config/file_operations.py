"""Compatibility facade for Phase 4C."""

from pathlib import Path
from typing import Any

from loguru import logger
from ruamel.yaml import YAML

from src.lib.strategy_runtime import file_operations as runtime_file_operations

__all__ = [
    "YAML",
    "delete_strategy_file",
    "duplicate_to_experimental",
    "load_yaml_file",
    "save_yaml_file",
]


def load_yaml_file(file_path: Path) -> dict[str, Any]:
    """
    Compatibility wrapper.

    Keep the `YAML` symbol patchable via `src.strategy_config.file_operations.YAML`.
    """
    runtime_file_operations.YAML = YAML
    return runtime_file_operations.load_yaml_file(file_path)


def save_yaml_file(file_path: Path, config: dict[str, Any]) -> None:
    """Compatibility wrapper with patchable YAML symbol."""
    runtime_file_operations.YAML = YAML
    runtime_file_operations.save_yaml_file(file_path, config)


def delete_strategy_file(strategy_path: Path, category: str) -> bool:
    """Compatibility wrapper for runtime implementation."""
    return runtime_file_operations.delete_strategy_file(strategy_path, category)


def duplicate_to_experimental(
    source_path: Path,
    target_path: Path,
    new_strategy_name: str,
) -> Path:
    """Compatibility wrapper using local load/save wrappers."""
    if target_path.exists():
        raise FileExistsError(f"戦略 '{new_strategy_name}' は既に存在します")

    source_config = load_yaml_file(source_path)
    save_yaml_file(target_path, source_config)
    logger.info(f"戦略複製成功: {new_strategy_name}")
    return target_path
