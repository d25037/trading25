"""
ファイルCRUD操作

戦略設定ファイルの読み書き・削除
"""

from pathlib import Path
from typing import Any, cast

from loguru import logger
from ruamel.yaml import YAML


def _create_yaml() -> YAML:
    """ruamel.yaml インスタンスを作成"""
    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    return ruamel_yaml


def load_yaml_file(file_path: Path) -> dict[str, Any]:
    """
    YAMLファイルを読み込む

    Args:
        file_path: ファイルパス

    Returns:
        読み込んだ設定辞書

    Raises:
        FileNotFoundError: ファイルが存在しない
    """
    with open(file_path, "r", encoding="utf-8") as f:
        config = _create_yaml().load(f)

    return cast(dict[str, Any], config) if isinstance(config, dict) else {}


def save_yaml_file(file_path: Path, config: dict[str, Any]) -> None:
    """
    YAMLファイルに保存

    Args:
        file_path: ファイルパス
        config: 保存する設定辞書

    Note:
        ruamel.yaml を使用してコメントとインデントを保持
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ruamel_yaml = _create_yaml()
    ruamel_yaml.default_flow_style = False
    ruamel_yaml.indent(mapping=2, sequence=2, offset=2)
    ruamel_yaml.allow_unicode = True

    with open(file_path, "w", encoding="utf-8") as f:
        ruamel_yaml.dump(config, f)


def delete_strategy_file(strategy_path: Path, category: str) -> bool:
    """
    戦略設定ファイルを削除

    Args:
        strategy_path: 戦略ファイルパス
        category: カテゴリ名

    Returns:
        bool: 削除成功時 True

    Raises:
        PermissionError: experimental 以外のカテゴリの削除試行
    """
    if category != "experimental":
        raise PermissionError(
            f"カテゴリ '{category}' は削除不可です。experimental のみ削除可能です。"
        )

    # 削除実行
    strategy_path.unlink()
    logger.info(f"戦略設定削除成功: {strategy_path}")
    return True


def duplicate_to_experimental(
    source_path: Path,
    target_path: Path,
    new_strategy_name: str,
) -> Path:
    """
    既存戦略を experimental にコピー

    Args:
        source_path: 複製元のパス
        target_path: 複製先のパス
        new_strategy_name: 新しい戦略名

    Returns:
        Path: 作成したファイルパス

    Raises:
        FileExistsError: 複製先が既に存在する
    """
    # 既に存在する場合はエラー
    if target_path.exists():
        raise FileExistsError(f"戦略 '{new_strategy_name}' は既に存在します")

    # 読み込み
    source_config = load_yaml_file(source_path)

    # 保存
    save_yaml_file(target_path, source_config)
    logger.info(f"戦略複製成功: {new_strategy_name}")

    return target_path
