"""
Configuration Loader

YAML設定ファイルの読み込みと管理を行うモジュール
"""

from pathlib import Path
from typing import Any

from loguru import logger

from src.lib.strategy_runtime.file_operations import (
    delete_strategy_file,
    load_yaml_file,
    save_yaml_file,
)
from src.lib.strategy_runtime.parameter_extractor import (
    extract_entry_filter_params,
    extract_exit_trigger_params,
    get_execution_config,
    get_output_directory,
    get_template_notebook_path,
    merge_shared_config,
)
from src.lib.strategy_runtime.path_resolver import (
    StrategyMetadata,
    get_available_strategies,
    get_strategy_metadata,
    infer_strategy_path,
    validate_path_within_strategies,
)
from src.lib.strategy_runtime.validator import (
    is_editable_category,
    validate_strategy_config,
    validate_strategy_name,
)


class ConfigLoader:
    """
    YAML設定ファイルローダー

    戦略設定やデフォルト設定を読み込み、パラメータをマージする
    """

    def __init__(self, config_dir: str = "config"):
        """
        設定ローダーの初期化

        Args:
            config_dir: 設定ディレクトリのパス
        """
        self.config_dir = Path(config_dir)
        self.default_config = self._load_default_config()

    def _load_default_config(self) -> dict[str, Any]:
        """デフォルト設定を読み込む"""
        default_path = self.config_dir / "default.yaml"
        try:
            if default_path.exists():
                config = load_yaml_file(default_path)
                logger.info(f"デフォルト設定読み込み成功: {default_path}")
                return config.get("default", {})
            logger.warning(
                f"デフォルト設定ファイルが見つかりません: {default_path}"
            )
            return {}
        except Exception as e:
            logger.error(f"デフォルト設定読み込みエラー: {e}")
            return {}

    def reload_default_config(self) -> None:
        """デフォルト設定をファイルからリロードしてメモリを更新する"""
        self.default_config = self._load_default_config()

    def _validate_strategy_name(self, strategy_name: str) -> None:
        """戦略名の安全性を検証（パストラバーサル攻撃対策）"""
        validate_strategy_name(strategy_name)

    def _infer_strategy_path(self, strategy_name: str) -> Path:
        """戦略名から設定ファイルパスを推測"""
        return infer_strategy_path(self.config_dir, strategy_name)

    def load_strategy_config(self, strategy_name: str) -> dict[str, Any]:
        """
        戦略設定を読み込む

        Args:
            strategy_name: 戦略名（カテゴリ付き or 戦略名のみ）
                例: "experimental/ema_break" or "ema_break"

        Returns:
            戦略設定辞書

        Note:
            戦略名のみの場合は自動推測（experimental → production → reference → legacy）
        """
        self._validate_strategy_name(strategy_name)

        strategy_path = self._infer_strategy_path(strategy_name)
        validate_path_within_strategies(strategy_path, self.config_dir)

        try:
            config = load_yaml_file(strategy_path)
            logger.info(f"戦略設定読み込み成功: {strategy_name}")
            return config
        except FileNotFoundError:
            logger.error(f"戦略設定ファイルが見つかりません: {strategy_name}")
            raise
        except Exception as e:
            logger.error(f"戦略設定読み込みエラー: {e}")
            raise

    def get_execution_config(self, strategy_config: dict[str, Any]) -> dict[str, Any]:
        """実行設定を取得"""
        return get_execution_config(strategy_config, self.default_config)

    def get_available_strategies(self) -> dict[str, list[str]]:
        """利用可能な戦略のリストをカテゴリ別に取得"""
        return get_available_strategies(self.config_dir)

    def get_strategy_metadata(self) -> list[StrategyMetadata]:
        """戦略ファイルのメタデータを取得"""
        return get_strategy_metadata(self.config_dir)

    def validate_strategy_config(self, config: dict[str, Any]) -> bool:
        """戦略設定の妥当性をチェック"""
        return validate_strategy_config(config)

    def get_template_notebook_path(self, strategy_config: dict[str, Any]) -> Path:
        """テンプレートNotebookのパスを取得"""
        execution_config = self.get_execution_config(strategy_config)
        return get_template_notebook_path(execution_config)

    def get_output_directory(self, strategy_config: dict[str, Any]) -> Path:
        """出力ディレクトリのパスを取得"""
        execution_config = self.get_execution_config(strategy_config)
        return get_output_directory(execution_config)

    def extract_entry_filter_params(self, config: dict[str, Any]) -> dict[str, Any]:
        """設定からエントリーフィルターパラメータを抽出"""
        return extract_entry_filter_params(config)

    def extract_exit_trigger_params(self, config: dict[str, Any]) -> dict[str, Any]:
        """設定からエグジットトリガーパラメータを抽出"""
        return extract_exit_trigger_params(config)

    def merge_shared_config(self, strategy_config: dict[str, Any]) -> dict[str, Any]:
        """デフォルト設定と戦略固有の shared_config をマージ"""
        return merge_shared_config(strategy_config, self.default_config)

    def is_editable_category(self, strategy_name: str) -> bool:
        """
        戦略が編集可能なカテゴリかチェック

        Args:
            strategy_name: 戦略名（カテゴリ付き or 戦略名のみ）

        Returns:
            bool: experimental カテゴリの場合のみ True
        """
        if "/" in strategy_name:
            category = strategy_name.split("/")[0]
        else:
            try:
                path = self._infer_strategy_path(strategy_name)
                category = path.parent.name
            except FileNotFoundError:
                return True

        return is_editable_category(category)

    def save_strategy_config(
        self, strategy_name: str, config: dict[str, Any], force: bool = False
    ) -> Path:
        """
        戦略設定を保存（experimental カテゴリのみ）

        experimentalカテゴリは外部ディレクトリ（~/.local/share/trading25）に保存
        ただし、config_dirがデフォルト("config")以外の場合はconfig_dir内に保存

        Args:
            strategy_name: 戦略名（カテゴリなしの場合は experimental に保存）
            config: 保存する設定辞書
            force: 強制上書き（既存ファイル上書き時の確認スキップ）

        Returns:
            Path: 保存したファイルパス

        Raises:
            PermissionError: experimental 以外のカテゴリへの保存試行
            ValueError: 不正な戦略名
        """
        from src.paths import get_strategies_dir

        self._validate_strategy_name(strategy_name)

        if "/" not in strategy_name:
            strategy_name = f"experimental/{strategy_name}"

        category = strategy_name.split("/")[0]
        if not is_editable_category(category):
            raise PermissionError(
                f"カテゴリ '{category}' は編集不可です。experimental のみ保存可能です。"
            )

        is_default_config = str(self.config_dir) == "config"

        name_only = "/".join(strategy_name.split("/")[1:])
        if category == "experimental" and is_default_config:
            strategy_path = get_strategies_dir("experimental") / f"{name_only}.yaml"
        else:
            strategy_path = self.config_dir / "strategies" / f"{strategy_name}.yaml"

        validate_path_within_strategies(strategy_path, self.config_dir)
        strategy_path.parent.mkdir(parents=True, exist_ok=True)

        if strategy_path.exists() and not force:
            logger.warning(f"既存ファイルを上書きします: {strategy_path}")

        save_yaml_file(strategy_path, config)
        logger.info(f"戦略設定保存成功: {strategy_name}")
        return strategy_path

    def duplicate_strategy(self, source_strategy: str, new_strategy_name: str) -> Path:
        """
        既存戦略を複製して新しい戦略を作成

        複製先は外部ディレクトリ（~/.local/share/trading25）のexperimentalに保存
        ただし、config_dirがデフォルト("config")以外の場合はconfig_dir内に保存

        Args:
            source_strategy: 複製元の戦略名
            new_strategy_name: 新しい戦略名（カテゴリなし、experimental に保存）

        Returns:
            Path: 作成したファイルパス

        Raises:
            FileNotFoundError: 複製元が存在しない
            FileExistsError: 複製先が既に存在する
        """
        from src.paths import find_strategy_path, get_strategies_dir

        source_config = self.load_strategy_config(source_strategy)
        self._validate_strategy_name(new_strategy_name)

        if "/" in new_strategy_name:
            raise ValueError("複製先の戦略名にカテゴリを含めないでください")

        is_default_config = str(self.config_dir) == "config"

        if is_default_config:
            target_path = (
                get_strategies_dir("experimental") / f"{new_strategy_name}.yaml"
            )
            existing = find_strategy_path(f"experimental/{new_strategy_name}")
            if existing is not None or target_path.exists():
                raise FileExistsError(f"戦略 '{new_strategy_name}' は既に存在します")
        else:
            target_path = (
                self.config_dir
                / "strategies"
                / "experimental"
                / f"{new_strategy_name}.yaml"
            )
            if target_path.exists():
                raise FileExistsError(f"戦略 '{new_strategy_name}' は既に存在します")

        return self.save_strategy_config(new_strategy_name, source_config, force=True)

    def delete_strategy(self, strategy_name: str) -> bool:
        """
        戦略設定を削除（experimental カテゴリのみ）

        Args:
            strategy_name: 戦略名

        Returns:
            bool: 削除成功時 True

        Raises:
            PermissionError: experimental 以外のカテゴリの削除試行
            FileNotFoundError: 戦略が存在しない
        """
        self._validate_strategy_name(strategy_name)
        strategy_path = self._infer_strategy_path(strategy_name)
        category = strategy_path.parent.name
        return delete_strategy_file(strategy_path, category)

    def rename_strategy(self, strategy_name: str, new_name: str) -> Path:
        """
        戦略をリネーム（experimental カテゴリのみ）

        Args:
            strategy_name: 現在の戦略名
            new_name: 新しい戦略名（カテゴリなし）

        Returns:
            Path: 新しいファイルパス

        Raises:
            PermissionError: experimental 以外のカテゴリのリネーム試行
            FileNotFoundError: 戦略が存在しない
            FileExistsError: 新しい名前の戦略が既に存在する
            ValueError: 不正な戦略名
        """
        from src.paths import find_strategy_path, get_strategies_dir

        self._validate_strategy_name(strategy_name)
        self._validate_strategy_name(new_name)

        if "/" in new_name:
            raise ValueError("新しい戦略名にカテゴリを含めないでください")

        current_path = self._infer_strategy_path(strategy_name)

        category = current_path.parent.name
        if not is_editable_category(category):
            raise PermissionError(
                f"カテゴリ '{category}' は編集不可です。experimental のみリネーム可能です。"
            )

        is_default_config = str(self.config_dir) == "config"

        if is_default_config:
            new_path = get_strategies_dir("experimental") / f"{new_name}.yaml"
            existing = find_strategy_path(f"experimental/{new_name}")
            if existing is not None or new_path.exists():
                raise FileExistsError(f"戦略 '{new_name}' は既に存在します")
        else:
            new_path = (
                self.config_dir / "strategies" / "experimental" / f"{new_name}.yaml"
            )
            if new_path.exists():
                raise FileExistsError(f"戦略 '{new_name}' は既に存在します")

        if current_path.resolve() == new_path.resolve():
            return new_path

        try:
            current_path.rename(new_path)
            logger.info(f"戦略リネーム成功: {strategy_name} -> experimental/{new_name}")
            return new_path
        except OSError as e:
            logger.error(f"戦略リネームエラー: {e}")
            raise


__all__ = ["ConfigLoader", "StrategyMetadata"]
