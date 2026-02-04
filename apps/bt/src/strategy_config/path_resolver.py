"""
戦略パス解決（薄いラッパー）

コアロジックは src.paths.resolver に統合済み。
strategy_config固有の config_dir 引数をハンドリングする薄いラッパーを提供。
"""

from pathlib import Path

from loguru import logger

from src.paths import find_strategy_path as paths_find_strategy_path
from src.paths.constants import SEARCH_ORDER
from src.paths.resolver import (
    StrategyMetadata,
    get_categorized_strategies,
    get_strategy_metadata_list,
)

# Re-export for backward compatibility
__all__ = [
    "StrategyMetadata",
    "infer_strategy_path",
    "get_available_strategies",
    "get_strategy_metadata",
    "validate_path_within_strategies",
]


def infer_strategy_path(config_dir: Path, strategy_name: str) -> Path:
    """
    戦略名から設定ファイルパスを推測

    Args:
        config_dir: 設定ディレクトリ（後方互換性のため維持、実際には使用しない場合あり）
        strategy_name: 戦略名（カテゴリ付き or 戦略名のみ）

    Returns:
        Path: 推測された戦略設定ファイルパス

    Raises:
        FileNotFoundError: 設定ファイルが見つからない場合

    推測ルール:
        1. カテゴリ付き（"/"含む）の場合:
           - experimentalは外部ディレクトリ → プロジェクト内の順で検索
           - その他はプロジェクト内を検索
        2. 戦略名のみの場合:
           - experimental（外部）→ experimental（プロジェクト）→ production → reference → legacy の順で探索
    """
    # config_dirがデフォルト(config)の場合のみ外部ディレクトリを検索
    # テスト時はtmp_pathが渡されるため、外部ディレクトリは検索しない
    is_default_config = str(config_dir) == "config"

    if is_default_config:
        # 新しいパス解決モジュールを使用（外部ディレクトリも検索）
        found_path = paths_find_strategy_path(strategy_name)
        if found_path:
            return found_path

    # 見つからない場合は旧ロジックでフォールバック（プロジェクト内のみ検索）
    candidates: list[str] = []

    if "/" in strategy_name:
        # カテゴリ付きの場合
        strategy_path = config_dir / "strategies" / f"{strategy_name}.yaml"
        candidates.append(str(strategy_path))
        if strategy_path.exists():
            return strategy_path
        raise FileNotFoundError(
            f"Strategy config not found: {strategy_path}\n" f"Specified: {strategy_name}"
        )

    # 戦略名のみの場合は自動推測（プロジェクト内のみ）
    for category in SEARCH_ORDER:
        candidate = config_dir / "strategies" / category / f"{strategy_name}.yaml"
        candidates.append(str(candidate))
        if candidate.exists():
            logger.info(f"Auto-detected strategy: {category}/{strategy_name}")
            return candidate

    # 見つからない場合はエラー
    raise FileNotFoundError(
        f"Strategy config not found for '{strategy_name}'. Searched: {candidates}"
    )


def get_available_strategies(config_dir: Path) -> dict[str, list[str]]:
    """
    利用可能な戦略のリストをカテゴリ別に取得

    外部ディレクトリ（~/.local/share/trading25）とプロジェクト内の両方を検索

    Args:
        config_dir: 設定ディレクトリ（プロジェクト内）

    Returns:
        カテゴリ別戦略辞書 {category: [strategy_names]}
    """
    return get_categorized_strategies(
        project_strategies_dir=config_dir / "strategies",
    )


def get_strategy_metadata(config_dir: Path) -> list[StrategyMetadata]:
    """
    戦略ファイルのメタデータを取得

    外部ディレクトリ（~/.local/share/trading25）とプロジェクト内の両方を検索
    ただし、config_dirがデフォルト("config")以外の場合はconfig_dirのみを検索

    Args:
        config_dir: 設定ディレクトリ（プロジェクト内）

    Returns:
        StrategyMetadataのリスト（更新日時などを含む）
    """
    is_default_config = str(config_dir) == "config"
    return get_strategy_metadata_list(
        project_strategies_dir=config_dir / "strategies",
        include_external=is_default_config,
    )


def validate_path_within_strategies(
    strategy_path: Path, config_dir: Path
) -> None:
    """
    パスが許可されたディレクトリ内に収まっているかチェック

    プロジェクト内のstrategiesディレクトリと外部ディレクトリの両方を許可

    Args:
        strategy_path: 検証するパス
        config_dir: 設定ディレクトリ（プロジェクト内）

    Raises:
        ValueError: パスが許可されたディレクトリ外の場合
    """
    from src.paths import get_data_dir

    try:
        strategy_resolved = strategy_path.resolve()
    except Exception as e:
        logger.error(f"パス検証エラー: {e}")
        raise ValueError(f"不正なファイルパス: {strategy_path}") from e

    allowed_dirs = [
        (config_dir / "strategies").resolve(),
        (get_data_dir() / "strategies").resolve(),
    ]

    for allowed_dir in allowed_dirs:
        if str(strategy_resolved).startswith(str(allowed_dir)):
            return

    raise ValueError(
        f"設定ファイルパスが許可されたディレクトリ外です: {strategy_path}"
    )
