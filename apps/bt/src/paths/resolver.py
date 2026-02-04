"""
パス解決ロジック

プロジェクト内パスと外部データディレクトリの統合的な解決
"""

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from loguru import logger

from .constants import (
    DEFAULT_DATA_DIR,
    ENV_BACKTEST_DIR,
    ENV_DATA_DIR,
    ENV_STRATEGIES_DIR,
    EXTERNAL_CATEGORIES,
    PROJECT_CATEGORIES,
    PROJECT_OPTIMIZATION_DIR,
    PROJECT_STRATEGIES_DIR,
    SEARCH_ORDER,
)


def get_data_dir() -> Path:
    """
    データディレクトリのベースパスを取得

    環境変数 TRADING25_DATA_DIR が設定されていればそれを使用、
    なければデフォルトの ~/.local/share/trading25 を使用

    Returns:
        Path: データディレクトリパス
    """
    env_value = os.environ.get(ENV_DATA_DIR)
    if env_value:
        return Path(env_value)
    return DEFAULT_DATA_DIR


def get_strategies_dir(category: str | None = None) -> Path:
    """
    戦略設定ディレクトリのパスを取得

    Args:
        category: 戦略カテゴリ（experimental/production/reference/legacy）
                  Noneの場合はベースディレクトリを返す

    Returns:
        Path: 戦略ディレクトリパス

    Note:
        - experimental: 外部ディレクトリ（~/.local/share/trading25/strategies/experimental）
        - production/reference/legacy: プロジェクト内（config/strategies/{category}）
    """
    # 環境変数による個別指定
    env_strategies = os.environ.get(ENV_STRATEGIES_DIR)
    if env_strategies:
        base = Path(env_strategies)
        return base / category if category else base

    if category is None:
        # ベースディレクトリを返す（プロジェクト内）
        return PROJECT_STRATEGIES_DIR

    if category in EXTERNAL_CATEGORIES:
        return get_data_dir() / "strategies" / category
    if category in PROJECT_CATEGORIES:
        return PROJECT_STRATEGIES_DIR / category

    # 未知のカテゴリはプロジェクト内に配置
    logger.warning(f"未知のカテゴリ: {category}、プロジェクト内に配置します")
    return PROJECT_STRATEGIES_DIR / category


def get_backtest_results_dir(strategy_name: str | None = None) -> Path:
    """
    バックテスト結果ディレクトリのパスを取得

    Args:
        strategy_name: 戦略名（Noneの場合はベースディレクトリを返す）

    Returns:
        Path: バックテスト結果ディレクトリパス

    Note:
        外部ディレクトリ: ~/.local/share/trading25/backtest/results/{strategy_name}
    """
    env_backtest = os.environ.get(ENV_BACKTEST_DIR)
    if env_backtest:
        base = Path(env_backtest) / "results"
    else:
        base = get_data_dir() / "backtest" / "results"

    return base / strategy_name if strategy_name else base


def get_optimization_results_dir(strategy_name: str | None = None) -> Path:
    """
    最適化結果ディレクトリのパスを取得

    Args:
        strategy_name: 戦略名（Noneの場合はベースディレクトリを返す）

    Returns:
        Path: 最適化結果ディレクトリパス

    Note:
        外部ディレクトリ: ~/.local/share/trading25/backtest/optimization/{strategy_name}
    """
    env_backtest = os.environ.get(ENV_BACKTEST_DIR)
    if env_backtest:
        base = Path(env_backtest) / "optimization"
    else:
        base = get_data_dir() / "backtest" / "optimization"

    return base / strategy_name if strategy_name else base


def get_optimization_grid_dir() -> Path:
    """
    最適化グリッド設定ディレクトリのパスを取得

    Returns:
        Path: 最適化グリッドディレクトリパス

    Note:
        外部ディレクトリ: ~/.local/share/trading25/optimization/
    """
    return get_data_dir() / "optimization"


def get_cache_dir() -> Path:
    """
    キャッシュディレクトリのパスを取得

    Returns:
        Path: キャッシュディレクトリパス

    Note:
        外部ディレクトリ: ~/.local/share/trading25/cache/
    """
    return get_data_dir() / "cache"


def _get_search_paths_for_strategy(category: str | None = None) -> list[Path]:
    """
    戦略ファイルの検索パスリストを取得

    Args:
        category: 特定カテゴリに限定（Noneで全カテゴリ）

    Returns:
        list[Path]: 検索パスリスト（優先度順）
    """
    search_paths: list[Path] = []

    if category:
        # 特定カテゴリのみ
        if category in EXTERNAL_CATEGORIES:
            # 外部ディレクトリ
            search_paths.append(get_strategies_dir(category))
            # フォールバック: プロジェクト内
            fallback = PROJECT_STRATEGIES_DIR / category
            if fallback not in search_paths:
                search_paths.append(fallback)
        else:
            # プロジェクト内
            search_paths.append(PROJECT_STRATEGIES_DIR / category)
    else:
        # 全カテゴリを検索順序に従って追加
        for cat in SEARCH_ORDER:
            if cat in EXTERNAL_CATEGORIES:
                # 外部ディレクトリを優先
                search_paths.append(get_strategies_dir(cat))
                # フォールバック: プロジェクト内も追加
                fallback = PROJECT_STRATEGIES_DIR / cat
                if fallback not in search_paths:
                    search_paths.append(fallback)
            else:
                search_paths.append(PROJECT_STRATEGIES_DIR / cat)

    return search_paths


def find_strategy_path(strategy_name: str) -> Path | None:
    """
    戦略名から設定ファイルパスを検索

    カテゴリ付き（"experimental/strategy_name"）の場合はそのまま使用、
    戦略名のみの場合は複数ディレクトリを検索

    Args:
        strategy_name: 戦略名（カテゴリ付き or 戦略名のみ）

    Returns:
        Path | None: 見つかったファイルパス、見つからない場合はNone

    Note:
        検索順序: experimental（外部）→ experimental（プロジェクト内）
                  → production → reference → legacy
    """
    if "/" in strategy_name:
        # カテゴリ付きの場合
        category, name = strategy_name.split("/", 1)
        search_paths = _get_search_paths_for_strategy(category)

        for search_path in search_paths:
            candidate = search_path / f"{name}.yaml"
            if candidate.exists():
                logger.debug(f"戦略ファイル発見: {candidate}")
                return candidate

            # サブディレクトリ内も検索
            for sub_path in search_path.rglob(f"{name}.yaml"):
                logger.debug(f"戦略ファイル発見（サブディレクトリ）: {sub_path}")
                return sub_path

        return None

    # 戦略名のみの場合は全カテゴリを検索
    search_paths = _get_search_paths_for_strategy()

    for search_path in search_paths:
        candidate = search_path / f"{strategy_name}.yaml"
        if candidate.exists():
            logger.info(f"戦略ファイル自動検出: {candidate}")
            return candidate

        # サブディレクトリ内も検索
        for sub_path in search_path.rglob(f"{strategy_name}.yaml"):
            logger.info(f"戦略ファイル自動検出（サブディレクトリ）: {sub_path}")
            return sub_path

    return None


def get_all_strategy_paths() -> dict[str, list[Path]]:
    """
    全戦略ファイルのパスをカテゴリ別に取得

    外部ディレクトリとプロジェクト内ディレクトリの両方を検索

    Returns:
        dict[str, list[Path]]: カテゴリ → ファイルパスリスト
    """
    result: dict[str, list[Path]] = {}

    for category in SEARCH_ORDER:
        paths: list[Path] = []

        # カテゴリの検索パスを取得
        search_paths = _get_search_paths_for_strategy(category)

        for search_path in search_paths:
            if not search_path.exists():
                continue

            for yaml_file in search_path.rglob("*.yaml"):
                if yaml_file not in paths:
                    paths.append(yaml_file)

        if paths:
            result[category] = sorted(paths, key=lambda p: p.name)

    return result


def get_all_backtest_result_dirs() -> list[Path]:
    """
    全バックテスト結果ディレクトリのパスを取得

    外部ディレクトリとプロジェクト内ディレクトリの両方を返す

    Returns:
        list[Path]: バックテスト結果ディレクトリパスリスト
    """
    dirs: list[Path] = []

    external_dir = get_backtest_results_dir()
    if external_dir.exists():
        dirs.append(external_dir)

    return dirs


def get_all_optimization_result_dirs() -> list[Path]:
    """
    全最適化結果ディレクトリのパスを取得

    Returns:
        list[Path]: 最適化結果ディレクトリパスリスト
    """
    dirs: list[Path] = []

    external_dir = get_optimization_results_dir()
    if external_dir.exists():
        dirs.append(external_dir)

    return dirs


def get_all_optimization_grid_dirs() -> list[Path]:
    """
    全最適化グリッド設定ディレクトリのパスを取得

    Returns:
        list[Path]: 最適化グリッドディレクトリパスリスト
    """
    dirs: list[Path] = []

    # 外部ディレクトリ
    external_dir = get_optimization_grid_dir()
    if external_dir.exists():
        dirs.append(external_dir)

    # プロジェクト内（フォールバック）
    project_dir = PROJECT_OPTIMIZATION_DIR
    if project_dir.exists() and project_dir not in dirs:
        dirs.append(project_dir)

    return dirs


def ensure_data_dirs() -> None:
    """
    データディレクトリ構造を作成

    ~/.local/share/trading25/ 配下に必要なディレクトリを作成
    """
    dirs_to_create = [
        get_data_dir(),
        get_strategies_dir("experimental"),
        get_strategies_dir("experimental") / "auto",
        get_strategies_dir("experimental") / "evolved",
        get_strategies_dir("experimental") / "optuna",
        get_backtest_results_dir(),
        get_optimization_results_dir(),
        get_optimization_grid_dir(),
        get_cache_dir(),
    ]

    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"ディレクトリ作成確認: {dir_path}")


# --- 戦略メタデータ・カテゴリ別取得 ---


@dataclass
class StrategyMetadata:
    """戦略ファイルのメタデータ"""

    name: str  # "category/strategy_name"
    category: str
    path: Path
    mtime: datetime  # 更新日時


def _resolve_strategy_name(yaml_file: Path, base_dir: Path, category: str) -> str:
    """YAMLファイルパスからカテゴリ付き戦略名を解決"""
    try:
        rel_path = yaml_file.relative_to(base_dir)
        if len(rel_path.parts) > 1:
            return f"{category}/{'/'.join(rel_path.parts[:-1])}/{rel_path.stem}"
        return f"{category}/{rel_path.stem}"
    except ValueError:
        return f"{category}/{yaml_file.stem}"


def _collect_strategy_names(search_dir: Path, category: str) -> list[str]:
    """ディレクトリ内のYAMLファイルから戦略名リストを構築"""
    if not search_dir.exists():
        return []
    strategies: list[str] = []
    for yaml_file in search_dir.rglob("*.yaml"):
        name = _resolve_strategy_name(yaml_file, search_dir, category)
        if name not in strategies:
            strategies.append(name)
    return strategies


def _merge_into(
    categorized: dict[str, list[str]], category: str, strategies: list[str]
) -> None:
    """戦略リストをカテゴリ辞書にマージ（重複除去・ソート）"""
    if not strategies:
        return
    if category in categorized:
        existing = set(categorized[category])
        for s in strategies:
            if s not in existing:
                categorized[category].append(s)
        categorized[category] = sorted(categorized[category])
    else:
        categorized[category] = sorted(strategies)


def get_categorized_strategies(
    project_strategies_dir: Path | None = None,
) -> dict[str, list[str]]:
    """
    利用可能な戦略のリストをカテゴリ別に取得

    外部ディレクトリとプロジェクト内ディレクトリの両方を検索

    Args:
        project_strategies_dir: プロジェクト内の戦略ディレクトリ
                                Noneの場合はデフォルト(config/strategies)を使用

    Returns:
        カテゴリ別戦略辞書 {category: [strategy_names]}
    """
    if project_strategies_dir is None:
        project_strategies_dir = PROJECT_STRATEGIES_DIR

    categorized: dict[str, list[str]] = {}

    # 外部ディレクトリ検索（experimental等）
    for category in EXTERNAL_CATEGORIES:
        ext_dir = get_strategies_dir(category)
        strategies = _collect_strategy_names(ext_dir, category)
        _merge_into(categorized, category, strategies)

    # プロジェクト内ディレクトリ検索
    if project_strategies_dir.exists():
        for category_dir in project_strategies_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                strategies = _collect_strategy_names(category_dir, category_name)
                _merge_into(categorized, category_name, strategies)

        # ルート直下の戦略（templateなど）
        root_strategies = [f.stem for f in project_strategies_dir.glob("*.yaml")]
        if root_strategies:
            categorized["root"] = sorted(root_strategies)

    return categorized


def get_strategy_metadata_list(
    project_strategies_dir: Path | None = None,
    include_external: bool = True,
) -> list[StrategyMetadata]:
    """
    戦略ファイルのメタデータを取得

    Args:
        project_strategies_dir: プロジェクト内の戦略ディレクトリ
        include_external: 外部ディレクトリも検索するか

    Returns:
        StrategyMetadataのリスト
    """
    if project_strategies_dir is None:
        project_strategies_dir = PROJECT_STRATEGIES_DIR

    result: list[StrategyMetadata] = []
    seen_paths: set[Path] = set()

    def _add_from_dir(search_dir: Path, category: str) -> None:
        if not search_dir.exists():
            return
        for yaml_file in search_dir.rglob("*.yaml"):
            if yaml_file in seen_paths:
                continue
            seen_paths.add(yaml_file)
            name = _resolve_strategy_name(yaml_file, search_dir, category)
            stat = yaml_file.stat()
            result.append(
                StrategyMetadata(
                    name=name,
                    category=category,
                    path=yaml_file,
                    mtime=datetime.fromtimestamp(stat.st_mtime),
                )
            )

    if include_external:
        for category in EXTERNAL_CATEGORIES:
            ext_dir = get_strategies_dir(category)
            _add_from_dir(ext_dir, category)

    # プロジェクト内
    if project_strategies_dir.exists():
        for category_dir in project_strategies_dir.iterdir():
            if category_dir.is_dir():
                _add_from_dir(category_dir, category_dir.name)

    return result
