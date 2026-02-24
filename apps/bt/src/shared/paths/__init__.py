"""
統一パス解決モジュール

XDG Base Directory Specificationに準拠した
バックテスト関連ファイルのパス管理
"""

from .constants import (
    ENV_DATA_DIR,
    ENV_STRATEGIES_DIR,
    ENV_BACKTEST_DIR,
    DEFAULT_DATA_DIR,
    STRATEGY_CATEGORIES,
    EXTERNAL_CATEGORIES,
    PROJECT_CATEGORIES,
)
from .resolver import (
    StrategyMetadata,
    get_data_dir,
    get_strategies_dir,
    get_backtest_results_dir,
    get_backtest_attribution_dir,
    get_optimization_results_dir,
    get_optimization_grid_dir,
    get_cache_dir,
    find_strategy_path,
    get_all_strategy_paths,
    get_all_backtest_result_dirs,
    get_all_optimization_result_dirs,
    get_all_optimization_grid_dirs,
    ensure_data_dirs,
    get_categorized_strategies,
    get_strategy_metadata_list,
)

__all__ = [
    # Constants
    "ENV_DATA_DIR",
    "ENV_STRATEGIES_DIR",
    "ENV_BACKTEST_DIR",
    "DEFAULT_DATA_DIR",
    "STRATEGY_CATEGORIES",
    "EXTERNAL_CATEGORIES",
    "PROJECT_CATEGORIES",
    # Resolver functions
    "get_data_dir",
    "get_strategies_dir",
    "get_backtest_results_dir",
    "get_backtest_attribution_dir",
    "get_optimization_results_dir",
    "get_optimization_grid_dir",
    "get_cache_dir",
    "find_strategy_path",
    "get_all_strategy_paths",
    "get_all_backtest_result_dirs",
    "get_all_optimization_result_dirs",
    "get_all_optimization_grid_dirs",
    "ensure_data_dirs",
    # Strategy metadata & categorized strategies
    "StrategyMetadata",
    "get_categorized_strategies",
    "get_strategy_metadata_list",
]
