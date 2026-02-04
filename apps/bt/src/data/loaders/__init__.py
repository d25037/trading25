"""
データローダーモジュール

SQLiteデータベースから株価データを読み込み、VectorBTで使用できる形式に変換します。

このモジュールは機能別に分割されており、以下のサブモジュールで構成されています：
- stock_loaders: 株価データローダー
- index_loaders: インデックス・ベンチマークデータローダー
- margin_loaders: 信用残高データローダー
- statements_loaders: 財務諸表データローダー
- multi_asset_loaders: マルチアセット・一括データローダー
- sector_loaders: セクター・業種別データローダー
- data_preparation: データ準備・統合処理
"""

# Import logger for backwards compatibility with tests
from loguru import logger

# Cache for optimization
from .cache import DataCache

# Stock data loaders
from .stock_loaders import (
    get_stock_list,
    load_stock_data,
    get_available_stocks,
)

# Index data loaders
from .index_loaders import (
    load_topix_data,
    load_topix_data_from_market_db,
    load_index_data,
    get_index_list,
    get_available_indices,
)

# Margin data loaders
from .margin_loaders import (
    load_margin_data,
    get_margin_available_stocks,
)

# Statements data loaders
from .statements_loaders import (
    load_statements_data,
)

# Multi-asset loaders
from .multi_asset_loaders import (
    load_multiple_stocks,
    load_multiple_indices,
    load_multiple_margin_data,
    load_multiple_statements_data,
)

# Sector loaders
from .sector_loaders import (
    get_sector_mapping,
    prepare_sector_data,
    load_all_sector_indices,
    get_stock_sector_mapping,
)

# High-level data preparation functions
from .data_preparation import (
    prepare_data,
    prepare_all_stocks_data,
    prepare_multi_data,
)


__all__ = [
    # Cache
    "DataCache",
    # Stock loaders
    "get_stock_list",
    "load_stock_data",
    "get_available_stocks",
    # Index loaders
    "load_topix_data",
    "load_topix_data_from_market_db",
    "load_index_data",
    "get_index_list",
    "get_available_indices",
    # Margin loaders
    "load_margin_data",
    "get_margin_available_stocks",
    # Statements loaders
    "load_statements_data",
    # Multi-asset loaders
    "load_multiple_stocks",
    "load_multiple_indices",
    "load_multiple_margin_data",
    "load_multiple_statements_data",
    # Sector loaders
    "get_sector_mapping",
    "prepare_sector_data",
    "load_all_sector_indices",
    "get_stock_sector_mapping",
    # Data preparation
    "prepare_data",
    "prepare_all_stocks_data",
    "prepare_multi_data",
    # Logger for backwards compatibility
    "logger",
]
