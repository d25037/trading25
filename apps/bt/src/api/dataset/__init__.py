"""
Dataset API client package.

This package provides the DatasetAPIClient with modular mixins.
"""

from .base import DatasetAPIClient, DatasetAPIClientBase
from .batch_mixin import BatchDataMixin
from .helpers import (
    build_date_params,
    convert_dated_response,
    convert_index_response,
    convert_ohlcv_response,
)
from .index_mixin import IndexDataMixin
from .margin_mixin import MarginDataMixin
from .sector_mixin import SectorDataMixin
from .statements_mixin import StatementsDataMixin
from .stock_mixin import StockDataMixin

__all__ = [
    # Main client
    "DatasetAPIClient",
    "DatasetAPIClientBase",
    # Mixins
    "BatchDataMixin",
    "IndexDataMixin",
    "MarginDataMixin",
    "SectorDataMixin",
    "StatementsDataMixin",
    "StockDataMixin",
    # Helpers
    "build_date_params",
    "convert_dated_response",
    "convert_index_response",
    "convert_ohlcv_response",
]
