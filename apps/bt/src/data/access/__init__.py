"""Data access helpers for loader/backtest paths."""

from src.data.access.clients import get_dataset_client, get_market_client
from src.data.access.mode import (
    DATA_ACCESS_MODE_ENV,
    data_access_mode_context,
    get_data_access_mode,
    normalize_data_access_mode,
    should_use_direct_db,
)

__all__ = [
    "DATA_ACCESS_MODE_ENV",
    "get_dataset_client",
    "get_market_client",
    "get_data_access_mode",
    "normalize_data_access_mode",
    "should_use_direct_db",
    "data_access_mode_context",
]
