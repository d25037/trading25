"""API Client package for trading25-bt.

This package provides HTTP clients for accessing the localhost:3001 API
instead of direct SQL queries.

Clients:
    - DatasetAPIClient: For dataset operations (backtest data)
    - MarketAPIClient: For market.db operations (market analysis)
    - PortfolioAPIClient: For portfolio operations (existing API)
"""

from src.api.client import BaseAPIClient
from src.api.dataset_client import DatasetAPIClient
from src.api.exceptions import APIConnectionError, APIError, APINotFoundError, APITimeoutError
from src.api.market_client import MarketAPIClient
from src.api.portfolio_client import PortfolioAPIClient

__all__ = [
    "BaseAPIClient",
    "DatasetAPIClient",
    "MarketAPIClient",
    "PortfolioAPIClient",
    "APIError",
    "APIConnectionError",
    "APITimeoutError",
    "APINotFoundError",
]
