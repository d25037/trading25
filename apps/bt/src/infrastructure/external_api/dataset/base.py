"""Base class for DatasetAPIClient with all mixins."""

from __future__ import annotations

from typing import Any, Protocol

from src.infrastructure.external_api.client import DEFAULT_BASE_URL, DEFAULT_TIMEOUT, BaseAPIClient

from .batch_mixin import BatchDataMixin
from .index_mixin import IndexDataMixin
from .margin_mixin import MarginDataMixin
from .sector_mixin import SectorDataMixin
from .statements_mixin import StatementsDataMixin
from .stock_mixin import StockDataMixin


class DatasetClientProtocol(Protocol):
    """Protocol defining the interface mixins depend on."""

    dataset_name: str

    def _dataset_path(self, endpoint: str) -> str: ...

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        response_model: Any = None,
    ) -> Any: ...


class DatasetAPIClientBase(BaseAPIClient):
    """Base class providing core functionality for dataset operations.

    This class provides the core methods that mixins depend on.
    """

    def __init__(
        self,
        dataset_name: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        """Initialize the dataset API client.

        Args:
            dataset_name: Name of the dataset (e.g., "sampleA", "topix100-A")
            base_url: Base URL of the API server
            timeout: Request timeout in seconds
        """
        super().__init__(base_url, timeout)
        self.dataset_name = dataset_name

    def _dataset_path(self, endpoint: str) -> str:
        """Build dataset-specific API path."""
        return f"/api/dataset/{self.dataset_name}{endpoint}"


class DatasetAPIClient(
    StockDataMixin,
    IndexDataMixin,
    MarginDataMixin,
    StatementsDataMixin,
    SectorDataMixin,
    BatchDataMixin,
    DatasetAPIClientBase,
):
    """API client for dataset operations.

    This client provides access to backtest data through the localhost:3002 API.

    Endpoints used:
        - GET /api/dataset/{name}/stocks/{code}/ohlcv - Stock OHLCV data
        - GET /api/dataset/{name}/stocks - Stock list
        - GET /api/dataset/{name}/topix - TOPIX data
        - GET /api/dataset/{name}/indices/{code} - Index data
        - GET /api/dataset/{name}/indices - Index list
        - GET /api/dataset/{name}/margin/{code} - Margin data
        - GET /api/dataset/{name}/margin - Margin list
        - GET /api/dataset/{name}/statements/{code} - Statements data
        - GET /api/dataset/{name}/sectors/mapping - Sector mapping
        - GET /api/dataset/{name}/stocks/ohlcv/batch - Batch OHLCV data
        - GET /api/dataset/{name}/margin/batch - Batch margin data
        - GET /api/dataset/{name}/statements/batch - Batch statements data

    Usage:
        ```python
        with DatasetAPIClient("sampleA") as client:
            df = client.get_stock_ohlcv("7203", "2024-01-01", "2024-12-31")
        ```
    """

    pass
