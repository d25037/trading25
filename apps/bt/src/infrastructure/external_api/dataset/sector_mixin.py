"""Sector mapping mixin for DatasetAPIClient."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

import pandas as pd

if TYPE_CHECKING:
    from .base import DatasetClientProtocol


class SectorDataMixin:
    """Mixin providing sector mapping methods."""

    def get_sector_mapping(self: "DatasetClientProtocol") -> pd.DataFrame:
        """Get sector to index mapping.

        Returns:
            DataFrame with sector_code, sector_name, index_code, index_name
        """
        data = self._get(self._dataset_path("/sectors/mapping"))
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_stock_sector_mapping(self: "DatasetClientProtocol") -> dict[str, str]:
        """Get stock code to sector name mapping for all stocks.

        Returns:
            dict mapping stock code to sector33Name
        """
        data = self._get(self._dataset_path("/sectors/stock-mapping"))
        if not data:
            return {}
        return {item["code"]: item["sector33Name"] for item in data}

    def get_sector_stocks(self: "DatasetClientProtocol", sector_name: str) -> list[str]:
        """Get stock codes belonging to a specific sector.

        Args:
            sector_name: Sector name (e.g., "電気機器")

        Returns:
            List of stock codes
        """
        encoded_name = quote(sector_name, safe="")
        data = self._get(self._dataset_path(f"/sectors/{encoded_name}/stocks"))
        return list(data) if data else []

    def get_all_sectors(self: "DatasetClientProtocol") -> pd.DataFrame:
        """Get all sectors with stock count.

        Returns:
            DataFrame with sector_code, sector_name, index_code, index_name, stock_count
        """
        data = self._get(self._dataset_path("/sectors"))
        return pd.DataFrame(data) if data else pd.DataFrame()
