"""Index data mixin for DatasetAPIClient."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .helpers import build_date_params, convert_index_response

if TYPE_CHECKING:
    from .base import DatasetClientProtocol


class IndexDataMixin:
    """Mixin providing index and TOPIX data methods."""

    def get_topix(
        self: "DatasetClientProtocol",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Get TOPIX data.

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            DataFrame with columns [Open, High, Low, Close]
            and DatetimeIndex
        """
        params = build_date_params(start_date, end_date)
        data = self._get(
            self._dataset_path("/topix"),
            params=params or None,
        )
        return convert_index_response(data)

    def get_index(
        self: "DatasetClientProtocol",
        index_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Get data for a specific index.

        Args:
            index_code: Index code
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            DataFrame with columns [Open, High, Low, Close]
            and DatetimeIndex
        """
        params = build_date_params(start_date, end_date)
        data = self._get(
            self._dataset_path(f"/indices/{index_code}"),
            params=params or None,
        )
        return convert_index_response(data)

    def get_index_list(
        self: "DatasetClientProtocol",
        min_records: int = 100,
        codes: list[str] | None = None,
    ) -> pd.DataFrame:
        """Get list of available indices.

        Args:
            min_records: Minimum number of records required
            codes: Optional list of specific index codes to query

        Returns:
            DataFrame with index information
        """
        params: dict[str, str | int] = {"min_records": min_records}
        if codes:
            params["codes"] = ",".join(codes)

        data = self._get(self._dataset_path("/indices"), params=params)
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_multiple_indices(
        self: "DatasetClientProtocol",
        codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Get data for multiple indices at once.

        Args:
            codes: List of index codes
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            Dictionary mapping index_code to DataFrame
        """
        result: dict[str, pd.DataFrame] = {}
        for code in codes:
            df = self.get_index(code, start_date, end_date)
            if not df.empty:
                result[code] = df
        return result
