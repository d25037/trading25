"""Stock data mixin for DatasetAPIClient."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pandas as pd

from .helpers import build_date_params, convert_ohlcv_response

if TYPE_CHECKING:
    from .base import DatasetClientProtocol


class StockDataMixin:
    """Mixin providing stock data methods."""

    def get_stock_ohlcv(
        self: "DatasetClientProtocol",
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> pd.DataFrame:
        """Get OHLCV data for a specific stock.

        Args:
            stock_code: Stock code (e.g., "7203")
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            timeframe: Data aggregation timeframe ("daily", "weekly", "monthly")

        Returns:
            DataFrame with columns [Open, High, Low, Close, Volume]
            and DatetimeIndex
        """
        params = {"timeframe": timeframe, **build_date_params(start_date, end_date)}
        data = self._get(
            self._dataset_path(f"/stocks/{stock_code}/ohlcv"),
            params=params,
        )
        return convert_ohlcv_response(data)

    def get_stock_list(
        self: "DatasetClientProtocol",
        min_records: int = 100,
        limit: int | None = None,
        detail: bool = False,
    ) -> pd.DataFrame:
        """Get list of available stocks in the dataset.

        Args:
            min_records: Minimum number of records required (default: 100)
            limit: Maximum number of stocks to return (optional)
            detail: Include start_date and end_date if True

        Returns:
            DataFrame with stock information
        """
        params: dict[str, str | int] = {"min_records": min_records}
        if limit:
            params["limit"] = limit
        if detail:
            params["detail"] = "true"

        data = self._get(self._dataset_path("/stocks"), params=params)
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_available_stocks(
        self: "DatasetClientProtocol",
        min_records: int = 100,
    ) -> pd.DataFrame:
        """Get detailed list of available stocks (with date range info).

        Args:
            min_records: Minimum number of records required

        Returns:
            DataFrame with stockCode, record_count, start_date, end_date
        """
        return self.get_stock_list(min_records=min_records, detail=True)

    def get_symbol_list(
        self: "DatasetClientProtocol",
        min_records: int = 100,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Get symbol list for backtesting.

        Args:
            min_records: Minimum number of records required
            limit: Maximum number of symbols (1-10000)

        Returns:
            DataFrame with code and name columns
        """
        # Validate limit range (security measure)
        if not 1 <= limit <= 10000:
            raise ValueError("limit must be between 1 and 10000")

        params = {"min_records": min_records, "limit": limit}
        data = self._get(self._dataset_path("/stocks"), params=params)
        return pd.DataFrame(data) if data else pd.DataFrame()
