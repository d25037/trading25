"""API client for market.db operations (market analysis)."""

from __future__ import annotations

from typing import Optional

import pandas as pd

from src.api.client import BaseAPIClient


class MarketAPIClient(BaseAPIClient):
    """API client for market.db operations.

    This client provides access to market analysis data stored in market.db
    through the localhost:3001 API.

    Endpoints used:
        - GET /api/market/stocks/{code}/ohlcv - Stock OHLCV data
        - GET /api/market/stocks - All stocks data (for screening)
        - GET /api/market/topix - TOPIX data

    Usage:
        ```python
        with MarketAPIClient() as client:
            df = client.get_topix()
            ohlcv = client.get_stock_ohlcv("7203")
        ```
    """

    # =========================================================================
    # Stock Data Methods
    # =========================================================================

    def get_stock_ohlcv(
        self,
        stock_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get OHLCV data for a specific stock from market.db.

        Args:
            stock_code: Stock code (e.g., "7203")
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            DataFrame with columns [Open, High, Low, Close, Volume]
            and DatetimeIndex
        """
        params: dict[str, str] = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        data = self._get(
            f"/api/market/stocks/{stock_code}/ohlcv",
            params=params if params else None,
        )

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.columns = pd.Index(["Open", "High", "Low", "Close", "Volume"])
        return df

    def get_stock_data_for_screening(
        self,
        market: str = "prime",
        history_days: int = 300,
    ) -> dict[str, tuple[pd.DataFrame, str]]:
        """Get all stock data for screening purposes.

        Args:
            market: Market code ("prime" or "standard")
            history_days: Number of days of history to fetch

        Returns:
            Dictionary mapping stock_code to (DataFrame, company_name)
            DataFrame has columns [Open, High, Low, Close, Volume]
        """
        data = self._get(
            "/api/market/stocks",
            params={"market": market, "history_days": history_days},
        )

        if not data:
            return {}

        result: dict[str, tuple[pd.DataFrame, str]] = {}
        for item in data:
            code = str(item.get("code", ""))
            company_name = str(item.get("company_name", ""))
            stock_data = item.get("data", [])

            if not stock_data:
                continue

            stock_df = pd.DataFrame(stock_data)
            stock_df["date"] = pd.to_datetime(stock_df["date"])
            stock_df.set_index("date", inplace=True)
            stock_df.columns = pd.Index(["Open", "High", "Low", "Close", "Volume"])
            result[code] = (stock_df, company_name)

        return result

    # =========================================================================
    # TOPIX Data Methods
    # =========================================================================

    def get_topix(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get TOPIX data from market.db (topix_data table).

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            DataFrame with columns [Open, High, Low, Close]
            and DatetimeIndex
        """
        params: dict[str, str] = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        data = self._get(
            "/api/market/topix",
            params=params if params else None,
        )

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.columns = pd.Index(["Open", "High", "Low", "Close"])
        return df
