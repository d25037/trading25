"""Batch data operations mixin for DatasetAPIClient."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pandas as pd

from .helpers import build_date_params, convert_ohlcv_response

if TYPE_CHECKING:
    from .base import DatasetClientProtocol


class BatchDataMixin:
    """Mixin providing batch data methods."""

    # API側の最大バッチサイズ制限（trading25-ts: MAX_BATCH_CODES = 100）
    MAX_BATCH_SIZE = 100

    def get_stocks_ohlcv_batch(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> dict[str, pd.DataFrame]:
        """Get OHLCV data for multiple stocks in a single API call.

        This method is optimized for loading multiple stocks efficiently
        by reducing the number of HTTP requests. Automatically splits
        requests into batches of MAX_BATCH_SIZE (100) to comply with
        server-side limits.

        Args:
            stock_codes: List of stock codes (e.g., ["7203", "9984"])
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            timeframe: Data aggregation timeframe ("daily", "weekly", "monthly")

        Returns:
            Dictionary mapping stock_code to DataFrame with columns
            [Open, High, Low, Close, Volume] and DatetimeIndex

        Note:
            Requires server-side batch endpoint:
            GET /api/dataset/{name}/stocks/ohlcv/batch?codes=1234,5678
        """
        if not stock_codes:
            return {}

        result: dict[str, pd.DataFrame] = {}

        # バッチサイズごとに分割してリクエスト
        for i in range(0, len(stock_codes), self.MAX_BATCH_SIZE):
            batch_codes = stock_codes[i : i + self.MAX_BATCH_SIZE]
            batch_result = self._get_stocks_ohlcv_single_batch(
                batch_codes, start_date, end_date, timeframe
            )
            result.update(batch_result)

        return result

    def _get_stocks_ohlcv_single_batch(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> dict[str, pd.DataFrame]:
        """Execute a single batch request for up to MAX_BATCH_SIZE stocks."""
        params = {
            "codes": ",".join(stock_codes),
            "timeframe": timeframe,
            **build_date_params(start_date, end_date),
        }

        try:
            data = self._get(
                self._dataset_path("/stocks/ohlcv/batch"),
                params=params,
            )
        except Exception:
            return self._get_stocks_ohlcv_fallback(
                stock_codes, start_date, end_date, timeframe
            )

        if not data:
            return {}

        return {
            code: convert_ohlcv_response(records)
            for code, records in data.items()
            if records
        }

    def _get_stocks_ohlcv_fallback(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> dict[str, pd.DataFrame]:
        """Fallback method when batch endpoint is not available.

        Fetches each stock individually.
        """
        result: dict[str, pd.DataFrame] = {}
        for code in stock_codes:
            try:
                df = self.get_stock_ohlcv(code, start_date, end_date, timeframe)
                if not df.empty:
                    result[code] = df
            except Exception:
                continue
        return result
