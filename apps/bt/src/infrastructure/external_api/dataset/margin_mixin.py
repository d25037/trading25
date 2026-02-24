"""Margin data mixin for DatasetAPIClient."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pandas as pd

from .helpers import build_date_params, convert_dated_response

if TYPE_CHECKING:
    from .base import DatasetClientProtocol

# API側の最大バッチサイズ制限（trading25-ts: MAX_BATCH_CODES = 100）
MAX_MARGIN_BATCH_SIZE = 100


class MarginDataMixin:
    """Mixin providing margin data methods."""

    def get_margin(
        self: "DatasetClientProtocol",
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Get margin data for a specific stock.

        Args:
            stock_code: Stock code
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            DataFrame with columns [longMarginVolume, shortMarginVolume]
            and DatetimeIndex
        """
        params = build_date_params(start_date, end_date)
        data = self._get(
            self._dataset_path(f"/margin/{stock_code}"),
            params=params,
        )
        return convert_dated_response(data)

    def get_margin_list(
        self: "DatasetClientProtocol",
        min_records: int = 10,
        codes: list[str] | None = None,
    ) -> pd.DataFrame:
        """Get list of stocks with margin data.

        Args:
            min_records: Minimum number of records required
            codes: Optional list of specific stock codes to query

        Returns:
            DataFrame with margin data summary
        """
        params: dict[str, str | int] = {"min_records": min_records}
        if codes:
            params["codes"] = ",".join(codes)

        data = self._get(self._dataset_path("/margin"), params=params)
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_margin_batch(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Get margin data for multiple stocks in a single API call.

        Automatically splits requests into batches of MAX_MARGIN_BATCH_SIZE (100)
        to comply with server-side limits.

        Args:
            stock_codes: List of stock codes (e.g., ["7203", "9984"])
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            Dictionary mapping stock_code to DataFrame with columns
            [longMarginVolume, shortMarginVolume] and DatetimeIndex
        """
        if not stock_codes:
            return {}

        result: dict[str, pd.DataFrame] = {}

        for i in range(0, len(stock_codes), MAX_MARGIN_BATCH_SIZE):
            batch_codes = stock_codes[i : i + MAX_MARGIN_BATCH_SIZE]
            batch_result = self._get_margin_single_batch(
                batch_codes, start_date, end_date
            )
            result.update(batch_result)

        return result

    def _get_margin_single_batch(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Execute a single batch request for up to MAX_MARGIN_BATCH_SIZE stocks."""
        params = {
            "codes": ",".join(stock_codes),
            **build_date_params(start_date, end_date),
        }

        try:
            data = self._get(
                self._dataset_path("/margin/batch"),
                params=params,
            )
        except Exception:
            return self._get_margin_fallback(stock_codes, start_date, end_date)

        if not data:
            return {}

        return {
            code: convert_dated_response(records)
            for code, records in data.items()
            if records
        }

    def _get_margin_fallback(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fallback method when batch endpoint is not available."""
        result: dict[str, pd.DataFrame] = {}
        for code in stock_codes:
            try:
                df = self.get_margin(code, start_date, end_date)
                if not df.empty:
                    result[code] = df
            except Exception:
                continue
        return result

    def get_multiple_margin(
        self: "DatasetClientProtocol",
        codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Get margin data for multiple stocks at once.

        .. deprecated::
            Use :meth:`get_margin_batch` instead, which uses the batch API
            for better performance.

        Args:
            codes: List of stock codes
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            Dictionary mapping stock_code to DataFrame
        """
        warnings.warn(
            "get_multiple_margin() is deprecated, use get_margin_batch() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_margin_batch(codes, start_date, end_date)
