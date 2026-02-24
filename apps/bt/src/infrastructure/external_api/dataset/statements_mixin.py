"""Financial statements mixin for DatasetAPIClient."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.shared.models.types import StatementsPeriodType

from .helpers import build_date_params, convert_dated_response

if TYPE_CHECKING:
    from .base import DatasetClientProtocol

# 後方互換性のためエイリアスを維持
APIPeriodType = StatementsPeriodType

# API側の最大バッチサイズ制限（trading25-ts: MAX_BATCH_CODES = 100）
MAX_STATEMENTS_BATCH_SIZE = 100


class StatementsDataMixin:
    """Mixin providing financial statements data methods."""

    def get_statements(
        self: "DatasetClientProtocol",
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: APIPeriodType = "all",
        actual_only: bool = True,
    ) -> pd.DataFrame:
        """Get financial statements data for a specific stock.

        Args:
            stock_code: Stock code
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            period_type: Filter by period type (default: "all")
                - "all": All periods
                - "FY": Full year only
                - "1Q", "2Q", "3Q": Specific quarters
            actual_only: If True, exclude forecast data (records without
                actual financial data like EPS/Profit/Equity)

        Returns:
            DataFrame with financial statements data
            and DatetimeIndex (disclosedDate)
        """
        params = {
            **build_date_params(start_date, end_date),
            "period_type": period_type,
            "actual_only": "true" if actual_only else "false",
        }

        data = self._get(
            self._dataset_path(f"/statements/{stock_code}"),
            params=params,
        )
        return convert_dated_response(data, date_column="disclosedDate")

    def get_statements_batch(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: APIPeriodType = "all",
        actual_only: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Get financial statements data for multiple stocks in a single API call.

        Automatically splits requests into batches of MAX_STATEMENTS_BATCH_SIZE (100)
        to comply with server-side limits.

        Args:
            stock_codes: List of stock codes (e.g., ["7203", "9984"])
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            period_type: Filter by period type (default: "all")
            actual_only: If True, exclude forecast data

        Returns:
            Dictionary mapping stock_code to DataFrame with financial statements
            and DatetimeIndex (disclosedDate)
        """
        if not stock_codes:
            return {}

        result: dict[str, pd.DataFrame] = {}

        for i in range(0, len(stock_codes), MAX_STATEMENTS_BATCH_SIZE):
            batch_codes = stock_codes[i : i + MAX_STATEMENTS_BATCH_SIZE]
            batch_result = self._get_statements_single_batch(
                batch_codes, start_date, end_date, period_type, actual_only
            )
            result.update(batch_result)

        return result

    def _get_statements_single_batch(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: APIPeriodType = "all",
        actual_only: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Execute a single batch request for up to MAX_STATEMENTS_BATCH_SIZE stocks."""
        params: dict[str, str] = {
            "codes": ",".join(stock_codes),
            **build_date_params(start_date, end_date),
            "period_type": period_type,
            "actual_only": "true" if actual_only else "false",
        }

        try:
            data = self._get(
                self._dataset_path("/statements/batch"),
                params=params,
            )
        except Exception:
            return self._get_statements_fallback(
                stock_codes, start_date, end_date, period_type, actual_only
            )

        if not data:
            return {}

        return {
            code: convert_dated_response(records, date_column="disclosedDate")
            for code, records in data.items()
            if records
        }

    def _get_statements_fallback(
        self: "DatasetClientProtocol",
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: APIPeriodType = "all",
        actual_only: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Fallback method when batch endpoint is not available."""
        result: dict[str, pd.DataFrame] = {}
        for code in stock_codes:
            try:
                df = self.get_statements(
                    code, start_date, end_date, period_type, actual_only
                )
                if not df.empty:
                    result[code] = df
            except Exception:
                continue
        return result
