"""Local SoT data providers for analytics, screening verification, and charts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

from src.infrastructure.data_access.clients import DirectDatasetClient, DirectMarketClient
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.screening_market_loader import (
    load_market_sector_indices,
    load_market_stock_sector_mapping,
)


class AnalyticsDataProvider(Protocol):
    """Provider abstraction for SoT-backed analytics data."""

    source_kind: str

    def get_stock_info(self, stock_code: str) -> Any | None: ...
    def get_stock_ohlcv(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame: ...
    def get_statements(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        period_type: str = "all",
        actual_only: bool = False,
    ) -> pd.DataFrame: ...
    def get_margin(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame: ...


@dataclass
class MarketAnalyticsDataProvider:
    """Analytics provider backed by local market.duckdb."""

    reader: MarketDbReader | None
    source_kind: str = "market"

    def __post_init__(self) -> None:
        self._client = DirectMarketClient()

    def close(self) -> None:
        self._client.close()

    def get_stock_info(self, stock_code: str) -> Any | None:
        return self._client.get_stock_info(stock_code)

    def get_stock_ohlcv(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._client.get_stock_ohlcv(stock_code, start_date, end_date)

    def get_statements(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        period_type: str = "all",
        actual_only: bool = False,
    ) -> pd.DataFrame:
        return self._client.get_statements(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            period_type=period_type,
            actual_only=actual_only,
        )

    def get_margin(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._client.get_margin(stock_code, start_date, end_date)

    def get_topix(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._client.get_topix(start_date, end_date)

    def get_sector_indices(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        if self.reader is None:
            return {}
        return load_market_sector_indices(
            self.reader,
            start_date=start_date,
            end_date=end_date,
        )

    def get_stock_sector_mapping(self) -> dict[str, str]:
        if self.reader is None:
            return {}
        return load_market_stock_sector_mapping(self.reader)

    def get_statements_by_date(self, disclosed_date: str) -> list[dict[str, Any]]:
        if self.reader is None:
            return []
        rows = self.reader.query(
            """
            WITH ranked AS (
                SELECT
                    code,
                    disclosed_date,
                    type_of_document,
                    type_of_current_period,
                    profit,
                    equity,
                    ROW_NUMBER() OVER (
                        PARTITION BY substr(code, 1, 4)
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                    ) AS rn
                FROM statements
                WHERE disclosed_date = ?
            )
            SELECT
                code,
                disclosed_date,
                type_of_document,
                type_of_current_period,
                profit,
                equity
            FROM ranked
            WHERE rn = 1
            ORDER BY code
            """,
            (disclosed_date,),
        )
        return [
            {
                "Code": str(row["code"]),
                "DiscDate": str(row["disclosed_date"]),
                "DocType": str(row["type_of_document"] or ""),
                "CurPerType": str(row["type_of_current_period"] or ""),
                "CurPerEn": str(row["disclosed_date"]),
                "NP": row["profit"],
                "Eq": row["equity"],
            }
            for row in rows
        ]


@dataclass
class DatasetAnalyticsDataProvider:
    """Dataset snapshot provider for future SoT-aligned analytics reuse."""

    dataset_name: str
    source_kind: str = "dataset"

    def __post_init__(self) -> None:
        self._client = DirectDatasetClient(self.dataset_name)

    def close(self) -> None:
        self._client.close()

    def get_stock_info(self, stock_code: str) -> Any | None:
        _ = stock_code
        return None

    def get_stock_ohlcv(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._client.get_stock_ohlcv(stock_code, start_date, end_date)

    def get_statements(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        period_type: str = "all",
        actual_only: bool = False,
    ) -> pd.DataFrame:
        return self._client.get_statements(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            period_type=period_type,
            actual_only=actual_only,
        )

    def get_margin(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._client.get_margin(stock_code, start_date, end_date)
