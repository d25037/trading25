from __future__ import annotations

from typing import Any

import pandas as pd

from src.application.services.fundamentals_service import FundamentalsService
from src.entrypoints.http.schemas.fundamentals import FundamentalsComputeRequest
from src.infrastructure.external_api.jquants_client import StockInfo


class FakeAdjustedMarketClient:
    def close(self) -> None:
        return None

    def get_statements(
        self,
        stock_code: str,
        *,
        period_type: str = "all",
        actual_only: bool = False,
    ) -> pd.DataFrame:
        del stock_code, period_type, actual_only
        return pd.DataFrame(
            [
                {
                    "code": "7203",
                    "typeOfDocument": "FinancialStatements",
                    "typeOfCurrentPeriod": "FY",
                    "periodEnd": "2024-03-31",
                    "sales": 1_000_000.0,
                    "operatingProfit": 100_000.0,
                    "profit": 50_000.0,
                    "equity": 500_000.0,
                    "earningsPerShare": 100.0,
                    "bps": 1000.0,
                    "forecastEps": 120.0,
                    "dividendFY": 30.0,
                    "sharesOutstanding": 10_000_000.0,
                }
            ],
            index=pd.Index(["2024-05-10"], name="disclosedDate"),
        )

    def get_stock_info(self, stock_code: str) -> StockInfo:
        return StockInfo(
            code=stock_code,
            companyName="Toyota",
            companyNameEnglish="Toyota",
            marketCode="0112",
            marketName="スタンダード",
            sector17Code="1",
            sector17Name="Sector",
            sector33Code="1",
            sector33Name="Industry",
            scaleCategory="",
            listedDate="1949-05-16",
        )

    def get_stock_ohlcv(self, stock_code: str) -> pd.DataFrame:
        del stock_code
        return pd.DataFrame(
            {"Close": [500.0], "Volume": [100_000]},
            index=pd.to_datetime(["2024-12-30"]),
        )

    def get_stock_adjustment_events(self, stock_code: str) -> list[Any]:
        del stock_code
        return []

    def get_adjusted_statement_metrics(
        self,
        stock_code: str,
        as_of_date: str | None = None,
    ) -> list[dict[str, Any]]:
        del stock_code, as_of_date
        return [
            {
                "code": "7203",
                "disclosed_date": "2024-05-10",
                "period_end": "2024-03-31",
                "period_type": "FY",
                "adjusted_eps": 50.0,
                "adjusted_bps": 500.0,
                "adjusted_forecast_eps": 60.0,
                "adjusted_dividend_fy": 15.0,
                "basis_version": "adjusted-v1:2024-12-30",
                "price_basis_date": "2024-12-30",
            }
        ]

    def get_daily_valuation(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        del stock_code, start_date, end_date
        return [
            {
                "code": "7203",
                "date": "2024-12-30",
                "price_basis_date": "2024-12-30",
                "close": 500.0,
                "eps": 50.0,
                "bps": 500.0,
                "forward_eps": 60.0,
                "per": 10.0,
                "forward_per": 8.3333333333,
                "pbr": 1.0,
                "market_cap": 10_000_000_000.0,
                "free_float_market_cap": 9_000_000_000.0,
                "statement_disclosed_date": "2024-05-10",
                "forward_eps_disclosed_date": "2024-05-10",
                "forward_eps_source": "fy",
                "basis_version": "adjusted-v1:2024-12-30",
            }
        ]


def test_compute_prefers_adjusted_tables_for_valuation_and_keeps_raw_history() -> None:
    service = FundamentalsService()
    service._market_client = FakeAdjustedMarketClient()

    result = service.compute_fundamentals(FundamentalsComputeRequest(symbol="7203"))

    assert result.priceBasisDate == "2024-12-30"
    assert result.valuationBasisVersion == "adjusted-v1:2024-12-30"
    assert result.adjustedMetricsSource == "daily_valuation"
    assert result.dailyValuation is not None
    assert result.dailyValuation[0].per == 10.0
    assert result.dailyValuation[0].forwardPer == 8.3333333333
    assert result.dailyValuation[0].priceBasisDate == "2024-12-30"
    assert result.dailyValuation[0].basisVersion == "adjusted-v1:2024-12-30"

    assert result.data[0].eps == 100.0
    assert result.data[0].bps == 1000.0
    assert result.data[0].adjustedEps == 50.0
    assert result.data[0].adjustedBps == 500.0
    assert result.data[0].adjustedForecastEps == 60.0

    assert result.latestMetrics is not None
    assert result.latestMetrics.per == 10.0
    assert result.latestMetrics.forwardPer == 8.3333333333
    assert result.latestMetrics.pbr == 1.0
    assert result.latestMetrics.marketCap == 10_000_000_000.0
    assert result.latestMetrics.freeFloatMarketCap == 9_000_000_000.0
    assert result.latestMetrics.adjustedEps == 50.0
    assert result.latestMetrics.adjustedBps == 500.0
