"""Fundamentals service (I/O + orchestration only)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from loguru import logger

from src.domains.fundamentals import (
    DailyValuationDataPoint as DomainDailyValuationDataPoint,
    FundamentalDataPoint as DomainFundamentalDataPoint,
    FundamentalsCalculator,
)
from src.entrypoints.http.schemas.fundamentals import (
    DailyValuationDataPoint,
    FundamentalDataPoint,
    FundamentalsComputeRequest,
    FundamentalsComputeResponse,
)
from src.infrastructure.external_api.jquants_client import JQuantsAPIClient, StockInfo
from src.infrastructure.external_api.market_client import MarketAPIClient


class FundamentalsService:
    """Service for fundamentals API orchestration."""

    def __init__(self) -> None:
        self._jquants_client: JQuantsAPIClient | None = None
        self._market_client: MarketAPIClient | None = None
        self._calculator = FundamentalsCalculator()

    def __del__(self) -> None:
        self.close()

    @property
    def jquants_client(self) -> JQuantsAPIClient:
        if self._jquants_client is None:
            self._jquants_client = JQuantsAPIClient()
        return self._jquants_client

    @property
    def market_client(self) -> MarketAPIClient:
        if self._market_client is None:
            self._market_client = MarketAPIClient()
        return self._market_client

    def close(self) -> None:
        if self._jquants_client is not None:
            self._jquants_client.close()
            self._jquants_client = None
        if self._market_client is not None:
            self._market_client.close()
            self._market_client = None

    @staticmethod
    def _to_api_fundamental_data_point(
        data_point: DomainFundamentalDataPoint,
    ) -> FundamentalDataPoint:
        return FundamentalDataPoint(**data_point.model_dump())

    @staticmethod
    def _to_api_daily_valuation_data_point(
        data_point: DomainDailyValuationDataPoint,
    ) -> DailyValuationDataPoint:
        return DailyValuationDataPoint(**data_point.model_dump())

    def _get_stock_info(self, symbol: str) -> StockInfo | None:
        try:
            return self.jquants_client.get_stock_info(symbol)
        except Exception as e:
            logger.warning(f"Failed to get stock info for {symbol}: {e}")
            return None

    def _get_daily_stock_ohlcv(self, symbol: str) -> pd.DataFrame:
        try:
            df = self.market_client.get_stock_ohlcv(symbol)
            if df.empty or "Close" not in df.columns:
                return pd.DataFrame()
            return df
        except Exception as e:
            logger.warning(f"Failed to get daily OHLCV data for {symbol}: {e}")
            return pd.DataFrame()

    def _get_daily_stock_prices(self, symbol: str) -> dict[str, float]:
        return self._calculator._to_daily_close_map(self._get_daily_stock_ohlcv(symbol))

    def compute_fundamentals(
        self, request: FundamentalsComputeRequest
    ) -> FundamentalsComputeResponse:
        logger.debug(f"Computing fundamentals for {request.symbol}")

        statements = self.jquants_client.get_statements(request.symbol)

        if not statements:
            logger.debug(f"No financial statements found for {request.symbol}")
            return FundamentalsComputeResponse(
                symbol=request.symbol,
                data=[],
                tradingValuePeriod=request.trading_value_period,
                forecastEpsLookbackFyCount=request.forecast_eps_lookback_fy_count,
                lastUpdated=datetime.now().isoformat(),
            )

        stock_info = self._get_stock_info(request.symbol)

        daily_ohlcv = self._get_daily_stock_ohlcv(request.symbol)
        daily_prices = self._calculator._to_daily_close_map(daily_ohlcv)

        daily_valuation = self._calculator._calculate_daily_valuation(
            statements, daily_prices, request.prefer_consolidated
        )

        filtered_statements = self._calculator._filter_statements(
            statements, request.period_type, request.from_date, request.to_date
        )

        if not filtered_statements:
            api_daily_valuation = (
                [
                    self._to_api_daily_valuation_data_point(item)
                    for item in daily_valuation
                ]
                if daily_valuation
                else None
            )
            return FundamentalsComputeResponse(
                symbol=request.symbol,
                companyName=stock_info.companyName if stock_info else None,
                data=[],
                dailyValuation=api_daily_valuation,
                tradingValuePeriod=request.trading_value_period,
                forecastEpsLookbackFyCount=request.forecast_eps_lookback_fy_count,
                lastUpdated=datetime.now().isoformat(),
            )

        price_map = self._calculator._get_stock_prices_for_statements(
            filtered_statements,
            daily_prices,
        )

        data = [
            self._calculator._calculate_all_metrics(
                stmt,
                price_map,
                request.prefer_consolidated,
            )
            for stmt in filtered_statements
        ]

        daily_market_cap_to_trading_value_ratio_map = (
            self._calculator._calculate_daily_market_cap_to_trading_value_ratio(
                daily_ohlcv,
                daily_valuation,
                request.trading_value_period,
            )
        )
        data = self._calculator._apply_trading_value_ratio_to_statements(
            data,
            daily_market_cap_to_trading_value_ratio_map,
        )

        data.sort(key=lambda x: x.date, reverse=True)

        latest_metrics = self._calculator._find_latest_with_actual_data(data)
        latest_metrics = self._calculator._update_latest_with_daily_valuation(
            latest_metrics,
            daily_valuation,
            data,
        )
        latest_metrics = self._calculator._apply_latest_trading_value_ratio(
            latest_metrics,
            daily_market_cap_to_trading_value_ratio_map,
        )

        latest_metrics = self._calculator._enhance_latest_metrics(
            latest_metrics,
            statements,
            request.prefer_consolidated,
        )

        self._calculator._annotate_latest_fy_with_revision(
            data,
            latest_metrics,
            statements,
            request.prefer_consolidated,
        )

        data, latest_metrics = self._calculator._apply_share_adjustments(
            data,
            statements,
            latest_metrics,
        )
        latest_metrics = self._calculator._apply_forecast_eps_above_recent_fy_actuals(
            latest_metrics,
            data,
            request.forecast_eps_lookback_fy_count,
        )

        logger.debug(
            f"Fundamentals calculation complete: {len(data)} data points, "
            f"{len(daily_valuation) if daily_valuation else 0} daily valuation points"
        )

        api_data = [self._to_api_fundamental_data_point(item) for item in data]
        api_latest_metrics = (
            self._to_api_fundamental_data_point(latest_metrics)
            if latest_metrics is not None
            else None
        )
        api_daily_valuation = (
            [self._to_api_daily_valuation_data_point(item) for item in daily_valuation]
            if daily_valuation
            else None
        )

        return FundamentalsComputeResponse(
            symbol=request.symbol,
            companyName=stock_info.companyName if stock_info else None,
            data=api_data,
            latestMetrics=api_latest_metrics,
            dailyValuation=api_daily_valuation,
            tradingValuePeriod=request.trading_value_period,
            forecastEpsLookbackFyCount=request.forecast_eps_lookback_fy_count,
            lastUpdated=datetime.now().isoformat(),
        )


fundamentals_service = FundamentalsService()
