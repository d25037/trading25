"""
Fundamentals Service

Calculates fundamental metrics from JQuants financial statements data.
This is a Python port of the legacy TypeScript fundamentals service.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from loguru import logger

from src.api.jquants_client import JQuantsAPIClient, JQuantsStatement, StockInfo
from src.api.market_client import MarketAPIClient
from src.models.types import normalize_period_type
from src.utils.financial import calc_market_cap_scalar
from src.server.schemas.fundamentals import (
    DailyValuationDataPoint,
    FundamentalDataPoint,
    FundamentalsComputeRequest,
    FundamentalsComputeResponse,
)


@dataclass
class FYDataPoint:
    """FY data point for daily valuation calculation."""

    disclosed_date: str
    eps: float | None
    bps: float | None


# Empty previous period cash flow dict (used as return value)
_EMPTY_PREV_CASH_FLOW: dict[str, float | None] = {
    "prevCashFlowOperating": None,
    "prevCashFlowInvesting": None,
    "prevCashFlowFinancing": None,
    "prevCashAndEquivalents": None,
}


class FundamentalsService:
    """Service for computing fundamental metrics."""

    def __init__(self) -> None:
        self._jquants_client: JQuantsAPIClient | None = None
        self._market_client: MarketAPIClient | None = None

    def __del__(self) -> None:
        """Cleanup on garbage collection."""
        self.close()

    @property
    def jquants_client(self) -> JQuantsAPIClient:
        """Lazy-initialized JQuants API client."""
        if self._jquants_client is None:
            self._jquants_client = JQuantsAPIClient()
        return self._jquants_client

    @property
    def market_client(self) -> MarketAPIClient:
        """Lazy-initialized Market API client."""
        if self._market_client is None:
            self._market_client = MarketAPIClient()
        return self._market_client

    def close(self) -> None:
        """Close all API clients."""
        if self._jquants_client is not None:
            self._jquants_client.close()
            self._jquants_client = None
        if self._market_client is not None:
            self._market_client.close()
            self._market_client = None

    def compute_fundamentals(
        self, request: FundamentalsComputeRequest
    ) -> FundamentalsComputeResponse:
        """Compute fundamental metrics for a stock.

        Args:
            request: Computation request parameters

        Returns:
            FundamentalsComputeResponse with computed metrics
        """
        logger.debug(f"Computing fundamentals for {request.symbol}")

        # Fetch financial statements from J-Quants
        statements = self.jquants_client.get_statements(request.symbol)

        if not statements:
            logger.debug(f"No financial statements found for {request.symbol}")
            return FundamentalsComputeResponse(
                symbol=request.symbol,
                data=[],
                tradingValuePeriod=request.trading_value_period,
                lastUpdated=datetime.now().isoformat(),
            )

        logger.debug(f"Found {len(statements)} statements")

        # Get stock info for company name
        stock_info = self._get_stock_info(request.symbol)

        # Get daily OHLCV and stock prices for valuation calculation
        daily_ohlcv = self._get_daily_stock_ohlcv(request.symbol)
        daily_prices = self._to_daily_close_map(daily_ohlcv)

        # Calculate daily valuation time-series
        daily_valuation = self._calculate_daily_valuation(
            statements, daily_prices, request.prefer_consolidated
        )

        # Filter statements by criteria
        filtered_statements = self._filter_statements(
            statements, request.period_type, request.from_date, request.to_date
        )

        if not filtered_statements:
            logger.debug("No statements match filter criteria")
            return FundamentalsComputeResponse(
                symbol=request.symbol,
                companyName=stock_info.companyName if stock_info else None,
                data=[],
                dailyValuation=daily_valuation if daily_valuation else None,
                tradingValuePeriod=request.trading_value_period,
                lastUpdated=datetime.now().isoformat(),
            )

        # Get stock prices for disclosure dates
        price_map = self._get_stock_prices_for_statements(
            request.symbol, filtered_statements, daily_prices
        )

        # Calculate metrics for each statement
        data = [
            self._calculate_all_metrics(stmt, price_map, request.prefer_consolidated)
            for stmt in filtered_statements
        ]

        daily_market_cap_to_trading_value_ratio_map = (
            self._calculate_daily_market_cap_to_trading_value_ratio(
                daily_ohlcv,
                daily_valuation,
                request.trading_value_period,
            )
        )
        data = self._apply_trading_value_ratio_to_statements(
            data, daily_market_cap_to_trading_value_ratio_map
        )

        # Sort by date descending
        data.sort(key=lambda x: x.date, reverse=True)

        # Get latest metrics with actual financial data
        latest_metrics = self._find_latest_with_actual_data(data)

        # Update latest metrics with daily valuation
        latest_metrics = self._update_latest_with_daily_valuation(
            latest_metrics, daily_valuation, data
        )
        latest_metrics = self._apply_latest_trading_value_ratio(
            latest_metrics, daily_market_cap_to_trading_value_ratio_map
        )

        # Enhance latest metrics with forecast EPS and previous period CF
        latest_metrics = self._enhance_latest_metrics(
            latest_metrics, statements, request.prefer_consolidated
        )

        # Annotate latest FY with revised forecast from latest Q
        self._annotate_latest_fy_with_revision(
            data, latest_metrics, statements, request.prefer_consolidated
        )

        # Apply share-based adjustments for EPS/BPS/Forecast EPS
        data, latest_metrics = self._apply_share_adjustments(
            data, statements, latest_metrics
        )

        logger.debug(
            f"Fundamentals calculation complete: {len(data)} data points, "
            f"{len(daily_valuation) if daily_valuation else 0} daily valuation points"
        )

        return FundamentalsComputeResponse(
            symbol=request.symbol,
            companyName=stock_info.companyName if stock_info else None,
            data=data,
            latestMetrics=latest_metrics,
            dailyValuation=daily_valuation if daily_valuation else None,
            tradingValuePeriod=request.trading_value_period,
            lastUpdated=datetime.now().isoformat(),
        )

    def _build_shares_map(
        self, statements: list[JQuantsStatement]
    ) -> dict[tuple[str, str, str | None], float | None]:
        """Build lookup map for shares outstanding by period metadata."""
        shares_map: dict[tuple[str, str, str | None], float | None] = {}
        for stmt in statements:
            period_type = normalize_period_type(stmt.CurPerType)
            key = (stmt.CurPerEn, stmt.DiscDate, period_type)
            shares_map[key] = stmt.ShOutFY
        return shares_map

    def _get_shares_for_datapoint(
        self,
        data_point: FundamentalDataPoint,
        shares_map: dict[tuple[str, str, str | None], float | None],
    ) -> float | None:
        """Resolve shares outstanding for a data point."""
        period_type = normalize_period_type(data_point.periodType)
        key = (data_point.date, data_point.disclosedDate, period_type)
        return shares_map.get(key)

    def _resolve_baseline_shares_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> float | None:
        """Find baseline shares from the latest quarterly disclosure (1Q/2Q/3Q).

        Falls back to the latest disclosure of any period type if no quarterly data exists.
        """
        quarterly_types = ("1Q", "2Q", "3Q")

        def _find_latest_shares(
            require_quarterly: bool,
        ) -> float | None:
            latest_disclosed: str | None = None
            latest_shares: float | None = None
            for stmt in statements:
                if require_quarterly:
                    period_type = normalize_period_type(stmt.CurPerType)
                    if period_type not in quarterly_types:
                        continue
                shares = stmt.ShOutFY
                if shares is None or shares == 0:
                    continue
                if latest_disclosed is None or stmt.DiscDate > latest_disclosed:
                    latest_disclosed = stmt.DiscDate
                    latest_shares = shares
            return latest_shares

        return _find_latest_shares(require_quarterly=True) or _find_latest_shares(
            require_quarterly=False
        )

    def _compute_adjusted_value(
        self,
        value: float | None,
        current_shares: float | None,
        base_shares: float | None,
    ) -> float | None:
        """Compute adjusted value using share count ratio."""
        if (
            value is None
            or current_shares is None
            or base_shares is None
            or current_shares == 0
            or base_shares == 0
        ):
            return None
        if math.isnan(current_shares) or math.isnan(base_shares):
            return None
        # Adjust to baseline share count (latest quarterly disclosure) so splits reduce historical EPS/BPS.
        adjusted = value * (current_shares / base_shares)
        return self._round_or_none(adjusted)

    def _build_adjusted_datapoint(
        self,
        item: FundamentalDataPoint,
        eps_shares: float | None,
        bps_shares: float | None,
        forecast_shares: float | None,
        dividend_shares: float | None,
        base_shares: float | None,
    ) -> FundamentalDataPoint:
        """Build a new FundamentalDataPoint with share-adjusted per-share metrics and recalculated PER/PBR."""
        adjusted_eps = self._compute_adjusted_value(item.eps, eps_shares, base_shares)
        adjusted_bps = self._compute_adjusted_value(item.bps, bps_shares, base_shares)
        adjusted_forecast = self._compute_adjusted_value(
            item.forecastEps, forecast_shares, base_shares
        )
        adjusted_dividend = self._compute_adjusted_value(
            item.dividendFy, dividend_shares, base_shares
        )
        display_eps = adjusted_eps if adjusted_eps is not None else item.eps
        display_bps = adjusted_bps if adjusted_bps is not None else item.bps

        return FundamentalDataPoint(
            **{
                **item.model_dump(),
                "adjustedEps": adjusted_eps,
                "adjustedForecastEps": adjusted_forecast,
                "adjustedBps": adjusted_bps,
                "adjustedDividendFy": adjusted_dividend,
                "per": self._round_or_none(self._calculate_per(display_eps, item.stockPrice)),
                "pbr": self._round_or_none(self._calculate_pbr(display_bps, item.stockPrice)),
            }
        )

    def _apply_share_adjustments(
        self,
        data: list[FundamentalDataPoint],
        statements: list[JQuantsStatement],
        latest_metrics: FundamentalDataPoint | None,
    ) -> tuple[list[FundamentalDataPoint], FundamentalDataPoint | None]:
        """Apply share-based adjustments to per-share metrics (EPS/BPS/forecast/dividend)."""
        shares_map = self._build_shares_map(statements)
        base_shares = self._resolve_baseline_shares_from_latest_quarter(statements)

        updated_data: list[FundamentalDataPoint] = []
        for item in data:
            current_shares = self._get_shares_for_datapoint(item, shares_map)
            updated_data.append(
                self._build_adjusted_datapoint(
                    item, current_shares, current_shares, current_shares, current_shares, base_shares
                )
            )

        updated_latest = self._apply_adjusted_to_latest_metrics(
            latest_metrics, updated_data, shares_map, base_shares
        )

        return updated_data, updated_latest

    def _apply_adjusted_to_latest_metrics(
        self,
        metrics: FundamentalDataPoint | None,
        data: list[FundamentalDataPoint],
        shares_map: dict[tuple[str, str, str | None], float | None],
        base_shares: float | None,
    ) -> FundamentalDataPoint | None:
        """Apply adjusted values to latestMetrics with FY alignment."""
        if metrics is None:
            return None

        # Use latest FY shares for EPS/BPS alignment when available
        latest_fy = next(
            (d for d in data if d.periodType == "FY" and self._has_actual_financial_data(d)),
            None,
        )
        fy_shares = (
            self._get_shares_for_datapoint(latest_fy, shares_map)
            if latest_fy is not None
            else None
        )
        metrics_shares = self._get_shares_for_datapoint(metrics, shares_map)
        eps_bps_shares = fy_shares or metrics_shares

        return self._build_adjusted_datapoint(
            metrics, eps_bps_shares, eps_bps_shares, metrics_shares, metrics_shares, base_shares
        )

    def _get_stock_info(self, symbol: str) -> StockInfo | None:
        """Get stock info from market.db."""
        try:
            return self.jquants_client.get_stock_info(symbol)
        except Exception as e:
            logger.warning(f"Failed to get stock info for {symbol}: {e}")
            return None

    def _get_daily_stock_prices(self, symbol: str) -> dict[str, float]:
        """Get daily stock prices from market.db."""
        daily_ohlcv = self._get_daily_stock_ohlcv(symbol)
        return self._to_daily_close_map(daily_ohlcv)

    def _get_daily_stock_ohlcv(self, symbol: str) -> pd.DataFrame:
        """Get daily OHLCV data from market.db."""
        try:
            df = self.market_client.get_stock_ohlcv(symbol)
            if df.empty or "Close" not in df.columns:
                return pd.DataFrame()
            return df
        except Exception as e:
            logger.warning(f"Failed to get daily OHLCV data for {symbol}: {e}")
            return pd.DataFrame()

    def _to_daily_close_map(self, daily_ohlcv: pd.DataFrame) -> dict[str, float]:
        """Convert OHLCV DataFrame into daily close map."""
        if daily_ohlcv.empty or "Close" not in daily_ohlcv.columns:
            return {}
        close_series: pd.Series[float] = daily_ohlcv["Close"]
        return {k.strftime("%Y-%m-%d"): v for k, v in close_series.items()}

    def _get_stock_prices_for_statements(
        self,
        symbol: str,
        statements: list[JQuantsStatement],
        daily_prices: dict[str, float] | None = None,
    ) -> dict[str, float]:
        """Get stock prices at statement disclosure dates."""
        if not statements:
            return {}

        try:
            # Get all daily prices
            if daily_prices is None:
                daily_prices = self._get_daily_stock_prices(symbol)
            if not daily_prices:
                return {}

            # Map disclosure dates to prices
            result: dict[str, float] = {}
            sorted_dates = sorted(daily_prices.keys())

            for stmt in statements:
                disc_date = stmt.DiscDate
                # Find the price on or before disclosure date
                price = self._find_price_at_date(disc_date, sorted_dates, daily_prices)
                if price is not None:
                    result[disc_date] = price

            return result
        except Exception as e:
            logger.warning(
                f"Failed to get stock prices for statements {symbol}: {e}"
            )
            return {}

    def _find_price_at_date(
        self,
        target_date: str,
        sorted_dates: list[str],
        price_map: dict[str, float],
    ) -> float | None:
        """Find the price at or before a target date using binary search."""
        if target_date in price_map:
            return price_map[target_date]

        # Binary search for closest prior date
        left, right = 0, len(sorted_dates) - 1
        closest_date: str | None = None

        while left <= right:
            mid = (left + right) // 2
            mid_date = sorted_dates[mid]

            if mid_date <= target_date:
                closest_date = mid_date
                left = mid + 1
            else:
                right = mid - 1

        return price_map.get(closest_date) if closest_date else None

    def _filter_statements(
        self,
        statements: list[JQuantsStatement],
        period_type: str,
        from_date: str | None,
        to_date: str | None,
    ) -> list[JQuantsStatement]:
        """Filter statements by date range and period type."""
        filtered = []
        normalized_period_type = normalize_period_type(period_type)

        for stmt in statements:
            # Filter by period type
            stmt_period_type = normalize_period_type(stmt.CurPerType)
            if normalized_period_type != "all" and stmt_period_type != normalized_period_type:
                continue

            # Filter by date range
            period_end = stmt.CurPerEn

            if from_date and period_end < from_date:
                continue

            if to_date and period_end > to_date:
                continue

            filtered.append(stmt)

        return filtered

    def _calculate_all_metrics(
        self,
        stmt: JQuantsStatement,
        price_map: dict[str, float],
        prefer_consolidated: bool,
    ) -> FundamentalDataPoint:
        """Calculate all fundamental metrics for a statement."""
        stock_price = price_map.get(stmt.DiscDate)

        # Calculate core metrics
        eps = self._calculate_eps(stmt, prefer_consolidated)
        diluted_eps = stmt.DEPS
        bps = self._calculate_bps(stmt, prefer_consolidated)

        # Calculate ROE
        roe = self._calculate_roe(stmt, prefer_consolidated)

        # Calculate valuation metrics
        per = self._calculate_per(eps, stock_price)
        pbr = self._calculate_pbr(bps, stock_price)

        # Calculate profitability metrics
        roa = self._calculate_roa(stmt, prefer_consolidated)
        operating_margin = self._calculate_operating_margin(stmt, prefer_consolidated)
        net_margin = self._calculate_net_margin(stmt, prefer_consolidated)

        # Get raw financial data
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        equity = self._get_equity(stmt, prefer_consolidated)
        total_assets = self._get_total_assets(stmt, prefer_consolidated)
        net_sales = self._get_net_sales(stmt, prefer_consolidated)
        operating_profit = self._get_operating_profit(stmt, prefer_consolidated)
        cfo_to_net_profit_ratio = self._calculate_cfo_to_net_profit_ratio(
            stmt.CFO, net_profit
        )

        # Calculate FCF metrics
        fcf = self._calculate_simple_fcf(stmt.CFO, stmt.CFI)
        fcf_yield = self._calculate_fcf_yield(
            fcf, stock_price, stmt.ShOutFY, stmt.TrShFY
        )
        fcf_margin = self._calculate_fcf_margin(fcf, net_sales)
        cfo_yield = self._calculate_cfo_yield(
            stmt.CFO, stock_price, stmt.ShOutFY, stmt.TrShFY
        )
        cfo_margin = self._calculate_cfo_margin(stmt.CFO, net_sales)

        # Get forecast EPS
        forecast_eps, forecast_eps_change_rate = self._get_forecast_eps(
            stmt, eps, prefer_consolidated
        )
        dividend_fy = self._round_or_none(stmt.DivAnn)

        normalized_period_type = normalize_period_type(stmt.CurPerType)

        return FundamentalDataPoint(
            date=stmt.CurPerEn,
            disclosedDate=stmt.DiscDate,
            periodType=normalized_period_type or stmt.CurPerType,
            isConsolidated=self._is_consolidated_statement(stmt),
            accountingStandard=self._get_accounting_standard(stmt),
            roe=self._round_or_none(roe),
            eps=self._round_or_none(eps),
            dilutedEps=self._round_or_none(diluted_eps),
            bps=self._round_or_none(bps),
            dividendFy=dividend_fy,
            adjustedDividendFy=dividend_fy,
            per=self._round_or_none(per),
            pbr=self._round_or_none(pbr),
            roa=self._round_or_none(roa),
            operatingMargin=self._round_or_none(operating_margin),
            netMargin=self._round_or_none(net_margin),
            stockPrice=stock_price,
            netProfit=self._to_millions(net_profit),
            equity=self._to_millions(equity),
            totalAssets=self._to_millions(total_assets),
            netSales=self._to_millions(net_sales),
            operatingProfit=self._to_millions(operating_profit),
            cashFlowOperating=self._to_millions(stmt.CFO),
            cashFlowInvesting=self._to_millions(stmt.CFI),
            cashFlowFinancing=self._to_millions(stmt.CFF),
            cashAndEquivalents=self._to_millions(stmt.CashEq),
            fcf=self._to_millions(self._round_or_none(fcf)),
            fcfYield=self._round_or_none(fcf_yield),
            fcfMargin=self._round_or_none(fcf_margin),
            cfoYield=self._round_or_none(cfo_yield),
            cfoMargin=self._round_or_none(cfo_margin),
            cfoToNetProfitRatio=self._round_or_none(cfo_to_net_profit_ratio),
            tradingValueToMarketCapRatio=None,
            forecastEps=self._round_or_none(forecast_eps),
            forecastEpsChangeRate=self._round_or_none(forecast_eps_change_rate),
            revisedForecastEps=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )

    # ===== Metric Calculation Methods =====

    def _calculate_eps(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Calculate EPS."""
        return self._get_value_with_fallback(stmt.EPS, stmt.NCEPS, prefer_consolidated)

    def _calculate_bps(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Calculate BPS."""
        return self._get_value_with_fallback(stmt.BPS, stmt.NCBPS, prefer_consolidated)

    def _calculate_roe(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Calculate ROE with annualization for quarterly data."""
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        equity = self._get_equity(stmt, prefer_consolidated)

        if net_profit is None or equity is None or equity <= 0:
            return None

        # Annualize quarterly profit
        adjusted_profit = net_profit
        period_type = normalize_period_type(stmt.CurPerType)
        if period_type in ("1Q", "2Q", "3Q"):
            adjusted_profit = self._annualize_quarterly_profit(
                net_profit, period_type
            )

        return (adjusted_profit / equity) * 100

    def _annualize_quarterly_profit(
        self, quarterly_profit: float, period_type: str
    ) -> float:
        """Annualize quarterly profit figures."""
        normalized = normalize_period_type(period_type)
        multipliers = {"1Q": 4.0, "2Q": 2.0, "3Q": 4.0 / 3.0}
        if normalized is None:
            return quarterly_profit
        return quarterly_profit * multipliers.get(normalized, 1.0)

    def _calculate_roa(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Calculate ROA."""
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        total_assets = self._get_total_assets(stmt, prefer_consolidated)

        if net_profit is None or total_assets is None or total_assets <= 0:
            return None

        return (net_profit / total_assets) * 100

    def _calculate_operating_margin(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Calculate operating margin."""
        operating_profit = self._get_operating_profit(stmt, prefer_consolidated)
        net_sales = self._get_net_sales(stmt, prefer_consolidated)

        if operating_profit is None or net_sales is None or net_sales <= 0:
            return None

        return (operating_profit / net_sales) * 100

    def _calculate_net_margin(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Calculate net margin."""
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        net_sales = self._get_net_sales(stmt, prefer_consolidated)

        if net_profit is None or net_sales is None or net_sales <= 0:
            return None

        return (net_profit / net_sales) * 100

    def _calculate_per(
        self, eps: float | None, stock_price: float | None
    ) -> float | None:
        """Calculate PER."""
        if eps is None or stock_price is None or eps == 0:
            return None
        return stock_price / eps

    def _calculate_pbr(
        self, bps: float | None, stock_price: float | None
    ) -> float | None:
        """Calculate PBR."""
        if bps is None or stock_price is None or bps <= 0:
            return None
        return stock_price / bps

    def _calculate_simple_fcf(
        self, cfo: float | None, cfi: float | None
    ) -> float | None:
        """Calculate simple FCF = CFO + CFI."""
        if cfo is None or cfi is None:
            return None
        return cfo + cfi

    def _calculate_fcf_yield(
        self,
        fcf: float | None,
        stock_price: float | None,
        shares_outstanding: float | None,
        treasury_shares: float | None,
    ) -> float | None:
        """Calculate FCF yield = (FCF / Market Cap) * 100."""
        if (
            fcf is None
            or stock_price is None
            or shares_outstanding is None
            or stock_price <= 0
        ):
            return None

        market_cap = calc_market_cap_scalar(stock_price, shares_outstanding, treasury_shares)
        if market_cap is None:
            return None
        return (fcf / market_cap) * 100

    def _calculate_fcf_margin(
        self, fcf: float | None, net_sales: float | None
    ) -> float | None:
        """Calculate FCF margin = (FCF / Net Sales) * 100."""
        if fcf is None or net_sales is None or net_sales <= 0:
            return None
        return (fcf / net_sales) * 100

    def _calculate_cfo_yield(
        self,
        cfo: float | None,
        stock_price: float | None,
        shares_outstanding: float | None,
        treasury_shares: float | None,
    ) -> float | None:
        """Calculate CFO yield = (CFO / Market Cap) * 100."""
        if (
            cfo is None
            or stock_price is None
            or shares_outstanding is None
            or stock_price <= 0
        ):
            return None

        market_cap = calc_market_cap_scalar(stock_price, shares_outstanding, treasury_shares)
        if market_cap is None:
            return None
        return (cfo / market_cap) * 100

    def _calculate_cfo_margin(
        self, cfo: float | None, net_sales: float | None
    ) -> float | None:
        """Calculate CFO margin = (CFO / Net Sales) * 100."""
        if cfo is None or net_sales is None or net_sales <= 0:
            return None
        return (cfo / net_sales) * 100

    def _calculate_cfo_to_net_profit_ratio(
        self, cfo: float | None, net_profit: float | None
    ) -> float | None:
        """Calculate operating cash flow to net profit ratio."""
        if cfo is None or net_profit is None or net_profit == 0:
            return None
        return cfo / net_profit

    def _calculate_daily_market_cap_to_trading_value_ratio(
        self,
        daily_ohlcv: pd.DataFrame,
        daily_valuation: list[DailyValuationDataPoint],
        period: int,
    ) -> dict[str, float]:
        """Calculate daily market cap / N-day average trading value ratio."""
        if (
            daily_ohlcv.empty
            or "Close" not in daily_ohlcv.columns
            or "Volume" not in daily_ohlcv.columns
        ):
            return {}

        market_cap_map = {
            item.date: item.marketCap
            for item in daily_valuation
            if item.marketCap is not None and item.marketCap > 0
        }
        if not market_cap_map:
            return {}

        close = pd.to_numeric(daily_ohlcv["Close"], errors="coerce")
        volume = pd.to_numeric(daily_ohlcv["Volume"], errors="coerce")
        trading_value = close * volume
        rolling_mean = trading_value.rolling(window=period, min_periods=period).mean()

        ratio_map: dict[str, float] = {}
        for timestamp, avg_trading_value in rolling_mean.items():
            if pd.isna(avg_trading_value):
                continue
            date_str = timestamp.strftime("%Y-%m-%d")
            market_cap = market_cap_map.get(date_str)
            if market_cap is None or market_cap <= 0:
                continue
            if avg_trading_value <= 0:
                continue
            rounded = self._round_ratio_or_none(float(market_cap / avg_trading_value))
            if rounded is not None:
                ratio_map[date_str] = rounded

        return ratio_map

    def _apply_trading_value_ratio_to_statements(
        self,
        data: list[FundamentalDataPoint],
        daily_ratio_map: dict[str, float],
    ) -> list[FundamentalDataPoint]:
        """Apply trading value ratio to each statement using disclosed date alignment."""
        if not data or not daily_ratio_map:
            return data

        sorted_dates = sorted(daily_ratio_map.keys())
        updated_data: list[FundamentalDataPoint] = []
        for item in data:
            ratio = self._find_price_at_date(item.disclosedDate, sorted_dates, daily_ratio_map)
            updated_data.append(
                FundamentalDataPoint(
                    **{
                        **item.model_dump(),
                        "tradingValueToMarketCapRatio": self._round_ratio_or_none(ratio),
                    }
                )
            )

        return updated_data

    def _apply_latest_trading_value_ratio(
        self,
        metrics: FundamentalDataPoint | None,
        daily_ratio_map: dict[str, float],
    ) -> FundamentalDataPoint | None:
        """Apply latest available trading value ratio to latest metrics."""
        if metrics is None or not daily_ratio_map:
            return metrics

        latest_ratio = daily_ratio_map[max(daily_ratio_map.keys())]
        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "tradingValueToMarketCapRatio": latest_ratio,
            }
        )

    # ===== Financial Data Extraction Methods =====

    def _get_value_with_fallback(
        self,
        consolidated: float | None,
        non_consolidated: float | None,
        prefer_consolidated: bool,
    ) -> float | None:
        """Get value with consolidated/non-consolidated fallback.

        Args:
            consolidated: Consolidated value
            non_consolidated: Non-consolidated value
            prefer_consolidated: Whether to prefer consolidated

        Returns:
            Value based on preference with fallback to other type
        """
        if prefer_consolidated:
            return consolidated if consolidated is not None else non_consolidated
        return non_consolidated if non_consolidated is not None else consolidated

    def _get_net_profit(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Get net profit."""
        return self._get_value_with_fallback(stmt.NP, stmt.NCNP, prefer_consolidated)

    def _get_equity(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Get equity."""
        return self._get_value_with_fallback(stmt.Eq, stmt.NCEq, prefer_consolidated)

    def _get_total_assets(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Get total assets."""
        return self._get_value_with_fallback(stmt.TA, stmt.NCTA, prefer_consolidated)

    def _get_net_sales(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Get net sales."""
        return self._get_value_with_fallback(stmt.Sales, stmt.NCSales, prefer_consolidated)

    def _get_operating_profit(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        """Get operating profit."""
        return self._get_value_with_fallback(stmt.OP, stmt.NCOP, prefer_consolidated)

    def _is_consolidated_statement(self, stmt: JQuantsStatement) -> bool:
        """Check if statement is consolidated."""
        doc_type = stmt.DocType.lower()
        if "非連結" in doc_type or "nonconsolidated" in doc_type:
            return False
        if "連結" in doc_type or "consolidated" in doc_type:
            return True
        # Default: check if consolidated data is available
        return stmt.NP is not None or stmt.Eq is not None

    def _get_accounting_standard(self, stmt: JQuantsStatement) -> str:
        """Extract accounting standard from document type."""
        doc_type = stmt.DocType.lower()

        if "ifrs" in doc_type:
            return "IFRS"
        if "us" in doc_type and "gaap" in doc_type:
            return "US GAAP"
        if "jp" in doc_type or "japanese" in doc_type:
            return "JGAAP"

        return "JGAAP"  # Default for Japanese companies

    def _get_forecast_eps(
        self,
        stmt: JQuantsStatement,
        actual_eps: float | None,
        prefer_consolidated: bool,
    ) -> tuple[float | None, float | None]:
        """Get forecast EPS and calculate change rate.

        Priority logic:
        - FY: NxFEPS (next FY forecast = forward-looking)
        - Q: FEPS (current FY forecast = forward-looking)
        """
        is_fy = stmt.CurPerType == "FY"

        # Select primary/fallback based on period type
        if prefer_consolidated:
            primary, fallback = (stmt.NxFEPS, stmt.FEPS) if is_fy else (stmt.FEPS, stmt.NxFEPS)
        else:
            primary, fallback = (stmt.NxFNCEPS, stmt.FNCEPS) if is_fy else (stmt.FNCEPS, stmt.NxFNCEPS)

        forecast_eps = primary if primary is not None else fallback

        # Calculate change rate
        forecast_eps_change_rate: float | None = None
        if forecast_eps is not None and actual_eps is not None and actual_eps != 0:
            forecast_eps_change_rate = ((forecast_eps - actual_eps) / abs(actual_eps)) * 100

        return forecast_eps, forecast_eps_change_rate

    # ===== Daily Valuation Methods =====

    def _calculate_daily_valuation(
        self,
        statements: list[JQuantsStatement],
        daily_prices: dict[str, float],
        prefer_consolidated: bool,
    ) -> list[DailyValuationDataPoint]:
        """Calculate daily PER/PBR time-series."""
        if not daily_prices:
            return []

        baseline_shares = self._resolve_baseline_shares_from_latest_quarter(statements)
        fy_data_points = self._get_applicable_fy_data(
            statements, prefer_consolidated, baseline_shares
        )
        if not fy_data_points:
            return []

        result: list[DailyValuationDataPoint] = []
        sorted_dates = sorted(daily_prices.keys())

        for date_str in sorted_dates:
            close = daily_prices[date_str]
            applicable_fy = self._find_applicable_fy(fy_data_points, date_str)

            if applicable_fy is None:
                continue

            per = None
            pbr = None
            market_cap = None

            if (
                applicable_fy.eps is not None
                and applicable_fy.eps != 0
            ):
                per = round(close / applicable_fy.eps, 2)

            if applicable_fy.bps is not None and applicable_fy.bps > 0:
                pbr = round(close / applicable_fy.bps, 2)

            if baseline_shares is not None and baseline_shares != 0:
                # baseline_shares は直近FYの発行済み株式数（treasury_shares は含まない）
                market_cap = self._round_or_none(
                    calc_market_cap_scalar(close, baseline_shares)
                )

            result.append(
                DailyValuationDataPoint(
                    date=date_str, close=close, per=per, pbr=pbr, marketCap=market_cap
                )
            )

        return result

    def _get_applicable_fy_data(
        self,
        statements: list[JQuantsStatement],
        prefer_consolidated: bool,
        baseline_shares: float | None,
    ) -> list[FYDataPoint]:
        """Get FY data points sorted by disclosure date for daily valuation."""
        fy_data: list[FYDataPoint] = []

        for stmt in statements:
            if stmt.CurPerType != "FY":
                continue

            eps = self._calculate_eps(stmt, prefer_consolidated)
            bps = self._calculate_bps(stmt, prefer_consolidated)
            adjusted_eps = self._compute_adjusted_value(eps, stmt.ShOutFY, baseline_shares)
            adjusted_bps = self._compute_adjusted_value(bps, stmt.ShOutFY, baseline_shares)
            display_eps = adjusted_eps if adjusted_eps is not None else eps
            display_bps = adjusted_bps if adjusted_bps is not None else bps

            # Exclude forecasts (eps=0/None and bps=0/None)
            if not self._has_valid_valuation_metrics(display_eps, display_bps):
                continue

            fy_data.append(
                FYDataPoint(
                    disclosed_date=stmt.DiscDate,
                    eps=display_eps,
                    bps=display_bps,
                )
            )

        # Sort by disclosure date ascending
        fy_data.sort(key=lambda x: x.disclosed_date)
        return fy_data

    def _has_valid_valuation_metrics(
        self, eps: float | None, bps: float | None
    ) -> bool:
        """Check if valuation metrics are valid."""
        eps_valid = eps is not None and eps != 0
        bps_valid = bps is not None and bps > 0
        return eps_valid or bps_valid

    def _find_applicable_fy(
        self, fy_data_points: list[FYDataPoint], date_str: str
    ) -> FYDataPoint | None:
        """Find the most recent FY data disclosed before or on a given date."""
        applicable_fy: FYDataPoint | None = None

        for fy in fy_data_points:
            if fy.disclosed_date <= date_str:
                applicable_fy = fy
            else:
                break  # Data is sorted, so we can stop early

        return applicable_fy

    # ===== Latest Metrics Methods =====

    def _find_latest_with_actual_data(
        self, data: list[FundamentalDataPoint]
    ) -> FundamentalDataPoint | None:
        """Find the first data point with actual financial data."""
        for d in data:
            if self._has_actual_financial_data(d):
                return d
        return None

    def _has_actual_financial_data(self, data: FundamentalDataPoint) -> bool:
        """Check if data point has actual financial data (not just forecast)."""
        return (
            data.roe is not None
            or (data.eps is not None and data.eps != 0)
            or data.netProfit is not None
            or data.equity is not None
        )

    def _update_latest_with_daily_valuation(
        self,
        metrics: FundamentalDataPoint | None,
        daily_valuation: list[DailyValuationDataPoint],
        data: list[FundamentalDataPoint],
    ) -> FundamentalDataPoint | None:
        """Update latest metrics with daily valuation data."""
        if metrics is None:
            return None

        if not daily_valuation:
            return self._apply_fy_data_to_metrics(metrics, data)

        latest_daily = daily_valuation[-1]

        # Find latest FY with actual data
        latest_fy = next(
            (d for d in data if d.periodType == "FY" and self._has_actual_financial_data(d)),
            None,
        )

        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "per": latest_daily.per,
                "pbr": latest_daily.pbr,
                "stockPrice": latest_daily.close,
                "eps": latest_fy.eps if latest_fy else metrics.eps,
                "bps": latest_fy.bps if latest_fy else metrics.bps,
            }
        )

    def _apply_fy_data_to_metrics(
        self,
        metrics: FundamentalDataPoint,
        data: list[FundamentalDataPoint],
    ) -> FundamentalDataPoint:
        """Apply FY EPS/BPS to metrics for PER/PBR calculation (fallback)."""
        latest_fy = next(
            (d for d in data if d.periodType == "FY" and self._has_actual_financial_data(d)),
            None,
        )

        if latest_fy is None or latest_fy.eps is None or latest_fy.eps == 0:
            return metrics

        fy_per = None
        fy_pbr = None

        if metrics.stockPrice is not None:
            fy_per = round(metrics.stockPrice / latest_fy.eps, 2)

            if latest_fy.bps is not None and latest_fy.bps > 0:
                fy_pbr = round(metrics.stockPrice / latest_fy.bps, 2)

        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "per": fy_per,
                "pbr": fy_pbr,
                "eps": latest_fy.eps,
                "bps": latest_fy.bps,
            }
        )

    def _enhance_latest_metrics(
        self,
        metrics: FundamentalDataPoint | None,
        statements: list[JQuantsStatement],
        prefer_consolidated: bool,
    ) -> FundamentalDataPoint | None:
        """Enhance latest metrics with forecast EPS and previous period CF."""
        if metrics is None:
            return None

        # Sort statements by period end date descending
        sorted_statements = sorted(
            statements, key=lambda s: s.CurPerEn, reverse=True
        )

        # Find the statement corresponding to latestMetrics
        current_statement = next(
            (s for s in sorted_statements if s.CurPerEn == metrics.date), None
        )

        # Get forecast EPS from current statement
        forecast_eps, forecast_eps_change_rate = self._get_forecast_eps(
            current_statement, metrics.eps, prefer_consolidated
        ) if current_statement else (None, None)

        # Find previous period CF data
        prev_cf = self._get_previous_period_cash_flow(
            metrics.date, metrics.periodType, sorted_statements
        )

        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "forecastEps": self._round_or_none(forecast_eps),
                "forecastEpsChangeRate": self._round_or_none(forecast_eps_change_rate),
                **prev_cf,
            }
        )

    def _get_previous_period_cash_flow(
        self,
        current_date: str,
        period_type: str,
        statements: list[JQuantsStatement],
    ) -> dict[str, float | None]:
        """Get cash flow data from previous period (same type, one year earlier)."""
        try:
            current_dt = datetime.fromisoformat(current_date)
            target_dt = current_dt.replace(year=current_dt.year - 1)
        except ValueError:
            return _EMPTY_PREV_CASH_FLOW

        normalized_period_type = normalize_period_type(period_type)
        # Find statement with same period type from ~1 year earlier
        for stmt in statements:
            stmt_period_type = normalize_period_type(stmt.CurPerType)
            if stmt_period_type != normalized_period_type:
                continue

            try:
                stmt_dt = datetime.fromisoformat(stmt.CurPerEn)
                # Allow +-45 days tolerance
                days_diff = abs((stmt_dt - target_dt).days)
                if days_diff < 45:
                    return {
                        "prevCashFlowOperating": self._to_millions(stmt.CFO),
                        "prevCashFlowInvesting": self._to_millions(stmt.CFI),
                        "prevCashFlowFinancing": self._to_millions(stmt.CFF),
                        "prevCashAndEquivalents": self._to_millions(stmt.CashEq),
                    }
            except ValueError:
                continue

        return _EMPTY_PREV_CASH_FLOW

    def _annotate_latest_fy_with_revision(
        self,
        data: list[FundamentalDataPoint],
        latest_metrics: FundamentalDataPoint | None,
        statements: list[JQuantsStatement],
        prefer_consolidated: bool,
    ) -> None:
        """Annotate latest FY with revised forecast from latest Q."""
        if latest_metrics is None:
            return

        # Find latest FY in data
        latest_fy_idx = next(
            (
                i
                for i, d in enumerate(data)
                if d.periodType == "FY" and self._has_actual_financial_data(d)
            ),
            None,
        )

        if latest_fy_idx is None:
            return

        latest_fy = data[latest_fy_idx]

        # Find latest Q statement newer than latest FY
        q_statements = [
            s
            for s in statements
            if normalize_period_type(s.CurPerType) in ("1Q", "2Q", "3Q")
        ]
        q_statements.sort(key=lambda s: s.DiscDate, reverse=True)

        if not q_statements:
            return

        latest_q = q_statements[0]

        # Ensure Q is disclosed after latest FY
        if latest_q.DiscDate <= latest_fy.disclosedDate:
            return

        # Get Q's forecast EPS (FEPS = current FY in progress)
        q_forecast = (
            latest_q.FEPS if prefer_consolidated else latest_q.FNCEPS
        )

        if q_forecast is None:
            return

        rounded_q_forecast = round(q_forecast, 2)

        # Annotate when Q forecast differs from FY forecast
        if latest_fy.forecastEps is None or rounded_q_forecast != latest_fy.forecastEps:
            # Create a new FundamentalDataPoint with revised forecast
            updated_fy = FundamentalDataPoint(
                **{
                    **latest_fy.model_dump(),
                    "revisedForecastEps": rounded_q_forecast,
                    "revisedForecastSource": normalize_period_type(
                        latest_q.CurPerType
                    )
                    or latest_q.CurPerType,
                }
            )
            data[latest_fy_idx] = updated_fy

    # ===== Utility Methods =====

    def _round_or_none(self, value: float | None) -> float | None:
        """Round to 2 decimal places or return None."""
        if value is None:
            return None
        return round(value, 2)

    def _round_ratio_or_none(self, value: float | None) -> float | None:
        """Round ratio values with higher precision to avoid over-rounding."""
        if value is None:
            return None
        return round(value, 6)

    def _to_millions(self, value: float | None) -> float | None:
        """Convert JPY to millions of JPY."""
        if value is None:
            return None
        return value / 1_000_000


# Global service instance
fundamentals_service = FundamentalsService()
