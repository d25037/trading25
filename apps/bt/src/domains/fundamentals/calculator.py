"""Fundamentals calculation domain engine."""

from __future__ import annotations

import math
from datetime import datetime

import pandas as pd

from src.infrastructure.external_api.jquants_client import JQuantsStatement
from src.shared.models.types import normalize_period_type
from src.shared.utils.financial import calc_market_cap_scalar
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    ShareCountSnapshot,
)
from src.shared.utils.statement_document import is_actual_fy_financial_statement

from .models import (
    DailyValuationDataPoint,
    EMPTY_PREV_CASH_FLOW,
    FYDataPoint,
    FundamentalDataPoint,
)
from .daily_valuation import (
    calculate_daily_valuation as _calculate_daily_valuation_impl,
    find_applicable_fy as _find_applicable_fy_impl,
    get_applicable_fy_data as _get_applicable_fy_data_impl,
    has_valid_valuation_metrics as _has_valid_valuation_metrics_impl,
    resolve_forward_eps_for_daily_valuation as _resolve_forward_eps_for_daily_valuation_impl,
    resolve_forward_operating_profit_for_daily_valuation as _resolve_forward_operating_profit_for_daily_valuation_impl,
)
from . import share_adjustments as _share_adjustments
from . import forecast_eps_comparison as _forecast_eps_comparison
from .valuation_primitives import valuation_ratio


class FundamentalsCalculator:
    """Pure(ish) fundamentals computation logic."""

    @staticmethod
    def _is_valid_share_metric(value: float | None, *, allow_zero: bool = False) -> bool:
        return _share_adjustments.is_valid_share_metric(value, allow_zero=allow_zero)

    def _build_shares_map(
        self, statements: list[JQuantsStatement]
    ) -> dict[tuple[str, str, str | None], float | None]:
        return _share_adjustments.build_shares_map(statements)

    def _get_shares_for_datapoint(
        self,
        data_point: FundamentalDataPoint,
        shares_map: dict[tuple[str, str, str | None], float | None],
    ) -> float | None:
        return _share_adjustments.get_shares_for_datapoint(data_point, shares_map)

    def _resolve_baseline_shares_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> float | None:
        return _share_adjustments.resolve_baseline_shares_from_latest_quarter(statements)

    def _resolve_baseline_share_snapshot_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> ShareCountSnapshot | None:
        return _share_adjustments.resolve_baseline_share_snapshot_from_latest_quarter(statements)

    def _resolve_latest_treasury_shares_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> float | None:
        return _share_adjustments.resolve_latest_treasury_shares_from_latest_quarter(statements)

    def _resolve_latest_treasury_share_snapshot_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> ShareCountSnapshot | None:
        return _share_adjustments.resolve_latest_treasury_share_snapshot_from_latest_quarter(
            statements
        )

    def _adjust_snapshot_shares_to_price_basis(
        self,
        snapshot: ShareCountSnapshot | None,
        share_adjustment_events: list[ShareAdjustmentEvent],
        *,
        through_date: str | None,
        allow_zero: bool = False,
    ) -> float | None:
        return _share_adjustments.adjust_snapshot_shares_to_price_basis(
            snapshot,
            share_adjustment_events,
            through_date=through_date,
            allow_zero=allow_zero,
        )

    def _compute_adjusted_value(
        self,
        value: float | None,
        current_shares: float | None,
        base_shares: float | None,
    ) -> float | None:
        return _share_adjustments.compute_adjusted_value(
            value,
            current_shares,
            base_shares,
            round_or_none=self._round_or_none,
        )

    def _build_adjusted_datapoint(
        self,
        item: FundamentalDataPoint,
        eps_shares: float | None,
        bps_shares: float | None,
        forecast_shares: float | None,
        dividend_shares: float | None,
        base_shares: float | None,
    ) -> FundamentalDataPoint:
        return _share_adjustments.build_adjusted_datapoint(
            item,
            eps_shares,
            bps_shares,
            forecast_shares,
            dividend_shares,
            base_shares,
            round_or_none=self._round_or_none,
            calculate_per=self._calculate_per,
            calculate_pbr=self._calculate_pbr,
            calculate_change_rate=self._calculate_change_rate,
        )

    def _apply_share_adjustments(
        self,
        data: list[FundamentalDataPoint],
        statements: list[JQuantsStatement],
        latest_metrics: FundamentalDataPoint | None,
        share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
        through_date: str | None = None,
    ) -> tuple[list[FundamentalDataPoint], FundamentalDataPoint | None]:
        return _share_adjustments.apply_share_adjustments(
            data,
            statements,
            latest_metrics,
            share_adjustment_events=share_adjustment_events,
            through_date=through_date,
            round_or_none=self._round_or_none,
            calculate_per=self._calculate_per,
            calculate_pbr=self._calculate_pbr,
            calculate_change_rate=self._calculate_change_rate,
            has_actual_financial_data=self._has_actual_financial_data,
        )

    def _apply_adjusted_to_latest_metrics(
        self,
        metrics: FundamentalDataPoint | None,
        data: list[FundamentalDataPoint],
        shares_map: dict[tuple[str, str, str | None], float | None],
        base_shares: float | None,
    ) -> FundamentalDataPoint | None:
        return _share_adjustments.apply_adjusted_to_latest_metrics(
            metrics,
            data,
            shares_map,
            base_shares,
            round_or_none=self._round_or_none,
            calculate_per=self._calculate_per,
            calculate_pbr=self._calculate_pbr,
            calculate_change_rate=self._calculate_change_rate,
            has_actual_financial_data=self._has_actual_financial_data,
        )

    def _to_daily_close_map(self, daily_ohlcv: pd.DataFrame) -> dict[str, float]:
        if daily_ohlcv.empty or "Close" not in daily_ohlcv.columns:
            return {}
        close_series: pd.Series[float] = daily_ohlcv["Close"]
        return {k.strftime("%Y-%m-%d"): v for k, v in close_series.items()}

    def calculate_latest_valuation(
        self,
        statements: list[JQuantsStatement],
        *,
        close: float,
        price_date: str,
        prefer_consolidated: bool,
        share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
        price_basis_date: str | None = None,
    ) -> DailyValuationDataPoint | None:
        values = self._calculate_daily_valuation(
            statements,
            {price_date: close},
            prefer_consolidated,
            share_adjustment_events=share_adjustment_events,
            price_basis_date=price_basis_date,
        )
        return values[-1] if values else None

    def _get_stock_prices_for_statements(
        self,
        statements: list[JQuantsStatement],
        daily_prices: dict[str, float] | None = None,
    ) -> dict[str, float]:
        if not statements:
            return {}
        if not daily_prices:
            return {}

        result: dict[str, float] = {}
        sorted_dates = sorted(daily_prices.keys())
        for stmt in statements:
            price = self._find_price_at_date(stmt.DiscDate, sorted_dates, daily_prices)
            if price is not None:
                result[stmt.DiscDate] = price
        return result

    def _find_price_at_date(
        self,
        target_date: str,
        sorted_dates: list[str],
        price_map: dict[str, float],
    ) -> float | None:
        if target_date in price_map:
            return price_map[target_date]

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
        filtered = []
        normalized_period_type = normalize_period_type(period_type)

        for stmt in statements:
            stmt_period_type = normalize_period_type(stmt.CurPerType)
            if normalized_period_type != "all" and stmt_period_type != normalized_period_type:
                continue
            if normalized_period_type == "FY" and not self._is_actual_fy_statement(stmt):
                continue
            period_end = stmt.CurPerEn
            if from_date and period_end < from_date:
                continue
            if to_date and period_end > to_date:
                continue
            filtered.append(stmt)

        return filtered

    def _is_actual_fy_statement(self, stmt: JQuantsStatement) -> bool:
        return is_actual_fy_financial_statement(
            stmt.CurPerType,
            stmt.DocType,
            allow_unknown_document=True,
        )

    def _calculate_all_metrics(
        self,
        stmt: JQuantsStatement,
        price_map: dict[str, float],
        prefer_consolidated: bool,
    ) -> FundamentalDataPoint:
        stock_price = price_map.get(stmt.DiscDate)
        eps = self._calculate_eps(stmt, prefer_consolidated)
        diluted_eps = stmt.DEPS
        bps = self._calculate_bps(stmt, prefer_consolidated)
        roe = self._calculate_roe(stmt, prefer_consolidated)
        per = self._calculate_per(eps, stock_price)
        pbr = self._calculate_pbr(bps, stock_price)
        roa = self._calculate_roa(stmt, prefer_consolidated)
        operating_margin = self._calculate_operating_margin(stmt, prefer_consolidated)
        net_margin = self._calculate_net_margin(stmt, prefer_consolidated)
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        equity = self._get_equity(stmt, prefer_consolidated)
        total_assets = self._get_total_assets(stmt, prefer_consolidated)
        net_sales = self._get_net_sales(stmt, prefer_consolidated)
        operating_profit = self._get_operating_profit(stmt, prefer_consolidated)
        cfo_to_net_profit_ratio = self._calculate_cfo_to_net_profit_ratio(stmt.CFO, net_profit)
        fcf = self._calculate_simple_fcf(stmt.CFO, stmt.CFI)
        fcf_yield = self._calculate_fcf_yield(fcf, stock_price, stmt.ShOutFY, stmt.TrShFY)
        fcf_margin = self._calculate_fcf_margin(fcf, net_sales)
        cfo_yield = self._calculate_cfo_yield(stmt.CFO, stock_price, stmt.ShOutFY, stmt.TrShFY)
        cfo_margin = self._calculate_cfo_margin(stmt.CFO, net_sales)
        forecast_eps, forecast_eps_change_rate = self._get_forecast_eps(
            stmt, eps, prefer_consolidated
        )
        forecast_operating_profit, forecast_operating_profit_change_rate = (
            self._get_forecast_operating_profit(stmt, operating_profit, prefer_consolidated)
        )
        raw_dividend_fy = self._get_dividend_fy(stmt)
        dividend_fy = self._round_or_none(raw_dividend_fy)
        forecast_dividend_fy, forecast_dividend_fy_change_rate = self._get_forecast_dividend_fy(
            stmt, dividend_fy
        )
        payout_ratio = self._round_or_none(
            self._normalize_payout_ratio(stmt.PayoutRatioAnn, dividend_fy=raw_dividend_fy, eps=eps)
        )
        forecast_payout_ratio, forecast_payout_ratio_change_rate = self._get_forecast_payout_ratio(
            stmt, payout_ratio
        )
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
            forecastDividendFy=self._round_or_none(forecast_dividend_fy),
            adjustedForecastDividendFy=self._round_or_none(forecast_dividend_fy),
            forecastDividendFyChangeRate=self._round_or_none(forecast_dividend_fy_change_rate),
            payoutRatio=payout_ratio,
            forecastPayoutRatio=self._round_or_none(forecast_payout_ratio),
            forecastPayoutRatioChangeRate=self._round_or_none(forecast_payout_ratio_change_rate),
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
            forecastOperatingProfit=self._to_millions(forecast_operating_profit),
            forecastOperatingProfitChangeRate=self._round_or_none(
                forecast_operating_profit_change_rate
            ),
            revisedForecastEps=None,
            revisedForecastOperatingProfit=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )

    def _calculate_eps(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.EPS, stmt.NCEPS, prefer_consolidated)

    def _calculate_bps(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.BPS, stmt.NCBPS, prefer_consolidated)

    def _calculate_roe(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        equity = self._get_equity(stmt, prefer_consolidated)
        if net_profit is None or equity is None or equity <= 0:
            return None
        adjusted_profit = net_profit
        period_type = normalize_period_type(stmt.CurPerType)
        if period_type in ("1Q", "2Q", "3Q"):
            adjusted_profit = self._annualize_quarterly_profit(net_profit, period_type)
        return (adjusted_profit / equity) * 100

    def _annualize_quarterly_profit(self, quarterly_profit: float, period_type: str) -> float:
        normalized = normalize_period_type(period_type)
        multipliers = {"1Q": 4.0, "2Q": 2.0, "3Q": 4.0 / 3.0}
        if normalized is None:
            return quarterly_profit
        return quarterly_profit * multipliers.get(normalized, 1.0)

    def _calculate_roa(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        total_assets = self._get_total_assets(stmt, prefer_consolidated)
        if net_profit is None or total_assets is None or total_assets <= 0:
            return None
        return (net_profit / total_assets) * 100

    def _calculate_operating_margin(
        self, stmt: JQuantsStatement, prefer_consolidated: bool
    ) -> float | None:
        operating_profit = self._get_operating_profit(stmt, prefer_consolidated)
        net_sales = self._get_net_sales(stmt, prefer_consolidated)
        if operating_profit is None or net_sales is None or net_sales <= 0:
            return None
        return (operating_profit / net_sales) * 100

    def _calculate_net_margin(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        net_profit = self._get_net_profit(stmt, prefer_consolidated)
        net_sales = self._get_net_sales(stmt, prefer_consolidated)
        if net_profit is None or net_sales is None or net_sales <= 0:
            return None
        return (net_profit / net_sales) * 100

    def _calculate_per(self, eps: float | None, stock_price: float | None) -> float | None:
        return valuation_ratio(stock_price, eps)

    def _calculate_pbr(self, bps: float | None, stock_price: float | None) -> float | None:
        return valuation_ratio(stock_price, bps)

    def _calculate_simple_fcf(self, cfo: float | None, cfi: float | None) -> float | None:
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
        if fcf is None or stock_price is None or shares_outstanding is None or stock_price <= 0:
            return None
        market_cap = calc_market_cap_scalar(stock_price, shares_outstanding, treasury_shares)
        if market_cap is None:
            return None
        return (fcf / market_cap) * 100

    def _calculate_fcf_margin(self, fcf: float | None, net_sales: float | None) -> float | None:
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
        if cfo is None or stock_price is None or shares_outstanding is None or stock_price <= 0:
            return None
        market_cap = calc_market_cap_scalar(stock_price, shares_outstanding, treasury_shares)
        if market_cap is None:
            return None
        return (cfo / market_cap) * 100

    def _calculate_cfo_margin(self, cfo: float | None, net_sales: float | None) -> float | None:
        if cfo is None or net_sales is None or net_sales <= 0:
            return None
        return (cfo / net_sales) * 100

    def _calculate_cfo_to_net_profit_ratio(self, cfo: float | None, net_profit: float | None) -> float | None:
        if cfo is None or net_profit is None or net_profit == 0:
            return None
        return cfo / net_profit

    def _calculate_daily_market_cap_to_trading_value_ratio(
        self,
        daily_ohlcv: pd.DataFrame,
        daily_valuation: list[DailyValuationDataPoint],
        period: int,
    ) -> dict[str, float]:
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
        if metrics is None or not daily_ratio_map:
            return metrics
        latest_ratio = daily_ratio_map[max(daily_ratio_map.keys())]
        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "tradingValueToMarketCapRatio": latest_ratio,
            }
        )

    def _get_value_with_fallback(
        self,
        consolidated: float | None,
        non_consolidated: float | None,
        prefer_consolidated: bool,
    ) -> float | None:
        if prefer_consolidated:
            return consolidated if consolidated is not None else non_consolidated
        return non_consolidated if non_consolidated is not None else consolidated

    def _get_net_profit(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.NP, stmt.NCNP, prefer_consolidated)

    def _get_equity(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.Eq, stmt.NCEq, prefer_consolidated)

    def _get_total_assets(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.TA, stmt.NCTA, prefer_consolidated)

    def _get_net_sales(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.Sales, stmt.NCSales, prefer_consolidated)

    def _get_operating_profit(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None:
        return self._get_value_with_fallback(stmt.OP, stmt.NCOP, prefer_consolidated)

    def _is_consolidated_statement(self, stmt: JQuantsStatement) -> bool:
        doc_type = stmt.DocType.lower()
        if "非連結" in doc_type or "nonconsolidated" in doc_type:
            return False
        if "連結" in doc_type or "consolidated" in doc_type:
            return True
        return stmt.NP is not None or stmt.Eq is not None

    def _get_accounting_standard(self, stmt: JQuantsStatement) -> str:
        doc_type = stmt.DocType.lower()
        if "ifrs" in doc_type:
            return "IFRS"
        if "us" in doc_type and "gaap" in doc_type:
            return "US GAAP"
        if "jp" in doc_type or "japanese" in doc_type:
            return "JGAAP"
        return "JGAAP"

    def _get_forecast_eps(
        self,
        stmt: JQuantsStatement,
        actual_eps: float | None,
        prefer_consolidated: bool,
    ) -> tuple[float | None, float | None]:
        is_fy = normalize_period_type(stmt.CurPerType) == "FY"
        if prefer_consolidated:
            primary, fallback = (stmt.NxFEPS, stmt.FEPS) if is_fy else (stmt.FEPS, stmt.NxFEPS)
        else:
            primary, fallback = (stmt.NxFNCEPS, stmt.FNCEPS) if is_fy else (stmt.FNCEPS, stmt.NxFNCEPS)
        forecast_eps = primary if primary is not None else fallback
        return forecast_eps, self._calculate_change_rate(actual_eps, forecast_eps)

    def _get_forecast_operating_profit(
        self,
        stmt: JQuantsStatement,
        actual_operating_profit: float | None,
        prefer_consolidated: bool,
    ) -> tuple[float | None, float | None]:
        if not prefer_consolidated:
            return None, None
        is_fy = normalize_period_type(stmt.CurPerType) == "FY"
        primary, fallback = (stmt.NxFOP, stmt.FOP) if is_fy else (stmt.FOP, stmt.NxFOP)
        forecast_operating_profit = primary if primary is not None else fallback
        return forecast_operating_profit, self._calculate_change_rate(
            actual_operating_profit,
            forecast_operating_profit,
        )

    def _get_dividend_fy(self, stmt: JQuantsStatement) -> float | None:
        return stmt.DivAnn if stmt.DivAnn is not None else stmt.DivFY

    def _get_forecast_dividend_fy(
        self,
        stmt: JQuantsStatement,
        actual_dividend_fy: float | None,
    ) -> tuple[float | None, float | None]:
        is_fy = normalize_period_type(stmt.CurPerType) == "FY"
        current_fy_forecast = stmt.FDivAnn if stmt.FDivAnn is not None else stmt.FDivFY
        next_fy_forecast = stmt.NxFDivAnn if stmt.NxFDivAnn is not None else stmt.NxFDivFY
        primary, fallback = (
            (next_fy_forecast, current_fy_forecast) if is_fy else (current_fy_forecast, next_fy_forecast)
        )
        forecast_dividend_fy = primary if primary is not None else fallback
        return forecast_dividend_fy, self._calculate_change_rate(actual_dividend_fy, forecast_dividend_fy)

    def _get_forecast_payout_ratio(
        self,
        stmt: JQuantsStatement,
        actual_payout_ratio: float | None,
    ) -> tuple[float | None, float | None]:
        is_fy = normalize_period_type(stmt.CurPerType) == "FY"
        current_fy_forecast = self._normalize_payout_ratio(
            stmt.FPayoutRatioAnn,
            dividend_fy=stmt.FDivAnn if stmt.FDivAnn is not None else stmt.FDivFY,
            eps=stmt.FEPS,
        )
        next_fy_forecast = self._normalize_payout_ratio(
            stmt.NxFPayoutRatioAnn,
            dividend_fy=stmt.NxFDivAnn if stmt.NxFDivAnn is not None else stmt.NxFDivFY,
            eps=stmt.NxFEPS,
        )
        primary, fallback = (
            (next_fy_forecast, current_fy_forecast) if is_fy else (current_fy_forecast, next_fy_forecast)
        )
        forecast_payout_ratio = primary if primary is not None else fallback
        return forecast_payout_ratio, self._calculate_change_rate(actual_payout_ratio, forecast_payout_ratio)

    def _normalize_payout_ratio(
        self,
        payout_ratio: float | None,
        *,
        dividend_fy: float | None = None,
        eps: float | None = None,
    ) -> float | None:
        if payout_ratio is None:
            return None
        if not math.isfinite(payout_ratio):
            return None
        normalized = float(payout_ratio)

        reference_percent: float | None = None
        if (
            dividend_fy is not None
            and eps is not None
            and eps != 0
            and math.isfinite(dividend_fy)
            and math.isfinite(eps)
        ):
            reference_percent = (dividend_fy / eps) * 100

        if reference_percent is not None and math.isfinite(reference_percent):
            direct_error = abs(normalized - reference_percent)
            scaled_error = abs((normalized * 100) - reference_percent)
            if scaled_error < direct_error:
                return normalized * 100
            return normalized

        if abs(normalized) <= 1:
            return normalized * 100
        return normalized

    def _calculate_daily_valuation(
        self,
        statements: list[JQuantsStatement],
        daily_prices: dict[str, float],
        prefer_consolidated: bool,
        share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
        price_basis_date: str | None = None,
    ) -> list[DailyValuationDataPoint]:
        return _calculate_daily_valuation_impl(
            self,
            statements,
            daily_prices,
            prefer_consolidated,
            share_adjustment_events=share_adjustment_events,
            price_basis_date=price_basis_date,
        )

    def _get_applicable_fy_data(
        self,
        statements: list[JQuantsStatement],
        prefer_consolidated: bool,
        baseline_shares: float | None,
    ) -> list[FYDataPoint]:
        return _get_applicable_fy_data_impl(
            self, statements, prefer_consolidated, baseline_shares
        )

    def _has_valid_valuation_metrics(self, eps: float | None, bps: float | None) -> bool:
        return _has_valid_valuation_metrics_impl(eps, bps)

    def _resolve_forward_eps_for_daily_valuation(
        self,
        statements: list[JQuantsStatement],
        applicable_fy: FYDataPoint,
        prefer_consolidated: bool,
        baseline_shares: float | None,
        date_str: str,
    ) -> tuple[float | None, str | None, str | None]:
        return _resolve_forward_eps_for_daily_valuation_impl(
            self,
            statements,
            applicable_fy,
            prefer_consolidated,
            baseline_shares,
            date_str,
        )

    def _resolve_forward_operating_profit_for_daily_valuation(
        self,
        statements: list[JQuantsStatement],
        applicable_fy: FYDataPoint,
        prefer_consolidated: bool,
        date_str: str,
    ) -> float | None:
        return _resolve_forward_operating_profit_for_daily_valuation_impl(
            self,
            statements,
            applicable_fy,
            prefer_consolidated,
            date_str,
        )

    def _find_applicable_fy(self, fy_data_points: list[FYDataPoint], date_str: str) -> FYDataPoint | None:
        return _find_applicable_fy_impl(fy_data_points, date_str)

    def _find_latest_with_actual_data(self, data: list[FundamentalDataPoint]) -> FundamentalDataPoint | None:
        for d in data:
            if self._has_actual_financial_data(d):
                return d
        return None

    def _has_actual_financial_data(self, data: FundamentalDataPoint) -> bool:
        return data.roe is not None or (data.eps is not None and data.eps != 0) or data.netProfit is not None or data.equity is not None

    def _update_latest_with_daily_valuation(
        self,
        metrics: FundamentalDataPoint | None,
        daily_valuation: list[DailyValuationDataPoint],
        data: list[FundamentalDataPoint],
    ) -> FundamentalDataPoint | None:
        if metrics is None:
            return None
        if not daily_valuation:
            return self._apply_fy_data_to_metrics(metrics, data)
        latest_daily = daily_valuation[-1]
        latest_fy = next((d for d in data if d.periodType == "FY" and self._has_actual_financial_data(d)), None)
        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "per": latest_daily.per,
                "forwardPer": latest_daily.forwardPer,
                "psr": latest_daily.psr,
                "forwardPsr": latest_daily.forwardPsr,
                "pOp": latest_daily.pOp,
                "forwardPOp": latest_daily.forwardPOp,
                "pbr": latest_daily.pbr,
                "stockPrice": latest_daily.close,
                "marketCap": latest_daily.marketCap,
                "freeFloatMarketCap": latest_daily.freeFloatMarketCap,
                "eps": latest_fy.eps if latest_fy else metrics.eps,
                "bps": latest_fy.bps if latest_fy else metrics.bps,
            }
        )

    def _apply_fy_data_to_metrics(self, metrics: FundamentalDataPoint, data: list[FundamentalDataPoint]) -> FundamentalDataPoint:
        latest_fy = next((d for d in data if d.periodType == "FY" and self._has_actual_financial_data(d)), None)
        if latest_fy is None:
            return metrics

        fy_per = None
        fy_pbr = None
        if metrics.stockPrice is not None:
            fy_per = self._round_or_none(
                valuation_ratio(metrics.stockPrice, latest_fy.eps)
            )
            fy_pbr = self._round_or_none(
                valuation_ratio(metrics.stockPrice, latest_fy.bps)
            )

        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "per": fy_per,
                "pbr": fy_pbr,
                "eps": latest_fy.eps,
                "bps": latest_fy.bps,
            }
        )

    def _resolve_display_actual_eps(self, point: FundamentalDataPoint) -> float | None:
        return _forecast_eps_comparison.resolve_display_actual_eps(point)

    def _resolve_display_forecast_eps(self, point: FundamentalDataPoint) -> float | None:
        return _forecast_eps_comparison.resolve_display_forecast_eps(point)

    def _collect_recent_fy_actual_eps_values(
        self,
        data: list[FundamentalDataPoint],
        lookback_fy_count: int,
    ) -> list[float]:
        return _forecast_eps_comparison.collect_recent_fy_actual_eps_values(
            data,
            lookback_fy_count,
        )

    def _calculate_forecast_eps_above_recent_fy_actuals(
        self,
        metrics: FundamentalDataPoint,
        data: list[FundamentalDataPoint],
        lookback_fy_count: int,
    ) -> bool | None:
        return _forecast_eps_comparison.calculate_forecast_eps_above_recent_fy_actuals(
            metrics,
            data,
            lookback_fy_count,
        )

    def _apply_forecast_eps_above_recent_fy_actuals(
        self,
        metrics: FundamentalDataPoint | None,
        data: list[FundamentalDataPoint],
        lookback_fy_count: int,
    ) -> FundamentalDataPoint | None:
        return _forecast_eps_comparison.apply_forecast_eps_above_recent_fy_actuals(
            metrics,
            data,
            lookback_fy_count,
        )

    def _enhance_latest_metrics(
        self,
        metrics: FundamentalDataPoint | None,
        statements: list[JQuantsStatement],
        prefer_consolidated: bool,
    ) -> FundamentalDataPoint | None:
        if metrics is None:
            return None

        sorted_statements = sorted(statements, key=lambda s: (s.CurPerEn, s.DiscDate), reverse=True)
        current_statement = self._select_statement_for_latest_metrics(
            sorted_statements,
            metrics,
            prefer_consolidated,
        )
        forecast_eps, forecast_eps_change_rate = self._get_forecast_eps(current_statement, metrics.eps, prefer_consolidated) if current_statement else (None, None)
        forecast_dividend_fy, forecast_dividend_fy_change_rate = (
            self._get_forecast_dividend_fy(current_statement, metrics.dividendFy) if current_statement else (None, None)
        )
        forecast_payout_ratio, forecast_payout_ratio_change_rate = (
            self._get_forecast_payout_ratio(current_statement, metrics.payoutRatio) if current_statement else (None, None)
        )
        prev_cf = self._get_previous_period_cash_flow(metrics.date, metrics.periodType, sorted_statements)

        return FundamentalDataPoint(
            **{
                **metrics.model_dump(),
                "forecastEps": self._round_or_none(forecast_eps),
                "forecastEpsChangeRate": self._round_or_none(forecast_eps_change_rate),
                "forecastDividendFy": self._round_or_none(forecast_dividend_fy),
                "forecastDividendFyChangeRate": self._round_or_none(forecast_dividend_fy_change_rate),
                "forecastPayoutRatio": self._round_or_none(forecast_payout_ratio),
                "forecastPayoutRatioChangeRate": self._round_or_none(forecast_payout_ratio_change_rate),
                **prev_cf,
            }
        )

    def _select_statement_for_latest_metrics(
        self,
        statements: list[JQuantsStatement],
        metrics: FundamentalDataPoint,
        prefer_consolidated: bool,
    ) -> JQuantsStatement | None:
        normalized_period_type = normalize_period_type(metrics.periodType)
        candidates = [
            stmt
            for stmt in statements
            if stmt.CurPerEn == metrics.date
            and (
                normalized_period_type is None
                or normalize_period_type(stmt.CurPerType) == normalized_period_type
            )
        ]
        if not candidates:
            candidates = [stmt for stmt in statements if stmt.CurPerEn == metrics.date]
        if not candidates:
            return None

        prioritized = sorted(
            candidates,
            key=lambda stmt: (stmt.DiscDate, self._forecast_doc_priority(stmt.DocType)),
            reverse=True,
        )
        for candidate in prioritized:
            forecast_eps, _ = self._get_forecast_eps(candidate, metrics.eps, prefer_consolidated)
            if forecast_eps is not None:
                return candidate
        return prioritized[0]

    def _forecast_doc_priority(self, doc_type: str | None) -> int:
        if doc_type is None:
            return 0
        normalized = doc_type.lower()
        if "earnforecastrevision" in normalized or "業績予想修正" in normalized:
            return 3
        if "financialstatements" in normalized or "決算短信" in normalized or "有価証券報告書" in normalized:
            return 2
        if "dividend" in normalized or "配当予想修正" in normalized:
            return 1
        return 0

    def _get_previous_period_cash_flow(
        self,
        current_date: str,
        period_type: str,
        statements: list[JQuantsStatement],
    ) -> dict[str, float | None]:
        try:
            current_dt = datetime.fromisoformat(current_date)
            target_dt = current_dt.replace(year=current_dt.year - 1)
        except ValueError:
            return EMPTY_PREV_CASH_FLOW

        normalized_period_type = normalize_period_type(period_type)
        for stmt in statements:
            stmt_period_type = normalize_period_type(stmt.CurPerType)
            if stmt_period_type != normalized_period_type:
                continue
            try:
                stmt_dt = datetime.fromisoformat(stmt.CurPerEn)
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
        return EMPTY_PREV_CASH_FLOW

    def _annotate_latest_fy_with_revision(
        self,
        data: list[FundamentalDataPoint],
        latest_metrics: FundamentalDataPoint | None,
        statements: list[JQuantsStatement],
        prefer_consolidated: bool,
        share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
        through_date: str | None = None,
    ) -> None:
        if latest_metrics is None:
            return
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
        q_statements = [
            s
            for s in statements
            if normalize_period_type(s.CurPerType) in ("1Q", "2Q", "3Q")
        ]
        q_statements.sort(key=lambda s: s.DiscDate, reverse=True)
        if not q_statements:
            return

        latest_q = q_statements[0]
        if latest_q.DiscDate <= latest_fy.disclosedDate:
            return
        revision_source = normalize_period_type(latest_q.CurPerType) or latest_q.CurPerType
        update_fields: dict[str, float | str] = {}

        baseline_snapshot = self._resolve_baseline_share_snapshot_from_latest_quarter(statements)
        baseline_shares = self._adjust_snapshot_shares_to_price_basis(
            baseline_snapshot,
            share_adjustment_events or [],
            through_date=through_date,
        )
        q_forecast = latest_q.FEPS if prefer_consolidated else latest_q.FNCEPS
        if q_forecast is not None:
            adjusted_q_forecast = self._compute_adjusted_value(q_forecast, latest_q.ShOutFY, baseline_shares)
            rounded_q_forecast = adjusted_q_forecast if adjusted_q_forecast is not None else round(q_forecast, 2)
            if latest_fy.forecastEps is None or rounded_q_forecast != latest_fy.forecastEps:
                update_fields["revisedForecastEps"] = rounded_q_forecast

        q_forecast_operating_profit = latest_q.FOP if prefer_consolidated else None
        if q_forecast_operating_profit is not None:
            rounded_q_forecast_operating_profit = self._to_millions(q_forecast_operating_profit)
            if (
                rounded_q_forecast_operating_profit is not None
                and (
                    latest_fy.forecastOperatingProfit is None
                    or rounded_q_forecast_operating_profit != latest_fy.forecastOperatingProfit
                )
            ):
                update_fields["revisedForecastOperatingProfit"] = rounded_q_forecast_operating_profit

        if update_fields:
            updated_fy = FundamentalDataPoint(
                **{
                    **latest_fy.model_dump(),
                    **update_fields,
                    "revisedForecastSource": revision_source,
                }
            )
            data[latest_fy_idx] = updated_fy

    def _round_or_none(self, value: float | None) -> float | None:
        if value is None:
            return None
        return round(value, 2)

    def _round_ratio_or_none(self, value: float | None) -> float | None:
        if value is None:
            return None
        return round(value, 6)

    def _to_millions(self, value: float | None) -> float | None:
        if value is None:
            return None
        return value / 1_000_000

    def _calculate_change_rate(self, actual_value: float | None, forecast_value: float | None) -> float | None:
        if actual_value is None or forecast_value is None or actual_value == 0:
            return None
        return ((forecast_value - actual_value) / abs(actual_value)) * 100
