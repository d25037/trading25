"""Fundamentals service (I/O + orchestration only)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, cast
from collections.abc import Iterable

import numpy as np
import pandas as pd
from loguru import logger

from src.application.services.analytics_provenance import build_market_provenance
from src.domains.fundamentals import (
    DailyValuationDataPoint as DomainDailyValuationDataPoint,
    FundamentalDataPoint as DomainFundamentalDataPoint,
    FundamentalsCalculator,
)
from src.entrypoints.http.schemas.analytics_common import ResponseDiagnostics
from src.entrypoints.http.schemas.fundamentals import (
    DailyValuationDataPoint,
    FundamentalDataPoint,
    FundamentalsComputeRequest,
    FundamentalsComputeResponse,
    LatestMetricsSource,
    LatestMetricsSourceItem,
    LiquidityProfile,
    LiquidityProfileWindow,
)
from src.infrastructure.external_api.jquants_client import JQuantsStatement, StockInfo
from src.infrastructure.data_access.clients import DirectMarketClient
from src.shared.utils.share_adjustment import ShareAdjustmentEvent
from src.shared.utils.market_code_alias import normalize_market_scope


class DailyValuationRequiredError(RuntimeError):
    """Raised when fundamentals summary cannot be composed from daily_valuation SoT."""

    reason = "daily_valuation_required"
    recovery = "market_db_sync"

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        super().__init__(
            f"daily_valuation is required for fundamentals summary: {symbol}"
        )


class FundamentalsService:
    """Service for fundamentals API orchestration."""

    def __init__(self) -> None:
        self._market_client: Any | None = None
        self._calculator = FundamentalsCalculator()

    def __del__(self) -> None:
        self.close()

    @property
    def market_client(self) -> Any:
        if self._market_client is None:
            self._market_client = DirectMarketClient()
        return self._market_client

    def close(self) -> None:
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

    @staticmethod
    def _normalize_optional_scalar(value: Any) -> Any | None:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d")
        if pd.isna(value):
            return None
        return value

    @classmethod
    def _normalize_optional_float(cls, value: Any) -> float | None:
        normalized = cls._normalize_optional_scalar(value)
        if normalized is None:
            return None
        return float(normalized)

    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        normalized = cls._normalize_optional_scalar(value)
        if normalized is None:
            return None
        return str(normalized)

    @classmethod
    def _normalize_required_text(cls, value: Any, fallback: str = "") -> str:
        normalized = cls._normalize_optional_text(value)
        return normalized if normalized is not None else fallback

    @classmethod
    def _to_market_statement(
        cls,
        symbol: str,
        disclosed_at: Any,
        row: pd.Series,
    ) -> JQuantsStatement:
        disclosed_date = cls._normalize_required_text(disclosed_at)
        period_end = (
            cls._normalize_optional_text(row.get("periodEnd"))
            or cls._normalize_optional_text(row.get("curPerEn"))
            or disclosed_date
        )
        code = cls._normalize_required_text(row.get("code"), fallback=symbol)
        doc_type = cls._normalize_required_text(row.get("typeOfDocument"))
        period_type = cls._normalize_required_text(row.get("typeOfCurrentPeriod"))
        dividend_fy = cls._normalize_optional_float(row.get("dividendFY"))
        forecast_dividend_fy = cls._normalize_optional_float(
            row.get("forecastDividendFY")
        )
        next_year_forecast_dividend_fy = cls._normalize_optional_float(
            row.get("nextYearForecastDividendFY")
        )

        return JQuantsStatement(
            DiscDate=disclosed_date,
            Code=code,
            DocType=doc_type,
            CurPerType=period_type,
            CurPerSt=period_end,
            CurPerEn=period_end,
            CurFYSt=period_end,
            CurFYEn=period_end,
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=cls._normalize_optional_float(row.get("sales")),
            OP=cls._normalize_optional_float(row.get("operatingProfit")),
            OdP=cls._normalize_optional_float(row.get("ordinaryProfit")),
            NP=cls._normalize_optional_float(row.get("profit")),
            EPS=cls._normalize_optional_float(row.get("earningsPerShare")),
            DEPS=None,
            TA=cls._normalize_optional_float(row.get("totalAssets")),
            Eq=cls._normalize_optional_float(row.get("equity")),
            EqAR=None,
            BPS=cls._normalize_optional_float(row.get("bps")),
            CFO=cls._normalize_optional_float(row.get("operatingCashFlow")),
            CFI=cls._normalize_optional_float(row.get("investingCashFlow")),
            CFF=cls._normalize_optional_float(row.get("financingCashFlow")),
            CashEq=cls._normalize_optional_float(row.get("cashAndEquivalents")),
            ShOutFY=cls._normalize_optional_float(row.get("sharesOutstanding")),
            TrShFY=cls._normalize_optional_float(row.get("treasuryShares")),
            AvgSh=cls._normalize_optional_float(row.get("sharesOutstanding")),
            FEPS=cls._normalize_optional_float(row.get("forecastEps")),
            NxFEPS=cls._normalize_optional_float(
                row.get("nextYearForecastEarningsPerShare")
            ),
            FSales=cls._normalize_optional_float(row.get("forecastSales")),
            NxFSales=cls._normalize_optional_float(row.get("nextYearForecastSales")),
            FOP=cls._normalize_optional_float(row.get("forecastOperatingProfit")),
            NxFOP=cls._normalize_optional_float(
                row.get("nextYearForecastOperatingProfit")
            ),
            DivFY=dividend_fy,
            DivAnn=dividend_fy,
            PayoutRatioAnn=cls._normalize_optional_float(row.get("payoutRatio")),
            FDivFY=forecast_dividend_fy,
            FDivAnn=forecast_dividend_fy,
            FPayoutRatioAnn=cls._normalize_optional_float(
                row.get("forecastPayoutRatio")
            ),
            NxFDivFY=next_year_forecast_dividend_fy,
            NxFDivAnn=next_year_forecast_dividend_fy,
            NxFPayoutRatioAnn=cls._normalize_optional_float(
                row.get("nextYearForecastPayoutRatio")
            ),
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )

    def _get_market_statements(self, symbol: str) -> list[JQuantsStatement]:
        try:
            df = self.market_client.get_statements(
                symbol,
                period_type="all",
                actual_only=False,
            )
        except Exception as e:
            logger.warning(f"Failed to get statements from market DB for {symbol}: {e}")
            return []

        if df.empty:
            return []

        statements: list[JQuantsStatement] = []
        for disclosed_at, row in df.sort_index().iterrows():
            statements.append(self._to_market_statement(symbol, disclosed_at, row))
        return statements

    def _get_stock_info(self, symbol: str) -> StockInfo | None:
        try:
            return self.market_client.get_stock_info(symbol)
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

    @staticmethod
    def _round_optional_float(value: Any, digits: int = 4) -> float | None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(numeric):
            return None
        return round(numeric, digits)

    def _get_stock_adjustment_events(self, symbol: str) -> list[ShareAdjustmentEvent]:
        try:
            getter = getattr(self.market_client, "get_stock_adjustment_events")
        except Exception as e:
            logger.warning(
                f"Failed to access stock adjustment events client for {symbol}: {e}"
            )
            return []
        if not callable(getter):
            return []
        try:
            events = cast(Iterable[object], getter(symbol))
        except Exception as e:
            logger.warning(f"Failed to get stock adjustment events for {symbol}: {e}")
            return []
        return [event for event in events if isinstance(event, ShareAdjustmentEvent)]

    def _get_adjusted_daily_valuation(
        self,
        symbol: str,
    ) -> list[DomainDailyValuationDataPoint]:
        getter = getattr(self.market_client, "get_daily_valuation", None)
        if not callable(getter):
            return []
        try:
            rows = getter(symbol)
        except Exception as e:
            logger.warning(f"Failed to get adjusted daily valuation for {symbol}: {e}")
            return []
        if isinstance(rows, pd.DataFrame):
            records = cast(list[dict[str, Any]], rows.to_dict(orient="records"))
        elif isinstance(rows, list):
            records = cast(list[dict[str, Any]], rows)
        else:
            return []

        valuation: list[DomainDailyValuationDataPoint] = []
        for row_obj in records:
            if not isinstance(row_obj, dict):
                continue
            date = self._normalize_optional_text(row_obj.get("date"))
            close = self._normalize_optional_float(row_obj.get("close"))
            if date is None or close is None:
                continue
            valuation.append(
                DomainDailyValuationDataPoint(
                    date=date,
                    close=close,
                    eps=self._normalize_optional_float(row_obj.get("eps")),
                    bps=self._normalize_optional_float(row_obj.get("bps")),
                    per=self._normalize_optional_float(row_obj.get("per")),
                    forwardPer=self._normalize_optional_float(
                        row_obj.get("forward_per", row_obj.get("forwardPer"))
                    ),
                    sales=self._normalize_optional_float(row_obj.get("sales")),
                    forwardSales=self._normalize_optional_float(
                        row_obj.get("forward_sales", row_obj.get("forwardSales"))
                    ),
                    psr=self._normalize_optional_float(row_obj.get("psr")),
                    forwardPsr=self._normalize_optional_float(
                        row_obj.get("forward_psr", row_obj.get("forwardPsr"))
                    ),
                    pOp=self._normalize_optional_float(
                        row_obj.get("p_op", row_obj.get("pOp"))
                    ),
                    forwardPOp=self._normalize_optional_float(
                        row_obj.get("forward_p_op", row_obj.get("forwardPOp"))
                    ),
                    pbr=self._normalize_optional_float(row_obj.get("pbr")),
                    marketCap=self._normalize_optional_float(
                        row_obj.get("market_cap", row_obj.get("marketCap"))
                    ),
                    freeFloatMarketCap=self._normalize_optional_float(
                        row_obj.get(
                            "free_float_market_cap",
                            row_obj.get("freeFloatMarketCap"),
                        )
                    ),
                    statementDisclosedDate=self._normalize_optional_text(
                        row_obj.get(
                            "statement_disclosed_date",
                            row_obj.get("statementDisclosedDate"),
                        )
                    ),
                    forwardEps=self._normalize_optional_float(
                        row_obj.get("forward_eps", row_obj.get("forwardEps"))
                    ),
                    forwardEpsDisclosedDate=self._normalize_optional_text(
                        row_obj.get(
                            "forward_eps_disclosed_date",
                            row_obj.get("forwardEpsDisclosedDate"),
                        )
                    ),
                    forwardEpsSource=self._normalize_forward_eps_source(
                        row_obj.get("forward_eps_source", row_obj.get("forwardEpsSource"))
                    ),
                    forwardSalesDisclosedDate=self._normalize_optional_text(
                        row_obj.get(
                            "forward_sales_disclosed_date",
                            row_obj.get("forwardSalesDisclosedDate"),
                        )
                    ),
                    forwardSalesSource=self._normalize_forward_eps_source(
                        row_obj.get(
                            "forward_sales_source",
                            row_obj.get("forwardSalesSource"),
                        )
                    ),
                    priceBasisDate=self._normalize_optional_text(
                        row_obj.get("price_basis_date", row_obj.get("priceBasisDate"))
                    ),
                    basisVersion=self._normalize_optional_text(
                        row_obj.get("basis_version", row_obj.get("basisVersion"))
                    ),
                )
            )
        valuation.sort(key=lambda item: item.date)
        return valuation

    def _get_adjusted_statement_metrics(self, symbol: str) -> dict[str, dict[str, Any]]:
        getter = getattr(self.market_client, "get_adjusted_statement_metrics", None)
        if not callable(getter):
            return {}
        try:
            rows = getter(symbol)
        except Exception as e:
            logger.warning(f"Failed to get adjusted statement metrics for {symbol}: {e}")
            return {}
        if isinstance(rows, pd.DataFrame):
            records = cast(list[dict[str, Any]], rows.to_dict(orient="records"))
        elif isinstance(rows, list):
            records = cast(list[dict[str, Any]], rows)
        else:
            return {}
        metrics: dict[str, dict[str, Any]] = {}
        for row in records:
            if not isinstance(row, dict):
                continue
            disclosed_date = self._normalize_optional_text(
                row.get("disclosed_date", row.get("disclosedDate"))
            )
            if disclosed_date is None:
                continue
            metrics[disclosed_date] = row
        return metrics

    @staticmethod
    def _normalize_forward_eps_source(value: Any) -> Literal["revised", "fy"] | None:
        if value in {"revised", "fy"}:
            return cast(Literal["revised", "fy"], value)
        return None

    def _apply_adjusted_statement_metrics(
        self,
        data: list[DomainFundamentalDataPoint],
        latest_metrics: DomainFundamentalDataPoint | None,
        adjusted_metrics_by_disclosed_date: dict[str, dict[str, Any]],
    ) -> tuple[list[DomainFundamentalDataPoint], DomainFundamentalDataPoint | None]:
        if not adjusted_metrics_by_disclosed_date:
            return data, latest_metrics
        adjusted_data = [
            self._apply_adjusted_statement_metric(item, adjusted_metrics_by_disclosed_date)
            for item in data
        ]
        adjusted_latest = (
            self._apply_adjusted_statement_metric(
                latest_metrics,
                adjusted_metrics_by_disclosed_date,
            )
            if latest_metrics is not None
            else None
        )
        return adjusted_data, adjusted_latest

    def _apply_adjusted_statement_metric(
        self,
        item: DomainFundamentalDataPoint,
        adjusted_metrics_by_disclosed_date: dict[str, dict[str, Any]],
    ) -> DomainFundamentalDataPoint:
        metric = adjusted_metrics_by_disclosed_date.get(item.disclosedDate)
        if metric is None:
            return item
        return DomainFundamentalDataPoint(
            **{
                **item.model_dump(),
                "adjustedEps": self._normalize_optional_float(
                    metric.get("adjusted_eps", metric.get("adjustedEps"))
                ),
                "adjustedBps": self._normalize_optional_float(
                    metric.get("adjusted_bps", metric.get("adjustedBps"))
                ),
                "adjustedForecastEps": self._normalize_optional_float(
                    metric.get("adjusted_forecast_eps", metric.get("adjustedForecastEps"))
                ),
                "adjustedDividendFy": self._normalize_optional_float(
                    metric.get("adjusted_dividend_fy", metric.get("adjustedDividendFy"))
                ),
            }
        )

    @staticmethod
    def _apply_latest_daily_valuation_fields(
        latest_metrics: DomainFundamentalDataPoint | None,
        daily_valuation: list[DomainDailyValuationDataPoint],
    ) -> DomainFundamentalDataPoint | None:
        if latest_metrics is None or not daily_valuation:
            return latest_metrics
        latest_daily = daily_valuation[-1]
        return DomainFundamentalDataPoint(
            **{
                **latest_metrics.model_dump(),
                "stockPrice": latest_daily.close,
                "per": latest_daily.per,
                "forwardPer": latest_daily.forwardPer,
                "psr": latest_daily.psr,
                "forwardPsr": latest_daily.forwardPsr,
                "pOp": latest_daily.pOp,
                "forwardPOp": latest_daily.forwardPOp,
                "pbr": latest_daily.pbr,
                "marketCap": latest_daily.marketCap,
                "freeFloatMarketCap": latest_daily.freeFloatMarketCap,
                "eps": latest_daily.eps,
                "bps": latest_daily.bps,
                "adjustedEps": latest_daily.eps,
                "adjustedForecastEps": latest_daily.forwardEps,
                "adjustedBps": latest_daily.bps,
                "revisedForecastEps": latest_daily.forwardEps
                if latest_daily.forwardEpsSource == "revised"
                else None,
            }
        )

    @staticmethod
    def _build_latest_metrics_source(
        latest_metrics: DomainFundamentalDataPoint | None,
        daily_valuation: list[DomainDailyValuationDataPoint],
    ) -> LatestMetricsSource | None:
        if latest_metrics is None or not daily_valuation:
            return None
        latest_daily = daily_valuation[-1]
        forecast_source = (
            LatestMetricsSourceItem(
                table="daily_valuation",
                date=latest_daily.date,
                disclosedDate=latest_daily.forwardEpsDisclosedDate,
                source=latest_daily.forwardEpsSource,
            )
            if latest_daily.forwardEps is not None
            else None
        )
        return LatestMetricsSource(
            actualPerShare=LatestMetricsSourceItem(
                table="daily_valuation",
                date=latest_daily.date,
                periodType="FY",
                disclosedDate=latest_daily.statementDisclosedDate,
            ),
            valuation=LatestMetricsSourceItem(
                table="daily_valuation",
                date=latest_daily.date,
            ),
            forecast=forecast_source,
            latestDisclosure=LatestMetricsSourceItem(
                table="statements",
                date=latest_metrics.date,
                periodType=latest_metrics.periodType,
                disclosedDate=latest_metrics.disclosedDate,
            ),
        )

    def _build_prime_liquidity_profile(
        self,
        *,
        stock_info: StockInfo | None,
        daily_ohlcv: pd.DataFrame,
        daily_valuation: list[DomainDailyValuationDataPoint],
        adv_windows: tuple[int, ...] = (20, 60),
    ) -> LiquidityProfile:
        market_scope = normalize_market_scope(
            stock_info.marketCode if stock_info else None,
            market_name=stock_info.marketName if stock_info else None,
            default=None,
        )
        if market_scope != "prime":
            return LiquidityProfile(
                supported=False,
                unsupportedReason="prime_only_model",
                modelScope="prime",
            )
        if daily_ohlcv.empty or not daily_valuation:
            return LiquidityProfile(
                supported=False,
                unsupportedReason="missing_price_or_valuation",
                modelScope="prime",
            )

        latest_valuation = next(
            (
                item
                for item in reversed(daily_valuation)
                if item.freeFloatMarketCap is not None
                and item.freeFloatMarketCap > 0
                and item.close > 0
            ),
            None,
        )
        if latest_valuation is None:
            return LiquidityProfile(
                supported=False,
                unsupportedReason="missing_free_float_market_cap",
                modelScope="prime",
            )
        latest_free_float_market_cap = float(cast(float, latest_valuation.freeFloatMarketCap))
        latest_close = float(latest_valuation.close)

        price_frame = daily_ohlcv.copy()
        price_frame.index = pd.to_datetime(price_frame.index)
        price_frame["date"] = price_frame.index.strftime("%Y-%m-%d")
        price_frame = price_frame.sort_index()
        recent_return_20d = self._recent_return_pct(
            price_frame, latest_valuation.date, 20
        )
        recent_return_60d = self._recent_return_pct(
            price_frame, latest_valuation.date, 60
        )

        regression_panel = (
            self.market_client.get_prime_free_float_liquidity_regression_panel(
                latest_valuation.date,
                adv_windows=adv_windows,
            )
        )
        windows: list[LiquidityProfileWindow] = []
        for adv_window in adv_windows:
            window_profile = self._build_prime_liquidity_profile_window(
                regression_panel=regression_panel,
                price_frame=price_frame,
                date=latest_valuation.date,
                current_price=latest_close,
                free_float_market_cap=latest_free_float_market_cap,
                adv_window=adv_window,
                recent_return_20d_pct=recent_return_20d,
                recent_return_60d_pct=recent_return_60d,
            )
            windows.append(window_profile)

        return LiquidityProfile(
            supported=any(item.averageTradingValue is not None for item in windows),
            unsupportedReason=None
            if any(item.averageTradingValue is not None for item in windows)
            else "insufficient_prime_regression_sample",
            modelScope="prime",
            date=latest_valuation.date,
            currentPrice=self._round_optional_float(latest_close, 4),
            freeFloatMarketCap=self._round_optional_float(
                latest_free_float_market_cap,
                2,
            ),
            recentReturn20dPct=recent_return_20d,
            recentReturn60dPct=recent_return_60d,
            windows=windows,
        )

    def _build_prime_liquidity_profile_window(
        self,
        *,
        regression_panel: pd.DataFrame,
        price_frame: pd.DataFrame,
        date: str,
        current_price: float,
        free_float_market_cap: float,
        adv_window: int,
        recent_return_20d_pct: float | None,
        recent_return_60d_pct: float | None,
    ) -> LiquidityProfileWindow:
        adv_column = f"adv{adv_window}_jpy"
        median_trading_value = self._median_trading_value(
            price_frame,
            date,
            adv_window,
        )
        base_payload: dict[str, Any] = {
            "advWindow": adv_window,
            "averageTradingValue": self._round_optional_float(median_trading_value, 2),
            "freeFloatTradingValueRatioPct": self._round_optional_float(
                (median_trading_value / free_float_market_cap) * 100.0
                if median_trading_value is not None and free_float_market_cap > 0
                else None,
                4,
            ),
        }
        if median_trading_value is None or regression_panel.empty:
            return LiquidityProfileWindow(**base_payload)

        panel = regression_panel[[adv_column, "free_float_market_cap"]].copy()
        panel[adv_column] = pd.to_numeric(panel[adv_column], errors="coerce")
        panel["free_float_market_cap"] = pd.to_numeric(
            panel["free_float_market_cap"],
            errors="coerce",
        )
        panel = panel[
            (panel[adv_column] > 0) & (panel["free_float_market_cap"] > 0)
        ].dropna()
        if len(panel) < 100:
            return LiquidityProfileWindow(**base_payload)

        x = np.log(panel["free_float_market_cap"].to_numpy(dtype=float))
        y = np.log(panel[adv_column].to_numpy(dtype=float))
        regression = self._fit_simple_regression(y, x)
        if regression is None:
            return LiquidityProfileWindow(**base_payload)
        alpha, beta, r_squared, residual_std = regression
        if beta <= 0 or residual_std <= 0:
            return LiquidityProfileWindow(**base_payload)

        log_adv = float(np.log(median_trading_value))
        log_ffcap = float(np.log(free_float_market_cap))
        expected_log_adv = alpha + beta * log_ffcap
        residual = log_adv - expected_log_adv
        residual_z = residual / residual_std
        implied_ffcap = float(np.exp((log_adv - alpha) / beta))
        implied_price = current_price * implied_ffcap / free_float_market_cap
        implied_price_gap_pct = (implied_price / current_price - 1.0) * 100.0

        return LiquidityProfileWindow(
            **base_payload,
            liquidityResidualZ=self._round_optional_float(residual_z, 4),
            liquidityImpliedFreeFloatMarketCap=self._round_optional_float(
                implied_ffcap, 2
            ),
            liquidityImpliedPrice=self._round_optional_float(implied_price, 4),
            liquidityImpliedPriceGapPct=self._round_optional_float(
                implied_price_gap_pct, 4
            ),
            liquidityRegime=self._classify_liquidity_regime(
                residual_z,
                recent_return_20d_pct,
                recent_return_60d_pct,
            ),
            regressionAlpha=self._round_optional_float(alpha, 6),
            regressionBeta=self._round_optional_float(beta, 6),
            regressionRSquared=self._round_optional_float(r_squared, 6),
            regressionObservationCount=int(len(panel)),
        )

    @staticmethod
    def _median_trading_value(
        price_frame: pd.DataFrame,
        date: str,
        window: int,
    ) -> float | None:
        eligible = price_frame[price_frame["date"] <= date].tail(window)
        if len(eligible) < window:
            return None
        close = pd.to_numeric(eligible["Close"], errors="coerce")
        volume = pd.to_numeric(eligible["Volume"], errors="coerce")
        trading_value = (close * volume).dropna()
        if len(trading_value) < window:
            return None
        value = float(trading_value.median())
        return value if value > 0 else None

    @staticmethod
    def _recent_return_pct(
        price_frame: pd.DataFrame,
        date: str,
        window: int,
    ) -> float | None:
        eligible = price_frame[price_frame["date"] <= date].tail(window + 1)
        if len(eligible) < window + 1:
            return None
        close = pd.to_numeric(eligible["Close"], errors="coerce")
        current = float(close.iloc[-1])
        prior = float(close.iloc[0])
        if prior <= 0 or current <= 0:
            return None
        return round((current / prior - 1.0) * 100.0, 4)

    @staticmethod
    def _fit_simple_regression(
        y: np.ndarray[Any, np.dtype[np.float64]],
        x: np.ndarray[Any, np.dtype[np.float64]],
    ) -> tuple[float, float, float, float] | None:
        if len(y) != len(x) or len(y) < 3:
            return None
        design = np.column_stack([np.ones(len(x)), x])
        beta_values, *_ = np.linalg.lstsq(design, y, rcond=None)
        fitted = design @ beta_values
        residual = y - fitted
        total = y - y.mean()
        ss_total = float(total @ total)
        if ss_total <= 0:
            return None
        dof = len(y) - design.shape[1]
        if dof <= 0:
            return None
        residual_std = float(np.sqrt((residual @ residual) / dof))
        r_squared = float(1.0 - (residual @ residual) / ss_total)
        return (
            float(beta_values[0]),
            float(beta_values[1]),
            r_squared,
            residual_std,
        )

    @staticmethod
    def _classify_liquidity_regime(
        residual_z: float,
        recent_return_20d_pct: float | None,
        recent_return_60d_pct: float | None,
    ) -> str:
        returns = [recent_return_20d_pct, recent_return_60d_pct]
        valid_returns = [value for value in returns if value is not None]
        has_persistent_runup = len(valid_returns) == 2 and all(
            value > 0 for value in valid_returns
        )
        if residual_z >= 1.0 and len(valid_returns) == 2:
            if has_persistent_runup:
                return "crowded_rerating"
            if any(value <= 0 for value in valid_returns):
                return "distribution_stress"
        if residual_z <= -1.0:
            return "stale_liquidity"
        if -1.0 < residual_z < 1.0 and has_persistent_runup:
            return "neutral_rerating"
        return "neutral"

    def compute_fundamentals(
        self, request: FundamentalsComputeRequest
    ) -> FundamentalsComputeResponse:
        logger.debug(f"Computing fundamentals for {request.symbol}")

        statements = self._get_market_statements(request.symbol)

        if not statements:
            logger.debug(f"No financial statements found for {request.symbol}")
            return FundamentalsComputeResponse(
                symbol=request.symbol,
                data=[],
                tradingValuePeriod=request.trading_value_period,
                forecastEpsLookbackFyCount=request.forecast_eps_lookback_fy_count,
                lastUpdated=datetime.now().isoformat(),
                provenance=build_market_provenance(
                    loaded_domains=("statements", "stock_data", "stocks"),
                ),
                diagnostics=ResponseDiagnostics(
                    missing_required_data=["statements"],
                    used_fields=["statements"],
                    effective_period_type=request.period_type,
                ),
            )

        stock_info = self._get_stock_info(request.symbol)

        daily_ohlcv = self._get_daily_stock_ohlcv(request.symbol)
        daily_prices = self._calculator._to_daily_close_map(daily_ohlcv)
        share_adjustment_events = self._get_stock_adjustment_events(request.symbol)
        latest_price_date = max(daily_prices.keys()) if daily_prices else None
        adjusted_statement_metrics = self._get_adjusted_statement_metrics(
            request.symbol,
        )

        daily_valuation = self._get_adjusted_daily_valuation(request.symbol)
        if not daily_valuation:
            raise DailyValuationRequiredError(request.symbol)
        price_basis_date = (
            daily_valuation[-1].priceBasisDate
            if daily_valuation and daily_valuation[-1].priceBasisDate is not None
            else latest_price_date
        )
        valuation_basis_version = (
            daily_valuation[-1].basisVersion
            if daily_valuation and daily_valuation[-1].basisVersion is not None
            else None
        )

        liquidity_profile = self._build_prime_liquidity_profile(
            stock_info=stock_info,
            daily_ohlcv=daily_ohlcv,
            daily_valuation=daily_valuation,
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
                priceBasisDate=price_basis_date,
                valuationBasisVersion=valuation_basis_version,
                liquidityProfile=liquidity_profile,
                tradingValuePeriod=request.trading_value_period,
                forecastEpsLookbackFyCount=request.forecast_eps_lookback_fy_count,
                lastUpdated=datetime.now().isoformat(),
                provenance=build_market_provenance(
                    loaded_domains=("statements", "stock_data", "stocks"),
                ),
                diagnostics=ResponseDiagnostics(
                    missing_required_data=["filtered_statements"],
                    used_fields=[
                        "statements",
                        "daily_valuation",
                    ],
                    effective_period_type=request.period_type,
                ),
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
            share_adjustment_events=share_adjustment_events,
            through_date=latest_price_date,
        )

        data, latest_metrics = self._calculator._apply_share_adjustments(
            data,
            statements,
            latest_metrics,
            share_adjustment_events=share_adjustment_events,
            through_date=latest_price_date,
        )
        data, latest_metrics = self._apply_adjusted_statement_metrics(
            data,
            latest_metrics,
            adjusted_statement_metrics,
        )
        latest_metrics = self._apply_latest_daily_valuation_fields(
            latest_metrics,
            daily_valuation,
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
        latest_metrics_source = self._build_latest_metrics_source(
            latest_metrics,
            daily_valuation,
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
            latestMetricsSource=latest_metrics_source,
            dailyValuation=api_daily_valuation,
            priceBasisDate=price_basis_date,
            valuationBasisVersion=valuation_basis_version,
            liquidityProfile=liquidity_profile,
            tradingValuePeriod=request.trading_value_period,
            forecastEpsLookbackFyCount=request.forecast_eps_lookback_fy_count,
            lastUpdated=datetime.now().isoformat(),
            provenance=build_market_provenance(
                loaded_domains=("statements", "stock_data", "stocks"),
            ),
            diagnostics=ResponseDiagnostics(
                missing_required_data=[],
                used_fields=[
                    "statements.earnings_per_share",
                    "statements.forecast_eps",
                    "statements.equity",
                    "daily_valuation",
                ],
                effective_period_type=request.period_type,
            ),
        )


fundamentals_service = FundamentalsService()
