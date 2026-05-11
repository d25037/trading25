from __future__ import annotations

from typing import Any

import pandas as pd

from src.application.services.fundamentals_service import FundamentalsService
from src.domains.fundamentals import DailyValuationDataPoint
from src.infrastructure.external_api.jquants_client import StockInfo


class _FakeMarketClient:
    def __init__(self, panel: pd.DataFrame) -> None:
        self.panel = panel

    def get_prime_free_float_liquidity_regression_panel(
        self,
        target_date: str,
        *,
        adv_windows: tuple[int, ...] = (20, 60),
    ) -> pd.DataFrame:
        _ = target_date, adv_windows
        return self.panel


def test_prime_liquidity_profile_builds_implied_price_and_regime() -> None:
    service = FundamentalsService()
    service._market_client = _FakeMarketClient(_build_regression_panel())
    ohlcv = _build_ohlcv()
    latest_close = float(ohlcv["Close"].iloc[-1])
    latest_date = str(ohlcv.index[-1].date())

    profile = service._build_prime_liquidity_profile(
        stock_info=_stock_info("0111", "プライム"),
        daily_ohlcv=ohlcv,
        daily_valuation=[
            DailyValuationDataPoint(
                date=latest_date,
                close=latest_close,
                freeFloatMarketCap=10_000_000_000.0,
                marketCap=12_000_000_000.0,
            )
        ],
    )

    assert profile.supported is True
    assert profile.modelScope == "prime"
    assert profile.date == latest_date
    adv60 = next(item for item in profile.windows if item.advWindow == 60)
    assert adv60.averageTradingValue is not None
    assert adv60.freeFloatTradingValueRatioPct is not None
    assert adv60.liquidityImpliedPrice is not None
    assert adv60.liquidityImpliedPriceGapPct is not None
    assert adv60.regressionObservationCount == 120
    assert adv60.liquidityRegime in {
        "rerating_participation",
        "distribution_stress",
        "neutral",
        "stale_liquidity",
    }


def test_non_prime_liquidity_profile_is_explicitly_unsupported() -> None:
    service = FundamentalsService()
    service._market_client = _FakeMarketClient(_build_regression_panel())

    profile = service._build_prime_liquidity_profile(
        stock_info=_stock_info("0112", "スタンダード"),
        daily_ohlcv=_build_ohlcv(),
        daily_valuation=[
            DailyValuationDataPoint(
                date="2024-04-30",
                close=100.0,
                freeFloatMarketCap=10_000_000_000.0,
            )
        ],
    )

    assert profile.supported is False
    assert profile.unsupportedReason == "prime_only_model"
    assert profile.windows == []


def _stock_info(market_code: str, market_name: str) -> StockInfo:
    return StockInfo(
        code="9999",
        companyName="Test",
        companyNameEnglish="Test",
        marketCode=market_code,
        marketName=market_name,
        sector17Code="1",
        sector17Name="Sector17",
        sector33Code="1",
        sector33Name="Sector33",
        scaleCategory="Small",
        listedDate="2020-01-01",
    )


def _build_ohlcv() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows: list[dict[str, Any]] = []
    for index, _date in enumerate(dates):
        close = 100.0 + index * 1.0
        rows.append(
            {
                "Open": close,
                "High": close,
                "Low": close,
                "Close": close,
                "Volume": 100_000.0 + index * 1_000.0,
            }
        )
    return pd.DataFrame(rows, index=dates)


def _build_regression_panel() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index in range(120):
        free_float_market_cap = 1_000_000_000.0 * (index + 10)
        adv60 = 0.02 * free_float_market_cap
        rows.append(
            {
                "code": f"{1000 + index}",
                "date": "2024-04-30",
                "close": 100.0,
                "volume": 100_000.0,
                "shares_outstanding": 1_000_000.0,
                "treasury_shares": 0.0,
                "free_float_market_cap": free_float_market_cap,
                "adv20_jpy": adv60 * 1.1,
                "adv60_jpy": adv60,
            }
        )
    return pd.DataFrame(rows)
