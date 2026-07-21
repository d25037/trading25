from __future__ import annotations

from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pydantic import ValidationError

from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshot
from src.application.services.fundamentals_service import FundamentalsService
from src.application.contracts.fundamentals import FundamentalsComputeQuery
from src.infrastructure.external_api.jquants_client import StockInfo


class SnapshotOnlyClient:
    def __init__(self, snapshot: FundamentalsPitSnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[str, str, date | None]] = []

    def get_fundamentals_pit_snapshot(
        self, symbol: str, cutoff_date: date | None
    ) -> FundamentalsPitSnapshot:
        self.calls.append(("get_fundamentals_pit_snapshot", symbol, cutoff_date))
        return self.snapshot

    def close(self) -> None:
        return None

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"current/independent getter must not be used: {name}")


class FutureSentinelClient(SnapshotOnlyClient):
    def __init__(self, snapshot: FundamentalsPitSnapshot) -> None:
        super().__init__(snapshot)
        self.independent_calls: list[str] = []

    def __getattr__(self, name: str) -> object:
        def future_getter(*args: object, **kwargs: object) -> object:
            del args, kwargs
            self.independent_calls.append(name)
            return {
                "futureRevision": "2099-12-31",
                "futureSplitFactor": 0.001,
                "currentCompanyName": "POST-CUTOFF SENTINEL",
            }

        return future_getter


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz: object | None = None) -> _FixedDateTime:
        del tz
        return cls(2024, 7, 1, 12, 0, 0)


def _stock_info() -> StockInfo:
    return StockInfo(
        code="7203",
        companyName="Toyota PIT",
        companyNameEnglish="Toyota PIT",
        marketCode="0112",
        marketName="スタンダード",
        sector17Code="16",
        sector17Name="自動車・輸送機",
        sector33Code="3250",
        sector33Name="自動車・輸送機",
        scaleCategory="TOPIX Large70",
        listedDate="1949-05-16",
    )


def _statements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "7203",
                "typeOfDocument": "FinancialStatements",
                "typeOfCurrentPeriod": "FY",
                "periodEnd": "2023-03-31",
                "sales": 900_000.0,
                "operatingProfit": 90_000.0,
                "profit": 45_000.0,
                "equity": 450_000.0,
                "earningsPerShare": 90.0,
                "bps": 900.0,
                "forecastEps": 105.0,
                "sharesOutstanding": 10_000_000.0,
            },
            {
                "code": "7203",
                "typeOfDocument": "FinancialStatements",
                "typeOfCurrentPeriod": "FY",
                # A forecast period may end after the knowledge cutoff.
                "periodEnd": "2025-03-31",
                "sales": 1_000_000.0,
                "operatingProfit": 100_000.0,
                "profit": 50_000.0,
                "equity": 500_000.0,
                "earningsPerShare": 100.0,
                "bps": 1_000.0,
                "forecastEps": 120.0,
                "sharesOutstanding": 10_000_000.0,
            },
        ],
        index=pd.to_datetime(["2023-05-10", "2024-05-10"]),
    )


def _pit_snapshot() -> FundamentalsPitSnapshot:
    provider_as_of = "2024-06-28T16:30:00+09:00"
    valuation = (
        {
            "code": "7203",
            "date": "2024-05-31",
            "close": 490.0,
            "eps": 50.0,
            "bps": 500.0,
            "forward_eps": 60.0,
            "per": 9.8,
            "forward_per": 8.1666666667,
            "pbr": 0.98,
            "market_cap": 9_800_000_000.0,
            "free_float_market_cap": 8_800_000_000.0,
            "statement_disclosed_date": "2024-05-10",
            "forward_eps_disclosed_date": "2024-05-10",
            "forward_eps_source": "fy",
            "price_basis_date": "2024-06-28",
            "fundamentals_adjustment_basis_date": "2024-06-28",
            "provider_as_of": provider_as_of,
        },
        {
            "code": "7203",
            "date": "2024-06-28",
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
            "price_basis_date": "2024-06-28",
            "fundamentals_adjustment_basis_date": "2024-06-28",
            "provider_as_of": provider_as_of,
        },
    )
    return FundamentalsPitSnapshot(
        requested_cutoff_date=date(2024, 6, 30),
        knowledge_cutoff_date=date(2024, 6, 30),
        effective_market_date=date(2024, 6, 28),
        stock_master_snapshot_date=date(2024, 6, 28),
        fundamentals_adjustment_basis_date=date(2024, 6, 28),
        provider_as_of=provider_as_of,
        provider_coverage_start=date(2024, 1, 1),
        provider_coverage_end=date(2024, 6, 28),
        stock_info=_stock_info(),
        statements=_statements(),
        adjusted_statement_metrics=(
            {
                "code": "7203",
                "disclosed_date": "2023-05-10",
                "adjusted_eps": 45.0,
                "adjusted_bps": 450.0,
                "adjusted_forecast_eps": 52.5,
                "fundamentals_adjustment_basis_date": "2024-06-28",
            },
            {
                "code": "7203",
                "disclosed_date": "2024-05-10",
                "adjusted_eps": 50.0,
                "adjusted_bps": 500.0,
                "adjusted_forecast_eps": 60.0,
                "fundamentals_adjustment_basis_date": "2024-06-28",
            },
        ),
        daily_valuation=valuation,
        ohlcv=pd.DataFrame(
            {"Close": [490.0, 500.0], "Volume": [100_000.0, 110_000.0]},
            index=pd.to_datetime(["2024-05-31", "2024-06-28"]),
        ),
        prime_liquidity_panel=pd.DataFrame(),
    )


def _request(**overrides: object) -> FundamentalsComputeQuery:
    return FundamentalsComputeQuery(
        symbol="7203",
        to_date=date(2024, 6, 30),
        **overrides,
    )


def test_service_uses_only_one_pit_snapshot() -> None:
    client = SnapshotOnlyClient(_pit_snapshot())
    result = FundamentalsService(client).compute_fundamentals(_request())

    assert client.calls == [
        ("get_fundamentals_pit_snapshot", "7203", date(2024, 6, 30))
    ]
    assert result.asOfDate == "2024-06-28"
    assert result.fundamentalsAdjustmentBasisDate == "2024-06-28"
    assert result.providerAsOf == "2024-06-28T16:30:00+09:00"
    assert result.provenance.reference_date == "2024-06-30"


def test_post_cutoff_current_getters_are_unreachable() -> None:
    clean_client = SnapshotOnlyClient(_pit_snapshot())
    poisoned_client = FutureSentinelClient(_pit_snapshot())

    with patch(
        "src.application.services.fundamentals_service.datetime", _FixedDateTime
    ):
        clean = FundamentalsService(clean_client).compute_fundamentals(_request())
        poisoned = FundamentalsService(poisoned_client).compute_fundamentals(_request())

    assert poisoned.model_dump() == clean.model_dump()
    assert poisoned_client.calls == [
        ("get_fundamentals_pit_snapshot", "7203", date(2024, 6, 30))
    ]
    assert poisoned_client.independent_calls == []


def test_from_is_applied_only_to_returned_series_after_computation() -> None:
    full = FundamentalsService(SnapshotOnlyClient(_pit_snapshot())).compute_fundamentals(
        _request()
    )
    cropped = FundamentalsService(
        SnapshotOnlyClient(_pit_snapshot())
    ).compute_fundamentals(_request(from_date=date(2024, 6, 1)))

    assert [item.date for item in full.data] == ["2025-03-31", "2023-03-31"]
    assert [item.date for item in cropped.data] == ["2025-03-31"]
    assert cropped.data[0].forecastEps == 120.0
    assert cropped.latestMetrics == full.latestMetrics
    assert cropped.liquidityProfile == full.liquidityProfile
    assert cropped.dailyValuation is not None
    assert [item.date for item in cropped.dailyValuation] == ["2024-06-28"]


def test_request_dates_are_typed_and_ordered() -> None:
    request = FundamentalsComputeQuery(
        symbol="7203", from_date="2024-06-01", to_date="2024-06-30"
    )
    assert request.from_date == date(2024, 6, 1)
    assert request.to_date == date(2024, 6, 30)

    with pytest.raises(ValidationError, match="from_date must be on or before to_date"):
        FundamentalsComputeQuery(
            symbol="7203", from_date="2024-07-01", to_date="2024-06-30"
        )
