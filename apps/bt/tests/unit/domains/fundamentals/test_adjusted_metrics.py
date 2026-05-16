import pytest

from src.domains.fundamentals.adjusted_metrics import (
    AdjustedStatementInput,
    DailyValuationInput,
    build_adjusted_statement_metric,
    build_daily_valuation_metric,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


def test_split_event_adjusts_per_share_values_to_price_basis() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="9880",
            disclosed_date="2023-05-10",
            period_end="2023-03-31",
            period_type="FY",
            eps=100.0,
            bps=1000.0,
            forecast_eps=120.0,
            dividend_fy=30.0,
            shares_outstanding=10_000_000.0,
            treasury_shares=1_000_000.0,
        ),
        events=[ShareAdjustmentEvent(date="2024-01-01", adjustment_factor=0.5)],
        price_basis_date="2024-12-30",
        basis_version="adjusted-v1:2024-12-30",
    )

    assert metric.adjusted_eps == pytest.approx(50.0)
    assert metric.adjusted_bps == pytest.approx(500.0)
    assert metric.adjusted_forecast_eps == pytest.approx(60.0)
    assert metric.adjusted_dividend_fy == pytest.approx(15.0)
    assert metric.adjusted_shares_outstanding == pytest.approx(20_000_000.0)
    assert metric.adjusted_treasury_shares == pytest.approx(2_000_000.0)
    assert metric.adjustment_factor_cumulative == pytest.approx(0.5)


def test_reverse_split_event_adjusts_per_share_values_to_price_basis() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="1234",
            disclosed_date="2023-05-10",
            period_end="2023-03-31",
            period_type="FY",
            eps=100.0,
            bps=1000.0,
            forecast_eps=120.0,
            dividend_fy=30.0,
            shares_outstanding=10_000_000.0,
            treasury_shares=1_000_000.0,
        ),
        events=[ShareAdjustmentEvent(date="2024-01-01", adjustment_factor=2.0)],
        price_basis_date="2024-12-30",
        basis_version="adjusted-v1:2024-12-30",
    )

    assert metric.adjusted_eps == pytest.approx(200.0)
    assert metric.adjusted_bps == pytest.approx(2000.0)
    assert metric.adjusted_forecast_eps == pytest.approx(240.0)
    assert metric.adjusted_dividend_fy == pytest.approx(60.0)
    assert metric.adjusted_shares_outstanding == pytest.approx(5_000_000.0)
    assert metric.adjusted_treasury_shares == pytest.approx(500_000.0)
    assert metric.adjustment_factor_cumulative == pytest.approx(2.0)


def test_share_count_change_without_adjustment_event_does_not_adjust_values() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="9880",
            disclosed_date="2023-05-10",
            period_end="2023-03-31",
            period_type="FY",
            eps=100.0,
            bps=1000.0,
            forecast_eps=120.0,
            dividend_fy=30.0,
            shares_outstanding=10_000_000.0,
        ),
        events=[],
        price_basis_date="2024-12-30",
        basis_version="adjusted-v1:2024-12-30",
    )

    assert metric.adjusted_eps == pytest.approx(100.0)
    assert metric.adjusted_bps == pytest.approx(1000.0)
    assert metric.adjusted_forecast_eps == pytest.approx(120.0)
    assert metric.adjusted_dividend_fy == pytest.approx(30.0)
    assert metric.adjusted_shares_outstanding == pytest.approx(10_000_000.0)
    assert metric.adjustment_factor_cumulative == pytest.approx(1.0)


def test_daily_valuation_keeps_negative_values_but_ratios_are_undefined() -> None:
    valuation = build_daily_valuation_metric(
        DailyValuationInput(
            code="9999",
            date="2024-12-30",
            price_basis_date="2024-12-30",
            close=500.0,
            eps=-10.0,
            bps=0.0,
            forward_eps=None,
            shares_outstanding=1_000_000.0,
            treasury_shares=100_000.0,
            statement_disclosed_date="2024-05-10",
            forward_eps_disclosed_date=None,
            forward_eps_source=None,
            basis_version="adjusted-v1:2024-12-30",
        )
    )

    assert valuation.eps == -10.0
    assert valuation.bps == 0.0
    assert valuation.per is None
    assert valuation.pbr is None
    assert valuation.forward_per is None
    assert valuation.market_cap == pytest.approx(500_000_000.0)
    assert valuation.free_float_market_cap == pytest.approx(450_000_000.0)
