import pytest

from src.domains.fundamentals.adjusted_metrics import (
    AdjustedStatementInput,
    build_adjusted_statement_metric,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


def test_split_event_adjusts_per_share_values_to_price_basis() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="9880",
            statement_id="disclosure-1",
            disclosed_date="2023-05-10",
            disclosed_at="2023-05-10T15:30:00+09:00",
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
        fundamentals_adjustment_basis_date="2024-12-30",
        source_fingerprint="fingerprint",
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
            statement_id="disclosure-1",
            disclosed_date="2023-05-10",
            disclosed_at="2023-05-10T15:30:00+09:00",
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
        fundamentals_adjustment_basis_date="2024-12-30",
        source_fingerprint="fingerprint",
    )

    assert metric.adjusted_eps == pytest.approx(200.0)
    assert metric.adjusted_bps == pytest.approx(2000.0)
    assert metric.adjusted_forecast_eps == pytest.approx(240.0)
    assert metric.adjusted_dividend_fy == pytest.approx(60.0)
    assert metric.adjusted_shares_outstanding == pytest.approx(5_000_000.0)
    assert metric.adjusted_treasury_shares == pytest.approx(500_000.0)
    assert metric.adjustment_factor_cumulative == pytest.approx(2.0)


def test_adjusts_all_per_share_fields_and_keeps_raw_values() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="7203",
            statement_id="disclosure-1",
            disclosed_date="2024-05-10",
            disclosed_at="2024-05-10T15:30:00+09:00",
            period_end="2024-03-31",
            period_type="FY",
            eps=100.0,
            diluted_eps=98.0,
            bps=1000.0,
            forecast_eps=120.0,
            dividend_fy=30.0,
            forecast_dividend_fy=40.0,
            shares_outstanding=10_000_000.0,
            treasury_shares=1_000_000.0,
        ),
        events=[ShareAdjustmentEvent(date="2024-08-01", adjustment_factor=0.5)],
        fundamentals_adjustment_basis_date="2024-12-30",
        source_fingerprint="fingerprint",
    )

    assert metric.statement_id == "disclosure-1"
    assert metric.disclosed_at == "2024-05-10T15:30:00+09:00"
    assert metric.raw_diluted_eps == 98.0
    assert metric.adjusted_diluted_eps == pytest.approx(49.0)
    assert metric.raw_forecast_dividend_fy == 40.0
    assert metric.adjusted_forecast_dividend_fy == pytest.approx(20.0)
    assert metric.adjusted_shares_outstanding == pytest.approx(20_000_000.0)
    assert metric.adjusted_treasury_shares == pytest.approx(2_000_000.0)


@pytest.mark.parametrize(
    ("event_date", "expected_factor"),
    [
        ("2024-05-09", 1.0),
        ("2024-05-10", 1.0),
        ("2024-05-11", 0.5),
        ("2024-12-31", 1.0),
    ],
)
def test_only_events_strictly_after_disclosure_through_current_basis_apply(
    event_date: str,
    expected_factor: float,
) -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="7203",
            statement_id="disclosure-1",
            disclosed_date="2024-05-10",
            disclosed_at="2024-05-10T15:30:00+09:00",
            period_end="2024-03-31",
            period_type="FY",
            eps=100.0,
            shares_outstanding=10_000_000.0,
        ),
        events=[ShareAdjustmentEvent(date=event_date, adjustment_factor=0.5)],
        fundamentals_adjustment_basis_date="2024-12-30",
        source_fingerprint="fingerprint",
    )

    assert metric.adjusted_eps == pytest.approx(100.0 * expected_factor)
    assert metric.adjusted_shares_outstanding == pytest.approx(
        10_000_000.0 / expected_factor
    )


def test_share_count_change_without_adjustment_event_does_not_adjust_values() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="9880",
            statement_id="disclosure-1",
            disclosed_date="2023-05-10",
            disclosed_at="2023-05-10T15:30:00+09:00",
            period_end="2023-03-31",
            period_type="FY",
            eps=100.0,
            bps=1000.0,
            forecast_eps=120.0,
            dividend_fy=30.0,
            shares_outstanding=10_000_000.0,
        ),
        events=[],
        fundamentals_adjustment_basis_date="2024-12-30",
        source_fingerprint="fingerprint",
    )

    assert metric.adjusted_eps == pytest.approx(100.0)
    assert metric.adjusted_bps == pytest.approx(1000.0)
    assert metric.adjusted_forecast_eps == pytest.approx(120.0)
    assert metric.adjusted_dividend_fy == pytest.approx(30.0)
    assert metric.adjusted_shares_outstanding == pytest.approx(10_000_000.0)
    assert metric.adjustment_factor_cumulative == pytest.approx(1.0)
