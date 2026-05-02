import pytest

from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_share_count_to_price_basis,
    cumulative_adjustment_factor_after,
)


def test_adjust_share_count_to_price_basis_applies_post_statement_split() -> None:
    events = [ShareAdjustmentEvent(date="2026-03-30", adjustment_factor=0.2)]

    adjusted = adjust_share_count_to_price_basis(
        2_260_000,
        events,
        from_date="2026-02-09",
        through_date="2026-05-01",
    )

    assert adjusted == pytest.approx(11_300_000)


def test_adjust_share_count_to_price_basis_ignores_future_split() -> None:
    events = [ShareAdjustmentEvent(date="2026-03-30", adjustment_factor=0.2)]

    adjusted = adjust_share_count_to_price_basis(
        2_260_000,
        events,
        from_date="2026-02-09",
        through_date="2026-03-13",
    )

    assert adjusted == pytest.approx(2_260_000)


def test_cumulative_adjustment_factor_after_multiplies_events_in_window() -> None:
    events = [
        ShareAdjustmentEvent(date="2025-01-10", adjustment_factor=0.5),
        ShareAdjustmentEvent(date="2026-03-30", adjustment_factor=0.2),
        ShareAdjustmentEvent(date="2026-06-01", adjustment_factor=2.0),
    ]

    factor = cumulative_adjustment_factor_after(
        events,
        from_date="2025-12-31",
        through_date="2026-05-01",
    )

    assert factor == pytest.approx(0.2)
