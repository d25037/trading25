from __future__ import annotations

import pytest

from src.shared.provider_stock_window import validate_provider_stock_window


def _row(
    date: str,
    *,
    factor: float = 1.0,
    future_factor: float = 1.0,
) -> dict[str, object]:
    return {
        "code": "7203",
        "date": date,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1_000,
        "turnover_value": 105_000.0,
        "adjustment_factor": factor,
        "adjusted_open": 100.0 * future_factor,
        "adjusted_high": 110.0 * future_factor,
        "adjusted_low": 90.0 * future_factor,
        "adjusted_close": 105.0 * future_factor,
        "adjusted_volume": round(1_000 / future_factor),
        "created_at": "2026-02-12T00:00:00+00:00",
    }


def _metadata() -> dict[str, str]:
    return {
        "provider_plan": "premium",
        "provider_as_of": "2026-02-12",
        "provider_source_fingerprint": "a" * 64,
    }


def test_validate_provider_stock_window_uses_only_strictly_future_factors() -> None:
    rows = [
        _row("2026-02-10", future_factor=0.5),
        _row("2026-02-11", future_factor=0.5),
        _row("2026-02-12", factor=0.5, future_factor=1.0),
    ]

    _code, validated, _coverage, _metadata_value = validate_provider_stock_window(
        "7203",
        rows,
        {"start": "2026-02-10", "end": "2026-02-12"},
        _metadata(),
    )

    assert [row["date"] for row in validated] == [
        "2026-02-10",
        "2026-02-11",
        "2026-02-12",
    ]


@pytest.mark.parametrize(
    ("field", "corrupt_value"),
    [
        ("adjusted_open", 500.0),
        ("adjusted_high", 500.0),
        ("adjusted_low", 1.0),
        ("adjusted_close", 500.0),
        ("adjusted_volume", 1),
    ],
)
def test_validate_provider_stock_window_rejects_corrupted_adjusted_chain(
    field: str,
    corrupt_value: float | int,
) -> None:
    rows = [
        _row("2026-02-10", future_factor=0.5),
        _row("2026-02-11", factor=0.5, future_factor=1.0),
    ]
    rows[0][field] = corrupt_value

    with pytest.raises(ValueError, match="provider-adjusted consistency"):
        validate_provider_stock_window(
            "7203",
            rows,
            {"start": "2026-02-10", "end": "2026-02-11"},
            _metadata(),
        )


def test_validate_provider_stock_window_accepts_small_price_rounding_tolerance() -> None:
    rows = [
        _row("2026-02-10", future_factor=1 / 3),
        _row("2026-02-11", factor=1 / 3, future_factor=1.0),
    ]
    rows[0]["adjusted_close"] = round(105.0 / 3, 6)

    validate_provider_stock_window(
        "7203",
        rows,
        {"start": "2026-02-10", "end": "2026-02-11"},
        _metadata(),
    )


def test_validate_provider_stock_window_rejects_unobserved_coverage_claims() -> None:
    with pytest.raises(ValueError, match="coverage must equal observed"):
        validate_provider_stock_window(
            "7203",
            [_row("2026-02-10")],
            {"start": "2026-01-01", "end": "2026-02-28"},
            _metadata(),
        )


def test_validate_provider_stock_window_rejects_provider_as_of_before_coverage() -> None:
    metadata = _metadata()
    metadata["provider_as_of"] = "2026-02-09"

    with pytest.raises(ValueError, match="provider as-of must be on or after coverage end"):
        validate_provider_stock_window(
            "7203",
            [_row("2026-02-10")],
            {"start": "2026-02-10", "end": "2026-02-10"},
            metadata,
        )


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("provider_plan", " premium", "whitespace padding"),
        ("provider_plan", "premium plan", "provider plan is invalid"),
        ("provider_as_of", "2026-2-10", "must be an ISO date"),
        ("provider_source_fingerprint", "ABC", "source fingerprint is invalid"),
    ],
)
def test_validate_provider_stock_window_rejects_malformed_metadata(
    key: str, value: str, message: str
) -> None:
    metadata = _metadata()
    metadata[key] = value

    with pytest.raises(ValueError, match=message):
        validate_provider_stock_window(
            "7203",
            [_row("2026-02-10")],
            {"start": "2026-02-10", "end": "2026-02-10"},
            metadata,
        )
