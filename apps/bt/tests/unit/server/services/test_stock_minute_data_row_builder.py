from __future__ import annotations

from src.application.services.stock_minute_data_row_builder import (
    _coerce_date,
    _coerce_float,
    _coerce_int,
    _coerce_time,
    _pick_first,
    build_stock_minute_data_row,
)


def test_pick_first_returns_first_non_none_value() -> None:
    assert _pick_first({"A": None, "B": 10}, "A", "B", "C") == 10
    assert _pick_first({"A": None}, "A", "B") is None


def test_coerce_date_and_time_handle_empty_and_compact_values() -> None:
    assert _coerce_date(None) is None
    assert _coerce_date(" 2026-02-10 ") == "2026-02-10"
    assert _coerce_time(None) is None
    assert _coerce_time("   ") is None
    assert _coerce_time("0900") == "09:00"
    assert _coerce_time("09:01") == "09:01"


def test_coerce_float_and_int_reject_invalid_values() -> None:
    assert _coerce_float(None) is None
    assert _coerce_float(True) is None
    assert _coerce_float("  ") is None
    assert _coerce_float("abc") is None
    assert _coerce_float("nan") is None
    assert _coerce_float("1.5") == 1.5
    assert _coerce_float(object()) is None
    assert _coerce_int(None) is None
    assert _coerce_int("10.0") == 10


def test_build_stock_minute_data_row_returns_none_for_missing_identity_or_ohlcv() -> None:
    assert build_stock_minute_data_row({}) is None
    assert build_stock_minute_data_row({"Code": "72030", "Date": "", "Time": "0900"}) is None
    assert (
        build_stock_minute_data_row(
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "Time": "0900",
                "O": 100.0,
                "H": 101.0,
                "L": 99.0,
                "C": 100.5,
                "Vo": None,
            }
        )
        is None
    )


def test_build_stock_minute_data_row_uses_normalized_code_and_created_at() -> None:
    row = build_stock_minute_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "Time": "0900",
            "O": "100.0",
            "H": "101.0",
            "L": "99.0",
            "C": "100.5",
            "Vo": "1000",
            "Va": "100500.0",
        },
        normalized_code="7203",
        created_at="2026-02-10T00:00:00+00:00",
    )

    assert row == {
        "code": "7203",
        "date": "2026-02-10",
        "time": "09:00",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1000,
        "turnover_value": 100500.0,
        "created_at": "2026-02-10T00:00:00+00:00",
    }
