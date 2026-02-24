from __future__ import annotations

from src.application.services.stock_data_row_builder import build_stock_data_row


def test_build_stock_data_row_with_raw_fields() -> None:
    row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": 100,
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 1000,
            "AdjFactor": 1.0,
        },
        created_at="2026-02-12T00:00:00+00:00",
    )

    assert row is not None
    assert row["code"] == "7203"
    assert row["open"] == 100.0
    assert row["volume"] == 1000
    assert row["created_at"] == "2026-02-12T00:00:00+00:00"


def test_build_stock_data_row_prefers_adjusted_fields() -> None:
    row = build_stock_data_row(
        {
            "Date": "2026-02-10",
            "AdjO": 201,
            "O": 100,
            "AdjH": 211,
            "H": 110,
            "AdjL": 191,
            "L": 90,
            "AdjC": 205,
            "C": 105,
            "AdjVo": 777,
            "Vo": 1000,
            "AdjFactor": "0.5",
        },
        normalized_code="131A",
    )

    assert row is not None
    assert row["code"] == "131A"
    assert row["open"] == 201.0
    assert row["high"] == 211.0
    assert row["low"] == 191.0
    assert row["close"] == 205.0
    assert row["volume"] == 777
    assert row["adjustment_factor"] == 0.5


def test_build_stock_data_row_returns_none_for_invalid_code_or_date() -> None:
    assert build_stock_data_row({"Code": "", "Date": "2026-02-10", "O": 1, "H": 1, "L": 1, "C": 1, "Vo": 1}) is None
    assert build_stock_data_row({"Code": "72030", "Date": None, "O": 1, "H": 1, "L": 1, "C": 1, "Vo": 1}) is None


def test_build_stock_data_row_returns_none_for_missing_ohlcv() -> None:
    row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": 100,
            "H": 110,
            "L": 90,
            "C": None,
            "Vo": 1000,
        }
    )
    assert row is None


def test_build_stock_data_row_handles_string_numeric_inputs() -> None:
    row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": "100.5",
            "H": "110.5",
            "L": "90.5",
            "C": "105.5",
            "Vo": "1000",
            "AdjFactor": " ",
        }
    )

    assert row is not None
    assert row["open"] == 100.5
    assert row["volume"] == 1000
    assert row["adjustment_factor"] is None


def test_build_stock_data_row_rejects_bool_and_non_numeric_values() -> None:
    bool_row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": True,
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 1000,
        }
    )
    text_row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": "abc",
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 1000,
        }
    )

    assert bool_row is None
    assert text_row is None


def test_build_stock_data_row_rejects_non_finite_numbers() -> None:
    inf_row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": float("inf"),
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 1000,
        }
    )
    nan_row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": 100,
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": float("nan"),
        }
    )

    assert inf_row is None
    assert nan_row is None
