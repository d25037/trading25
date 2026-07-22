from __future__ import annotations

import pytest

from src.application.services.stock_data_row_builder import build_stock_data_row
from src.application.services.sync_row_converters import convert_stock_bulk_rows


def test_build_stock_data_row_preserves_provider_raw_and_adjusted_fields() -> None:
    row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": 100,
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 1000,
            "Va": 105_500.25,
            "AdjFactor": 1.0,
            "AdjO": 100.125,
            "AdjH": 110.125,
            "AdjL": 90.125,
            "AdjC": 105.125,
            "AdjVo": 999,
        },
        created_at="2026-02-12T00:00:00+00:00",
    )

    assert row is not None
    assert row["code"] == "7203"
    assert row["open"] == 100.0
    assert row["volume"] == 1000
    assert row["turnover_value"] == 105_500.25
    assert row["adjustment_factor"] == 1.0
    assert row["adjusted_open"] == 100.125
    assert row["adjusted_high"] == 110.125
    assert row["adjusted_low"] == 90.125
    assert row["adjusted_close"] == 105.125
    assert row["adjusted_volume"] == 999
    assert row["created_at"] == "2026-02-12T00:00:00+00:00"


def test_build_stock_data_row_preserves_fractional_provider_adjusted_volume() -> None:
    row = build_stock_data_row(
        {
            "Code": "72030",
            "Date": "2026-02-10",
            "O": 100,
            "H": 110,
            "L": 90,
            "C": 105,
            "Vo": 8_730_892,
            "Va": 105_500.25,
            "AdjFactor": 1.0,
            "AdjO": 100,
            "AdjH": 110,
            "AdjL": 90,
            "AdjC": 105,
            "AdjVo": 87_308.9,
        }
    )

    assert row is not None
    assert row["volume"] == 8_730_892
    assert row["adjusted_volume"] == 87_308.9


@pytest.mark.parametrize("adjusted_volume", [-0.1, float("nan"), float("inf")])
def test_build_stock_data_row_rejects_invalid_provider_adjusted_volume(
    adjusted_volume: float,
) -> None:
    assert (
        build_stock_data_row(
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 100,
                "H": 110,
                "L": 90,
                "C": 105,
                "Vo": 8_730_892,
                "Va": 105_500.25,
                "AdjFactor": 1.0,
                "AdjO": 100,
                "AdjH": 110,
                "AdjL": 90,
                "AdjC": 105,
                "AdjVo": adjusted_volume,
            }
        )
        is None
    )


def test_build_stock_data_row_rejects_fractional_raw_volume() -> None:
    assert (
        build_stock_data_row(
            {
                "Code": "72030",
                "Date": "2026-02-10",
                "O": 100,
                "H": 110,
                "L": 90,
                "C": 105,
                "Vo": 8_730_892.5,
                "Va": 105_500.25,
                "AdjFactor": 1.0,
                "AdjO": 100,
                "AdjH": 110,
                "AdjL": 90,
                "AdjC": 105,
                "AdjVo": 87_308.9,
            }
        )
        is None
    )


def test_build_stock_data_row_keeps_raw_and_adjusted_ohlcv_separate() -> None:
    row = build_stock_data_row(
        {
            "Code": "131A0",
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
            "Va": 100_000,
            "AdjFactor": "0.5",
        },
        normalized_code="131A",
    )

    assert row is not None
    assert row["code"] == "131A"
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 90.0
    assert row["close"] == 105.0
    assert row["volume"] == 1000
    assert row["adjustment_factor"] == 0.5
    assert row["adjusted_open"] == 201.0
    assert row["adjusted_high"] == 211.0
    assert row["adjusted_low"] == 191.0
    assert row["adjusted_close"] == 205.0
    assert row["adjusted_volume"] == 777


def test_build_stock_data_row_rejects_incomplete_provider_adjusted_fields() -> None:
    quote = {
        "Code": "72030",
        "Date": "2026-02-10",
        "O": 100,
        "H": 110,
        "L": 90,
        "C": 105,
        "Vo": 1000,
        "Va": 105_000,
        "AdjFactor": 1.0,
        "AdjO": 100,
        "AdjH": 110,
        "AdjL": 90,
        "AdjC": 105,
        "AdjVo": 1000,
    }

    for key in ("Va", "AdjFactor", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"):
        incomplete = dict(quote)
        incomplete[key] = None
        assert build_stock_data_row(incomplete) is None


def test_build_stock_data_row_rejects_non_finite_provider_adjusted_fields() -> None:
    quote = {
        "Code": "72030",
        "Date": "2026-02-10",
        "O": 100,
        "H": 110,
        "L": 90,
        "C": 105,
        "Vo": 1000,
        "Va": 105_000,
        "AdjFactor": 1.0,
        "AdjO": 100,
        "AdjH": 110,
        "AdjL": 90,
        "AdjC": 105,
        "AdjVo": 1000,
    }

    for key in ("Va", "AdjFactor", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"):
        invalid = dict(quote)
        invalid[key] = float("nan")
        assert build_stock_data_row(invalid) is None


def test_convert_stock_bulk_rows_preserves_provider_fields_after_alias_normalization() -> (
    None
):
    rows = convert_stock_bulk_rows(
        [
            {
                "code": "72030",
                "date": "20260210",
                "open": "100",
                "high": "110",
                "low": "90",
                "close": "105",
                "volume": "1000",
                "turnover_value": "105500.25",
                "adjfactor": "0.5",
                "adjopen": "50",
                "adjhigh": "55",
                "adjlow": "45",
                "adjclose": "52.5",
                "adjvolume": "2000",
            }
        ],
        target_dates={"2026-02-10"},
    )

    assert len(rows) == 1
    assert rows[0]["turnover_value"] == 105500.25
    assert rows[0]["adjustment_factor"] == 0.5
    assert rows[0]["adjusted_open"] == 50.0
    assert rows[0]["adjusted_high"] == 55.0
    assert rows[0]["adjusted_low"] == 45.0
    assert rows[0]["adjusted_close"] == 52.5
    assert rows[0]["adjusted_volume"] == 2000


def test_convert_stock_bulk_rows_accepts_v3_style_raw_rows_for_sql_projection() -> None:
    rows = convert_stock_bulk_rows(
        [
            {
                "Code": "72030",
                "Date": "2021-09-28",
                "O": 10420,
                "H": 10460,
                "L": 10250,
                "C": 10385,
                "Vo": 7_563_400,
                "Va": 78_532_052_000,
                "AdjFactor": 1.0,
            }
        ],
        target_dates={"2021-09-28"},
        allow_raw_only=True,
    )

    assert rows[0]["close"] == 10385.0
    assert rows[0]["adjusted_close"] is None
    assert rows[0]["adjusted_volume"] is None


def test_convert_stock_bulk_rows_rejects_incomplete_adjusted_row_for_retry() -> None:
    with pytest.raises(ValueError, match="incomplete provider daily row.*7203.*2026-02-10"):
        convert_stock_bulk_rows(
            [
                {
                    "code": "72030",
                    "date": "20260210",
                    "open": 100,
                    "high": 110,
                    "low": 90,
                    "close": 105,
                    "volume": 1000,
                    "turnover_value": 105000,
                    "adjfactor": 1,
                    "adjopen": 100,
                    "adjhigh": 110,
                    "adjlow": 90,
                    "adjclose": None,
                    "adjvolume": 1000,
                }
            ],
            target_dates={"2026-02-10"},
        )


def test_convert_stock_bulk_rows_skips_legitimate_no_trade_row() -> None:
    assert (
        convert_stock_bulk_rows(
            [
                {
                    "code": "72030",
                    "date": "20260210",
                    "adjfactor": 1,
                }
            ],
            target_dates={"2026-02-10"},
        )
        == []
    )


@pytest.mark.parametrize(
    "factor",
    [
        pytest.param(0.5, id="non-unit"),
        pytest.param(0.0, id="zero"),
        pytest.param(-0.5, id="negative"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="infinite"),
    ],
)
def test_convert_stock_bulk_rows_rejects_no_trade_row_with_invalid_factor(
    factor: float,
) -> None:
    with pytest.raises(ValueError, match="incomplete provider daily row.*7203.*2026-02-10"):
        convert_stock_bulk_rows(
            [
                {
                    "code": "72030",
                    "date": "20260210",
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": None,
                    "turnover_value": None,
                    "adjfactor": factor,
                    "adjopen": None,
                    "adjhigh": None,
                    "adjlow": None,
                    "adjclose": None,
                    "adjvolume": None,
                }
            ],
            target_dates={"2026-02-10"},
        )


@pytest.mark.parametrize(
    ("include_factor", "factor"),
    [
        pytest.param(False, None, id="absent"),
        pytest.param(True, 1.0, id="unit"),
    ],
)
def test_convert_stock_bulk_rows_skips_no_trade_row_with_absent_or_unit_factor(
    include_factor: bool,
    factor: float | None,
) -> None:
    row: dict[str, object] = {
        "code": "72030",
        "date": "20260210",
    }
    if include_factor:
        row["adjfactor"] = factor

    assert convert_stock_bulk_rows([row], target_dates={"2026-02-10"}) == []


def test_build_stock_data_row_returns_none_for_invalid_code_or_date() -> None:
    assert (
        build_stock_data_row(
            {"Code": "", "Date": "2026-02-10", "O": 1, "H": 1, "L": 1, "C": 1, "Vo": 1}
        )
        is None
    )
    assert (
        build_stock_data_row(
            {
                "Code": "72030",
                "Date": "not-a-date",
                "O": 1,
                "H": 1,
                "L": 1,
                "C": 1,
                "Vo": 1,
                "Va": 1,
                "AdjFactor": 1,
                "AdjO": 1,
                "AdjH": 1,
                "AdjL": 1,
                "AdjC": 1,
                "AdjVo": 1,
            }
        )
        is None
    )


def test_build_stock_data_row_rejects_payload_code_mismatch() -> None:
    quote = {
        "Code": "67580",
        "Date": "2026-02-10",
        "O": 100,
        "H": 110,
        "L": 90,
        "C": 105,
        "Vo": 1000,
        "Va": 105_000,
        "AdjFactor": 1.0,
        "AdjO": 100,
        "AdjH": 110,
        "AdjL": 90,
        "AdjC": 105,
        "AdjVo": 1000,
    }

    assert build_stock_data_row(quote, normalized_code="7203") is None
    assert (
        build_stock_data_row(
            {"Code": "72030", "Date": None, "O": 1, "H": 1, "L": 1, "C": 1, "Vo": 1}
        )
        is None
    )


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
            "Va": "105500.5",
            "AdjFactor": "1",
            "AdjO": "100.5",
            "AdjH": "110.5",
            "AdjL": "90.5",
            "AdjC": "105.5",
            "AdjVo": "1000",
        }
    )

    assert row is not None
    assert row["open"] == 100.5
    assert row["volume"] == 1000
    assert row["adjustment_factor"] == 1.0


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
