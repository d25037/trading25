from __future__ import annotations

import pytest

from src.application.services import options_225 as options_225_module
from src.application.services.options_225 import (
    OPTIONS_225_SYNTHETIC_INDEX_CODE,
    OPTIONS_225_SYNTHETIC_INDEX_SECTOR_NAME,
    build_options_225_response,
    build_options_225_summary,
    build_synthetic_underpx_index_rows,
    classify_underlying_price_issue_dates,
    map_options_225_item,
    normalize_options_225_date,
    normalize_options_225_raw_row,
)


def test_normalize_options_225_date_accepts_iso_and_compact_formats() -> None:
    assert normalize_options_225_date("2024-01-16") == "2024-01-16"
    assert normalize_options_225_date("20240116") == "2024-01-16"


def test_normalize_options_225_date_rejects_empty_and_invalid_formats() -> None:
    with pytest.raises(ValueError, match="date must not be empty"):
        normalize_options_225_date("  ")

    with pytest.raises(ValueError, match="date must be YYYY-MM-DD or YYYYMMDD"):
        normalize_options_225_date("2024/01/16")


def test_normalize_raw_row_and_map_item_handle_short_keys_and_invalid_values() -> None:
    row = normalize_options_225_raw_row(
        {
            "Date": " 2024-01-16 ",
            "Code": 131040018,
            "O": "1.5",
            "H": 2,
            "L": " ",
            "C": None,
            "EO": "3.5",
            "Vo": "",
            "OI": "10",
            "Va": "15.5",
            "CM": " 2024-04 ",
            "Strike": "36000",
            "VoOA": " ",
            "EmMrgnTrgDiv": "001",
            "PCDiv": "1",
            "LTD": "2024-04-11",
            "SQD": "2024-04-12",
            "Settle": "7.0",
            "Theo": "7.5",
            "BaseVol": "bad",
            "UnderPx": "36100.0",
            "IV": "10.2",
            "IR": object(),
        },
        created_at="2026-03-19T00:00:00+00:00",
    )

    assert row == {
        "date": "2024-01-16",
        "code": "131040018",
        "whole_day_open": 1.5,
        "whole_day_high": 2.0,
        "whole_day_low": None,
        "whole_day_close": None,
        "night_session_open": 3.5,
        "night_session_high": None,
        "night_session_low": None,
        "night_session_close": None,
        "day_session_open": None,
        "day_session_high": None,
        "day_session_low": None,
        "day_session_close": None,
        "volume": None,
        "open_interest": 10.0,
        "turnover_value": 15.5,
        "contract_month": "2024-04",
        "strike_price": 36000.0,
        "only_auction_volume": None,
        "emergency_margin_trigger_division": "001",
        "put_call_division": "1",
        "last_trading_day": "2024-04-11",
        "special_quotation_day": "2024-04-12",
        "settlement_price": 7.0,
        "theoretical_price": 7.5,
        "base_volatility": None,
        "underlying_price": 36100.0,
        "implied_volatility": 10.2,
        "interest_rate": None,
        "created_at": "2026-03-19T00:00:00+00:00",
    }

    item = map_options_225_item(row)

    assert item.putCallLabel == "put"
    assert item.emergencyMarginTriggerLabel == "emergency_margin_triggered"
    assert item.volume is None
    assert item.baseVolatility is None
    assert item.interestRate is None


def test_build_options_225_summary_and_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(options_225_module, "now_iso", lambda: "2026-03-19T12:34:56+00:00")
    normalized_rows = [
        normalize_options_225_raw_row(
            {
                "Date": "2024-01-16",
                "Code": "131040018",
                "Volume": 10,
                "OpenInterest": 100,
                "ContractMonth": "2024-04",
                "StrikePrice": 36000,
                "PutCallDivision": "1",
                "SettlementPrice": 8,
                "UnderlyingPrice": 36100,
            }
        ),
        normalize_options_225_raw_row(
            {
                "Date": "2024-01-16",
                "Code": "141040018",
                "Volume": 15,
                "OpenInterest": 200,
                "ContractMonth": "2024-05",
                "StrikePrice": 37000,
                "PutCallDivision": "2",
                "SettlementPrice": 12,
                "UnderlyingPrice": 36150,
            }
        ),
    ]

    response = build_options_225_response(
        requested_date=None,
        resolved_date="2024-01-16",
        normalized_rows=normalized_rows,
        source_call_count=0,
    )

    assert response.lastUpdated == "2026-03-19T12:34:56+00:00"
    assert response.availableContractMonths == ["2024-04", "2024-05"]
    assert response.summary.totalCount == 2
    assert response.summary.putCount == 1
    assert response.summary.callCount == 1
    assert response.summary.totalVolume == 25.0
    assert response.summary.totalOpenInterest == 300.0
    assert response.summary.strikePriceRange.min == 36000.0
    assert response.summary.strikePriceRange.max == 37000.0
    assert response.summary.underlyingPriceRange.min == 36100.0
    assert response.summary.underlyingPriceRange.max == 36150.0
    assert response.summary.settlementPriceRange.min == 8.0
    assert response.summary.settlementPriceRange.max == 12.0

    empty_summary = build_options_225_summary([{"putCallDivision": "1"}])
    assert empty_summary.totalCount == 1
    assert empty_summary.strikePriceRange.min is None
    assert empty_summary.strikePriceRange.max is None


def test_build_options_225_response_raises_when_rows_are_empty() -> None:
    with pytest.raises(ValueError, match="No N225 options data found for 2024-01-16"):
        build_options_225_response(
            requested_date="2024-01-16",
            resolved_date="2024-01-16",
            normalized_rows=[],
            source_call_count=0,
        )


def test_classify_underlying_issues_and_build_synthetic_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(options_225_module, "now_iso", lambda: "2026-03-19T12:00:00+00:00")
    rows = [
        {"date": "2024-01-17", "underlying_price": 36200},
        {"date": "2024-01-16", "underlying_price": None},
        {"date": "2024-01-18", "underlying_price": 36300},
        {"date": "2024-01-17", "underlying_price": "36250"},
        {"date": "2024-01-18", "underlying_price": "36300"},
        {"date": None, "underlying_price": 99999},
        {"date": "2024-01-19", "underlying_price": ""},
    ]

    missing_dates, conflicting_dates = classify_underlying_price_issue_dates(rows)

    assert missing_dates == ["2024-01-16", "2024-01-19"]
    assert conflicting_dates == ["2024-01-17"]

    synthetic_rows = build_synthetic_underpx_index_rows(rows)

    assert synthetic_rows == [
        {
            "code": OPTIONS_225_SYNTHETIC_INDEX_CODE,
            "date": "2024-01-18",
            "open": 36300.0,
            "high": 36300.0,
            "low": 36300.0,
            "close": 36300.0,
            "sector_name": OPTIONS_225_SYNTHETIC_INDEX_SECTOR_NAME,
            "created_at": "2026-03-19T12:00:00+00:00",
        }
    ]
