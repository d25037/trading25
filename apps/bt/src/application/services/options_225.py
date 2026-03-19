"""Shared helpers for Nikkei 225 options ingestion and read models."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from src.entrypoints.http.schemas.jquants import (
    N225OptionItem,
    N225OptionsExplorerResponse,
    N225OptionsNumericRange,
    N225OptionsSummary,
)

OPTIONS_225_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
OPTIONS_225_SYNTHETIC_INDEX_NAME = "日経平均"
OPTIONS_225_SYNTHETIC_INDEX_NAME_EN = "Nikkei 225 (UnderPx derived)"
OPTIONS_225_SYNTHETIC_INDEX_CATEGORY = "synthetic"
OPTIONS_225_SYNTHETIC_INDEX_SECTOR_NAME = "日経平均"

PUT_CALL_LABELS = {
    "1": "put",
    "2": "call",
}

EMERGENCY_MARGIN_TRIGGER_LABELS = {
    "001": "emergency_margin_triggered",
    "002": "settlement_price_calculation",
}


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_options_225_date(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("date must not be empty")

    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(raw, date_format).date().isoformat()
        except ValueError:
            continue

    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def _first_present(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in item:
            return item[key]
    return None


def _nullable_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _nullable_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_options_225_raw_row(item: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    return {
        "date": _nullable_text(_first_present(item, "Date")) or "",
        "code": str(_first_present(item, "Code") or ""),
        "whole_day_open": _nullable_float(_first_present(item, "WholeDayOpen", "O")),
        "whole_day_high": _nullable_float(_first_present(item, "WholeDayHigh", "H")),
        "whole_day_low": _nullable_float(_first_present(item, "WholeDayLow", "L")),
        "whole_day_close": _nullable_float(_first_present(item, "WholeDayClose", "C")),
        "night_session_open": _nullable_float(_first_present(item, "NightSessionOpen", "EO")),
        "night_session_high": _nullable_float(_first_present(item, "NightSessionHigh", "EH")),
        "night_session_low": _nullable_float(_first_present(item, "NightSessionLow", "EL")),
        "night_session_close": _nullable_float(_first_present(item, "NightSessionClose", "EC")),
        "day_session_open": _nullable_float(_first_present(item, "DaySessionOpen", "AO")),
        "day_session_high": _nullable_float(_first_present(item, "DaySessionHigh", "AH")),
        "day_session_low": _nullable_float(_first_present(item, "DaySessionLow", "AL")),
        "day_session_close": _nullable_float(_first_present(item, "DaySessionClose", "AC")),
        "volume": _nullable_float(_first_present(item, "Volume", "Vo")),
        "open_interest": _nullable_float(_first_present(item, "OpenInterest", "OI")),
        "turnover_value": _nullable_float(_first_present(item, "TurnoverValue", "Va")),
        "contract_month": _nullable_text(_first_present(item, "ContractMonth", "CM")),
        "strike_price": _nullable_float(_first_present(item, "StrikePrice", "Strike")),
        "only_auction_volume": _nullable_float(_first_present(item, "Volume(OnlyAuction)", "VoOA")),
        "emergency_margin_trigger_division": _nullable_text(
            _first_present(item, "EmergencyMarginTriggerDivision", "EmMrgnTrgDiv")
        ),
        "put_call_division": _nullable_text(_first_present(item, "PutCallDivision", "PCDiv")),
        "last_trading_day": _nullable_text(_first_present(item, "LastTradingDay", "LTD")),
        "special_quotation_day": _nullable_text(_first_present(item, "SpecialQuotationDay", "SQD")),
        "settlement_price": _nullable_float(_first_present(item, "SettlementPrice", "Settle")),
        "theoretical_price": _nullable_float(_first_present(item, "TheoreticalPrice", "Theo")),
        "base_volatility": _nullable_float(_first_present(item, "BaseVolatility", "BaseVol")),
        "underlying_price": _nullable_float(_first_present(item, "UnderlyingPrice", "UnderPx")),
        "implied_volatility": _nullable_float(_first_present(item, "ImpliedVolatility", "IV")),
        "interest_rate": _nullable_float(_first_present(item, "InterestRate", "IR")),
        "created_at": created_at,
    }


def map_options_225_item(item: dict[str, Any]) -> N225OptionItem:
    put_call_division = _nullable_text(item.get("put_call_division"))
    emergency_division = _nullable_text(item.get("emergency_margin_trigger_division"))
    return N225OptionItem(
        date=_nullable_text(item.get("date")) or "",
        code=str(item.get("code") or ""),
        wholeDayOpen=_nullable_float(item.get("whole_day_open")),
        wholeDayHigh=_nullable_float(item.get("whole_day_high")),
        wholeDayLow=_nullable_float(item.get("whole_day_low")),
        wholeDayClose=_nullable_float(item.get("whole_day_close")),
        nightSessionOpen=_nullable_float(item.get("night_session_open")),
        nightSessionHigh=_nullable_float(item.get("night_session_high")),
        nightSessionLow=_nullable_float(item.get("night_session_low")),
        nightSessionClose=_nullable_float(item.get("night_session_close")),
        daySessionOpen=_nullable_float(item.get("day_session_open")),
        daySessionHigh=_nullable_float(item.get("day_session_high")),
        daySessionLow=_nullable_float(item.get("day_session_low")),
        daySessionClose=_nullable_float(item.get("day_session_close")),
        volume=_nullable_float(item.get("volume")),
        openInterest=_nullable_float(item.get("open_interest")),
        turnoverValue=_nullable_float(item.get("turnover_value")),
        contractMonth=_nullable_text(item.get("contract_month")),
        strikePrice=_nullable_float(item.get("strike_price")),
        onlyAuctionVolume=_nullable_float(item.get("only_auction_volume")),
        emergencyMarginTriggerDivision=emergency_division,
        emergencyMarginTriggerLabel=EMERGENCY_MARGIN_TRIGGER_LABELS.get(emergency_division or ""),
        putCallDivision=put_call_division,
        putCallLabel=PUT_CALL_LABELS.get(put_call_division or ""),
        lastTradingDay=_nullable_text(item.get("last_trading_day")),
        specialQuotationDay=_nullable_text(item.get("special_quotation_day")),
        settlementPrice=_nullable_float(item.get("settlement_price")),
        theoreticalPrice=_nullable_float(item.get("theoretical_price")),
        baseVolatility=_nullable_float(item.get("base_volatility")),
        underlyingPrice=_nullable_float(item.get("underlying_price")),
        impliedVolatility=_nullable_float(item.get("implied_volatility")),
        interestRate=_nullable_float(item.get("interest_rate")),
    )


def _range_from_values(values: list[float | None]) -> N225OptionsNumericRange:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return N225OptionsNumericRange()
    return N225OptionsNumericRange(min=min(filtered), max=max(filtered))


def build_options_225_summary(items: list[dict[str, Any]]) -> N225OptionsSummary:
    put_count = sum(1 for item in items if item.get("putCallDivision") == "1")
    call_count = sum(1 for item in items if item.get("putCallDivision") == "2")
    total_volume = sum(float(value) for value in (item.get("volume") for item in items) if value is not None)
    total_open_interest = sum(
        float(value) for value in (item.get("openInterest") for item in items) if value is not None
    )
    return N225OptionsSummary(
        totalCount=len(items),
        putCount=put_count,
        callCount=call_count,
        totalVolume=total_volume,
        totalOpenInterest=total_open_interest,
        strikePriceRange=_range_from_values([item.get("strikePrice") for item in items]),
        underlyingPriceRange=_range_from_values([item.get("underlyingPrice") for item in items]),
        settlementPriceRange=_range_from_values([item.get("settlementPrice") for item in items]),
    )


def build_options_225_response(
    *,
    requested_date: str | None,
    resolved_date: str,
    normalized_rows: list[dict[str, Any]],
    source_call_count: int,
) -> N225OptionsExplorerResponse:
    if not normalized_rows:
        raise ValueError(f"No N225 options data found for {resolved_date}")

    items = [map_options_225_item(row).model_dump() for row in normalized_rows]
    contract_months = sorted(
        {
            item["contractMonth"]
            for item in items
            if isinstance(item.get("contractMonth"), str) and item["contractMonth"]
        }
    )
    return N225OptionsExplorerResponse(
        requestedDate=requested_date,
        resolvedDate=resolved_date,
        lastUpdated=now_iso(),
        sourceCallCount=source_call_count,
        availableContractMonths=contract_months,
        items=[N225OptionItem.model_validate(item) for item in items],
        summary=build_options_225_summary(items),
    )


def _group_underlying_prices_by_date(rows: list[dict[str, Any]]) -> dict[str, set[float]]:
    grouped: dict[str, set[float]] = defaultdict(set)
    for row in rows:
        row_date = _nullable_text(row.get("date"))
        underlying_price = _nullable_float(row.get("underlying_price"))
        if row_date is None:
            continue
        if underlying_price is not None:
            grouped[row_date].add(underlying_price)
        else:
            grouped.setdefault(row_date, set())
    return grouped


def classify_underlying_price_issue_dates(
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    grouped = _group_underlying_prices_by_date(rows)
    missing_dates: list[str] = []
    conflicting_dates: list[str] = []
    for row_date in sorted(grouped):
        values = grouped[row_date]
        if not values:
            missing_dates.append(row_date)
        elif len(values) > 1:
            conflicting_dates.append(row_date)
    return missing_dates, conflicting_dates


def build_synthetic_underpx_index_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = _group_underlying_prices_by_date(rows)
    created_at = now_iso()
    synthetic_rows: list[dict[str, Any]] = []
    for row_date, values in grouped.items():
        if len(values) != 1:
            continue
        value = next(iter(values))
        synthetic_rows.append(
            {
                "code": OPTIONS_225_SYNTHETIC_INDEX_CODE,
                "date": row_date,
                "open": value,
                "high": value,
                "low": value,
                "close": value,
                "sector_name": OPTIONS_225_SYNTHETIC_INDEX_SECTOR_NAME,
                "created_at": created_at,
            }
        )
    return sorted(synthetic_rows, key=lambda row: (str(row["date"]), str(row["code"])))
