"""Helpers for structured HTTP error details."""

from __future__ import annotations

from typing import Any, Protocol

from fastapi import HTTPException

from src.application.services.market_data_errors import MarketDataError
from src.entrypoints.http.schemas.error import ErrorDetail
from src.infrastructure.db.market.query_helpers import stock_code_candidates

LOCAL_STOCK_DATA_MISSING = "local_stock_data_missing"
STOCK_NOT_FOUND = "stock_not_found"
TOPIX_DATA_MISSING = "topix_data_missing"

STOCK_REFRESH = "stock_refresh"
MARKET_DB_SYNC = "market_db_sync"

_STOCK_DATA_ERROR_PATTERNS = (
    "OHLCVデータが取得できません",
    "リサンプル後データが不足しています",
    "ローカルOHLCVデータがありません",
)


class MarketReaderLookup(Protocol):
    def query_one(self, sql: str, params: tuple[str, ...] = ()) -> Any: ...


def build_structured_http_exception(
    status_code: int,
    message: str,
    *,
    reason: str | None = None,
    recovery: str | None = None,
) -> HTTPException:
    details: list[ErrorDetail] = []
    if reason:
        details.append(ErrorDetail(field="reason", message=reason))
    if recovery:
        details.append(ErrorDetail(field="recovery", message=recovery))

    detail: dict[str, Any] = {"message": message}
    if details:
        detail["details"] = [item.model_dump() for item in details]

    return HTTPException(status_code=status_code, detail=detail)


def market_data_http_exception(error: MarketDataError) -> HTTPException:
    return build_structured_http_exception(
        error.status_code,
        error.message,
        reason=error.reason,
        recovery=error.recovery,
    )


def extract_http_exception_detail(detail: Any) -> tuple[str, list[ErrorDetail] | None]:
    if isinstance(detail, str):
        return detail, None

    if isinstance(detail, dict):
        message = detail.get("message")
        raw_details = detail.get("details")
        if isinstance(message, str):
            parsed_details = _parse_error_details(raw_details)
            return message, parsed_details

    return str(detail), None


def classify_market_data_http_exception(
    *,
    stock_code: str,
    source: str,
    raw_message: str,
    market_reader: MarketReaderLookup | None,
    benchmark_code: str | None = None,
    force_lookup: bool = False,
) -> HTTPException | None:
    if benchmark_code == "topix" and (_is_topix_missing_message(raw_message) or force_lookup):
        return build_structured_http_exception(
            404,
            raw_message,
            reason=TOPIX_DATA_MISSING,
            recovery=MARKET_DB_SYNC,
        )

    if source != "market":
        return None

    should_classify_stock = force_lookup or any(
        pattern in raw_message for pattern in _STOCK_DATA_ERROR_PATTERNS
    )
    if not should_classify_stock:
        return None

    stock_exists = stock_exists_in_market_snapshot(market_reader, stock_code)
    return build_structured_http_exception(
        404,
        raw_message,
        reason=LOCAL_STOCK_DATA_MISSING if stock_exists else STOCK_NOT_FOUND,
        recovery=STOCK_REFRESH if stock_exists else None,
    )


def stock_exists_in_market_snapshot(
    market_reader: MarketReaderLookup | None,
    stock_code: str,
) -> bool:
    if market_reader is None:
        return False

    candidates = stock_code_candidates(stock_code)
    placeholders = ",".join("?" for _ in candidates)
    row = market_reader.query_one(
        f"SELECT code FROM stocks WHERE code IN ({placeholders}) "
        "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
        tuple(candidates),
    )
    return row is not None


def _is_topix_missing_message(raw_message: str) -> bool:
    lowered = raw_message.lower()
    return (
        "TOPIX のローカルデータがありません" in raw_message
        or ("topix" in lowered and ("ベンチマーク" in raw_message or "benchmark" in lowered))
    )


def _parse_error_details(raw_details: Any) -> list[ErrorDetail] | None:
    if not isinstance(raw_details, list):
        return None

    parsed: list[ErrorDetail] = []
    for item in raw_details:
        if isinstance(item, ErrorDetail):
            parsed.append(item)
            continue
        if not isinstance(item, dict):
            continue
        field = item.get("field")
        message = item.get("message")
        if isinstance(field, str) and isinstance(message, str):
            parsed.append(ErrorDetail(field=field, message=message))

    return parsed or None
