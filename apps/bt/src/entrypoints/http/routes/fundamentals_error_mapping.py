"""Shared HTTP error contract for Fundamentals PIT routes."""

from __future__ import annotations

from typing import Any, NoReturn

from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.entrypoints.http.error_utils import build_structured_http_exception
from src.entrypoints.http.schemas.error import ErrorResponse


FUNDAMENTALS_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorResponse, "description": "Stock not listed as of the PIT date"},
    409: {"model": ErrorResponse, "description": "Fundamentals PIT snapshot unavailable"},
    422: {"model": ErrorResponse, "description": "Invalid fundamentals request"},
}


def raise_fundamentals_http_error(exc: FundamentalsPitSnapshotError) -> NoReturn:
    """Map one typed PIT failure identically for every Fundamentals endpoint."""
    status = 404 if exc.reason == "stock_not_listed_as_of" else 409
    recovery = None if status == 404 else "market_db_sync"
    raise build_structured_http_exception(
        status,
        str(exc),
        reason=exc.reason,
        recovery=recovery,
    ) from exc
