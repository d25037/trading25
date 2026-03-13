"""Structured market-data errors for chart-facing endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class MarketDataError(ValueError):
    """ValueError carrying structured recovery metadata for HTTP responses."""

    message: str
    reason: str
    recovery: str | None = None
    status_code: int = 404

    def __post_init__(self) -> None:
        ValueError.__init__(self, self.message)


def to_http_error_detail(error: MarketDataError) -> dict[str, Any]:
    details = [{"field": "reason", "message": error.reason}]
    if error.recovery:
        details.append({"field": "recovery", "message": error.recovery})
    return {"message": error.message, "details": details}
