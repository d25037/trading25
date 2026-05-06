from __future__ import annotations

from typing import Any

import pytest

from src.application.services.moomoo_market_data_service import MoomooMarketDataService


class _FakeClient:
    def status(self) -> dict[str, Any]:
        return {"enabled": True}

    def search_us_stocks(self, query: str, limit: int) -> dict[str, Any]:
        return {"query": query, "limit": limit}

    def get_us_history(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
        max_rows: int | None,
    ) -> dict[str, Any]:
        return {"symbol": symbol, "start": start, "end": end, "maxRows": max_rows}

    def get_us_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        return {"symbols": symbols}


@pytest.mark.asyncio
async def test_moomoo_market_data_service_delegates_to_blocking_client() -> None:
    service = MoomooMarketDataService(_FakeClient())  # type: ignore[arg-type]

    assert await service.get_status() == {"enabled": True}
    assert await service.search_us_stocks("AAPL", 5) == {"query": "AAPL", "limit": 5}
    assert await service.get_us_history("AAPL", "2025-01-01", "2025-01-31", 10) == {
        "symbol": "AAPL",
        "start": "2025-01-01",
        "end": "2025-01-31",
        "maxRows": 10,
    }
    assert await service.get_us_snapshot(["AAPL"]) == {"symbols": ["AAPL"]}
