"""Read-only moomoo OpenD market data service."""

from __future__ import annotations

import asyncio
from typing import Any

from src.infrastructure.external_api.clients.moomoo_quote_client import MoomooQuoteClient


class MoomooMarketDataService:
    """Async service facade for blocking moomoo OpenD quote calls."""

    def __init__(self, client: MoomooQuoteClient) -> None:
        self._client = client

    async def get_status(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._client.status)

    async def search_us_stocks(self, query: str, limit: int) -> dict[str, Any]:
        return await asyncio.to_thread(self._client.search_us_stocks, query, limit)

    async def get_us_history(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
        max_rows: int | None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._client.get_us_history, symbol, start, end, max_rows)

    async def get_us_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        return await asyncio.to_thread(self._client.get_us_snapshot, symbols)
