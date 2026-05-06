from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.application.services.moomoo_market_data_service import MoomooMarketDataService
from src.entrypoints.http.routes.moomoo import router
from src.infrastructure.external_api.clients.moomoo_quote_client import MoomooOpenDError


class _FakeMoomooService:
    async def get_status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "sdkInstalled": False,
            "openDReachable": False,
            "quoteContextReady": False,
            "host": "127.0.0.1",
            "port": 11111,
            "message": "moomoo Python SDK is not installed",
        }

    async def search_us_stocks(self, query: str, limit: int) -> dict[str, Any]:
        return {
            "query": query,
            "items": [
                {
                    "code": "US.AAPL",
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "lotSize": 1,
                }
            ][:limit],
            "count": 1,
            "lastUpdated": "2025-01-02T00:00:00+00:00",
        }

    async def get_us_history(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
        max_rows: int | None,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol.upper(),
            "code": "US.AAPL",
            "timeframe": "1d",
            "adjustment": "qfq",
            "rows": [
                {
                    "code": "US.AAPL",
                    "timeKey": "2025-01-02 00:00:00",
                    "open": 100,
                    "close": 101,
                    "high": 102,
                    "low": 99,
                }
            ],
            "count": 1,
            "hasMore": False,
            "lastUpdated": "2025-01-02T00:00:00+00:00",
        }

    async def get_us_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        return {
            "symbols": [symbol.upper() for symbol in symbols],
            "items": [
                {
                    "code": "US.AAPL",
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "lastPrice": 101,
                }
            ],
            "count": 1,
            "lastUpdated": "2025-01-02T00:00:00+00:00",
        }


class _FailingMoomooService(_FakeMoomooService):
    async def search_us_stocks(self, query: str, limit: int) -> dict[str, Any]:
        raise MoomooOpenDError(503, "moomoo OpenD is not reachable")

    async def get_us_history(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
        max_rows: int | None,
    ) -> dict[str, Any]:
        raise MoomooOpenDError(503, "moomoo OpenD is not reachable")

    async def get_us_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        raise MoomooOpenDError(503, "moomoo OpenD is not reachable")


def _client(service: MoomooMarketDataService | _FakeMoomooService) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.moomoo_market_data_service = service
    return TestClient(app)


def test_status_is_read_only_and_reports_sdk_state() -> None:
    client = _client(_FakeMoomooService())
    resp = client.get("/api/moomoo/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "read_only"
    assert body["tradeApiEnabled"] is False
    assert body["sdkInstalled"] is False


def test_us_search_history_and_snapshot_routes() -> None:
    client = _client(_FakeMoomooService())

    search_resp = client.get("/api/moomoo/us/stocks/search?query=AAPL")
    assert search_resp.status_code == 200
    assert search_resp.json()["items"][0]["symbol"] == "AAPL"

    history_resp = client.get("/api/moomoo/us/history?symbol=AAPL&from=2025-01-01&to=2025-01-31")
    assert history_resp.status_code == 200
    assert history_resp.json()["rows"][0]["close"] == 101

    snapshot_resp = client.get("/api/moomoo/us/snapshot?symbols=AAPL")
    assert snapshot_resp.status_code == 200
    assert snapshot_resp.json()["items"][0]["lastPrice"] == 101


def test_history_rejects_reversed_date_range() -> None:
    client = _client(_FakeMoomooService())
    resp = client.get("/api/moomoo/us/history?symbol=AAPL&from=2025-02-01&to=2025-01-01")
    assert resp.status_code == 422


def test_service_errors_are_mapped_to_http_errors() -> None:
    client = _client(_FailingMoomooService())
    search_resp = client.get("/api/moomoo/us/stocks/search?query=AAPL")
    assert search_resp.status_code == 503

    history_resp = client.get("/api/moomoo/us/history?symbol=AAPL")
    assert history_resp.status_code == 503

    resp = client.get("/api/moomoo/us/snapshot?symbols=AAPL")
    assert resp.status_code == 503


def test_missing_service_returns_503() -> None:
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    resp = client.get("/api/moomoo/status")
    assert resp.status_code == 503
