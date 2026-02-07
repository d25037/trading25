"""Analytics fundamentals GET ルートのテスト"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.schemas.fundamentals import (
    FundamentalDataPoint,
    FundamentalsComputeResponse,
)


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    return TestClient(app, raise_server_exceptions=False)


def _make_response(symbol: str = "7203", data_count: int = 1) -> FundamentalsComputeResponse:
    """テスト用レスポンスを生成"""
    data = [
        FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-10",
            periodType="FY",
            isConsolidated=True,
            roe=12.5,
            eps=350.0,
            bps=2800.0,
        )
    ] * data_count
    return FundamentalsComputeResponse(
        symbol=symbol,
        companyName="Toyota Motor",
        data=data,
        latestMetrics=data[0] if data else None,
        dailyValuation=None,
        lastUpdated="2024-06-01T00:00:00Z",
    )


class TestGetFundamentals:
    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_basic(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get("/api/analytics/fundamentals/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "7203"
        assert data["companyName"] == "Toyota Motor"
        assert len(data["data"]) == 1
        assert data["data"][0]["periodType"] == "FY"

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_not_found(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = FundamentalsComputeResponse(
            symbol="9999",
            companyName=None,
            data=[],
            latestMetrics=None,
            dailyValuation=None,
            lastUpdated="2024-06-01T00:00:00Z",
        )
        resp = client.get("/api/analytics/fundamentals/9999")
        assert resp.status_code == 404
        assert "9999" in resp.json()["message"]

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_query_params(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={"from": "2020-01-01", "to": "2024-12-31", "periodType": "FY"},
        )
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.from_date == "2020-01-01"
        assert call_args.to_date == "2024-12-31"
        assert call_args.period_type == "FY"

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_default_params(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get("/api/analytics/fundamentals/7203")
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.from_date is None
        assert call_args.to_date is None
        assert call_args.period_type == "all"
        assert call_args.prefer_consolidated is True

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_prefer_consolidated_false(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={"preferConsolidated": "false"},
        )
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.prefer_consolidated is False
