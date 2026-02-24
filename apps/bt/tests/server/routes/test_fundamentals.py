"""
Fundamentals API Routes テスト

/api/fundamentals/compute エンドポイントのテスト
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import app
from src.entrypoints.http.schemas.fundamentals import (
    FundamentalDataPoint,
    FundamentalsComputeResponse,
)


@pytest.fixture
def client():
    """TestClient fixture"""
    return TestClient(app)


class TestFundamentalsComputeEndpoint:
    """POST /api/fundamentals/compute のテスト"""

    def test_compute_success(self, client: TestClient):
        """正常なリクエストで指標が計算される"""
        mock_response = FundamentalsComputeResponse(
            symbol="7203",
            companyName="トヨタ自動車",
            data=[
                FundamentalDataPoint(
                    date="2024-03-31",
                    disclosedDate="2024-05-15",
                    periodType="FY",
                    isConsolidated=True,
                    accountingStandard="JGAAP",
                    roe=13.33,
                    eps=300.0,
                    dilutedEps=290.0,
                    bps=2250.0,
                    per=20.0,
                    pbr=2.67,
                    roa=4.44,
                    operatingMargin=11.11,
                    netMargin=8.89,
                    stockPrice=6000.0,
                    netProfit=4000000.0,
                    equity=30000000.0,
                    totalAssets=90000000.0,
                    netSales=45000000.0,
                    operatingProfit=5000000.0,
                    cashFlowOperating=6000000.0,
                    cashFlowInvesting=-2000000.0,
                    cashFlowFinancing=-1000000.0,
                    cashAndEquivalents=8000000.0,
                    fcf=4000000.0,
                    fcfYield=0.5,
                    fcfMargin=8.89,
                    forecastEps=350.0,
                    forecastEpsChangeRate=16.67,
                    revisedForecastEps=None,
                    revisedForecastSource=None,
                    prevCashFlowOperating=None,
                    prevCashFlowInvesting=None,
                    prevCashFlowFinancing=None,
                    prevCashAndEquivalents=None,
                )
            ],
            latestMetrics=None,
            dailyValuation=None,
            tradingValuePeriod=15,
            lastUpdated="2024-05-15T10:00:00",
        )

        with patch(
            "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
            return_value=mock_response,
        ):
            response = client.post(
                "/api/fundamentals/compute",
                json={"symbol": "7203"},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["symbol"] == "7203"
            assert data["companyName"] == "トヨタ自動車"
            assert len(data["data"]) == 1
            assert data["data"][0]["eps"] == 300.0

    def test_compute_with_all_params(self, client: TestClient):
        """全パラメータを指定したリクエスト"""
        mock_response = FundamentalsComputeResponse(
            symbol="7203",
            companyName=None,
            data=[],
            latestMetrics=None,
            dailyValuation=None,
            tradingValuePeriod=15,
            lastUpdated="2024-05-15T10:00:00",
        )

        with patch(
            "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
            return_value=mock_response,
        ):
            response = client.post(
                "/api/fundamentals/compute",
                json={
                    "symbol": "7203",
                    "from_date": "2020-01-01",
                    "to_date": "2024-12-31",
                    "period_type": "FY",
                    "prefer_consolidated": False,
                },
            )

            assert response.status_code == 200

    def test_compute_no_data(self, client: TestClient):
        """データが見つからない場合"""
        mock_response = FundamentalsComputeResponse(
            symbol="9999",
            companyName=None,
            data=[],
            latestMetrics=None,
            dailyValuation=None,
            tradingValuePeriod=15,
            lastUpdated="2024-05-15T10:00:00",
        )

        with patch(
            "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
            return_value=mock_response,
        ):
            response = client.post(
                "/api/fundamentals/compute",
                json={"symbol": "9999"},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["data"] == []

    def test_compute_missing_symbol(self, client: TestClient):
        """symbol未指定でバリデーションエラー"""
        response = client.post(
            "/api/fundamentals/compute",
            json={},
        )

        assert response.status_code == 422

    def test_compute_invalid_period_type(self, client: TestClient):
        """無効なperiod_type"""
        response = client.post(
            "/api/fundamentals/compute",
            json={
                "symbol": "7203",
                "period_type": "INVALID",
            },
        )

        assert response.status_code == 422

    def test_compute_stock_not_found(self, client: TestClient):
        """銘柄が見つからない場合(404)"""
        from src.infrastructure.external_api.exceptions import APINotFoundError

        with patch(
            "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
            side_effect=APINotFoundError("Stock not found"),
        ):
            response = client.post(
                "/api/fundamentals/compute",
                json={"symbol": "9999"},
            )

            assert response.status_code == 404

    def test_compute_api_error(self, client: TestClient):
        """APIエラー(500)"""
        from src.infrastructure.external_api.exceptions import APIError

        with patch(
            "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
            side_effect=APIError("API error"),
        ):
            response = client.post(
                "/api/fundamentals/compute",
                json={"symbol": "7203"},
            )

            assert response.status_code == 500

    def test_compute_unexpected_error(self, client: TestClient):
        """予期しないエラー(500)"""
        with patch(
            "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
            side_effect=RuntimeError("Unexpected error"),
        ):
            response = client.post(
                "/api/fundamentals/compute",
                json={"symbol": "7203"},
            )

            assert response.status_code == 500
            data = response.json()
            assert "Unexpected error" in data["message"]


class TestFundamentalsSchemaValidation:
    """スキーマバリデーションのテスト"""

    def test_request_valid_period_types(self, client: TestClient):
        """有効なperiod_type値"""
        valid_types = ["all", "FY", "1Q", "2Q", "3Q"]

        for period_type in valid_types:
            mock_response = FundamentalsComputeResponse(
                symbol="7203",
                companyName=None,
                data=[],
                latestMetrics=None,
                dailyValuation=None,
                tradingValuePeriod=15,
                lastUpdated="2024-05-15T10:00:00",
            )

            with patch(
                "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
                return_value=mock_response,
            ):
                response = client.post(
                    "/api/fundamentals/compute",
                    json={
                        "symbol": "7203",
                        "period_type": period_type,
                    },
                )
                assert (
                    response.status_code == 200
                ), f"Failed for period_type={period_type}"
