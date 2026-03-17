"""
Indicator Routes ユニットテスト
"""

from typing import Any, cast
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.infrastructure.external_api.exceptions import APIError, APINotFoundError
from src.entrypoints.http.app import app

client = TestClient(app)
client_app = cast(Any, client.app)


def _market_provenance() -> dict[str, Any]:
    return {
        "source_kind": "market",
        "market_snapshot_id": None,
        "dataset_snapshot_id": None,
        "reference_date": "2024-01-01",
        "loaded_domains": ["stock_data"],
        "strategy_name": None,
        "strategy_fingerprint": None,
        "warnings": [],
    }


class TestComputeEndpoint:
    """POST /api/indicators/compute テスト"""

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_success(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "timeframe": "daily",
            "meta": {"bars": 500},
            "indicators": {
                "sma_20": [{"date": "2024-01-01", "value": 100.5}],
            },
            "provenance": _market_provenance(),
        }

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["stock_code"] == "7203"
        assert "sma_20" in data["indicators"]
        mock_compute.assert_called_once()

    def test_compute_rejects_non_market_source(self):
        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "source": "primeExTopix500",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 422

    def test_compute_rejects_blank_source(self):
        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "source": "   ",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 422

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_not_found(self, mock_compute: MagicMock):
        mock_compute.side_effect = ValueError("銘柄 9999 のOHLCVデータが取得できません")

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "9999",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 404
        assert response.json()["details"] == [
            {"field": "reason", "message": "stock_not_found"},
        ]

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_local_stock_data_missing_details(self, mock_compute: MagicMock):
        class StaticReader:
            def query_one(self, sql, params=()):  # noqa: ANN001, ANN201
                del sql, params
                return {"code": "7203"}

        previous_reader = getattr(client_app.state, "market_reader", None)
        client_app.state.market_reader = StaticReader()
        mock_compute.side_effect = ValueError("銘柄 7203 のOHLCVデータが取得できません")

        try:
            response = client.post(
                "/api/indicators/compute",
                json={
                    "stock_code": "7203",
                    "indicators": [{"type": "sma", "params": {"period": 20}}],
                },
            )
        finally:
            client_app.state.market_reader = previous_reader

        assert response.status_code == 404
        assert response.json()["details"] == [
            {"field": "reason", "message": "local_stock_data_missing"},
            {"field": "recovery", "message": "stock_refresh"},
        ]

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_topix_missing_details(self, mock_compute: MagicMock):
        mock_compute.side_effect = ValueError("ベンチマーク 'topix' のデータが取得できません")

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "benchmark_code": "topix",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 404
        assert response.json()["details"] == [
            {"field": "reason", "message": "topix_data_missing"},
            {"field": "recovery", "message": "market_db_sync"},
        ]

    def test_compute_invalid_source_with_topix_returns_422(self):
        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "9999",
                "source": "primeExTopix500",
                "benchmark_code": "topix",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 422

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_local_stock_data_missing_details_when_only_stock_data_exists(
        self,
        mock_compute: MagicMock,
    ):
        class StaticReader:
            def query_one(self, sql, params=()):  # noqa: ANN001, ANN201
                del params
                if "FROM stock_data" in sql:
                    return {"code": "7203"}
                return None

        previous_reader = getattr(client_app.state, "market_reader", None)
        client_app.state.market_reader = StaticReader()
        mock_compute.side_effect = ValueError("銘柄 7203 のOHLCVデータが取得できません")

        try:
            response = client.post(
                "/api/indicators/compute",
                json={
                    "stock_code": "7203",
                    "indicators": [{"type": "sma", "params": {"period": 20}}],
                },
            )
        finally:
            client_app.state.market_reader = previous_reader

        assert response.status_code == 404
        assert response.json()["details"] == [
            {"field": "reason", "message": "local_stock_data_missing"},
            {"field": "recovery", "message": "stock_refresh"},
        ]

    def test_invalid_indicator_type(self):
        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "invalid_type", "params": {}}],
            },
        )

        assert response.status_code == 422

    def test_empty_indicators(self):
        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [],
            },
        )

        assert response.status_code == 422

    def test_invalid_sma_period(self):
        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 0}}],
            },
        )

        assert response.status_code == 422

    @patch("src.entrypoints.http.routes.indicators.TIMEOUT_SECONDS", 0.001)
    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_timeout(self, mock_compute: MagicMock):
        """タイムアウト時に504を返す"""
        import time

        def slow_compute(*args, **kwargs):
            time.sleep(1)
            return {}

        mock_compute.side_effect = slow_compute

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 504
        assert "タイムアウト" in response.json()["message"]

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_unexpected_error(self, mock_compute: MagicMock):
        """予期しないエラー時に500を返す"""
        mock_compute.side_effect = RuntimeError("unexpected error")

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 500
        assert "計算エラー" in response.json()["message"]

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_value_error_422(self, mock_compute: MagicMock):
        """ValueError（取得できません以外）で422を返す"""
        mock_compute.side_effect = ValueError("パラメータが不正です")

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 422

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_multiple_indicators(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "timeframe": "daily",
            "meta": {"bars": 500},
            "indicators": {
                "sma_20": [{"date": "2024-01-01", "value": 100.5}],
                "rsi_14": [{"date": "2024-01-01", "value": 55.0}],
                "macd_12_26_9": [{"date": "2024-01-01", "macd": 0.5, "signal": 0.3, "histogram": 0.2}],
            },
            "provenance": _market_provenance(),
        }

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [
                    {"type": "sma", "params": {"period": 20}},
                    {"type": "rsi", "params": {}},
                    {"type": "macd", "params": {}},
                ],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["indicators"]) == 3

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_risk_adjusted_return_indicator(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "timeframe": "daily",
            "meta": {"bars": 500},
            "indicators": {
                "risk_adjusted_return_60_sortino": [{"date": "2024-01-01", "value": 1.23}],
            },
            "provenance": _market_provenance(),
        }

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [
                    {"type": "risk_adjusted_return", "params": {"lookback_period": 60, "ratio_type": "sortino"}},
                ],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "risk_adjusted_return_60_sortino" in data["indicators"]

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_api_not_found_returns_404(self, mock_compute: MagicMock):
        """APINotFoundError が 404 で返ること"""
        mock_compute.side_effect = APINotFoundError("Resource not found: Stock not found")

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "9999",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 404

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_api_error_returns_status_code(self, mock_compute: MagicMock):
        """APIError が適切なステータスコードで返ること"""
        mock_compute.side_effect = APIError("Bad request", status_code=400)

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 400

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_api_error_no_status_returns_500(self, mock_compute: MagicMock):
        """status_code=None の APIError が 500 で返ること"""
        mock_compute.side_effect = APIError("Connection failed")

        response = client.post(
            "/api/indicators/compute",
            json={
                "stock_code": "7203",
                "indicators": [{"type": "sma", "params": {"period": 20}}],
            },
        )

        assert response.status_code == 500


class TestMarginEndpoint:
    """POST /api/indicators/margin テスト"""

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_margin_indicators")
    def test_margin_success(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "indicators": {
                "margin_long_pressure": [
                    {"date": "2024-01-01", "pressure": 1.5, "longVol": 100000, "shortVol": 20000, "avgVolume": 50000.0}
                ],
            },
            "provenance": {
                **_market_provenance(),
                "loaded_domains": ["margin_data", "stock_data"],
            },
        }

        response = client.post(
            "/api/indicators/margin",
            json={
                "stock_code": "7203",
                "indicators": ["margin_long_pressure"],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["stock_code"] == "7203"
        assert "margin_long_pressure" in data["indicators"]

    @patch("src.entrypoints.http.routes.indicators.IndicatorService.compute_margin_indicators")
    def test_margin_not_found(self, mock_compute: MagicMock):
        mock_compute.side_effect = ValueError("銘柄 9999 の信用データが取得できません")

        response = client.post(
            "/api/indicators/margin",
            json={
                "stock_code": "9999",
                "indicators": ["margin_long_pressure"],
            },
        )

        assert response.status_code == 404

    def test_invalid_margin_indicator(self):
        response = client.post(
            "/api/indicators/margin",
            json={
                "stock_code": "7203",
                "indicators": ["unknown_indicator"],
            },
        )

        assert response.status_code == 422
