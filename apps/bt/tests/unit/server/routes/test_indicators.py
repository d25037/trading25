"""
Indicator Routes ユニットテスト
"""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.exceptions import APIError, APINotFoundError
from src.server.app import app

client = TestClient(app)


class TestComputeEndpoint:
    """POST /api/indicators/compute テスト"""

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
    def test_compute_success(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "timeframe": "daily",
            "meta": {"bars": 500},
            "indicators": {
                "sma_20": [{"date": "2024-01-01", "value": 100.5}],
            },
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.TIMEOUT_SECONDS", 0.001)
    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
    def test_risk_adjusted_return_indicator(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "timeframe": "daily",
            "meta": {"bars": 500},
            "indicators": {
                "risk_adjusted_return_60_sortino": [{"date": "2024-01-01", "value": 1.23}],
            },
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_indicators")
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

    @patch("src.server.routes.indicators.IndicatorService.compute_margin_indicators")
    def test_margin_success(self, mock_compute: MagicMock):
        mock_compute.return_value = {
            "stock_code": "7203",
            "indicators": {
                "margin_long_pressure": [
                    {"date": "2024-01-01", "pressure": 1.5, "longVol": 100000, "shortVol": 20000, "avgVolume": 50000.0}
                ],
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

    @patch("src.server.routes.indicators.IndicatorService.compute_margin_indicators")
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
