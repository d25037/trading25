"""Unit tests for MarketAPIClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from src.api.market_client import MarketAPIClient


class TestMarketAPIClient:
    """Tests for MarketAPIClient."""

    def test_init(self) -> None:
        """Test initialization."""
        client = MarketAPIClient()
        assert client.base_url == "http://localhost:3002"
        assert client.timeout == 30.0

    @patch("httpx.Client")
    def test_get_stock_ohlcv(self, mock_client_class: MagicMock) -> None:
        """Test stock OHLCV retrieval from market.db."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = MarketAPIClient()
        df = client.get_stock_ohlcv("7203")

        assert len(df) == 1
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]

    @patch("httpx.Client")
    def test_get_stock_ohlcv_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty stock OHLCV response."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = MarketAPIClient()
        df = client.get_stock_ohlcv("9999")

        assert df.empty

    @patch("httpx.Client")
    def test_get_topix(self, mock_client_class: MagicMock) -> None:
        """Test TOPIX data retrieval from market.db."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"date": "2024-01-01", "open": 2500, "high": 2550, "low": 2480, "close": 2530},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = MarketAPIClient()
        df = client.get_topix()

        assert len(df) == 1
        assert list(df.columns) == ["Open", "High", "Low", "Close"]
        assert "Volume" not in df.columns

    @patch("httpx.Client")
    def test_get_stock_data_for_screening(self, mock_client_class: MagicMock) -> None:
        """Test stock data retrieval for screening."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {
                "code": "7203",
                "company_name": "トヨタ自動車",
                "data": [
                    {"date": "2024-01-01", "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
                ],
            },
            {
                "code": "9984",
                "company_name": "ソフトバンクグループ",
                "data": [
                    {"date": "2024-01-01", "open": 200, "high": 220, "low": 195, "close": 210, "volume": 2000},
                ],
            },
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = MarketAPIClient()
        result = client.get_stock_data_for_screening(market="prime", history_days=300)

        assert len(result) == 2
        assert "7203" in result
        assert "9984" in result

        df, company_name = result["7203"]
        assert company_name == "トヨタ自動車"
        assert len(df) == 1
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]

    @patch("httpx.Client")
    def test_get_stock_data_for_screening_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty screening data response."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = MarketAPIClient()
        result = client.get_stock_data_for_screening()

        assert result == {}

    @patch("httpx.Client")
    def test_context_manager(self, mock_client_class: MagicMock) -> None:
        """Test context manager usage."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        with MarketAPIClient() as client:
            _ = client.get_topix()

        mock_httpx_client.close.assert_called_once()
