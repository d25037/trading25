"""Unit tests for PortfolioAPIClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from src.infrastructure.external_api.portfolio_client import PortfolioAPIClient


class TestPortfolioAPIClient:
    """Tests for PortfolioAPIClient."""

    def test_init(self) -> None:
        """Test initialization."""
        client = PortfolioAPIClient()
        assert client.base_url == "http://localhost:3002"
        assert client.timeout == 30.0

    @patch("httpx.Client")
    def test_get_portfolio_list(self, mock_client_class: MagicMock) -> None:
        """Test portfolio list retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"id": 1, "name": "my_portfolio", "description": "Test portfolio"},
            {"id": 2, "name": "other_portfolio", "description": None},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        df = client.get_portfolio_list()

        assert len(df) == 2
        assert "name" in df.columns
        assert "id" in df.columns

    @patch("httpx.Client")
    def test_get_portfolio_list_empty(self, mock_client_class: MagicMock) -> None:
        """Test empty portfolio list."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        df = client.get_portfolio_list()

        assert df.empty

    @patch("httpx.Client")
    def test_get_portfolio(self, mock_client_class: MagicMock) -> None:
        """Test portfolio retrieval by ID."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "id": 1,
            "name": "my_portfolio",
            "description": "Test",
            "items": [
                {"id": 1, "code": "7203", "company_name": "トヨタ", "quantity": 100},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        result = client.get_portfolio(1)

        assert result["name"] == "my_portfolio"
        assert len(result["items"]) == 1

    @patch("httpx.Client")
    def test_get_portfolio_by_name(self, mock_client_class: MagicMock) -> None:
        """Test portfolio retrieval by name."""
        # First call returns list of portfolios
        # Second call returns the specific portfolio
        list_response = MagicMock()
        list_response.is_success = True
        list_response.json.return_value = [
            {"id": 1, "name": "my_portfolio"},
            {"id": 2, "name": "other_portfolio"},
        ]

        detail_response = MagicMock()
        detail_response.is_success = True
        detail_response.json.return_value = {
            "id": 1,
            "name": "my_portfolio",
            "items": [],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = [list_response, detail_response]
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        result = client.get_portfolio_by_name("my_portfolio")

        assert result["name"] == "my_portfolio"

    @patch("httpx.Client")
    def test_get_portfolio_by_name_not_found(self, mock_client_class: MagicMock) -> None:
        """Test portfolio retrieval when name not found."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"id": 1, "name": "other_portfolio"},
        ]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        result = client.get_portfolio_by_name("nonexistent")

        assert result == {}

    @patch("httpx.Client")
    def test_get_portfolio_items(self, mock_client_class: MagicMock) -> None:
        """Test portfolio items retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "id": 1,
            "name": "my_portfolio",
            "items": [
                {"id": 1, "code": "7203", "quantity": 100, "purchase_price": 2500},
                {"id": 2, "code": "9984", "quantity": 50, "purchase_price": 6000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        df = client.get_portfolio_items(1)

        assert len(df) == 2
        assert "code" in df.columns

    @patch("httpx.Client")
    def test_get_portfolio_codes(self, mock_client_class: MagicMock) -> None:
        """Test portfolio codes retrieval."""
        # First call returns list
        list_response = MagicMock()
        list_response.is_success = True
        list_response.json.return_value = [{"id": 1, "name": "my_portfolio"}]

        # Second call returns portfolio with items
        detail_response = MagicMock()
        detail_response.is_success = True
        detail_response.json.return_value = {
            "id": 1,
            "name": "my_portfolio",
            "items": [
                {"code": "7203"},
                {"code": "9984"},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = [list_response, detail_response]
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        codes = client.get_portfolio_codes("my_portfolio")

        assert codes == ["7203", "9984"]

    @patch("httpx.Client")
    def test_get_portfolio_summary(self, mock_client_class: MagicMock) -> None:
        """Test portfolio summary retrieval."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "id": 1,
            "name": "my_portfolio",
            "description": "Test",
            "items": [
                {"code": "7203", "quantity": 100, "purchase_price": 2500},
                {"code": "9984", "quantity": 50, "purchase_price": 6000},
            ],
        }

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = PortfolioAPIClient()
        summary = client.get_portfolio_summary(1)

        assert summary["name"] == "my_portfolio"
        assert summary["item_count"] == 2
        assert summary["total_invested"] == 100 * 2500 + 50 * 6000
        assert set(summary["stock_codes"]) == {"7203", "9984"}

    @patch("httpx.Client")
    def test_context_manager(self, mock_client_class: MagicMock) -> None:
        """Test context manager usage."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        with PortfolioAPIClient() as client:
            _ = client.get_portfolio_list()

        mock_httpx_client.close.assert_called_once()
