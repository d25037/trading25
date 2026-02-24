"""data/loaders/portfolio_loaders.py のテスト"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from src.infrastructure.data_access.loaders.portfolio_loaders import (
    _convert_portfolio_code_to_market_code,
    create_portfolio_price_matrix,
    create_portfolio_returns_matrix,
    load_portfolio_list,
    load_portfolio_summary,
)


class TestConvertPortfolioCodeToMarketCode:
    def test_four_digit_numeric(self):
        assert _convert_portfolio_code_to_market_code("2207") == "22070"

    def test_four_digit_alphanumeric(self):
        assert _convert_portfolio_code_to_market_code("285A") == "285A0"

    def test_five_digit_unchanged(self):
        assert _convert_portfolio_code_to_market_code("22070") == "22070"

    def test_three_digit(self):
        assert _convert_portfolio_code_to_market_code("123") == "123"

    def test_six_digit(self):
        assert _convert_portfolio_code_to_market_code("123456") == "123456"


class TestLoadPortfolioList:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.get_portfolio_list.return_value = pd.DataFrame(
            {"name": ["portfolio1", "portfolio2"]}
        )
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.infrastructure.data_access.loaders.portfolio_loaders.PortfolioAPIClient", return_value=mock_client):
            result = load_portfolio_list()
        assert len(result) == 2
        assert "name" in result.columns


class TestLoadPortfolioSummary:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.get_portfolio_by_name.return_value = {
            "id": 1,
            "name": "test",
            "description": "desc",
            "createdAt": "2025-01-01T00:00:00",
            "updatedAt": "2025-01-01T00:00:00",
            "items": [
                {
                    "id": 1,
                    "portfolioId": 1,
                    "code": "1234",
                    "companyName": "Test Corp",
                    "quantity": 100,
                    "purchasePrice": 1000.0,
                    "purchaseDate": "2025-01-01",
                    "account": "NISA",
                    "notes": None,
                    "createdAt": "2025-01-01T00:00:00",
                    "updatedAt": "2025-01-01T00:00:00",
                }
            ],
        }
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.infrastructure.data_access.loaders.portfolio_loaders.PortfolioAPIClient", return_value=mock_client):
            result = load_portfolio_summary("test")
        assert result.total_stocks == 1
        assert result.items[0].code == "1234"

    def test_not_found(self):
        mock_client = MagicMock()
        mock_client.get_portfolio_by_name.return_value = None
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch("src.infrastructure.data_access.loaders.portfolio_loaders.PortfolioAPIClient", return_value=mock_client):
            import pytest
            with pytest.raises(ValueError, match="not found"):
                load_portfolio_summary("nonexistent")


class TestCreatePortfolioReturnsMatrix:
    def test_basic(self):
        dates = pd.date_range("2025-01-01", periods=5)
        stock_data = {
            "1234": pd.DataFrame({"Close": [100.0, 102.0, 101.0, 103.0, 105.0]}, index=dates),
            "5678": pd.DataFrame({"Close": [200.0, 198.0, 202.0, 204.0, 203.0]}, index=dates),
        }
        result = create_portfolio_returns_matrix(stock_data)
        assert "1234" in result.columns
        assert "5678" in result.columns
        assert len(result) > 0

    def test_no_close_column_skipped(self):
        dates = pd.date_range("2025-01-01", periods=3)
        stock_data = {
            "1234": pd.DataFrame({"Close": [100.0, 101.0, 102.0]}, index=dates),
            "5678": pd.DataFrame({"Open": [200.0, 201.0, 202.0]}, index=dates),
        }
        result = create_portfolio_returns_matrix(stock_data)
        assert "1234" in result.columns
        assert "5678" not in result.columns

    def test_inf_replaced(self):
        dates = pd.date_range("2025-01-01", periods=3)
        stock_data = {
            "1234": pd.DataFrame({"Close": [0.0, 100.0, 101.0]}, index=dates),
        }
        result = create_portfolio_returns_matrix(stock_data)
        assert not np.any(np.isinf(result.values[~np.isnan(result.values)]))


class TestCreatePortfolioPriceMatrix:
    def test_basic(self):
        dates = pd.date_range("2025-01-01", periods=3)
        stock_data = {
            "1234": pd.DataFrame({"Close": [100.0, 101.0, 102.0]}, index=dates),
            "5678": pd.DataFrame({"Close": [200.0, 201.0, 202.0]}, index=dates),
        }
        result = create_portfolio_price_matrix(stock_data)
        assert result.shape == (3, 2)
        assert "1234" in result.columns

    def test_no_close_column_skipped(self):
        dates = pd.date_range("2025-01-01", periods=3)
        stock_data = {
            "1234": pd.DataFrame({"Close": [100.0, 101.0, 102.0]}, index=dates),
            "5678": pd.DataFrame({"Open": [200.0, 201.0, 202.0]}, index=dates),
        }
        result = create_portfolio_price_matrix(stock_data)
        assert "5678" not in result.columns
