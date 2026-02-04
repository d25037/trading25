"""cli_portfolio/summary.py のテスト"""

from unittest.mock import patch, MagicMock

import pytest


class TestRunSummary:
    @patch("src.cli_portfolio.summary.load_portfolio_summary")
    def test_with_empty_items(self, mock_load):
        from src.cli_portfolio.summary import run_summary

        mock_summary = MagicMock()
        mock_summary.portfolio.name = "test"
        mock_summary.portfolio.description = "desc"
        mock_summary.portfolio.created_at = "2025-01-01"
        mock_summary.portfolio.updated_at = "2025-01-02"
        mock_summary.total_stocks = 0
        mock_summary.total_cost = 0
        mock_summary.items = []
        mock_load.return_value = mock_summary

        run_summary("test")
        mock_load.assert_called_once_with("test")

    @patch("src.cli_portfolio.summary.load_portfolio_summary")
    def test_with_items(self, mock_load):
        from src.cli_portfolio.summary import run_summary

        mock_item = MagicMock()
        mock_item.code = "7203"
        mock_item.company_name = "トヨタ"
        mock_item.quantity = 100
        mock_item.purchase_price = 2500
        mock_item.total_cost = 250000
        mock_item.purchase_date = "2025-01-01"

        mock_summary = MagicMock()
        mock_summary.portfolio.name = "test"
        mock_summary.portfolio.description = None
        mock_summary.portfolio.created_at = "2025-01-01"
        mock_summary.portfolio.updated_at = "2025-01-02"
        mock_summary.total_stocks = 1
        mock_summary.total_cost = 250000
        mock_summary.items = [mock_item]
        mock_load.return_value = mock_summary

        run_summary("test")
        mock_load.assert_called_once_with("test")

    @patch("src.cli_portfolio.summary.load_portfolio_summary")
    def test_value_error_raised(self, mock_load):
        from src.cli_portfolio.summary import run_summary

        mock_load.side_effect = ValueError("not found")
        with pytest.raises(ValueError, match="not found"):
            run_summary("unknown")
