"""cli_portfolio/list.py のテスト"""

from unittest.mock import patch

import pandas as pd


class TestRunList:
    @patch("src.cli_portfolio.list.load_portfolio_list")
    def test_empty_portfolio(self, mock_load):
        from src.cli_portfolio.list import run_list

        mock_load.return_value = pd.DataFrame()
        run_list()
        mock_load.assert_called_once()

    @patch("src.cli_portfolio.list.load_portfolio_list")
    def test_with_portfolios(self, mock_load):
        from src.cli_portfolio.list import run_list

        mock_load.return_value = pd.DataFrame({
            "id": [1, 2],
            "name": ["portfolio1", "portfolio2"],
            "stockCount": [5, 10],
            "totalShares": [500, 1000],
            "createdAt": ["2025-01-01T00:00:00", "2025-01-02T00:00:00"],
        })
        run_list()
        mock_load.assert_called_once()
