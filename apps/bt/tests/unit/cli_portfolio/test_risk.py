"""cli_portfolio/risk.py のテスト"""

from unittest.mock import patch

import pandas as pd
import pytest


class TestRunRiskAnalysis:
    @patch("src.cli_portfolio.risk.analyze_portfolio_risk")
    @patch("src.cli_portfolio.risk.create_portfolio_returns_matrix")
    @patch("src.cli_portfolio.risk.load_portfolio_code_name_mapping")
    @patch("src.cli_portfolio.risk.load_portfolio_stock_data")
    def test_basic_run(self, mock_stock, mock_mapping, mock_returns, mock_analyze):
        from src.cli_portfolio.risk import run_risk_analysis

        mock_stock.return_value = {"7203": pd.DataFrame({"Close": [100, 101]})}
        mock_mapping.return_value = {"7203": "トヨタ"}
        mock_returns.return_value = pd.DataFrame({"7203": [0.01, -0.005]})
        mock_analyze.return_value = {
            "num_stocks": 1,
            "num_days": 2,
            "portfolio_volatility": 0.15,
            "sharpe_ratio": 1.2,
            "var": -0.02,
            "cvar": -0.03,
            "diversification_metrics": {
                "avg_correlation": 1.0,
                "max_correlation": 1.0,
                "min_correlation": 1.0,
                "diversification_ratio": 1.0,
            },
            "risk_contribution": pd.Series({"7203": 1.0}),
            "correlation_matrix": pd.DataFrame(
                {"7203": [1.0]}, index=["7203"]
            ),
        }

        run_risk_analysis("test", lookback_days=30, verbose=False)
        mock_stock.assert_called_once_with("test", lookback_days=30)

    @patch("src.cli_portfolio.risk.load_portfolio_stock_data")
    def test_empty_stock_data(self, mock_stock):
        from src.cli_portfolio.risk import run_risk_analysis

        mock_stock.return_value = {}
        run_risk_analysis("test", verbose=False)

    @patch("src.cli_portfolio.risk.load_portfolio_stock_data")
    def test_exception_raised(self, mock_stock):
        from src.cli_portfolio.risk import run_risk_analysis

        mock_stock.side_effect = Exception("connection error")
        with pytest.raises(Exception, match="connection error"):
            run_risk_analysis("test", verbose=False)


class TestDisplayRiskResults:
    def test_display(self):
        from src.cli_portfolio.risk import _display_risk_results

        results = {
            "num_stocks": 2,
            "num_days": 100,
            "portfolio_volatility": 0.2,
            "sharpe_ratio": 1.5,
            "var": -0.025,
            "cvar": -0.04,
            "diversification_metrics": {
                "avg_correlation": 0.5,
                "max_correlation": 0.8,
                "min_correlation": 0.2,
                "diversification_ratio": 1.3,
            },
            "risk_contribution": pd.Series({"7203": 0.6, "6758": 0.4}),
            "correlation_matrix": pd.DataFrame(
                {"7203": [1.0, 0.5], "6758": [0.5, 1.0]},
                index=["7203", "6758"],
            ),
        }
        code_name_mapping = {"7203": "トヨタ", "6758": "ソニー"}
        _display_risk_results(results, 0.95, code_name_mapping)
