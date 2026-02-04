"""portfolio_risk.py のテスト"""

import numpy as np
import pandas as pd
import pytest

from src.analysis.portfolio_risk import (
    analyze_portfolio_risk,
    calculate_correlation_matrix,
    calculate_cvar,
    calculate_diversification_metrics,
    calculate_portfolio_volatility,
    calculate_risk_contribution,
    calculate_sharpe_ratio,
    calculate_var,
)


def _returns_df(n=252, stocks=3, seed=42):
    np.random.seed(seed)
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    data = np.random.randn(n, stocks) * 0.02
    return pd.DataFrame(data, index=idx, columns=[f"S{i}" for i in range(stocks)])


class TestCorrelationMatrix:
    def test_shape(self, returns_df):
        corr = calculate_correlation_matrix(returns_df)
        assert corr.shape == (3, 3)

    def test_diagonal_is_one(self, returns_df):
        corr = calculate_correlation_matrix(returns_df)
        for i in range(3):
            assert corr.iloc[i, i] == pytest.approx(1.0)

    def test_symmetric(self, returns_df):
        corr = calculate_correlation_matrix(returns_df)
        assert (corr - corr.T).abs().max().max() < 1e-10


class TestPortfolioVolatility:
    def test_positive(self, returns_df):
        vol = calculate_portfolio_volatility(returns_df)
        assert vol > 0

    def test_custom_weights(self, returns_df):
        weights = pd.Series([1.0, 0.0, 0.0], index=returns_df.columns)
        vol = calculate_portfolio_volatility(returns_df, weights)
        assert vol > 0

    def test_single_stock(self):
        df = _returns_df(stocks=1)
        vol = calculate_portfolio_volatility(df)
        expected = df.iloc[:, 0].std() * np.sqrt(252)
        assert vol == pytest.approx(expected, rel=0.01)


class TestRiskContribution:
    def test_sums_to_one(self, returns_df):
        rc = calculate_risk_contribution(returns_df)
        assert rc.sum() == pytest.approx(1.0, abs=1e-6)

    def test_all_positive_equal_weights(self, returns_df):
        rc = calculate_risk_contribution(returns_df)
        assert (rc > -0.1).all()


class TestVaR:
    def test_negative_value(self, returns_df):
        var = calculate_var(returns_df)
        assert var < 0

    def test_higher_confidence_more_negative(self, returns_df):
        var_95 = calculate_var(returns_df, confidence_level=0.95)
        var_99 = calculate_var(returns_df, confidence_level=0.99)
        assert var_99 <= var_95


class TestCVaR:
    def test_less_than_var(self, returns_df):
        var = calculate_var(returns_df)
        cvar = calculate_cvar(returns_df)
        assert cvar <= var


class TestSharpeRatio:
    def test_returns_float(self, returns_df):
        sr = calculate_sharpe_ratio(returns_df)
        assert isinstance(sr, float)

    def test_zero_volatility(self):
        idx = pd.date_range("2025-01-01", periods=10, freq="D")
        df = pd.DataFrame({"S0": [0.0] * 10}, index=idx)
        sr = calculate_sharpe_ratio(df)
        assert sr == 0.0


class TestDiversificationMetrics:
    def test_keys(self, returns_df):
        metrics = calculate_diversification_metrics(returns_df)
        expected_keys = {"avg_correlation", "max_correlation", "min_correlation", "diversification_ratio"}
        assert expected_keys == set(metrics.keys())

    def test_correlation_bounds(self, returns_df):
        metrics = calculate_diversification_metrics(returns_df)
        assert -1.0 <= metrics["min_correlation"] <= 1.0
        assert -1.0 <= metrics["max_correlation"] <= 1.0


class TestAnalyzePortfolioRisk:
    def test_comprehensive(self, returns_df):
        results = analyze_portfolio_risk(returns_df)
        expected_keys = {
            "correlation_matrix", "portfolio_volatility", "risk_contribution",
            "var", "cvar", "sharpe_ratio", "diversification_metrics",
            "num_stocks", "num_days",
        }
        assert expected_keys <= set(results.keys())
        assert results["num_stocks"] == 3
        assert results["num_days"] == 252
