"""
optimization/scoring.py のテスト
"""

from unittest.mock import MagicMock

import numpy as np
import pytest

from src.optimization.scoring import (
    calculate_composite_score,
    is_valid_metric,
    normalize_and_recalculate_scores,
)


# ===== is_valid_metric =====


class TestIsValidMetric:
    def test_normal_float(self):
        assert is_valid_metric(1.5) is True

    def test_zero(self):
        assert is_valid_metric(0.0) is True

    def test_negative(self):
        assert is_valid_metric(-3.14) is True

    def test_nan(self):
        assert is_valid_metric(float("nan")) is False

    def test_inf(self):
        assert is_valid_metric(float("inf")) is False

    def test_neg_inf(self):
        assert is_valid_metric(float("-inf")) is False

    def test_numpy_nan(self):
        assert is_valid_metric(np.nan) is False

    def test_numpy_inf(self):
        assert is_valid_metric(np.inf) is False

    def test_int(self):
        assert is_valid_metric(42) is True

    def test_string_raises(self):
        assert is_valid_metric("not_a_number") is False

    def test_none_raises(self):
        assert is_valid_metric(None) is False

    def test_large_float(self):
        assert is_valid_metric(1e308) is True


# ===== calculate_composite_score =====


class TestCalculateCompositeScore:
    def test_all_metrics(self):
        portfolio = MagicMock()
        portfolio.sharpe_ratio.return_value = 2.0
        portfolio.calmar_ratio.return_value = 3.0
        portfolio.total_return.return_value = 0.5
        weights = {"sharpe_ratio": 0.4, "calmar_ratio": 0.3, "total_return": 0.3}
        score = calculate_composite_score(portfolio, weights)
        expected = 0.4 * 2.0 + 0.3 * 3.0 + 0.3 * 0.5
        assert score == pytest.approx(expected)

    def test_sharpe_only(self):
        portfolio = MagicMock()
        portfolio.sharpe_ratio.return_value = 1.5
        score = calculate_composite_score(portfolio, {"sharpe_ratio": 1.0})
        assert score == pytest.approx(1.5)

    def test_calmar_only(self):
        portfolio = MagicMock()
        portfolio.calmar_ratio.return_value = 2.5
        score = calculate_composite_score(portfolio, {"calmar_ratio": 1.0})
        assert score == pytest.approx(2.5)

    def test_total_return_only(self):
        portfolio = MagicMock()
        portfolio.total_return.return_value = 0.3
        score = calculate_composite_score(portfolio, {"total_return": 1.0})
        assert score == pytest.approx(0.3)

    def test_nan_metric_ignored(self):
        portfolio = MagicMock()
        portfolio.sharpe_ratio.return_value = float("nan")
        portfolio.calmar_ratio.return_value = 2.0
        weights = {"sharpe_ratio": 0.5, "calmar_ratio": 0.5}
        score = calculate_composite_score(portfolio, weights)
        assert score == pytest.approx(0.5 * 2.0)

    def test_inf_metric_ignored(self):
        portfolio = MagicMock()
        portfolio.sharpe_ratio.return_value = float("inf")
        score = calculate_composite_score(portfolio, {"sharpe_ratio": 1.0})
        assert score == 0.0

    def test_exception_in_metric(self):
        portfolio = MagicMock()
        portfolio.sharpe_ratio.side_effect = Exception("fail")
        portfolio.calmar_ratio.return_value = 1.0
        weights = {"sharpe_ratio": 0.5, "calmar_ratio": 0.5}
        score = calculate_composite_score(portfolio, weights)
        assert score == pytest.approx(0.5 * 1.0)

    def test_empty_weights(self):
        portfolio = MagicMock()
        score = calculate_composite_score(portfolio, {})
        assert score == 0.0

    def test_unknown_weight_ignored(self):
        portfolio = MagicMock()
        score = calculate_composite_score(portfolio, {"unknown_metric": 1.0})
        assert score == 0.0

    def test_exception_in_calmar(self):
        portfolio = MagicMock()
        portfolio.calmar_ratio.side_effect = Exception("fail")
        portfolio.sharpe_ratio.return_value = 1.0
        weights = {"sharpe_ratio": 0.5, "calmar_ratio": 0.5}
        score = calculate_composite_score(portfolio, weights)
        assert score == pytest.approx(0.5)

    def test_exception_in_total_return(self):
        portfolio = MagicMock()
        portfolio.total_return.side_effect = Exception("fail")
        portfolio.sharpe_ratio.return_value = 1.0
        weights = {"sharpe_ratio": 0.5, "total_return": 0.5}
        score = calculate_composite_score(portfolio, weights)
        assert score == pytest.approx(0.5)


# ===== normalize_and_recalculate_scores =====


class TestNormalizeAndRecalculateScores:
    def test_empty_results(self):
        result = normalize_and_recalculate_scores([], {"sharpe_ratio": 0.5, "total_return": 0.5})
        assert result == []

    def test_single_result(self):
        results = [
            {
                "params": {"a": 1},
                "metric_values": {"sharpe_ratio": 1.5, "total_return": 0.1},
            }
        ]
        scoring_weights = {"sharpe_ratio": 0.6, "total_return": 0.4}
        result = normalize_and_recalculate_scores(results, scoring_weights)
        assert len(result) == 1
        assert result[0]["normalized_metrics"]["sharpe_ratio"] == 0.5
        assert result[0]["normalized_metrics"]["total_return"] == 0.5
        assert result[0]["score"] == pytest.approx(0.5)

    def test_multiple_results_normalization(self):
        results = [
            {"params": {"a": 1}, "metric_values": {"sharpe_ratio": 0.0, "total_return": 0.0}},
            {"params": {"a": 2}, "metric_values": {"sharpe_ratio": 1.0, "total_return": 0.5}},
            {"params": {"a": 3}, "metric_values": {"sharpe_ratio": 2.0, "total_return": 1.0}},
        ]
        scoring_weights = {"sharpe_ratio": 0.5, "total_return": 0.5}
        result = normalize_and_recalculate_scores(results, scoring_weights)
        assert len(result) == 3
        assert result[0]["score"] == pytest.approx(0.0)
        assert result[1]["score"] == pytest.approx(0.5)
        assert result[2]["score"] == pytest.approx(1.0)

    def test_weighted_score_calculation(self):
        results = [
            {"params": {"a": 1}, "metric_values": {"sharpe_ratio": 0.0, "total_return": 1.0}},
            {"params": {"a": 2}, "metric_values": {"sharpe_ratio": 1.0, "total_return": 0.0}},
        ]
        scoring_weights = {"sharpe_ratio": 0.8, "total_return": 0.2}
        result = normalize_and_recalculate_scores(results, scoring_weights)
        assert result[0]["score"] == pytest.approx(0.2)
        assert result[1]["score"] == pytest.approx(0.8)

    def test_same_values_all_normalized_to_half(self):
        results = [
            {"params": {"a": 1}, "metric_values": {"sharpe_ratio": 1.5, "total_return": 0.3}},
            {"params": {"a": 2}, "metric_values": {"sharpe_ratio": 1.5, "total_return": 0.3}},
        ]
        scoring_weights = {"sharpe_ratio": 0.5, "total_return": 0.5}
        result = normalize_and_recalculate_scores(results, scoring_weights)
        for r in result:
            assert r["normalized_metrics"]["sharpe_ratio"] == 0.5
            assert r["score"] == pytest.approx(0.5)

    def test_negative_values(self):
        results = [
            {"params": {"a": 1}, "metric_values": {"sharpe_ratio": -2.0}},
            {"params": {"a": 2}, "metric_values": {"sharpe_ratio": 2.0}},
        ]
        result = normalize_and_recalculate_scores(results, {"sharpe_ratio": 1.0})
        assert result[0]["normalized_metrics"]["sharpe_ratio"] == pytest.approx(0.0)
        assert result[1]["normalized_metrics"]["sharpe_ratio"] == pytest.approx(1.0)
