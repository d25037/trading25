"""score_normalizer.py のテスト"""

import pytest

from src.domains.lab_agent.evaluator.score_normalizer import normalize_scores, normalize_value
from src.domains.lab_agent.models import EvaluationResult, StrategyCandidate


def _make_candidate(sid="test"):
    return StrategyCandidate(
        strategy_id=sid,
        entry_filter_params={"volume": {"enabled": True}},
        exit_trigger_params={},
    )


def _make_result(sharpe=1.0, calmar=0.5, total_return=0.1, score=0.0):
    return EvaluationResult(
        candidate=_make_candidate(),
        score=score,
        sharpe_ratio=sharpe,
        calmar_ratio=calmar,
        total_return=total_return,
        max_drawdown=-0.1,
        win_rate=0.5,
        trade_count=100,
        success=True,
    )


class TestNormalizeValue:
    def test_middle(self):
        assert normalize_value(5.0, 0.0, 10.0) == pytest.approx(0.5)

    def test_min(self):
        assert normalize_value(0.0, 0.0, 10.0) == pytest.approx(0.0)

    def test_max(self):
        assert normalize_value(10.0, 0.0, 10.0) == pytest.approx(1.0)

    def test_same_min_max(self):
        assert normalize_value(5.0, 5.0, 5.0) == pytest.approx(0.5)

    def test_near_zero_range(self):
        assert normalize_value(1.0, 1.0, 1.0 + 1e-12) == pytest.approx(0.5)


class TestNormalizeScores:
    def test_empty_list(self):
        result = normalize_scores([], {"sharpe_ratio": 1.0})
        assert result == []

    def test_single_result(self):
        results = [_make_result(sharpe=1.5)]
        normalized = normalize_scores(results, {"sharpe_ratio": 1.0})
        assert len(normalized) == 1
        assert normalized[0].score == pytest.approx(0.5)

    def test_two_results_ordering(self):
        results = [
            _make_result(sharpe=1.0, calmar=0.5, total_return=0.1),
            _make_result(sharpe=2.0, calmar=1.0, total_return=0.2),
        ]
        weights = {"sharpe_ratio": 0.5, "calmar_ratio": 0.3, "total_return": 0.2}
        normalized = normalize_scores(results, weights)
        assert normalized[1].score > normalized[0].score

    def test_preserves_original_metrics(self):
        results = [_make_result(sharpe=1.5, calmar=0.8)]
        normalized = normalize_scores(results, {"sharpe_ratio": 1.0})
        assert normalized[0].sharpe_ratio == 1.5
        assert normalized[0].calmar_ratio == 0.8

    def test_preserves_candidate(self):
        results = [_make_result()]
        normalized = normalize_scores(results, {"sharpe_ratio": 1.0})
        assert normalized[0].candidate.strategy_id == "test"

    def test_all_same_values_get_half(self):
        results = [
            _make_result(sharpe=1.0, calmar=1.0, total_return=0.1),
            _make_result(sharpe=1.0, calmar=1.0, total_return=0.1),
        ]
        weights = {"sharpe_ratio": 1.0}
        normalized = normalize_scores(results, weights)
        assert normalized[0].score == pytest.approx(0.5)
        assert normalized[1].score == pytest.approx(0.5)
