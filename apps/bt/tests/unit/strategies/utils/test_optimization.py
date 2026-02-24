"""strategies/utils/optimization.py のテスト

Note: 循環import回避のため importlib で直接モジュールをロードする
(src.domains.optimization.__init__ -> engine -> src.domains.strategy.utils.optimization の循環)
"""

import random
import sys
from unittest.mock import MagicMock

import pytest

# 循環import回避: src.domains.optimization をダミーで挿入してからインポート
if "src.domains.optimization" not in sys.modules:
    import types

    _dummy = types.ModuleType("src.domains.optimization")
    _dummy.scoring = types.ModuleType("src.domains.optimization.scoring")  # type: ignore[attr-defined]

    def _is_valid_metric(value):
        import numpy as np
        try:
            return bool(np.isfinite(float(value)))
        except (TypeError, ValueError):
            return False

    def _normalize_and_recalculate_scores(results, weights):
        return results

    _dummy.scoring.is_valid_metric = _is_valid_metric  # type: ignore[attr-defined]
    _dummy.scoring.normalize_and_recalculate_scores = _normalize_and_recalculate_scores  # type: ignore[attr-defined]
    sys.modules["src.domains.optimization"] = _dummy
    sys.modules["src.domains.optimization.scoring"] = _dummy.scoring  # type: ignore[attr-defined]

from src.domains.strategy.utils.optimization import (  # noqa: E402
    OptimizationResult,
    ParameterOptimizer,
    ParameterRange,
)


# ===== Helpers =====


def _make_mock_portfolio(
    sharpe=1.5,
    total_return=0.2,
    calmar=2.0,
    max_dd=0.15,
    win_rate=0.6,
):
    """テスト用モックポートフォリオ"""
    portfolio = MagicMock()
    portfolio.sharpe_ratio.return_value = sharpe
    portfolio.total_return.return_value = total_return
    portfolio.calmar_ratio.return_value = calmar
    portfolio.max_drawdown.return_value = max_dd
    trades_mock = MagicMock()
    trades_mock.win_rate.return_value = win_rate
    portfolio.trades = trades_mock
    return portfolio


def _make_optimizer(scoring_weights=None):
    """テスト用オプティマイザ（strategyクラスはMock）"""
    weights = scoring_weights or {"sharpe_ratio": 0.6, "total_return": 0.4}
    mock_strategy_cls = MagicMock()
    return ParameterOptimizer(
        strategy_class=mock_strategy_cls,
        strategy_kwargs={},
        scoring_weights=weights,
    )


# ===== _validate_scoring_weights =====


class TestValidateScoringWeights:
    def test_empty_weights_raises(self):
        with pytest.raises(ValueError, match="scoring_weightsが設定されていません"):
            ParameterOptimizer(
                strategy_class=MagicMock(),
                strategy_kwargs={},
                scoring_weights={},
            )

    def test_unsupported_metric_raises(self):
        with pytest.raises(ValueError, match="Unsupported metric"):
            ParameterOptimizer(
                strategy_class=MagicMock(),
                strategy_kwargs={},
                scoring_weights={"unknown_metric": 1.0},
            )

    def test_valid_weights_accepted(self):
        opt = _make_optimizer({"sharpe_ratio": 0.5, "total_return": 0.5})
        assert opt.scoring_weights == {"sharpe_ratio": 0.5, "total_return": 0.5}

    def test_non_unity_weights_warns(self, capsys):
        _make_optimizer({"sharpe_ratio": 0.3, "total_return": 0.3})
        captured = capsys.readouterr()
        assert "警告" in captured.out


# ===== _generate_param_combinations =====


class TestGenerateParamCombinations:
    def test_two_params(self):
        opt = _make_optimizer()
        ranges = [
            ParameterRange("a", [1, 2]),
            ParameterRange("b", [10, 20, 30]),
        ]
        combos = opt._generate_param_combinations(ranges)
        assert len(combos) == 6
        assert {"a": 1, "b": 10} in combos
        assert {"a": 2, "b": 30} in combos

    def test_single_param(self):
        opt = _make_optimizer()
        ranges = [ParameterRange("x", [5])]
        combos = opt._generate_param_combinations(ranges)
        assert len(combos) == 1
        assert combos[0] == {"x": 5}

    def test_three_params_cartesian(self):
        opt = _make_optimizer()
        ranges = [
            ParameterRange("a", [1, 2]),
            ParameterRange("b", [3, 4]),
            ParameterRange("c", [5, 6]),
        ]
        combos = opt._generate_param_combinations(ranges)
        assert len(combos) == 8


# ===== _generate_random_combinations =====


class TestGenerateRandomCombinations:
    def test_output_length(self):
        opt = _make_optimizer()
        random.seed(42)
        ranges = [
            ParameterRange("a", [1, 2, 3]),
            ParameterRange("b", [10, 20]),
        ]
        combos = opt._generate_random_combinations(ranges, 5)
        assert len(combos) == 5

    def test_values_in_range(self):
        opt = _make_optimizer()
        random.seed(42)
        ranges = [ParameterRange("x", [1, 2, 3])]
        combos = opt._generate_random_combinations(ranges, 20)
        for c in combos:
            assert c["x"] in [1, 2, 3]

    def test_structure(self):
        opt = _make_optimizer()
        random.seed(42)
        ranges = [
            ParameterRange("a", [1]),
            ParameterRange("b", [2]),
        ]
        combos = opt._generate_random_combinations(ranges, 3)
        for c in combos:
            assert "a" in c
            assert "b" in c


# ===== _calculate_composite_score =====


class TestCalculateCompositeScore:
    def test_basic_calculation(self):
        opt = _make_optimizer({"sharpe_ratio": 0.6, "total_return": 0.4})
        portfolio = _make_mock_portfolio(sharpe=2.0, total_return=0.5)
        strategy = MagicMock()
        score = opt._calculate_composite_score(portfolio, strategy)
        expected = 0.6 * 2.0 + 0.4 * 0.5
        assert abs(score - expected) < 1e-6

    def test_empty_weights_raises(self):
        opt = _make_optimizer({"sharpe_ratio": 1.0})
        opt.scoring_weights = {}
        with pytest.raises(ValueError, match="scoring_weights is not set"):
            opt._calculate_composite_score(MagicMock(), MagicMock())


# ===== Metric extractors =====


class TestMetricExtractors:
    def test_extract_sharpe_ratio_normal(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio(sharpe=1.5)
        assert opt._extract_sharpe_ratio(p, MagicMock()) == 1.5

    def test_extract_sharpe_ratio_nan(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio()
        p.sharpe_ratio.return_value = float("nan")
        assert opt._extract_sharpe_ratio(p, MagicMock()) == 0.0

    def test_extract_sharpe_ratio_inf(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio()
        p.sharpe_ratio.return_value = float("inf")
        assert opt._extract_sharpe_ratio(p, MagicMock()) == 0.0

    def test_extract_sharpe_ratio_exception(self):
        opt = _make_optimizer()
        p = MagicMock()
        p.sharpe_ratio.side_effect = Exception("fail")
        assert opt._extract_sharpe_ratio(p, MagicMock()) == 0.0

    def test_extract_total_return(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio(total_return=0.25)
        assert opt._extract_total_return(p, MagicMock()) == 0.25

    def test_extract_total_return_exception(self):
        opt = _make_optimizer()
        p = MagicMock()
        p.total_return.side_effect = Exception("fail")
        assert opt._extract_total_return(p, MagicMock()) == 0.0

    def test_extract_calmar_ratio_normal(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio(calmar=3.0)
        assert opt._extract_calmar_ratio(p, MagicMock()) == 3.0

    def test_extract_calmar_ratio_nan(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio()
        p.calmar_ratio.return_value = float("nan")
        assert opt._extract_calmar_ratio(p, MagicMock()) == 0.0

    def test_extract_max_drawdown_normal(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio(max_dd=0.15)
        result = opt._extract_max_drawdown(p, MagicMock())
        assert result == -0.15

    def test_extract_max_drawdown_nan(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio()
        p.max_drawdown.return_value = float("nan")
        assert opt._extract_max_drawdown(p, MagicMock()) == -100.0

    def test_extract_win_rate_normal(self):
        opt = _make_optimizer()
        p = _make_mock_portfolio(win_rate=0.65)
        assert opt._extract_win_rate(p, MagicMock()) == 0.65

    def test_extract_win_rate_exception_fallback(self):
        opt = _make_optimizer()
        p = MagicMock()
        p.trades.win_rate.side_effect = Exception("fail")
        p.trades.records_readable = MagicMock(spec=[])
        assert opt._extract_win_rate(p, MagicMock()) == 0.0


# ===== _create_optimization_result =====


class TestCreateOptimizationResult:
    def test_empty_raises(self):
        opt = _make_optimizer()
        with pytest.raises(ValueError, match="最適化結果が空です"):
            opt._create_optimization_result([])

    def test_returns_best(self):
        opt = _make_optimizer()
        results = [
            {"params": {"a": 1}, "score": 0.5, "portfolio": "p1"},
            {"params": {"a": 2}, "score": 0.9, "portfolio": "p2"},
            {"params": {"a": 3}, "score": 0.3, "portfolio": "p3"},
        ]
        result = opt._create_optimization_result(results)
        assert isinstance(result, OptimizationResult)
        assert result.best_score == 0.9
        assert result.best_params == {"a": 2}

    def test_result_includes_all(self):
        opt = _make_optimizer()
        results = [{"params": {"a": 1}, "score": 0.5, "portfolio": "p1"}]
        result = opt._create_optimization_result(results)
        assert len(result.all_results) == 1


# ===== get_optimization_summary =====


class TestGetOptimizationSummary:
    def test_dataframe_columns_and_sort(self):
        opt = _make_optimizer()
        result = OptimizationResult(
            best_params={"a": 2},
            best_score=0.9,
            best_portfolio="p",
            all_results=[
                {"params": {"a": 1}, "score": 0.5},
                {"params": {"a": 2}, "score": 0.9},
                {"params": {"a": 3}, "score": 0.3},
            ],
            scoring_weights={"sharpe_ratio": 0.6, "total_return": 0.4},
        )
        df = opt.get_optimization_summary(result)
        assert "score" in df.columns
        assert "a" in df.columns
        assert df.iloc[0]["score"] == 0.9
        assert df.iloc[-1]["score"] == 0.3
