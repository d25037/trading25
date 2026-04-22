"""strategies/utils/optimization.py のテスト

Note: 循環import回避のため importlib で直接モジュールをロードする
(src.domains.optimization.__init__ -> engine -> src.domains.strategy.utils.optimization の循環)
"""

import random
import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest

# 循環import回避: src.domains.optimization をダミーで挿入してからインポート
if "src.domains.optimization" not in sys.modules:
    import types

    _dummy = types.ModuleType("src.domains.optimization")
    _dummy_scoring = types.ModuleType("src.domains.optimization.scoring")
    setattr(_dummy, "scoring", _dummy_scoring)

    def _is_valid_metric(value):
        import numpy as np
        try:
            return bool(np.isfinite(float(value)))
        except (TypeError, ValueError):
            return False

    def _normalize_and_recalculate_scores(results, weights):
        return results

    setattr(_dummy_scoring, "is_valid_metric", _is_valid_metric)
    setattr(_dummy_scoring, "normalize_and_recalculate_scores", _normalize_and_recalculate_scores)
    sys.modules["src.domains.optimization"] = _dummy
    sys.modules["src.domains.optimization.scoring"] = _dummy_scoring

from src.domains.strategy.utils.optimization import (  # noqa: E402
    OptimizationResult,
    ParameterOptimizer,
    ParameterRange,
)
from src.domains.strategy.utils import optimization as optimization_module  # noqa: E402


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


class TestSearchFlows:
    def test_grid_search_limits_combinations_and_normalizes(self, monkeypatch):
        opt = _make_optimizer()
        combos = [{"a": 1}, {"a": 2}, {"a": 3}]
        sentinel = OptimizationResult(
            best_params={"a": 1},
            best_score=1.0,
            best_portfolio="p",
            all_results=[{"params": {"a": 1}, "score": 1.0, "portfolio": "p"}],
            scoring_weights=opt.scoring_weights,
        )

        monkeypatch.setattr(opt, "_generate_param_combinations", lambda _ranges: combos)
        monkeypatch.setattr(opt, "_run_optimization", lambda _combos: sentinel.all_results)
        monkeypatch.setattr(
            optimization_module,
            "normalize_and_recalculate_scores",
            lambda results, _weights: results,
        )
        monkeypatch.setattr(opt, "_create_optimization_result", lambda results: sentinel)
        monkeypatch.setattr("random.sample", lambda items, n: items[:n])

        result = opt.grid_search([ParameterRange("a", [1, 2, 3])], max_combinations=2)

        assert result is sentinel

    def test_random_search_uses_generated_trials(self, monkeypatch):
        opt = _make_optimizer()
        sentinel = OptimizationResult(
            best_params={"a": 1},
            best_score=1.0,
            best_portfolio="p",
            all_results=[{"params": {"a": 1}, "score": 1.0, "portfolio": "p"}],
            scoring_weights=opt.scoring_weights,
        )

        monkeypatch.setattr(
            opt,
            "_generate_random_combinations",
            lambda _ranges, trials: [{"a": 1}] * trials,
        )
        monkeypatch.setattr(opt, "_run_optimization", lambda _combos: sentinel.all_results)
        monkeypatch.setattr(
            optimization_module,
            "normalize_and_recalculate_scores",
            lambda results, _weights: results,
        )
        monkeypatch.setattr(opt, "_create_optimization_result", lambda results: sentinel)

        result = opt.random_search([ParameterRange("a", [1, 2, 3])], n_trials=4)

        assert result is sentinel

    def test_run_optimization_single_process_skips_none_results(self, monkeypatch):
        opt = _make_optimizer()
        opt.n_jobs = 1
        results_iter = iter([{"params": {"a": 1}}, None])

        monkeypatch.setattr(opt, "_evaluate_single_params", lambda _params: next(results_iter))

        results = opt._run_optimization([{"a": 1}, {"a": 2}])

        assert results == [{"params": {"a": 1}}]

    def test_run_optimization_parallel_processes_futures(self, monkeypatch):
        opt = _make_optimizer()
        opt.n_jobs = 2

        class _FakeFuture:
            def __init__(self, result):
                self._result = result

            def result(self):
                return self._result

        class _FakeExecutor:
            def __init__(self, max_workers):
                self.max_workers = max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, params):
                return _FakeFuture(fn(params))

        monkeypatch.setattr(optimization_module, "ProcessPoolExecutor", _FakeExecutor)
        monkeypatch.setattr(optimization_module, "as_completed", lambda futures: list(futures))
        monkeypatch.setattr(opt, "_evaluate_single_params", lambda params: {"params": params})

        results = opt._run_optimization([{"a": 1}, {"a": 2}])

        assert results == [{"params": {"a": 1}}, {"params": {"a": 2}}]

    def test_evaluate_single_params_success(self):
        strategy_instance = MagicMock()
        portfolio_equal = _make_mock_portfolio(sharpe=1.0)
        portfolio_final = _make_mock_portfolio(sharpe=2.0, total_return=0.3)
        strategy_instance.run_backtest.side_effect = [portfolio_equal, portfolio_final]
        strategy_instance.calculate_kelly_allocations.return_value = {"7203": 0.5}

        strategy_class = MagicMock(return_value=strategy_instance)
        shared_config = MagicMock(kelly_fraction=0.4)
        opt = ParameterOptimizer(
            strategy_class=strategy_class,
            strategy_kwargs={"shared_config": shared_config},
            scoring_weights={"sharpe_ratio": 1.0},
        )

        result = opt._evaluate_single_params({"foo": 1})

        assert result is not None
        assert result["score"] == pytest.approx(2.0)
        assert result["metric_values"]["sharpe_ratio"] == pytest.approx(2.0)
        strategy_class.assert_called_once_with(shared_config=shared_config, foo=1, printlog=False)

    def test_evaluate_single_params_returns_none_on_exception(self):
        strategy_class = MagicMock(side_effect=RuntimeError("boom"))
        opt = ParameterOptimizer(
            strategy_class=strategy_class,
            strategy_kwargs={},
            scoring_weights={"sharpe_ratio": 1.0},
        )

        assert opt._evaluate_single_params({"foo": 1}) is None


class TestSearchEntryPoints:
    def test_grid_search_limits_combinations_and_returns_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        opt = _make_optimizer()
        ranges = [
            ParameterRange("a", [1, 2]),
            ParameterRange("b", [10, 20]),
        ]

        monkeypatch.setattr(
            opt,
            "_run_optimization",
            lambda combos: [{"params": combo, "score": 1.0, "portfolio": "p"} for combo in combos],
        )
        monkeypatch.setattr(
            optimization_module,
            "normalize_and_recalculate_scores",
            lambda results, weights: results,
        )
        monkeypatch.setattr(
            opt,
            "_create_optimization_result",
            lambda results: ("done", len(results)),
        )

        result = opt.grid_search(ranges, max_combinations=2)

        assert result == ("done", 2)

    def test_random_search_runs_requested_trials(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        opt = _make_optimizer()
        ranges = [ParameterRange("a", [1, 2, 3])]

        monkeypatch.setattr(
            opt,
            "_run_optimization",
            lambda combos: [{"params": combo, "score": 1.0, "portfolio": "p"} for combo in combos],
        )
        monkeypatch.setattr(
            optimization_module,
            "normalize_and_recalculate_scores",
            lambda results, weights: results,
        )
        monkeypatch.setattr(
            opt,
            "_create_optimization_result",
            lambda results: ("random", len(results)),
        )

        result = opt.random_search(ranges, n_trials=3)

        assert result == ("random", 3)


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


class TestRunOptimizationBranches:
    def test_run_optimization_single_process_filters_none_results(self):
        opt = _make_optimizer()
        opt.n_jobs = 1
        calls = iter(
            [
                {"params": {"a": 1}, "score": 1.0, "portfolio": "p"},
                None,
                {"params": {"a": 3}, "score": 0.5, "portfolio": "p"},
            ]
        )
        opt._evaluate_single_params = lambda params: next(calls)

        results = opt._run_optimization([{"a": 1}, {"a": 2}, {"a": 3}])

        assert len(results) == 2

    def test_run_optimization_parallel_collects_completed_futures(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        opt = _make_optimizer()
        opt.n_jobs = 2

        class _FakeFuture:
            def __init__(self, result):
                self._result = result

            def result(self):
                return self._result

        class _FakeExecutor:
            def __init__(self, *args, **kwargs):
                _ = (args, kwargs)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                _ = (exc_type, exc, tb)
                return None

            def submit(self, fn, params):
                return _FakeFuture(fn(params))

        monkeypatch.setattr(optimization_module, "ProcessPoolExecutor", _FakeExecutor)
        monkeypatch.setattr(optimization_module, "as_completed", lambda futures: list(futures))
        monkeypatch.setattr(
            opt,
            "_evaluate_single_params",
            lambda params: {"params": params, "score": 1.0, "portfolio": "p"},
        )

        results = opt._run_optimization([{"a": 1}, {"a": 2}])

        assert len(results) == 2


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

    def test_extract_win_rate_uses_records_readable_fallback(self):
        opt = _make_optimizer()
        p = MagicMock()
        p.trades.win_rate.side_effect = Exception("fail")
        p.trades.records_readable = pd.DataFrame({"PnL": [1.0, -0.5, 0.2, 0.3]})

        assert opt._extract_win_rate(p, MagicMock()) == pytest.approx(0.75)


class TestEvaluateSingleParams:
    def test_evaluate_single_params_success(self):
        class _FakeStrategy:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.calls = 0

            def run_backtest(self, custom_allocations=None):
                self.calls += 1
                portfolio = _make_mock_portfolio(
                    sharpe=1.2 if custom_allocations is None else 2.0,
                    total_return=0.1 if custom_allocations is None else 0.3,
                    calmar=1.1 if custom_allocations is None else 2.5,
                    max_dd=0.2,
                    win_rate=0.6,
                )
                portfolio.custom_allocations = custom_allocations
                return portfolio

            def calculate_kelly_allocations(self, portfolio, kelly_fraction=0.5):
                assert portfolio is not None
                assert kelly_fraction == 0.7
                return {"1111": 0.7}

        opt = ParameterOptimizer(
            strategy_class=_FakeStrategy,
            strategy_kwargs={"shared_config": types.SimpleNamespace(kelly_fraction=0.7)},
            scoring_weights={"sharpe_ratio": 0.6, "total_return": 0.4},
        )

        result = opt._evaluate_single_params({"window": 20})

        assert result is not None
        assert result["params"] == {"window": 20}
        assert result["metric_values"]["sharpe_ratio"] == 2.0
        assert result["metric_values"]["total_return"] == 0.3

    def test_evaluate_single_params_failure_returns_none(self):
        class _BrokenStrategy:
            def __init__(self, **kwargs):
                _ = kwargs

            def run_backtest(self, custom_allocations=None):
                _ = custom_allocations
                raise RuntimeError("boom")

        opt = ParameterOptimizer(
            strategy_class=_BrokenStrategy,
            strategy_kwargs={},
            scoring_weights={"sharpe_ratio": 1.0},
        )

        assert opt._evaluate_single_params({"window": 20}) is None


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


class TestPlotOptimizationSurface:
    def test_plot_optimization_surface_save_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ):
        opt = _make_optimizer()
        result = OptimizationResult(
            best_params={"a": 1},
            best_score=1.0,
            best_portfolio="p",
            all_results=[
                {"params": {"x": 1, "y": 10}, "score": 0.1},
                {"params": {"x": 2, "y": 20}, "score": 0.2},
            ],
            scoring_weights={"sharpe_ratio": 1.0},
        )
        save_path = tmp_path / "surface.png"
        saved_paths: list[object] = []

        class _FakeAxes:
            def scatter(self, *args, **kwargs):
                _ = (args, kwargs)
                return object()

            def set_xlabel(self, value):
                _ = value

            def set_ylabel(self, value):
                _ = value

            def set_zlabel(self, value):
                _ = value

            def set_title(self, value):
                _ = value

        class _FakeFigure:
            def add_subplot(self, *args, **kwargs):
                _ = (args, kwargs)
                return _FakeAxes()

        fake_plt = types.SimpleNamespace(
            figure=lambda **kwargs: _FakeFigure(),
            colorbar=lambda value: value,
            savefig=lambda path: saved_paths.append(path),
            show=lambda: None,
        )
        fake_matplotlib = types.ModuleType("matplotlib")
        fake_matplotlib.pyplot = fake_plt
        fake_mpl_toolkits = types.ModuleType("mpl_toolkits")
        fake_mplot3d = types.ModuleType("mpl_toolkits.mplot3d")
        fake_mplot3d.Axes3D = object
        fake_mpl_toolkits.mplot3d = fake_mplot3d

        monkeypatch.setitem(sys.modules, "matplotlib", fake_matplotlib)
        monkeypatch.setitem(sys.modules, "matplotlib.pyplot", fake_plt)
        monkeypatch.setitem(sys.modules, "mpl_toolkits", fake_mpl_toolkits)
        monkeypatch.setitem(sys.modules, "mpl_toolkits.mplot3d", fake_mplot3d)

        opt.plot_optimization_surface(result, "x", "y", save_path=str(save_path))

        assert saved_paths == [str(save_path)]

    def test_plot_optimization_surface_import_error_prints_hint(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ):
        opt = _make_optimizer()
        result = OptimizationResult(
            best_params={"a": 1},
            best_score=1.0,
            best_portfolio="p",
            all_results=[{"params": {"x": 1, "y": 10}, "score": 0.1}],
            scoring_weights={"sharpe_ratio": 1.0},
        )

        original_import = __import__

        def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name in {"matplotlib.pyplot", "mpl_toolkits.mplot3d"}:
                raise ImportError("missing")
            return original_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr("builtins.__import__", _fake_import)

        opt.plot_optimization_surface(result, "x", "y")

        captured = capsys.readouterr()
        assert "matplotlib が必要です" in captured.out
