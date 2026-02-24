from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import src.domains.optimization.engine as engine_mod
from src.shared.models.signals import SignalParams
from src.domains.optimization.engine import (
    ParameterOptimizationEngine,
    _init_worker_data,
    _run_with_timeout,
    _timeout_guard,
)


def _make_engine() -> ParameterOptimizationEngine:
    engine = object.__new__(ParameterOptimizationEngine)
    engine.verbose = False
    engine.strategy_basename = "demo_strategy"
    engine.parameter_ranges = {}
    engine.optimization_config = {
        "n_jobs": 1,
        "scoring_weights": {
            "sharpe_ratio": 0.5,
            "calmar_ratio": 0.3,
            "total_return": 0.2,
        },
    }
    engine.base_entry_params = SignalParams()
    engine.base_exit_params = SignalParams()
    engine.shared_config_dict = {
        "dataset": "primeExTopix500",
        "stock_codes": ["1301"],
        "start_date": "",
        "end_date": "",
        "include_margin_data": True,
        "include_statements_data": True,
        "timeframe": "daily",
        "relative_mode": False,
        "benchmark_table": "",
        "kelly_fraction": 0.5,
        "min_allocation": 0.1,
        "max_allocation": 0.5,
    }
    engine._prefetched_data = {}
    engine._prefetched_benchmark = None
    return engine


class _DummySharedConfig:
    def __init__(self, **kwargs):
        self.dataset = kwargs.get("dataset", "primeExTopix500")
        self.stock_codes = kwargs.get("stock_codes", ["1301"])
        self.start_date = kwargs.get("start_date", "")
        self.end_date = kwargs.get("end_date", "")
        self.include_margin_data = kwargs.get("include_margin_data", True)
        self.include_statements_data = kwargs.get("include_statements_data", True)
        self.timeframe = kwargs.get("timeframe", "daily")
        self.relative_mode = kwargs.get("relative_mode", False)
        self.benchmark_table = kwargs.get("benchmark_table", "")
        self.kelly_fraction = kwargs.get("kelly_fraction", 0.5)
        self.min_allocation = kwargs.get("min_allocation", 0.1)
        self.max_allocation = kwargs.get("max_allocation", 0.5)


def _shared_config_ns() -> SimpleNamespace:
    return SimpleNamespace(
        kelly_fraction=0.5,
        min_allocation=0.1,
        max_allocation=0.5,
    )


def test_timeout_guard_short_circuits_non_positive_and_non_main_thread(monkeypatch):
    with _timeout_guard(0):
        pass

    fake_main = object()
    fake_worker = object()
    monkeypatch.setattr(engine_mod.threading, "main_thread", lambda: fake_main)
    monkeypatch.setattr(engine_mod.threading, "current_thread", lambda: fake_worker)

    with _timeout_guard(1):
        pass


def test_timeout_guard_main_thread_configures_and_restores_signal(monkeypatch):
    fake_main = object()
    signal_calls: list[tuple[int, object]] = []
    timer_calls: list[tuple[int, int]] = []

    def fake_signal(sig, handler):
        signal_calls.append((sig, handler))
        return "prev_handler"

    def fake_setitimer(which, seconds):
        timer_calls.append((which, seconds))

    monkeypatch.setattr(engine_mod.threading, "main_thread", lambda: fake_main)
    monkeypatch.setattr(engine_mod.threading, "current_thread", lambda: fake_main)
    monkeypatch.setattr(engine_mod.signal, "signal", fake_signal)
    monkeypatch.setattr(engine_mod.signal, "setitimer", fake_setitimer)

    with _timeout_guard(1):
        pass

    assert timer_calls == [
        (engine_mod.signal.ITIMER_REAL, 1),
        (engine_mod.signal.ITIMER_REAL, 0),
    ]
    assert signal_calls[0][0] == engine_mod.signal.SIGALRM
    assert signal_calls[1] == (engine_mod.signal.SIGALRM, "prev_handler")


def test_run_with_timeout_invokes_callable():
    assert _run_with_timeout(0, lambda: 42) == 42


def test_init_worker_data_sets_module_globals():
    _init_worker_data({"A": {}}, None)
    assert engine_mod._worker_shared_data == {"A": {}}
    assert engine_mod._worker_shared_benchmark is None


def test_init_with_explicit_base_config(monkeypatch, tmp_path):
    base_config = tmp_path / "base.yaml"
    base_config.write_text("entry_filter_params: {}\nexit_trigger_params: {}\n", encoding="utf-8")

    monkeypatch.setattr(
        ParameterOptimizationEngine,
        "_configure_logger",
        staticmethod(lambda verbose: None),
    )
    monkeypatch.setattr(engine_mod, "find_grid_config_path", lambda _name, _path: "grid.yaml")
    monkeypatch.setattr(
        engine_mod,
        "load_grid_config",
        lambda _path: {
            "description": "demo",
            "parameter_ranges": {"entry_filter_params": {}},
            "base_config": str(base_config),
        },
    )
    monkeypatch.setattr(
        engine_mod,
        "load_default_config",
        lambda: {"parameter_optimization": {"n_jobs": 1, "scoring_weights": {"sharpe_ratio": 1.0}}},
    )

    import src.domains.strategy.runtime.loader as loader_mod

    class DummyLoader:
        def merge_shared_config(self, _config):
            return {"dataset": "primeExTopix500", "stock_codes": ["1301"]}

        def load_strategy_config(self, _name):
            raise AssertionError("load_strategy_config should not be called")

        def _infer_strategy_path(self, _name):
            raise AssertionError("_infer_strategy_path should not be called")

    monkeypatch.setattr(loader_mod, "ConfigLoader", DummyLoader)

    engine = ParameterOptimizationEngine("experimental/demo_strategy", verbose=True)
    assert engine.strategy_basename == "demo_strategy"
    assert engine.base_config_path == str(base_config)
    assert engine.description == "demo"
    assert engine.total_combinations == 1


def test_init_with_inferred_base_config(monkeypatch, tmp_path):
    monkeypatch.setattr(
        ParameterOptimizationEngine,
        "_configure_logger",
        staticmethod(lambda verbose: None),
    )
    monkeypatch.setattr(engine_mod, "find_grid_config_path", lambda _name, _path: "grid.yaml")
    monkeypatch.setattr(
        engine_mod,
        "load_grid_config",
        lambda _path: {
            "description": "demo",
            "parameter_ranges": {"entry_filter_params": {}},
        },
    )
    monkeypatch.setattr(
        engine_mod,
        "load_default_config",
        lambda: {"parameter_optimization": {"n_jobs": 1, "scoring_weights": {"sharpe_ratio": 1.0}}},
    )

    import src.domains.strategy.runtime.loader as loader_mod

    inferred = tmp_path / "inferred.yaml"

    class DummyLoader:
        def load_strategy_config(self, _name):
            return {"entry_filter_params": {}, "exit_trigger_params": {}}

        def _infer_strategy_path(self, _name):
            return inferred

        def merge_shared_config(self, _config):
            return {"dataset": "primeExTopix500", "stock_codes": ["1301"]}

    monkeypatch.setattr(loader_mod, "ConfigLoader", DummyLoader)

    engine = ParameterOptimizationEngine("production/demo_strategy")
    assert engine.base_config_path == str(inferred)


def test_total_combinations_uses_grid_loader(monkeypatch):
    engine = _make_engine()
    monkeypatch.setattr(engine_mod, "generate_combinations", lambda _ranges: [{}, {}, {}])
    assert engine.total_combinations == 3


def test_optimize_raises_for_empty_combinations(monkeypatch):
    engine = _make_engine()
    engine.grid_config_path = "dummy-grid.yaml"
    monkeypatch.setattr(engine_mod, "generate_combinations", lambda _ranges: [])

    with pytest.raises(ValueError, match="パラメータ範囲が空です"):
        engine.optimize()


def test_optimize_raises_when_results_empty(monkeypatch):
    engine = _make_engine()
    engine.grid_config_path = "dummy-grid.yaml"

    monkeypatch.setattr(engine_mod, "generate_combinations", lambda _ranges: [{"id": 1}])
    monkeypatch.setattr(engine_mod, "build_signal_params", lambda combo, section, base: f"{section}:{combo['id']}")
    monkeypatch.setattr(engine_mod, "SharedConfig", _DummySharedConfig)
    monkeypatch.setattr(engine, "_prefetch_data", lambda: ({}, None))
    monkeypatch.setattr(engine, "_run_custom_optimization", lambda _kwargs, _combos: [])
    monkeypatch.setattr(engine_mod, "normalize_and_recalculate_scores", lambda results, weights: results)

    with pytest.raises(RuntimeError, match="最適化結果が空です"):
        engine.optimize()


def test_optimize_success_returns_best_result(monkeypatch):
    engine = _make_engine()
    engine.parameter_ranges = {"entry_filter_params": {"dummy": {"x": [1, 2]}}}

    combos = [{"id": 1}, {"id": 2}]
    raw_results = [
        {
            "params": {"id": 1},
            "score": 0.3,
            "metric_values": {"sharpe_ratio": 1.0, "calmar_ratio": 0.8, "total_return": 0.1},
        },
        {
            "params": {"id": 2},
            "score": 0.9,
            "metric_values": {"sharpe_ratio": 1.5, "calmar_ratio": 1.1, "total_return": 0.2},
        },
    ]

    class FakeStrategy:
        instances: list["FakeStrategy"] = []

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.multi_data_dict = None
            self.benchmark_data = None
            FakeStrategy.instances.append(self)

        def run_optimized_backtest_kelly(self, **kwargs):
            self.kelly_kwargs = kwargs
            return None, "best-portfolio", None, None, None

    monkeypatch.setattr(engine_mod, "generate_combinations", lambda _ranges: combos)
    monkeypatch.setattr(engine_mod, "build_signal_params", lambda combo, section, base: f"{section}:{combo['id']}")
    monkeypatch.setattr(engine_mod, "SharedConfig", _DummySharedConfig)
    monkeypatch.setattr(engine_mod, "YamlConfigurableStrategy", FakeStrategy)
    monkeypatch.setattr(engine, "_prefetch_data", lambda: ({"1301": {"close": []}}, "bench"))
    monkeypatch.setattr(engine, "_run_custom_optimization", lambda _kwargs, _combos: raw_results)
    monkeypatch.setattr(
        engine_mod,
        "normalize_and_recalculate_scores",
        lambda results, _weights: list(results),
    )
    monkeypatch.setattr(engine, "_generate_visualization_notebook", lambda _results, _combos: "/tmp/result.html")

    result = engine.optimize()

    assert result.best_params == {"id": 2}
    assert result.best_score == 0.9
    assert result.best_portfolio == "best-portfolio"
    assert result.notebook_path == "/tmp/result.html"

    best_strategy = FakeStrategy.instances[-1]
    assert best_strategy.multi_data_dict == {"1301": {"close": []}}
    assert best_strategy.benchmark_data == "bench"


def test_prefetch_data_loads_multi_data_and_benchmark(monkeypatch):
    engine = _make_engine()
    engine.shared_config_dict["benchmark_table"] = "topix"
    monkeypatch.setattr(engine_mod, "SharedConfig", _DummySharedConfig)
    monkeypatch.setattr(engine, "_should_include_forecast_revision", lambda: True)

    import src.infrastructure.data_access.loaders as data_mod
    import src.infrastructure.data_access.loaders as loaders_mod

    called: dict[str, object] = {}

    def fake_prepare_multi_data(**kwargs):
        called.update(kwargs)
        return {"1301": {"close": []}}

    monkeypatch.setattr(loaders_mod, "prepare_multi_data", fake_prepare_multi_data)
    monkeypatch.setattr(data_mod, "load_topix_data", lambda dataset, start, end: {"Date": []})

    multi_data, benchmark = engine._prefetch_data()
    assert multi_data == {"1301": {"close": []}}
    assert benchmark == {"Date": []}
    assert called["include_forecast_revision"] is True


def test_prefetch_data_continues_when_benchmark_load_fails(monkeypatch):
    engine = _make_engine()
    engine.shared_config_dict["benchmark_table"] = "topix"
    monkeypatch.setattr(engine_mod, "SharedConfig", _DummySharedConfig)
    monkeypatch.setattr(engine, "_should_include_forecast_revision", lambda: False)

    import src.infrastructure.data_access.loaders as data_mod
    import src.infrastructure.data_access.loaders as loaders_mod

    monkeypatch.setattr(loaders_mod, "prepare_multi_data", lambda **kwargs: {"1301": {"close": []}})

    def _raise(*args, **kwargs):
        raise RuntimeError("benchmark failed")

    monkeypatch.setattr(data_mod, "load_topix_data", _raise)

    multi_data, benchmark = engine._prefetch_data()
    assert multi_data == {"1301": {"close": []}}
    assert benchmark is None


def test_prefetch_data_skips_benchmark_when_not_requested(monkeypatch):
    engine = _make_engine()
    monkeypatch.setattr(engine_mod, "SharedConfig", _DummySharedConfig)
    monkeypatch.setattr(engine, "_should_include_forecast_revision", lambda: False)

    import src.infrastructure.data_access.loaders as loaders_mod

    monkeypatch.setattr(loaders_mod, "prepare_multi_data", lambda **kwargs: {"1301": {"close": []}})

    multi_data, benchmark = engine._prefetch_data()
    assert multi_data == {"1301": {"close": []}}
    assert benchmark is None


def test_grid_may_enable_forecast_signals_returns_false_for_non_dict_ranges():
    engine = _make_engine()
    engine.parameter_ranges = []  # type: ignore[assignment]

    assert engine._grid_may_enable_forecast_signals() is False


def test_grid_may_enable_forecast_signals_handles_invalid_section_shapes():
    engine = _make_engine()
    engine.parameter_ranges = {
        "entry_filter_params": [],
        "exit_trigger_params": {"fundamental": []},
    }

    assert engine._grid_may_enable_forecast_signals() is False


def test_grid_may_enable_forecast_signals_handles_scalar_and_none_enabled_values():
    engine = _make_engine()
    entry = SignalParams()
    entry.fundamental.enabled = True
    engine.base_entry_params = entry
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = {
        "entry_filter_params": {
            "fundamental": {
                "forward_eps_growth": {},
                "peg_ratio": {"enabled": False},
            }
        }
    }

    assert engine._grid_may_enable_forecast_signals() is False


def test_grid_may_enable_forecast_signals_uses_missing_and_scalar_enabled_flags():
    engine = _make_engine()
    engine.parameter_ranges = {
        "entry_filter_params": {"fundamental": {}},
        "exit_trigger_params": {
            "fundamental": {
                "enabled": True,
                "peg_ratio": {"enabled": [True]},
            }
        },
    }

    assert engine._grid_may_enable_forecast_signals() is True


def test_grid_may_enable_forecast_signals_returns_true_from_base_signal_enabled():
    engine = _make_engine()
    entry = SignalParams()
    entry.fundamental.enabled = True
    entry.fundamental.forward_eps_growth.enabled = True
    engine.base_entry_params = entry
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = {
        "entry_filter_params": {
            "fundamental": {
                "forward_eps_growth": {},
            }
        }
    }

    assert engine._grid_may_enable_forecast_signals() is True


def test_run_custom_optimization_dispatches_single_and_parallel(monkeypatch):
    engine = _make_engine()
    monkeypatch.setattr(engine, "_log_parallel_mode", lambda n_jobs, max_workers: None)
    monkeypatch.setattr(engine, "_run_optimization_single_process", lambda _kwargs, _combos: ["single"])
    monkeypatch.setattr(engine, "_run_optimization_parallel", lambda _kwargs, _combos, _workers: ["parallel"])

    engine.optimization_config["n_jobs"] = 1
    assert engine._run_custom_optimization([{}], [{}]) == ["single"]

    engine.optimization_config["n_jobs"] = -1
    assert engine._run_custom_optimization([{}], [{}]) == ["parallel"]


def test_log_parallel_mode_outputs_expected_messages(monkeypatch):
    engine = _make_engine()
    messages: list[str] = []

    class FakeLogger:
        def info(self, message):
            messages.append(message)

    monkeypatch.setattr(engine_mod, "logger", FakeLogger())
    monkeypatch.setattr(engine_mod.os, "cpu_count", lambda: 4)

    engine._log_parallel_mode(-1, None)
    engine._log_parallel_mode(1, 1)
    engine._log_parallel_mode(3, 3)

    assert "全CPUコア使用" in messages[0]
    assert "シングルプロセス実行" in messages[1]
    assert "3 ワーカー" in messages[2]


def test_run_optimization_single_process_filters_none_results(monkeypatch):
    engine = _make_engine()
    evaluated = iter(
        [
            None,
            {
                "params": {"id": 2},
                "score": 0.5,
                "metric_values": {"sharpe_ratio": 1.0, "calmar_ratio": 1.0, "total_return": 0.1},
            },
        ]
    )
    logs: list[tuple[int, int, dict]] = []

    monkeypatch.setattr(engine, "_evaluate_single_params", lambda *_args: next(evaluated))
    monkeypatch.setattr(engine, "_log_evaluation_result", lambda i, total, result, params: logs.append((i, total, params)))

    out = engine._run_optimization_single_process(
        [{"id": 1}, {"id": 2}],
        [{"id": 1}, {"id": 2}],
    )
    assert len(out) == 1
    assert logs == [(2, 2, {"id": 2})]


def test_run_optimization_parallel_handles_success_timeout_and_error(monkeypatch):
    engine = _make_engine()
    engine._prefetched_data = {"1301": {"close": []}}
    engine._prefetched_benchmark = None

    logs: list[tuple] = []
    monkeypatch.setattr(engine, "_log_evaluation_result", lambda *args: logs.append(args))

    class FakeFuture:
        def __init__(self, result=None, error=None):
            self._result = result
            self._error = error

        def result(self):
            if self._error:
                raise self._error
            return self._result

    class FakeExecutor:
        def __init__(self, max_workers=None, initializer=None, initargs=()):
            self._futures: list[FakeFuture] = []
            if initializer is not None:
                initializer(*initargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, _fn, _strategy_kwargs, combo, _verbose):
            idx = len(self._futures)
            if idx == 0:
                fut = FakeFuture(
                    {
                        "params": combo,
                        "score": 0.8,
                        "metric_values": {"sharpe_ratio": 1.0, "calmar_ratio": 1.0, "total_return": 0.1},
                    }
                )
            elif idx == 1:
                fut = FakeFuture(None)
            elif idx == 2:
                fut = FakeFuture(error=TimeoutError())
            else:
                fut = FakeFuture(error=RuntimeError("boom"))
            self._futures.append(fut)
            return fut

    import concurrent.futures as futures_mod

    monkeypatch.setattr(futures_mod, "ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr(futures_mod, "as_completed", lambda mapping: list(mapping))

    out = engine._run_optimization_parallel(
        [{}, {}, {}, {}],
        [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
        max_workers=2,
    )
    assert len(out) == 1
    assert out[0]["params"] == {"id": 1}
    assert len(logs) == 1


def test_evaluate_single_params_success(monkeypatch):
    engine = _make_engine()
    monkeypatch.setattr(
        ParameterOptimizationEngine,
        "_configure_logger",
        staticmethod(lambda verbose: None),
    )
    monkeypatch.setattr(engine_mod, "_run_with_timeout", lambda _seconds, func: func())
    monkeypatch.setattr(engine, "_run_kelly_backtest", lambda _kwargs: "portfolio")
    monkeypatch.setattr(
        engine,
        "_collect_metrics",
        lambda _portfolio: {"sharpe_ratio": 1.1, "calmar_ratio": 0.9, "total_return": 0.2},
    )
    monkeypatch.setattr(engine_mod, "calculate_composite_score", lambda _portfolio, _weights: 1.23)

    result = engine._evaluate_single_params({"shared_config": object()}, {"id": 1}, verbose=True)
    assert result is not None
    assert result["score"] == 1.23
    assert result["params"] == {"id": 1}


def test_evaluate_single_params_returns_none_on_timeout(monkeypatch):
    engine = _make_engine()
    monkeypatch.setattr(
        ParameterOptimizationEngine,
        "_configure_logger",
        staticmethod(lambda verbose: None),
    )

    def _raise_timeout(_seconds, _func):
        raise TimeoutError()

    monkeypatch.setattr(engine_mod, "_run_with_timeout", _raise_timeout)

    assert engine._evaluate_single_params({"shared_config": object()}, {"id": 1}) is None


def test_evaluate_single_params_returns_none_on_exception(monkeypatch):
    engine = _make_engine()
    monkeypatch.setattr(
        ParameterOptimizationEngine,
        "_configure_logger",
        staticmethod(lambda verbose: None),
    )

    def _raise_error(_seconds, _func):
        raise RuntimeError("failed")

    monkeypatch.setattr(engine_mod, "_run_with_timeout", _raise_error)

    assert engine._evaluate_single_params({"shared_config": object()}, {"id": 1}) is None


def test_run_kelly_backtest_prefers_worker_shared_data(monkeypatch):
    engine = _make_engine()

    class FakeStrategy:
        instances: list["FakeStrategy"] = []

        def __init__(self, **kwargs):
            self.multi_data_dict = None
            self.benchmark_data = None
            FakeStrategy.instances.append(self)

        def run_optimized_backtest_kelly(self, **kwargs):
            return None, "portfolio", None, None, None

    monkeypatch.setattr(engine_mod, "YamlConfigurableStrategy", FakeStrategy)
    engine_mod._worker_shared_data = {"worker": {"close": []}}
    engine_mod._worker_shared_benchmark = "worker-bench"

    portfolio = engine._run_kelly_backtest(
        {"shared_config": _shared_config_ns()}
    )
    assert portfolio == "portfolio"
    strategy = FakeStrategy.instances[-1]
    assert strategy.multi_data_dict == {"worker": {"close": []}}
    assert strategy.benchmark_data == "worker-bench"

    engine_mod._worker_shared_data = None
    engine_mod._worker_shared_benchmark = None


def test_run_kelly_backtest_uses_prefetched_data_when_worker_data_absent(monkeypatch):
    engine = _make_engine()
    engine._prefetched_data = {"prefetched": {"close": []}}
    engine._prefetched_benchmark = "prefetched-bench"
    engine_mod._worker_shared_data = None
    engine_mod._worker_shared_benchmark = None

    class FakeStrategy:
        instances: list["FakeStrategy"] = []

        def __init__(self, **kwargs):
            self.multi_data_dict = None
            self.benchmark_data = None
            FakeStrategy.instances.append(self)

        def run_optimized_backtest_kelly(self, **kwargs):
            return None, "portfolio", None, None, None

    monkeypatch.setattr(engine_mod, "YamlConfigurableStrategy", FakeStrategy)

    portfolio = engine._run_kelly_backtest(
        {"shared_config": _shared_config_ns()}
    )
    assert portfolio == "portfolio"
    strategy = FakeStrategy.instances[-1]
    assert strategy.multi_data_dict == {"prefetched": {"close": []}}
    assert strategy.benchmark_data == "prefetched-bench"


def test_run_kelly_backtest_works_without_prefetched_data(monkeypatch):
    engine = _make_engine()
    engine_mod._worker_shared_data = None
    engine_mod._worker_shared_benchmark = None
    engine._prefetched_data = None
    engine._prefetched_benchmark = None

    class FakeStrategy:
        instances: list["FakeStrategy"] = []

        def __init__(self, **kwargs):
            self.multi_data_dict = None
            self.benchmark_data = None
            FakeStrategy.instances.append(self)

        def run_optimized_backtest_kelly(self, **kwargs):
            return None, "portfolio", None, None, None

    monkeypatch.setattr(engine_mod, "YamlConfigurableStrategy", FakeStrategy)

    portfolio = engine._run_kelly_backtest(
        {"shared_config": _shared_config_ns()}
    )
    assert portfolio == "portfolio"
    strategy = FakeStrategy.instances[-1]
    assert strategy.multi_data_dict is None
    assert strategy.benchmark_data is None


def test_generate_visualization_notebook_handles_unknown_dataset(monkeypatch, tmp_path):
    engine = _make_engine()
    engine.shared_config_dict = {}

    import src.domains.optimization.notebook_generator as notebook_mod
    import src.shared.paths as paths_mod

    out_dir = tmp_path / "optimization"
    monkeypatch.setattr(paths_mod, "get_optimization_results_dir", lambda _strategy: out_dir)
    monkeypatch.setattr(
        notebook_mod,
        "generate_optimization_notebook",
        lambda **kwargs: kwargs["output_path"],
    )

    out = engine._generate_visualization_notebook([{"score": 1.0}], [{"id": 1}])
    assert Path(out).parent == out_dir
    assert Path(out).name.startswith("unknown_")
    assert out.endswith(".html")


def test_generate_visualization_notebook_uses_dataset_stem(monkeypatch, tmp_path):
    engine = _make_engine()
    engine.shared_config_dict = {"dataset": "datasets/primeExTopix500.db"}

    import src.domains.optimization.notebook_generator as notebook_mod
    import src.shared.paths as paths_mod

    out_dir = tmp_path / "optimization"
    monkeypatch.setattr(paths_mod, "get_optimization_results_dir", lambda _strategy: out_dir)
    monkeypatch.setattr(
        notebook_mod,
        "generate_optimization_notebook",
        lambda **kwargs: kwargs["output_path"],
    )

    out = engine._generate_visualization_notebook([{"score": 1.0}], [{"id": 1}])
    assert Path(out).name.startswith("primeExTopix500_")
    assert out.endswith(".html")


def test_format_params_adds_entry_exit_prefixes():
    engine = _make_engine()
    formatted = engine._format_params(
        {
            "entry_filter_params.rsi_threshold.period": 14,
            "exit_trigger_params.rsi_threshold.period": 21,
            "misc": "x",
        }
    )
    assert "entry_period=14" in formatted
    assert "exit_period=21" in formatted
    assert "misc=x" in formatted


def test_configure_logger_non_verbose_reconfigures_logger(monkeypatch):
    calls: list[tuple[str, tuple, dict]] = []

    class FakeLogger:
        def remove(self):
            calls.append(("remove", (), {}))

        def add(self, *args, **kwargs):
            calls.append(("add", args, kwargs))

    monkeypatch.setattr(engine_mod, "logger", FakeLogger())
    ParameterOptimizationEngine._configure_logger(False)
    assert calls[0][0] == "remove"
    assert calls[1][0] == "add"
    assert calls[1][2]["level"] == "WARNING"


def test_configure_logger_verbose_keeps_existing_logger(monkeypatch):
    calls: list[str] = []

    class FakeLogger:
        def remove(self):
            calls.append("remove")

        def add(self, *args, **kwargs):
            calls.append("add")

    monkeypatch.setattr(engine_mod, "logger", FakeLogger())
    ParameterOptimizationEngine._configure_logger(True)
    assert calls == []
