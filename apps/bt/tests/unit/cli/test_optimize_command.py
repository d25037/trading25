"""Unit tests for src.entrypoints.cli.optimize."""

from __future__ import annotations

import sys
import types

import pytest

from src.entrypoints.cli import optimize as optimize_module


def _make_result() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        best_score=1.2345,
        scoring_weights={"sharpe_ratio": 0.6, "total_return": 0.4},
        best_params={"entry_filter_params.period": 20, "exit_trigger_params.threshold": 10},
        best_portfolio=object(),
        all_results=[
            {"params": {"a": 1}, "score": 0.5},
            {"params": {"a": 2}, "score": 0.8},
        ],
        html_path="/tmp/result.html",
    )


def test_format_params_table() -> None:
    formatted = optimize_module._format_params_table(
        {
            "entry_filter_params.period": 20,
            "exit_trigger_params.threshold": 10,
        }
    )

    assert "period=20" in formatted
    assert "threshold=10" in formatted


def test_display_ranking_prints_table(monkeypatch: pytest.MonkeyPatch) -> None:
    printed: list[object] = []
    monkeypatch.setattr(
        optimize_module.console,
        "print",
        lambda value=None, *args, **kwargs: printed.append(value),  # noqa: ARG005
    )

    optimize_module._display_ranking(_make_result(), top_n=2)

    assert any(value is not None for value in printed)


def test_display_best_params_uses_canonical_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    printed: list[object] = []
    monkeypatch.setattr(
        optimize_module.console,
        "print",
        lambda value=None, *args, **kwargs: printed.append(value),  # noqa: ARG005
    )
    monkeypatch.setattr(
        optimize_module,
        "canonical_metrics_from_portfolio",
        lambda _portfolio: types.SimpleNamespace(
            sharpe_ratio=1.1,
            calmar_ratio=0.8,
            total_return=0.12,
            max_drawdown=-0.05,
        ),
    )

    optimize_module._display_best_params(_make_result())

    rendered = "\n".join(str(value) for value in printed if value is not None)
    assert "Sharpe Ratio" in rendered
    assert "Total Return" in rendered


def test_display_best_params_prints_error_when_metrics_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    printed: list[object] = []
    monkeypatch.setattr(
        optimize_module.console,
        "print",
        lambda value=None, *args, **kwargs: printed.append(value),  # noqa: ARG005
    )
    monkeypatch.setattr(
        optimize_module,
        "canonical_metrics_from_portfolio",
        lambda _portfolio: None,
    )

    optimize_module._display_best_params(_make_result())

    assert any("指標取得エラー" in str(value) for value in printed)


def test_display_html_path_prints_result(monkeypatch: pytest.MonkeyPatch) -> None:
    printed: list[object] = []
    monkeypatch.setattr(
        optimize_module.console,
        "print",
        lambda value=None, *args, **kwargs: printed.append(value),  # noqa: ARG005
    )

    optimize_module._display_html_path(_make_result())

    assert any("/tmp/result.html" in str(value) for value in printed)


def test_run_optimization_success(monkeypatch: pytest.MonkeyPatch) -> None:
    printed: list[object] = []

    class _FakeEngine:
        def __init__(self, strategy_name: str, grid_config_path=None, verbose: bool = False):
            _ = (strategy_name, grid_config_path, verbose)
            self.base_config_path = "/tmp/base.yaml"
            self.optimization_config = {"n_jobs": 2}
            self.total_combinations = 12

        def optimize(self):
            return _make_result()

    monkeypatch.setattr(optimize_module, "ParameterOptimizationEngine", _FakeEngine)
    monkeypatch.setattr(optimize_module, "_display_ranking", lambda *args, **kwargs: None)
    monkeypatch.setattr(optimize_module, "_display_best_params", lambda *args, **kwargs: None)
    monkeypatch.setattr(optimize_module, "_display_html_path", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        optimize_module.console,
        "print",
        lambda value=None, *args, **kwargs: printed.append(value),  # noqa: ARG005
    )

    optimize_module.run_optimization("strategy")

    assert any("最適化完了" in str(value) for value in printed)


@pytest.mark.parametrize(
    ("error", "expected_exit"),
    [
        (FileNotFoundError("missing"), 1),
        (ValueError("bad config"), 1),
        (RuntimeError("runtime"), 1),
    ],
)
def test_run_optimization_known_errors_exit_with_one(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_exit: int,
) -> None:
    class _FailEngine:
        def __init__(self, *args, **kwargs):
            raise error

    monkeypatch.setattr(optimize_module, "ParameterOptimizationEngine", _FailEngine)
    monkeypatch.setattr(optimize_module.console, "print", lambda *args, **kwargs: None)  # noqa: ARG005

    with pytest.raises(SystemExit) as excinfo:
        optimize_module.run_optimization("strategy")

    assert excinfo.value.code == expected_exit


def test_run_optimization_keyboard_interrupt_exits_130(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _InterruptEngine:
        def __init__(self, *args, **kwargs):
            _ = (args, kwargs)
            self.base_config_path = "/tmp/base.yaml"
            self.optimization_config = {"n_jobs": 2}
            self.total_combinations = 12

        def optimize(self):
            raise KeyboardInterrupt()

    monkeypatch.setattr(optimize_module, "ParameterOptimizationEngine", _InterruptEngine)
    monkeypatch.setattr(optimize_module.console, "print", lambda *args, **kwargs: None)  # noqa: ARG005

    with pytest.raises(SystemExit) as excinfo:
        optimize_module.run_optimization("strategy")

    assert excinfo.value.code == 130


def test_run_optimization_unexpected_error_prints_traceback_and_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenEngine:
        def __init__(self, *args, **kwargs):
            _ = (args, kwargs)
            self.base_config_path = "/tmp/base.yaml"
            self.optimization_config = {"n_jobs": 2}
            self.total_combinations = 12

        def optimize(self):
            raise Exception("boom")

    monkeypatch.setattr(optimize_module, "ParameterOptimizationEngine", _BrokenEngine)
    monkeypatch.setattr(optimize_module.console, "print", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setitem(
        sys.modules,
        "traceback",
        types.SimpleNamespace(print_exc=lambda: None),
    )

    with pytest.raises(SystemExit) as excinfo:
        optimize_module.run_optimization("strategy")

    assert excinfo.value.code == 1
