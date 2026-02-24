"""Signal attribution core logic tests."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from src.domains.backtest.core.signal_attribution import (
    AttributionMetrics,
    SignalAttributionAnalyzer,
    SignalAttributionCancelled,
    SignalTarget,
    StrategyRuntimeCache,
    _build_signal_params,
    _disable_signal_in_parameters,
    _evaluate_parameters,
    _iter_enabled_signals,
    _safe_metric,
)
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY


def _make_parameters(
    entry: dict[str, Any] | None = None,
    exit_: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "shared_config": {},
        "entry_filter_params": entry or {},
        "exit_trigger_params": exit_ or {},
    }


def _enabled_signal_ids(parameters: dict[str, Any]) -> set[str]:
    entry_signal_params, exit_signal_params = _build_signal_params(parameters)
    return {
        target.signal_id
        for target in _iter_enabled_signals(entry_signal_params, exit_signal_params)
    }


def _evaluate_from_weights(
    weights: dict[str, tuple[float, float]],
    *,
    fail_when_disabled: str | None = None,
):
    def _hook(
        parameters: dict[str, Any],
        runtime_cache: StrategyRuntimeCache | None,
    ) -> tuple[AttributionMetrics, StrategyRuntimeCache]:
        enabled = _enabled_signal_ids(parameters)
        if fail_when_disabled and fail_when_disabled not in enabled:
            raise RuntimeError(f"forced failure: {fail_when_disabled}")

        total_return = 0.0
        sharpe_ratio = 0.0
        for signal_id in enabled:
            if signal_id not in weights:
                continue
            ret, sharpe = weights[signal_id]
            total_return += ret
            sharpe_ratio += sharpe

        return (
            AttributionMetrics(total_return=total_return, sharpe_ratio=sharpe_ratio),
            runtime_cache or StrategyRuntimeCache(),
        )

    return _hook


def test_iter_enabled_signals_entry_exit_and_nested() -> None:
    parameters = _make_parameters(
        entry={
            "volume": {"enabled": True},
            "fundamental": {"enabled": True, "per": {"enabled": True}},
            "buy_and_hold": {"enabled": True},
        },
        exit_={
            "volume": {"enabled": True},
            "fundamental": {"enabled": True, "per": {"enabled": True}},
            "buy_and_hold": {"enabled": True},
        },
    )

    signal_ids = _enabled_signal_ids(parameters)

    assert "entry.volume" in signal_ids
    assert "exit.volume" in signal_ids
    assert "entry.fundamental.per" in signal_ids
    assert "exit.fundamental.per" in signal_ids
    assert "entry.buy_and_hold" in signal_ids
    assert "exit.buy_and_hold" not in signal_ids  # exit_disabled=True


def test_nested_signal_requires_top_level_enabled_flag() -> None:
    parameters = _make_parameters(
        entry={"fundamental": {"enabled": False, "per": {"enabled": True}}},
    )
    assert "entry.fundamental.per" not in _enabled_signal_ids(parameters)


def test_loo_delta_sign_is_baseline_minus_variant() -> None:
    parameters = _make_parameters(
        entry={"volume": {"enabled": True}},
        exit_={"volume": {"enabled": True}},
    )
    weights = {
        "entry.volume": (10.0, 1.0),
        "exit.volume": (-3.0, -0.3),
    }
    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        shapley_top_n=1,
        parameters_hook=lambda: parameters,
        evaluate_hook=_evaluate_from_weights(weights),
    )

    result = analyzer.run()
    by_signal = {s["signal_id"]: s for s in result["signals"]}

    assert by_signal["entry.volume"]["loo"]["delta_total_return"] == pytest.approx(10.0)
    assert by_signal["entry.volume"]["loo"]["delta_sharpe_ratio"] == pytest.approx(1.0)
    assert by_signal["exit.volume"]["loo"]["delta_total_return"] == pytest.approx(-3.0)
    assert by_signal["exit.volume"]["loo"]["delta_sharpe_ratio"] == pytest.approx(-0.3)


def test_top_n_selection_uses_normalized_abs_loo_score() -> None:
    parameters = _make_parameters(
        entry={
            "volume": {"enabled": True},
            "fundamental": {"enabled": True, "per": {"enabled": True}},
        },
        exit_={"volume": {"enabled": True}},
    )
    weights = {
        "entry.volume": (10.0, 0.5),
        "entry.fundamental.per": (5.0, 2.0),
        "exit.volume": (1.0, 0.1),
    }
    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        shapley_top_n=2,
        parameters_hook=lambda: parameters,
        evaluate_hook=_evaluate_from_weights(weights),
    )

    result = analyzer.run()
    top_n = result["top_n_selection"]
    assert top_n["top_n_effective"] == 2
    assert top_n["selected_signal_ids"] == [
        "entry.fundamental.per",
        "entry.volume",
    ]


def test_shapley_exact_matches_additive_weights() -> None:
    parameters = _make_parameters(
        entry={
            "volume": {"enabled": True},
            "fundamental": {"enabled": True, "per": {"enabled": True}},
        },
    )
    weights = {
        "entry.volume": (7.0, 0.7),
        "entry.fundamental.per": (2.0, 0.2),
    }
    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        shapley_top_n=2,
        parameters_hook=lambda: parameters,
        evaluate_hook=_evaluate_from_weights(weights),
    )

    result = analyzer.run()
    by_signal = {s["signal_id"]: s for s in result["signals"]}

    assert result["shapley"]["method"] == "exact"
    assert result["shapley"]["sample_size"] == 4
    assert by_signal["entry.volume"]["shapley"]["total_return"] == pytest.approx(7.0)
    assert by_signal["entry.volume"]["shapley"]["sharpe_ratio"] == pytest.approx(0.7)
    assert by_signal["entry.fundamental.per"]["shapley"]["total_return"] == pytest.approx(2.0)
    assert by_signal["entry.fundamental.per"]["shapley"]["sharpe_ratio"] == pytest.approx(0.2)


def test_loo_failure_is_isolated_per_signal() -> None:
    parameters = _make_parameters(
        entry={"volume": {"enabled": True}},
        exit_={"volume": {"enabled": True}},
    )
    weights = {
        "entry.volume": (3.0, 0.3),
        "exit.volume": (1.0, 0.1),
    }
    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        shapley_top_n=2,
        parameters_hook=lambda: parameters,
        evaluate_hook=_evaluate_from_weights(
            weights,
            fail_when_disabled="entry.volume",
        ),
    )

    result = analyzer.run()
    by_signal = {s["signal_id"]: s for s in result["signals"]}

    assert by_signal["entry.volume"]["loo"]["status"] == "error"
    assert by_signal["exit.volume"]["loo"]["status"] == "ok"
    assert result["top_n_selection"]["selected_signal_ids"] == ["exit.volume"]


def test_safe_metric_and_disable_signal_helpers() -> None:
    class _WithMean:
        def mean(self) -> float:
            return 2.5

    assert _safe_metric(_WithMean()) == pytest.approx(2.5)
    assert _safe_metric(float("inf")) == 0.0
    assert _safe_metric("not-a-number") == 0.0

    params: dict[str, Any] = {"entry_filter_params": "invalid"}
    _disable_signal_in_parameters(params, scope="entry", param_key="volume")
    assert params["entry_filter_params"]["volume"]["enabled"] is False

    nested_params: dict[str, Any] = {"entry_filter_params": {"fundamental": "invalid"}}
    _disable_signal_in_parameters(
        nested_params,
        scope="entry",
        param_key="fundamental.per",
    )
    assert nested_params["entry_filter_params"]["fundamental"]["per"]["enabled"] is False


def test_run_with_no_enabled_signals() -> None:
    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        parameters_hook=lambda: _make_parameters(),
        evaluate_hook=lambda _p, rc: (
            AttributionMetrics(total_return=0.0, sharpe_ratio=0.0),
            rc or StrategyRuntimeCache(),
        ),
    )
    progresses: list[float] = []
    result = analyzer.run(progress_callback=lambda _m, p: progresses.append(p))

    assert result["signals"] == []
    assert result["top_n_selection"]["top_n_effective"] == 0
    assert result["top_n_selection"]["selected_signal_ids"] == []
    assert result["shapley"]["method"] is None
    assert progresses[-1] == 1.0


def test_run_raises_when_cancelled_after_baseline() -> None:
    parameters = _make_parameters(
        entry={"volume": {"enabled": True}},
        exit_={"volume": {"enabled": True}},
    )
    weights = {
        "entry.volume": (1.0, 0.1),
        "exit.volume": (1.0, 0.1),
    }
    cancel_state = {"cancel": False}

    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        parameters_hook=lambda: parameters,
        evaluate_hook=_evaluate_from_weights(weights),
        cancel_check=lambda: cancel_state["cancel"],
    )

    def _progress(message: str, _progress: float) -> None:
        if message == "Signal attribution: baseline 完了":
            cancel_state["cancel"] = True

    with pytest.raises(SignalAttributionCancelled):
        analyzer.run(progress_callback=_progress)


def test_compute_shapley_raises_when_cancelled() -> None:
    selected_signals = [
        SignalTarget(
            signal_id="entry.sig",
            scope="entry",
            param_key="sig",
            signal_name="sig",
            definition=SIGNAL_REGISTRY[0],
        )
    ]

    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        evaluate_hook=lambda _p, rc: (
            AttributionMetrics(total_return=1.0, sharpe_ratio=1.0),
            rc or StrategyRuntimeCache(),
        ),
        cancel_check=lambda: True,
    )

    with pytest.raises(SignalAttributionCancelled):
        analyzer._compute_shapley(
            baseline_parameters={},
            selected_signals=selected_signals,
            runtime_cache=StrategyRuntimeCache(),
            progress_callback=None,
        )


def test_shapley_failure_is_reported_without_stopping_job() -> None:
    parameters = _make_parameters(
        entry={"volume": {"enabled": True}},
        exit_={"volume": {"enabled": True}},
    )
    weights = {
        "entry.volume": (3.0, 0.3),
        "exit.volume": (2.0, 0.2),
    }

    def _hook(
        payload: dict[str, Any],
        runtime_cache: StrategyRuntimeCache | None,
    ) -> tuple[AttributionMetrics, StrategyRuntimeCache]:
        enabled = _enabled_signal_ids(payload)
        if "entry.volume" not in enabled and "exit.volume" not in enabled:
            raise RuntimeError("forced shapley subset failure")
        total_return = sum(weights[sid][0] for sid in enabled if sid in weights)
        sharpe_ratio = sum(weights[sid][1] for sid in enabled if sid in weights)
        return (
            AttributionMetrics(total_return=total_return, sharpe_ratio=sharpe_ratio),
            runtime_cache or StrategyRuntimeCache(),
        )

    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        shapley_top_n=2,
        parameters_hook=lambda: parameters,
        evaluate_hook=_hook,
    )
    result = analyzer.run()
    by_signal = {s["signal_id"]: s for s in result["signals"]}

    assert result["shapley"]["method"] == "error"
    assert by_signal["entry.volume"]["loo"]["status"] == "ok"
    assert by_signal["exit.volume"]["loo"]["status"] == "ok"
    assert by_signal["entry.volume"]["shapley"]["status"] == "error"
    assert by_signal["exit.volume"]["shapley"]["status"] == "error"


def test_compute_shapley_permutation_path() -> None:
    selected_signals = [
        SignalTarget(
            signal_id=f"entry.sig{i}",
            scope="entry",
            param_key=f"sig{i}",
            signal_name=f"sig{i}",
            definition=SIGNAL_REGISTRY[0],
        )
        for i in range(9)
    ]

    def _hook(
        payload: dict[str, Any],
        runtime_cache: StrategyRuntimeCache | None,
    ) -> tuple[AttributionMetrics, StrategyRuntimeCache]:
        section = payload.get("entry_filter_params", {})
        active = 0.0
        for i in range(9):
            entry = section.get(f"sig{i}", {})
            disabled = isinstance(entry, dict) and entry.get("enabled") is False
            if not disabled:
                active += 1.0
        return (
            AttributionMetrics(total_return=active, sharpe_ratio=active),
            runtime_cache or StrategyRuntimeCache(),
        )

    analyzer = SignalAttributionAnalyzer(
        strategy_name="dummy",
        shapley_permutations=16,
        random_seed=7,
        evaluate_hook=_hook,
    )
    values, meta = analyzer._compute_shapley(
        baseline_parameters={},
        selected_signals=selected_signals,
        runtime_cache=StrategyRuntimeCache(),
        progress_callback=None,
    )

    assert meta["method"] == "permutation"
    assert meta["sample_size"] == 16
    assert meta["evaluations"] >= 1
    for player in values.values():
        assert player["total_return"] == pytest.approx(1.0)
        assert player["sharpe_ratio"] == pytest.approx(1.0)


def test_load_parameters_uses_runner_when_hook_absent() -> None:
    built_parameters = {"shared_config": {"dataset": "sample"}}
    analyzer = SignalAttributionAnalyzer(
        strategy_name="production/demo",
        config_override={"shared_config": {"initial_cash": 123}},
    )
    analyzer._runner.build_parameters_for_strategy = lambda strategy, config_override: built_parameters  # type: ignore[assignment]

    loaded = analyzer._load_parameters()
    loaded["shared_config"]["dataset"] = "changed"

    assert built_parameters["shared_config"]["dataset"] == "sample"


def test_evaluate_falls_back_to_default_function(monkeypatch: Any) -> None:
    called: dict[str, Any] = {}

    def _fake_eval(
        parameters: dict[str, Any],
        runtime_cache: StrategyRuntimeCache | None,
    ) -> tuple[AttributionMetrics, StrategyRuntimeCache]:
        called["parameters"] = parameters
        called["runtime_cache"] = runtime_cache
        return AttributionMetrics(total_return=1.0, sharpe_ratio=2.0), StrategyRuntimeCache()

    monkeypatch.setattr(
        "src.domains.backtest.core.signal_attribution._evaluate_parameters",
        _fake_eval,
    )

    analyzer = SignalAttributionAnalyzer(strategy_name="dummy")
    metrics, runtime_cache = analyzer._evaluate({"shared_config": {}})

    assert called["parameters"] == {"shared_config": {}}
    assert isinstance(runtime_cache, StrategyRuntimeCache)
    assert metrics.total_return == pytest.approx(1.0)
    assert metrics.sharpe_ratio == pytest.approx(2.0)


def test_evaluate_parameters_applies_cache_and_extracts_metrics(monkeypatch: Any) -> None:
    class _FakePortfolio:
        def total_return(self) -> Any:
            class _WithMean:
                def mean(self) -> float:
                    return 12.5

            return _WithMean()

        def sharpe_ratio(self) -> float:
            return 1.75

    class _FakeSharedConfig:
        kelly_fraction = 0.5
        min_allocation = 0.1
        max_allocation = 0.9

    class _FakeStrategy:
        def __init__(self) -> None:
            self.multi_data_dict: dict[str, Any] | None = {"original": 1}
            self.benchmark_data: pd.DataFrame | None = pd.DataFrame({"Close": [1.0]})
            self.relative_data_dict: dict[str, Any] | None = {"original": 1}
            self.execution_data_dict: dict[str, Any] | None = {"original": 1}

        def run_optimized_backtest_kelly(self, **kwargs: Any) -> tuple[Any, ...]:
            assert kwargs["kelly_fraction"] == 0.5
            assert kwargs["min_allocation"] == 0.1
            assert kwargs["max_allocation"] == 0.9
            assert self.multi_data_dict == {"cached": 1}
            self.multi_data_dict = {"after": 1}
            self.benchmark_data = pd.DataFrame({"Close": [2.0]})
            self.relative_data_dict = {"after": 1}
            self.execution_data_dict = {"after": 1}
            return None, _FakePortfolio(), None, None, None

    fake_strategy = _FakeStrategy()
    monkeypatch.setattr(
        "src.domains.backtest.core.signal_attribution._create_strategy_from_parameters",
        lambda _p: (fake_strategy, _FakeSharedConfig()),
    )

    runtime_cache = StrategyRuntimeCache(
        multi_data_dict={"cached": 1},
        benchmark_data=pd.DataFrame({"Close": [0.0]}),
        relative_data_dict={"cached": 1},
        execution_data_dict={"cached": 1},
    )
    metrics, returned_cache = _evaluate_parameters(
        parameters={"shared_config": {}},
        runtime_cache=runtime_cache,
    )

    assert metrics.total_return == pytest.approx(12.5)
    assert metrics.sharpe_ratio == pytest.approx(1.75)
    assert returned_cache.multi_data_dict == {"after": 1}
    assert returned_cache.relative_data_dict == {"after": 1}
    assert returned_cache.execution_data_dict == {"after": 1}
