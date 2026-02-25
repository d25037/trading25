"""optimization/metrics.py のメトリクス収集テスト"""

from unittest.mock import MagicMock

import pandas as pd

from src.domains.optimization.engine import ParameterOptimizationEngine
from src.domains.optimization.metrics import collect_metrics
from src.shared.models.signals import SignalParams


def _make_scoring_weights() -> dict[str, float]:
    return {
        "sharpe_ratio": 0.4,
        "calmar_ratio": 0.3,
        "total_return": 0.3,
    }


def _make_portfolio() -> MagicMock:
    portfolio = MagicMock()
    portfolio.sharpe_ratio.return_value = 1.5
    portfolio.calmar_ratio.return_value = 1.2
    portfolio.total_return.return_value = 0.25
    return portfolio


def test_collect_metrics_includes_trade_count_from_trades_count():
    portfolio = _make_portfolio()
    portfolio.trades.count.return_value = pd.Series([3, 4, 5])

    metrics = collect_metrics(portfolio, _make_scoring_weights())

    assert metrics["sharpe_ratio"] == 1.5
    assert metrics["calmar_ratio"] == 1.2
    assert metrics["total_return"] == 0.25
    assert metrics["trade_count"] == 12


def test_collect_metrics_fallbacks_to_records_readable_when_count_fails():
    portfolio = _make_portfolio()
    portfolio.trades.count.side_effect = RuntimeError("count unavailable")
    portfolio.trades.records_readable = pd.DataFrame({"PnL": [1.0, -0.5, 0.2]})

    metrics = collect_metrics(portfolio, _make_scoring_weights())

    assert metrics["trade_count"] == 3


def test_collect_metrics_sets_trade_count_zero_when_trades_access_fails():
    portfolio = _make_portfolio()

    class _BrokenTrades:
        def count(self):
            raise RuntimeError("count failed")

        @property
        def records_readable(self):
            raise RuntimeError("records failed")

    portfolio.trades = _BrokenTrades()

    metrics = collect_metrics(portfolio, _make_scoring_weights())

    assert metrics["trade_count"] == 0


def test_collect_metrics_ignores_unsupported_metric_key():
    portfolio = _make_portfolio()
    portfolio.trades.count.return_value = 1

    metrics = collect_metrics(portfolio, {"unsupported": 1.0})

    assert "unsupported" not in metrics
    assert metrics["trade_count"] == 1


def test_engine_collect_metrics_delegates_to_metrics_module():
    engine = object.__new__(ParameterOptimizationEngine)
    engine.optimization_config = {"scoring_weights": _make_scoring_weights()}
    portfolio = _make_portfolio()
    portfolio.trades.count.return_value = 2

    metrics = engine._collect_metrics(portfolio)

    assert metrics["trade_count"] == 2


def test_engine_should_include_forecast_revision_when_base_signal_enabled():
    engine = object.__new__(ParameterOptimizationEngine)
    entry = SignalParams()
    entry.fundamental.enabled = True
    entry.fundamental.forward_eps_growth.enabled = True
    engine.base_entry_params = entry
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = {}

    assert engine._should_include_forecast_revision() is True


def test_engine_should_include_forecast_revision_when_forward_payout_enabled():
    engine = object.__new__(ParameterOptimizationEngine)
    entry = SignalParams()
    entry.fundamental.enabled = True
    entry.fundamental.forward_payout_ratio.enabled = True
    engine.base_entry_params = entry
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = {}

    assert engine._should_include_forecast_revision() is True


def test_engine_should_include_forecast_revision_when_forecast_vs_actual_enabled():
    engine = object.__new__(ParameterOptimizationEngine)
    entry = SignalParams()
    entry.fundamental.enabled = True
    entry.fundamental.forecast_eps_above_all_actuals.enabled = True
    engine.base_entry_params = entry
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = {}

    assert engine._should_include_forecast_revision() is True


def test_engine_should_include_forecast_revision_when_grid_can_enable():
    engine = object.__new__(ParameterOptimizationEngine)
    entry = SignalParams()
    exit_params = SignalParams()
    engine.base_entry_params = entry
    engine.base_exit_params = exit_params
    engine.parameter_ranges = {
        "entry_filter_params": {
            "fundamental": {
                "enabled": [False, True],
                "peg_ratio": {
                    "enabled": [False, True],
                    "threshold": [0.8, 1.2],
                },
            }
        }
    }

    assert engine._should_include_forecast_revision() is True


def test_engine_should_include_forecast_revision_when_grid_can_enable_forecast_vs_actual():
    engine = object.__new__(ParameterOptimizationEngine)
    engine.base_entry_params = SignalParams()
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = {
        "entry_filter_params": {
            "fundamental": {
                "enabled": [False, True],
                "forecast_eps_above_all_actuals": {
                    "enabled": [False, True],
                },
            }
        }
    }

    assert engine._should_include_forecast_revision() is True


def test_engine_forecast_helpers_return_false_when_not_enabled():
    params = SignalParams()
    params.fundamental.enabled = False
    params.fundamental.forecast_eps_above_all_actuals.enabled = True

    engine = object.__new__(ParameterOptimizationEngine)
    assert engine._is_forecast_signal_enabled(params) is False


def test_engine_grid_forecast_helpers_return_false_for_non_dict_and_disabled_grid():
    engine = object.__new__(ParameterOptimizationEngine)
    engine.base_entry_params = SignalParams()
    engine.base_exit_params = SignalParams()
    engine.parameter_ranges = None
    assert engine._grid_may_enable_forecast_signals() is False

    engine.parameter_ranges = {
        "entry_filter_params": {
            "fundamental": {
                "enabled": [False],
                "forecast_eps_above_all_actuals": {"enabled": [True]},
            }
        }
    }
    assert engine._grid_may_enable_forecast_signals() is False
