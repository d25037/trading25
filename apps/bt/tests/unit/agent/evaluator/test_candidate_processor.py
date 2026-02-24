"""candidate_processor.py のテスト"""

from contextlib import contextmanager

import pandas as pd

from src.domains.lab_agent.evaluator import candidate_processor
from src.domains.lab_agent.evaluator.candidate_processor import _safe_float, evaluate_single_candidate
from src.domains.lab_agent.models import StrategyCandidate


class TestSafeFloat:
    def test_normal_value(self):
        assert _safe_float(1.5) == 1.5

    def test_zero(self):
        assert _safe_float(0.0) == 0.0

    def test_negative(self):
        assert _safe_float(-3.14) == -3.14

    def test_nan_returns_default(self):
        assert _safe_float(float("nan")) == 0.0

    def test_inf_returns_default(self):
        assert _safe_float(float("inf")) == 0.0

    def test_neg_inf_returns_default(self):
        assert _safe_float(float("-inf")) == 0.0

    def test_custom_default(self):
        assert _safe_float(float("nan"), default=-1.0) == -1.0

    def test_large_value(self):
        assert _safe_float(1e15) == 1e15


def _candidate() -> StrategyCandidate:
    return StrategyCandidate(
        strategy_id="c1",
        entry_filter_params={},
        exit_trigger_params={},
        shared_config={},
    )


def test_evaluate_single_candidate_success_with_prefetched_data(monkeypatch) -> None:
    mode_calls: list[str] = []
    created: dict[str, object] = {}

    @contextmanager
    def _fake_mode_context(mode: str):
        mode_calls.append(mode)
        yield

    class _FakeSharedConfig:
        def __init__(self, **kwargs):
            self.params = kwargs
            self.kelly_fraction = 0.5
            self.min_allocation = 0.01
            self.max_allocation = 0.5

    class _FakePortfolio:
        def sharpe_ratio(self) -> float:
            return 1.2

        def calmar_ratio(self) -> float:
            return 0.8

        def total_return(self) -> float:
            return 0.2

        def max_drawdown(self) -> float:
            return -0.1

        class trades:
            records_readable = pd.DataFrame({"Return": [0.1, -0.2, 0.3]})

    class _FakeStrategy:
        def __init__(self, shared_config, entry_filter_params, exit_trigger_params):
            self.shared_config = shared_config
            self.entry_filter_params = entry_filter_params
            self.exit_trigger_params = exit_trigger_params
            self.multi_data_dict = None
            self.benchmark_data = None
            created["strategy"] = self

        def run_optimized_backtest_kelly(self, **_kwargs):
            return None, _FakePortfolio(), None, None, None

    monkeypatch.setattr(candidate_processor, "data_access_mode_context", _fake_mode_context)
    monkeypatch.setattr(candidate_processor, "SharedConfig", _FakeSharedConfig)
    monkeypatch.setattr(candidate_processor, "YamlConfigurableStrategy", _FakeStrategy)
    monkeypatch.setattr(
        candidate_processor,
        "convert_dict_to_dataframes",
        lambda _data: {"7203": {"daily": pd.DataFrame({"Close": [1.0]})}},
    )

    result = evaluate_single_candidate(
        candidate=_candidate(),
        shared_config_dict={"dataset": "demo", "stock_codes": ["all"]},
        scoring_weights={"sharpe_ratio": 0.5, "calmar_ratio": 0.3, "total_return": 0.2},
        pre_fetched_stock_codes=["7203", "9984"],
        pre_fetched_ohlcv_data={"dummy": {}},
        pre_fetched_benchmark_data={
            "data": [[100.0], [101.0]],
            "index": ["2025-01-01", "2025-01-02"],
            "columns": ["Close"],
        },
    )

    assert result.success is True
    assert result.trade_count == 3
    assert result.win_rate == 2 / 3
    assert mode_calls == ["direct"]
    strategy = created["strategy"]
    assert getattr(strategy, "multi_data_dict") is not None
    assert getattr(strategy, "benchmark_data") is not None


def test_evaluate_single_candidate_trade_parse_error(monkeypatch) -> None:
    @contextmanager
    def _fake_mode_context(_mode: str):
        yield

    class _FakeSharedConfig:
        def __init__(self, **_kwargs):
            self.kelly_fraction = 0.5
            self.min_allocation = 0.01
            self.max_allocation = 0.5

    class _BrokenTrades:
        @property
        def records_readable(self):
            raise RuntimeError("broken")

    class _FakePortfolio:
        def sharpe_ratio(self) -> float:
            return 1.0

        def calmar_ratio(self) -> float:
            return 0.5

        def total_return(self) -> float:
            return 0.1

        def max_drawdown(self) -> float:
            return -0.2

        trades = _BrokenTrades()

    class _FakeStrategy:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_optimized_backtest_kelly(self, **_kwargs):
            return None, _FakePortfolio(), None, None, None

    monkeypatch.setattr(candidate_processor, "SharedConfig", _FakeSharedConfig)
    monkeypatch.setattr(candidate_processor, "YamlConfigurableStrategy", _FakeStrategy)
    monkeypatch.setattr(candidate_processor, "data_access_mode_context", _fake_mode_context)

    result = evaluate_single_candidate(
        candidate=_candidate(),
        shared_config_dict={"dataset": "demo", "stock_codes": ["7203"]},
        scoring_weights={"sharpe_ratio": 1.0},
    )

    assert result.success is True
    assert result.win_rate == 0.0
    assert result.trade_count == 0


def test_evaluate_single_candidate_failure_returns_failed_result(monkeypatch) -> None:
    def _raise(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(candidate_processor, "SignalParams", _raise)

    result = evaluate_single_candidate(
        candidate=_candidate(),
        shared_config_dict={},
        scoring_weights={},
    )

    assert result.success is False
    assert result.score == -999.0
    assert "boom" in (result.error_message or "")
