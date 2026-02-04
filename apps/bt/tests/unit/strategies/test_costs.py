"""
Cost model tests for BacktestExecutorMixin
"""

import pandas as pd

from src.strategies.core.mixins.backtest_executor_mixin import BacktestExecutorMixin


class _DummyStrategy(BacktestExecutorMixin):
    def __init__(self):
        self.direction = "longonly"
        self.initial_cash = 100000
        self.fees = 0.001
        self.slippage = 0.002
        self.spread = 0.003
        self.borrow_fee = 0.01
        self.max_concurrent_positions = None
        self.max_exposure = None

    def _log(self, message: str, level: str = "info") -> None:
        pass


def test_effective_fees_used(monkeypatch):
    dummy = _DummyStrategy()

    data = pd.DataFrame(
        {"Close": [100.0, 101.0], "Volume": [1000.0, 1100.0]},
        index=pd.date_range("2023-01-01", periods=2),
    )
    data_dict = {"TEST": data}
    entries_dict = {"TEST": pd.Series([True, False], index=data.index)}
    exits_dict = {"TEST": pd.Series([False, True], index=data.index)}

    captured = {}

    def _fake_from_signals(*args, **kwargs):
        captured["fees"] = kwargs.get("fees")
        captured["slippage"] = kwargs.get("slippage")
        return "portfolio"

    monkeypatch.setattr(
        "vectorbt.Portfolio.from_signals", _fake_from_signals, raising=False
    )

    result = dummy._create_individual_portfolios(
        data_dict, entries_dict, exits_dict, pyramid_enabled=False
    )

    assert result == "portfolio"
    # fees = fees(0.001) + spread(0.003) — slippageは分離
    assert captured["fees"] == 0.001 + 0.003
    # slippageはvectorbtネイティブパラメータとして渡される
    assert captured["slippage"] == 0.002


def test_effective_fees_with_short_direction(monkeypatch):
    """shortonly/both方向ではborrow_feeがfeesに加算される"""
    dummy = _DummyStrategy()
    dummy.direction = "shortonly"

    data = pd.DataFrame(
        {"Close": [100.0, 101.0], "Volume": [1000.0, 1100.0]},
        index=pd.date_range("2023-01-01", periods=2),
    )
    data_dict = {"TEST": data}
    entries_dict = {"TEST": pd.Series([True, False], index=data.index)}
    exits_dict = {"TEST": pd.Series([False, True], index=data.index)}

    captured = {}

    def _fake_from_signals(*args, **kwargs):
        captured["fees"] = kwargs.get("fees")
        captured["slippage"] = kwargs.get("slippage")
        return "portfolio"

    monkeypatch.setattr(
        "vectorbt.Portfolio.from_signals", _fake_from_signals, raising=False
    )

    dummy._create_individual_portfolios(
        data_dict, entries_dict, exits_dict, pyramid_enabled=False
    )

    # fees = fees(0.001) + spread(0.003) + borrow_fee(0.01) — slippageは分離
    assert captured["fees"] == 0.001 + 0.003 + 0.01
    assert captured["slippage"] == 0.002


class TestCalculateCostParams:
    """_calculate_cost_params helper unit tests"""

    def test_longonly(self):
        dummy = _DummyStrategy()
        fees, slippage = dummy._calculate_cost_params()
        assert fees == 0.001 + 0.003
        assert slippage == 0.002

    def test_shortonly_includes_borrow_fee(self):
        dummy = _DummyStrategy()
        dummy.direction = "shortonly"
        fees, slippage = dummy._calculate_cost_params()
        assert fees == 0.001 + 0.003 + 0.01
        assert slippage == 0.002

    def test_both_includes_borrow_fee(self):
        dummy = _DummyStrategy()
        dummy.direction = "both"
        fees, slippage = dummy._calculate_cost_params()
        assert fees == 0.001 + 0.003 + 0.01
        assert slippage == 0.002
