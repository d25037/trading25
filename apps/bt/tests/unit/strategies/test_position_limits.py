"""
Position limit tests
"""

import pandas as pd

from src.strategies.core.mixins.backtest_executor_mixin import BacktestExecutorMixin


class _DummyStrategy(BacktestExecutorMixin):
    def __init__(self):
        self.direction = "longonly"
        self.initial_cash = 100000
        self.fees = 0.001
        self.slippage = 0.0
        self.spread = 0.0
        self.borrow_fee = 0.0
        self.max_concurrent_positions = 1
        self.max_exposure = 0.2

    def _log(self, message: str, level: str = "info") -> None:
        pass


def test_limit_entries_per_day():
    entries = pd.DataFrame(
        {
            "A": [True, True],
            "B": [True, False],
            "C": [True, True],
        },
        index=pd.date_range("2023-01-01", periods=2),
    )

    limited = BacktestExecutorMixin._limit_entries_per_day(entries, 1)

    assert limited.iloc[0].sum() == 1
    assert limited.iloc[1].sum() == 1


def test_max_exposure_passed(monkeypatch):
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
        captured["max_size"] = kwargs.get("max_size")
        return "portfolio"

    monkeypatch.setattr(
        "vectorbt.Portfolio.from_signals", _fake_from_signals, raising=False
    )

    result = dummy._create_individual_portfolios(
        data_dict, entries_dict, exits_dict, pyramid_enabled=False
    )

    assert result == "portfolio"
    assert captured["max_size"] == 0.2
