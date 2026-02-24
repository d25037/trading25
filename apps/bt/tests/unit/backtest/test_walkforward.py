"""
Walk-forward tests
"""

import pandas as pd
import pytest

from src.domains.backtest.core.runner import BacktestRunner
from src.domains.backtest.core.walkforward import generate_walkforward_splits


def test_negative_step_raises():
    dates = pd.date_range("2023-01-01", periods=10, freq="D")
    with pytest.raises(ValueError, match="正の値"):
        generate_walkforward_splits(dates, train_window=4, test_window=2, step=-3)


def test_generate_walkforward_splits():
    dates = pd.date_range("2023-01-01", periods=10, freq="D")
    splits = generate_walkforward_splits(dates, train_window=4, test_window=2, step=2)

    assert len(splits) == 3
    assert splits[0].train_start == "2023-01-01"
    assert splits[0].train_end == "2023-01-04"
    assert splits[0].test_start == "2023-01-05"
    assert splits[0].test_end == "2023-01-06"


def test_run_walk_forward_collects_metrics(monkeypatch):
    runner = BacktestRunner()

    dates = pd.date_range("2023-01-01", periods=10, freq="D")
    fake_df = pd.DataFrame({"Close": range(10), "Volume": range(10)}, index=dates)

    def _fake_load_stock_data(*args, **kwargs):
        return fake_df

    class _FakePortfolio:
        def total_return(self):
            return 0.1

        def sharpe_ratio(self):
            return 1.0

        def calmar_ratio(self):
            return 0.5

    def _fake_execute_strategy_with_config(*args, **kwargs):
        return {"kelly_portfolio": _FakePortfolio()}

    monkeypatch.setattr(
        "src.infrastructure.data_access.loaders.stock_loaders.load_stock_data", _fake_load_stock_data
    )
    monkeypatch.setattr(
        "src.domains.strategy.core.factory.StrategyFactory.execute_strategy_with_config",
        _fake_execute_strategy_with_config,
    )

    parameters = {
        "shared_config": {
            "dataset": "sample",
            "stock_codes": ["TEST"],
            "start_date": "",
            "end_date": "",
            "timeframe": "daily",
            "walk_forward": {
                "enabled": True,
                "train_window": 4,
                "test_window": 2,
                "step": 2,
                "max_splits": 2,
            },
        },
        "entry_filter_params": {},
        "exit_trigger_params": {},
    }

    result = runner._run_walk_forward(parameters)

    assert result is not None
    assert result["count"] == 2
    assert result["aggregate"]["total_return"] == 0.1
    assert result["aggregate"]["sharpe_ratio"] == 1.0
    assert result["aggregate"]["calmar_ratio"] == 0.5
