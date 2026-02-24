"""performance.py シグナルのテスト"""

import numpy as np
import pandas as pd

from src.domains.strategy.signals.performance import (
    multi_timeframe_relative_performance_signal,
    relative_performance_signal,
)


def _make_prices(values: list[float], start: str = "2024-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=len(values), freq="B")
    return pd.Series(values, index=idx)


class TestRelativePerformanceSignal:
    def test_basic_outperformance(self) -> None:
        # stock doubles, benchmark flat => outperformance
        stock = _make_prices([100.0] + [200.0] * 70)
        bench = _make_prices([1000.0] * 71)
        result = relative_performance_signal(stock, bench, lookback_days=5, performance_multiplier=1.5)
        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_empty_price_data(self) -> None:
        stock = pd.Series([], dtype=float, index=pd.DatetimeIndex([]))
        bench = _make_prices([100.0] * 10)
        result = relative_performance_signal(stock, bench)
        assert len(result) == 0

    def test_empty_benchmark_data(self) -> None:
        stock = _make_prices([100.0] * 10)
        bench = pd.Series([], dtype=float, index=pd.DatetimeIndex([]))
        result = relative_performance_signal(stock, bench)
        assert result.all() == False  # noqa: E712

    def test_non_datetime_index(self) -> None:
        stock = pd.Series([100.0, 110.0, 120.0], index=[0, 1, 2])
        bench = _make_prices([100.0, 105.0, 110.0])
        result = relative_performance_signal(stock, bench)
        assert (result == False).all()  # noqa: E712

    def test_benchmark_non_datetime_index(self) -> None:
        stock = _make_prices([100.0, 110.0, 120.0])
        bench = pd.Series([100.0, 105.0, 110.0], index=[0, 1, 2])
        result = relative_performance_signal(stock, bench)
        assert (result == False).all()  # noqa: E712

    def test_insufficient_common_dates(self) -> None:
        stock = _make_prices([100.0, 110.0])
        bench = _make_prices([1000.0, 1010.0])
        result = relative_performance_signal(stock, bench, lookback_days=60)
        assert (result == False).all()  # noqa: E712


class TestMultiTimeframeRelativePerformanceSignal:
    def _make_long_data(self, n: int = 120) -> tuple[pd.Series, pd.Series]:
        idx = pd.date_range("2024-01-01", periods=n, freq="B")
        stock = pd.Series(np.linspace(100, 200, n), index=idx)
        bench = pd.Series(np.linspace(1000, 1050, n), index=idx)
        return stock, bench

    def test_any_timeframe(self) -> None:
        stock, bench = self._make_long_data()
        result = multi_timeframe_relative_performance_signal(
            stock, bench, timeframes=[10, 20], require_all_timeframes=False,
        )
        assert isinstance(result, pd.Series)

    def test_all_timeframes(self) -> None:
        stock, bench = self._make_long_data()
        result = multi_timeframe_relative_performance_signal(
            stock, bench, timeframes=[10, 20], require_all_timeframes=True,
        )
        assert isinstance(result, pd.Series)
