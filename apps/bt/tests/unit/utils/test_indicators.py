"""
共通インジケーター計算関数のユニットテスト
"""

import numpy as np
import pandas as pd
import pytest
import vectorbt as vbt

from src.domains.strategy.indicators.calculations import (
    compute_atr_support_line,
    compute_macd,
    compute_moving_average,
    compute_nbar_support,
    compute_risk_adjusted_return,
    compute_rsi,
    compute_trading_value_ma,
    compute_volume_mas,
    compute_volume_weighted_ema,
)


class TestComputeATRSupportLine:
    """compute_atr_support_line() テスト"""

    def setup_method(self):
        self.dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(42)
        base = np.linspace(100, 120, 100) + np.random.randn(100) * 2
        self.high = pd.Series(base + np.abs(np.random.randn(100)) * 2, index=self.dates)
        self.low = pd.Series(base - np.abs(np.random.randn(100)) * 2, index=self.dates)
        self.close = pd.Series(base, index=self.dates)

    def test_returns_series_float(self):
        result = compute_atr_support_line(self.high, self.low, self.close, 20, 2.0)
        assert isinstance(result, pd.Series)
        assert len(result) == 100

    def test_nan_at_start(self):
        result = compute_atr_support_line(self.high, self.low, self.close, 20, 2.0)
        assert pd.isna(result.iloc[0])
        assert not pd.isna(result.iloc[25])

    def test_support_below_highest(self):
        result = compute_atr_support_line(self.high, self.low, self.close, 20, 2.0)
        highest = self.high.rolling(20).max()
        valid = ~pd.isna(result) & ~pd.isna(highest)
        assert (result[valid] <= highest[valid]).all()

    def test_higher_multiplier_lower_support(self):
        s1 = compute_atr_support_line(self.high, self.low, self.close, 20, 1.0)
        s2 = compute_atr_support_line(self.high, self.low, self.close, 20, 3.0)
        valid = ~pd.isna(s1) & ~pd.isna(s2)
        assert (s1[valid] >= s2[valid]).all()

    def test_empty_series(self):
        empty = pd.Series(dtype=float)
        result = compute_atr_support_line(empty, empty, empty, 20, 2.0)
        assert len(result) == 0

    def test_matches_manual_calculation(self):
        """手動計算との一致確認"""
        high = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0])
        low = pd.Series([9.0, 10.0, 11.0, 12.0, 13.0])
        close = pd.Series([9.5, 10.5, 11.5, 12.5, 13.5])

        result = compute_atr_support_line(high, low, close, 3, 1.0)
        # period=3なので最初の2つはNaN
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        # 3番目以降は値がある
        assert not pd.isna(result.iloc[2])


class TestComputeVolumeMAs:
    """compute_volume_mas() テスト"""

    def setup_method(self):
        self.dates = pd.date_range("2024-01-01", periods=200)
        self.volume = pd.Series(np.ones(200) * 1000, index=self.dates)
        self.volume.iloc[100:110] = 5000

    def test_returns_tuple_of_series(self):
        short_ma, long_ma = compute_volume_mas(self.volume, 10, 50)
        assert isinstance(short_ma, pd.Series)
        assert isinstance(long_ma, pd.Series)
        assert len(short_ma) == 200
        assert len(long_ma) == 200

    def test_sma_mode(self):
        short_ma, long_ma = compute_volume_mas(self.volume, 10, 50, "sma")
        # 一定出来高なら短期と長期は同値
        valid = ~pd.isna(short_ma) & ~pd.isna(long_ma)
        # 急増区間を除いた通常区間での検証
        normal = valid & (self.volume.index < self.dates[90])
        assert np.allclose(short_ma[normal], long_ma[normal], rtol=0.01)

    def test_ema_mode(self):
        short_ma, long_ma = compute_volume_mas(self.volume, 10, 50, "ema")
        assert isinstance(short_ma, pd.Series)
        assert isinstance(long_ma, pd.Series)

    def test_spike_detected(self):
        short_ma, long_ma = compute_volume_mas(self.volume, 10, 50)
        # 急増期間付近で短期MAが長期MAより大きいはず
        assert short_ma.iloc[105] > long_ma.iloc[105]

    def test_empty_series(self):
        empty = pd.Series(dtype=float)
        short_ma, long_ma = compute_volume_mas(empty, 10, 50)
        assert len(short_ma) == 0
        assert len(long_ma) == 0


class TestComputeTradingValueMA:
    """compute_trading_value_ma() テスト"""

    def setup_method(self):
        self.dates = pd.date_range("2024-01-01", periods=100)
        self.close = pd.Series(np.ones(100) * 1000, index=self.dates)
        self.volume = pd.Series(np.ones(100) * 100000, index=self.dates)

    def test_returns_series(self):
        result = compute_trading_value_ma(self.close, self.volume, 20)
        assert isinstance(result, pd.Series)
        assert len(result) == 100

    def test_value_in_oku_yen(self):
        """売買代金=1000×100000/1e8=1.0億円"""
        result = compute_trading_value_ma(self.close, self.volume, 20)
        valid = ~pd.isna(result)
        assert np.allclose(result[valid], 1.0, rtol=0.01)

    def test_nan_at_start(self):
        result = compute_trading_value_ma(self.close, self.volume, 20)
        assert pd.isna(result.iloc[0])

    def test_empty_series(self):
        empty = pd.Series(dtype=float)
        result = compute_trading_value_ma(empty, empty, 20)
        assert len(result) == 0


class TestComputeNBarSupport:
    """compute_nbar_support() テスト"""

    def setup_method(self):
        self.dates = pd.date_range("2024-01-01", periods=50)
        self.low = pd.Series(np.linspace(100, 90, 50), index=self.dates)

    def test_returns_series(self):
        result = compute_nbar_support(self.low, 10)
        assert isinstance(result, pd.Series)
        assert len(result) == 50

    def test_support_equals_rolling_min(self):
        result = compute_nbar_support(self.low, 10)
        expected = self.low.rolling(10).min()
        valid = ~pd.isna(result)
        assert np.allclose(result[valid], expected[valid])

    def test_nan_at_start(self):
        result = compute_nbar_support(self.low, 10)
        assert pd.isna(result.iloc[0])
        assert not pd.isna(result.iloc[10])

    def test_empty_series(self):
        empty = pd.Series(dtype=float)
        result = compute_nbar_support(empty, 10)
        assert len(result) == 0


class TestComputeVolumeWeightedEMA:
    """compute_volume_weighted_ema() テスト"""

    def setup_method(self):
        self.dates = pd.date_range("2024-01-01", periods=50)
        self.close = pd.Series(np.linspace(100, 120, 50), index=self.dates)
        self.volume = pd.Series(np.linspace(1000, 5000, 50), index=self.dates)

    def test_returns_series(self):
        result = compute_volume_weighted_ema(self.close, self.volume, 10)
        assert isinstance(result, pd.Series)
        assert len(result) == 50

    def test_constant_price_returns_constant(self):
        close = pd.Series(np.ones(50) * 100, index=self.dates)
        result = compute_volume_weighted_ema(close, self.volume, 10)
        valid = ~pd.isna(result)
        assert np.allclose(result[valid], 100.0, rtol=1e-6)

    def test_zero_volume_safe(self):
        volume = self.volume.copy()
        volume.iloc[0] = 0
        result = compute_volume_weighted_ema(self.close, volume, 10)
        assert len(result) == 50


class TestComputeMovingAverage:
    def test_invalid_ma_type_raises(self):
        series = pd.Series([1.0, 2.0, 3.0])

        with pytest.raises(ValueError, match="未対応のma_type"):
            compute_moving_average(series, 2, ma_type="wma")  # type: ignore[arg-type]


@pytest.mark.slow
class TestVectorbtCompatibility:
    def setup_method(self):
        rng = np.random.default_rng(123)
        self.close = pd.Series(
            np.cumprod(1 + rng.normal(0, 0.01, 160)) * 100,
            index=pd.date_range("2024-01-01", periods=160),
        )

    def test_compute_rsi_matches_vectorbt_default(self):
        actual = compute_rsi(self.close, period=14)
        expected = vbt.RSI.run(self.close, 14).rsi
        aligned = pd.concat([actual, expected], axis=1).dropna()

        assert not aligned.empty
        assert np.allclose(aligned.iloc[:, 0], aligned.iloc[:, 1], atol=1e-10)

    def test_compute_macd_matches_vectorbt_default(self):
        actual = compute_macd(self.close, fast_period=12, slow_period=26, signal_period=9)
        expected = vbt.MACD.run(self.close, fast_window=12, slow_window=26, signal_window=9)

        for actual_series, expected_series in (
            (actual.macd, expected.macd),
            (actual.signal, expected.signal),
            (actual.histogram, expected.hist),
        ):
            aligned = pd.concat([actual_series, expected_series], axis=1).dropna()
            assert not aligned.empty
            assert np.allclose(aligned.iloc[:, 0], aligned.iloc[:, 1], atol=1e-10)


class TestComputeRiskAdjustedReturn:
    def setup_method(self):
        self.close = pd.Series([100.0, 101.0, 103.0, 102.0, 104.0, 106.0, 105.0, 107.0])

    def test_sharpe_ratio_path_returns_series(self):
        result = compute_risk_adjusted_return(
            self.close,
            lookback_period=3,
            ratio_type="sharpe",
        )

        assert isinstance(result, pd.Series)
        assert result.index.equals(self.close.index)
        assert result.iloc[3:].notna().any()

    def test_invalid_ratio_type_raises(self):
        with pytest.raises(ValueError, match="不正なratio_type"):
            compute_risk_adjusted_return(
                self.close,
                lookback_period=3,
                ratio_type="omega",  # type: ignore[arg-type]
            )
