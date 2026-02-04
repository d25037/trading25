"""
β値シグナル ユニットテスト
"""

import numpy as np
import pandas as pd

from src.strategies.signals.beta import (
    beta_range_signal,
    beta_range_signal_with_value,
    calculate_beta,
    dynamic_beta_signal,
    numba_rolling_beta,
    pandas_rolling_beta,
    rolling_beta_calculation,
)


class TestCalculateBeta:
    """calculate_beta() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        np.random.seed(42)
        # 市場リターン
        self.market_returns = pd.Series(np.random.normal(0.001, 0.02, 100))
        # β=1.2の銘柄リターン
        self.stock_returns = pd.Series(
            1.2 * self.market_returns.values + np.random.normal(0, 0.01, 100)
        )

    def test_basic_beta_calculation(self):
        """β値基本計算テスト"""
        beta = calculate_beta(self.stock_returns, self.market_returns)
        assert isinstance(beta, float)
        assert not np.isnan(beta)
        # β ≈ 1.2 のはず（ノイズあり）
        assert 0.8 < beta < 1.6

    def test_insufficient_data(self):
        """データ不足時はNaN"""
        short_stock = pd.Series([0.01])
        short_market = pd.Series([0.01])
        beta = calculate_beta(short_stock, short_market)
        assert np.isnan(beta)

    def test_zero_variance_market(self):
        """市場分散がゼロの場合はNaN"""
        stock = pd.Series([0.01, 0.02, 0.03])
        market = pd.Series([0.01, 0.01, 0.01])  # 変動なし
        beta = calculate_beta(stock, market)
        assert np.isnan(beta)


class TestRollingBetaCalculation:
    """rolling_beta_calculation() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(42)
        # 市場価格
        market_returns = np.random.normal(0.001, 0.02, 100)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=self.dates
        )
        # 銘柄価格
        stock_returns = 1.2 * market_returns + np.random.normal(0, 0.01, 100)
        self.stock_price = pd.Series(
            100 * np.cumprod(1 + stock_returns), index=self.dates
        )

    def test_rolling_beta_basic(self):
        """ローリングβ値基本テスト"""
        rolling_beta = rolling_beta_calculation(
            self.stock_price, self.market_price, window=30, fast=True
        )
        assert isinstance(rolling_beta, pd.Series)
        assert len(rolling_beta) == len(self.stock_price)
        # 初期期間はNaN
        assert rolling_beta.iloc[:29].isna().all()
        # その後は値あり
        assert rolling_beta.iloc[29:].notna().any()

    def test_rolling_beta_slow(self):
        """ローリングβ値（従来実装）テスト"""
        rolling_beta = rolling_beta_calculation(
            self.stock_price, self.market_price, window=30, fast=False
        )
        assert isinstance(rolling_beta, pd.Series)
        assert len(rolling_beta) == len(self.stock_price)


class TestBetaRangeSignal:
    """beta_range_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=250)
        np.random.seed(42)
        # 市場価格
        market_returns = np.random.normal(0.001, 0.02, 250)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=self.dates
        )
        # 高β銘柄（β≈1.2）
        high_beta_returns = 1.2 * market_returns + np.random.normal(0, 0.01, 250)
        self.high_beta_price = pd.Series(
            100 * np.cumprod(1 + high_beta_returns), index=self.dates
        )
        # 低β銘柄（β≈0.5）
        low_beta_returns = 0.5 * market_returns + np.random.normal(0, 0.01, 250)
        self.low_beta_price = pd.Series(
            100 * np.cumprod(1 + low_beta_returns), index=self.dates
        )

    def test_beta_range_signal_basic(self):
        """β値範囲シグナル基本テスト"""
        signal = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.high_beta_price)

    def test_beta_range_signal_narrow(self):
        """狭いβ範囲テスト"""
        # 高β銘柄は0.9-1.1範囲を通過しにくい
        signal = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.9,
            beta_max=1.1,
            lookback_period=50,
        )
        assert isinstance(signal, pd.Series)
        # フィルター効果で少ない
        pass_rate = signal.sum() / len(signal)
        assert pass_rate < 0.5

    def test_beta_range_signal_wide(self):
        """広いβ範囲テスト"""
        signal = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.0,
            beta_max=3.0,
            lookback_period=50,
        )
        assert isinstance(signal, pd.Series)
        # 広い範囲では多く通過（NaN期間を除く）
        valid_signal = signal.iloc[50:]
        pass_rate = valid_signal.sum() / len(valid_signal)
        assert pass_rate > 0.8

    def test_lookback_period_effect(self):
        """lookback期間効果テスト"""
        signal_short = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=30,
        )
        signal_long = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=100,
        )
        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)
        # 短いlookbackの方が早くシグナルが出る（NaN期間が短い）
        assert signal_short.notna().sum() >= signal_long.notna().sum()

    def test_method_pandas(self):
        """pandas計算方法テスト"""
        signal = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
            fast=True,
            method="pandas",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_method_numba(self):
        """numba計算方法テスト"""
        signal = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
            fast=True,
            method="numba",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_fast_false(self):
        """従来実装テスト"""
        signal = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
            fast=False,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool


class TestDynamicBetaSignal:
    """dynamic_beta_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=250)
        np.random.seed(42)
        market_returns = np.random.normal(0.001, 0.02, 250)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=self.dates
        )
        stock_returns = 1.0 * market_returns + np.random.normal(0, 0.01, 250)
        self.stock_price = pd.Series(
            100 * np.cumprod(1 + stock_returns), index=self.dates
        )

    def test_dynamic_beta_basic(self):
        """動的β値シグナル基本テスト"""
        signal = dynamic_beta_signal(
            self.stock_price,
            self.market_price,
            target_beta=1.0,
            tolerance=0.3,
            lookback_period=50,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.stock_price)

    def test_tolerance_effect(self):
        """tolerance効果テスト"""
        signal_strict = dynamic_beta_signal(
            self.stock_price,
            self.market_price,
            target_beta=1.0,
            tolerance=0.1,
            lookback_period=50,
        )
        signal_loose = dynamic_beta_signal(
            self.stock_price,
            self.market_price,
            target_beta=1.0,
            tolerance=0.5,
            lookback_period=50,
        )
        # 広いtoleranceの方がTrue数が多い
        assert signal_loose.sum() >= signal_strict.sum()


class TestPandasVsNumbaConsistency:
    """pandas/numba実装の一貫性テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=200)
        np.random.seed(42)
        market_returns = np.random.normal(0.001, 0.02, 200)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=self.dates
        )
        stock_returns = 1.2 * market_returns + np.random.normal(0, 0.01, 200)
        self.stock_price = pd.Series(
            100 * np.cumprod(1 + stock_returns), index=self.dates
        )

    def test_pandas_vs_numba_similar_results(self):
        """pandas/numbaで同様の結果"""
        pandas_beta = pandas_rolling_beta(
            self.stock_price, self.market_price, window=50
        )
        numba_beta = numba_rolling_beta(
            self.stock_price, self.market_price, window=50
        )

        # NaN以外の値で比較
        valid_idx = pandas_beta.notna() & numba_beta.notna()
        if valid_idx.any():
            correlation = pandas_beta[valid_idx].corr(numba_beta[valid_idx])
            # 高い相関を期待
            assert correlation > 0.9


class TestBetaSignalEdgeCases:
    """エッジケーステスト"""

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = beta_range_signal(
            empty, empty, beta_min=0.5, beta_max=1.5, lookback_period=20
        )
        assert len(signal) == 0

    def test_short_series(self):
        """短いSeries処理テスト"""
        dates = pd.date_range("2024-01-01", periods=10)
        stock = pd.Series(np.linspace(100, 110, 10), index=dates)
        market = pd.Series(np.linspace(1000, 1100, 10), index=dates)
        signal = beta_range_signal(
            stock, market, beta_min=0.5, beta_max=1.5, lookback_period=20
        )
        assert isinstance(signal, pd.Series)
        # lookback > データ長なので全てFalse
        assert signal.sum() == 0

    def test_nan_handling(self):
        """NaN処理テスト"""
        dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(42)
        stock = pd.Series(np.cumsum(np.random.randn(100)) + 100, index=dates)
        market = pd.Series(np.cumsum(np.random.randn(100)) + 1000, index=dates)
        stock.iloc[40:50] = np.nan

        signal = beta_range_signal(
            stock, market, beta_min=0.5, beta_max=1.5, lookback_period=20
        )
        assert isinstance(signal, pd.Series)
        # NaN期間周辺はFalseになる
        assert not signal.iloc[40:50].any()


class TestBetaRangeSignalWithValue:
    """beta_range_signal_with_value() テスト（二重計算排除版）"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=250)
        np.random.seed(42)
        # 市場価格
        market_returns = np.random.normal(0.001, 0.02, 250)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=self.dates
        )
        # 高β銘柄（β≈1.2）
        high_beta_returns = 1.2 * market_returns + np.random.normal(0, 0.01, 250)
        self.high_beta_price = pd.Series(
            100 * np.cumprod(1 + high_beta_returns), index=self.dates
        )

    def test_returns_signal_and_value(self):
        """シグナルとβ値を同時に返すテスト"""
        signal, beta_value = beta_range_signal_with_value(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.high_beta_price)
        # β値が返される
        assert beta_value is not None
        assert isinstance(beta_value, float)
        # β値は範囲内
        assert 0.8 < beta_value < 1.6  # 高β銘柄なので≈1.2

    def test_consistent_with_beta_range_signal(self):
        """beta_range_signalと同じシグナル結果を返すテスト"""
        signal_with_value, _ = beta_range_signal_with_value(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
            method="numba",
        )
        signal_original = beta_range_signal(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
            fast=True,
            method="numba",
        )
        # 結果が一致することを確認
        pd.testing.assert_series_equal(signal_with_value, signal_original)

    def test_beta_value_matches_numba_rolling_beta(self):
        """β値がnumba_rolling_betaと一致するテスト"""
        _, beta_value = beta_range_signal_with_value(
            self.high_beta_price,
            self.market_price,
            beta_min=0.5,
            beta_max=1.5,
            lookback_period=50,
            method="numba",
        )
        # 直接計算
        rolling_beta = numba_rolling_beta(
            self.high_beta_price, self.market_price, window=50
        )
        expected_beta = float(rolling_beta.dropna().iloc[-1])
        # 一致確認
        assert beta_value is not None
        assert abs(beta_value - expected_beta) < 1e-10

    def test_different_methods(self):
        """異なる計算方法で動作テスト"""
        for method in ["pandas", "numba"]:
            signal, beta_value = beta_range_signal_with_value(
                self.high_beta_price,
                self.market_price,
                beta_min=0.5,
                beta_max=1.5,
                lookback_period=50,
                method=method,
            )
            assert isinstance(signal, pd.Series)
            assert beta_value is not None
            # β値は妥当な範囲
            assert 0.5 < beta_value < 2.0
