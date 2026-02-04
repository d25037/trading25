"""
ATRサポートブレイクシグナル ユニットテスト
"""

import numpy as np
import pandas as pd

from src.strategies.signals.breakout import atr_support_break_signal


class TestATRSupportBreakSignal:
    """atr_support_break_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(42)

        # 基本価格データ
        base_price = np.linspace(100, 120, 100) + np.random.randn(100) * 2
        self.high = pd.Series(base_price + np.abs(np.random.randn(100)) * 2, index=self.dates)
        self.low = pd.Series(base_price - np.abs(np.random.randn(100)) * 2, index=self.dates)
        self.close = pd.Series(base_price, index=self.dates)

    def test_break_direction_basic(self):
        """サポートブレイク基本テスト"""
        signal = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=20,
            atr_multiplier=2.0,
            direction="break",
            price_column="close",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)

    def test_recovery_direction_basic(self):
        """サポート回復基本テスト"""
        signal = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=20,
            atr_multiplier=2.0,
            direction="recovery",
            price_column="close",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_atr_multiplier_effect(self):
        """ATR倍率効果テスト"""
        signal_low = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=20,
            atr_multiplier=1.0,
            direction="break",
        )
        signal_high = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=20,
            atr_multiplier=3.0,
            direction="break",
        )
        # 高い倍率 = 広いバンド = ブレイクが少ない
        assert signal_low.sum() >= signal_high.sum()

    def test_lookback_period_effect(self):
        """lookback期間効果テスト"""
        signal_short = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=10,
            atr_multiplier=2.0,
            direction="break",
        )
        signal_long = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=30,
            atr_multiplier=2.0,
            direction="break",
        )
        # 期間の違いでシグナルパターンが異なる
        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)

    def test_price_column_low(self):
        """low価格での判定テスト"""
        signal = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=self.close,
            lookback_period=20,
            atr_multiplier=2.0,
            direction="break",
            price_column="low",
        )
        assert isinstance(signal, pd.Series)

    def test_nan_handling(self):
        """NaN処理テスト"""
        close_nan = self.close.copy()
        close_nan.iloc[10:15] = np.nan
        signal = atr_support_break_signal(
            high=self.high,
            low=self.low,
            close=close_nan,
            lookback_period=20,
            atr_multiplier=2.0,
            direction="break",
        )
        assert isinstance(signal, pd.Series)

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = atr_support_break_signal(
            high=empty,
            low=empty,
            close=empty,
            lookback_period=20,
            atr_multiplier=2.0,
            direction="break",
        )
        assert len(signal) == 0

    def test_short_series(self):
        """短いSeries処理テスト"""
        dates = pd.date_range("2024-01-01", periods=10)
        high = pd.Series(np.linspace(100, 110, 10), index=dates)
        low = pd.Series(np.linspace(95, 105, 10), index=dates)
        close = pd.Series(np.linspace(98, 108, 10), index=dates)
        signal = atr_support_break_signal(
            high=high,
            low=low,
            close=close,
            lookback_period=20,
            atr_multiplier=2.0,
            direction="break",
        )
        assert isinstance(signal, pd.Series)
        # lookback > データ長なので全てFalse
        assert signal.sum() == 0
