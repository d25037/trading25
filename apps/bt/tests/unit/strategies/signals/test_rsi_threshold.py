"""
RSI閾値シグナル ユニットテスト
"""

import numpy as np
import pandas as pd
import pytest

from src.domains.strategy.signals.rsi_threshold import rsi_threshold_signal


class TestRSIThresholdSignal:
    """rsi_threshold_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=100)
        # 上昇トレンド（RSI高め）
        self.price_up = pd.Series(
            100 * np.cumprod(1 + np.random.normal(0.005, 0.01, 100)), index=self.dates
        )
        # 下降トレンド（RSI低め）
        self.price_down = pd.Series(
            100 * np.cumprod(1 + np.random.normal(-0.005, 0.01, 100)), index=self.dates
        )
        # フラット
        self.price_flat = pd.Series(
            100 + np.random.randn(100) * 0.5, index=self.dates
        )

    def test_below_condition_basic(self):
        """below条件基本テスト（売られすぎ判定）"""
        signal = rsi_threshold_signal(
            close=self.price_down,
            period=14,
            threshold=30,
            condition="below",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price_down)

    def test_above_condition_basic(self):
        """above条件基本テスト（買われすぎ判定）"""
        signal = rsi_threshold_signal(
            close=self.price_up,
            period=14,
            threshold=70,
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_threshold_sensitivity(self):
        """閾値感度テスト"""
        signal_low = rsi_threshold_signal(
            close=self.price_down,
            period=14,
            threshold=20,
            condition="below",
        )
        signal_high = rsi_threshold_signal(
            close=self.price_down,
            period=14,
            threshold=40,
            condition="below",
        )
        # 高い閾値の方が多くのシグナルが出る
        assert signal_high.sum() >= signal_low.sum()

    def test_period_effect(self):
        """期間効果テスト"""
        signal_short = rsi_threshold_signal(
            close=self.price_down,
            period=7,
            threshold=30,
            condition="below",
        )
        signal_long = rsi_threshold_signal(
            close=self.price_down,
            period=21,
            threshold=30,
            condition="below",
        )
        # 短い期間はより敏感（極端な値が出やすい）
        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)

    def test_nan_handling(self):
        """NaN処理テスト"""
        price_with_nan = self.price_down.copy()
        price_with_nan.iloc[10:15] = np.nan
        signal = rsi_threshold_signal(
            close=price_with_nan,
            period=14,
            threshold=30,
            condition="below",
        )
        assert isinstance(signal, pd.Series)
        # NaN期間はFalseになる
        assert not signal.iloc[10:20].all()  # NaN周辺はFalse

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = rsi_threshold_signal(
            close=empty,
            period=14,
            threshold=30,
            condition="below",
        )
        assert len(signal) == 0

    def test_invalid_condition(self):
        """不正なconditionテスト"""
        with pytest.raises(ValueError):
            rsi_threshold_signal(
                close=self.price_down,
                period=14,
                threshold=30,
                condition="invalid",
            )

    def test_flat_price_rsi(self):
        """フラット価格のRSI（約50付近）"""
        signal_below = rsi_threshold_signal(
            close=self.price_flat,
            period=14,
            threshold=40,
            condition="below",
        )
        signal_above = rsi_threshold_signal(
            close=self.price_flat,
            period=14,
            threshold=60,
            condition="above",
        )
        # フラット価格はRSI約50なので、どちらも少ない
        assert isinstance(signal_below, pd.Series)
        assert isinstance(signal_above, pd.Series)
