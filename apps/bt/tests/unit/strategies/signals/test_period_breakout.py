"""
期間ブレイクアウトシグナル ユニットテスト
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from src.domains.strategy.signals.breakout import (
    period_breakout_signal,
    period_extrema_break_signal,
    period_extrema_position_signal,
)


class TestPeriodBreakoutSignal:
    """period_breakout_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=100)
        # 上昇トレンドの価格データ
        self.price_up = pd.Series(
            np.linspace(100, 150, 100) + np.random.randn(100) * 2, index=self.dates
        )
        # 下降トレンドの価格データ
        self.price_down = pd.Series(
            np.linspace(150, 100, 100) + np.random.randn(100) * 2, index=self.dates
        )

    def test_high_break_basic(self):
        """高値ブレイク基本テスト"""
        signal = period_breakout_signal(
            price=self.price_up,
            period=20,
            direction="high",
            condition="break",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price_up)

    def test_low_break_basic(self):
        """安値ブレイク基本テスト"""
        signal = period_breakout_signal(
            price=self.price_down,
            period=20,
            direction="low",
            condition="break",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_maintained_condition(self):
        """維持条件テスト"""
        period_breakout_signal(
            price=self.price_up,
            period=20,
            direction="high",
            condition="break",
            lookback_days=1,
        )
        signal_maintained = period_breakout_signal(
            price=self.price_up,
            period=20,
            direction="high",
            condition="maintained",
            lookback_days=1,
        )
        # break と maintained は相互排他ではない（maintainedはbreakの継続）
        assert isinstance(signal_maintained, pd.Series)

    def test_lookback_days_effect(self):
        """lookback_days効果テスト"""
        signal_1 = period_breakout_signal(
            price=self.price_up,
            period=20,
            direction="high",
            condition="break",
            lookback_days=1,
        )
        signal_5 = period_breakout_signal(
            price=self.price_up,
            period=20,
            direction="high",
            condition="break",
            lookback_days=5,
        )
        # lookback_daysが長いほど、より多くのシグナルが出る可能性
        assert signal_5.sum() >= signal_1.sum()

    def test_period_effect(self):
        """期間効果テスト"""
        signal_short = period_breakout_signal(
            price=self.price_up,
            period=10,
            direction="high",
            condition="break",
            lookback_days=1,
        )
        signal_long = period_breakout_signal(
            price=self.price_up,
            period=50,
            direction="high",
            condition="break",
            lookback_days=1,
        )
        # 短い期間の方がブレイクが起きやすい
        assert signal_short.sum() >= signal_long.sum()

    def test_nan_handling(self):
        """NaN処理テスト"""
        price_with_nan = self.price_up.copy()
        price_with_nan.iloc[10:15] = np.nan
        signal = period_breakout_signal(
            price=price_with_nan,
            period=20,
            direction="high",
            condition="break",
        )
        assert isinstance(signal, pd.Series)
        # NaN期間はFalseになる
        assert not signal.iloc[10:15].any()

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = period_breakout_signal(
            price=empty,
            period=20,
            direction="high",
            condition="break",
        )
        assert len(signal) == 0

    def test_direction_low(self):
        """direction=lowテスト"""
        signal = period_breakout_signal(
            price=self.price_down,
            period=20,
            direction="low",
            condition="break",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_condition_maintained(self):
        """condition=maintainedテスト"""
        signal = period_breakout_signal(
            price=self.price_up,
            period=20,
            direction="high",
            condition="maintained",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_period_extrema_break_avoids_fillna_downcasting_warning(self):
        """object dtype + fillna(False) の FutureWarning を出さない"""
        price = pd.Series(
            [100.0, 101.0, 103.0, 102.0, 105.0],
            index=pd.date_range("2024-01-01", periods=5),
        )

        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            signal = period_extrema_break_signal(
                price=price,
                period=2,
                direction="high",
                lookback_days=1,
            )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_period_extrema_position_supports_direct_states(self):
        signal = period_extrema_position_signal(
            price=self.price_up,
            period=10,
            direction="high",
            state="at_extrema",
            lookback_days=3,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_period_extrema_break_rejects_invalid_direction(self):
        with pytest.raises(ValueError, match="不正なdirection"):
            period_extrema_break_signal(
                price=self.price_up,
                period=10,
                direction="sideways",
            )

    def test_period_extrema_position_rejects_invalid_state(self):
        with pytest.raises(ValueError, match="不正なstate"):
            period_extrema_position_signal(
                price=self.price_up,
                period=10,
                direction="high",
                state="invalid",
            )

    def test_period_breakout_rejects_invalid_condition(self):
        with pytest.raises(ValueError, match="不正なcondition"):
            period_breakout_signal(
                price=self.price_up,
                period=10,
                direction="high",
                condition="invalid",
            )
