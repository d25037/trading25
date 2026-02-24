"""
統合ブレイクアウトシグナルのユニットテスト
"""

import pandas as pd

from src.domains.strategy.signals.breakout import ma_breakout_signal, period_breakout_signal


class TestPeriodBreakoutSignal:
    """period_breakout_signal関数のテスト"""

    def test_high_break_short_1(self):
        """今日の高値が20日最高値をブレイク"""
        high = pd.Series([100, 105, 110, 115, 120, 125, 130, 135, 140, 145])
        signal = period_breakout_signal(
            high, period=5, direction="high", condition="break", lookback_days=1
        )
        # lookback_days=1(今日の高値) >= period=5(5日最高値)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(high)

    def test_high_maintained(self):
        """高値維持（lookback < period）"""
        high = pd.Series([100, 95, 90, 85, 80, 75, 70, 65, 60, 55])
        signal = period_breakout_signal(
            high,
            period=5,
            direction="high",
            condition="maintained",
            lookback_days=2,
        )
        # lookback_days=2(2日最高値) < period=5(5日最高値)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_low_break(self):
        """安値ブレイク（lookback <= period）"""
        low = pd.Series([100, 95, 90, 85, 80, 75, 70, 65, 60, 55])
        signal = period_breakout_signal(
            low, period=5, direction="low", condition="break", lookback_days=1
        )
        # lookback_days=1(今日の安値) <= period=5(5日最安値)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_low_maintained(self):
        """安値維持（lookback > period）"""
        low = pd.Series([50, 55, 60, 65, 70, 75, 80, 85, 90, 95])
        signal = period_breakout_signal(
            low,
            period=5,
            direction="low",
            condition="maintained",
            lookback_days=2,
        )
        # lookback_days=2(2日最安値) > period=5(5日最安値)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_horizontal_high_water_平線高値一致(self):
        """水平線高値一致（range_break_v5ユースケース）"""
        high = pd.Series([100, 100, 100, 100, 100, 100, 100, 100, 100, 100])
        signal = period_breakout_signal(
            high, period=10, direction="high", condition="break", lookback_days=3
        )
        # lookback_days=3(3日最高値) >= period=10(10日最高値)
        # 同じ価格なので全てTrue（NaN除く）
        assert signal[9]  # 最後の行は確実にTrue

    def test_lookback_days_1(self):
        """lookback_days=1（今日の価格のみ）"""
        high = pd.Series([100, 110, 105, 115, 105, 120, 105, 125, 105, 130])
        signal = period_breakout_signal(
            high,
            period=3,
            direction="high",
            condition="break",
            lookback_days=1,
        )
        # その日のみの判定
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_lookback_days_5(self):
        """lookback_days=5（直近5日の最高値）"""
        high = pd.Series(
            [100, 110, 105, 105, 105, 105, 105, 105, 105, 105]
        )  # 2日目でブレイク
        signal = period_breakout_signal(
            high,
            period=3,
            direction="high",
            condition="break",
            lookback_days=5,
        )
        # 直近5日の最高値でブレイク判定
        # period=3なので最初の数行はNaN
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 後半ではTrueが継続する可能性がある
        assert signal.sum() >= 1  # 少なくとも1つはTrueがあるはず


class TestMABreakoutSignal:
    """ma_breakout_signal関数のテスト"""

    def test_sma_above(self):
        """SMA上抜け"""
        close = pd.Series([100, 105, 110, 115, 120, 125, 130, 135, 140, 145])
        signal = ma_breakout_signal(
            close, period=3, ma_type="sma", direction="above", lookback_days=1
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(close)

    def test_sma_below(self):
        """SMA下抜け"""
        close = pd.Series([145, 140, 135, 130, 125, 120, 115, 110, 105, 100])
        signal = ma_breakout_signal(
            close, period=3, ma_type="sma", direction="below", lookback_days=1
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_ema_above(self):
        """EMA上抜け"""
        close = pd.Series([100, 105, 110, 115, 120, 125, 130, 135, 140, 145])
        signal = ma_breakout_signal(
            close, period=3, ma_type="ema", direction="above", lookback_days=1
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_lookback_days_3(self):
        """lookback_days=3（直近3日以内にイベント発生）"""
        close = pd.Series([100, 105, 110, 115, 105, 105, 105, 105, 105, 105])
        signal = ma_breakout_signal(
            close, period=3, ma_type="sma", direction="above", lookback_days=3
        )
        # 直近3日以内に上抜けがあればTrue
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool


class TestHorizontalPriceActionReplacement:
    """horizontal_price_action.py の4関数置き換えテスト"""

    def test_horizontal_resistance_break_replacement(self):
        """horizontal_resistance_break_signal置き換え"""
        high = pd.Series([100, 105, 110, 115, 120, 125, 130, 135, 140, 145])
        signal = period_breakout_signal(
            high, period=120, direction="high", condition="break", lookback_days=20
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_horizontal_support_break_replacement(self):
        """horizontal_support_break_signal置き換え"""
        low = pd.Series([100, 95, 90, 85, 80, 75, 70, 65, 60, 55])
        signal = period_breakout_signal(
            low, period=120, direction="low", condition="break", lookback_days=20
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_horizontal_support_maintained_replacement(self):
        """horizontal_support_maintained_signal置き換え"""
        low = pd.Series([50, 55, 60, 65, 70, 75, 80, 85, 90, 95])
        signal = period_breakout_signal(
            low,
            period=120,
            direction="low",
            condition="maintained",
            lookback_days=20,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_horizontal_resistance_maintained_replacement(self):
        """horizontal_resistance_maintained_signal置き換え（未実装だが対応）"""
        high = pd.Series([100, 95, 90, 85, 80, 75, 70, 65, 60, 55])
        signal = period_breakout_signal(
            high,
            period=120,
            direction="high",
            condition="maintained",
            lookback_days=20,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
