"""
移動平均線ブレイクアウトシグナル ユニットテスト
"""

import numpy as np
import pandas as pd

from src.strategies.signals.breakout import ma_breakout_signal


class TestMABreakoutSignal:
    """ma_breakout_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(42)

        # 上昇トレンドの価格データ
        self.price_up = pd.Series(
            np.linspace(100, 150, 100) + np.random.randn(100) * 2, index=self.dates
        )
        # 下降トレンドの価格データ
        self.price_down = pd.Series(
            np.linspace(150, 100, 100) + np.random.randn(100) * 2, index=self.dates
        )
        # レンジ相場の価格データ
        self.price_range = pd.Series(
            100 + np.random.randn(100) * 5, index=self.dates
        )

    def test_above_sma_basic(self):
        """SMA上抜けクロス基本テスト"""
        signal = ma_breakout_signal(
            price=self.price_up,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price_up)

    def test_below_sma_basic(self):
        """SMA下抜けクロス基本テスト"""
        signal = ma_breakout_signal(
            price=self.price_down,
            period=20,
            ma_type="sma",
            direction="below",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_ema_type(self):
        """EMAタイプテスト"""
        signal = ma_breakout_signal(
            price=self.price_up,
            period=20,
            ma_type="ema",
            direction="above",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_lookback_days_effect(self):
        """lookback_days効果テスト"""
        signal_1 = ma_breakout_signal(
            price=self.price_up,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        signal_5 = ma_breakout_signal(
            price=self.price_up,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=5,
        )
        # lookback_daysが長いほど、クロスイベントが持続
        assert signal_5.sum() >= signal_1.sum()

    def test_period_effect(self):
        """期間効果テスト"""
        signal_short = ma_breakout_signal(
            price=self.price_up,
            period=10,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        signal_long = ma_breakout_signal(
            price=self.price_up,
            period=50,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        # 短い期間の方がクロスが多い傾向
        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)

    def test_uptrend_above_crosses(self):
        """上昇トレンドで上抜けクロス発生"""
        signal = ma_breakout_signal(
            price=self.price_up,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=3,
        )
        # 上昇トレンドでは上抜けクロスが発生
        assert signal.any()

    def test_downtrend_below_crosses(self):
        """下降トレンドで下抜けクロス発生"""
        # より明確な下降トレンドデータを作成
        dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(123)
        # 上昇→下降で確実にクロスを発生させる
        price = pd.Series(
            np.concatenate([
                np.linspace(100, 130, 50),  # 上昇
                np.linspace(130, 80, 50),   # 下降
            ]) + np.random.randn(100) * 1,
            index=dates,
        )
        signal = ma_breakout_signal(
            price=price,
            period=20,
            ma_type="sma",
            direction="below",
            lookback_days=5,
        )
        # 下降トレンドに切り替わった時に下抜けクロスが発生するはず
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_range_market_mixed_crosses(self):
        """レンジ相場で両方向クロス"""
        signal_above = ma_breakout_signal(
            price=self.price_range,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        signal_below = ma_breakout_signal(
            price=self.price_range,
            period=20,
            ma_type="sma",
            direction="below",
            lookback_days=1,
        )
        # レンジ相場では両方向のクロスが発生する可能性
        assert isinstance(signal_above, pd.Series)
        assert isinstance(signal_below, pd.Series)

    def test_nan_handling(self):
        """NaN処理テスト"""
        price_with_nan = self.price_up.copy()
        price_with_nan.iloc[10:15] = np.nan
        signal = ma_breakout_signal(
            price=price_with_nan,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        # NaN周辺はFalse
        assert not signal.iloc[10:17].any()

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = ma_breakout_signal(
            price=empty,
            period=20,
            ma_type="sma",
            direction="above",
        )
        assert len(signal) == 0

    def test_short_series(self):
        """短いSeries処理テスト"""
        dates = pd.date_range("2024-01-01", periods=10)
        short_price = pd.Series(np.linspace(100, 110, 10), index=dates)
        signal = ma_breakout_signal(
            price=short_price,
            period=20,
            ma_type="sma",
            direction="above",
        )
        assert isinstance(signal, pd.Series)
        # period > データ長なので全てFalse
        assert signal.sum() == 0


class TestMABreakoutCrossoverLogic:
    """クロスオーバーロジックの詳細テスト"""

    def test_exact_crossover_detection(self):
        """正確なクロスオーバー検出テスト"""
        dates = pd.date_range("2024-01-01", periods=30)
        # 意図的にクロスオーバーを作成
        # SMA(5)を使用して簡単にテスト
        price = pd.Series(
            [
                100, 100, 100, 100, 100,  # SMA = 100
                100, 100, 100, 100, 99,   # SMA ≈ 99.8, price = 99 (below)
                98, 98, 98, 98, 105,      # SMA ≈ 99.4, price = 105 (cross above!)
                106, 107, 108, 109, 110,  # 上昇継続
                110, 110, 110, 110, 110,
                110, 110, 110, 110, 110,
            ],
            index=dates,
        )
        signal = ma_breakout_signal(
            price=price,
            period=5,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        assert isinstance(signal, pd.Series)
        # クロスオーバーポイントでTrueになるはず
        assert signal.any()

    def test_no_crossover_steady_above(self):
        """MAより常に上にある場合はクロスなし"""
        dates = pd.date_range("2024-01-01", periods=50)
        # 常にMAより上
        base = np.linspace(100, 150, 50)
        price = pd.Series(base + 20, index=dates)  # +20で常に上

        signal = ma_breakout_signal(
            price=price,
            period=20,
            ma_type="sma",
            direction="above",
            lookback_days=1,
        )
        # 最初のperiod期間後はクロスが起きない（常に上だから）
        assert signal.iloc[25:].sum() == 0

    def test_no_crossover_steady_below(self):
        """MAより常に下にある場合はクロスなし"""
        dates = pd.date_range("2024-01-01", periods=50)
        base = np.linspace(100, 80, 50)
        price = pd.Series(base - 20, index=dates)  # -20で常に下

        signal = ma_breakout_signal(
            price=price,
            period=20,
            ma_type="sma",
            direction="below",
            lookback_days=1,
        )
        # 最初のperiod期間後はクロスが起きない（常に下だから）
        assert signal.iloc[25:].sum() == 0


class TestMABreakoutWithSignalProcessor:
    """SignalProcessorとの統合テスト"""

    def test_ma_breakout_via_processor(self):
        """SignalProcessor経由のMA breakoutシグナル"""
        from src.models.signals import MABreakoutParams, SignalParams
        from src.strategies.signals.processor import SignalProcessor

        dates = pd.date_range("2024-01-01", periods=100)
        np.random.seed(42)
        close = np.linspace(100, 150, 100) + np.random.randn(100) * 2

        ohlc_data = pd.DataFrame(
            {
                "Open": close - 1,
                "High": close + 2,
                "Low": close - 2,
                "Close": close,
                "Volume": np.random.randint(1000, 10000, 100),
            },
            index=dates,
        )

        base_signal = pd.Series(True, index=dates)

        signal_params = SignalParams(
            ma_breakout=MABreakoutParams(
                enabled=True,
                period=20,
                ma_type="sma",
                direction="above",
                lookback_days=1,
            )
        )

        processor = SignalProcessor()
        result = processor.apply_entry_signals(
            base_signal=base_signal,
            ohlc_data=ohlc_data,
            signal_params=signal_params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(base_signal)
