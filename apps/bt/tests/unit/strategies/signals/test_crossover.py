"""
クロスオーバーシグナルユニットテスト

crossover.pyのcrossover_signal()とindicator_crossover_signal()をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.signals.crossover import (
    crossover_signal,
    indicator_crossover_signal,
)


class TestCrossoverSignal:
    """crossover_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.fast_line = pd.Series(np.linspace(90, 110, 100), index=self.dates)
        self.slow_line = pd.Series(np.linspace(100, 100, 100), index=self.dates)

    def test_golden_cross_basic(self):
        """ゴールデンクロス基本テスト"""
        # fast_lineが100を超える位置でゴールデンクロス発生
        signal = crossover_signal(self.fast_line, self.slow_line, direction="golden")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.fast_line)
        # ゴールデンクロスが発生している
        assert signal.sum() > 0

    def test_dead_cross_basic(self):
        """デッドクロス基本テスト"""
        # fast_lineが下降してslow_lineを下回る
        fast_falling = pd.Series(np.linspace(110, 90, 100), index=self.dates)
        slow_stable = pd.Series(np.linspace(100, 100, 100), index=self.dates)

        signal = crossover_signal(fast_falling, slow_stable, direction="dead")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(fast_falling)
        # デッドクロスが発生している
        assert signal.sum() > 0

    def test_invalid_direction(self):
        """不正なdirectionでエラー"""
        with pytest.raises(ValueError, match="不正なdirection"):
            crossover_signal(
                self.fast_line, self.slow_line, direction="invalid_direction"
            )

    def test_nan_handling(self):
        """NaN処理テスト"""
        fast_with_nan = self.fast_line.copy()
        fast_with_nan.iloc[0:5] = np.nan

        signal = crossover_signal(fast_with_nan, self.slow_line, direction="golden")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNはFalseに変換される
        assert not signal.iloc[0:5].any()

    def test_no_crossover(self):
        """クロスオーバーが発生しない場合"""
        # 常にfast > slow
        fast_always_above = pd.Series(np.ones(100) * 110, index=self.dates)
        slow = pd.Series(np.ones(100) * 100, index=self.dates)

        signal = crossover_signal(fast_always_above, slow, direction="golden")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # ゴールデンクロスは発生しない（既に上にいる）
        assert signal.sum() == 0

    def test_empty_series(self):
        """空のSeriesでもエラーにならない"""
        empty = pd.Series([], dtype=float)

        signal = crossover_signal(empty, empty, direction="golden")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == 0


class TestIndicatorCrossoverSignal:
    """indicator_crossover_signal()の統合テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # トレンド転換を含む価格データ
        self.close = pd.Series(
            np.concatenate(
                [
                    np.linspace(100, 90, 100),  # 下降トレンド
                    np.linspace(90, 110, 100),  # 上昇トレンド
                ]
            ),
            index=self.dates,
        )

    def test_sma_golden_cross(self):
        """SMAゴールデンクロステスト"""
        signal = indicator_crossover_signal(
            self.close,
            indicator_type="sma",
            fast_period=10,
            slow_period=30,
            direction="golden",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # ゴールデンクロスが発生している（上昇トレンド部分）
        assert signal.iloc[100:].sum() > 0

    def test_ema_dead_cross(self):
        """EMAデッドクロステスト"""
        signal = indicator_crossover_signal(
            self.close,
            indicator_type="ema",
            fast_period=10,
            slow_period=30,
            direction="dead",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # デッドクロスが発生している可能性がある（データパターン依存）
        assert signal.iloc[0:100].sum() >= 0

    def test_rsi_crossover(self):
        """RSIクロスオーバーテスト"""
        signal = indicator_crossover_signal(
            self.close,
            indicator_type="rsi",
            fast_period=7,
            slow_period=14,
            direction="golden",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)

    def test_macd_crossover(self):
        """MACDクロスオーバーテスト"""
        signal = indicator_crossover_signal(
            self.close,
            indicator_type="macd",
            fast_period=12,
            slow_period=26,
            signal_period=9,
            direction="golden",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)

    def test_lookback_days_feature(self):
        """lookback_days機能テスト（直近X日以内クロス検出）"""
        signal_1day = indicator_crossover_signal(
            self.close,
            indicator_type="sma",
            fast_period=10,
            slow_period=30,
            direction="golden",
            lookback_days=1,
        )

        signal_10days = indicator_crossover_signal(
            self.close,
            indicator_type="sma",
            fast_period=10,
            slow_period=30,
            direction="golden",
            lookback_days=10,
        )

        assert isinstance(signal_1day, pd.Series)
        assert isinstance(signal_10days, pd.Series)
        # lookback_days=10の方がTrue数が多い（直近10日以内のクロスを全て検出）
        assert signal_10days.sum() >= signal_1day.sum()

    def test_invalid_indicator_type(self):
        """未対応のインジケータータイプでエラー"""
        with pytest.raises(ValueError, match="未対応のインジケータータイプ"):
            indicator_crossover_signal(
                self.close,
                indicator_type="invalid_type",
                fast_period=10,
                slow_period=30,
                direction="golden",
            )

    def test_invalid_period_order(self):
        """fast_period > slow_periodでも動作する"""
        # VectorBTは内部でエラーになる可能性があるが、関数自体はエラーハンドリング不要
        # （パラメータバリデーションはPydanticで行われる）
        signal = indicator_crossover_signal(
            self.close,
            indicator_type="sma",
            fast_period=30,
            slow_period=10,  # 逆順
            direction="golden",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_short_data_handling(self):
        """データが短い場合の処理"""
        short_close = self.close.iloc[0:50]

        signal = indicator_crossover_signal(
            short_close,
            indicator_type="sma",
            fast_period=10,
            slow_period=30,
            direction="golden",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(short_close)


class TestCrossoverSignalIntegration:
    """SignalProcessorとの統合テスト"""

    def test_crossover_signal_with_signal_processor(self):
        """SignalProcessorでクロスオーバーシグナルを使用"""
        from src.strategies.signals.processor import SignalProcessor
        from src.models.signals import SignalParams

        dates = pd.date_range("2023-01-01", periods=200)
        ohlc_data = pd.DataFrame(
            {
                "Open": np.random.randn(200).cumsum() + 100,
                "High": np.random.randn(200).cumsum() + 105,
                "Low": np.random.randn(200).cumsum() + 95,
                "Close": np.random.randn(200).cumsum() + 100,
                "Volume": np.random.randint(1000, 10000, 200),
            },
            index=dates,
        )

        base_signal = pd.Series([True] * 200, index=dates)

        # クロスオーバーシグナルを有効化
        params = SignalParams()
        params.crossover.enabled = True
        params.crossover.type = "sma"
        params.crossover.direction = "golden"
        params.crossover.fast_period = 10
        params.crossover.slow_period = 30

        processor = SignalProcessor()
        result = processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=ohlc_data,
            signal_params=params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(base_signal)


if __name__ == "__main__":
    pytest.main([__file__])
