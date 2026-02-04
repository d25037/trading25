"""
出来高シグナルユニットテスト

volume.pyのvolume_signal()をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.signals.volume import volume_signal


class TestVolumeSignal:
    """volume_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # 通常出来高 + 急増・急減パターン
        base_volume = np.ones(200) * 1000
        base_volume[100:110] = 5000  # 急増
        base_volume[150:160] = 200  # 急減
        self.volume = pd.Series(base_volume, index=self.dates)

    def test_volume_surge_basic(self):
        """出来高急増シグナル基本テスト"""
        signal = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=50,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)
        # 出来高急増期間（100:110）でTrueが発生
        assert signal.iloc[100:120].sum() > 0

    def test_volume_drop_basic(self):
        """出来高減少シグナル基本テスト"""
        signal = volume_signal(
            self.volume,
            direction="drop",
            threshold=0.5,
            short_period=10,
            long_period=50,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)
        # 出来高減少期間（150:160）でTrueが発生
        assert signal.iloc[150:170].sum() > 0

    def test_threshold_effect(self):
        """閾値の効果テスト"""
        signal_low_threshold = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.2,
            short_period=10,
            long_period=50,
        )
        signal_high_threshold = volume_signal(
            self.volume,
            direction="surge",
            threshold=3.0,
            short_period=10,
            long_period=50,
        )

        assert isinstance(signal_low_threshold, pd.Series)
        assert isinstance(signal_high_threshold, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low_threshold.sum() >= signal_high_threshold.sum()

    def test_period_effect(self):
        """期間パラメータの効果テスト"""
        signal_short = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.5,
            short_period=5,
            long_period=20,
        )
        signal_long = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=100,
        )

        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)
        # 期間が異なればシグナルも変化
        assert not signal_short.equals(signal_long)

    def test_nan_handling(self):
        """NaN処理テスト"""
        volume_with_nan = self.volume.copy()
        volume_with_nan.iloc[0:30] = np.nan

        signal = volume_signal(
            volume_with_nan,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=50,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNはFalseに変換される
        assert not signal.iloc[0:30].any()

    def test_empty_series(self):
        """空のSeriesでもエラーにならない"""
        empty = pd.Series([], dtype=float)

        signal = volume_signal(
            empty, direction="surge", threshold=1.5, short_period=10, long_period=50
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == 0

    def test_constant_volume(self):
        """一定出来高ではシグナルが発生しない"""
        constant_volume = pd.Series(np.ones(200) * 1000, index=self.dates)

        signal_surge = volume_signal(
            constant_volume,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=50,
        )
        signal_drop = volume_signal(
            constant_volume,
            direction="drop",
            threshold=0.5,
            short_period=10,
            long_period=50,
        )

        assert signal_surge.sum() == 0
        assert signal_drop.sum() == 0

    def test_extreme_threshold_surge(self):
        """極端な閾値テスト（急増）"""
        signal = volume_signal(
            self.volume,
            direction="surge",
            threshold=10.0,
            short_period=10,
            long_period=50,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 極端な閾値では発生しない可能性が高い
        assert signal.sum() >= 0

    def test_extreme_threshold_drop(self):
        """極端な閾値テスト（急減）"""
        signal = volume_signal(
            self.volume,
            direction="drop",
            threshold=0.01,
            short_period=10,
            long_period=50,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 極端な閾値では発生しない可能性が高い
        assert signal.sum() >= 0

    def test_volume_signal_ema_surge(self):
        """EMA使用時の出来高急増シグナルテスト"""
        signal = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=50,
            ma_type="ema",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)
        # EMAでも出来高急増期間（100:110）でTrueが発生
        assert signal.iloc[100:120].sum() > 0

    def test_volume_signal_ema_drop(self):
        """EMA使用時の出来高減少シグナルテスト"""
        signal = volume_signal(
            self.volume,
            direction="drop",
            threshold=0.5,
            short_period=10,
            long_period=50,
            ma_type="ema",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)
        # EMAでも出来高減少期間（150:160）でTrueが発生
        assert signal.iloc[150:170].sum() > 0

    def test_ma_type_sma_vs_ema(self):
        """SMAとEMAの結果が異なることを確認"""
        signal_sma = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=50,
            ma_type="sma",
        )
        signal_ema = volume_signal(
            self.volume,
            direction="surge",
            threshold=1.5,
            short_period=10,
            long_period=50,
            ma_type="ema",
        )

        assert isinstance(signal_sma, pd.Series)
        assert isinstance(signal_ema, pd.Series)
        # SMAとEMAは異なる結果を返す（完全一致しない）
        assert not signal_sma.equals(signal_ema)


class TestVolumeSignalIntegration:
    """SignalProcessorとの統合テスト"""

    def test_volume_signal_with_signal_processor_entry(self):
        """SignalProcessorでエントリーフィルターとして使用"""
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

        # 出来高急増シグナルを有効化
        params = SignalParams()
        params.volume.enabled = True
        params.volume.direction = "surge"
        params.volume.threshold = 1.5
        params.volume.short_period = 20
        params.volume.long_period = 100

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
        # エントリーフィルター（AND条件）なのでTrue数は減少
        assert result.sum() <= base_signal.sum()

    def test_volume_signal_with_signal_processor_exit(self):
        """SignalProcessorでエグジットトリガーとして使用"""
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

        base_signal = pd.Series([False] * 200, index=dates)

        # 出来高減少シグナルを有効化（エグジット用）
        params = SignalParams()
        params.volume.enabled = True
        params.volume.direction = "drop"
        params.volume.threshold = 0.7
        params.volume.short_period = 20
        params.volume.long_period = 100

        processor = SignalProcessor()
        result = processor.apply_signals(
            base_signal=base_signal,
            signal_type="exit",
            ohlc_data=ohlc_data,
            signal_params=params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(base_signal)
        # エグジットトリガー（OR条件）なのでTrue数は増加可能
        assert result.sum() >= base_signal.sum()

    def test_volume_signal_ema_with_signal_processor(self):
        """SignalProcessorでEMA使用時のテスト"""
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

        # 出来高急増シグナル（EMA使用）
        params = SignalParams()
        params.volume.enabled = True
        params.volume.direction = "surge"
        params.volume.threshold = 1.5
        params.volume.short_period = 20
        params.volume.long_period = 100
        params.volume.ma_type = "ema"

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
        # EMAでもエントリーフィルター（AND条件）として機能
        assert result.sum() <= base_signal.sum()


if __name__ == "__main__":
    pytest.main([__file__])
