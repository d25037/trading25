"""
売買代金シグナルユニットテスト

trading_value.pyのtrading_value_signal()をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.signals.trading_value import trading_value_signal


class TestTradingValueSignal:
    """trading_value_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)

        # 終値データ（1000円ベース）
        self.close = pd.Series(
            np.random.randn(200).cumsum() + 1000, index=self.dates
        ).clip(lower=100)

        # 出来高データ（通常 + 大きい・小さいパターン）
        base_volume = np.ones(200) * 100000  # 基本10万株
        base_volume[100:110] = 1000000  # 100-110: 100万株（売買代金大）
        base_volume[150:160] = 10000  # 150-160: 1万株（売買代金小）
        self.volume = pd.Series(base_volume, index=self.dates)

    def test_trading_value_above_basic(self):
        """売買代金閾値以上シグナル基本テスト"""
        # 20日平均売買代金が10億円以上
        signal = trading_value_signal(
            self.close,
            self.volume,
            direction="above",
            period=20,
            threshold_value=10.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)

    def test_trading_value_below_basic(self):
        """売買代金閾値以下シグナル基本テスト"""
        # 20日平均売買代金が1億円以下
        signal = trading_value_signal(
            self.close,
            self.volume,
            direction="below",
            period=20,
            threshold_value=1.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)

    def test_threshold_value_effect(self):
        """閾値の効果テスト"""
        signal_low_threshold = trading_value_signal(
            self.close,
            self.volume,
            direction="above",
            period=20,
            threshold_value=1.0,
        )
        signal_high_threshold = trading_value_signal(
            self.close,
            self.volume,
            direction="above",
            period=20,
            threshold_value=100.0,
        )

        assert isinstance(signal_low_threshold, pd.Series)
        assert isinstance(signal_high_threshold, pd.Series)
        # 低い閾値の方がTrue数が多い（above方向の場合）
        assert signal_low_threshold.sum() >= signal_high_threshold.sum()

    def test_threshold_value_effect_below(self):
        """閾値の効果テスト（below方向）"""
        signal_low_threshold = trading_value_signal(
            self.close,
            self.volume,
            direction="below",
            period=20,
            threshold_value=1.0,
        )
        signal_high_threshold = trading_value_signal(
            self.close,
            self.volume,
            direction="below",
            period=20,
            threshold_value=100.0,
        )

        assert isinstance(signal_low_threshold, pd.Series)
        assert isinstance(signal_high_threshold, pd.Series)
        # 高い閾値の方がTrue数が多い（below方向の場合）
        assert signal_high_threshold.sum() >= signal_low_threshold.sum()

    def test_period_effect(self):
        """期間パラメータの効果テスト"""
        signal_short = trading_value_signal(
            self.close,
            self.volume,
            direction="above",
            period=5,
            threshold_value=5.0,
        )
        signal_long = trading_value_signal(
            self.close,
            self.volume,
            direction="above",
            period=100,
            threshold_value=5.0,
        )

        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)
        # 期間が異なればシグナルも変化（またはTrue数が異なる）
        # 同じ場合もあるが、True数は変化する可能性が高い
        assert signal_short.sum() != signal_long.sum() or not signal_short.equals(signal_long)

    def test_nan_handling(self):
        """NaN処理テスト"""
        close_with_nan = self.close.copy()
        close_with_nan.iloc[0:30] = np.nan

        signal = trading_value_signal(
            close_with_nan,
            self.volume,
            direction="above",
            period=20,
            threshold_value=10.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNはFalseに変換される
        assert not signal.iloc[0:30].any()

    def test_empty_series(self):
        """空のSeriesでもエラーにならない"""
        empty_close = pd.Series([], dtype=float)
        empty_volume = pd.Series([], dtype=float)

        signal = trading_value_signal(
            empty_close,
            empty_volume,
            direction="above",
            period=20,
            threshold_value=10.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == 0

    def test_constant_trading_value_above(self):
        """一定売買代金での閾値以上判定"""
        constant_close = pd.Series(np.ones(200) * 1000, index=self.dates)
        constant_volume = pd.Series(np.ones(200) * 100000, index=self.dates)

        # 売買代金 = 1000 × 100000 / 1e8 = 1.0億円
        # 閾値1.0億円以上 → Trueになるはず
        signal_above = trading_value_signal(
            constant_close,
            constant_volume,
            direction="above",
            period=20,
            threshold_value=1.0,
        )

        # 期間20以降はTrueになる
        assert signal_above.iloc[20:].sum() > 0

    def test_constant_trading_value_below(self):
        """一定売買代金での閾値以下判定"""
        constant_close = pd.Series(np.ones(200) * 1000, index=self.dates)
        constant_volume = pd.Series(np.ones(200) * 100000, index=self.dates)

        # 売買代金 = 1000 × 100000 / 1e8 = 1.0億円
        # 閾値10億円以下 → Trueになるはず
        signal_below = trading_value_signal(
            constant_close,
            constant_volume,
            direction="below",
            period=20,
            threshold_value=10.0,
        )

        # 期間20以降はTrueになる
        assert signal_below.iloc[20:].sum() > 0

    def test_trading_value_calculation_precise(self):
        """売買代金計算の正しさ検証"""
        # シンプルなデータで売買代金計算を検証
        simple_close = pd.Series(
            [1000.0] * 100, index=pd.date_range("2023-01-01", periods=100)
        )
        simple_volume = pd.Series(
            [100000.0] * 100, index=pd.date_range("2023-01-01", periods=100)
        )

        # 売買代金 = 1000 × 100000 / 1e8 = 1.0億円
        # 閾値1.0億円以上 → 期間経過後はTrueになるはず
        signal_above = trading_value_signal(
            simple_close,
            simple_volume,
            direction="above",
            period=20,
            threshold_value=1.0,
        )

        assert isinstance(signal_above, pd.Series)
        assert signal_above.dtype == bool
        # 20日以降はTrueになる
        assert signal_above.iloc[20:].sum() > 0

    def test_trading_value_calculation_below(self):
        """売買代金計算の正しさ検証（below）"""
        simple_close = pd.Series(
            [1000.0] * 100, index=pd.date_range("2023-01-01", periods=100)
        )
        simple_volume = pd.Series(
            [100000.0] * 100, index=pd.date_range("2023-01-01", periods=100)
        )

        # 売買代金 = 1000 × 100000 / 1e8 = 1.0億円
        # 閾値10億円以下 → 期間経過後はTrueになるはず
        signal_below = trading_value_signal(
            simple_close,
            simple_volume,
            direction="below",
            period=20,
            threshold_value=10.0,
        )

        assert isinstance(signal_below, pd.Series)
        assert signal_below.dtype == bool
        # 20日以降はTrueになる
        assert signal_below.iloc[20:].sum() > 0

    def test_high_volume_period(self):
        """高出来高期間でのシグナル検証"""
        # 100-110で出来高が高い → 売買代金も高い
        signal_above = trading_value_signal(
            self.close,
            self.volume,
            direction="above",
            period=5,
            threshold_value=50.0,
        )

        # 高出来高期間（100-110）付近でTrueが発生する可能性
        assert isinstance(signal_above, pd.Series)
        assert signal_above.dtype == bool

    def test_low_volume_period(self):
        """低出来高期間でのシグナル検証"""
        # 150-160で出来高が低い → 売買代金も低い
        signal_below = trading_value_signal(
            self.close,
            self.volume,
            direction="below",
            period=5,
            threshold_value=5.0,
        )

        # 低出来高期間（150-160）付近でTrueが発生する可能性
        assert isinstance(signal_below, pd.Series)
        assert signal_below.dtype == bool


class TestTradingValueSignalIntegration:
    """SignalProcessorとの統合テスト"""

    def test_trading_value_signal_with_signal_processor_entry(self):
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
                "Volume": np.random.randint(100000, 1000000, 200),
            },
            index=dates,
        )

        base_signal = pd.Series([True] * 200, index=dates)

        # 売買代金閾値以上シグナルを有効化
        params = SignalParams()
        params.trading_value.enabled = True
        params.trading_value.direction = "above"
        params.trading_value.period = 20
        params.trading_value.threshold_value = 10.0

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

    def test_trading_value_signal_with_signal_processor_exit(self):
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
                "Volume": np.random.randint(100000, 1000000, 200),
            },
            index=dates,
        )

        base_signal = pd.Series([False] * 200, index=dates)

        # 売買代金閾値以下シグナルを有効化（エグジット用）
        params = SignalParams()
        params.trading_value.enabled = True
        params.trading_value.direction = "below"
        params.trading_value.period = 20
        params.trading_value.threshold_value = 1.0

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


if __name__ == "__main__":
    pytest.main([__file__])
