"""
売買代金範囲シグナルユニットテスト

trading_value_range.pyのtrading_value_range_signal()をテスト
"""

import pytest
import pandas as pd
import numpy as np
from pydantic import ValidationError

from src.strategies.signals.trading_value_range import trading_value_range_signal
from src.models.signals import TradingValueRangeSignalParams, SignalParams
from src.strategies.signals.processor import SignalProcessor


class TestTradingValueRangeSignal:
    """trading_value_range_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)

        # 終値データ（1000円ベース）
        self.close = pd.Series(
            np.random.randn(200).cumsum() + 1000, index=self.dates
        ).clip(lower=100)

        # 出来高データ（異なる売買代金パターン）
        base_volume = np.ones(200) * 100000  # 基本10万株（売買代金: 約1億円）
        base_volume[50:60] = 500000  # 50-60: 50万株（売買代金: 約5億円）
        base_volume[100:110] = 2000000  # 100-110: 200万株（売買代金: 約20億円）
        base_volume[150:160] = 10000  # 150-160: 1万株（売買代金: 約0.1億円）
        self.volume = pd.Series(base_volume, index=self.dates)

    def test_in_range_basic(self):
        """範囲内シグナル基本テスト"""
        # 20日平均売買代金が0.5-100億円の範囲内
        signal = trading_value_range_signal(
            self.close,
            self.volume,
            period=20,
            min_threshold=0.5,
            max_threshold=100.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.volume)
        # 大半のデータが範囲内に入るはず
        assert signal.sum() > 0

    def test_out_of_range_basic(self):
        """範囲外シグナル基本テスト"""
        # 狭い範囲を設定して範囲外を多くする（0.01-0.05億円）
        signal = trading_value_range_signal(
            self.close,
            self.volume,
            period=20,
            min_threshold=0.01,
            max_threshold=0.05,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 狭い範囲なのでFalseが多いはず
        assert signal.sum() < len(signal) * 0.5

    def test_min_threshold_effect(self):
        """最小閾値の効果テスト"""
        signal_low_min = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=0.1, max_threshold=100.0
        )
        signal_high_min = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=10.0, max_threshold=100.0
        )

        # 最小閾値が低い方がTrue数が多い
        assert signal_low_min.sum() >= signal_high_min.sum()

    def test_max_threshold_effect(self):
        """最大閾値の効果テスト"""
        signal_low_max = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=0.5, max_threshold=10.0
        )
        signal_high_max = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=0.5, max_threshold=1000.0
        )

        # 最大閾値が高い方がTrue数が多い
        assert signal_high_max.sum() >= signal_low_max.sum()

    def test_period_effect(self):
        """期間パラメータの効果テスト"""
        signal_short = trading_value_range_signal(
            self.close, self.volume, period=5, min_threshold=0.5, max_threshold=100.0
        )
        signal_long = trading_value_range_signal(
            self.close, self.volume, period=100, min_threshold=0.5, max_threshold=100.0
        )

        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)
        # 期間が異なると結果も異なる
        assert not signal_short.equals(signal_long)

    def test_threshold_range_narrowing(self):
        """範囲を狭めるとTrue数が減少することをテスト"""
        signal_wide = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=0.1, max_threshold=1000.0
        )
        signal_narrow = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=5.0, max_threshold=10.0
        )

        # 広い範囲の方がTrue数が多い
        assert signal_wide.sum() >= signal_narrow.sum()

    def test_nan_handling(self):
        """NaN処理テスト"""
        close_with_nan = self.close.copy()
        close_with_nan.iloc[0:30] = np.nan

        signal = trading_value_range_signal(
            close_with_nan,
            self.volume,
            period=20,
            min_threshold=0.5,
            max_threshold=100.0,
        )

        # NaN期間はFalseに変換される
        assert not signal.iloc[0:30].any()
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_empty_series(self):
        """空Seriesテスト"""
        signal = trading_value_range_signal(
            pd.Series([]), pd.Series([]), period=20, min_threshold=0.5, max_threshold=100.0
        )

        assert len(signal) == 0
        assert isinstance(signal, pd.Series)

    def test_constant_trading_value(self):
        """一定の売買代金テスト"""
        constant_close = pd.Series([1000.0] * 200, index=self.dates)
        constant_volume = pd.Series([100000.0] * 200, index=self.dates)

        # 売買代金 = 1000 * 100000 / 1e8 = 1億円（一定）
        signal = trading_value_range_signal(
            constant_close,
            constant_volume,
            period=20,
            min_threshold=0.5,
            max_threshold=10.0,
        )

        # 一定値が範囲内なので、期間経過後は全てTrue
        assert signal.iloc[20:].all()

    def test_min_equals_max_threshold(self):
        """最小閾値 = 最大閾値のエッジケーステスト"""
        signal = trading_value_range_signal(
            self.close, self.volume, period=20, min_threshold=1.0, max_threshold=1.0
        )

        # ちょうど1億円の日のみTrue（実際にはほとんどFalse）
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_boundary_values(self):
        """境界値テスト（min = value, max = value）"""
        # 売買代金が正確に1億円になるようなデータ
        exact_close = pd.Series([1000.0] * 200, index=self.dates)
        exact_volume = pd.Series([100000.0] * 200, index=self.dates)
        # 1000 * 100000 / 1e8 = 1億円

        # min = 1, max = 1 で境界値を確認
        signal = trading_value_range_signal(
            exact_close, exact_volume, period=20, min_threshold=1.0, max_threshold=1.0
        )

        # 期間経過後は全てTrue（1 ≤ 1 ≤ 1）
        assert signal.iloc[20:].all()


class TestTradingValueRangeSignalValidation:
    """Pydanticバリデーションテスト"""

    def test_invalid_range_max_less_than_min(self):
        """最大閾値 < 最小閾値のバリデーションエラー"""
        with pytest.raises(ValidationError, match="最大閾値は最小閾値より大きい必要があります"):
            TradingValueRangeSignalParams(
                enabled=True,
                period=20,
                min_threshold=100.0,
                max_threshold=50.0,  # max < min
            )

    def test_period_range(self):
        """期間パラメータの範囲テスト"""
        # 正常範囲
        params = TradingValueRangeSignalParams(
            enabled=True, period=20, min_threshold=0.5, max_threshold=100.0
        )
        assert params.period == 20

        # 範囲外（0以下）
        with pytest.raises(ValidationError):
            TradingValueRangeSignalParams(
                enabled=True, period=0, min_threshold=0.5, max_threshold=100.0
            )

        # 範囲外（200超）
        with pytest.raises(ValidationError):
            TradingValueRangeSignalParams(
                enabled=True, period=201, min_threshold=0.5, max_threshold=100.0
            )


class TestTradingValueRangeSignalIntegration:
    """SignalProcessor統合テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        self.close = pd.Series(
            np.random.randn(200).cumsum() + 1000, index=self.dates
        ).clip(lower=100)
        self.volume = pd.Series(np.ones(200) * 100000, index=self.dates)
        self.base_signal = pd.Series([True] * 200, index=self.dates)

        # OHLCV DataFrame作成
        self.ohlc_data = pd.DataFrame(
            {
                "Open": self.close,
                "High": self.close * 1.05,
                "Low": self.close * 0.95,
                "Close": self.close,
                "Volume": self.volume,
            },
            index=self.dates,
        )

    def test_with_signal_processor_entry(self):
        """エントリーフィルターとして使用（AND条件）"""
        # SignalParams作成
        signal_params = SignalParams(
            trading_value_range=TradingValueRangeSignalParams(
                enabled=True, period=20, min_threshold=0.5, max_threshold=100.0
            )
        )

        # SignalProcessor作成
        processor = SignalProcessor()

        # エントリーシグナル適用
        entry_signal = processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="entry",
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
        )

        # AND条件なので、True数が減少またはベースシグナルと同じ
        assert entry_signal.sum() <= self.base_signal.sum()
        assert isinstance(entry_signal, pd.Series)
        assert entry_signal.dtype == bool

    def test_with_signal_processor_exit(self):
        """エグジットトリガーとして使用（OR条件）"""
        # SignalParams作成
        signal_params = SignalParams(
            trading_value_range=TradingValueRangeSignalParams(
                enabled=True, period=20, min_threshold=0.5, max_threshold=100.0
            )
        )

        # SignalProcessor作成
        processor = SignalProcessor()

        # ベースエグジットシグナル（全てFalse）
        base_exit = pd.Series([False] * 200, index=self.dates)

        # エグジットシグナル適用
        # 注: 売買代金範囲シグナルは「範囲内でTrue」なので、範囲外の売却トリガーとしては逆転が必要
        # しかし、レジストリの exit_purpose では「流動性異常警告（範囲外）」と定義されている
        # 実際のエグジット用途では、範囲外でTrueにするためのロジックが別途必要
        exit_signal = processor.apply_signals(
            base_signal=base_exit,
            signal_type="exit",
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
        )

        # OR条件なので、True数が増加またはベースシグナルと同じ
        assert exit_signal.sum() >= base_exit.sum()
        assert isinstance(exit_signal, pd.Series)
        assert exit_signal.dtype == bool
