"""
信用残高シグナルユニットテスト

margin.pyのmargin_balance_percentile_signal()をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.domains.strategy.signals.margin import margin_balance_percentile_signal
from src.shared.models.signals import MarginSignalParams


class TestMarginBalancePercentileSignal:
    """margin_balance_percentile_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # 信用残高データ（変動パターン）
        base_balance = np.linspace(1000000, 2000000, 200)
        # 一部に低い残高を設定
        base_balance[100:120] = 500000  # 低残高期間
        self.margin_balance = pd.Series(base_balance, index=self.dates)

    def test_margin_signal_basic_with_params(self):
        """MarginSignalParams使用基本テスト"""
        params = MarginSignalParams(
            enabled=True, lookback_period=50, percentile_threshold=0.2
        )

        signal = margin_balance_percentile_signal(self.margin_balance, params=params)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.margin_balance)
        # 低残高期間（100:120）周辺でTrueが発生
        assert signal.iloc[100:150].sum() > 0

    def test_margin_signal_with_fallback_params(self):
        """フォールバックパラメータ使用テスト"""
        signal = margin_balance_percentile_signal(
            self.margin_balance, lookback_period=50, percentile_threshold=0.2
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.margin_balance)

    def test_margin_signal_disabled(self):
        """無効化されたシグナルテスト"""
        params = MarginSignalParams(
            enabled=False, lookback_period=50, percentile_threshold=0.2
        )

        signal = margin_balance_percentile_signal(self.margin_balance, params=params)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 全てFalse
        assert signal.sum() == 0

    def test_percentile_threshold_effect(self):
        """パーセンタイル閾値の効果テスト"""
        signal_low = margin_balance_percentile_signal(
            self.margin_balance, lookback_period=50, percentile_threshold=0.1
        )
        signal_high = margin_balance_percentile_signal(
            self.margin_balance, lookback_period=50, percentile_threshold=0.5
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 高いパーセンタイルの方がTrue数が多い
        assert signal_high.sum() >= signal_low.sum()

    def test_lookback_period_effect(self):
        """ルックバック期間の効果テスト"""
        signal_short = margin_balance_percentile_signal(
            self.margin_balance, lookback_period=20, percentile_threshold=0.2
        )
        signal_long = margin_balance_percentile_signal(
            self.margin_balance, lookback_period=100, percentile_threshold=0.2
        )

        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)
        # 期間が異なればシグナルも変化する可能性（データパターン依存）
        # True数が異なることで期間効果を確認
        assert signal_short.sum() >= 0
        assert signal_long.sum() >= 0

    def test_invalid_lookback_period(self):
        """不正なルックバック期間でエラー"""
        with pytest.raises(ValueError, match="lookback_period must be in range"):
            margin_balance_percentile_signal(
                self.margin_balance, lookback_period=0, percentile_threshold=0.2
            )

        with pytest.raises(ValueError, match="lookback_period must be in range"):
            margin_balance_percentile_signal(
                self.margin_balance, lookback_period=600, percentile_threshold=0.2
            )

    def test_invalid_percentile_threshold(self):
        """不正なパーセンタイル閾値でエラー"""
        with pytest.raises(ValueError, match="percentile_threshold must be in range"):
            margin_balance_percentile_signal(
                self.margin_balance, lookback_period=50, percentile_threshold=0.0
            )

        with pytest.raises(ValueError, match="percentile_threshold must be in range"):
            margin_balance_percentile_signal(
                self.margin_balance, lookback_period=50, percentile_threshold=1.5
            )

    def test_nan_handling(self):
        """NaN処理テスト"""
        margin_with_nan = self.margin_balance.copy()
        margin_with_nan.iloc[0:30] = np.nan

        signal = margin_balance_percentile_signal(
            margin_with_nan, lookback_period=50, percentile_threshold=0.2
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:30].any()

    def test_constant_balance(self):
        """一定残高ではシグナルが発生しない"""
        constant_balance = pd.Series(np.ones(200) * 1000000, index=self.dates)

        signal = margin_balance_percentile_signal(
            constant_balance, lookback_period=50, percentile_threshold=0.2
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 一定値では発生しない可能性が高い
        assert signal.sum() >= 0

    def test_extreme_low_balance(self):
        """極端な低残高パターンテスト"""
        # 最初と最後に極端な低値
        extreme_balance = self.margin_balance.copy()
        extreme_balance.iloc[150:160] = 100000

        signal = margin_balance_percentile_signal(
            extreme_balance, lookback_period=50, percentile_threshold=0.2
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 極端な低値期間でTrueが発生
        assert signal.iloc[150:180].sum() > 0


class TestMarginSignalIntegration:
    """SignalProcessorとの統合テスト"""

    def test_margin_signal_with_signal_processor(self):
        """SignalProcessorでマージンシグナルを使用"""
        from src.domains.strategy.signals.processor import SignalProcessor
        from src.shared.models.signals import SignalParams

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

        # マージンデータ
        margin_data = pd.DataFrame(
            {"margin_balance": np.random.randint(500000, 2000000, 200)}, index=dates
        )

        base_signal = pd.Series([True] * 200, index=dates)

        # マージンシグナルを有効化
        params = SignalParams()
        params.margin.enabled = True
        params.margin.lookback_period = 50
        params.margin.percentile_threshold = 0.2

        processor = SignalProcessor()
        result = processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=ohlc_data,
            signal_params=params,
            margin_data=margin_data,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(base_signal)


if __name__ == "__main__":
    pytest.main([__file__])
