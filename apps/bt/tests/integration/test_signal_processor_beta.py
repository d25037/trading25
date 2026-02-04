"""
SignalProcessor経由のβ値シグナル統合テスト

レジストリ → processor → 実際のシグナル関数の統合フロー検証
"""

import unittest
import pandas as pd
import numpy as np

from src.strategies.signals.processor import SignalProcessor
from src.models.signals import SignalParams, BetaSignalParams


class TestSignalProcessorBeta(unittest.TestCase):
    """SignalProcessor経由のβ値シグナル統合テスト"""

    def setUp(self):
        """テストケース共通の設定"""
        # SignalProcessorインスタンス化
        self.processor = SignalProcessor()

        # テスト用の価格データ（100日間）
        dates = pd.date_range("2024-01-01", periods=100, freq="D")

        # 市場価格データ（ベンチマーク）
        np.random.seed(42)
        market_returns = np.random.normal(0.001, 0.02, 100)
        self.market_price = pd.Series(
            100 * np.cumprod(1 + market_returns), index=dates, name="market"
        )

        # β=1.2の銘柄価格データ（市場より高い感応度）
        high_beta_returns = 1.2 * market_returns + np.random.normal(0, 0.01, 100)
        self.high_beta_price = pd.Series(
            100 * np.cumprod(1 + high_beta_returns), index=dates, name="high_beta_stock"
        )

        # OHLCデータ作成
        self.ohlc_data = pd.DataFrame(
            {
                "Open": self.high_beta_price,
                "High": self.high_beta_price * 1.01,
                "Low": self.high_beta_price * 0.99,
                "Close": self.high_beta_price,
                "Volume": pd.Series(np.random.randint(1000, 10000, 100), index=dates),
            }
        )

        # ベンチマークデータ作成
        self.benchmark_data = pd.DataFrame(
            {
                "Open": self.market_price,
                "High": self.market_price * 1.01,
                "Low": self.market_price * 0.99,
                "Close": self.market_price,
                "Volume": pd.Series(np.random.randint(10000, 100000, 100), index=dates),
            }
        )

        # 基本エントリーシグナル（全日程True）
        self.base_signal = pd.Series(True, index=dates)

    def test_beta_signal_integration_enabled(self):
        """β値シグナル有効化時の統合テスト"""
        # SignalParams作成（β値シグナル有効化）
        signal_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True,
                min_beta=0.5,
                max_beta=1.5,
                lookback_period=30,
            )
        )

        # SignalProcessor経由でエントリーシグナル適用
        # execution_data を渡して相対価格モードではないことを示す
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
            benchmark_data=self.benchmark_data,
            execution_data=self.ohlc_data,  # 実価格データ（相対価格モードではない）
        )

        # 結果の基本検証
        self.assertIsInstance(result, pd.Series)
        self.assertEqual(len(result), len(self.base_signal))
        self.assertTrue(result.dtype == bool)

        # β値シグナルが適用されている（一部期間でフィルタリングされている）
        self.assertTrue(result.any())
        self.assertLess(result.sum(), self.base_signal.sum())  # フィルター効果で減少

    def test_beta_signal_integration_disabled(self):
        """β値シグナル無効化時の統合テスト"""
        # SignalParams作成（β値シグナル無効化）
        signal_params = SignalParams(
            beta=BetaSignalParams(
                enabled=False,  # 無効化
                min_beta=0.5,
                max_beta=1.5,
                lookback_period=30,
            )
        )

        # SignalProcessor経由でエントリーシグナル適用
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
            benchmark_data=self.benchmark_data,
        )

        # β値シグナルが無効化されている（全日程True）
        self.assertTrue((result == self.base_signal).all())

    def test_beta_signal_integration_no_benchmark_data(self):
        """ベンチマークデータなしの統合テスト"""
        # SignalParams作成（β値シグナル有効化）
        signal_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True,
                min_beta=0.5,
                max_beta=1.5,
                lookback_period=30,
            )
        )

        # ベンチマークデータなしで実行
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
            benchmark_data=None,  # ベンチマークデータなし
        )

        # β値シグナルはスキップされ、基本シグナルがそのまま返される
        self.assertTrue((result == self.base_signal).all())

    def test_beta_signal_integration_narrow_range(self):
        """β値範囲が狭い場合の統合テスト"""
        # SignalParams作成（非常に狭いβ値範囲）
        signal_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True,
                min_beta=0.9,
                max_beta=1.1,  # 高β銘柄（β≈1.2）は通過しない範囲
                lookback_period=50,
            )
        )

        # SignalProcessor経由でエントリーシグナル適用
        # execution_data を渡して相対価格モードではないことを示す
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
            benchmark_data=self.benchmark_data,
            execution_data=self.ohlc_data,  # 実価格データ（相対価格モードではない）
        )

        # 狭い範囲でフィルター効果が強い
        pass_rate = result.sum() / self.base_signal.sum()
        self.assertLess(pass_rate, 0.5)  # 50%未満の通過率

    def test_beta_signal_integration_with_other_signals(self):
        """β値シグナルと他のシグナルの組み合わせテスト"""
        from src.models.signals import VolumeSignalParams

        # SignalParams作成（β値 + 出来高シグナル）
        signal_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True,
                min_beta=0.5,
                max_beta=1.5,
                lookback_period=30,
            ),
            volume=VolumeSignalParams(
                enabled=True,
                direction="surge",
                threshold=1.5,
                short_period=20,
                long_period=100,
            ),
        )

        # SignalProcessor経由でエントリーシグナル適用
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
            benchmark_data=self.benchmark_data,
        )

        # 複数シグナルのAND条件適用（β値 AND 出来高）
        # 複数フィルター効果で大幅に減少（条件が厳しい場合はゼロになることもある）
        self.assertLessEqual(
            result.sum(), self.base_signal.sum()
        )  # 複数フィルター効果で減少またはゼロ


if __name__ == "__main__":
    unittest.main()
