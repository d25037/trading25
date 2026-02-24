"""
Buy&Holdシグナル ユニットテスト
"""

import numpy as np
import pandas as pd

from src.domains.strategy.signals.buy_and_hold import generate_buy_and_hold_signals
from src.domains.strategy.signals.processor import SignalProcessor
from src.shared.models.signals import SignalParams, BuyAndHoldSignalParams


class TestBuyAndHoldSignal:
    """generate_buy_and_hold_signals() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2024-01-01", periods=100)
        self.close = pd.Series(np.linspace(100, 150, 100), index=self.dates)

    def test_all_true_basic(self):
        """全日程True基本テスト"""
        signal = generate_buy_and_hold_signals(close=self.close)
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.close)
        # 全日程True
        assert signal.all()

    def test_index_preservation(self):
        """インデックス保持テスト"""
        signal = generate_buy_and_hold_signals(close=self.close)
        assert signal.index.equals(self.close.index)

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = generate_buy_and_hold_signals(close=empty)
        assert len(signal) == 0

    def test_nan_handling(self):
        """NaN含むデータでも全True"""
        close_with_nan = self.close.copy()
        close_with_nan.iloc[10:15] = np.nan
        signal = generate_buy_and_hold_signals(close=close_with_nan)
        # NaNがあっても全日程True
        assert signal.all()


class TestBuyAndHoldExitDisabled:
    """Buy&Hold Exit無効化テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.processor = SignalProcessor()
        self.dates = pd.date_range("2024-01-01", periods=100)
        self.close = pd.Series(np.linspace(100, 150, 100), index=self.dates)
        self.ohlc_data = pd.DataFrame(
            {
                "Open": self.close,
                "High": self.close * 1.01,
                "Low": self.close * 0.99,
                "Close": self.close,
                "Volume": pd.Series(np.random.randint(1000, 10000, 100), index=self.dates),
            }
        )
        self.base_signal = pd.Series(False, index=self.dates)  # 基本Exit無し

    def test_buy_and_hold_entry_enabled(self):
        """Entry用途では正常動作"""
        signal_params = SignalParams(
            buy_and_hold=BuyAndHoldSignalParams(enabled=True)
        )
        base_entry = pd.Series(True, index=self.dates)

        result = self.processor.apply_entry_signals(
            base_signal=base_entry,
            ohlc_data=self.ohlc_data,
            signal_params=signal_params,
        )
        # Entry用途では全日程True（AND条件なのでbase_entryと同じ）
        assert result.all()

    def test_buy_and_hold_exit_skipped(self):
        """Exit用途ではスキップされる"""
        signal_params = SignalParams(
            buy_and_hold=BuyAndHoldSignalParams(enabled=True)
        )
        base_exit = pd.Series(False, index=self.dates)

        result = self.processor.apply_exit_signals(
            base_exits=base_exit,
            data=self.ohlc_data,
            signal_params=signal_params,
        )
        # Exit用途ではBuy&Holdはスキップされ、base_exitがそのまま返る
        assert (result == base_exit).all()
