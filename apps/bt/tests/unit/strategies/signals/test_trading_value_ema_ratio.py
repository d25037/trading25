"""Unit tests for trading value EMA ratio signals."""

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from src.domains.strategy.signals.trading_value_ema_ratio import (
    trading_value_ema_ratio_above_signal,
    trading_value_ema_ratio_below_signal,
)
from src.shared.models.signals import (
    TradingValueEmaRatioAboveSignalParams,
    TradingValueEmaRatioBelowSignalParams,
)


class TestTradingValueEmaRatioAboveSignal:
    def setup_method(self) -> None:
        self.dates = pd.date_range("2023-01-01", periods=80)
        self.close = pd.Series(np.full(80, 1000.0), index=self.dates)

    def test_signal_detects_participation_spike(self) -> None:
        volume = pd.Series(np.full(80, 100_000.0), index=self.dates)
        volume.iloc[40] = 2_000_000.0
        volume.iloc[41:46] = 300_000.0

        signal = trading_value_ema_ratio_above_signal(
            self.close,
            volume,
            ratio_threshold=1.0,
            ema_period=3,
            baseline_period=20,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert signal.iloc[40:46].any()

    def test_signal_turns_off_after_post_spike_cooling(self) -> None:
        volume = pd.Series(np.full(80, 100_000.0), index=self.dates)
        volume.iloc[40] = 2_000_000.0
        volume.iloc[41:60] = 25_000.0

        signal = trading_value_ema_ratio_above_signal(
            self.close,
            volume,
            ratio_threshold=1.0,
            ema_period=3,
            baseline_period=20,
        )

        assert signal.iloc[40]
        assert not signal.iloc[47:55].any()

    def test_higher_threshold_reduces_true_count(self) -> None:
        volume = pd.Series(np.full(80, 100_000.0), index=self.dates)
        volume.iloc[40] = 2_000_000.0
        volume.iloc[41:46] = 250_000.0

        low_threshold = trading_value_ema_ratio_above_signal(
            self.close,
            volume,
            ratio_threshold=1.0,
            ema_period=3,
            baseline_period=20,
        )
        high_threshold = trading_value_ema_ratio_above_signal(
            self.close,
            volume,
            ratio_threshold=1.2,
            ema_period=3,
            baseline_period=20,
        )

        assert low_threshold.sum() >= high_threshold.sum()


class TestTradingValueEmaRatioAboveParams:
    def test_invalid_period_order_raises(self) -> None:
        with pytest.raises(ValidationError, match="ADV期間はEMA期間より大きい必要があります"):
            TradingValueEmaRatioAboveSignalParams(
                enabled=True,
                ema_period=20,
                baseline_period=3,
            )


class TestTradingValueEmaRatioBelowSignal:
    def setup_method(self) -> None:
        self.dates = pd.date_range("2023-01-01", periods=80)
        self.close = pd.Series(np.full(80, 1000.0), index=self.dates)

    def test_signal_detects_post_spike_cooling(self) -> None:
        volume = pd.Series(np.full(80, 100_000.0), index=self.dates)
        volume.iloc[40] = 2_000_000.0
        volume.iloc[41:60] = 25_000.0

        signal = trading_value_ema_ratio_below_signal(
            self.close,
            volume,
            ratio_threshold=0.9,
            ema_period=3,
            baseline_period=20,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert signal.iloc[47:55].any()

    def test_lower_threshold_reduces_true_count(self) -> None:
        volume = pd.Series(np.full(80, 100_000.0), index=self.dates)
        volume.iloc[40] = 2_000_000.0
        volume.iloc[41:60] = 35_000.0

        looser_threshold = trading_value_ema_ratio_below_signal(
            self.close,
            volume,
            ratio_threshold=1.0,
            ema_period=3,
            baseline_period=20,
        )
        tighter_threshold = trading_value_ema_ratio_below_signal(
            self.close,
            volume,
            ratio_threshold=0.8,
            ema_period=3,
            baseline_period=20,
        )

        assert looser_threshold.sum() >= tighter_threshold.sum()


class TestTradingValueEmaRatioBelowParams:
    def test_invalid_period_order_raises(self) -> None:
        with pytest.raises(ValidationError, match="ADV期間はEMA期間より大きい必要があります"):
            TradingValueEmaRatioBelowSignalParams(
                enabled=True,
                ema_period=20,
                baseline_period=3,
            )
