"""
基準線シグナルユニットテスト
"""

import numpy as np
import pandas as pd
import pytest

from src.domains.strategy.signals.baseline import (
    baseline_cross_signal,
    baseline_deviation_signal,
    baseline_position_signal,
    cross_signal,
    deviation_signal,
    position_signal,
)
from src.domains.strategy.signals.processor import SignalProcessor
from src.shared.models.signals import BaselineCrossSignalParams, SignalParams


def _make_ohlc(close_values: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=len(close_values))
    close = pd.Series(close_values, index=dates, dtype=float)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 3,
            "Low": close - 3,
            "Close": close,
            "Volume": pd.Series(np.linspace(1000, 2000, len(close_values)), index=dates),
        }
    )


class TestDeviationSignal:
    def test_below(self):
        dates = pd.date_range("2024-01-01", periods=5)
        baseline = pd.Series([100.0] * 5, index=dates)
        price = pd.Series([100.0, 95.0, 80.0, 90.0, 100.0], index=dates)

        signal = deviation_signal(price, baseline, threshold=0.15, direction="below")

        assert signal.tolist() == [False, False, True, False, False]

    def test_above(self):
        dates = pd.date_range("2024-01-01", periods=5)
        baseline = pd.Series([100.0] * 5, index=dates)
        price = pd.Series([100.0, 105.0, 120.0, 110.0, 100.0], index=dates)

        signal = deviation_signal(price, baseline, threshold=0.15, direction="above")

        assert signal.tolist() == [False, False, True, False, False]

    def test_invalid_direction(self):
        dates = pd.date_range("2024-01-01", periods=3)
        baseline = pd.Series([100.0] * 3, index=dates)
        price = pd.Series([100.0, 80.0, 120.0], index=dates)

        with pytest.raises(ValueError, match="不正なdirection"):
            deviation_signal(price, baseline, threshold=0.1, direction="flat")


class TestPositionSignal:
    def test_above_and_below(self):
        dates = pd.date_range("2024-01-01", periods=3)
        baseline = pd.Series([100.0] * 3, index=dates)
        price = pd.Series([95.0, 100.0, 105.0], index=dates)

        assert position_signal(price, baseline, direction="above").tolist() == [
            False,
            False,
            True,
        ]
        assert position_signal(price, baseline, direction="below").tolist() == [
            True,
            False,
            False,
        ]

    def test_invalid_direction(self):
        dates = pd.date_range("2024-01-01", periods=3)
        baseline = pd.Series([100.0] * 3, index=dates)
        price = pd.Series([95.0, 100.0, 105.0], index=dates)

        with pytest.raises(ValueError, match="不正なdirection"):
            position_signal(price, baseline, direction="flat")


class TestCrossSignal:
    def test_cross_above(self):
        dates = pd.date_range("2024-01-01", periods=5)
        baseline = pd.Series([100.0] * 5, index=dates)
        price = pd.Series([99.0, 98.0, 101.0, 102.0, 99.0], index=dates)

        signal = cross_signal(price, baseline, direction="above", lookback_days=1)

        assert signal.tolist() == [False, False, True, False, False]

    def test_cross_below(self):
        dates = pd.date_range("2024-01-01", periods=5)
        baseline = pd.Series([100.0] * 5, index=dates)
        price = pd.Series([101.0, 102.0, 99.0, 98.0, 101.0], index=dates)

        signal = cross_signal(price, baseline, direction="below", lookback_days=1)

        assert signal.tolist() == [False, False, True, False, False]

    def test_lookback_days(self):
        dates = pd.date_range("2024-01-01", periods=5)
        baseline = pd.Series([100.0] * 5, index=dates)
        price = pd.Series([99.0, 98.0, 101.0, 102.0, 99.0], index=dates)

        signal = cross_signal(price, baseline, direction="above", lookback_days=2)

        assert signal.tolist() == [False, False, True, True, False]

    def test_invalid_direction(self):
        dates = pd.date_range("2024-01-01", periods=3)
        baseline = pd.Series([100.0] * 3, index=dates)
        price = pd.Series([99.0, 101.0, 99.0], index=dates)

        with pytest.raises(ValueError, match="不正なdirection"):
            cross_signal(price, baseline, direction="flat", lookback_days=1)


class TestBaselineDeviationSignal:
    def test_sma(self):
        ohlc = _make_ohlc([100, 100, 100, 100, 100, 80, 80, 100, 100, 100])

        signal = baseline_deviation_signal(
            ohlc,
            baseline_type="sma",
            baseline_period=5,
            deviation_threshold=0.15,
            direction="below",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert signal.any()

    def test_ema(self):
        ohlc = _make_ohlc([100, 100, 100, 100, 100, 80, 80, 100, 100, 100])

        signal = baseline_deviation_signal(
            ohlc,
            baseline_type="ema",
            baseline_period=5,
            deviation_threshold=0.15,
            direction="below",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_vwema_requires_volume(self):
        ohlc = _make_ohlc([100, 100, 100, 100, 100, 80, 80, 100, 100, 100]).drop(
            columns=["Volume"]
        )

        with pytest.raises(ValueError, match="Volume"):
            baseline_deviation_signal(
                ohlc,
                baseline_type="vwema",
                baseline_period=5,
                deviation_threshold=0.15,
                direction="below",
            )

    def test_invalid_baseline_type(self):
        ohlc = _make_ohlc([100, 100, 100, 100, 100, 80, 80, 100, 100, 100])

        with pytest.raises(ValueError, match="未対応のベースラインタイプ"):
            baseline_deviation_signal(
                ohlc,
                baseline_type="wma",
                baseline_period=5,
                deviation_threshold=0.15,
                direction="below",
            )


class TestBaselinePositionSignal:
    def test_high_above(self):
        ohlc = _make_ohlc([100] * 10)

        signal = baseline_position_signal(
            ohlc,
            baseline_type="sma",
            baseline_period=5,
            direction="above",
            price_column="high",
        )

        assert signal.iloc[4:].all()

    def test_low_below(self):
        ohlc = _make_ohlc([100] * 10)

        signal = baseline_position_signal(
            ohlc,
            baseline_type="sma",
            baseline_period=5,
            direction="below",
            price_column="low",
        )

        assert signal.iloc[4:].all()

    def test_invalid_price_column(self):
        ohlc = _make_ohlc([100] * 10)

        with pytest.raises(ValueError, match="不正なprice_column"):
            baseline_position_signal(
                ohlc,
                baseline_type="sma",
                baseline_period=5,
                direction="above",
                price_column="open",
            )


class TestBaselineCrossSignal:
    def test_sma(self):
        ohlc = _make_ohlc([100, 100, 100, 95, 94, 93, 110, 111, 112, 113])

        signal = baseline_cross_signal(
            ohlc,
            baseline_type="sma",
            baseline_period=3,
            direction="above",
            lookback_days=1,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert signal.any()

    def test_vwema(self):
        ohlc = _make_ohlc([110, 110, 110, 115, 116, 117, 95, 94, 93, 92])

        signal = baseline_cross_signal(
            ohlc,
            baseline_type="vwema",
            baseline_period=3,
            direction="below",
            lookback_days=1,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_invalid_price_column(self):
        ohlc = _make_ohlc([100, 100, 100, 95, 94, 93, 110, 111, 112, 113])

        with pytest.raises(ValueError, match="不正なprice_column"):
            baseline_cross_signal(
                ohlc,
                baseline_type="sma",
                baseline_period=3,
                direction="above",
                lookback_days=1,
                price_column="open",
            )

    def test_signal_processor_integration(self):
        ohlc = _make_ohlc([100, 100, 100, 95, 94, 93, 110, 111, 112, 113])
        base_signal = pd.Series(True, index=ohlc.index)
        signal_params = SignalParams(
            baseline_cross=BaselineCrossSignalParams(
                enabled=True,
                baseline_type="sma",
                baseline_period=3,
                direction="above",
                lookback_days=1,
            )
        )

        result = SignalProcessor().apply_entry_signals(
            base_signal=base_signal,
            ohlc_data=ohlc,
            signal_params=signal_params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert result.any()
