"""各シグナルパラメータモデルのバリデーションテスト"""

import pytest
from pydantic import ValidationError

from src.models.signals.breakout import (
    BreakoutSignalParams,
    BuyAndHoldSignalParams,
    CrossoverSignalParams,
    MABreakoutParams,
    MeanReversionSignalParams,
    PeriodBreakoutParams,
    RiskAdjustedReturnSignalParams,
)
from src.models.signals.macro import (
    BetaSignalParams,
    IndexDailyChangeSignalParams,
    IndexMACDHistogramSignalParams,
    MarginSignalParams,
)
from src.models.signals.oscillator import RSISpreadSignalParams, RSIThresholdSignalParams
from src.models.signals.sector import (
    SectorRotationPhaseParams,
    SectorStrengthRankingParams,
    SectorVolatilityRegimeParams,
)
from src.models.signals.volatility import (
    ATRSupportBreakParams,
    BollingerBandsSignalParams,
)
from src.models.signals.volume import (
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    VolumeSignalParams,
)


# ---- Crossover ----
class TestCrossoverSignalParams:
    def test_defaults(self):
        p = CrossoverSignalParams()
        assert p.type == "sma"
        assert p.direction == "golden"
        assert p.fast_period == 10
        assert p.slow_period == 30

    def test_valid_period_order(self):
        p = CrossoverSignalParams(fast_period=5, slow_period=20)
        assert p.slow_period == 20

    def test_invalid_period_order(self):
        with pytest.raises(ValidationError, match="slow_period"):
            CrossoverSignalParams(fast_period=30, slow_period=10)

    def test_equal_periods(self):
        with pytest.raises(ValidationError):
            CrossoverSignalParams(fast_period=20, slow_period=20)


# ---- PeriodBreakout ----
class TestPeriodBreakoutParams:
    def test_defaults(self):
        p = PeriodBreakoutParams()
        assert p.direction == "high"
        assert p.condition == "break"

    def test_custom(self):
        p = PeriodBreakoutParams(direction="low", condition="maintained", period=50)
        assert p.direction == "low"
        assert p.period == 50


# ---- MABreakout ----
class TestMABreakoutParams:
    def test_defaults(self):
        p = MABreakoutParams()
        assert p.period == 200
        assert p.ma_type == "sma"
        assert p.direction == "above"

    def test_ema(self):
        p = MABreakoutParams(ma_type="ema")
        assert p.ma_type == "ema"


# ---- MeanReversion ----
class TestMeanReversionSignalParams:
    def test_defaults(self):
        p = MeanReversionSignalParams()
        assert p.baseline_type == "sma"
        assert p.deviation_threshold == 0.2

    def test_deviation_threshold_zero(self):
        p = MeanReversionSignalParams(deviation_threshold=0.0)
        assert p.deviation_threshold == 0.0

    def test_deviation_threshold_max(self):
        p = MeanReversionSignalParams(deviation_threshold=1.0)
        assert p.deviation_threshold == 1.0

    def test_deviation_threshold_above_max(self):
        with pytest.raises(ValidationError):
            MeanReversionSignalParams(deviation_threshold=1.1)


# ---- BuyAndHold ----
class TestBuyAndHoldSignalParams:
    def test_defaults(self):
        p = BuyAndHoldSignalParams()
        assert p.enabled is False

    def test_enabled(self):
        p = BuyAndHoldSignalParams(enabled=True)
        assert p.enabled is True


# ---- RiskAdjustedReturn ----
class TestRiskAdjustedReturnSignalParams:
    def test_defaults(self):
        p = RiskAdjustedReturnSignalParams()
        assert p.ratio_type == "sortino"
        assert p.condition == "above"

    def test_valid_ratio_types(self):
        for rt in ["sharpe", "sortino"]:
            p = RiskAdjustedReturnSignalParams(ratio_type=rt)
            assert p.ratio_type == rt

    def test_invalid_ratio_type(self):
        with pytest.raises(ValidationError, match="ratio_type"):
            RiskAdjustedReturnSignalParams(ratio_type="calmar")

    def test_valid_conditions(self):
        for c in ["above", "below"]:
            p = RiskAdjustedReturnSignalParams(condition=c)
            assert p.condition == c

    def test_invalid_condition(self):
        with pytest.raises(ValidationError, match="condition"):
            RiskAdjustedReturnSignalParams(condition="equal")


# ---- Volume ----
class TestVolumeSignalParams:
    def test_defaults(self):
        p = VolumeSignalParams()
        assert p.direction == "surge"
        assert p.short_period == 20
        assert p.long_period == 100

    def test_valid_directions(self):
        for d in ["surge", "drop"]:
            p = VolumeSignalParams(direction=d)
            assert p.direction == d

    def test_invalid_direction(self):
        with pytest.raises(ValidationError, match="direction"):
            VolumeSignalParams(direction="flat")

    def test_period_order_invalid(self):
        with pytest.raises(ValidationError):
            VolumeSignalParams(short_period=100, long_period=50)

    def test_valid_ma_types(self):
        for mt in ["sma", "ema"]:
            p = VolumeSignalParams(ma_type=mt)
            assert p.ma_type == mt

    def test_invalid_ma_type(self):
        with pytest.raises(ValidationError, match="ma_type"):
            VolumeSignalParams(ma_type="wma")

    def test_threshold_boundaries(self):
        with pytest.raises(ValidationError):
            VolumeSignalParams(threshold=0.1)
        with pytest.raises(ValidationError):
            VolumeSignalParams(threshold=10.1)


class TestTradingValueSignalParams:
    def test_defaults(self):
        p = TradingValueSignalParams()
        assert p.direction == "above"
        assert p.period == 20

    def test_invalid_direction(self):
        with pytest.raises(ValidationError, match="direction"):
            TradingValueSignalParams(direction="equal")


class TestTradingValueRangeSignalParams:
    def test_defaults(self):
        p = TradingValueRangeSignalParams()
        assert p.min_threshold == 0.5
        assert p.max_threshold == 100.0

    def test_invalid_range(self):
        with pytest.raises(ValidationError, match="最大閾値"):
            TradingValueRangeSignalParams(min_threshold=100.0, max_threshold=50.0)

    def test_equal_thresholds(self):
        with pytest.raises(ValidationError, match="最大閾値"):
            TradingValueRangeSignalParams(min_threshold=50.0, max_threshold=50.0)


# ---- Beta ----
class TestBetaSignalParams:
    def test_defaults(self):
        p = BetaSignalParams()
        assert p.min_beta == 0.5
        assert p.max_beta == 1.5

    def test_invalid_range(self):
        with pytest.raises(ValidationError, match="β値"):
            BetaSignalParams(min_beta=1.5, max_beta=0.5)

    def test_equal_betas(self):
        with pytest.raises(ValidationError, match="β値"):
            BetaSignalParams(min_beta=1.0, max_beta=1.0)

    def test_negative_beta(self):
        p = BetaSignalParams(min_beta=-2.0, max_beta=-0.5)
        assert p.min_beta == -2.0

    def test_boundary_values(self):
        p = BetaSignalParams(min_beta=-2.0, max_beta=5.0)
        assert p.min_beta == -2.0
        assert p.max_beta == 5.0


class TestMarginSignalParams:
    def test_defaults(self):
        p = MarginSignalParams()
        assert p.lookback_period == 150
        assert p.percentile_threshold == 0.2


class TestIndexDailyChangeSignalParams:
    def test_defaults(self):
        p = IndexDailyChangeSignalParams()
        assert p.max_daily_change_pct == 1.0
        assert p.direction == "below"


class TestIndexMACDHistogramSignalParams:
    def test_defaults(self):
        p = IndexMACDHistogramSignalParams()
        assert p.fast_period == 12
        assert p.slow_period == 26
        assert p.signal_period == 9

    def test_invalid_period_order(self):
        with pytest.raises(ValidationError, match="slow_period"):
            IndexMACDHistogramSignalParams(fast_period=30, slow_period=10)


# ---- RSI ----
class TestRSIThresholdSignalParams:
    def test_defaults(self):
        p = RSIThresholdSignalParams()
        assert p.period > 0
        assert 0 < p.threshold < 100

    def test_valid_conditions(self):
        for c in ["below", "above"]:
            p = RSIThresholdSignalParams(condition=c)
            assert p.condition == c

    def test_invalid_condition(self):
        with pytest.raises(ValidationError, match="condition"):
            RSIThresholdSignalParams(condition="equal")


class TestRSISpreadSignalParams:
    def test_defaults(self):
        p = RSISpreadSignalParams()
        assert p.fast_period < p.slow_period

    def test_invalid_period_order(self):
        with pytest.raises(ValidationError):
            RSISpreadSignalParams(fast_period=30, slow_period=10)


# ---- Sector ----
class TestSectorStrengthRankingParams:
    def test_defaults(self):
        p = SectorStrengthRankingParams()
        assert 1 <= p.top_n <= 33

    def test_custom(self):
        p = SectorStrengthRankingParams(top_n=5, momentum_weight=0.8)
        assert p.top_n == 5
        assert p.momentum_weight == 0.8


class TestSectorRotationPhaseParams:
    def test_valid_directions(self):
        for d in ["leading", "weakening"]:
            p = SectorRotationPhaseParams(direction=d)
            assert p.direction == d

    def test_invalid_direction(self):
        with pytest.raises(ValidationError, match="direction"):
            SectorRotationPhaseParams(direction="stable")


class TestSectorVolatilityRegimeParams:
    def test_valid_directions(self):
        for d in ["low_vol", "high_vol"]:
            p = SectorVolatilityRegimeParams(direction=d)
            assert p.direction == d

    def test_invalid_direction(self):
        with pytest.raises(ValidationError, match="direction"):
            SectorVolatilityRegimeParams(direction="mid_vol")

    def test_spike_multiplier_boundary(self):
        with pytest.raises(ValidationError):
            SectorVolatilityRegimeParams(spike_multiplier=0.5)
        with pytest.raises(ValidationError):
            SectorVolatilityRegimeParams(spike_multiplier=5.1)


# ---- Volatility ----
class TestBollingerBandsSignalParams:
    def test_defaults(self):
        p = BollingerBandsSignalParams()
        assert p.window > 0
        assert p.alpha > 0

    def test_valid_positions(self):
        positions = [
            "below_upper", "above_lower", "above_middle",
            "below_middle", "above_upper", "below_lower",
        ]
        for pos in positions:
            p = BollingerBandsSignalParams(position=pos)
            assert p.position == pos

    def test_invalid_position(self):
        with pytest.raises(ValidationError, match="position"):
            BollingerBandsSignalParams(position="inside")


class TestATRSupportBreakParams:
    def test_defaults(self):
        p = ATRSupportBreakParams()
        assert p.direction in ["break", "recovery"]

    def test_custom(self):
        p = ATRSupportBreakParams(direction="recovery", atr_multiplier=2.0)
        assert p.direction == "recovery"
        assert p.atr_multiplier == 2.0


# ---- Breakout (legacy) ----
class TestBreakoutSignalParams:
    def test_defaults(self):
        p = BreakoutSignalParams()
        assert p.price_column == "high"
        assert p.direction == "upward"
