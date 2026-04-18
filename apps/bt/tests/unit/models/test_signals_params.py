"""各シグナルパラメータモデルのバリデーションテスト"""

import pytest
from pydantic import ValidationError

from src.shared.models.signals.breakout import (
    BaselineCrossSignalParams,
    BaselineDeviationSignalParams,
    BaselinePositionSignalParams,
    BreakoutSignalParams,
    BuyAndHoldSignalParams,
    CrossoverSignalParams,
    PeriodExtremaBreakSignalParams,
    PeriodExtremaPositionSignalParams,
    RiskAdjustedReturnSignalParams,
)
from src.shared.models.signals.macro import (
    BetaSignalParams,
    IndexDailyChangeSignalParams,
    IndexMACDHistogramSignalParams,
    IndexOpenGapRegimeSignalParams,
    MarginSignalParams,
    UniverseRankBucketSignalParams,
)
from src.shared.models.signals.oscillator import RSISpreadSignalParams, RSIThresholdSignalParams
from src.shared.models.signals.sector import (
    SectorRotationPhaseParams,
    SectorStrengthRankingParams,
    SectorVolatilityRegimeParams,
)
from src.shared.models.signals.volatility import (
    ATRSupportCrossParams,
    ATRSupportPositionParams,
    BollingerCrossSignalParams,
    BollingerPositionSignalParams,
    VolatilityPercentileSignalParams,
)
from src.shared.models.signals.volume import (
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    VolumeRatioAboveSignalParams,
    VolumeRatioBelowSignalParams,
)


class TestCrossoverSignalParams:
    def test_defaults(self) -> None:
        p = CrossoverSignalParams()
        assert p.type == "sma"
        assert p.direction == "golden"
        assert p.fast_period == 10
        assert p.slow_period == 30

    def test_invalid_period_order(self) -> None:
        with pytest.raises(ValidationError, match="slow_period"):
            CrossoverSignalParams(fast_period=30, slow_period=10)


class TestPeriodExtremaParams:
    def test_break_defaults(self) -> None:
        p = PeriodExtremaBreakSignalParams()
        assert p.direction == "high"
        assert p.lookback_days == 1

    def test_position_defaults(self) -> None:
        p = PeriodExtremaPositionSignalParams()
        assert p.direction == "high"
        assert p.state == "at_extrema"


class TestBaselineSignalParams:
    def test_cross_defaults(self) -> None:
        p = BaselineCrossSignalParams()
        assert p.baseline_period == 200
        assert p.baseline_type == "sma"
        assert p.price_column == "close"

    def test_deviation_boundaries(self) -> None:
        assert BaselineDeviationSignalParams(deviation_threshold=0.0).deviation_threshold == 0.0
        with pytest.raises(ValidationError):
            BaselineDeviationSignalParams(deviation_threshold=1.1)

    def test_position_custom(self) -> None:
        p = BaselinePositionSignalParams(direction="below", price_column="low")
        assert p.direction == "below"
        assert p.price_column == "low"


class TestLegacyBreakoutSignalParams:
    def test_defaults(self) -> None:
        p = BreakoutSignalParams()
        assert p.price_column == "high"
        assert p.direction == "upward"


class TestBuyAndHoldSignalParams:
    def test_enabled_flag(self) -> None:
        assert BuyAndHoldSignalParams().enabled is False
        assert BuyAndHoldSignalParams(enabled=True).enabled is True


class TestRiskAdjustedReturnSignalParams:
    def test_defaults(self) -> None:
        p = RiskAdjustedReturnSignalParams()
        assert p.ratio_type == "sortino"
        assert p.condition == "above"
        assert p.margin_min is None
        assert p.margin_max is None

    def test_invalid_ratio_type(self) -> None:
        with pytest.raises(ValidationError, match="ratio_type"):
            RiskAdjustedReturnSignalParams(ratio_type="calmar")

    def test_invalid_margin_order(self) -> None:
        with pytest.raises(ValidationError, match="margin_max"):
            RiskAdjustedReturnSignalParams(margin_min=0.5, margin_max=0.2)


class TestVolumeRatioSignalParams:
    def test_above_defaults(self) -> None:
        p = VolumeRatioAboveSignalParams()
        assert p.ratio_threshold == 1.5
        assert p.ma_type == "sma"

    def test_below_defaults(self) -> None:
        p = VolumeRatioBelowSignalParams()
        assert p.ratio_threshold == 0.7

    def test_invalid_period_order(self) -> None:
        with pytest.raises(ValidationError):
            VolumeRatioAboveSignalParams(short_period=100, long_period=50)

    def test_invalid_ma_type(self) -> None:
        with pytest.raises(ValidationError, match="ma_type"):
            VolumeRatioAboveSignalParams(ma_type="wma")


class TestTradingValueSignalParams:
    def test_defaults(self) -> None:
        p = TradingValueSignalParams()
        assert p.direction == "above"
        assert p.period == 20

    def test_invalid_direction(self) -> None:
        with pytest.raises(ValidationError, match="direction"):
            TradingValueSignalParams(direction="equal")


class TestTradingValueRangeSignalParams:
    def test_defaults(self) -> None:
        p = TradingValueRangeSignalParams()
        assert p.min_threshold == 0.5
        assert p.max_threshold == 100.0

    def test_invalid_range(self) -> None:
        with pytest.raises(ValidationError, match="最大閾値"):
            TradingValueRangeSignalParams(min_threshold=100.0, max_threshold=50.0)


class TestMacroSignalParams:
    def test_beta_defaults(self) -> None:
        p = BetaSignalParams()
        assert p.min_beta == 0.5
        assert p.max_beta == 1.5

    def test_beta_invalid_range(self) -> None:
        with pytest.raises(ValidationError, match="β値"):
            BetaSignalParams(min_beta=1.5, max_beta=0.5)

    def test_margin_defaults(self) -> None:
        p = MarginSignalParams()
        assert p.lookback_period == 150
        assert p.percentile_threshold == 0.2

    def test_index_daily_change_defaults(self) -> None:
        p = IndexDailyChangeSignalParams()
        assert p.max_daily_change_pct == 1.0
        assert p.direction == "below"

    def test_index_macd_invalid_period_order(self) -> None:
        with pytest.raises(ValidationError, match="slow_period"):
            IndexMACDHistogramSignalParams(fast_period=30, slow_period=10)

    def test_oracle_gap_threshold_order(self) -> None:
        with pytest.raises(ValidationError, match="gap_threshold_2_pct"):
            IndexOpenGapRegimeSignalParams(
                gap_threshold_1_pct=2.0,
                gap_threshold_2_pct=1.0,
            )

    def test_universe_rank_bucket_defaults(self) -> None:
        p = UniverseRankBucketSignalParams()
        assert p.price_sma_period == 50
        assert p.price_bucket == "q1"
        assert p.min_constituents == 10


class TestOscillatorSignalParams:
    def test_rsi_threshold_defaults(self) -> None:
        p = RSIThresholdSignalParams()
        assert p.period == 14
        assert p.condition == "below"

    def test_rsi_threshold_invalid_condition(self) -> None:
        with pytest.raises(ValidationError, match="condition"):
            RSIThresholdSignalParams(condition="equal")

    def test_rsi_spread_invalid_period_order(self) -> None:
        with pytest.raises(ValidationError, match="slow_period"):
            RSISpreadSignalParams(fast_period=30, slow_period=10)


class TestVolatilitySignalParams:
    def test_volatility_percentile_defaults(self) -> None:
        p = VolatilityPercentileSignalParams()
        assert p.window == 20
        assert p.lookback == 252
        assert p.percentile == 50.0

    def test_bollinger_position_defaults(self) -> None:
        p = BollingerPositionSignalParams()
        assert p.level == "upper"
        assert p.direction == "below"

    def test_bollinger_invalid_level(self) -> None:
        with pytest.raises(ValidationError, match="level"):
            BollingerPositionSignalParams(level="outer")

    def test_bollinger_cross_defaults(self) -> None:
        p = BollingerCrossSignalParams()
        assert p.lookback_days == 1

    def test_atr_support_position_defaults(self) -> None:
        p = ATRSupportPositionParams()
        assert p.direction == "below"
        assert p.price_column == "close"

    def test_atr_support_cross_defaults(self) -> None:
        p = ATRSupportCrossParams()
        assert p.lookback_days == 1


class TestSectorSignalParams:
    def test_sector_strength_defaults(self) -> None:
        p = SectorStrengthRankingParams()
        assert 1 <= p.top_n <= 33

    def test_sector_rotation_valid_direction(self) -> None:
        p = SectorRotationPhaseParams(direction="leading")
        assert p.direction == "leading"

    def test_sector_volatility_invalid_spike_multiplier(self) -> None:
        with pytest.raises(ValidationError):
            SectorVolatilityRegimeParams(spike_multiplier=0.5)
