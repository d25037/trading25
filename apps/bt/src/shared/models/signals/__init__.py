"""
シグナルモデルパッケージ

後方互換性のため、全クラスを再エクスポート
"""

from .base import BaseSignalParams, Signals, _validate_condition_above_below, _validate_period_order
from .breakout import (
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
from .composite import SignalParams
from .fundamental import FundamentalSignalParams
from .sector import (
    SectorRotationPhaseParams,
    SectorStrengthRankingParams,
    SectorVolatilityRegimeParams,
)
from .macro import (
    BetaSignalParams,
    IndexDailyChangeSignalParams,
    IndexOpenGapRegimeSignalParams,
    IndexMACDHistogramSignalParams,
    MarginSignalParams,
)
from .oscillator import RSISpreadSignalParams, RSIThresholdSignalParams
from .trend import (
    RetracementCrossSignalParams,
    RetracementPositionSignalParams,
    TrendSignalParams,
)
from .volatility import (
    ATRSupportCrossParams,
    ATRSupportPositionParams,
    BollingerCrossSignalParams,
    BollingerPositionSignalParams,
    VolatilityPercentileSignalParams,
)
from .volume import (
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    VolumeRatioAboveSignalParams,
    VolumeRatioBelowSignalParams,
)

__all__ = [
    # base
    "_validate_condition_above_below",
    "_validate_period_order",
    "BaseSignalParams",
    "Signals",
    # volume
    "TradingValueRangeSignalParams",
    "TradingValueSignalParams",
    "VolumeRatioAboveSignalParams",
    "VolumeRatioBelowSignalParams",
    # trend
    "RetracementCrossSignalParams",
    "RetracementPositionSignalParams",
    "TrendSignalParams",
    # volatility
    "ATRSupportCrossParams",
    "ATRSupportPositionParams",
    "BollingerCrossSignalParams",
    "BollingerPositionSignalParams",
    "VolatilityPercentileSignalParams",
    # oscillator
    "RSISpreadSignalParams",
    "RSIThresholdSignalParams",
    # macro
    "BetaSignalParams",
    "IndexDailyChangeSignalParams",
    "IndexMACDHistogramSignalParams",
    "IndexOpenGapRegimeSignalParams",
    "MarginSignalParams",
    # fundamental
    "FundamentalSignalParams",
    # breakout
    "BaselineCrossSignalParams",
    "BaselineDeviationSignalParams",
    "BaselinePositionSignalParams",
    "BreakoutSignalParams",
    "BuyAndHoldSignalParams",
    "CrossoverSignalParams",
    "PeriodExtremaBreakSignalParams",
    "PeriodExtremaPositionSignalParams",
    "RiskAdjustedReturnSignalParams",
    # sector
    "SectorRotationPhaseParams",
    "SectorStrengthRankingParams",
    "SectorVolatilityRegimeParams",
    # composite
    "SignalParams",
]
