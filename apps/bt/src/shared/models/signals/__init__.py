"""
シグナルモデルパッケージ

後方互換性のため、全クラスを再エクスポート
"""

from .base import BaseSignalParams, Signals, _validate_condition_above_below, _validate_period_order
from .breakout import (
    BreakoutSignalParams,
    BuyAndHoldSignalParams,
    CrossoverSignalParams,
    MABreakoutParams,
    MeanReversionSignalParams,
    PeriodBreakoutParams,
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
    IndexMACDHistogramSignalParams,
    MarginSignalParams,
)
from .oscillator import RSISpreadSignalParams, RSIThresholdSignalParams
from .trend import RetracementSignalParams, TrendSignalParams
from .volatility import (
    ATRSupportBreakParams,
    BollingerBandsSignalParams,
    VolatilitySignalParams,
)
from .volume import (
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    VolumeSignalParams,
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
    "VolumeSignalParams",
    # trend
    "RetracementSignalParams",
    "TrendSignalParams",
    # volatility
    "ATRSupportBreakParams",
    "BollingerBandsSignalParams",
    "VolatilitySignalParams",
    # oscillator
    "RSISpreadSignalParams",
    "RSIThresholdSignalParams",
    # macro
    "BetaSignalParams",
    "IndexDailyChangeSignalParams",
    "IndexMACDHistogramSignalParams",
    "MarginSignalParams",
    # fundamental
    "FundamentalSignalParams",
    # breakout
    "BreakoutSignalParams",
    "BuyAndHoldSignalParams",
    "CrossoverSignalParams",
    "MABreakoutParams",
    "MeanReversionSignalParams",
    "PeriodBreakoutParams",
    "RiskAdjustedReturnSignalParams",
    # sector
    "SectorRotationPhaseParams",
    "SectorStrengthRankingParams",
    "SectorVolatilityRegimeParams",
    # composite
    "SignalParams",
]
