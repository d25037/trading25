"""
統合シグナル生成システム

エントリー・エグジットシグナル生成のための統一アーキテクチャ
フィルター・トリガーの概念を廃止し、シグナル概念に統一
"""

# Volume signals
from .volume import volume_signal


# Fundamental signals
from .fundamental import (
    is_undervalued_by_per,
    is_undervalued_by_pbr,
    is_growing_eps,
    is_high_roe,
    is_undervalued_growth_by_peg,
    is_expected_growth_eps,
)

# Volatility signals
from .volatility import (
    volatility_relative_signal,
    rolling_volatility_signal,
    volatility_percentile_signal,
    low_volatility_stock_screen_signal,
)

# Performance signals
from .performance import (
    relative_performance_signal,
    create_relative_performance_signal_from_db,
    multi_timeframe_relative_performance_signal,
)

# Price action signals: period_breakout_signalに統合済み（horizontal_price_action.py削除）

# Margin signals
from .margin import margin_balance_percentile_signal

# Beta signals
from .beta import beta_range_signal

# Buy and Hold signals
from .buy_and_hold import generate_buy_and_hold_signals

# Generic signals (新アーキテクチャ: YAML制御用)
from .crossover import crossover_signal
from .breakout import period_breakout_signal, ma_breakout_signal
from .mean_reversion import deviation_signal, price_recovery_signal
from .risk_adjusted import risk_adjusted_return_signal

# Sector signals
from .sector import (
    get_sector_index_code,
    get_sector_stocks,
    get_all_sectors,
    create_sector_signal_by_index_performance,
    validate_sector_name,
    get_sector_correlation_matrix,
)

# Sector strength signals (new)
from .sector_strength import (
    sector_strength_ranking_signal,
    sector_rotation_phase_signal,
    sector_volatility_regime_signal,
)

# Signal processor
from .processor import SignalProcessor

__all__ = [
    # Volume
    "volume_signal",
    # Fundamental
    "is_undervalued_by_per",
    "is_undervalued_by_pbr",
    "is_growing_eps",
    "is_high_roe",
    "is_undervalued_growth_by_peg",
    "is_expected_growth_eps",
    # Volatility
    "volatility_relative_signal",
    "rolling_volatility_signal",
    "volatility_percentile_signal",
    "low_volatility_stock_screen_signal",
    # Performance
    "relative_performance_signal",
    "create_relative_performance_signal_from_db",
    "multi_timeframe_relative_performance_signal",
    # Price action: period_breakout_signalに統合済み
    # Margin
    "margin_balance_percentile_signal",
    # Beta
    "beta_range_signal",
    # Buy and Hold
    "generate_buy_and_hold_signals",
    # Generic signals (YAML制御用)
    "crossover_signal",
    "period_breakout_signal",
    "ma_breakout_signal",
    "deviation_signal",
    "price_recovery_signal",
    "risk_adjusted_return_signal",
    # Sector
    "get_sector_index_code",
    "get_sector_stocks",
    "get_all_sectors",
    "create_sector_signal_by_index_performance",
    "validate_sector_name",
    "get_sector_correlation_matrix",
    # Sector strength
    "sector_strength_ranking_signal",
    "sector_rotation_phase_signal",
    "sector_volatility_regime_signal",
    # Processor
    "SignalProcessor",
]
