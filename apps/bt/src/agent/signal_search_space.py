"""
Signal search space definitions for Lab (optimize/evolve/generate).

This module centralizes:
- Numeric parameter ranges (used by GA mutation and Optuna sampling)
- Parameters treated as categorical (excluded from numeric mutation/sampling)
"""

from __future__ import annotations

from typing import Literal

ParamType = Literal["int", "float"]


# Parameters that should be treated as categorical knobs and excluded from
# numeric mutation/sampling.
CATEGORICAL_PARAMS: set[str] = {
    "enabled",
    "direction",
    "condition",
    "type",
    "ma_type",
    "position",
    "baseline_type",
    "recovery_price",
    "recovery_direction",
    "deviation_direction",
    "price_column",
}


# Numeric parameter ranges (min, max, type).
PARAM_RANGES: dict[str, dict[str, tuple[float, float, ParamType]]] = {
    "period_breakout": {
        "period": (20, 500, "int"),
        "lookback_days": (1, 30, "int"),
    },
    "ma_breakout": {
        "period": (20, 500, "int"),
        "lookback_days": (1, 30, "int"),
    },
    "crossover": {
        "fast_period": (5, 50, "int"),
        "slow_period": (20, 200, "int"),
        "signal_period": (5, 20, "int"),
        "lookback_days": (1, 10, "int"),
    },
    "mean_reversion": {
        "baseline_period": (10, 100, "int"),
        "deviation_threshold": (0.05, 0.5, "float"),
    },
    "bollinger_bands": {
        "window": (10, 100, "int"),
        "alpha": (1.0, 4.0, "float"),
    },
    "atr_support_break": {
        "lookback_period": (10, 100, "int"),
        "atr_multiplier": (1.0, 10.0, "float"),
    },
    "rsi_threshold": {
        "period": (5, 50, "int"),
        "threshold": (10.0, 90.0, "float"),
    },
    "rsi_spread": {
        "fast_period": (5, 20, "int"),
        "slow_period": (10, 50, "int"),
        "threshold": (5.0, 30.0, "float"),
    },
    "volume": {
        "threshold": (0.3, 3.0, "float"),
        "short_period": (10, 100, "int"),
        "long_period": (50, 300, "int"),
    },
    "trading_value": {
        "period": (5, 50, "int"),
        "threshold_value": (0.1, 100.0, "float"),
    },
    "trading_value_range": {
        "period": (5, 50, "int"),
        "min_threshold": (0.1, 50.0, "float"),
        "max_threshold": (10.0, 500.0, "float"),
    },
    "beta": {
        "lookback_period": (20, 200, "int"),
        "min_beta": (-1.0, 2.0, "float"),
        "max_beta": (0.5, 5.0, "float"),
    },
    "margin": {
        "lookback_period": (50, 300, "int"),
        "percentile_threshold": (0.1, 0.9, "float"),
    },
    "index_daily_change": {
        "max_daily_change_pct": (0.5, 3.0, "float"),
    },
    "index_macd_histogram": {
        "fast_period": (5, 20, "int"),
        "slow_period": (15, 50, "int"),
        "signal_period": (5, 15, "int"),
    },
}

