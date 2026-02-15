"""
Signal parameter factory for Lab.

Used by:
- lab generate (StrategyGenerator)
- lab optimize/evolve (random-add structure mode)

This module keeps the "reasonable defaults per usage (entry/exit)" in one place.
"""

from __future__ import annotations

import copy
from typing import Any, Literal, Protocol, Sequence, TypeVar

from .signal_search_space import PARAM_RANGES

UsageType = Literal["entry", "exit"]

T = TypeVar("T")


class RandomLike(Protocol[T]):
    def randint(self, a: int, b: int) -> int: ...
    def uniform(self, a: float, b: float) -> float: ...
    def choice(self, seq: Sequence[T]) -> T: ...


def build_signal_params(
    signal_name: str,
    usage_type: UsageType,
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """
    Build enabled + randomized params for a signal.

    Args:
        signal_name: signal key (e.g. "period_breakout")
        usage_type: "entry" or "exit"
        rng: random generator-like object

    Returns:
        Signal params dict with enabled=True.
    """
    base = get_default_params(signal_name, usage_type, rng)
    base = copy.deepcopy(base)
    base["enabled"] = True
    return randomize_params(signal_name, base, rng)


def get_default_params(
    signal_name: str,
    usage_type: UsageType,
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Get reasonable default params for a signal."""
    signal_defaults: dict[str, dict[str, Any]] = {
        "period_breakout": {
            "direction": "high" if usage_type == "entry" else "low",
            "condition": "break",
            "period": 200,
            "lookback_days": 10 if usage_type == "entry" else 1,
        },
        "ma_breakout": {
            "period": 200,
            "ma_type": "sma",
            "direction": "above" if usage_type == "entry" else "below",
            "lookback_days": 1,
        },
        "crossover": {
            "type": "sma",
            "direction": "golden" if usage_type == "entry" else "dead",
            "fast_period": 10,
            "slow_period": 30,
            "signal_period": 9,
            "lookback_days": 1,
        },
        "mean_reversion": {
            "baseline_type": "sma",
            "baseline_period": 25,
            "deviation_threshold": 0.2 if usage_type == "entry" else 0.0,
            "deviation_direction": "below" if usage_type == "entry" else "above",
            "recovery_price": "none" if usage_type == "entry" else "high",
            "recovery_direction": "above",
        },
        "bollinger_bands": {
            "window": 50,
            "alpha": 2.0,
            "position": "below_upper" if usage_type == "entry" else "above_upper",
        },
        "atr_support_break": {
            "direction": "recovery" if usage_type == "entry" else "break",
            "lookback_period": 20,
            "atr_multiplier": 3.0,
            "price_column": "close",
        },
        "rsi_threshold": {
            "period": 14,
            "threshold": 40.0 if usage_type == "entry" else 70.0,
            "condition": "above" if usage_type == "entry" else "below",
        },
        "rsi_spread": {
            "fast_period": 9,
            "slow_period": 14,
            "threshold": 10.0,
            "condition": "above" if usage_type == "entry" else "below",
        },
        "volume": {
            "direction": "surge" if usage_type == "entry" else "drop",
            "threshold": 1.5 if usage_type == "entry" else 0.5,
            "short_period": 50,
            "long_period": 150,
            "ma_type": "sma",
        },
        "trading_value": {
            "direction": "above",
            "period": 15,
            "threshold_value": 1.0,
        },
        "trading_value_range": {
            "period": 15,
            "min_threshold": 1.0,
            "max_threshold": 75.0,
        },
        "beta": {
            "lookback_period": 50,
            "min_beta": 0.2,
            "max_beta": 3.0,
        },
        "margin": {
            "lookback_period": 150,
            "percentile_threshold": 0.2,
        },
        "index_daily_change": {
            "max_daily_change_pct": 1.0,
            "direction": "below" if usage_type == "entry" else "above",
        },
        "index_macd_histogram": {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
            "direction": "positive" if usage_type == "entry" else "negative",
        },
        "fundamental": _get_default_fundamental_params(usage_type, rng),
    }

    return signal_defaults.get(signal_name, {})


def randomize_params(
    signal_name: str,
    params: dict[str, Any],
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Randomize numeric params within PARAM_RANGES."""
    if signal_name == "fundamental":
        return _randomize_fundamental_params(params, rng)

    randomized = params.copy()
    ranges = PARAM_RANGES.get(signal_name, {})

    for param_name in list(params.keys()):
        if param_name == "enabled":
            continue
        if param_name not in ranges:
            continue

        min_val, max_val, param_type = ranges[param_name]
        if param_type == "int":
            randomized[param_name] = rng.randint(int(min_val), int(max_val))
        else:
            randomized[param_name] = rng.uniform(min_val, max_val)

    return randomized


def _get_default_fundamental_params(
    usage_type: UsageType,
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Build nested params for fundamental."""
    if usage_type != "entry":
        return {
            "use_adjusted": True,
            "period_type": "FY",
            "per": {
                "enabled": False,
                "threshold": 20.0,
                "condition": "below",
                "exclude_negative": True,
            },
        }

    options: dict[str, dict[str, Any]] = {
        "per": {
            "enabled": False,
            "threshold": 15.0,
            "condition": "below",
            "exclude_negative": True,
        },
        "pbr": {
            "enabled": False,
            "threshold": 1.2,
            "condition": "below",
            "exclude_negative": True,
        },
        "peg_ratio": {
            "enabled": False,
            "threshold": 1.2,
            "condition": "below",
        },
        "forward_eps_growth": {
            "enabled": False,
            "threshold": 0.1,
            "condition": "above",
        },
        "eps_growth": {
            "enabled": False,
            "threshold": 0.1,
            "periods": 1,
            "condition": "above",
        },
        "roe": {
            "enabled": False,
            "threshold": 10.0,
            "condition": "above",
        },
        "roa": {
            "enabled": False,
            "threshold": 5.0,
            "condition": "above",
        },
        "operating_margin": {
            "enabled": False,
            "threshold": 10.0,
            "condition": "above",
        },
        "dividend_yield": {
            "enabled": False,
            "threshold": 2.0,
            "condition": "above",
        },
        "dividend_per_share_growth": {
            "enabled": False,
            "threshold": 0.1,
            "periods": 1,
            "condition": "above",
        },
        "cfo_yield_growth": {
            "enabled": False,
            "threshold": 0.1,
            "periods": 1,
            "condition": "above",
            "use_floating_shares": True,
        },
        "simple_fcf_yield_growth": {
            "enabled": False,
            "threshold": 0.1,
            "periods": 1,
            "condition": "above",
            "use_floating_shares": True,
        },
        "market_cap": {
            "enabled": False,
            "threshold": 300.0,
            "condition": "above",
            "use_floating_shares": True,
        },
    }

    selected_key = rng.choice(list(options.keys()))
    options[selected_key]["enabled"] = True

    return {
        "use_adjusted": True,
        "period_type": "FY",
        **options,
    }


def _randomize_fundamental_params(
    params: dict[str, Any],
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Randomize thresholds inside nested fundamental params."""
    randomized = copy.deepcopy(params)

    ranges: dict[str, dict[str, tuple[float, float, str]]] = {
        "per": {"threshold": (5.0, 40.0, "float")},
        "pbr": {"threshold": (0.3, 5.0, "float")},
        "peg_ratio": {"threshold": (0.3, 3.0, "float")},
        "forward_eps_growth": {"threshold": (0.02, 0.5, "float")},
        "eps_growth": {"threshold": (0.02, 0.5, "float"), "periods": (1, 8, "int")},
        "roe": {"threshold": (3.0, 25.0, "float")},
        "roa": {"threshold": (2.0, 15.0, "float")},
        "operating_margin": {"threshold": (3.0, 30.0, "float")},
        "dividend_yield": {"threshold": (0.5, 8.0, "float")},
        "dividend_per_share_growth": {
            "threshold": (0.02, 0.5, "float"),
            "periods": (1, 8, "int"),
        },
        "cfo_yield_growth": {"threshold": (0.02, 0.5, "float"), "periods": (1, 8, "int")},
        "simple_fcf_yield_growth": {
            "threshold": (0.02, 0.5, "float"),
            "periods": (1, 8, "int"),
        },
        "market_cap": {"threshold": (50.0, 5000.0, "float")},
    }

    for field_name, field_ranges in ranges.items():
        field_value = randomized.get(field_name)
        if not isinstance(field_value, dict) or not field_value.get("enabled"):
            continue

        for param_name, (min_val, max_val, param_type) in field_ranges.items():
            if param_name not in field_value:
                continue

            if param_type == "int":
                field_value[param_name] = rng.randint(int(min_val), int(max_val))
            else:
                field_value[param_name] = rng.uniform(min_val, max_val)

    return randomized
