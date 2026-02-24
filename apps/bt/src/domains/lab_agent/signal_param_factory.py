"""
Signal parameter factory for Lab.

Used by:
- lab generate (StrategyGenerator)
- lab optimize/evolve (random-add structure mode)

This module keeps the "reasonable defaults per usage (entry/exit)" in one place.
"""

from __future__ import annotations

import copy
from typing import Any, Literal, Protocol, Sequence, TypeVar, cast

from src.shared.models.signals import SignalParams

from .signal_search_space import PARAM_RANGES, ParamType

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

    if signal_name in signal_defaults:
        return signal_defaults[signal_name]

    return _get_signal_model_defaults(signal_name)


def randomize_params(
    signal_name: str,
    params: dict[str, Any],
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Randomize numeric params within PARAM_RANGES."""
    if signal_name == "fundamental":
        return _randomize_fundamental_params(params, rng)

    randomized = copy.deepcopy(params)
    ranges = PARAM_RANGES.get(signal_name, {})
    if not ranges:
        return randomized

    _randomize_nested_params(randomized, ranges, rng)
    return randomized


def _randomize_nested_params(
    params: dict[str, Any],
    ranges: dict[str, tuple[float, float, ParamType]],
    rng: RandomLike[Any],
    prefix: str = "",
    enabled_gated: bool = False,
) -> None:
    for key, value in params.items():
        if key == "enabled":
            continue

        param_name = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            if enabled_gated and "enabled" in value and not bool(value["enabled"]):
                continue
            _randomize_nested_params(
                value,
                ranges,
                rng,
                prefix=param_name,
                enabled_gated=enabled_gated,
            )
            continue

        if param_name not in ranges:
            continue

        min_val, max_val, param_type = ranges[param_name]
        if param_type == "int":
            params[key] = rng.randint(int(min_val), int(max_val))
        else:
            params[key] = rng.uniform(min_val, max_val)


def _get_signal_model_defaults(signal_name: str) -> dict[str, Any]:
    """SignalParams からデフォルト値を取得（未知シグナル向けフォールバック）。"""
    field_info = SignalParams.model_fields.get(signal_name)
    if field_info is None:
        return {}

    default_value = field_info.get_default(call_default_factory=True)
    if default_value is None:
        return {}
    if isinstance(default_value, dict):
        return copy.deepcopy(default_value)
    if hasattr(default_value, "model_dump"):
        return cast(dict[str, Any], default_value.model_dump())
    return {}


def _get_default_fundamental_params(
    usage_type: UsageType,
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Build nested params for fundamental."""
    defaults = _get_signal_model_defaults("fundamental")
    if not defaults:
        return {}

    child_keys = _list_enable_children(defaults)
    for key in child_keys:
        child = defaults.get(key)
        if isinstance(child, dict):
            child["enabled"] = False

    if usage_type == "entry" and child_keys:
        selected_key = rng.choice(child_keys)
        selected = defaults.get(selected_key)
        if isinstance(selected, dict):
            selected["enabled"] = True

    return defaults


def _randomize_fundamental_params(
    params: dict[str, Any],
    rng: RandomLike[Any],
) -> dict[str, Any]:
    """Randomize thresholds inside nested fundamental params."""
    randomized = copy.deepcopy(params)
    ranges = PARAM_RANGES.get("fundamental", {})
    _randomize_nested_params(randomized, ranges, rng, enabled_gated=True)
    return randomized


def _list_enable_children(params: dict[str, Any]) -> list[str]:
    children: list[str] = []
    for key, value in params.items():
        if isinstance(value, dict) and "enabled" in value:
            children.append(key)
    return children
