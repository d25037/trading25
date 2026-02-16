"""
Lab signal catalog (single source of truth for agent-side signal metadata).

This module builds Lab metadata from two runtime sources:
- Runtime signal availability: ``src.strategies.signals.registry.SIGNAL_REGISTRY``
- Parameter schema constraints: ``src.models.signals.SignalParams``

Lab固有の制約（相互排他・推奨組み合わせ・カテゴリ互換・usage補正）は
最小限のオーバーレイとして定義する。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast, get_args, get_origin

from pydantic import BaseModel

from src.models.signals import SignalParams
from src.strategies.signals.registry import SIGNAL_REGISTRY

from .models import SignalCategory, SignalConstraints

ParamType = Literal["int", "float"]
ParamRange = tuple[float, float, ParamType]


# Keep the historical order for stable random-add behavior in tests/CLI output.
LEGACY_SIGNAL_ORDER: list[str] = [
    "period_breakout",
    "ma_breakout",
    "crossover",
    "mean_reversion",
    "bollinger_bands",
    "atr_support_break",
    "rsi_threshold",
    "rsi_spread",
    "volume",
    "trading_value",
    "trading_value_range",
    "beta",
    "margin",
    "index_daily_change",
    "index_macd_histogram",
    "fundamental",
]


# Lab-specific usage guardrails.
SIGNAL_USAGE_OVERRIDES: dict[str, Literal["entry", "exit", "both"]] = {
    "beta": "entry",
    "margin": "entry",
    "fundamental": "entry",
}


# Registry has no "trend" category. Keep legacy mapping compatibility.
SIGNAL_CATEGORY_OVERRIDES: dict[str, SignalCategory] = {
    "ma_breakout": "trend",
    "crossover": "trend",
    "retracement": "trend",
}


# Lab heuristics (not represented by runtime registry).
SIGNAL_RELATION_OVERRIDES: dict[str, dict[str, list[str]]] = {
    "period_breakout": {
        "recommended_with": ["volume", "bollinger_bands"],
    },
    "ma_breakout": {
        "recommended_with": ["volume", "rsi_threshold"],
    },
    "crossover": {
        "recommended_with": ["volume"],
    },
    "mean_reversion": {
        "mutually_exclusive": ["period_breakout"],
    },
    "bollinger_bands": {
        "recommended_with": ["rsi_threshold"],
    },
    "rsi_threshold": {
        "recommended_with": ["volume"],
    },
    "rsi_spread": {
        "mutually_exclusive": ["rsi_threshold"],
    },
    "trading_value": {
        "mutually_exclusive": ["trading_value_range"],
    },
    "trading_value_range": {
        "mutually_exclusive": ["trading_value"],
    },
    "beta": {
        "recommended_with": ["volume"],
    },
}


# Keep established search space for already-supported signals.
# New signals are generated from SignalParams constraints.
LEGACY_PARAM_RANGE_OVERRIDES: dict[str, dict[str, ParamRange]] = {
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


_VALID_CATEGORIES: set[str] = set(cast(tuple[str, ...], get_args(SignalCategory)))


@dataclass
class _SignalAggregate:
    categories: set[str] = field(default_factory=set)
    required_data: set[str] = field(default_factory=set)
    exit_enabled: bool = False


def _aggregate_registry() -> dict[str, _SignalAggregate]:
    aggregates: dict[str, _SignalAggregate] = {}
    signal_fields = SignalParams.model_fields

    for signal_def in SIGNAL_REGISTRY:
        signal_name = signal_def.param_key.split(".", 1)[0]
        if signal_name not in signal_fields:
            continue

        agg = aggregates.setdefault(signal_name, _SignalAggregate())
        agg.categories.add(signal_def.category)
        agg.required_data.update(signal_def.data_requirements)
        if not signal_def.exit_disabled:
            agg.exit_enabled = True

    return aggregates


def _sort_signal_names(names: set[str]) -> list[str]:
    order_index = {name: i for i, name in enumerate(LEGACY_SIGNAL_ORDER)}
    default_rank = len(order_index)
    return sorted(names, key=lambda name: (order_index.get(name, default_rank), name))


def _normalize_category_name(category: str) -> SignalCategory | None:
    if category in _VALID_CATEGORIES:
        return cast(SignalCategory, category)
    return None


def _resolve_category(signal_name: str, categories: set[str]) -> SignalCategory:
    override = SIGNAL_CATEGORY_OVERRIDES.get(signal_name)
    if override is not None:
        return override

    for category in sorted(categories):
        normalized = _normalize_category_name(category)
        if normalized is not None:
            return normalized

    return "breakout"


def _resolve_usage(signal_name: str, exit_enabled: bool) -> Literal["entry", "exit", "both"]:
    override = SIGNAL_USAGE_OVERRIDES.get(signal_name)
    if override is not None:
        return override
    return "both" if exit_enabled else "entry"


def _build_available_signals() -> list[SignalConstraints]:
    aggregates = _aggregate_registry()
    available_signals: list[SignalConstraints] = []

    for signal_name in _sort_signal_names(set(aggregates.keys())):
        agg = aggregates[signal_name]
        relation = SIGNAL_RELATION_OVERRIDES.get(signal_name, {})
        available_signals.append(
            SignalConstraints(
                name=signal_name,
                required_data=sorted(agg.required_data),
                mutually_exclusive=relation.get("mutually_exclusive", []),
                recommended_with=relation.get("recommended_with", []),
                usage=_resolve_usage(signal_name, agg.exit_enabled),
                category=_resolve_category(signal_name, agg.categories),
            )
        )

    return available_signals


def _extract_model_class(annotation: Any) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation

    origin = get_origin(annotation)
    if origin is None:
        return None

    for arg in get_args(annotation):
        nested = _extract_model_class(arg)
        if nested is not None:
            return nested
    return None


def _extract_param_type(annotation: Any) -> ParamType | None:
    if annotation is bool:
        return None
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"

    origin = get_origin(annotation)
    if origin is None:
        return None

    for arg in get_args(annotation):
        if arg is type(None):
            continue
        param_type = _extract_param_type(arg)
        if param_type is not None:
            return param_type
    return None


def _read_numeric_attr(metadata: Any, attr: str) -> float | None:
    value = getattr(metadata, attr, None)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _extract_range(field_info: Any, param_type: ParamType) -> ParamRange | None:
    lower: float | None = None
    upper: float | None = None

    for metadata in field_info.metadata:
        ge = _read_numeric_attr(metadata, "ge")
        if ge is not None:
            lower = ge if lower is None else max(lower, ge)

        gt = _read_numeric_attr(metadata, "gt")
        if gt is not None:
            gt_value = float(int(gt) + 1) if param_type == "int" else gt
            lower = gt_value if lower is None else max(lower, gt_value)

        le = _read_numeric_attr(metadata, "le")
        if le is not None:
            upper = le if upper is None else min(upper, le)

        lt = _read_numeric_attr(metadata, "lt")
        if lt is not None:
            lt_value = float(int(lt) - 1) if param_type == "int" else lt
            upper = lt_value if upper is None else min(upper, lt_value)

    if lower is None or upper is None:
        return None

    if param_type == "int":
        int_lower = int(lower)
        int_upper = int(upper)
        if int_lower > int_upper:
            return None
        return (float(int_lower), float(int_upper), "int")

    if lower > upper:
        return None
    return (lower, upper, "float")


def _collect_numeric_ranges(
    model_cls: type[BaseModel],
    result: dict[str, ParamRange],
    prefix: str = "",
) -> None:
    for field_name, field_info in model_cls.model_fields.items():
        param_name = f"{prefix}.{field_name}" if prefix else field_name

        nested_model = _extract_model_class(field_info.annotation)
        if nested_model is not None:
            _collect_numeric_ranges(nested_model, result, param_name)
            continue

        param_type = _extract_param_type(field_info.annotation)
        if param_type is None:
            continue

        extracted = _extract_range(field_info, param_type)
        if extracted is None:
            continue

        result[param_name] = extracted


def _build_param_ranges() -> dict[str, dict[str, ParamRange]]:
    param_ranges: dict[str, dict[str, ParamRange]] = {}

    for signal_name in SIGNAL_CONSTRAINTS_MAP:
        field_info = SignalParams.model_fields.get(signal_name)
        if field_info is None:
            continue

        model_cls = _extract_model_class(field_info.annotation)
        if model_cls is None:
            continue

        signal_ranges: dict[str, ParamRange] = {}
        _collect_numeric_ranges(model_cls, signal_ranges)

        legacy_override = LEGACY_PARAM_RANGE_OVERRIDES.get(signal_name, {})
        signal_ranges.update(legacy_override)

        if signal_ranges:
            param_ranges[signal_name] = signal_ranges

    return param_ranges


AVAILABLE_SIGNALS: list[SignalConstraints] = _build_available_signals()
SIGNAL_CONSTRAINTS_MAP: dict[str, SignalConstraints] = {
    signal.name: signal for signal in AVAILABLE_SIGNALS
}
SIGNAL_CATEGORY_MAP: dict[str, SignalCategory] = {
    signal.name: signal.category for signal in AVAILABLE_SIGNALS
}
PARAM_RANGES: dict[str, dict[str, ParamRange]] = _build_param_ranges()

