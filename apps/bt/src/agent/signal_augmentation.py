"""
Helpers to mutate strategy *structure* (signal sets) for Lab.

Current use case:
- Support "random_add" mode in lab optimize/evolve:
  keep the base signals and randomly add extra signals.
"""

from __future__ import annotations

import copy
from typing import Any, Literal

from loguru import logger

from .models import SignalCategory, SignalConstraints, StrategyCandidate
from .signal_param_factory import RandomLike, UsageType, build_signal_params
from .strategy_generator import AVAILABLE_SIGNALS, SIGNAL_CONSTRAINTS_MAP


def _is_enabled_signal_params(value: Any) -> bool:
    if not isinstance(value, dict):
        return True
    # If "enabled" is missing, treat it as enabled (legacy configs).
    return bool(value.get("enabled", True))


def _get_enabled_signal_names(params_dict: dict[str, Any]) -> set[str]:
    return {name for name, v in params_dict.items() if _is_enabled_signal_params(v)}


def _is_mutually_exclusive(signal_name: str, selected: set[str]) -> bool:
    constraint = SIGNAL_CONSTRAINTS_MAP.get(signal_name)
    if constraint and set(constraint.mutually_exclusive) & selected:
        return True

    # Symmetric check: other signals may exclude this one.
    for other in selected:
        other_constraint = SIGNAL_CONSTRAINTS_MAP.get(other)
        if other_constraint and signal_name in other_constraint.mutually_exclusive:
            return True

    return False


def _pop_random(items: list[str], rng: RandomLike[Any]) -> str:
    idx = rng.randint(0, len(items) - 1)
    return items.pop(idx)


def _is_fundamental_only(
    usage_type: UsageType,
    allowed_categories: set[SignalCategory],
) -> bool:
    return usage_type == "entry" and allowed_categories == {"fundamental"}


def _list_enabled_fundamental_children(
    fundamental_params: dict[str, Any],
) -> set[str]:
    enabled_children: set[str] = set()
    for child_name, child_params in fundamental_params.items():
        if not isinstance(child_params, dict):
            continue
        if "enabled" not in child_params:
            continue
        if child_params.get("enabled", False):
            enabled_children.add(child_name)
    return enabled_children


def _enable_fundamental_children(
    working: dict[str, Any],
    *,
    rng: RandomLike[Any],
    add_signals: int,
) -> list[str]:
    if add_signals <= 0:
        return []

    fundamental = working.get("fundamental")
    if not isinstance(fundamental, dict):
        fundamental = build_signal_params("fundamental", "entry", rng)
    else:
        fundamental = copy.deepcopy(fundamental)
        default_fundamental = build_signal_params("fundamental", "entry", rng)
        for key, value in default_fundamental.items():
            if key in fundamental:
                continue
            if isinstance(value, dict) and "enabled" in value:
                copied = value.copy()
                copied["enabled"] = False
                fundamental[key] = copied
                continue
            fundamental[key] = copy.deepcopy(value)

    fundamental["enabled"] = True
    enabled_children = _list_enabled_fundamental_children(fundamental)
    available_children = [
        child_name
        for child_name, child_params in fundamental.items()
        if (
            isinstance(child_params, dict)
            and "enabled" in child_params
            and child_name not in enabled_children
        )
    ]

    added_children: list[str] = []
    while available_children and len(added_children) < add_signals:
        child_name = _pop_random(available_children, rng)
        child_params = fundamental.get(child_name)
        if not isinstance(child_params, dict):
            child_params = {"enabled": True}
        else:
            child_params = child_params.copy()
            child_params["enabled"] = True
        fundamental[child_name] = child_params
        added_children.append(f"fundamental.{child_name}")

    working["fundamental"] = fundamental
    return added_children


def apply_random_add_structure(
    candidate: StrategyCandidate,
    *,
    rng: RandomLike[Any],
    add_entry_signals: int,
    add_exit_signals: int,
    base_entry_signals: set[str],
    base_exit_signals: set[str],
    allowed_categories: set[SignalCategory] | None = None,
) -> tuple[StrategyCandidate, dict[str, list[str]]]:
    """
    Keep base signals and randomly add extra signals.

    Returns:
        (new_candidate, {"entry": [...added...], "exit": [...added...]})
    """
    updated = candidate.model_copy(deep=True)
    added: dict[str, list[str]] = {"entry": [], "exit": []}

    allowed_set = set(allowed_categories or [])

    updated.entry_filter_params, added["entry"] = _apply_random_add_side(
        updated.entry_filter_params,
        usage_type="entry",
        rng=rng,
        base_signals=base_entry_signals,
        add_signals=max(0, add_entry_signals),
        allowed_categories=allowed_set,
    )
    updated.exit_trigger_params, added["exit"] = _apply_random_add_side(
        updated.exit_trigger_params,
        usage_type="exit",
        rng=rng,
        base_signals=base_exit_signals,
        add_signals=max(0, add_exit_signals),
        allowed_categories=allowed_set,
    )

    # Keep a small metadata trail for reproducibility/debugging.
    updated.metadata = copy.deepcopy(updated.metadata or {})
    updated.metadata.update(
        {
            "structure_mode": "random_add",
            "random_add_entry_signals": add_entry_signals,
            "random_add_exit_signals": add_exit_signals,
            "random_added_entry": added["entry"],
            "random_added_exit": added["exit"],
        }
    )

    return updated, added


def _apply_random_add_side(
    params_dict: dict[str, Any],
    *,
    usage_type: UsageType,
    rng: RandomLike[Any],
    base_signals: set[str],
    add_signals: int,
    allowed_categories: set[SignalCategory],
) -> tuple[dict[str, Any], list[str]]:
    working = copy.deepcopy(params_dict or {})
    # Ensure base signals exist and are enabled.
    for base_name in base_signals:
        existing = working.get(base_name)
        if isinstance(existing, dict):
            existing = existing.copy()
            existing["enabled"] = True
            working[base_name] = existing
        elif existing is None:
            working[base_name] = build_signal_params(base_name, usage_type, rng)

    enabled = _get_enabled_signal_names(working)

    desired_total = len(base_signals) + add_signals

    # Trim extras if we somehow exceed the desired total (e.g. crossover union).
    if len(enabled) > desired_total:
        removable = [s for s in enabled if s not in base_signals]
        while removable and len(enabled) > desired_total:
            to_remove = _pop_random(removable, rng)
            working.pop(to_remove, None)
            enabled.discard(to_remove)

    # Add new signals until we hit the target.
    added: list[str] = []
    candidates = _list_addable_signals(
        usage_type=usage_type,
        enabled=enabled,
        allowed_categories=allowed_categories,
    )

    while candidates and len(enabled) < desired_total:
        signal_name = _pop_random(candidates, rng)
        if _is_mutually_exclusive(signal_name, enabled):
            continue

        working[signal_name] = build_signal_params(signal_name, usage_type, rng)
        enabled.add(signal_name)
        added.append(signal_name)

    if _is_fundamental_only(usage_type, allowed_categories):
        fundamental_added = sum(1 for signal_name in added if signal_name == "fundamental")
        nested_add_count = max(0, add_signals - fundamental_added)
        added.extend(
            _enable_fundamental_children(
                working,
                rng=rng,
                add_signals=nested_add_count,
            )
        )

    if added:
        logger.debug(
            f"Random-add ({usage_type}): added={added}, total_enabled={len(enabled)}"
        )
    return working, added


def _list_addable_signals(
    *,
    usage_type: Literal["entry", "exit"],
    enabled: set[str],
    allowed_categories: set[SignalCategory],
) -> list[str]:
    addable: list[str] = []

    for signal in AVAILABLE_SIGNALS:
        if not _is_usage_allowed(signal, usage_type):
            continue

        if allowed_categories and signal.category not in allowed_categories:
            continue

        if signal.name in enabled:
            continue

        if _is_mutually_exclusive(signal.name, enabled):
            continue

        addable.append(signal.name)

    return addable


def _is_usage_allowed(signal: SignalConstraints, usage_type: Literal["entry", "exit"]) -> bool:
    if signal.usage == "both":
        return True
    return signal.usage == usage_type
