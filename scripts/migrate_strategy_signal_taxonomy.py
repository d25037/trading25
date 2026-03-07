#!/usr/bin/env python3
"""Migrate legacy strategy signal names to the canonical taxonomy."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

BOLLINGER_POSITION_MAP: dict[str, tuple[str, str]] = {
    "below_upper": ("upper", "below"),
    "above_lower": ("lower", "above"),
    "above_middle": ("middle", "above"),
    "below_middle": ("middle", "below"),
    "above_upper": ("upper", "above"),
    "below_lower": ("lower", "below"),
}

SIGNAL_SECTION_KEYS = ("entry_filter_params", "exit_trigger_params")

FLAT_KEY_RENAMES: dict[str, str] = {
    "entry_volume_threshold": "entry_volume_ratio_above_ratio_threshold",
    "entry_volume_short_period": "entry_volume_ratio_above_short_period",
    "entry_volume_long_period": "entry_volume_ratio_above_long_period",
    "entry_volume_ma_type": "entry_volume_ratio_above_ma_type",
    "exit_volume_threshold": "exit_volume_ratio_below_ratio_threshold",
    "exit_volume_short_period": "exit_volume_ratio_below_short_period",
    "exit_volume_long_period": "exit_volume_ratio_below_long_period",
    "exit_volume_ma_type": "exit_volume_ratio_below_ma_type",
}

FLAT_KEY_PREFIX_RENAMES: dict[str, str] = {
    "entry_bollinger_bands_": "entry_bollinger_position_",
    "exit_bollinger_bands_": "exit_bollinger_position_",
    "entry_period_breakout_": "entry_period_extrema_break_",
    "exit_period_breakout_": "exit_period_extrema_break_",
    "entry_atr_support_break_": "entry_atr_support_position_",
    "exit_atr_support_break_": "exit_atr_support_position_",
    "entry_retracement_": "entry_retracement_position_",
    "exit_retracement_": "exit_retracement_position_",
}

FUNDAMENTAL_FLAT_FIELD_MAP: dict[str, tuple[str, str]] = {
    "per_enabled": ("per", "enabled"),
    "per_threshold": ("per", "threshold"),
    "roe_enabled": ("roe", "enabled"),
    "roe_threshold": ("roe", "threshold"),
    "peg_ratio_enabled": ("peg_ratio", "enabled"),
    "peg_ratio_threshold": ("peg_ratio", "threshold"),
    "forward_eps_growth_enabled": ("forward_eps_growth", "enabled"),
    "forward_eps_growth_threshold": ("forward_eps_growth", "threshold"),
    "eps_enabled": ("eps_growth", "enabled"),
}


def _new_map() -> CommentedMap:
    return CommentedMap()


def _new_seq() -> CommentedSeq:
    return CommentedSeq()


def _rename_flat_key(key: str) -> str:
    renamed = FLAT_KEY_RENAMES.get(key)
    if renamed is not None:
        return renamed

    for old_prefix, new_prefix in FLAT_KEY_PREFIX_RENAMES.items():
        if key.startswith(old_prefix):
            return new_prefix + key.removeprefix(old_prefix)

    return key


def _migrate_flat_keys(node: Any) -> tuple[Any, bool]:
    if isinstance(node, dict):
        changed = False
        migrated = _new_map()
        for key, value in node.items():
            new_key = _rename_flat_key(key) if isinstance(key, str) else key
            new_value, child_changed = _migrate_flat_keys(value)
            migrated[new_key] = new_value
            changed = changed or child_changed or new_key != key
        return migrated, changed

    if isinstance(node, list):
        changed = False
        migrated = _new_seq()
        for item in node:
            new_item, child_changed = _migrate_flat_keys(item)
            migrated.append(new_item)
            changed = changed or child_changed
        return migrated, changed

    return node, False


def _migrate_period_breakout(params: CommentedMap) -> tuple[str, CommentedMap]:
    migrated = _new_map()
    condition = params.get("condition", "break")
    for key, value in params.items():
        if key == "condition":
            continue
        migrated[key] = value

    if condition == "maintained":
        migrated["state"] = "away_from_extrema"
        return "period_extrema_position", migrated

    return "period_extrema_break", migrated


def _migrate_atr_support_break(params: CommentedMap) -> tuple[str, CommentedMap]:
    migrated = _new_map()
    for key, value in params.items():
        if key == "direction":
            migrated[key] = {"break": "below", "recovery": "above"}.get(value, value)
        else:
            migrated[key] = value
    return "atr_support_position", migrated


def _migrate_retracement(params: CommentedMap) -> tuple[str, CommentedMap]:
    migrated = _new_map()
    for key, value in params.items():
        if key == "direction":
            migrated[key] = {"break": "below", "recovery": "above"}.get(value, value)
        else:
            migrated[key] = value
    return "retracement_position", migrated


def _migrate_bollinger_bands(params: CommentedMap) -> tuple[str, CommentedMap]:
    migrated = _new_map()
    position = params.get("position", "below_upper")
    level, direction = BOLLINGER_POSITION_MAP.get(position, ("upper", "below"))

    for key, value in params.items():
        if key == "position":
            continue
        migrated[key] = value

    migrated["level"] = level
    migrated["direction"] = direction
    return "bollinger_position", migrated


def _migrate_volume(params: CommentedMap) -> tuple[str, CommentedMap]:
    migrated = _new_map()
    direction = params.get("direction", "surge")

    for key, value in params.items():
        if key == "direction":
            continue
        if key == "threshold":
            migrated["ratio_threshold"] = value
            continue
        migrated[key] = value

    if direction == "drop":
        return "volume_ratio_below", migrated

    return "volume_ratio_above", migrated


def _migrate_legacy_volatility(params: CommentedMap) -> tuple[str, CommentedMap] | None:
    if not params.get("enabled", False):
        return None

    if params.get("method") != "percentile":
        return None

    migrated = _new_map()
    for key, value in params.items():
        if key == "method":
            continue
        if key == "threshold":
            migrated["percentile"] = value
            continue
        migrated[key] = value

    if "lookback" not in migrated:
        migrated["lookback"] = 252

    return "volatility_percentile", migrated


def _migrate_fundamental(params: CommentedMap) -> tuple[str, CommentedMap]:
    migrated = _new_map()

    for key, value in params.items():
        if key in FUNDAMENTAL_FLAT_FIELD_MAP or key == "forward_eps":
            continue
        migrated[key] = value

    for flat_key, (metric_key, field_key) in FUNDAMENTAL_FLAT_FIELD_MAP.items():
        if flat_key not in params:
            continue
        metric = migrated.get(metric_key)
        if not isinstance(metric, dict):
            metric = _new_map()
            migrated[metric_key] = metric
        metric[field_key] = params[flat_key]

    forward_eps = params.get("forward_eps")
    if isinstance(forward_eps, dict):
        metric = migrated.get("forward_eps_growth")
        if not isinstance(metric, dict):
            metric = _new_map()
            migrated["forward_eps_growth"] = metric
        for key, value in forward_eps.items():
            metric[key] = value

    return "fundamental", migrated


def _migrate_signal_entry(key: str, value: Any) -> tuple[str, Any, bool]:
    if not isinstance(value, dict):
        return key, value, False

    params = value if isinstance(value, CommentedMap) else CommentedMap(value)

    if key == "period_breakout":
        new_key, new_value = _migrate_period_breakout(params)
        return new_key, new_value, True

    if key == "atr_support_break":
        new_key, new_value = _migrate_atr_support_break(params)
        return new_key, new_value, True

    if key == "retracement":
        new_key, new_value = _migrate_retracement(params)
        return new_key, new_value, True

    if key == "bollinger_bands":
        new_key, new_value = _migrate_bollinger_bands(params)
        return new_key, new_value, True

    if key == "volume":
        new_key, new_value = _migrate_volume(params)
        return new_key, new_value, True

    if key == "volatility":
        migrated = _migrate_legacy_volatility(params)
        if migrated is not None:
            new_key, new_value = migrated
            return new_key, new_value, True
        if not params.get("enabled", False):
            return key, None, True

    if key == "fundamental":
        new_key, new_value = _migrate_fundamental(params)
        return new_key, new_value, new_value != value

    if key == "relative_performance" and not params.get("enabled", False):
        return key, None, True

    if key in {"atr_support_position", "atr_support_cross"} and "atr_period" in params:
        migrated = _new_map()
        for param_key, param_value in params.items():
            if param_key == "atr_period":
                continue
            migrated[param_key] = param_value
        return key, migrated, True

    return key, value, False


def _migrate_signal_sections(data: Any) -> tuple[Any, bool]:
    if not isinstance(data, dict):
        return data, False

    changed = False
    migrated = _new_map()

    for key, value in data.items():
        if key not in SIGNAL_SECTION_KEYS or not isinstance(value, dict):
            migrated[key] = value
            continue

        section_map = _new_map()
        for signal_key, signal_value in value.items():
            new_key, new_value, entry_changed = _migrate_signal_entry(
                signal_key, signal_value
            )
            changed = changed or entry_changed or new_key != signal_key
            if new_value is None:
                continue
            section_map[new_key] = new_value
        migrated[key] = section_map

    return migrated, changed


def migrate_document(data: Any) -> tuple[Any, bool]:
    migrated, changed_sections = _migrate_signal_sections(data)
    changed_top_level = False

    if isinstance(migrated, dict):
        normalized = _new_map()
        for key, value in migrated.items():
            if key == "strategy_name":
                changed_top_level = True
                continue
            if key == "strategy_params":
                changed_top_level = True
                continue
            normalized[key] = value
        migrated = normalized

    migrated, changed_flat = _migrate_flat_keys(migrated)
    return migrated, changed_sections or changed_top_level or changed_flat


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy strategy signal taxonomy in YAML files."
    )
    parser.add_argument("root", type=Path, help="Root directory containing YAML files")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write migrated YAML back to disk. Defaults to dry-run.",
    )
    parser.add_argument(
        "--include-history",
        action="store_true",
        help="Include *_history.yaml files in migration.",
    )
    return parser.parse_args()


def should_process(path: Path, include_history: bool) -> bool:
    if path.suffix.lower() != ".yaml":
        return False
    if not include_history and path.name.endswith("_history.yaml"):
        return False
    return True


def main() -> int:
    args = parse_args()

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=4, offset=2)

    if not args.root.exists():
        raise SystemExit(f"Root does not exist: {args.root}")

    migrated_files: list[Path] = []
    scanned = 0

    for path in sorted(args.root.rglob("*.yaml")):
        if not should_process(path, args.include_history):
            continue
        scanned += 1
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.load(fh)

        migrated, changed = migrate_document(data)
        if not changed:
            continue

        migrated_files.append(path)
        if args.write:
            with path.open("w", encoding="utf-8") as fh:
                yaml.dump(migrated, fh)

    mode = "write" if args.write else "dry-run"
    print(f"mode={mode} scanned={scanned} changed={len(migrated_files)}")
    for path in migrated_files:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
