from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.application.services.dataset_presets import get_preset
from src.application.services.dataset_resolver import DatasetResolver
from src.application.services.run_contracts import extract_dataset_name_from_shared_config
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetSnapshotReader,
    read_dataset_snapshot_manifest,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.config.settings import get_settings
from src.domains.strategy.runtime.loader import ConfigLoader

_PREFERRED_MARKET_ORDER = ("prime", "standard", "growth")
_MARKET_ALIAS_TO_CANONICAL = {
    "prime": "prime",
    "standard": "standard",
    "growth": "growth",
    "0111": "prime",
    "0112": "standard",
    "0113": "growth",
}
_MARKET_LABELS = {
    "prime": "Prime",
    "standard": "Standard",
    "growth": "Growth",
}


@dataclass(frozen=True)
class StrategyDatasetMetadata:
    dataset_name: str | None
    dataset_preset: str | None
    screening_default_markets: list[str] | None


def _dataset_resolver(dataset_base_path: str | None = None) -> DatasetResolver:
    base_path = dataset_base_path or get_settings().dataset_base_path
    return DatasetResolver(base_path)


def canonicalize_market_list(markets: list[str]) -> list[str]:
    canonical: list[str] = []
    seen: set[str] = set()

    for preferred in _PREFERRED_MARKET_ORDER:
        if preferred in {
            _MARKET_ALIAS_TO_CANONICAL.get(market.lower(), market)
            for market in markets
        }:
            canonical.append(preferred)
            seen.add(preferred)

    for market in markets:
        normalized = _MARKET_ALIAS_TO_CANONICAL.get(market.lower(), market)
        if normalized in seen:
            continue
        canonical.append(normalized)
        seen.add(normalized)

    return canonical


def union_market_lists(market_lists: list[list[str]]) -> list[str]:
    union: list[str] = []
    seen: set[str] = set()

    for markets in market_lists:
        for market in canonicalize_market_list(markets):
            if market in seen:
                continue
            union.append(market)
            seen.add(market)

    return canonicalize_market_list(union)


def stringify_markets(markets: list[str]) -> str:
    return ",".join(canonicalize_market_list(markets))


def format_market_scope_label(markets: list[str]) -> str:
    normalized = canonicalize_market_list(markets)
    if not normalized:
        return "Auto"
    if normalized == list(_PREFERRED_MARKET_ORDER):
        return "All Markets"
    return " + ".join(_MARKET_LABELS.get(market, market) for market in normalized)


def resolve_dataset_metadata(
    dataset_name: str,
    *,
    dataset_base_path: str | None = None,
) -> StrategyDatasetMetadata:
    resolver = _dataset_resolver(dataset_base_path)
    normalized_name = extract_dataset_name_from_shared_config({"dataset": dataset_name})
    if normalized_name is None:
        raise ValueError(f"Invalid dataset name: {dataset_name}")
    if not resolver.exists(normalized_name):
        raise FileNotFoundError(f"Dataset not found: {normalized_name}")

    snapshot_dir = Path(resolver.get_snapshot_dir(normalized_name))
    manifest = read_dataset_snapshot_manifest(snapshot_dir)
    preset_name = manifest.dataset.preset
    preset = get_preset(preset_name)
    if preset is None:
        raise ValueError(f"Unknown dataset preset in manifest: {preset_name}")

    return StrategyDatasetMetadata(
        dataset_name=normalized_name,
        dataset_preset=preset_name,
        screening_default_markets=canonicalize_market_list(preset.markets),
    )


def resolve_dataset_stock_codes(
    dataset_name: str,
    *,
    dataset_base_path: str | None = None,
) -> list[str]:
    resolver = _dataset_resolver(dataset_base_path)
    normalized_name = extract_dataset_name_from_shared_config({"dataset": dataset_name})
    if normalized_name is None:
        raise ValueError(f"Invalid dataset name: {dataset_name}")
    if not resolver.exists(normalized_name):
        raise FileNotFoundError(f"Dataset not found: {normalized_name}")

    reader = DatasetSnapshotReader(resolver.get_snapshot_dir(normalized_name))
    try:
        rows = reader.query("SELECT code FROM stocks ORDER BY code")
    finally:
        reader.close()

    codes: list[str] = []
    seen: set[str] = set()
    for row in rows:
        raw_code = row["code"]
        if raw_code is None:
            continue
        normalized_code = normalize_stock_code(str(raw_code))
        if normalized_code in seen:
            continue
        seen.add(normalized_code)
        codes.append(normalized_code)

    return codes


def resolve_strategy_dataset_metadata(
    strategy_name: str,
    *,
    config_loader: ConfigLoader | None = None,
    strategy_config: dict[str, Any] | None = None,
    dataset_base_path: str | None = None,
) -> StrategyDatasetMetadata:
    loader = config_loader or ConfigLoader()
    config = strategy_config if strategy_config is not None else loader.load_strategy_config(strategy_name)
    merged_shared_config = loader.merge_shared_config(config)
    dataset_name = extract_dataset_name_from_shared_config(merged_shared_config)
    if dataset_name is None:
        return StrategyDatasetMetadata(
            dataset_name=None,
            dataset_preset=None,
            screening_default_markets=None,
        )

    resolved = resolve_dataset_metadata(dataset_name, dataset_base_path=dataset_base_path)
    return StrategyDatasetMetadata(
        dataset_name=resolved.dataset_name,
        dataset_preset=resolved.dataset_preset,
        screening_default_markets=resolved.screening_default_markets,
    )
