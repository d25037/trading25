from __future__ import annotations

from dataclasses import dataclass

from src.application.services.dataset_presets import get_preset_label
from src.application.services.screening_strategy_selection import (
    ScreeningStrategyCatalog,
    build_strategy_selection_catalog,
    resolve_selected_strategy_names,
)
from src.application.services.strategy_dataset_metadata import (
    StrategyDatasetMetadata,
    format_market_scope_label,
    resolve_dataset_metadata,
    stringify_markets,
    union_market_lists,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.screening_profile import (
    EntryDecidability,
    load_strategy_screening_config,
)


@dataclass(frozen=True)
class ScreeningMarketResolution:
    strategy_names: list[str]
    markets: list[str]
    markets_param: str
    scope_label: str


def _resolve_selected_strategy_context(
    *,
    entry_decidability: EntryDecidability,
    strategies: str | None,
    config_loader: ConfigLoader | None = None,
) -> tuple[ScreeningStrategyCatalog, list[str]]:
    loader = config_loader or ConfigLoader()
    metadata = [item for item in loader.get_strategy_metadata() if item.category == "production"]
    catalog = build_strategy_selection_catalog(
        metadata,
        load_strategy_config=lambda name: load_strategy_screening_config(loader, name),
        entry_decidability=entry_decidability,
    )
    selected_names = resolve_selected_strategy_names(
        strategies=strategies,
        catalog=catalog,
        entry_decidability=entry_decidability,
    )
    return catalog, selected_names


def _resolve_selected_strategy_datasets(
    *,
    selected_names: list[str],
    catalog: ScreeningStrategyCatalog,
    dataset_base_path: str | None = None,
) -> list[StrategyDatasetMetadata]:
    metadata_items: list[StrategyDatasetMetadata] = []

    for name in selected_names:
        runtime_payload = catalog.runtime_payloads[name]
        try:
            dataset_metadata = resolve_dataset_metadata(
                runtime_payload.shared_config.dataset,
                dataset_base_path=dataset_base_path,
            )
        except Exception as exc:
            raise ValueError(
                f"Invalid dataset for screening strategy {name}: {exc}"
            ) from exc

        if not dataset_metadata.screening_default_markets:
            raise ValueError(f"Failed to resolve default markets for {name}")
        metadata_items.append(dataset_metadata)

    return metadata_items


def _build_scope_label(dataset_metadata_items: list[StrategyDatasetMetadata], markets: list[str]) -> str:
    fallback_label = format_market_scope_label(markets)
    preset_labels: list[str] = []
    seen_labels: set[str] = set()

    for dataset_metadata in dataset_metadata_items:
        if dataset_metadata.dataset_preset is None:
            return fallback_label
        preset_label = get_preset_label(dataset_metadata.dataset_preset)
        if preset_label is None:
            return fallback_label
        if preset_label in seen_labels:
            continue
        preset_labels.append(preset_label)
        seen_labels.add(preset_label)

    if preset_labels:
        return " + ".join(preset_labels)
    return fallback_label


def validate_selected_screening_strategy_datasets(
    *,
    entry_decidability: EntryDecidability,
    strategies: str | None,
    config_loader: ConfigLoader | None = None,
    dataset_base_path: str | None = None,
) -> list[str]:
    catalog, selected_names = _resolve_selected_strategy_context(
        entry_decidability=entry_decidability,
        strategies=strategies,
        config_loader=config_loader,
    )
    _resolve_selected_strategy_datasets(
        selected_names=selected_names,
        catalog=catalog,
        dataset_base_path=dataset_base_path,
    )
    return selected_names


def resolve_default_screening_markets(
    *,
    entry_decidability: EntryDecidability,
    strategies: str | None,
    config_loader: ConfigLoader | None = None,
    dataset_base_path: str | None = None,
) -> ScreeningMarketResolution:
    catalog, selected_names = _resolve_selected_strategy_context(
        entry_decidability=entry_decidability,
        strategies=strategies,
        config_loader=config_loader,
    )

    dataset_metadata_items = _resolve_selected_strategy_datasets(
        selected_names=selected_names,
        catalog=catalog,
        dataset_base_path=dataset_base_path,
    )
    market_lists = [
        dataset_metadata.screening_default_markets
        for dataset_metadata in dataset_metadata_items
        if dataset_metadata.screening_default_markets
    ]

    markets = union_market_lists(market_lists)
    if not markets:
        raise ValueError(
            "Failed to resolve default screening markets from production strategy datasets"
        )

    return ScreeningMarketResolution(
        strategy_names=selected_names,
        markets=markets,
        markets_param=stringify_markets(markets),
        scope_label=_build_scope_label(dataset_metadata_items, markets),
    )
