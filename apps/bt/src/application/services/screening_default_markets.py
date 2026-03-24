from __future__ import annotations

from dataclasses import dataclass

from src.application.services.screening_strategy_selection import (
    ScreeningStrategyCatalog,
    build_strategy_selection_catalog,
    resolve_selected_strategy_names,
)
from src.application.services.strategy_dataset_metadata import (
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


def _resolve_selected_strategy_market_lists(
    *,
    selected_names: list[str],
    catalog: ScreeningStrategyCatalog,
    dataset_base_path: str | None = None,
) -> list[list[str]]:
    market_lists: list[list[str]] = []

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
        market_lists.append(dataset_metadata.screening_default_markets)

    return market_lists


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
    _resolve_selected_strategy_market_lists(
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

    market_lists = _resolve_selected_strategy_market_lists(
        selected_names=selected_names,
        catalog=catalog,
        dataset_base_path=dataset_base_path,
    )

    markets = union_market_lists(market_lists)
    if not markets:
        raise ValueError(
            "Failed to resolve default screening markets from production strategy datasets"
        )

    return ScreeningMarketResolution(
        strategy_names=selected_names,
        markets=markets,
        markets_param=stringify_markets(markets),
    )
