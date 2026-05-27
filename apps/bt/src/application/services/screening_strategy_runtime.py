"""Runtime strategy resolution for market screening."""

from __future__ import annotations

from typing import cast

from src.application.services.dataset_presets import get_preset_label
from src.application.services.screening_execution import StrategyRuntime
from src.application.services.screening_strategy_selection import (
    build_strategy_response_names,
    build_strategy_selection_catalog,
    resolve_selected_strategy_names,
)
from src.application.services.strategy_dataset_metadata import (
    resolve_dataset_metadata,
    resolve_dataset_stock_codes,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.screening_profile import load_strategy_screening_config
from src.entrypoints.http.schemas.screening import EntryDecidability


def resolve_screening_strategy_runtimes(
    config_loader: ConfigLoader,
    strategies: str | None,
    *,
    entry_decidability: EntryDecidability,
    use_strategy_dataset_universe: bool = False,
) -> list[StrategyRuntime]:
    """対象戦略を production カテゴリから screening 実行形式に解決する。"""
    metadata = [m for m in config_loader.get_strategy_metadata() if m.category == "production"]
    catalog = build_strategy_selection_catalog(
        metadata,
        load_strategy_config=lambda name: load_strategy_screening_config(
            config_loader,
            name,
        ),
        entry_decidability=entry_decidability,
    )
    selected_names = resolve_selected_strategy_names(
        strategies=strategies,
        catalog=catalog,
        entry_decidability=entry_decidability,
    )
    response_names = build_strategy_response_names(
        catalog.metadata_by_name,
        selected_names,
    )

    runtimes: list[StrategyRuntime] = []
    for name in selected_names:
        metadata_entry = catalog.metadata_by_name[name]
        runtime_payload = catalog.runtime_payloads[name]
        screening_support = runtime_payload.screening_support
        resolved_entry_decidability = runtime_payload.entry_decidability
        if screening_support != "supported" or resolved_entry_decidability is None:
            raise ValueError(f"Unsupported screening strategy selected: {name}")

        dataset_universe_codes: frozenset[str] | None = None
        dataset_scope_label: str | None = None
        if use_strategy_dataset_universe:
            if runtime_payload.shared_config.data_source == "dataset_snapshot":
                try:
                    dataset_metadata = resolve_dataset_metadata(runtime_payload.shared_config.dataset)
                    dataset_universe_codes = frozenset(
                        resolve_dataset_stock_codes(runtime_payload.shared_config.dataset)
                    )
                    if dataset_metadata.dataset_preset is not None:
                        dataset_scope_label = get_preset_label(dataset_metadata.dataset_preset)
                except Exception as exc:
                    raise ValueError(
                        f"Invalid dataset universe for screening strategy {name}: {exc}"
                    ) from exc

        runtimes.append(
            StrategyRuntime(
                name=name,
                response_name=response_names[name],
                basename=metadata_entry.path.stem,
                entry_params=runtime_payload.entry_params,
                exit_params=runtime_payload.exit_params,
                shared_config=runtime_payload.shared_config,
                compiled_strategy=runtime_payload.compiled_strategy,
                entry_decidability=cast(EntryDecidability, resolved_entry_decidability),
                dataset_universe_codes=dataset_universe_codes,
                dataset_scope_label=dataset_scope_label,
            )
        )

    return runtimes
