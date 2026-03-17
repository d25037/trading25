from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from src.domains.strategy.runtime.compiler import CompiledStrategyIR
from src.domains.strategy.runtime.screening_profile import (
    EntryDecidability,
    LoadedStrategyScreeningConfig,
    ScreeningSupport,
)
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.shared.paths.resolver import StrategyMetadata


@dataclass(frozen=True)
class StrategyRuntimePayload:
    entry_params: SignalParams
    exit_params: SignalParams
    shared_config: SharedConfig
    compiled_strategy: CompiledStrategyIR
    screening_support: ScreeningSupport
    entry_decidability: EntryDecidability | None


@dataclass(frozen=True)
class ScreeningStrategyCatalog:
    metadata_by_name: dict[str, StrategyMetadata]
    basename_map: dict[str, list[str]]
    runtime_payloads: dict[str, StrategyRuntimePayload]
    supported_names: set[str]
    eligible_names: set[str]


def build_strategy_selection_catalog(
    metadata: list[StrategyMetadata],
    *,
    load_strategy_config: Callable[[str], LoadedStrategyScreeningConfig],
    entry_decidability: EntryDecidability,
) -> ScreeningStrategyCatalog:
    production_metadata = [item for item in metadata if item.category == "production"]
    if not production_metadata:
        raise ValueError("No production strategies found")

    metadata_by_name = {item.name: item for item in production_metadata}
    basename_map: dict[str, list[str]] = {}
    runtime_payloads: dict[str, StrategyRuntimePayload] = {}
    supported_names: set[str] = set()
    eligible_names: set[str] = set()

    for item in production_metadata:
        try:
            loaded = load_strategy_config(item.name)
        except Exception as exc:
            raise ValueError(
                f"Invalid production strategy config for screening: {item.name} ({exc})"
            ) from exc

        runtime_payloads[item.name] = StrategyRuntimePayload(
            entry_params=loaded.entry_params,
            exit_params=loaded.exit_params,
            shared_config=loaded.shared_config,
            compiled_strategy=loaded.compiled_strategy,
            screening_support=loaded.screening_support,
            entry_decidability=loaded.entry_decidability,
        )
        basename_map.setdefault(item.path.stem, []).append(item.name)

        if loaded.screening_support == "supported":
            supported_names.add(item.name)
        if (
            loaded.screening_support == "supported"
            and loaded.entry_decidability == entry_decidability
        ):
            eligible_names.add(item.name)

    if not eligible_names:
        raise ValueError(
            f"No production strategies found for {entry_decidability} screening"
        )

    return ScreeningStrategyCatalog(
        metadata_by_name=metadata_by_name,
        basename_map=basename_map,
        runtime_payloads=runtime_payloads,
        supported_names=supported_names,
        eligible_names=eligible_names,
    )


def resolve_strategy_token(
    token: str,
    metadata_by_name: dict[str, StrategyMetadata],
    basename_map: dict[str, list[str]],
) -> str | None:
    if token in metadata_by_name:
        return token

    if token.startswith("production/"):
        return token if token in metadata_by_name else None

    production_prefixed = f"production/{token}"
    if production_prefixed in metadata_by_name:
        return production_prefixed

    candidates = basename_map.get(token, [])
    if len(candidates) == 1:
        return candidates[0]

    return None


def resolve_selected_strategy_names(
    *,
    strategies: str | None,
    catalog: ScreeningStrategyCatalog,
    entry_decidability: EntryDecidability,
) -> list[str]:
    if strategies is None or not strategies.strip():
        return sorted(catalog.eligible_names)

    selected_names: list[str] = []
    invalid: list[str] = []
    unsupported: list[str] = []
    wrong_decidability: list[str] = []

    for token in [part.strip() for part in strategies.split(",") if part.strip()]:
        resolved = resolve_strategy_token(
            token,
            catalog.metadata_by_name,
            catalog.basename_map,
        )
        if resolved is None:
            invalid.append(token)
            continue
        if resolved not in catalog.supported_names:
            unsupported.append(token)
            continue
        if resolved not in catalog.eligible_names:
            wrong_decidability.append(token)
            continue
        if resolved not in selected_names:
            selected_names.append(resolved)

    if unsupported:
        raise ValueError(
            "Unsupported screening strategies: " + ", ".join(sorted(set(unsupported)))
        )
    if wrong_decidability:
        raise ValueError(
            f"Strategies do not support {entry_decidability} screening: "
            + ", ".join(sorted(set(wrong_decidability)))
        )
    if invalid:
        raise ValueError(
            "Invalid strategies (production only): " + ", ".join(sorted(set(invalid)))
        )
    if not selected_names:
        raise ValueError(
            f"No valid {entry_decidability} production strategies selected"
        )

    return selected_names


def build_strategy_response_names(
    metadata_by_name: dict[str, StrategyMetadata],
    selected_names: list[str],
) -> dict[str, str]:
    selected_metadata = [metadata_by_name[name] for name in selected_names]
    basename_counts: dict[str, int] = {}
    for item in selected_metadata:
        basename_counts[item.path.stem] = basename_counts.get(item.path.stem, 0) + 1

    response_names: dict[str, str] = {}
    for item in selected_metadata:
        response_names[item.name] = (
            item.name if basename_counts[item.path.stem] > 1 else item.path.stem
        )
    return response_names
