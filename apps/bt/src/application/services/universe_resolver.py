"""Compatibility re-export for market universe resolver."""

from src.infrastructure.db.market.universe_resolver import (
    UniversePreset,
    UniverseProvenance,
    UniverseResolution,
    UniverseResolutionError,
    UniverseResolverDbLike,
    dataset_to_universe_preset,
    resolve_universe,
)

__all__ = [
    "UniversePreset",
    "UniverseProvenance",
    "UniverseResolution",
    "UniverseResolutionError",
    "UniverseResolverDbLike",
    "dataset_to_universe_preset",
    "resolve_universe",
]
