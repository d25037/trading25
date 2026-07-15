"""Shared result contracts for semantic Market data mutations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MarketMutationStats:
    """Classify input and storage mutations without conflating the two."""

    input: int
    inserted: int
    updated: int
    unchanged: int
    deleted: int

    @property
    def mutated_rows(self) -> int:
        """Return the number of rows whose persisted state changed."""
        return self.inserted + self.updated + self.deleted

    @classmethod
    def empty(cls) -> MarketMutationStats:
        """Return an explicit zero-input, zero-mutation result."""
        return cls(input=0, inserted=0, updated=0, unchanged=0, deleted=0)


MutationKey = tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class SemanticDeltaResult:
    """Exact semantic delta produced before persistent Market DML."""

    stats: MarketMutationStats
    inserted_keys: tuple[MutationKey, ...] = ()
    updated_keys: tuple[MutationKey, ...] = ()
    deleted_keys: tuple[MutationKey, ...] = ()
    affected_dates: frozenset[str] = frozenset()
    affected_codes: frozenset[str] = frozenset()

    @property
    def mutated_rows(self) -> int:
        return self.stats.mutated_rows

    @property
    def mutated_keys(self) -> tuple[MutationKey, ...]:
        return self.inserted_keys + self.updated_keys + self.deleted_keys

    @classmethod
    def empty(cls, *, input_count: int = 0) -> SemanticDeltaResult:
        return cls(
            stats=MarketMutationStats(
                input=input_count,
                inserted=0,
                updated=0,
                unchanged=0,
                deleted=0,
            )
        )


def deterministic_last_wins(
    rows: list[dict[str, Any]],
    *,
    key_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Deduplicate by semantic key while preserving first-key order."""
    positions: dict[MutationKey, int] = {}
    deduplicated: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(row.get(column) for column in key_columns)
        position = positions.get(key)
        if position is None:
            positions[key] = len(deduplicated)
            deduplicated.append(row)
        else:
            deduplicated[position] = row
    return deduplicated
