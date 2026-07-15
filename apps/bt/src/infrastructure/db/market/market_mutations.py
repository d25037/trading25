"""Shared result contracts for semantic Market data mutations."""

from __future__ import annotations

from dataclasses import dataclass


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
