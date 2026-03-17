"""Shared provenance helpers for analytics/chart verification flows."""

from __future__ import annotations

from collections.abc import Iterable

from src.application.services.snapshot_resolver import (
    resolve_dataset_snapshot_id,
    resolve_market_snapshot_id,
)
from src.entrypoints.http.schemas.analytics_common import DataProvenance


def _normalize_domains(domains: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for domain in domains:
        value = str(domain).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _normalize_warnings(warnings: Iterable[str]) -> list[str]:
    return [str(warning).strip() for warning in warnings if str(warning).strip()]


def build_market_provenance(
    *,
    reference_date: str | None = None,
    loaded_domains: Iterable[str] = (),
    warnings: Iterable[str] = (),
    strategy_name: str | None = None,
    strategy_fingerprint: str | None = None,
) -> DataProvenance:
    return DataProvenance(
        source_kind="market",
        market_snapshot_id=resolve_market_snapshot_id(),
        dataset_snapshot_id=None,
        reference_date=reference_date,
        loaded_domains=_normalize_domains(loaded_domains),
        strategy_name=strategy_name,
        strategy_fingerprint=strategy_fingerprint,
        warnings=_normalize_warnings(warnings),
    )


def build_dataset_provenance(
    *,
    dataset_name: str,
    reference_date: str | None = None,
    loaded_domains: Iterable[str] = (),
    warnings: Iterable[str] = (),
    strategy_name: str | None = None,
    strategy_fingerprint: str | None = None,
) -> DataProvenance:
    return DataProvenance(
        source_kind="dataset",
        market_snapshot_id=resolve_market_snapshot_id(),
        dataset_snapshot_id=resolve_dataset_snapshot_id(dataset_name),
        reference_date=reference_date,
        loaded_domains=_normalize_domains(loaded_domains),
        strategy_name=strategy_name,
        strategy_fingerprint=strategy_fingerprint,
        warnings=_normalize_warnings(warnings),
    )

