"""Shared snapshot identifier normalization helpers."""

from __future__ import annotations

import re

_DATASET_SNAPSHOT_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def normalize_dataset_snapshot_name(dataset_name: str | None) -> str | None:
    """Normalize dataset snapshot names without accepting path-like input."""

    if not isinstance(dataset_name, str):
        return None

    normalized = dataset_name.strip()
    if not normalized:
        return None

    if not _DATASET_SNAPSHOT_NAME_RE.fullmatch(normalized):
        raise ValueError(f"Invalid dataset name: {dataset_name}")

    return normalized


def canonicalize_dataset_snapshot_id(dataset_name: str | None) -> str | None:
    """Best-effort canonical snapshot ID for metadata."""

    if not isinstance(dataset_name, str):
        return None

    try:
        return normalize_dataset_snapshot_name(dataset_name)
    except ValueError:
        return None


def normalize_market_snapshot_id(snapshot_id: str | None) -> str:
    """Canonicalize supported market snapshot identifiers."""

    if snapshot_id is None:
        return "market:latest"

    normalized = snapshot_id.strip()
    if not normalized or normalized in {"latest", "market:latest"}:
        return "market:latest"

    raise FileNotFoundError(f"Unsupported market snapshot: {snapshot_id}")
