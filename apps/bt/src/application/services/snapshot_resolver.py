"""Unified snapshot resolver for market and dataset planes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from src.application.services.dataset_resolver import DatasetResolver
from src.shared.config.settings import Settings, get_settings


class SnapshotPlane(str, Enum):
    """Logical data plane for a resolved snapshot."""

    DATASET = "dataset"
    MARKET = "market"


class SnapshotBackend(str, Enum):
    """Physical backend used by a resolved snapshot."""

    DUCKDB_PARQUET = "duckdb-parquet"
    SQLITE_COMPATIBILITY = "sqlite-compatibility"
    SQLITE_LEGACY = "sqlite-legacy"


@dataclass(frozen=True, slots=True)
class ResolvedSnapshot:
    """Resolved input snapshot with canonical identifier and storage paths."""

    plane: SnapshotPlane
    snapshot_id: str
    requested_id: str | None
    backend: SnapshotBackend
    root_path: str
    primary_path: str
    duckdb_path: str | None = None
    compatibility_db_path: str | None = None
    manifest_path: str | None = None

    @property
    def path(self) -> str:
        return self.primary_path


def normalize_dataset_snapshot_name(dataset_name: str | None) -> str | None:
    """Normalize dataset snapshot input while preserving current name semantics."""

    if not isinstance(dataset_name, str):
        return None

    normalized = dataset_name.strip()
    if not normalized:
        return None

    stem = Path(normalized).stem
    return stem or None


def normalize_market_snapshot_id(snapshot_id: str | None) -> str:
    """Canonicalize market snapshot identifiers.

    The current data plane exposes only the mutable latest pointer backed by
    ``market.duckdb``. Future immutable market snapshots can extend this
    normalization without changing client call sites.
    """

    if snapshot_id is None:
        return "market:latest"

    normalized = snapshot_id.strip()
    if not normalized or normalized in {"latest", "market:latest"}:
        return "market:latest"

    raise FileNotFoundError(f"Unsupported market snapshot: {snapshot_id}")


class SnapshotResolver:
    """Resolve dataset and market snapshots through one service boundary."""

    def __init__(self, dataset_base_path: str, market_timeseries_dir: str) -> None:
        self._dataset_resolver = DatasetResolver(dataset_base_path)
        normalized_market_timeseries_dir = market_timeseries_dir.strip()
        self._market_timeseries_dir = (
            str(Path(normalized_market_timeseries_dir).resolve())
            if normalized_market_timeseries_dir
            else ""
        )

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> SnapshotResolver:
        resolved_settings = settings or get_settings()
        return cls(
            dataset_base_path=resolved_settings.dataset_base_path,
            market_timeseries_dir=resolved_settings.market_timeseries_dir,
        )

    def resolve(
        self,
        plane: SnapshotPlane | str,
        snapshot_id: str | None = None,
    ) -> ResolvedSnapshot | None:
        resolved_plane = (
            plane if isinstance(plane, SnapshotPlane) else SnapshotPlane(str(plane).strip())
        )
        if resolved_plane == SnapshotPlane.DATASET:
            if snapshot_id is None:
                raise ValueError("dataset snapshot id is required")
            return self.resolve_dataset(snapshot_id)
        return self.resolve_market(snapshot_id)

    def resolve_dataset(self, dataset_name: str) -> ResolvedSnapshot | None:
        normalized_name = normalize_dataset_snapshot_name(dataset_name)
        if normalized_name is None:
            return None

        snapshot_dir = Path(self._dataset_resolver.get_snapshot_dir(normalized_name))
        duckdb_path = Path(self._dataset_resolver.get_duckdb_path(normalized_name))
        compatibility_db_path = Path(self._dataset_resolver.get_db_path(normalized_name))
        manifest_path = Path(self._dataset_resolver.get_manifest_path(normalized_name))
        legacy_db_path = Path(self._dataset_resolver.get_legacy_db_path(normalized_name))

        resolved_snapshot_dir = str(snapshot_dir.resolve())
        resolved_duckdb_path = str(duckdb_path.resolve()) if duckdb_path.exists() else None
        resolved_compatibility_db_path = (
            str(compatibility_db_path.resolve()) if compatibility_db_path.exists() else None
        )
        resolved_manifest_path = str(manifest_path.resolve()) if manifest_path.exists() else None

        if resolved_duckdb_path is not None:
            return ResolvedSnapshot(
                plane=SnapshotPlane.DATASET,
                snapshot_id=normalized_name,
                requested_id=dataset_name,
                backend=SnapshotBackend.DUCKDB_PARQUET,
                root_path=resolved_snapshot_dir,
                primary_path=resolved_duckdb_path,
                duckdb_path=resolved_duckdb_path,
                compatibility_db_path=resolved_compatibility_db_path,
                manifest_path=resolved_manifest_path,
            )

        if resolved_compatibility_db_path is not None:
            return ResolvedSnapshot(
                plane=SnapshotPlane.DATASET,
                snapshot_id=normalized_name,
                requested_id=dataset_name,
                backend=SnapshotBackend.SQLITE_COMPATIBILITY,
                root_path=resolved_snapshot_dir,
                primary_path=resolved_compatibility_db_path,
                compatibility_db_path=resolved_compatibility_db_path,
                manifest_path=resolved_manifest_path,
            )

        if legacy_db_path.exists():
            resolved_legacy_db_path = str(legacy_db_path.resolve())
            return ResolvedSnapshot(
                plane=SnapshotPlane.DATASET,
                snapshot_id=normalized_name,
                requested_id=dataset_name,
                backend=SnapshotBackend.SQLITE_LEGACY,
                root_path=str(legacy_db_path.parent.resolve()),
                primary_path=resolved_legacy_db_path,
                compatibility_db_path=resolved_legacy_db_path,
            )

        return None

    def require_dataset(self, dataset_name: str) -> ResolvedSnapshot:
        resolved = self.resolve_dataset(dataset_name)
        if resolved is None:
            raise FileNotFoundError(f"Dataset not found: {dataset_name}")
        return resolved

    def resolve_market(self, snapshot_id: str | None = None) -> ResolvedSnapshot:
        normalized_snapshot_id = normalize_market_snapshot_id(snapshot_id)
        if not self._market_timeseries_dir:
            raise FileNotFoundError("MARKET_TIMESERIES_DIR is not configured")

        market_root = Path(self._market_timeseries_dir)
        market_duckdb_path = market_root / "market.duckdb"
        if not market_duckdb_path.exists():
            raise FileNotFoundError(f"market.duckdb not found: {market_duckdb_path}")

        resolved_market_duckdb_path = str(market_duckdb_path.resolve())
        return ResolvedSnapshot(
            plane=SnapshotPlane.MARKET,
            snapshot_id=normalized_snapshot_id,
            requested_id=snapshot_id,
            backend=SnapshotBackend.DUCKDB_PARQUET,
            root_path=str(market_root.resolve()),
            primary_path=resolved_market_duckdb_path,
            duckdb_path=resolved_market_duckdb_path,
        )


def resolve_dataset_snapshot_id(
    dataset_name: str | None,
    *,
    resolver: SnapshotResolver | None = None,
) -> str | None:
    """Best-effort snapshot-id resolution for execution metadata."""

    normalized_name = normalize_dataset_snapshot_name(dataset_name)
    if normalized_name is None:
        return None

    active_resolver = resolver
    if active_resolver is None:
        try:
            active_resolver = SnapshotResolver.from_settings()
        except Exception:
            return normalized_name

    try:
        resolved = active_resolver.resolve_dataset(normalized_name)
    except Exception:
        return normalized_name

    return resolved.snapshot_id if resolved is not None else normalized_name


def resolve_market_snapshot_id(
    snapshot_id: str | None = None,
    *,
    resolver: SnapshotResolver | None = None,
) -> str:
    """Best-effort market snapshot-id resolution for execution metadata."""

    normalized_snapshot_id = normalize_market_snapshot_id(snapshot_id)

    active_resolver = resolver
    if active_resolver is None:
        try:
            active_resolver = SnapshotResolver.from_settings()
        except Exception:
            return normalized_snapshot_id

    try:
        resolved = active_resolver.resolve_market(snapshot_id)
    except Exception:
        return normalized_snapshot_id

    return resolved.snapshot_id
