"""Tests for the unified market/dataset snapshot resolver."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.application.services.snapshot_resolver import (
    SnapshotBackend,
    SnapshotPlane,
    SnapshotResolver,
    normalize_dataset_snapshot_name,
    normalize_market_snapshot_id,
    resolve_dataset_snapshot_id,
    resolve_market_snapshot_id,
)


def test_resolve_dataset_prefers_duckdb_snapshot(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "sample"
    snapshot_dir.mkdir()
    (snapshot_dir / "dataset.duckdb").write_text("", encoding="utf-8")
    (snapshot_dir / "dataset.db").write_text("", encoding="utf-8")
    (snapshot_dir / "manifest.v1.json").write_text("{}", encoding="utf-8")

    resolver = SnapshotResolver(str(tmp_path), str(tmp_path / "market-timeseries"))
    resolved = resolver.resolve_dataset("sample.db")

    assert resolved is not None
    assert resolved.plane == SnapshotPlane.DATASET
    assert resolved.snapshot_id == "sample"
    assert resolved.backend == SnapshotBackend.DUCKDB_PARQUET
    assert resolved.primary_path.endswith("dataset.duckdb")
    assert resolved.compatibility_db_path is not None
    assert resolved.compatibility_db_path.endswith("dataset.db")
    assert resolved.manifest_path is not None
    assert resolved.manifest_path.endswith("manifest.v1.json")


def test_resolve_dataset_uses_compatibility_db_when_duckdb_missing(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "sample"
    snapshot_dir.mkdir()
    (snapshot_dir / "dataset.db").write_text("", encoding="utf-8")

    resolver = SnapshotResolver(str(tmp_path), str(tmp_path / "market-timeseries"))
    resolved = resolver.resolve("dataset", "sample")

    assert resolved is not None
    assert resolved.backend == SnapshotBackend.SQLITE_COMPATIBILITY
    assert resolved.primary_path.endswith("dataset.db")


def test_resolve_dataset_uses_legacy_db_fallback(tmp_path: Path) -> None:
    legacy_db_path = tmp_path / "legacy.db"
    legacy_db_path.write_text("", encoding="utf-8")

    resolver = SnapshotResolver(str(tmp_path), str(tmp_path / "market-timeseries"))
    resolved = resolver.require_dataset("legacy")

    assert resolved.backend == SnapshotBackend.SQLITE_LEGACY
    assert resolved.primary_path == str(legacy_db_path.resolve())


def test_resolve_market_latest_uses_shared_api(tmp_path: Path) -> None:
    market_dir = tmp_path / "market-timeseries"
    market_dir.mkdir()
    (market_dir / "market.duckdb").write_text("", encoding="utf-8")

    resolver = SnapshotResolver(str(tmp_path / "datasets"), str(market_dir))
    resolved = resolver.resolve(SnapshotPlane.MARKET)

    assert resolved is not None
    assert resolved.plane == SnapshotPlane.MARKET
    assert resolved.snapshot_id == "market:latest"
    assert resolved.backend == SnapshotBackend.DUCKDB_PARQUET
    assert resolved.primary_path.endswith("market.duckdb")


def test_normalize_market_snapshot_id_rejects_unknown_snapshots() -> None:
    with pytest.raises(FileNotFoundError, match="Unsupported market snapshot"):
        normalize_market_snapshot_id("market:20260309")


def test_normalize_dataset_snapshot_name_rejects_path_like_input() -> None:
    with pytest.raises(ValueError, match="Invalid dataset name"):
        normalize_dataset_snapshot_name("../sample.db")


def test_resolve_dataset_rejects_invalid_dataset_name(tmp_path: Path) -> None:
    resolver = SnapshotResolver(str(tmp_path), str(tmp_path / "market-timeseries"))

    with pytest.raises(ValueError, match="Invalid dataset name"):
        resolver.resolve_dataset("../sample.db")


def test_resolve_dataset_snapshot_id_falls_back_to_normalized_name_for_missing_snapshot() -> None:
    assert resolve_dataset_snapshot_id(" sample.db ") == "sample"


def test_resolve_dataset_snapshot_id_returns_none_for_invalid_name() -> None:
    assert resolve_dataset_snapshot_id("../sample.db") is None


def test_resolve_market_snapshot_id_defaults_to_latest() -> None:
    assert resolve_market_snapshot_id() == "market:latest"
