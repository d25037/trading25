"""Snapshot resolver route tests."""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from src.application.services.snapshot_resolver import (
    ResolvedSnapshot,
    SnapshotBackend,
    SnapshotPlane,
)
from src.entrypoints.http.app import create_app


class _FakeSnapshotResolver:
    def __init__(self, resolved: ResolvedSnapshot | None) -> None:
        self._resolved = resolved

    def resolve(self, plane: str, snapshot_id: str | None = None) -> ResolvedSnapshot | None:
        _ = (plane, snapshot_id)
        return self._resolved


def test_resolve_market_latest() -> None:
    app = create_app()
    client = TestClient(app)
    resolved = ResolvedSnapshot(
        plane=SnapshotPlane.MARKET,
        snapshot_id="market:latest",
        requested_id=None,
        backend=SnapshotBackend.DUCKDB_PARQUET,
        root_path="/tmp/market",
        primary_path="/tmp/market/market.duckdb",
        duckdb_path="/tmp/market/market.duckdb",
    )

    with patch(
        "src.entrypoints.http.routes.snapshots.SnapshotResolver.from_settings",
        return_value=_FakeSnapshotResolver(resolved),
    ):
        response = client.get("/api/snapshots/resolve", params={"plane": "market"})

    assert response.status_code == 200
    data = response.json()
    assert data["plane"] == "market"
    assert data["snapshot_id"] == "market:latest"
    assert data["backend"] == "duckdb-parquet"


def test_resolve_dataset_snapshot() -> None:
    app = create_app()
    client = TestClient(app)
    resolved = ResolvedSnapshot(
        plane=SnapshotPlane.DATASET,
        snapshot_id="primeExTopix500",
        requested_id="primeExTopix500",
        backend=SnapshotBackend.DUCKDB_PARQUET,
        root_path="/tmp/datasets/primeExTopix500",
        primary_path="/tmp/datasets/primeExTopix500/dataset.duckdb",
        duckdb_path="/tmp/datasets/primeExTopix500/dataset.duckdb",
        manifest_path="/tmp/datasets/primeExTopix500/manifest.v2.json",
    )

    with patch(
        "src.entrypoints.http.routes.snapshots.SnapshotResolver.from_settings",
        return_value=_FakeSnapshotResolver(resolved),
    ):
        response = client.get(
            "/api/snapshots/resolve",
            params={"plane": "dataset", "snapshot_id": "primeExTopix500"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["plane"] == "dataset"
    assert data["snapshot_id"] == "primeExTopix500"
    assert data["manifest_path"].endswith("manifest.v2.json")


def test_resolve_dataset_snapshot_requires_id() -> None:
    app = create_app()
    client = TestClient(app)

    response = client.get("/api/snapshots/resolve", params={"plane": "dataset"})

    assert response.status_code == 422
    assert "dataset plane requires snapshot_id" in response.json()["message"]


def test_resolve_missing_snapshot_returns_404() -> None:
    app = create_app()
    client = TestClient(app)

    with patch(
        "src.entrypoints.http.routes.snapshots.SnapshotResolver.from_settings",
        return_value=_FakeSnapshotResolver(None),
    ):
        response = client.get(
            "/api/snapshots/resolve",
            params={"plane": "dataset", "snapshot_id": "missing"},
        )

    assert response.status_code == 404
    assert response.json()["message"] == "Dataset not found: missing"
