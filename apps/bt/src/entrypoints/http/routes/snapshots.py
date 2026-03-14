"""Snapshot resolver HTTP endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from src.application.services.snapshot_resolver import SnapshotResolver
from src.entrypoints.http.schemas.snapshots import (
    SnapshotResolveRequest,
    SnapshotResolveResponse,
)

router = APIRouter(tags=["Snapshots"])


@router.get(
    "/api/snapshots/resolve",
    response_model=SnapshotResolveResponse,
    summary="Resolve market or dataset snapshot",
)
async def resolve_snapshot(
    request: Annotated[SnapshotResolveRequest, Depends()],
) -> SnapshotResolveResponse:
    """Resolve one logical snapshot contract for market and dataset planes."""
    snapshot_id = request.snapshot_id.strip() if request.snapshot_id else None
    if request.plane == "dataset" and snapshot_id is None:
        raise HTTPException(status_code=422, detail="dataset plane requires snapshot_id")

    try:
        resolved = SnapshotResolver.from_settings().resolve(
            request.plane,
            snapshot_id,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if resolved is None:
        detail = (
            f"Dataset not found: {request.snapshot_id}"
            if request.plane == "dataset"
            else f"Snapshot not found: {request.snapshot_id}"
        )
        raise HTTPException(status_code=404, detail=detail)

    return SnapshotResolveResponse(
        plane=resolved.plane.value,
        snapshot_id=resolved.snapshot_id,
        requested_id=resolved.requested_id,
        backend=resolved.backend.value,
        root_path=resolved.root_path,
        primary_path=resolved.primary_path,
        duckdb_path=resolved.duckdb_path,
        manifest_path=resolved.manifest_path,
    )
