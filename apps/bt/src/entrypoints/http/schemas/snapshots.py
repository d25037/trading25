"""Snapshot resolver HTTP schemas."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class SnapshotResolveRequest(BaseModel):
    """Unified snapshot resolve request."""

    plane: Literal["market", "dataset"] = Field(
        description="解決対象 plane",
    )
    snapshot_id: str | None = Field(
        default=None,
        description="snapshot 識別子。market は省略時に latest を解決する",
    )


class SnapshotResolveResponse(BaseModel):
    """Resolved snapshot metadata."""

    plane: Literal["market", "dataset"] = Field(description="解決された plane")
    snapshot_id: str = Field(description="canonical snapshot identifier")
    requested_id: str | None = Field(
        default=None,
        description="要求時の snapshot identifier",
    )
    backend: str = Field(description="resolved storage backend")
    root_path: str = Field(description="snapshot root path")
    primary_path: str = Field(description="primary readable artifact path")
    duckdb_path: str | None = Field(default=None, description="DuckDB path")
    compatibility_db_path: str | None = Field(
        default=None,
        description="compatibility SQLite path",
    )
    manifest_path: str | None = Field(default=None, description="manifest path")
