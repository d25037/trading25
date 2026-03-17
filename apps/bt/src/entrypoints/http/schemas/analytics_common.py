"""Shared analytics provenance and diagnostics schemas."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


AnalyticsSourceKind = Literal["market", "dataset"]


class ResponseDiagnostics(BaseModel):
    """Common diagnostics payload for analytics-style responses."""

    missing_required_data: list[str] = Field(default_factory=list)
    used_fields: list[str] = Field(default_factory=list)
    effective_period_type: str | None = None
    warnings: list[str] = Field(default_factory=list)


class DataProvenance(BaseModel):
    """Common provenance payload for SoT-backed analytics responses."""

    source_kind: AnalyticsSourceKind
    market_snapshot_id: str | None = None
    dataset_snapshot_id: str | None = None
    reference_date: str | None = None
    loaded_domains: list[str] = Field(default_factory=list)
    strategy_name: str | None = None
    strategy_fingerprint: str | None = None
    warnings: list[str] = Field(default_factory=list)

