"""Schemas for published analytics research bundles."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ResearchHighlightTone = Literal["neutral", "accent", "success", "warning", "danger"]


class ResearchLabelValue(BaseModel):
    label: str
    value: str


class ResearchHighlight(BaseModel):
    label: str
    value: str
    tone: ResearchHighlightTone = "neutral"
    detail: str | None = None


class ResearchTableHighlight(BaseModel):
    name: str
    label: str
    description: str | None = None


class PublishedResearchSummary(BaseModel):
    title: str
    tags: list[str] = Field(default_factory=list)
    purpose: str
    method: list[str] = Field(default_factory=list)
    resultHeadline: str | None = None
    resultBullets: list[str] = Field(default_factory=list)
    considerations: list[str] = Field(default_factory=list)
    selectedParameters: list[ResearchLabelValue] = Field(default_factory=list)
    highlights: list[ResearchHighlight] = Field(default_factory=list)
    tableHighlights: list[ResearchTableHighlight] = Field(default_factory=list)


class ResearchCatalogItem(BaseModel):
    experimentId: str
    runId: str
    title: str
    objective: str | None = None
    headline: str | None = None
    createdAt: str
    analysisStartDate: str | None = None
    analysisEndDate: str | None = None
    gitCommit: str | None = None
    tags: list[str] = Field(default_factory=list)
    hasStructuredSummary: bool = False


class ResearchRunReference(BaseModel):
    runId: str
    createdAt: str
    isLatest: bool = False


class ResearchCatalogResponse(BaseModel):
    items: list[ResearchCatalogItem] = Field(default_factory=list)
    lastUpdated: str


class ResearchDetailResponse(BaseModel):
    item: ResearchCatalogItem
    summary: PublishedResearchSummary | None = None
    summaryMarkdown: str
    outputTables: list[str] = Field(default_factory=list)
    availableRuns: list[ResearchRunReference] = Field(default_factory=list)
    resultMetadata: dict[str, Any] = Field(default_factory=dict)
