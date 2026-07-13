"""Shared analytics provenance and diagnostics schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class MarketBubbleFootprintHorizon(BaseModel):
    """Latest bubble footprint metrics for one return horizon."""

    horizon: int
    score: int
    regime: str
    nearBlowoff: bool = False
    breadthUpPct: float | None = None
    pctAboveSma50: float | None = None
    pctAboveSma200: float | None = None
    expensiveMcapSharePct: float | None = None
    returnP90P10SpreadPct: float | None = None
    returnDispersionPercentile: float | None = None
    capWeightLeadershipPct: float | None = None
    activeFlags: list[str] = Field(default_factory=list)


class MarketBubbleFootprintLatestResponse(BaseModel):
    """Latest market-level bubble footprint monitor response."""

    date: str
    markets: list[str]
    overallRegime: str
    overallScore: int
    nearBlowoff: bool = False
    researchExperimentId: str
    reratingExperimentId: str
    horizons: list[MarketBubbleFootprintHorizon] = Field(default_factory=list)
