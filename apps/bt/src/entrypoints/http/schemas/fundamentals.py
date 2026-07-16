"""Fundamentals HTTP request schema."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from src.application.contracts import fundamentals as fundamentals_contracts


class FundamentalsComputeRequest(BaseModel):
    """Request body for fundamentals computation."""

    symbol: str = Field(
        ...,
        description="Stock code (4-5 digits)",
        json_schema_extra={"example": "7203"},
    )
    from_date: fundamentals_contracts.StrictIsoDate | None = Field(
        None,
        description=fundamentals_contracts.FUNDAMENTALS_FROM_DATE_DESCRIPTION,
        json_schema_extra={"example": "2020-01-01"},
    )
    to_date: fundamentals_contracts.StrictIsoDate | None = Field(
        None,
        description=fundamentals_contracts.FUNDAMENTALS_TO_DATE_DESCRIPTION,
        json_schema_extra={"example": "2025-12-31"},
    )
    period_type: Literal["all", "FY", "1Q", "2Q", "3Q"] = Field(
        default="all", description="Filter by period type (FY, 1Q, 2Q, 3Q)"
    )
    prefer_consolidated: bool = Field(
        default=True, description="Prefer consolidated statements"
    )
    trading_value_period: int = Field(
        default=15,
        ge=1,
        le=250,
        description="Rolling period (days) for market cap to trading value ratio",
    )
    forecast_eps_lookback_fy_count: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Lookback FY count for forecast EPS vs recent actual EPS comparison",
    )

    @model_validator(mode="after")
    def validate_date_range(self) -> FundamentalsComputeRequest:
        if (
            self.from_date is not None
            and self.to_date is not None
            and self.from_date > self.to_date
        ):
            raise ValueError("from_date must be on or before to_date")
        return self
