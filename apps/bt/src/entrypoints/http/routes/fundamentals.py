"""
Fundamentals API Routes

Provides endpoints for fundamental analysis calculations.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter
from loguru import logger

from src.application.contracts import fundamentals as fundamentals_contracts
from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.application.services.fundamentals_service import fundamentals_service
from src.entrypoints.http.routes.fundamentals_error_mapping import (
    FUNDAMENTALS_ERROR_RESPONSES,
    raise_fundamentals_http_error,
)
from src.entrypoints.http.schemas.fundamentals import FundamentalsComputeRequest

router = APIRouter(prefix="/api/fundamentals", tags=["Fundamentals"])

# ThreadPoolExecutor for blocking operations
_executor = ThreadPoolExecutor(max_workers=4)


def _get_executor() -> ThreadPoolExecutor:
    """shutdown 済みの場合は再作成して返す"""
    global _executor
    if getattr(_executor, "_shutdown", False):
        _executor = ThreadPoolExecutor(max_workers=4)
    return _executor


@router.post(
    "/compute",
    response_model=fundamentals_contracts.FundamentalsComputeResponse,
    responses=FUNDAMENTALS_ERROR_RESPONSES,
    summary="Compute fundamental metrics for a stock",
    description="""
Compute fundamental analysis metrics for a stock symbol.

**Calculated Metrics** (17 types):
- **Valuation**: PER, PBR
- **Profitability**: ROE, ROA, Operating Margin, Net Margin
- **Per-share**: EPS, BPS, Diluted EPS
- **FCF**: FCF, FCF Yield, FCF Margin
- **Time-series**: Daily PER/PBR valuation
- **Forecast**: Forecast EPS, Forecast Change Rate

**Data Sources**:
- Financial statements: local `market.duckdb`
- Valuation/per-share summary: local `daily_valuation` in `market.duckdb`
- Historical adjustment basis, exact stock-master snapshot, and PIT-consistent
  adjusted metrics are required; unavailable PIT inputs return 409 with
  `adjusted_metrics_pit` recovery guidance.

**Response includes**:
- `data`: Array of fundamental data points sorted by date (descending)
- `latestMetrics`: Summary metrics composed from `daily_valuation` and latest disclosure data
- `latestMetricsSource`: Source tables/dates used to compose `latestMetrics`
- `dailyValuation`: Daily PER/PBR time-series for charting
""",
)
async def compute_fundamentals(
    request: FundamentalsComputeRequest,
) -> fundamentals_contracts.FundamentalsComputeResponse:
    """Compute fundamental metrics for a stock."""
    logger.info(f"Computing fundamentals for {request.symbol}")

    try:
        # Run in thread pool to avoid blocking
        import asyncio

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _get_executor(),
            fundamentals_service.compute_fundamentals,
            fundamentals_contracts.FundamentalsComputeQuery.model_validate(request.model_dump()),
        )

        if not result.data:
            logger.warning(f"No fundamental data found for {request.symbol}")

        return result

    except FundamentalsPitSnapshotError as exc:
        raise_fundamentals_http_error(exc)
