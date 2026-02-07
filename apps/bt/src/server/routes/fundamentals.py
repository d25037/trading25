"""
Fundamentals API Routes

Provides endpoints for fundamental analysis calculations.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException
from loguru import logger

from src.api.exceptions import APIError, APINotFoundError
from src.server.schemas.fundamentals import (
    FundamentalsComputeRequest,
    FundamentalsComputeResponse,
)
from src.server.services.fundamentals_service import fundamentals_service

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
    response_model=FundamentalsComputeResponse,
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
- Financial statements: JQuants API via apps/ts/api proxy
- Stock prices: market.db via apps/ts/api proxy

**Response includes**:
- `data`: Array of fundamental data points sorted by date (descending)
- `latestMetrics`: Latest metrics with daily valuation applied
- `dailyValuation`: Daily PER/PBR time-series for charting
""",
)
async def compute_fundamentals(
    request: FundamentalsComputeRequest,
) -> FundamentalsComputeResponse:
    """Compute fundamental metrics for a stock."""
    logger.info(f"Computing fundamentals for {request.symbol}")

    try:
        # Run in thread pool to avoid blocking
        import asyncio

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _get_executor(),
            fundamentals_service.compute_fundamentals,
            request,
        )

        if not result.data:
            logger.warning(f"No fundamental data found for {request.symbol}")

        return result

    except APINotFoundError as e:
        logger.warning(f"Stock not found: {request.symbol}")
        raise HTTPException(status_code=404, detail=str(e))

    except APIError as e:
        logger.error(f"API error computing fundamentals: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    except Exception as e:
        logger.exception(f"Error computing fundamentals for {request.symbol}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute fundamentals: {e}",
        )
