"""
Complex Analytics Routes

ランキング・ファクター回帰・スクリーニングAPI。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from loguru import logger

from src.lib.market_db.query_helpers import is_valid_stock_code
from src.server.schemas.factor_regression import FactorRegressionResponse
from src.server.schemas.portfolio_factor_regression import PortfolioFactorRegressionResponse
from src.server.schemas.ranking import MarketRankingResponse
from src.server.schemas.screening import MarketScreeningResponse

router = APIRouter(tags=["Analytics"])


def _normalize_factor_regression_symbol(symbol: str) -> str:
    normalized = symbol.upper()
    if not is_valid_stock_code(normalized):
        raise HTTPException(
            status_code=400,
            detail="Symbol must be a valid 4-character stock code (e.g., 7203 or 285A)",
        )
    return normalized


# --- Ranking ---


@router.get(
    "/api/analytics/ranking",
    response_model=MarketRankingResponse,
    summary="Get market rankings",
    description="Get market rankings including top stocks by trading value, price gainers, and price losers.",
)
async def get_ranking(
    request: Request,
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(20, ge=1, le=100),
    markets: str = Query("prime"),
    lookbackDays: int = Query(1, ge=1, le=100),
    periodDays: int = Query(250, ge=1, le=250),
) -> MarketRankingResponse:
    """マーケットランキングを取得"""
    from src.server.services.ranking_service import RankingService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = RankingService(reader)
    try:
        return service.get_rankings(
            date=date,
            limit=limit,
            markets=markets,
            lookback_days=lookbackDays,
            period_days=periodDays,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Ranking error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get rankings: {e}")


# --- Factor Regression ---


@router.get(
    "/api/analytics/factor-regression/{symbol}",
    response_model=FactorRegressionResponse,
    summary="Analyze stock factor regression",
    description="Two-stage factor regression analysis for risk decomposition.",
)
async def get_factor_regression(
    request: Request,
    symbol: str,
    lookbackDays: int = Query(252, ge=60, le=1000),
) -> FactorRegressionResponse:
    """ファクター回帰分析を実行"""
    from src.server.services.factor_regression_service import FactorRegressionService

    normalized_symbol = _normalize_factor_regression_symbol(symbol)

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = FactorRegressionService(reader)
    try:
        return service.analyze_stock(normalized_symbol, lookback_days=lookbackDays)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        if "insufficient" in msg.lower():
            raise HTTPException(status_code=422, detail=msg)
        raise HTTPException(status_code=400, detail=msg)
    except Exception as e:
        logger.exception(f"Factor regression error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to analyze: {e}")


# --- Screening ---


@router.get(
    "/api/analytics/screening",
    response_model=MarketScreeningResponse,
    summary="Run stock screening",
    description="Run stock screening analysis with Range Break Fast and Slow strategies.",
)
async def get_screening(
    request: Request,
    markets: str = Query("prime"),
    rangeBreakFast: bool = Query(True),
    rangeBreakSlow: bool = Query(True),
    recentDays: int = Query(10, ge=1, le=90),
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    minBreakPercentage: float | None = Query(None),
    minVolumeRatio: float | None = Query(None),
    sortBy: str = Query("date", pattern=r"^(date|stockCode|volumeRatio|breakPercentage)$"),
    order: str = Query("desc", pattern=r"^(asc|desc)$"),
    limit: int | None = Query(None, ge=1),
) -> MarketScreeningResponse:
    """スクリーニングを実行"""
    from src.server.services.screening_service import ScreeningService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = ScreeningService(reader)
    try:
        return service.run_screening(
            markets=markets,
            range_break_fast=rangeBreakFast,
            range_break_slow=rangeBreakSlow,
            recent_days=recentDays,
            reference_date=date,
            min_break_percentage=minBreakPercentage,
            min_volume_ratio=minVolumeRatio,
            sort_by=sortBy,
            order=order,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Screening error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to run screening: {e}")


# --- Portfolio Factor Regression (Phase 3E-2) ---


@router.get(
    "/api/analytics/portfolio-factor-regression/{portfolioId}",
    response_model=PortfolioFactorRegressionResponse,
    summary="Analyze portfolio factor regression",
    description="ポートフォリオ全体のファクター回帰分析",
)
async def get_portfolio_factor_regression(
    request: Request,
    portfolioId: int,
    lookbackDays: int = Query(252, ge=60, le=1000),
) -> PortfolioFactorRegressionResponse:
    """ポートフォリオファクター回帰分析を実行"""
    from src.lib.market_db.portfolio_db import PortfolioDb
    from src.server.services.portfolio_factor_regression_service import PortfolioFactorRegressionService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Market database not initialized")

    portfolio_db: PortfolioDb | None = getattr(request.app.state, "portfolio_db", None)
    if portfolio_db is None:
        raise HTTPException(status_code=422, detail="Portfolio database not initialized")

    service = PortfolioFactorRegressionService(reader, portfolio_db)
    try:
        return service.analyze(portfolioId, lookback_days=lookbackDays)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from e
        if "insufficient" in msg.lower() or "no valid" in msg.lower() or "zero" in msg.lower() or "no stocks" in msg.lower():
            raise HTTPException(status_code=422, detail=msg) from e
        raise HTTPException(status_code=400, detail=msg) from e
    except Exception as e:
        logger.exception(f"Portfolio factor regression error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to analyze: {e}") from e
