"""
Complex Analytics Routes

ランキング・ファクター回帰・スクリーニングAPI。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from loguru import logger

from src.infrastructure.db.market.query_helpers import is_valid_stock_code
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.factor_regression import FactorRegressionResponse
from src.entrypoints.http.schemas.portfolio_factor_regression import PortfolioFactorRegressionResponse
from src.entrypoints.http.schemas.ranking import MarketRankingResponse
from src.entrypoints.http.schemas.ranking import (
    MarketFundamentalRankingResponse,
)
from src.entrypoints.http.schemas.screening import (
    MarketScreeningResponse,
)
from src.entrypoints.http.schemas.screening_job import (
    ScreeningJobPayload,
    ScreeningJobRequest,
    ScreeningJobResponse,
)
from src.application.services.job_manager import JobInfo
from src.application.services.screening_job_service import (
    screening_job_manager,
    screening_job_service,
)

router = APIRouter(tags=["Analytics"])
_SCREENING_JOB_TYPE = "screening"
_SCREENING_DEPRECATED_MESSAGE = (
    "GET /api/analytics/screening is removed. "
    "Use POST /api/analytics/screening/jobs to start a job, "
    "GET /api/analytics/screening/jobs/{job_id} to poll status, "
    "and GET /api/analytics/screening/result/{job_id} to fetch result."
)


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
    from src.application.services.ranking_service import RankingService

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


@router.get(
    "/api/analytics/fundamental-ranking",
    response_model=MarketFundamentalRankingResponse,
    summary="Get market fundamental rankings",
    description=(
        "Get fundamental rankings by ratio (high/low). "
        "Use metricKey to select ratio metric (currently: eps_forecast_to_actual)."
    ),
)
async def get_fundamental_ranking(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    markets: str = Query("prime"),
    metricKey: str = Query("eps_forecast_to_actual"),
) -> MarketFundamentalRankingResponse:
    """ファンダメンタルランキングを取得"""
    from src.application.services.ranking_service import RankingService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = RankingService(reader)
    try:
        return service.get_fundamental_rankings(limit=limit, markets=markets, metric_key=metricKey)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Fundamental ranking error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get fundamental rankings: {e}",
        )


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
    from src.application.services.factor_regression_service import FactorRegressionService

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
    summary="Legacy screening endpoint (removed)",
    description="Legacy synchronous screening endpoint is removed. Use screening job endpoints.",
)
async def get_screening_legacy() -> None:
    """削除済み同期エンドポイント。移行ガイドを返す。"""
    raise HTTPException(status_code=410, detail=_SCREENING_DEPRECATED_MESSAGE)


def _get_screening_job_or_404(job_id: str) -> JobInfo:
    """Screeningジョブを取得、存在しなければ404。"""
    job = screening_job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"ジョブが見つかりません: {job_id}")
    if job.job_type != _SCREENING_JOB_TYPE:
        raise HTTPException(status_code=400, detail=f"Screeningジョブではありません: {job.job_type}")
    return job


def _build_screening_job_response(job: JobInfo) -> ScreeningJobResponse:
    """JobInfo から ScreeningJobResponse を構築。"""
    params = screening_job_service.get_job_request(job.job_id) or ScreeningJobRequest()

    return ScreeningJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        message=job.message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        markets=params.markets,
        strategies=params.strategies,
        recentDays=params.recentDays,
        referenceDate=params.date,
        sortBy=params.sortBy,
        order=params.order,
        limit=params.limit,
    )


@router.post(
    "/api/analytics/screening/jobs",
    response_model=ScreeningJobResponse,
    status_code=202,
    summary="Create screening job",
    description="Submit an async screening job.",
)
async def create_screening_job(
    request: Request,
    payload: ScreeningJobRequest,
) -> ScreeningJobResponse:
    """非同期 screening ジョブを開始"""
    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    try:
        job_id = await screening_job_service.submit_screening(
            reader=reader,
            request=payload,
        )
        return _build_screening_job_response(_get_screening_job_or_404(job_id))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        logger.exception(f"Screening job submit error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start screening job: {e}",
        ) from e


@router.get(
    "/api/analytics/screening/jobs/{job_id}",
    response_model=ScreeningJobResponse,
    summary="Get screening job status",
)
async def get_screening_job(job_id: str) -> ScreeningJobResponse:
    """Screening ジョブ状態を取得"""
    return _build_screening_job_response(_get_screening_job_or_404(job_id))


@router.post(
    "/api/analytics/screening/jobs/{job_id}/cancel",
    response_model=ScreeningJobResponse,
    summary="Cancel screening job",
)
async def cancel_screening_job(job_id: str) -> ScreeningJobResponse:
    """Screening ジョブをキャンセル"""
    _get_screening_job_or_404(job_id)

    cancelled_job = await screening_job_manager.cancel_job(job_id)
    if cancelled_job is None:
        job = screening_job_manager.get_job(job_id)
        status = job.status if job else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"ジョブは既に終了しています（状態: {status}）",
        )

    return _build_screening_job_response(cancelled_job)


@router.get(
    "/api/analytics/screening/result/{job_id}",
    response_model=MarketScreeningResponse,
    summary="Get screening result",
)
async def get_screening_result(job_id: str) -> MarketScreeningResponse:
    """完了済み screening ジョブの結果を取得"""
    job = _get_screening_job_or_404(job_id)

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"ジョブが完了していません（状態: {job.status}）",
        )

    if not isinstance(job.raw_result, dict):
        raise HTTPException(status_code=500, detail="結果がありません")

    try:
        payload = ScreeningJobPayload.model_validate(job.raw_result)
        return MarketScreeningResponse.model_validate(payload.response)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"結果の復元に失敗しました: {e}",
        ) from e


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
    from src.infrastructure.db.market.portfolio_db import PortfolioDb
    from src.application.services.portfolio_factor_regression_service import PortfolioFactorRegressionService

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
