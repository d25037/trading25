"""
Complex Analytics Routes

ランキング・ファクター回帰・スクリーニングAPI。
"""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, HTTPException, Query, Request
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from src.application.contracts import factor_regression as factor_contracts
from src.application.contracts import (
    portfolio_factor_regression as portfolio_factor_contracts,
)
from src.application.contracts import ranking as ranking_contracts
from src.application.contracts import screening as screening_contracts
from src.application.contracts.jobs import JobStatus
from src.infrastructure.db.market.query_helpers import is_valid_stock_code
from src.entrypoints.http.routes.job_response_utils import (
    build_job_response_base,
)
from src.entrypoints.http.schemas import screening_job as screening_job_schema
from src.application.services.job_manager import JobInfo
from src.application.services.screening_job_service import (
    screening_job_manager,
    screening_job_service,
)
from src.application.services.strategy_dataset_metadata import format_market_scope_label

router = APIRouter(tags=["Analytics"])
_SCREENING_JOB_TYPE = "screening"
_SCREENING_DEPRECATED_MESSAGE = (
    "GET /api/analytics/screening is removed. "
    "Use POST /api/analytics/screening/jobs to start a job, "
    "GET /api/analytics/screening/jobs/{job_id} to poll status, "
    "and GET /api/analytics/screening/result/{job_id} to fetch result."
)
_SCREENING_JOB_REQUEST_FIELDS = frozenset(
    screening_contracts.ScreeningJobRequest.model_fields
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
    "/api/analytics/ranking/symbol/{code}",
    response_model=ranking_contracts.MarketRankingSymbolResponse,
    summary="Get latest Daily Ranking snapshot for a symbol",
)
async def get_ranking_symbol_snapshot(
    request: Request,
    code: str,
) -> ranking_contracts.MarketRankingSymbolResponse:
    """単一銘柄の最新 Daily Ranking スナップショットを取得。"""
    from src.application.services.ranking_service import RankingService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")
    try:
        return RankingService(reader).get_symbol_ranking_snapshot(code)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except Exception as error:
        logger.exception(f"Ranking symbol snapshot error: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get ranking symbol snapshot: {error}",
        ) from error


@router.get(
    "/api/analytics/ranking",
    response_model=ranking_contracts.MarketRankingResponse,
    summary="Get market rankings",
    description="Get market rankings including top stocks by trading value, price gainers, and price losers.",
)
async def get_ranking(
    request: Request,
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(20, ge=0, le=1000, description="Maximum rows per ranking. Use 0 for no row limit."),
    markets: str = Query("prime"),
    lookbackDays: int = Query(1, ge=1, le=100),
    periodDays: int = Query(250, ge=1, le=250),
    sector33Name: str | None = Query(None, description="Optional TOPIX-33/industry sector name filter"),
    sector17Name: str | None = Query(None, description="Optional TOPIX-17 sector name filter"),
    includeValuation: bool = Query(False, description="Include PER, forward PER, PBR, and market cap"),
    includeSectorStrength: bool = Query(
        False,
        description="Include TOPIX-33 sector strength score and bucket in ranking and index performance rows.",
    ),
    sectorStrengthFamily: str = Query(
        "balanced_sector_strength",
        enum=["balanced_sector_strength", "long_hybrid_leadership"],
        description=(
            "Sector strength family used when includeSectorStrength is true. "
            "balanced_sector_strength uses the Daily Ranking balanced sector strength baseline; "
            "long_hybrid_leadership "
            "uses long-side 120/252/504 session sector leadership."
        ),
    ),
    forwardEpsDisclosedWithinDays: int = Query(
        0,
        ge=0,
        le=3650,
        description=(
            "Keep valuation-enriched stocks whose forward EPS source was disclosed within this many calendar days. "
            "Use 0 to disable the filter."
        ),
    ),
    regimeState: ranking_contracts.RankingRegimeStateFilter | None = Query(
        None,
        description=(
            "Keep valuation-enriched stocks matching a base Daily Ranking liquidity regime."
        ),
    ),
    fundamentalState: ranking_contracts.RankingFundamentalStateFilter | None = Query(
        None,
        description=(
            "Keep valuation-enriched stocks matching a Daily Ranking fundamental/value condition. "
            "Use deep_value or value_confirmed with regimeState for former good-regime subsets."
        ),
    ),
    riskState: ranking_contracts.RankingRiskStateFilter | None = Query(
        None,
        description="Keep valuation-enriched stocks matching a Daily Ranking warning/risk flag.",
    ),
    technicalState: ranking_contracts.RankingTechnicalStateFilter | None = Query(
        None,
        description=(
            "Keep stocks matching a Daily Ranking technical confirmation state, "
            "such as atr20_acceleration or momentum_20_60_top20."
        ),
    ),
) -> ranking_contracts.MarketRankingResponse:
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
            sector33_name=sector33Name,
            sector17_name=sector17Name,
            include_valuation=includeValuation,
            include_sector_strength=includeSectorStrength,
            sector_strength_family=ranking_contracts.normalize_sector_strength_family(
                sectorStrengthFamily
            ),
            forward_eps_disclosed_within_days=forwardEpsDisclosedWithinDays,
            regime_state=regimeState,
            fundamental_state=fundamentalState,
            risk_state=riskState,
            technical_state=technicalState,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Ranking error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get rankings: {e}")


@router.get(
    "/api/analytics/fundamental-ranking",
    response_model=ranking_contracts.MarketFundamentalRankingResponse,
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
    forecastAboveRecentFyActuals: bool | None = Query(
        None,
        description=(
            "If true, return only stocks whose latest forecast EPS is greater than "
            "the max actual EPS in recent FY lookback window."
        ),
    ),
    forecastLookbackFyCount: int = Query(
        3,
        ge=1,
        le=20,
        description="Lookback FY count used by forecastAboveRecentFyActuals filter.",
    ),
) -> ranking_contracts.MarketFundamentalRankingResponse:
    """ファンダメンタルランキングを取得"""
    from src.application.services.ranking_service import RankingService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = RankingService(reader)
    try:
        return service.get_fundamental_rankings(
            limit=limit,
            markets=markets,
            metric_key=metricKey,
            forecast_above_recent_fy_actuals=bool(forecastAboveRecentFyActuals),
            forecast_lookback_fy_count=forecastLookbackFyCount,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Fundamental ranking error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get fundamental rankings: {e}",
        )


@router.get(
    "/api/analytics/value-composite-ranking",
    response_model=ranking_contracts.ValueCompositeRankingResponse,
    summary="Get value-composite rankings",
    description=(
        "Get the standard-market value composite ranking based on small market cap, "
        "low PBR, and low forward PER. The score intentionally does not apply an ADV60 floor."
    ),
)
async def get_value_composite_ranking(
    request: Request,
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(50, ge=1, le=200),
    markets: str = Query("standard"),
    profileId: ranking_contracts.ValueCompositeProfileId | None = Query(None),
    scoreMethod: ranking_contracts.ValueCompositeScoreMethod | None = Query(None),
    forwardEpsMode: ranking_contracts.ValueCompositeForwardEpsMode = Query("latest"),
    applyLiquidityFilter: bool = Query(True),
) -> ranking_contracts.ValueCompositeRankingResponse:
    """小型バリュー複合スコアランキングを取得"""
    from src.application.services.ranking_service import RankingService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = RankingService(reader)
    try:
        return service.get_value_composite_ranking(
            date=date,
            limit=limit,
            markets=markets,
            score_method=scoreMethod,
            profile_id=profileId,
            forward_eps_mode=forwardEpsMode,
            apply_liquidity_filter=applyLiquidityFilter,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Value composite ranking error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get value composite rankings: {e}",
        )


@router.get(
    "/api/analytics/value-composite-score/{code}",
    response_model=ranking_contracts.ValueCompositeScoreResponse,
    summary="Get a single-symbol value-composite score",
    description=(
        "Get a market-specific value composite score for one symbol. Prime uses prime_size_tilt, "
        "Standard uses standard_pbr_tilt, and unsupported markets return scoreAvailable=false."
    ),
)
async def get_value_composite_score(
    code: str,
    request: Request,
    date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    forwardEpsMode: ranking_contracts.ValueCompositeForwardEpsMode = Query("latest"),
) -> ranking_contracts.ValueCompositeScoreResponse:
    """単一銘柄の小型バリュー複合スコアを取得"""
    from src.application.services.ranking_service import RankingService

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")

    service = RankingService(reader)
    try:
        return service.get_value_composite_score(
            code=code,
            date=date,
            forward_eps_mode=forwardEpsMode,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(f"Value composite score error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get value composite score: {e}",
        )


# --- Factor Regression ---


@router.get(
    "/api/analytics/factor-regression/{symbol}",
    response_model=factor_contracts.FactorRegressionResponse,
    summary="Analyze stock factor regression",
    description="Two-stage factor regression analysis for risk decomposition.",
)
async def get_factor_regression(
    request: Request,
    symbol: str,
    lookbackDays: int = Query(252, ge=60, le=1000),
) -> factor_contracts.FactorRegressionResponse:
    """ファクター回帰分析を実行"""
    from src.application.services.factor_regression_service import (
        FactorRegressionService,
    )

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
        raise HTTPException(
            status_code=400, detail=f"Screeningジョブではありません: {job.job_type}"
        )
    return job


def _resolve_screening_job_request(
    job: JobInfo,
) -> screening_contracts.ScreeningJobRequest:
    params = screening_job_service.get_job_request(job.job_id)
    if isinstance(params, screening_contracts.ScreeningJobRequest):
        return params

    run_parameters = getattr(job.run_spec, "parameters", None)
    if not isinstance(run_parameters, dict):
        return screening_contracts.ScreeningJobRequest()

    request_payload = {
        key: value
        for key, value in run_parameters.items()
        if key in _SCREENING_JOB_REQUEST_FIELDS
    }
    try:
        return screening_contracts.ScreeningJobRequest.model_validate(request_payload)
    except Exception:
        logger.warning(
            "Failed to restore screening job request from run_spec", job_id=job.job_id
        )
        return screening_contracts.ScreeningJobRequest()


def _resolve_screening_job_scope_label(
    job: JobInfo,
    params: screening_contracts.ScreeningJobRequest,
) -> str | None:
    scope_label = screening_job_service.get_job_scope_label(job.job_id)
    if isinstance(scope_label, str) and scope_label:
        return scope_label

    run_parameters = getattr(job.run_spec, "parameters", None)
    if isinstance(run_parameters, dict):
        persisted_scope_label = run_parameters.get("scopeLabel")
        if isinstance(persisted_scope_label, str) and persisted_scope_label:
            return persisted_scope_label

    raw_response = (
        job.raw_result.get("response") if isinstance(job.raw_result, dict) else None
    )
    if isinstance(raw_response, dict):
        result_scope_label = raw_response.get("scopeLabel")
        if isinstance(result_scope_label, str) and result_scope_label:
            return result_scope_label

    return (
        format_market_scope_label(params.markets.split(",")) if params.markets else None
    )


def _build_screening_job_response(
    job: JobInfo,
) -> screening_job_schema.ScreeningJobResponse:
    """JobInfo から ScreeningJobResponse を構築。"""
    params = _resolve_screening_job_request(job)
    scope_label = _resolve_screening_job_scope_label(job, params)

    return screening_job_schema.ScreeningJobResponse(
        **build_job_response_base(job),
        entry_decidability=params.entry_decidability,
        markets=params.markets or "",
        scopeLabel=scope_label,
        strategies=params.strategies,
        recentDays=params.recentDays,
        referenceDate=params.date,
        sortBy=params.sortBy,
        order=params.order,
        limit=params.limit,
    )


async def _screening_job_event_generator(job_id: str):
    queue = screening_job_manager.subscribe(job_id)
    try:
        job = screening_job_manager.get_job(job_id)
        if job is None:
            yield {
                "event": "error",
                "data": json.dumps(
                    {"job_id": job_id, "message": "ジョブが見つかりません"},
                    ensure_ascii=False,
                ),
            }
            return

        yield {
            "event": "snapshot",
            "data": _build_screening_job_response(job).model_dump_json(),
        }
        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            return

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                yield {"event": "heartbeat", "data": "{}"}
                continue

            if event is None:
                return

            latest_job = screening_job_manager.get_job(job_id)
            if latest_job is None:
                return

            yield {
                "event": "job",
                "data": _build_screening_job_response(latest_job).model_dump_json(),
            }
            if latest_job.status in (
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
            ):
                return
    finally:
        screening_job_manager.unsubscribe(job_id, queue)


@router.post(
    "/api/analytics/screening/jobs",
    response_model=screening_job_schema.ScreeningJobResponse,
    status_code=202,
    summary="Create screening job",
    description="Submit an async screening job.",
)
async def create_screening_job(
    request: Request,
    payload: screening_contracts.ScreeningJobRequest,
) -> screening_job_schema.ScreeningJobResponse:
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
    response_model=screening_job_schema.ScreeningJobResponse,
    summary="Get screening job status",
)
async def get_screening_job(job_id: str) -> screening_job_schema.ScreeningJobResponse:
    """Screening ジョブ状態を取得"""
    return _build_screening_job_response(_get_screening_job_or_404(job_id))


@router.get(
    "/api/analytics/screening/jobs/{job_id}/stream",
    operation_id="stream_screening_job",
    response_class=EventSourceResponse,
    responses={
        200: {
            "content": {"text/event-stream": {"schema": {"type": "string"}}},
            "description": "Screening job events stream",
        }
    },
    summary="Stream screening job events",
)
async def stream_screening_job(job_id: str) -> EventSourceResponse:
    _get_screening_job_or_404(job_id)
    return EventSourceResponse(_screening_job_event_generator(job_id))


@router.post(
    "/api/analytics/screening/jobs/{job_id}/cancel",
    response_model=screening_job_schema.ScreeningJobResponse,
    summary="Cancel screening job",
)
async def cancel_screening_job(
    job_id: str,
) -> screening_job_schema.ScreeningJobResponse:
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
    response_model=screening_contracts.MarketScreeningResponse,
    summary="Get screening result",
)
async def get_screening_result(
    job_id: str,
) -> screening_contracts.MarketScreeningResponse:
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
        payload = screening_contracts.ScreeningJobPayload.model_validate(job.raw_result)
        return screening_contracts.MarketScreeningResponse.model_validate(
            payload.response
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"結果の復元に失敗しました: {e}",
        ) from e


# --- Portfolio Factor Regression (Phase 3E-2) ---


@router.get(
    "/api/analytics/portfolio-factor-regression/{portfolioId}",
    response_model=portfolio_factor_contracts.PortfolioFactorRegressionResponse,
    summary="Analyze portfolio factor regression",
    description="ポートフォリオ全体のファクター回帰分析",
)
async def get_portfolio_factor_regression(
    request: Request,
    portfolioId: int,
    lookbackDays: int = Query(252, ge=60, le=1000),
) -> portfolio_factor_contracts.PortfolioFactorRegressionResponse:
    """ポートフォリオファクター回帰分析を実行"""
    from src.infrastructure.db.market.portfolio_db import PortfolioDb
    from src.application.services.portfolio_factor_regression_service import (
        PortfolioFactorRegressionService,
    )

    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Market database not initialized")

    portfolio_db: PortfolioDb | None = getattr(request.app.state, "portfolio_db", None)
    if portfolio_db is None:
        raise HTTPException(
            status_code=422, detail="Portfolio database not initialized"
        )

    service = PortfolioFactorRegressionService(reader, portfolio_db)
    try:
        return service.analyze(portfolioId, lookback_days=lookbackDays)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from e
        if (
            "insufficient" in msg.lower()
            or "no valid" in msg.lower()
            or "zero" in msg.lower()
            or "no stocks" in msg.lower()
        ):
            raise HTTPException(status_code=422, detail=msg) from e
        raise HTTPException(status_code=400, detail=msg) from e
    except Exception as e:
        logger.exception(f"Portfolio factor regression error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to analyze: {e}") from e
