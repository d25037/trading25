"""
FastAPI Application

trading25-bt バックテストAPIサーバー
"""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from src.config.settings import get_settings
from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.middleware.correlation import CorrelationIdMiddleware, get_correlation_id
from src.server.middleware.request_logger import RequestLoggerMiddleware
from src.server.openapi_config import customize_openapi, get_openapi_config
from src.server.routes import backtest, fundamentals, health, indicators, lab, ohlcv, optimize, signal_reference, strategies
from src.server.routes import analytics_complex, analytics_jquants, chart, jquants_proxy, market_data
from src.server.routes import dataset, dataset_data, db, portfolio, watchlist
from src.server.schemas.error import ErrorDetail, ErrorResponse
from sqlalchemy.exc import SQLAlchemyError

from src.lib.market_db.market_reader import MarketDbReader
from src.lib.market_db.market_db import MarketDb
from src.lib.market_db.portfolio_db import PortfolioDb
from src.server.services.backtest_attribution_service import backtest_attribution_service
from src.server.services.backtest_service import backtest_service
from src.server.services.job_manager import job_manager
from src.server.services.jquants_proxy_service import JQuantsProxyService
from src.server.services.lab_service import lab_service
from src.server.services.chart_service import ChartService
from src.server.services.margin_analytics_service import MarginAnalyticsService
from src.server.services.market_data_service import MarketDataService
from src.server.services.optimization_service import optimization_service
from src.server.services.dataset_resolver import DatasetResolver
from src.server.services.roe_service import ROEService

# HTTP ステータスコード → ステータステキスト
_STATUS_TEXT: dict[int, str] = {
    400: "Bad Request",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    500: "Internal Server Error",
    501: "Not Implemented",
}


def _status_text(status_code: int) -> str:
    """ステータスコードからテキストを取得"""
    return _STATUS_TEXT.get(status_code, f"Error {status_code}")


async def _periodic_cleanup(interval_seconds: int = 3600) -> None:
    """古いジョブを定期的にクリーンアップ"""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            deleted = job_manager.cleanup_old_jobs(max_age_hours=24)
            if deleted > 0:
                logger.info(f"定期クリーンアップ: {deleted}件のジョブを削除")
        except Exception as e:
            logger.warning(f"定期クリーンアップエラー: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """アプリケーションのライフサイクル管理"""
    logger.info("trading25-bt API サーバーを起動しています...")

    # JQuants async client (Phase 3B-1)
    settings = get_settings()
    jquants_client = JQuantsAsyncClient(
        api_key=settings.jquants_api_key,
        plan=settings.jquants_plan,
    )
    app.state.jquants_client = jquants_client
    app.state.jquants_proxy_service = JQuantsProxyService(jquants_client)
    app.state.roe_service = ROEService(jquants_client)
    app.state.margin_analytics_service = MarginAnalyticsService(jquants_client)

    # market.db reader (Phase 3B-2a)
    market_reader: MarketDbReader | None = None
    if settings.market_db_path:
        try:
            market_reader = MarketDbReader(settings.market_db_path)
            app.state.market_data_service = MarketDataService(market_reader)
            logger.info(f"market.db 読み取りリーダーを初期化: {settings.market_db_path}")
        except Exception as e:
            logger.warning(f"market.db の初期化に失敗: {e}")
            app.state.market_data_service = None
    else:
        app.state.market_data_service = None

    # Phase 3B-3: market_reader を直接公開（ranking/factor-regression/screening 用）
    app.state.market_reader = market_reader

    # Chart service (Phase 3B-2b) — market.db reader + JQuants fallback
    app.state.chart_service = ChartService(market_reader, jquants_client)

    # Phase 3C: SQLAlchemy Core DB accessors
    market_db: MarketDb | None = None
    if settings.market_db_path:
        try:
            market_db = MarketDb(settings.market_db_path, read_only=False)
            logger.info(f"MarketDb (SQLAlchemy) を初期化: {settings.market_db_path}")
        except Exception as e:
            logger.warning(f"MarketDb の初期化に失敗: {e}")
    app.state.market_db = market_db

    portfolio_db: PortfolioDb | None = None
    if settings.portfolio_db_path:
        try:
            portfolio_db = PortfolioDb(settings.portfolio_db_path)
            logger.info(f"PortfolioDb を初期化: {settings.portfolio_db_path}")
        except Exception as e:
            logger.warning(f"PortfolioDb の初期化に失敗: {e}")
    app.state.portfolio_db = portfolio_db

    app.state.dataset_base_path = settings.dataset_base_path

    # Phase 3D: DatasetResolver
    dataset_resolver: DatasetResolver | None = None
    if settings.dataset_base_path:
        try:
            dataset_resolver = DatasetResolver(settings.dataset_base_path)
            logger.info(f"DatasetResolver を初期化: {settings.dataset_base_path}")
        except Exception as e:
            logger.warning(f"DatasetResolver の初期化に失敗: {e}")
    app.state.dataset_resolver = dataset_resolver

    cleanup_task = asyncio.create_task(_periodic_cleanup())

    yield

    # JQuants client shutdown
    await jquants_client.close()

    # market.db reader shutdown
    if market_reader is not None:
        market_reader.close()

    # Phase 3C: SQLAlchemy DB shutdown
    if market_db is not None:
        market_db.close()
    if portfolio_db is not None:
        portfolio_db.close()

    # Phase 3D: DatasetResolver shutdown
    if dataset_resolver is not None:
        dataset_resolver.close_all()

    # Phase 3D: Job manager shutdown
    from src.server.services.sync_service import sync_job_manager
    from src.server.services.dataset_builder_service import dataset_job_manager
    await sync_job_manager.shutdown()
    await dataset_job_manager.shutdown()

    # クリーンアップタスクを停止
    cleanup_task.cancel()
    with suppress(asyncio.CancelledError):
        await cleanup_task

    # ThreadPoolExecutorをシャットダウン
    # NOTE: モジュールレベル executor は shutdown 後に再利用不可。
    # テスト環境で同一プロセス内の複数 lifespan サイクルに対応するため
    # _broken フラグを確認してから shutdown する。
    for executor in [
        backtest_service._executor,
        backtest_attribution_service._executor,
        optimization_service._executor,
        lab_service._executor,
        indicators._executor,
        ohlcv._executor,
        fundamentals._executor,
    ]:
        if not bool(getattr(executor, "_broken", False)) and not bool(getattr(executor, "_shutdown", False)):
            executor.shutdown(wait=True)

    # Fundamentals service cleanup (close API clients)
    from src.server.services.fundamentals_service import fundamentals_service

    fundamentals_service.close()

    logger.info("trading25-bt API サーバーをシャットダウンしています...")


def _build_error_response(status_code: int, message: str, details: list[ErrorDetail] | None = None) -> JSONResponse:
    """統一エラーレスポンスを構築"""
    body = ErrorResponse(
        status="error",
        error=_status_text(status_code),
        message=message,
        details=details,
        timestamp=datetime.now(UTC).isoformat(),
        correlationId=get_correlation_id(),
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(exclude_none=True))


def create_app() -> FastAPI:
    """FastAPIアプリケーションを作成"""
    openapi_config = get_openapi_config()
    app = FastAPI(
        lifespan=lifespan,
        **openapi_config,
    )

    # カスタム OpenAPI スキーマ（ErrorResponse 共通注入）
    app.openapi = lambda: customize_openapi(app)  # type: ignore[assignment]

    # --- ミドルウェア登録（LIFO: 下から上に実行される） ---

    # 3番目に登録 = 1番目に実行（最外側）: リクエストロギング
    app.add_middleware(RequestLoggerMiddleware)

    # 2番目に登録 = 2番目に実行: Correlation ID
    app.add_middleware(CorrelationIdMiddleware)

    # 1番目に登録 = 3番目に実行（最内側）: CORS
    origins = [
        "http://localhost:5173",  # ts Web (dev)
        "http://localhost:4173",  # ts Web (preview)
        "http://127.0.0.1:5173",
        "http://127.0.0.1:4173",
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "x-correlation-id"],
        expose_headers=["x-correlation-id"],
    )

    # 例外ハンドラ: HTTPException → 統一エラーレスポンス
    @app.exception_handler(HTTPException)
    async def http_exception_handler(  # pyright: ignore[reportUnusedFunction]
        _request: Request, exc: HTTPException
    ) -> JSONResponse:
        message = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
        return _build_error_response(exc.status_code, message)

    # 例外ハンドラ: RequestValidationError → 統一エラーレスポンス + details
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(  # pyright: ignore[reportUnusedFunction]
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = []
        for err in exc.errors():
            loc = err.get("loc", ())
            field = ".".join(str(part) for part in loc if part != "body")
            details.append(ErrorDetail(field=field or "unknown", message=err.get("msg", "Validation error")))
        return _build_error_response(422, "Validation failed", details)

    # 例外ハンドラ: SQLAlchemyError → 統一エラーレスポンス (DB 系)
    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_exception_handler(  # pyright: ignore[reportUnusedFunction]
        _request: Request, exc: SQLAlchemyError
    ) -> JSONResponse:
        logger.exception(f"Database error: {exc}")
        return _build_error_response(500, "Database error")

    # 例外ハンドラ: 汎用 Exception → 統一エラーレスポンス (安全ネット)
    @app.exception_handler(Exception)
    async def generic_exception_handler(  # pyright: ignore[reportUnusedFunction]
        _request: Request, exc: Exception
    ) -> JSONResponse:
        logger.exception(f"Unhandled exception: {exc}")
        return _build_error_response(500, "Internal server error")

    # ルーターを登録
    app.include_router(health.router)
    app.include_router(strategies.router)
    app.include_router(backtest.router)
    app.include_router(optimize.router)
    app.include_router(signal_reference.router)
    app.include_router(lab.router)
    app.include_router(indicators.router)
    app.include_router(ohlcv.router)
    app.include_router(fundamentals.router)
    # Phase 3B-1: JQuants Proxy + Analytics
    app.include_router(jquants_proxy.router)
    app.include_router(analytics_jquants.router)
    # Phase 3B-2a: Market Data (market.db)
    app.include_router(market_data.router)
    # Phase 3B-2b: Chart + Sector Stocks
    app.include_router(chart.router)
    # Phase 3B-3: Complex Analytics (Ranking, Factor Regression, Screening)
    app.include_router(analytics_complex.router)
    # Phase 3D: Dataset Data + Dataset Management + DB
    app.include_router(db.router)
    app.include_router(dataset_data.router)
    app.include_router(dataset.router)
    # Phase 3E: Portfolio + Watchlist
    app.include_router(portfolio.router)
    app.include_router(watchlist.router)

    return app


# アプリケーションインスタンス
app = create_app()
