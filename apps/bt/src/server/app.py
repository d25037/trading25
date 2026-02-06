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

from src.server.middleware.correlation import CorrelationIdMiddleware, get_correlation_id
from src.server.routes import backtest, fundamentals, health, indicators, lab, ohlcv, optimize, signal_reference, strategies
from src.server.schemas.error import ErrorDetail, ErrorResponse
from src.server.services.backtest_service import backtest_service
from src.server.services.job_manager import job_manager
from src.server.services.lab_service import lab_service
from src.server.services.optimization_service import optimization_service

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

    cleanup_task = asyncio.create_task(_periodic_cleanup())

    yield

    # クリーンアップタスクを停止
    cleanup_task.cancel()
    with suppress(asyncio.CancelledError):
        await cleanup_task

    # ThreadPoolExecutorをシャットダウン
    backtest_service._executor.shutdown(wait=True)
    optimization_service._executor.shutdown(wait=True)
    lab_service._executor.shutdown(wait=True)
    indicators._executor.shutdown(wait=True)
    ohlcv._executor.shutdown(wait=True)
    fundamentals._executor.shutdown(wait=True)

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
    app = FastAPI(
        title="trading25-bt API",
        description="バックテスト実行のためのREST API",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Correlation ID ミドルウェア（CORS より先に追加 = レスポンス側では後に実行）
    app.add_middleware(CorrelationIdMiddleware)

    # CORS設定（開発環境）
    origins = [
        "http://localhost:3001",  # ts API
        "http://localhost:5173",  # ts Web (dev)
        "http://127.0.0.1:3001",
        "http://127.0.0.1:5173",
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
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

    return app


# アプリケーションインスタンス
app = create_app()
