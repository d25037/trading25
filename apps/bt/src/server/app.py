"""
FastAPI Application

trading25-bt バックテストAPIサーバー
"""

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from src.server.routes import backtest, fundamentals, health, indicators, lab, ohlcv, optimize, signal_reference, strategies
from src.server.services.backtest_service import backtest_service
from src.server.services.job_manager import job_manager
from src.server.services.lab_service import lab_service
from src.server.services.optimization_service import optimization_service


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


def create_app() -> FastAPI:
    """FastAPIアプリケーションを作成"""
    app = FastAPI(
        title="trading25-bt API",
        description="バックテスト実行のためのREST API",
        version="0.1.0",
        lifespan=lifespan,
    )

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
