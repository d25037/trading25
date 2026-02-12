"""
Backtest Signal Attribution Service.

Runs signal attribution as an async background job.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from loguru import logger

from src.lib.backtest_core.signal_attribution import SignalAttributionAnalyzer
from src.server.schemas.backtest import JobStatus
from src.server.services.job_manager import JobManager, job_manager


class BacktestAttributionService:
    """Signal attribution execution service."""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 1,
    ) -> None:
        self._manager = manager or job_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    async def submit_attribution(
        self,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
        shapley_top_n: int = 5,
        shapley_permutations: int = 128,
        random_seed: int | None = None,
    ) -> str:
        """Submit a signal attribution job."""
        job_id = self._manager.create_job(
            strategy_name=strategy_name,
            job_type="backtest_attribution",
        )

        task = asyncio.create_task(
            self._run_attribution(
                job_id=job_id,
                strategy_name=strategy_name,
                config_override=config_override,
                shapley_top_n=shapley_top_n,
                shapley_permutations=shapley_permutations,
                random_seed=random_seed,
            )
        )
        await self._manager.set_job_task(job_id, task)
        return job_id

    async def _run_attribution(
        self,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
    ) -> None:
        try:
            await self._manager.acquire_slot()
            await self._manager.update_job_status(
                job_id,
                JobStatus.RUNNING,
                message="シグナル寄与分析を開始しています...",
                progress=0.0,
            )

            logger.info(f"シグナル寄与分析開始: {job_id} (戦略: {strategy_name})")
            loop = asyncio.get_running_loop()

            result = await loop.run_in_executor(
                self._executor,
                self._execute_attribution_sync,
                job_id,
                strategy_name,
                loop,
                config_override,
                shapley_top_n,
                shapley_permutations,
                random_seed,
            )

            job = self._manager.get_job(job_id)
            if job is not None:
                job.raw_result = result

            await self._manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                message="シグナル寄与分析完了",
                progress=1.0,
            )
            logger.info(f"シグナル寄与分析完了: {job_id}")
        except asyncio.CancelledError:
            logger.info(f"シグナル寄与分析がキャンセルされました: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                message="シグナル寄与分析がキャンセルされました",
            )
        except Exception as e:
            logger.exception(f"シグナル寄与分析エラー: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="シグナル寄与分析に失敗しました",
                error=str(e),
            )
        finally:
            self._manager.release_slot()

    def _execute_attribution_sync(
        self,
        job_id: str,
        strategy_name: str,
        loop: asyncio.AbstractEventLoop,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
    ) -> dict[str, Any]:
        def progress_callback(message: str, progress: float) -> None:
            asyncio.run_coroutine_threadsafe(
                self._manager.update_job_status(
                    job_id,
                    JobStatus.RUNNING,
                    message=message,
                    progress=progress,
                ),
                loop,
            )

        analyzer = SignalAttributionAnalyzer(
            strategy_name=strategy_name,
            config_override=config_override,
            shapley_top_n=shapley_top_n,
            shapley_permutations=shapley_permutations,
            random_seed=random_seed,
        )
        return analyzer.run(progress_callback=progress_callback)


backtest_attribution_service = BacktestAttributionService()

