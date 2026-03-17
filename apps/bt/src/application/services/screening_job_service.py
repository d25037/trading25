"""
Screening Job Service

非同期 Screening 実行ジョブの管理。
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from loguru import logger

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.screening_job import ScreeningJobPayload, ScreeningJobRequest
from src.application.services.job_manager import JobManager
from src.application.services.run_contracts import build_parameterized_run_spec
from src.application.services.screening_service import ScreeningService
from src.shared.observability.correlation import get_correlation_id
from src.shared.observability.metrics import metrics_recorder


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError("must be > 0")
        return value
    except ValueError:
        logger.warning(f"Invalid {name}={raw}. Fallback to {default}.")
        return default


class ScreeningJobService:
    """Screening ジョブ実行サービス"""

    def __init__(self, manager: JobManager, max_workers: int = 1) -> None:
        self._manager = manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._job_requests: dict[str, ScreeningJobRequest] = {}

    def get_job_request(self, job_id: str) -> ScreeningJobRequest | None:
        """ジョブIDに紐づくリクエストを取得"""
        return self._job_requests.get(job_id)

    async def submit_screening(
        self,
        reader: MarketDbReader,
        request: ScreeningJobRequest,
    ) -> str:
        """Screening ジョブをサブミット"""
        request_copy = request.model_copy(deep=True)
        run_spec = build_parameterized_run_spec(
            "screening",
            "analytics/screening",
            parameters=request_copy.model_dump(exclude_none=True),
        )
        job_id = self._manager.create_job(
            strategy_name="analytics/screening",
            job_type="screening",
            run_spec=run_spec,
        )
        self._job_requests[job_id] = request_copy

        task = asyncio.create_task(self._run_job(job_id, reader, request_copy))
        await self._manager.set_job_task(job_id, task)

        return job_id

    async def _run_job(
        self,
        job_id: str,
        reader: MarketDbReader,
        request: ScreeningJobRequest,
    ) -> None:
        """バックグラウンドで Screening を実行"""
        started_at = perf_counter()
        loop = asyncio.get_running_loop()
        acquired_slot = False

        def progress_callback(completed: int, total: int) -> None:
            if total <= 0:
                progress = 0.0
            else:
                progress = max(0.0, min(1.0, completed / total))

            message = f"スクリーニング評価 {completed}/{total}"
            loop.call_soon_threadsafe(
                asyncio.create_task,
                self._manager.update_job_status(
                    job_id,
                    JobStatus.RUNNING,
                    progress=progress,
                    message=message,
                ),
            )

        try:
            await self._manager.acquire_slot()
            acquired_slot = True
            await self._manager.update_job_status(
                job_id,
                JobStatus.RUNNING,
                progress=0.0,
                message="Screening ジョブを開始しました",
            )

            service = ScreeningService(reader)
            response = await loop.run_in_executor(
                self._executor,
                lambda: service.run_screening(
                    entry_decidability=request.entry_decidability,
                    markets=request.markets,
                    strategies=request.strategies,
                    recent_days=request.recentDays,
                    reference_date=request.date,
                    sort_by=request.sortBy,
                    order=request.order,
                    limit=request.limit,
                    progress_callback=progress_callback,
                ),
            )

            payload = ScreeningJobPayload(response=response.model_dump())
            await self._manager.set_job_raw_result(job_id, payload.model_dump())
            await self._manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                progress=1.0,
                message="Screening ジョブが完了しました",
            )

            elapsed_ms = round((perf_counter() - started_at) * 1000, 2)
            metrics_recorder.record_job_duration("screening", JobStatus.COMPLETED.value, elapsed_ms)
            logger.info(
                f"Screening job completed: {job_id} ({elapsed_ms}ms)",
                event="job_lifecycle",
                jobType="screening",
                jobId=job_id,
                status=JobStatus.COMPLETED.value,
                durationMs=elapsed_ms,
                correlationId=get_correlation_id(),
            )

        except asyncio.CancelledError:
            elapsed_ms = round((perf_counter() - started_at) * 1000, 2)
            metrics_recorder.record_job_duration("screening", JobStatus.CANCELLED.value, elapsed_ms)
            await self._manager.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                message="Screening ジョブがキャンセルされました",
            )
            logger.info(
                f"Screening job cancelled: {job_id} ({elapsed_ms}ms)",
                event="job_lifecycle",
                jobType="screening",
                jobId=job_id,
                status=JobStatus.CANCELLED.value,
                durationMs=elapsed_ms,
                correlationId=get_correlation_id(),
            )
            raise

        except Exception as exc:
            elapsed_ms = round((perf_counter() - started_at) * 1000, 2)
            metrics_recorder.record_job_duration("screening", JobStatus.FAILED.value, elapsed_ms)
            logger.exception(
                f"Screening job failed: {job_id}",
                event="job_lifecycle",
                jobType="screening",
                jobId=job_id,
                status=JobStatus.FAILED.value,
                durationMs=elapsed_ms,
                correlationId=get_correlation_id(),
            )
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="Screening ジョブに失敗しました",
                error=str(exc),
            )

        finally:
            if acquired_slot:
                self._manager.release_slot()

    async def shutdown(self) -> None:
        """Executor を終了する。job state の遷移は manager/app 側で扱う。"""
        if not bool(getattr(self._executor, "_broken", False)) and not bool(
            getattr(self._executor, "_shutdown", False)
        ):
            self._executor.shutdown(wait=True)


screening_job_manager = JobManager(
    max_concurrent_jobs=_read_positive_int_env(
        "BT_SCREENING_MAX_CONCURRENT_JOBS",
        default=1,
    )
)

screening_job_service = ScreeningJobService(
    manager=screening_job_manager,
    max_workers=_read_positive_int_env("BT_SCREENING_JOB_EXECUTOR_WORKERS", default=1),
)
