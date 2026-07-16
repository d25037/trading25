"""
Generic Job Manager

単一アクティブジョブ制約付き汎用ジョブマネージャ。
DB Sync / Dataset Build 用（バックテスト用 JobManager とは別）。
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from collections.abc import Callable
from typing import Generic, TypeVar

from src.application.contracts.jobs import JobStatus

TData = TypeVar("TData")
TProgress = TypeVar("TProgress")
TResult = TypeVar("TResult")

ACTIVE_GENERIC_JOB_STATUSES = (JobStatus.PENDING, JobStatus.RUNNING)
TERMINAL_GENERIC_JOB_STATUSES = (
    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
)


@dataclass
class JobInfo(Generic[TData, TProgress, TResult]):
    job_id: str
    status: JobStatus
    data: TData
    progress: TProgress | None = None
    result: TResult | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    task: asyncio.Task[None] | None = None
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    publication_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_progress_update: datetime | None = None


class GenericJobManager(Generic[TData, TProgress, TResult]):
    """単一アクティブジョブ制約付き汎用ジョブマネージャ"""

    def __init__(self, max_completed: int = 10) -> None:
        self._jobs: dict[str, JobInfo[TData, TProgress, TResult]] = {}
        self._lock = asyncio.Lock()
        self._max_completed = max_completed

    def _get_inflight_job(self) -> JobInfo[TData, TProgress, TResult] | None:
        for job in self._jobs.values():
            if job.status in ACTIVE_GENERIC_JOB_STATUSES:
                return job
            if job.task is not None and not job.task.done():
                return job
        return None

    @staticmethod
    def _consume_task_result(task: asyncio.Task[None]) -> None:
        with suppress(asyncio.CancelledError):
            task.result()

    async def create_job(
        self, data: TData
    ) -> JobInfo[TData, TProgress, TResult] | None:
        """ジョブを作成。アクティブジョブがある場合は None を返す。"""
        async with self._lock:
            if self._get_inflight_job() is not None:
                return None
            job_id = str(uuid.uuid4())
            job = JobInfo(
                job_id=job_id,
                status=JobStatus.PENDING,
                data=data,
            )
            self._jobs[job_id] = job
            return job

    def get_job(self, job_id: str) -> JobInfo[TData, TProgress, TResult] | None:
        return self._jobs.get(job_id)

    def get_active_job(self) -> JobInfo[TData, TProgress, TResult] | None:
        for job in self._jobs.values():
            if job.status in ACTIVE_GENERIC_JOB_STATUSES:
                return job
        return None

    def update_progress(self, job_id: str, progress: TProgress) -> None:
        job = self._jobs.get(job_id)
        if job and job.status in ACTIVE_GENERIC_JOB_STATUSES:
            if job.status == JobStatus.PENDING:
                job.status = JobStatus.RUNNING
                job.started_at = datetime.now(UTC)
            job.progress = progress
            job.last_progress_update = datetime.now(UTC)

    def complete_job(self, job_id: str, result: TResult) -> None:
        job = self._jobs.get(job_id)
        if (
            job
            and job.status in ACTIVE_GENERIC_JOB_STATUSES
            and not job.cancelled.is_set()
        ):
            job.status = JobStatus.COMPLETED
            job.result = result
            job.completed_at = datetime.now(UTC)

    async def complete_job_with_publication(
        self,
        job_id: str,
        result: TResult,
        publish: Callable[[], None],
        *,
        final_progress: TProgress | None = None,
    ) -> bool:
        """Atomically publish an artifact and commit the terminal success state."""
        job = self._jobs.get(job_id)
        if job is None:
            return False
        async with job.publication_lock:
            if job.status not in ACTIVE_GENERIC_JOB_STATUSES or job.cancelled.is_set():
                return False
            publish()
            if final_progress is not None:
                job.progress = final_progress
                job.last_progress_update = datetime.now(UTC)
            job.status = JobStatus.COMPLETED
            job.result = result
            job.error = None
            job.completed_at = datetime.now(UTC)
            return True

    def fail_job(self, job_id: str, error: str) -> None:
        job = self._jobs.get(job_id)
        if (
            job
            and job.status in ACTIVE_GENERIC_JOB_STATUSES
            and not job.cancelled.is_set()
        ):
            job.status = JobStatus.FAILED
            job.error = error
            job.completed_at = datetime.now(UTC)

    def finalize_job(
        self,
        job_id: str,
        *,
        status: JobStatus,
        result: TResult | None = None,
        error: str | None = None,
    ) -> bool:
        """Commit the joined outer finalizer's terminal decision.

        Unlike complete_job/fail_job this deliberately resolves a pending cancel
        request: lifecycle failures may override cancellation, and cancellation
        becomes terminal only after the writer finalizer has joined.
        """
        if status not in TERMINAL_GENERIC_JOB_STATUSES:
            raise ValueError("Finalized job status must be terminal")
        job = self._jobs.get(job_id)
        if job is None or job.status not in ACTIVE_GENERIC_JOB_STATUSES:
            return False
        job.status = status
        job.result = result
        job.error = error
        job.completed_at = datetime.now(UTC)
        return True

    async def cancel_job(self, job_id: str, *, wait: bool = True) -> bool:
        """ジョブをキャンセル。キャンセルできた場合 True。"""
        job = self._jobs.get(job_id)
        if job is None:
            return False
        async with job.publication_lock:
            if job.status not in ACTIVE_GENERIC_JOB_STATUSES or job.cancelled.is_set():
                return False
            job.cancelled.set()
            if not wait:
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.now(UTC)
        if wait:
            if job.task is not None:
                job.task.cancel()
                with suppress(asyncio.CancelledError):
                    await job.task
            async with job.publication_lock:
                if job.status in ACTIVE_GENERIC_JOB_STATUSES:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = datetime.now(UTC)
        elif job.task is not None:
            job.task.add_done_callback(self._consume_task_result)
        return True

    def is_cancelled(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        return job is not None and job.cancelled.is_set()

    def cleanup_old(self) -> int:
        """完了済みジョブを max_completed 超過分削除"""
        terminal = [
            j for j in self._jobs.values() if j.status in TERMINAL_GENERIC_JOB_STATUSES
        ]
        terminal.sort(key=lambda j: j.completed_at or j.created_at)
        deleted = 0
        while len(terminal) > self._max_completed:
            old = terminal.pop(0)
            del self._jobs[old.job_id]
            deleted += 1
        return deleted

    async def shutdown(self) -> None:
        """アクティブジョブをキャンセル"""
        for job in list(self._jobs.values()):
            if job.status in ACTIVE_GENERIC_JOB_STATUSES:
                await self.cancel_job(job.job_id)
