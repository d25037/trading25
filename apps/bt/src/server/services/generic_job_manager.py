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
from typing import Generic, TypeVar

from src.server.schemas.job import JobStatus

TData = TypeVar("TData")
TProgress = TypeVar("TProgress")
TResult = TypeVar("TResult")


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
    last_progress_update: datetime | None = None


class GenericJobManager(Generic[TData, TProgress, TResult]):
    """単一アクティブジョブ制約付き汎用ジョブマネージャ"""

    def __init__(self, max_completed: int = 10) -> None:
        self._jobs: dict[str, JobInfo[TData, TProgress, TResult]] = {}
        self._lock = asyncio.Lock()
        self._max_completed = max_completed

    async def create_job(self, data: TData) -> JobInfo[TData, TProgress, TResult] | None:
        """ジョブを作成。アクティブジョブがある場合は None を返す。"""
        async with self._lock:
            if self.get_active_job() is not None:
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
            if job.status in (JobStatus.PENDING, JobStatus.RUNNING):
                return job
        return None

    def update_progress(self, job_id: str, progress: TProgress) -> None:
        job = self._jobs.get(job_id)
        if job and job.status in (JobStatus.PENDING, JobStatus.RUNNING):
            if job.status == JobStatus.PENDING:
                job.status = JobStatus.RUNNING
                job.started_at = datetime.now(UTC)
            job.progress = progress
            job.last_progress_update = datetime.now(UTC)

    def complete_job(self, job_id: str, result: TResult) -> None:
        job = self._jobs.get(job_id)
        if job and job.status in (JobStatus.PENDING, JobStatus.RUNNING):
            job.status = JobStatus.COMPLETED
            job.result = result
            job.completed_at = datetime.now(UTC)

    def fail_job(self, job_id: str, error: str) -> None:
        job = self._jobs.get(job_id)
        if job and job.status in (JobStatus.PENDING, JobStatus.RUNNING):
            job.status = JobStatus.FAILED
            job.error = error
            job.completed_at = datetime.now(UTC)

    async def cancel_job(self, job_id: str) -> bool:
        """ジョブをキャンセル。キャンセルできた場合 True。"""
        job = self._jobs.get(job_id)
        if job is None:
            return False
        if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            return False
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.now(UTC)
        job.cancelled.set()
        if job.task is not None:
            job.task.cancel()
            with suppress(asyncio.CancelledError):
                await job.task
        return True

    def is_cancelled(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        return job is not None and job.cancelled.is_set()

    def cleanup_old(self) -> int:
        """完了済みジョブを max_completed 超過分削除"""
        terminal = [
            j for j in self._jobs.values()
            if j.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
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
            if job.status in (JobStatus.PENDING, JobStatus.RUNNING):
                await self.cancel_job(job.job_id)
