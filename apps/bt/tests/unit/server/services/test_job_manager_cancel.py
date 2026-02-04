"""
JobManager キャンセル機能のユニットテスト
"""

import asyncio

import pytest

from src.server.schemas.backtest import JobStatus
from src.server.services.job_manager import JobManager


@pytest.fixture
def manager() -> JobManager:
    return JobManager(max_concurrent_jobs=2)


@pytest.mark.asyncio
class TestCancelJob:
    """cancel_job のテスト"""

    async def test_cancel_pending_job(self, manager: JobManager):
        """PENDINGジョブをキャンセルできる"""
        job_id = manager.create_job("test_strategy")
        result = await manager.cancel_job(job_id)
        assert result is not None
        assert result.status == JobStatus.CANCELLED
        assert result.completed_at is not None

    async def test_cancel_running_job(self, manager: JobManager):
        """RUNNINGジョブをキャンセルできる"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.RUNNING)
        result = await manager.cancel_job(job_id)
        assert result is not None
        assert result.status == JobStatus.CANCELLED

    async def test_cancel_completed_job_returns_none(self, manager: JobManager):
        """COMPLETEDジョブのキャンセルはNoneを返す"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.COMPLETED)
        result = await manager.cancel_job(job_id)
        assert result is None

    async def test_cancel_failed_job_returns_none(self, manager: JobManager):
        """FAILEDジョブのキャンセルはNoneを返す"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.FAILED)
        result = await manager.cancel_job(job_id)
        assert result is None

    async def test_cancel_already_cancelled_is_idempotent(self, manager: JobManager):
        """既にCANCELLEDのジョブを再キャンセルすると冪等に返す"""
        job_id = manager.create_job("test_strategy")
        await manager.cancel_job(job_id)
        result = await manager.cancel_job(job_id)
        assert result is not None
        assert result.status == JobStatus.CANCELLED

    async def test_cancel_nonexistent_job_returns_none(self, manager: JobManager):
        """存在しないジョブのキャンセルはNoneを返す"""
        result = await manager.cancel_job("nonexistent-id")
        assert result is None

    async def test_cancel_cancels_asyncio_task(self, manager: JobManager):
        """RUNNINGジョブのasyncio.Taskがキャンセルされる"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.RUNNING)

        # ダミータスクを設定
        async def _long_running():
            await asyncio.sleep(100)

        task = asyncio.create_task(_long_running())
        await manager.set_job_task(job_id, task)

        await manager.cancel_job(job_id)
        # タスクのキャンセルが反映されるのを待つ
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
class TestTerminalStateGuard:
    """terminal状態ガードのテスト"""

    async def test_cancelled_job_cannot_become_running(self, manager: JobManager):
        """CANCELLEDジョブはRUNNINGに巻き戻らない"""
        job_id = manager.create_job("test_strategy")
        await manager.cancel_job(job_id)
        await manager.update_job_status(job_id, JobStatus.RUNNING)

        updated = manager.get_job(job_id)
        assert updated is not None
        assert updated.status == JobStatus.CANCELLED

    async def test_completed_job_cannot_become_running(self, manager: JobManager):
        """COMPLETEDジョブはRUNNINGに巻き戻らない"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.COMPLETED)
        await manager.update_job_status(job_id, JobStatus.RUNNING)

        updated = manager.get_job(job_id)
        assert updated is not None
        assert updated.status == JobStatus.COMPLETED

    async def test_failed_job_cannot_become_running(self, manager: JobManager):
        """FAILEDジョブはRUNNINGに巻き戻らない"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.FAILED)
        await manager.update_job_status(job_id, JobStatus.RUNNING)

        updated = manager.get_job(job_id)
        assert updated is not None
        assert updated.status == JobStatus.FAILED
