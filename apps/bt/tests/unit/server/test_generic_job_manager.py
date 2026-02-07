"""GenericJobManager のユニットテスト"""

from __future__ import annotations

import asyncio

import pytest

from src.server.schemas.job import JobStatus
from src.server.services.generic_job_manager import GenericJobManager


@pytest.fixture
def manager():
    return GenericJobManager[str, str, str](max_completed=3)


class TestGenericJobManager:
    @pytest.mark.asyncio
    async def test_create_job(self, manager) -> None:
        job = await manager.create_job("test-data")
        assert job is not None
        assert job.status == JobStatus.PENDING
        assert job.data == "test-data"

    @pytest.mark.asyncio
    async def test_single_active_constraint(self, manager) -> None:
        job1 = await manager.create_job("first")
        assert job1 is not None
        job2 = await manager.create_job("second")
        assert job2 is None  # Blocked by first

    @pytest.mark.asyncio
    async def test_create_after_complete(self, manager) -> None:
        job1 = await manager.create_job("first")
        manager.complete_job(job1.job_id, "done")
        job2 = await manager.create_job("second")
        assert job2 is not None

    @pytest.mark.asyncio
    async def test_get_job(self, manager) -> None:
        job = await manager.create_job("data")
        retrieved = manager.get_job(job.job_id)
        assert retrieved is not None
        assert retrieved.job_id == job.job_id

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, manager) -> None:
        assert manager.get_job("nonexistent") is None

    @pytest.mark.asyncio
    async def test_update_progress(self, manager) -> None:
        job = await manager.create_job("data")
        manager.update_progress(job.job_id, "50%")
        assert job.progress == "50%"
        assert job.status == JobStatus.RUNNING  # Auto-transition

    @pytest.mark.asyncio
    async def test_complete_job(self, manager) -> None:
        job = await manager.create_job("data")
        manager.complete_job(job.job_id, "result")
        assert job.status == JobStatus.COMPLETED
        assert job.result == "result"
        assert job.completed_at is not None

    @pytest.mark.asyncio
    async def test_fail_job(self, manager) -> None:
        job = await manager.create_job("data")
        manager.fail_job(job.job_id, "error msg")
        assert job.status == JobStatus.FAILED
        assert job.error == "error msg"

    @pytest.mark.asyncio
    async def test_cancel_job(self, manager) -> None:
        job = await manager.create_job("data")
        result = await manager.cancel_job(job.job_id)
        assert result is True
        assert job.status == JobStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_completed_fails(self, manager) -> None:
        job = await manager.create_job("data")
        manager.complete_job(job.job_id, "done")
        result = await manager.cancel_job(job.job_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_is_cancelled(self, manager) -> None:
        job = await manager.create_job("data")
        assert not manager.is_cancelled(job.job_id)
        await manager.cancel_job(job.job_id)
        assert manager.is_cancelled(job.job_id)

    @pytest.mark.asyncio
    async def test_cancel_with_task(self, manager) -> None:
        job = await manager.create_job("data")
        cancelled_flag = asyncio.Event()

        async def long_task():
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                cancelled_flag.set()
                raise

        job.task = asyncio.create_task(long_task())
        await asyncio.sleep(0.01)  # Let task start
        await manager.cancel_job(job.job_id)
        await asyncio.sleep(0.01)  # Let cancellation propagate
        assert cancelled_flag.is_set()

    @pytest.mark.asyncio
    async def test_cleanup_old(self, manager) -> None:
        # Create 5 completed jobs (max is 3)
        for i in range(5):
            job = await manager.create_job(f"data-{i}")
            manager.complete_job(job.job_id, f"result-{i}")

        deleted = manager.cleanup_old()
        assert deleted == 2  # 5 - 3 = 2

    @pytest.mark.asyncio
    async def test_get_active_job(self, manager) -> None:
        assert manager.get_active_job() is None
        job = await manager.create_job("data")
        assert manager.get_active_job() is job
        manager.complete_job(job.job_id, "done")
        assert manager.get_active_job() is None

    @pytest.mark.asyncio
    async def test_shutdown(self, manager) -> None:
        job = await manager.create_job("data")
        await manager.shutdown()
        assert job.status == JobStatus.CANCELLED
