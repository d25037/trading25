"""job_manager.py のテスト"""

import asyncio
from datetime import datetime, timedelta

import pytest

from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.entrypoints.http.schemas.common import SSEJobEvent
from src.application.services.job_manager import JobInfo, JobManager


class TestJobInfo:
    def test_creation(self):
        info = JobInfo("id1", "test_strategy")
        assert info.job_id == "id1"
        assert info.strategy_name == "test_strategy"
        assert info.job_type == "backtest"
        assert info.status == JobStatus.PENDING
        assert info.progress is None
        assert info.error is None

    def test_optimization_type(self):
        info = JobInfo("id1", "test_strategy", "optimization")
        assert info.job_type == "optimization"

    def test_raw_result_field(self):
        info = JobInfo("id1", "test_strategy")
        assert info.raw_result is None


class TestJobManager:
    def test_create_job(self):
        mgr = JobManager()
        job_id = mgr.create_job("test_strat")
        assert job_id is not None
        job = mgr.get_job(job_id)
        assert job is not None
        assert job.strategy_name == "test_strat"
        assert job.status == JobStatus.PENDING

    def test_get_nonexistent_job(self):
        mgr = JobManager()
        assert mgr.get_job("nonexistent") is None

    def test_list_jobs_sorted(self):
        mgr = JobManager()
        mgr.create_job("strat1")
        id2 = mgr.create_job("strat2")
        jobs = mgr.list_jobs()
        assert len(jobs) == 2
        assert jobs[0].job_id == id2

    def test_list_jobs_limit(self):
        mgr = JobManager()
        for i in range(5):
            mgr.create_job(f"strat{i}")
        jobs = mgr.list_jobs(limit=3)
        assert len(jobs) == 3

    def test_list_jobs_filter_by_job_types(self):
        mgr = JobManager()
        lab_id = mgr.create_job("lab_strat", job_type="lab_generate")
        mgr.create_job("bt_strat", job_type="backtest")

        jobs = mgr.list_jobs(job_types={"lab_generate"})

        assert len(jobs) == 1
        assert jobs[0].job_id == lab_id
        assert jobs[0].job_type == "lab_generate"

    @pytest.mark.asyncio
    async def test_update_job_status_running(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        await mgr.update_job_status(job_id, JobStatus.RUNNING, message="running")
        job = mgr.get_job(job_id)
        assert job.status == JobStatus.RUNNING
        assert job.message == "running"
        assert job.started_at is not None

    @pytest.mark.asyncio
    async def test_update_job_status_completed(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        await mgr.update_job_status(job_id, JobStatus.COMPLETED)
        job = mgr.get_job(job_id)
        assert job.status == JobStatus.COMPLETED
        assert job.completed_at is not None

    @pytest.mark.asyncio
    async def test_update_job_status_failed(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        await mgr.update_job_status(job_id, JobStatus.FAILED, error="some error")
        job = mgr.get_job(job_id)
        assert job.status == JobStatus.FAILED
        assert job.error == "some error"
        assert job.completed_at is not None

    @pytest.mark.asyncio
    async def test_update_nonexistent_job_no_error(self):
        mgr = JobManager()
        await mgr.update_job_status("fake", JobStatus.RUNNING)

    @pytest.mark.asyncio
    async def test_set_job_result(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        summary = BacktestResultSummary(
            total_return=0.1, sharpe_ratio=1.5, calmar_ratio=0.8,
            max_drawdown=-0.1, win_rate=0.6, trade_count=50,
        )
        await mgr.set_job_result(job_id, summary, {"key": "val"}, "/path/html", "ds", 10.5)
        job = mgr.get_job(job_id)
        assert job.result == summary
        assert job.html_path == "/path/html"
        assert job.execution_time == 10.5

    @pytest.mark.asyncio
    async def test_set_job_task(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")

        async def dummy():
            pass

        task = asyncio.create_task(dummy())
        await mgr.set_job_task(job_id, task)
        job = mgr.get_job(job_id)
        assert job.task is task
        await task

    def test_subscribe_and_unsubscribe(self):
        mgr = JobManager()
        q = mgr.subscribe("job1")
        assert isinstance(q, asyncio.Queue)
        mgr.unsubscribe("job1", q)

    def test_unsubscribe_nonexistent(self):
        mgr = JobManager()
        q = asyncio.Queue()
        mgr.unsubscribe("nonexistent", q)

    @pytest.mark.asyncio
    async def test_notify_subscribers(self):
        mgr = JobManager()
        q = mgr.subscribe("job1")
        event = SSEJobEvent(job_id="job1", status="running", progress=0.5, message="test")
        await mgr._notify_subscribers("job1", event)
        received = q.get_nowait()
        assert received.status == "running"

    def test_cleanup_old_jobs(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        job = mgr.get_job(job_id)
        job.status = JobStatus.COMPLETED
        job.created_at = datetime.now() - timedelta(hours=25)
        deleted = mgr.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 1
        assert mgr.get_job(job_id) is None

    def test_cleanup_preserves_recent_jobs(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        job = mgr.get_job(job_id)
        job.status = JobStatus.COMPLETED
        deleted = mgr.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 0
        assert mgr.get_job(job_id) is not None

    def test_cleanup_preserves_running_jobs(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        job = mgr.get_job(job_id)
        job.status = JobStatus.RUNNING
        job.created_at = datetime.now() - timedelta(hours=25)
        deleted = mgr.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 0

    @pytest.mark.asyncio
    async def test_acquire_release_slot(self):
        mgr = JobManager(max_concurrent_jobs=1)
        await mgr.acquire_slot()
        mgr.release_slot()
