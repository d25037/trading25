"""job_manager.py のテスト"""

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.entrypoints.http.schemas.common import SSEJobEvent
from src.application.services.job_manager import JobInfo, JobManager
from src.infrastructure.db.market.portfolio_db import PortfolioDb


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

    def test_get_job_returns_none_when_db_row_missing(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            mgr = JobManager()
            mgr.set_portfolio_db(db)
            assert mgr.get_job("missing-job-id") is None
        finally:
            db.close()

    def test_list_jobs_with_db_and_memory_fallback(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            persisted_mgr = JobManager()
            persisted_mgr.set_portfolio_db(db)
            persisted_mgr.create_job("persisted", job_type="screening")

            # DB未設定で作ったジョブを後から同じmanagerに残したままにする
            mixed_mgr = JobManager()
            memory_only_id = mixed_mgr.create_job("memory-only", job_type="backtest")
            mixed_mgr.set_portfolio_db(db)

            jobs = mixed_mgr.list_jobs(limit=10)
            ids = {j.job_id for j in jobs}
            assert memory_only_id in ids

            screening_jobs = mixed_mgr.list_jobs(limit=10, job_types={"screening"})
            assert all(job.job_type == "screening" for job in screening_jobs)
        finally:
            db.close()

    def test_internal_deserialize_and_parse_helpers(self):
        mgr = JobManager()
        assert mgr._deserialize_json(None) is None
        assert mgr._deserialize_json("[]") is None
        assert mgr._deserialize_json("{not-json") is None
        assert mgr._deserialize_summary('{"invalid":"shape"}') is None
        assert mgr._parse_datetime("bad-date") is None

    def test_job_from_row_handles_invalid_status(self):
        mgr = JobManager()
        row = SimpleNamespace(
            job_id="j1",
            strategy_name="s1",
            job_type="screening",
            status="invalid-status",
            progress=0.0,
            message=None,
            error=None,
            created_at="bad-date",
            started_at=None,
            completed_at=None,
            result_json='{"total_return": 0.1}',
            raw_result_json='{"x":1}',
            html_path=None,
            dataset_name=None,
            execution_time=None,
            best_score=None,
            best_params_json=None,
            worst_score=None,
            worst_params_json=None,
            total_combinations=None,
        )
        job = mgr._job_from_row(row)
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_setters_noop_when_job_missing(self):
        mgr = JobManager()
        summary = BacktestResultSummary(
            total_return=0.0,
            sharpe_ratio=0.0,
            calmar_ratio=0.0,
            max_drawdown=0.0,
            win_rate=0.0,
            trade_count=0,
        )
        await mgr.set_job_result("missing", summary, {}, "", "", 0.0)
        await mgr.set_job_raw_result("missing", {"k": "v"})
        task = asyncio.create_task(asyncio.sleep(0))
        await mgr.set_job_task("missing", task)
        await task

    def test_persist_job_handles_db_exception(self):
        mgr = JobManager()
        mock_db = Mock()
        mock_db.upsert_job.side_effect = RuntimeError("db down")
        mgr.set_portfolio_db(mock_db)
        job_id = mgr.create_job("s1")
        assert mgr.get_job(job_id) is not None

    def test_unsubscribe_handles_value_error(self):
        mgr = JobManager()
        job_id = mgr.create_job("s1")
        q1 = mgr.subscribe(job_id)
        q2: asyncio.Queue[SSEJobEvent | None] = asyncio.Queue()
        mgr.unsubscribe(job_id, q2)
        assert job_id in mgr._subscribers
        mgr.unsubscribe(job_id, q1)
        assert job_id not in mgr._subscribers

    @pytest.mark.asyncio
    async def test_notify_subscribers_handles_queue_full(self):
        mgr = JobManager()
        job_id = mgr.create_job("s1")
        queue: asyncio.Queue[SSEJobEvent | None] = asyncio.Queue(maxsize=1)
        mgr._subscribers[job_id] = [queue]
        queue.put_nowait(SSEJobEvent(job_id=job_id, status="running"))
        await mgr._notify_subscribers(
            job_id,
            SSEJobEvent(job_id=job_id, status="running", message="next"),
        )
        assert queue.qsize() == 1

    def test_cleanup_old_jobs_with_db_delete_mismatch(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            mgr = JobManager()
            mgr.set_portfolio_db(db)
            job_id = mgr.create_job("s1")
            job = mgr.get_job(job_id)
            assert job is not None
            job.status = JobStatus.COMPLETED
            job.created_at = datetime.now() - timedelta(hours=25)
            db.delete_jobs = Mock(return_value=0)  # type: ignore[method-assign]
            deleted = mgr.cleanup_old_jobs(max_age_hours=24)
            assert deleted == 1
        finally:
            db.close()

    def test_load_job_from_portfolio_db(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            writer = JobManager()
            writer.set_portfolio_db(db)
            job_id = writer.create_job("persisted-strategy", job_type="screening")

            reader = JobManager()
            reader.set_portfolio_db(db)
            loaded = reader.get_job(job_id)

            assert loaded is not None
            assert loaded.job_id == job_id
            assert loaded.strategy_name == "persisted-strategy"
            assert loaded.job_type == "screening"
            assert loaded.status == JobStatus.PENDING
        finally:
            db.close()

    @pytest.mark.asyncio
    async def test_acquire_release_slot(self):
        mgr = JobManager(max_concurrent_jobs=1)
        await mgr.acquire_slot()
        mgr.release_slot()
