"""job_manager.py のテスト"""

import asyncio
import json
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
        assert info.lease_owner is None
        assert info.cancel_requested_at is None

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
        assert job.run_metadata is not None
        assert job.run_metadata.run_id == job_id
        assert job.run_metadata.engine_family == "vectorbt"
        assert job.run_metadata.market_snapshot_id == "market:latest"

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
        assert job.lease_owner is not None
        assert job.lease_expires_at is not None
        assert job.last_heartbeat_at is not None

    @pytest.mark.asyncio
    async def test_update_job_status_completed(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        await mgr.update_job_status(job_id, JobStatus.RUNNING)
        await mgr.update_job_status(job_id, JobStatus.COMPLETED)
        job = mgr.get_job(job_id)
        assert job.status == JobStatus.COMPLETED
        assert job.completed_at is not None
        assert job.lease_owner is None
        assert job.lease_expires_at is None

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
        assert job.run_metadata is not None
        assert job.run_metadata.dataset_snapshot_id == "ds"
        assert job.run_metadata.market_snapshot_id == "market:latest"
        assert job.canonical_result is not None
        assert job.canonical_result.market_snapshot_id == "market:latest"
        assert job.canonical_result.summary_metrics is not None
        assert job.canonical_result.summary_metrics.trade_count == 50
        assert job.artifact_index is not None

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

    @pytest.mark.asyncio
    async def test_claim_and_heartbeat_job_execution(self):
        mgr = JobManager(default_lease_seconds=30)
        job_id = mgr.create_job("test")

        claimed = await mgr.claim_job_execution(
            job_id,
            lease_owner="worker-1",
            lease_seconds=30,
            timeout_seconds=120,
        )
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.lease_owner == "worker-1"
        assert claimed.timeout_at is not None

        previous_lease_expiry = claimed.lease_expires_at
        assert previous_lease_expiry is not None

        await asyncio.sleep(0)
        heartbeated = await mgr.heartbeat_job_execution(
            job_id,
            lease_owner="worker-1",
            lease_seconds=45,
        )
        assert heartbeated is not None
        assert heartbeated.lease_expires_at is not None
        assert heartbeated.lease_expires_at > previous_lease_expiry

    @pytest.mark.asyncio
    async def test_claim_job_execution_rejects_other_owner_until_lease_expires(self):
        mgr = JobManager(default_lease_seconds=30)
        job_id = mgr.create_job("test")

        claimed = await mgr.claim_job_execution(
            job_id,
            lease_owner="worker-1",
            lease_seconds=30,
        )
        assert claimed is not None

        rejected = await mgr.claim_job_execution(
            job_id,
            lease_owner="worker-2",
            lease_seconds=30,
        )
        assert rejected is None

        active_job = mgr.get_job(job_id)
        assert active_job is not None
        assert active_job.lease_owner == "worker-1"

        active_job.lease_expires_at = datetime.now() - timedelta(seconds=1)
        taken_over = await mgr.claim_job_execution(
            job_id,
            lease_owner="worker-2",
            lease_seconds=30,
        )
        assert taken_over is not None
        assert taken_over.lease_owner == "worker-2"

    @pytest.mark.asyncio
    async def test_heartbeat_job_execution_rejects_other_owner(self):
        mgr = JobManager(default_lease_seconds=30)
        job_id = mgr.create_job("test")

        claimed = await mgr.claim_job_execution(
            job_id,
            lease_owner="worker-1",
            lease_seconds=30,
        )
        assert claimed is not None
        previous_heartbeat = claimed.last_heartbeat_at

        rejected = await mgr.heartbeat_job_execution(
            job_id,
            lease_owner="worker-2",
            lease_seconds=45,
        )
        assert rejected is None

        active_job = mgr.get_job(job_id)
        assert active_job is not None
        assert active_job.lease_owner == "worker-1"
        assert active_job.last_heartbeat_at == previous_heartbeat

    @pytest.mark.asyncio
    async def test_heartbeat_job_execution_requires_existing_owner(self):
        mgr = JobManager(default_lease_seconds=30)
        job_id = mgr.create_job("test")

        rejected = await mgr.heartbeat_job_execution(
            job_id,
            lease_owner="worker-1",
            lease_seconds=45,
        )

        assert rejected is None
        active_job = mgr.get_job(job_id)
        assert active_job is not None
        assert active_job.lease_owner is None
        assert active_job.last_heartbeat_at is None

    @pytest.mark.asyncio
    async def test_request_job_cancel_records_intent(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        job = await mgr.request_job_cancel(job_id, reason="user_requested")
        assert job is not None
        assert job.cancel_requested_at is not None
        assert job.cancel_reason == "user_requested"
        assert mgr.is_cancel_requested(job_id) is True

    @pytest.mark.asyncio
    async def test_request_job_cancel_best_effort_cancels_task(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")

        async def _long_running() -> None:
            await asyncio.sleep(100)

        task = asyncio.create_task(_long_running())
        await mgr.set_job_task(job_id, task)

        await mgr.request_job_cancel(job_id, reason="shutdown_requested")

        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_reconcile_orphaned_jobs_marks_failed(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        job = mgr.get_job(job_id)
        assert job is not None
        job.updated_at = datetime.now() - timedelta(minutes=5)

        reconciled = await mgr.reconcile_orphaned_jobs(reason="api_restart")

        assert reconciled == [job_id]
        updated = mgr.get_job(job_id)
        assert updated is not None
        assert updated.status == JobStatus.FAILED
        assert updated.error == "orphaned_after_api_restart"

    @pytest.mark.asyncio
    async def test_reconcile_orphaned_jobs_respects_cancel_intent(self):
        mgr = JobManager()
        job_id = mgr.create_job("test")
        await mgr.request_job_cancel(job_id, reason="user_requested")
        job = mgr.get_job(job_id)
        assert job is not None
        job.updated_at = datetime.now() - timedelta(minutes=5)

        reconciled = await mgr.reconcile_orphaned_jobs(reason="api_restart")

        assert reconciled == [job_id]
        updated = mgr.get_job(job_id)
        assert updated is not None
        assert updated.status == JobStatus.CANCELLED
        assert updated.cancel_reason == "user_requested"

    @pytest.mark.asyncio
    async def test_reconcile_orphaned_jobs_skips_recent_heartbeat_without_cancel_intent(self):
        mgr = JobManager(default_lease_seconds=60)
        job_id = mgr.create_job("test")
        await mgr.claim_job_execution(
            job_id,
            lease_owner="worker-1",
            lease_seconds=60,
        )

        reconciled = await mgr.reconcile_orphaned_jobs(
            reason="api_restart",
            stale_after_seconds=60,
        )

        assert reconciled == []
        updated = mgr.get_job(job_id)
        assert updated is not None
        assert updated.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_shutdown_requests_cancel_for_incomplete_jobs(self):
        mgr = JobManager()
        pending_id = mgr.create_job("pending")
        running_id = mgr.create_job("running")
        await mgr.update_job_status(running_id, JobStatus.RUNNING)

        requested_count = await mgr.shutdown()

        assert requested_count == 2
        pending_job = mgr.get_job(pending_id)
        running_job = mgr.get_job(running_id)
        assert pending_job is not None
        assert running_job is not None
        assert pending_job.status == JobStatus.PENDING
        assert running_job.status == JobStatus.RUNNING
        assert pending_job.cancel_requested_at is not None
        assert running_job.cancel_requested_at is not None
        assert pending_job.cancel_reason == "process_shutdown"
        assert running_job.cancel_reason == "process_shutdown"

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_cancelled_tasks_to_finish(self):
        mgr = JobManager()
        job_id = mgr.create_job("running")
        await mgr.update_job_status(job_id, JobStatus.RUNNING)
        started = asyncio.Event()
        finished = asyncio.Event()

        async def _long_running() -> None:
            started.set()
            try:
                await asyncio.sleep(100)
            finally:
                finished.set()

        task = asyncio.create_task(_long_running())
        await mgr.set_job_task(job_id, task)
        await started.wait()

        requested_count = await mgr.shutdown(task_timeout_seconds=1.0)

        assert requested_count == 1
        assert finished.is_set()
        assert task.done()

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
            run_spec_json=None,
            run_metadata_json=None,
            result_json='{"total_return": 0.1}',
            raw_result_json='{"x":1}',
            canonical_result_json=None,
            artifact_index_json=None,
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
        await mgr.set_job_optimization_result(
            "missing",
            raw_result={"best_score": 0.0},
            best_score=0.0,
            best_params={},
            worst_score=None,
            worst_params=None,
            total_combinations=0,
            html_path=None,
        )
        task = asyncio.create_task(asyncio.sleep(0))
        await mgr.set_job_task("missing", task)
        await task

    def test_persist_job_handles_db_exception(self):
        mgr = JobManager()
        mock_db = Mock()
        mock_db.upsert_job.side_effect = RuntimeError("db down")
        mock_db.get_job_row.return_value = None
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

    def test_cleanup_old_jobs_with_db_delete_mismatch(self, tmp_path, monkeypatch: pytest.MonkeyPatch):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            mgr = JobManager()
            mgr.set_portfolio_db(db)
            job_id = mgr.create_job("s1")
            job = mgr.get_job(job_id)
            assert job is not None
            job.status = JobStatus.COMPLETED
            job.created_at = datetime.now() - timedelta(hours=25)
            mgr._persist_job(job)
            monkeypatch.setattr(db, "delete_jobs", Mock(return_value=0))
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
            assert loaded.updated_at is not None
        finally:
            db.close()

    @pytest.mark.asyncio
    async def test_execution_control_fields_persist_in_portfolio_db(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            writer = JobManager(default_lease_seconds=15)
            writer.set_portfolio_db(db)
            job_id = writer.create_job("persisted-strategy", job_type="backtest")

            await writer.claim_job_execution(
                job_id,
                lease_owner="worker-lease",
                lease_seconds=15,
                timeout_seconds=90,
            )
            await writer.request_job_cancel(job_id, reason="user_requested")

            reader = JobManager()
            reader.set_portfolio_db(db)
            loaded = reader.get_job(job_id)

            assert loaded is not None
            assert loaded.lease_owner == "worker-lease"
            assert loaded.lease_expires_at is not None
            assert loaded.last_heartbeat_at is not None
            assert loaded.cancel_requested_at is not None
            assert loaded.cancel_reason == "user_requested"
            assert loaded.timeout_at is not None
        finally:
            db.close()

    def test_get_job_backfills_missing_execution_contracts_from_legacy_row(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            db.upsert_job(
                job_id="legacy-job",
                job_type="backtest",
                strategy_name="legacy-strategy",
                status="completed",
                progress=1.0,
                message="done",
                error=None,
                created_at="2026-03-09T00:00:00",
                started_at="2026-03-09T00:00:01",
                completed_at="2026-03-09T00:00:02",
                run_spec_json=None,
                run_metadata_json=None,
                result_json=json.dumps(
                    {
                        "total_return": 12.3,
                        "sharpe_ratio": 1.4,
                        "sortino_ratio": 1.8,
                        "calmar_ratio": 1.1,
                        "max_drawdown": -5.0,
                        "win_rate": 56.7,
                        "trade_count": 14,
                        "html_path": None,
                    }
                ),
                raw_result_json='{"legacy":"payload"}',
                canonical_result_json=None,
                artifact_index_json=None,
                html_path=None,
                dataset_name="dataset-v1",
                execution_time=3.2,
                best_score=None,
                best_params_json=None,
                worst_score=None,
                worst_params_json=None,
                total_combinations=None,
                updated_at="2026-03-09T00:00:02",
            )

            reader = JobManager()
            reader.set_portfolio_db(db)
            loaded = reader.get_job("legacy-job")

            assert loaded is not None
            assert loaded.run_spec is not None
            assert loaded.run_spec.dataset_name == "dataset-v1"
            assert loaded.run_spec.dataset_snapshot_id == "dataset-v1"
            assert loaded.run_spec.market_snapshot_id == "market:latest"
            assert loaded.run_metadata is not None
            assert loaded.run_metadata.dataset_snapshot_id == "dataset-v1"
            assert loaded.run_metadata.market_snapshot_id == "market:latest"
            assert loaded.canonical_result is not None
            assert loaded.canonical_result.market_snapshot_id == "market:latest"
            assert loaded.canonical_result.summary_metrics is not None
            assert loaded.canonical_result.summary_metrics.trade_count == 14
            assert loaded.artifact_index is not None

            persisted = db.get_job_row("legacy-job")
            assert persisted is not None
            assert persisted.run_spec_json is not None
            assert persisted.run_metadata_json is not None
            assert persisted.canonical_result_json is not None
            assert persisted.artifact_index_json is not None
        finally:
            db.close()

    def test_list_jobs_backfills_missing_execution_contracts_from_legacy_rows(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            db.upsert_job(
                job_id="legacy-screening-job",
                job_type="screening",
                strategy_name="legacy-screening",
                status="completed",
                progress=1.0,
                message=None,
                error=None,
                created_at="2026-03-09T01:00:00",
                started_at=None,
                completed_at="2026-03-09T01:00:10",
                run_spec_json=None,
                run_metadata_json=None,
                result_json=None,
                raw_result_json='{"matched_count": 5}',
                canonical_result_json=None,
                artifact_index_json=None,
                html_path=None,
                dataset_name="screening-dataset",
                execution_time=1.0,
                best_score=None,
                best_params_json=None,
                worst_score=None,
                worst_params_json=None,
                total_combinations=None,
                updated_at="2026-03-09T01:00:10",
            )

            reader = JobManager()
            reader.set_portfolio_db(db)
            jobs = reader.list_jobs(limit=10, job_types={"screening"})

            assert len(jobs) == 1
            loaded = jobs[0]
            assert loaded.job_id == "legacy-screening-job"
            assert loaded.run_spec is not None
            assert loaded.run_spec.dataset_snapshot_id == "screening-dataset"
            assert loaded.run_spec.market_snapshot_id == "market:latest"
            assert loaded.run_metadata is not None
            assert loaded.run_metadata.market_snapshot_id == "market:latest"
            assert loaded.canonical_result is not None
            assert loaded.canonical_result.market_snapshot_id == "market:latest"
            assert loaded.artifact_index is not None

            persisted = db.get_job_row("legacy-screening-job")
            assert persisted is not None
            assert persisted.run_spec_json is not None
            assert persisted.run_metadata_json is not None
            assert persisted.canonical_result_json is not None
            assert persisted.artifact_index_json is not None
        finally:
            db.close()

    @pytest.mark.asyncio
    async def test_acquire_release_slot(self):
        mgr = JobManager(max_concurrent_jobs=1)
        await mgr.acquire_slot()
        mgr.release_slot()

    @pytest.mark.asyncio
    async def test_reload_job_from_storage_refreshes_in_memory_state(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            writer = JobManager()
            writer.set_portfolio_db(db)
            job_id = writer.create_job("persisted-strategy")

            reader = JobManager()
            reader.set_portfolio_db(db)
            loaded = reader.get_job(job_id)
            assert loaded is not None
            assert loaded.status == JobStatus.PENDING

            await writer.update_job_status(
                job_id,
                JobStatus.RUNNING,
                message="worker-running",
                progress=0.4,
            )
            refreshed = await reader.reload_job_from_storage(job_id, notify=True)

            assert refreshed is not None
            assert refreshed.status == JobStatus.RUNNING
            assert refreshed.message == "worker-running"
            assert refreshed.progress == 0.4
        finally:
            db.close()

    @pytest.mark.asyncio
    async def test_set_job_optimization_result_persists_fields(self, tmp_path):
        db = PortfolioDb(str(tmp_path / "portfolio.db"))
        try:
            writer = JobManager()
            writer.set_portfolio_db(db)
            job_id = writer.create_job("opt-strategy", job_type="optimization")

            await writer.set_job_optimization_result(
                job_id,
                raw_result={"best_score": 1.5, "html_path": "/tmp/out.html"},
                best_score=1.5,
                best_params={"period": 20},
                worst_score=0.2,
                worst_params={"period": 5},
                total_combinations=8,
                html_path="/tmp/out.html",
            )

            reader = JobManager()
            reader.set_portfolio_db(db)
            loaded = reader.get_job(job_id)

            assert loaded is not None
            assert loaded.best_score == 1.5
            assert loaded.best_params == {"period": 20}
            assert loaded.worst_score == 0.2
            assert loaded.worst_params == {"period": 5}
            assert loaded.total_combinations == 8
            assert loaded.html_path == "/tmp/out.html"
            assert loaded.raw_result == {"best_score": 1.5, "html_path": "/tmp/out.html"}
        finally:
            db.close()
