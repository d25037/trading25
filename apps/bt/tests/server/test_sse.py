"""
SSE (Server-Sent Events) Tests

JobManager Pub/Sub機構 + SSEManager + SSEエンドポイントのテスト
"""

import asyncio

import pytest

from src.server.schemas.backtest import JobStatus
from src.server.schemas.common import SSEJobEvent
from src.server.services.job_manager import JobManager
from src.server.services.sse_manager import SSEManager


@pytest.fixture
def manager() -> JobManager:
    """テスト用JobManager"""
    return JobManager(max_concurrent_jobs=2)


@pytest.fixture
def sse_manager(manager: JobManager) -> SSEManager:
    """テスト用SSEManager"""
    return SSEManager(manager=manager)


class TestJobManagerPubSub:
    """JobManager Pub/Sub機構のテスト"""

    def test_subscribe_creates_queue(self, manager: JobManager) -> None:
        """subscribeでQueueが作成される"""
        job_id = manager.create_job("test_strategy")
        queue = manager.subscribe(job_id)
        assert queue is not None
        assert job_id in manager._subscribers
        assert len(manager._subscribers[job_id]) == 1

    def test_unsubscribe_removes_queue(self, manager: JobManager) -> None:
        """unsubscribeでQueueが削除される"""
        job_id = manager.create_job("test_strategy")
        queue = manager.subscribe(job_id)
        manager.unsubscribe(job_id, queue)
        assert job_id not in manager._subscribers

    def test_unsubscribe_nonexistent_queue(self, manager: JobManager) -> None:
        """存在しないQueueをunsubscribeしてもエラーにならない"""
        job_id = manager.create_job("test_strategy")
        queue: asyncio.Queue[SSEJobEvent | None] = asyncio.Queue()
        manager.unsubscribe(job_id, queue)  # エラーが発生しない

    def test_multiple_subscribers(self, manager: JobManager) -> None:
        """複数サブスクライバーが登録できる"""
        job_id = manager.create_job("test_strategy")
        q1 = manager.subscribe(job_id)
        q2 = manager.subscribe(job_id)
        assert len(manager._subscribers[job_id]) == 2
        manager.unsubscribe(job_id, q1)
        assert len(manager._subscribers[job_id]) == 1
        manager.unsubscribe(job_id, q2)
        assert job_id not in manager._subscribers

    @pytest.mark.asyncio
    async def test_notify_subscribers(self, manager: JobManager) -> None:
        """_notify_subscribersが全サブスクライバーにイベントを配信する"""
        job_id = manager.create_job("test_strategy")
        q1 = manager.subscribe(job_id)
        q2 = manager.subscribe(job_id)

        event = SSEJobEvent(
            job_id=job_id,
            status="running",
            progress=0.5,
            message="テスト中",
        )
        await manager._notify_subscribers(job_id, event)

        assert not q1.empty()
        assert not q2.empty()

        e1 = q1.get_nowait()
        e2 = q2.get_nowait()
        assert e1 is not None
        assert e1.status == "running"
        assert e1.progress == 0.5
        assert e2 is not None
        assert e2.message == "テスト中"

    @pytest.mark.asyncio
    async def test_update_job_status_notifies_subscribers(self, manager: JobManager) -> None:
        """update_job_statusがSSEサブスクライバーに通知する"""
        job_id = manager.create_job("test_strategy")
        queue = manager.subscribe(job_id)

        await manager.update_job_status(
            job_id, JobStatus.RUNNING, message="実行中", progress=0.3
        )

        event = queue.get_nowait()
        assert event is not None
        assert event.status == "running"
        assert event.progress == 0.3
        assert event.message == "実行中"

    @pytest.mark.asyncio
    async def test_completed_status_sends_none_terminator(self, manager: JobManager) -> None:
        """完了時にNone終了シグナルが送信される"""
        job_id = manager.create_job("test_strategy")
        queue = manager.subscribe(job_id)

        await manager.update_job_status(
            job_id, JobStatus.COMPLETED, message="完了", progress=1.0
        )

        # 1つ目: 完了イベント
        event = queue.get_nowait()
        assert event is not None
        assert event.status == "completed"

        # 2つ目: None終了シグナル
        terminator = queue.get_nowait()
        assert terminator is None

    def test_cleanup_removes_subscribers(self, manager: JobManager) -> None:
        """cleanup_old_jobsでサブスクライバーも削除される"""
        job_id = manager.create_job("test_strategy")
        manager.subscribe(job_id)

        # ジョブを完了状態にして古くする
        job = manager.get_job(job_id)
        assert job is not None
        job.status = JobStatus.COMPLETED
        from datetime import datetime, timedelta

        job.created_at = datetime.now() - timedelta(hours=25)

        deleted = manager.cleanup_old_jobs(max_age_hours=24)
        assert deleted == 1
        assert job_id not in manager._subscribers


class TestSSEManager:
    """SSEManagerのテスト"""

    @pytest.mark.asyncio
    async def test_completed_job_returns_immediately(
        self, manager: JobManager, sse_manager: SSEManager
    ) -> None:
        """完了済みジョブは即時に現在状態を返して終了"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(
            job_id, JobStatus.COMPLETED, message="完了", progress=1.0
        )

        events = []
        async for event in sse_manager.job_event_generator(job_id):
            events.append(event)

        assert len(events) == 1
        assert events[0]["event"] == "completed"

    @pytest.mark.asyncio
    async def test_nonexistent_job_returns_error(
        self, manager: JobManager, sse_manager: SSEManager
    ) -> None:
        """存在しないジョブはエラーイベントを返す"""
        events = []
        async for event in sse_manager.job_event_generator("nonexistent"):
            events.append(event)

        assert len(events) == 1
        assert events[0]["event"] == "error"

    @pytest.mark.asyncio
    async def test_running_job_receives_events(
        self, manager: JobManager, sse_manager: SSEManager
    ) -> None:
        """実行中ジョブのイベントを受信できる"""
        job_id = manager.create_job("test_strategy")
        await manager.update_job_status(job_id, JobStatus.RUNNING)

        collected: list[dict[str, str]] = []

        async def collect_events() -> None:
            async for event in sse_manager.job_event_generator(job_id):
                collected.append(event)
                if event.get("event") == "completed":
                    break

        task = asyncio.create_task(collect_events())

        # 少し待ってからイベントを送信
        await asyncio.sleep(0.05)
        await manager.update_job_status(
            job_id, JobStatus.RUNNING, progress=0.5, message="50% 完了"
        )
        await asyncio.sleep(0.05)
        await manager.update_job_status(
            job_id, JobStatus.COMPLETED, progress=1.0, message="完了"
        )

        await asyncio.wait_for(task, timeout=5.0)

        # runningイベントとcompletedイベントを受信
        assert len(collected) >= 1
        statuses = [e.get("event") for e in collected]
        assert "completed" in statuses


class TestSSEJobEvent:
    """SSEJobEventモデルのテスト"""

    def test_serialize(self) -> None:
        """SSEJobEventがJSON変換できる"""
        event = SSEJobEvent(
            job_id="test-123",
            status="running",
            progress=0.5,
            message="テスト中",
        )
        json_str = event.model_dump_json()
        assert "test-123" in json_str
        assert "running" in json_str

    def test_optional_fields(self) -> None:
        """オプションフィールドがNoneで初期化できる"""
        event = SSEJobEvent(job_id="test", status="pending")
        assert event.progress is None
        assert event.message is None
        assert event.data is None
