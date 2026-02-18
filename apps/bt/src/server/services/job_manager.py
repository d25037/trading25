"""
Job Manager for Async Backtest Execution

非同期バックテストジョブの管理
"""

import asyncio
import uuid
from datetime import datetime
from typing import Any

from loguru import logger

from src.server.schemas.backtest import BacktestResultSummary, JobStatus
from src.server.schemas.common import SSEJobEvent

_TERMINAL_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


class JobInfo:
    """ジョブ情報"""

    def __init__(self, job_id: str, strategy_name: str, job_type: str = "backtest") -> None:
        self.job_id = job_id
        self.strategy_name = strategy_name
        self.job_type = job_type
        self.status: JobStatus = JobStatus.PENDING
        self.progress: float | None = None
        self.message: str | None = None
        self.created_at: datetime = datetime.now()
        self.started_at: datetime | None = None
        self.completed_at: datetime | None = None
        self.error: str | None = None
        self.result: BacktestResultSummary | None = None
        self.raw_result: dict[str, Any] | None = None
        self.html_path: str | None = None
        self.dataset_name: str | None = None
        self.execution_time: float | None = None
        self.task: asyncio.Task[None] | None = None
        # Optimization-specific fields
        self.best_score: float | None = None
        self.best_params: dict[str, Any] | None = None
        self.worst_score: float | None = None
        self.worst_params: dict[str, Any] | None = None
        self.total_combinations: int | None = None
        self.notebook_path: str | None = None


class JobManager:
    """非同期ジョブマネージャー"""

    def __init__(self, max_concurrent_jobs: int = 2) -> None:
        """
        初期化

        Args:
            max_concurrent_jobs: 最大同時実行ジョブ数
        """
        self._jobs: dict[str, JobInfo] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._lock = asyncio.Lock()
        self._subscribers: dict[str, list[asyncio.Queue[SSEJobEvent | None]]] = {}

    def create_job(self, strategy_name: str, job_type: str = "backtest") -> str:
        """
        新しいジョブを作成

        Args:
            strategy_name: 戦略名
            job_type: ジョブタイプ（backtest or optimization）

        Returns:
            ジョブID
        """
        job_id = str(uuid.uuid4())
        job = JobInfo(job_id=job_id, strategy_name=strategy_name, job_type=job_type)
        self._jobs[job_id] = job
        logger.info(f"ジョブ作成: {job_id} (戦略: {strategy_name}, タイプ: {job_type})")
        return job_id

    def get_job(self, job_id: str) -> JobInfo | None:
        """
        ジョブを取得

        Args:
            job_id: ジョブID

        Returns:
            ジョブ情報（存在しない場合はNone）
        """
        return self._jobs.get(job_id)

    def list_jobs(
        self, limit: int = 50, job_types: set[str] | None = None
    ) -> list[JobInfo]:
        """
        ジョブ一覧を取得（最新順）

        Args:
            limit: 取得件数上限
            job_types: ジョブタイプのフィルタ（None の場合は全件）

        Returns:
            ジョブ情報リスト
        """
        jobs = list(self._jobs.values())
        if job_types is not None:
            jobs = [job for job in jobs if job.job_type in job_types]

        sorted_jobs = sorted(jobs, key=lambda j: j.created_at, reverse=True)
        return sorted_jobs[:limit]

    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        message: str | None = None,
        progress: float | None = None,
        error: str | None = None,
    ) -> None:
        """
        ジョブステータスを更新

        Args:
            job_id: ジョブID
            status: 新しいステータス
            message: ステータスメッセージ
            progress: 進捗（0.0-1.0）
            error: エラーメッセージ
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return

            # terminal状態からの巻き戻しを防止
            if job.status in _TERMINAL_STATUSES:
                logger.debug(
                    f"ジョブ {job_id} は既にterminal状態 ({job.status})、"
                    f"{status} への更新をスキップ"
                )
                return

            job.status = status
            if message is not None:
                job.message = message
            if progress is not None:
                job.progress = progress
            if error is not None:
                job.error = error

            if status == JobStatus.RUNNING and job.started_at is None:
                job.started_at = datetime.now()
            elif status in _TERMINAL_STATUSES:
                job.completed_at = datetime.now()

        # ロック外でSSE通知
        event = SSEJobEvent(
            job_id=job_id,
            status=status.value if isinstance(status, JobStatus) else status,
            progress=progress,
            message=message,
        )
        await self._notify_subscribers(job_id, event)

        # terminal状態ではNoneを送信して終了シグナル
        if status in _TERMINAL_STATUSES:
            await self._notify_subscribers(job_id, None)

    async def set_job_result(
        self,
        job_id: str,
        result_summary: BacktestResultSummary,
        raw_result: dict[str, Any],
        html_path: str,
        dataset_name: str,
        execution_time: float,
    ) -> None:
        """
        ジョブ結果を設定

        Args:
            job_id: ジョブID
            result_summary: 結果サマリー
            raw_result: 生の結果データ
            html_path: HTMLファイルパス
            dataset_name: データセット名
            execution_time: 実行時間
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return

            job.result = result_summary
            job.raw_result = raw_result
            job.html_path = html_path
            job.dataset_name = dataset_name
            job.execution_time = execution_time

    async def set_job_raw_result(self, job_id: str, raw_result: dict[str, Any]) -> None:
        """ジョブに任意の raw_result payload を保存する。"""
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.raw_result = raw_result

    async def set_job_task(self, job_id: str, task: asyncio.Task[None]) -> None:
        """
        ジョブにasyncioタスクを関連付け

        Args:
            job_id: ジョブID
            task: asyncioタスク
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.task = task

    async def cancel_job(self, job_id: str) -> JobInfo | None:
        """
        ジョブをキャンセル

        PENDING/RUNNINGからCANCELLEDに遷移。
        既にCANCELLEDの場合はそのまま返却（冪等）。
        COMPLETED/FAILEDの場合はNoneを返す。

        Args:
            job_id: ジョブID

        Returns:
            キャンセルされたジョブ情報（遷移不可の場合はNone）
        """
        task_to_cancel: asyncio.Task[None] | None = None

        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None

            # 既にキャンセル済みなら冪等に返却
            if job.status == JobStatus.CANCELLED:
                return job

            # terminal状態（COMPLETED/FAILED）からは遷移不可
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                return None

            # ステータスをCANCELLEDに変更
            cancel_message = "ジョブがキャンセルされました"
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.now()
            job.message = cancel_message
            task_to_cancel = job.task

        # ロック外でタスクキャンセル + SSE通知
        if task_to_cancel is not None and not task_to_cancel.done():
            task_to_cancel.cancel()

        event = SSEJobEvent(
            job_id=job_id,
            status=JobStatus.CANCELLED.value,
            message=cancel_message,
        )
        await self._notify_subscribers(job_id, event)
        await self._notify_subscribers(job_id, None)

        logger.info(f"ジョブキャンセル: {job_id}")
        return job

    async def acquire_slot(self) -> None:
        """実行スロットを取得（同時実行数制限）"""
        await self._semaphore.acquire()

    def release_slot(self) -> None:
        """実行スロットを解放"""
        self._semaphore.release()

    # ============================================
    # Pub/Sub for SSE
    # ============================================

    def subscribe(self, job_id: str) -> asyncio.Queue[SSEJobEvent | None]:
        """
        ジョブのSSEサブスクリプションを開始

        Args:
            job_id: ジョブID

        Returns:
            イベント受信用Queue
        """
        queue: asyncio.Queue[SSEJobEvent | None] = asyncio.Queue()
        if job_id not in self._subscribers:
            self._subscribers[job_id] = []
        self._subscribers[job_id].append(queue)
        logger.debug(f"SSEサブスクリプション開始: {job_id}")
        return queue

    def unsubscribe(self, job_id: str, queue: asyncio.Queue[SSEJobEvent | None]) -> None:
        """
        SSEサブスクリプションを解除

        Args:
            job_id: ジョブID
            queue: 解除するQueue
        """
        if job_id in self._subscribers:
            try:
                self._subscribers[job_id].remove(queue)
            except ValueError:
                pass
            if not self._subscribers[job_id]:
                del self._subscribers[job_id]
        logger.debug(f"SSEサブスクリプション解除: {job_id}")

    async def _notify_subscribers(self, job_id: str, event: SSEJobEvent | None) -> None:
        """
        全サブスクライバーにイベントを配信

        Args:
            job_id: ジョブID
            event: SSEイベント（Noneは終了シグナル）
        """
        if job_id not in self._subscribers:
            return

        for queue in self._subscribers[job_id]:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(f"SSE Queueが満杯: {job_id}")

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """
        古いジョブを削除

        Args:
            max_age_hours: 保持時間（時間）

        Returns:
            削除されたジョブ数
        """
        now = datetime.now()
        to_delete = []

        for job_id, job in self._jobs.items():
            age = (now - job.created_at).total_seconds() / 3600
            if age > max_age_hours and job.status in _TERMINAL_STATUSES:
                to_delete.append(job_id)

        for job_id in to_delete:
            del self._jobs[job_id]
            # サブスクライバーも削除
            self._subscribers.pop(job_id, None)

        if to_delete:
            logger.info(f"古いジョブを削除: {len(to_delete)}件")

        return len(to_delete)


# グローバルインスタンス
job_manager = JobManager()
