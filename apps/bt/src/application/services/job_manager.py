"""
Job Manager for Async Backtest Execution

非同期バックテストジョブの管理
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from loguru import logger

from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.entrypoints.http.schemas.common import SSEJobEvent

_TERMINAL_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

if TYPE_CHECKING:
    from src.infrastructure.db.market.portfolio_db import PortfolioDb


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
        self._portfolio_db: PortfolioDb | None = None

    def set_portfolio_db(self, portfolio_db: PortfolioDb | None) -> None:
        """ジョブメタデータ永続化先（portfolio.db）を設定する。"""
        self._portfolio_db = portfolio_db

    def _serialize_json(self, value: dict[str, Any] | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False)

    def _deserialize_json(self, value: str | None) -> dict[str, Any] | None:
        if value is None:
            return None
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except Exception as e:
            logger.warning(f"ジョブJSONの復元に失敗: {e}")
            return None

    def _serialize_summary(self, value: BacktestResultSummary | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value.model_dump(mode="json"), ensure_ascii=False)

    def _deserialize_summary(self, value: str | None) -> BacktestResultSummary | None:
        payload = self._deserialize_json(value)
        if payload is None:
            return None
        try:
            return BacktestResultSummary.model_validate(payload)
        except Exception as e:
            logger.warning(f"BacktestResultSummary の復元に失敗: {e}")
            return None

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if value is None:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _job_from_row(self, row: Any) -> JobInfo:
        status_raw = row.status
        try:
            status = JobStatus(status_raw)
        except ValueError:
            logger.warning(f"不正なジョブステータスを検出: {status_raw}")
            status = JobStatus.PENDING

        job = JobInfo(
            job_id=row.job_id,
            strategy_name=row.strategy_name,
            job_type=row.job_type,
        )
        job.status = status
        job.progress = row.progress
        job.message = row.message
        job.error = row.error
        job.created_at = self._parse_datetime(row.created_at) or datetime.now()
        job.started_at = self._parse_datetime(row.started_at)
        job.completed_at = self._parse_datetime(row.completed_at)
        job.result = self._deserialize_summary(row.result_json)
        job.raw_result = self._deserialize_json(row.raw_result_json)
        job.html_path = row.html_path
        job.dataset_name = row.dataset_name
        job.execution_time = row.execution_time
        job.best_score = row.best_score
        job.best_params = self._deserialize_json(row.best_params_json)
        job.worst_score = row.worst_score
        job.worst_params = self._deserialize_json(row.worst_params_json)
        job.total_combinations = row.total_combinations
        return job

    def _persist_job(self, job: JobInfo) -> None:
        if self._portfolio_db is None:
            return

        try:
            self._portfolio_db.upsert_job(
                job_id=job.job_id,
                job_type=job.job_type,
                strategy_name=job.strategy_name,
                status=job.status.value,
                progress=job.progress,
                message=job.message,
                error=job.error,
                created_at=job.created_at.isoformat(),
                started_at=job.started_at.isoformat() if job.started_at else None,
                completed_at=job.completed_at.isoformat() if job.completed_at else None,
                result_json=self._serialize_summary(job.result),
                raw_result_json=self._serialize_json(job.raw_result),
                html_path=job.html_path,
                dataset_name=job.dataset_name,
                execution_time=job.execution_time,
                best_score=job.best_score,
                best_params_json=self._serialize_json(job.best_params),
                worst_score=job.worst_score,
                worst_params_json=self._serialize_json(job.worst_params),
                total_combinations=job.total_combinations,
                updated_at=datetime.now().isoformat(),
            )
        except Exception as e:
            logger.warning(f"ジョブ永続化に失敗: {job.job_id}, error={e}")

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
        self._persist_job(job)
        logger.info(f"ジョブ作成: {job_id} (戦略: {strategy_name}, タイプ: {job_type})")
        return job_id

    def _resolve_job(self, job_id: str) -> JobInfo | None:
        """メモリ優先でジョブを取得し、必要時のみDBから復元する。"""
        return self._jobs.get(job_id) or self.get_job(job_id)

    def get_job(self, job_id: str) -> JobInfo | None:
        """
        ジョブを取得

        Args:
            job_id: ジョブID

        Returns:
            ジョブ情報（存在しない場合はNone）
        """
        job = self._jobs.get(job_id)
        if job is not None:
            return job

        if self._portfolio_db is None:
            return None

        row = self._portfolio_db.get_job_row(job_id)
        if row is None:
            return None

        hydrated = self._job_from_row(row)
        self._jobs[job_id] = hydrated
        return hydrated

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
        if self._portfolio_db is not None:
            rows = self._portfolio_db.list_job_rows(limit=limit, job_types=job_types)
            hydrated_jobs: list[JobInfo] = []
            seen_ids: set[str] = set()
            for row in rows:
                in_memory = self._jobs.get(row.job_id)
                if in_memory is not None:
                    hydrated_jobs.append(in_memory)
                    seen_ids.add(in_memory.job_id)
                    continue
                loaded = self._job_from_row(row)
                self._jobs[loaded.job_id] = loaded
                hydrated_jobs.append(loaded)
                seen_ids.add(loaded.job_id)

            # DB未設定時に作成されたメモリジョブも取りこぼさないよう補完する。
            for mem_job in self._jobs.values():
                if mem_job.job_id in seen_ids:
                    continue
                if job_types is not None and mem_job.job_type not in job_types:
                    continue
                hydrated_jobs.append(mem_job)

            hydrated_jobs.sort(key=lambda j: j.created_at, reverse=True)
            return hydrated_jobs[:limit]

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
            job = self._resolve_job(job_id)
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
            self._persist_job(job)

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
            job = self._resolve_job(job_id)
            if job is None:
                return

            job.result = result_summary
            job.raw_result = raw_result
            job.html_path = html_path
            job.dataset_name = dataset_name
            job.execution_time = execution_time
            self._persist_job(job)

    async def set_job_raw_result(self, job_id: str, raw_result: dict[str, Any]) -> None:
        """ジョブに任意の raw_result payload を保存する。"""
        async with self._lock:
            job = self._resolve_job(job_id)
            if job is None:
                return
            job.raw_result = raw_result
            self._persist_job(job)

    async def set_job_task(self, job_id: str, task: asyncio.Task[None]) -> None:
        """
        ジョブにasyncioタスクを関連付け

        Args:
            job_id: ジョブID
            task: asyncioタスク
        """
        async with self._lock:
            job = self._resolve_job(job_id)
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
            job = self._resolve_job(job_id)
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
            self._persist_job(job)

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
        to_delete: list[str] = []

        # 永続化が有効ならDB上のジョブを基準に削除判定する。
        # （プロセス再起動後にメモリへ未ロードのジョブも対象にするため）
        source_jobs = (
            self.list_jobs(limit=10_000)
            if self._portfolio_db is not None
            else list(self._jobs.values())
        )

        for job in source_jobs:
            age = (now - job.created_at).total_seconds() / 3600
            if age > max_age_hours and job.status in _TERMINAL_STATUSES:
                to_delete.append(job.job_id)

        for job_id in to_delete:
            self._jobs.pop(job_id, None)
            # サブスクライバーも削除
            self._subscribers.pop(job_id, None)

        if self._portfolio_db is not None and to_delete:
            deleted = self._portfolio_db.delete_jobs(to_delete)
            if deleted != len(to_delete):
                logger.warning(
                    f"一部ジョブのDB削除に失敗: expected={len(to_delete)}, actual={deleted}"
                )

        if to_delete:
            logger.info(f"古いジョブを削除: {len(to_delete)}件")

        return len(to_delete)


# グローバルインスタンス
job_manager = JobManager()
