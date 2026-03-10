"""
Job Manager for Async Backtest Execution

非同期バックテストジョブの管理
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import uuid
from contextlib import suppress
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from loguru import logger
from pydantic import BaseModel

from src.domains.backtest.contracts import (
    ArtifactIndex,
    CanonicalExecutionResult,
    RunMetadata,
    RunSpec,
)
from src.application.services.run_contracts import (
    build_default_run_spec,
    build_run_metadata_from_spec,
    refresh_job_execution_contracts,
)
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.entrypoints.http.schemas.common import SSEJobEvent

_TERMINAL_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
_INCOMPLETE_STATUSES = (JobStatus.PENDING, JobStatus.RUNNING)
_DEFAULT_LEASE_SECONDS = 60

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
        self.updated_at: datetime | None = None
        self.error: str | None = None
        self.run_spec: RunSpec | None = None
        self.run_metadata: RunMetadata | None = None
        self.result: BacktestResultSummary | None = None
        self.raw_result: dict[str, Any] | None = None
        self.canonical_result: CanonicalExecutionResult | None = None
        self.artifact_index: ArtifactIndex | None = None
        self.html_path: str | None = None
        self.dataset_name: str | None = None
        self.execution_time: float | None = None
        self.task: asyncio.Task[None] | None = None
        self.lease_owner: str | None = None
        self.lease_expires_at: datetime | None = None
        self.last_heartbeat_at: datetime | None = None
        self.cancel_requested_at: datetime | None = None
        self.cancel_reason: str | None = None
        self.timeout_at: datetime | None = None
        # Optimization-specific fields
        self.best_score: float | None = None
        self.best_params: dict[str, Any] | None = None
        self.worst_score: float | None = None
        self.worst_params: dict[str, Any] | None = None
        self.total_combinations: int | None = None


class JobManager:
    """非同期ジョブマネージャー"""

    def __init__(
        self,
        max_concurrent_jobs: int = 2,
        *,
        default_lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        default_timeout_seconds: int | None = None,
    ) -> None:
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
        self._default_lease_seconds = max(default_lease_seconds, 1)
        self._default_timeout_seconds = default_timeout_seconds
        self._default_lease_owner = f"in-process:{socket.gethostname()}:{os.getpid()}"

    @property
    def default_lease_seconds(self) -> int:
        return self._default_lease_seconds

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

    def _serialize_model(self, value: BaseModel | None) -> str | None:
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

    def _deserialize_model(
        self,
        value: str | None,
        model_cls: type[BaseModel],
    ) -> BaseModel | None:
        payload = self._deserialize_json(value)
        if payload is None:
            return None
        try:
            return model_cls.model_validate(payload)
        except Exception as e:
            logger.warning(f"{model_cls.__name__} の復元に失敗: {e}")
            return None

    def _parse_datetime(self, value: str | None) -> datetime | None:
        if value is None:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _job_from_row(self, row: Any) -> JobInfo:
        job, _ = self._hydrate_job_from_row(row)
        return job

    def _cache_hydrated_job(
        self,
        row: Any,
        *,
        existing_job: JobInfo | None = None,
    ) -> tuple[JobInfo, bool]:
        loaded, contracts_backfilled = self._hydrate_job_from_row(row)
        if existing_job is not None:
            loaded.task = existing_job.task
        self._jobs[loaded.job_id] = loaded
        return loaded, contracts_backfilled

    def _resolve_timeout_at(
        self,
        started_at: datetime,
        timeout_seconds: int | None = None,
    ) -> datetime | None:
        effective_timeout = timeout_seconds
        if effective_timeout is None:
            effective_timeout = self._default_timeout_seconds
        if effective_timeout is None:
            return None
        return started_at + timedelta(seconds=max(effective_timeout, 1))

    def _claim_execution_locked(
        self,
        job: JobInfo,
        *,
        lease_owner: str | None = None,
        lease_seconds: int | None = None,
        timeout_seconds: int | None = None,
        now: datetime | None = None,
    ) -> None:
        current_time = now or datetime.now()
        started_at = job.started_at or current_time
        job.started_at = started_at
        job.lease_owner = lease_owner or job.lease_owner or self._default_lease_owner
        job.last_heartbeat_at = current_time
        job.lease_expires_at = current_time + timedelta(
            seconds=max(lease_seconds or self._default_lease_seconds, 1)
        )
        resolved_timeout_at = self._resolve_timeout_at(started_at, timeout_seconds)
        if resolved_timeout_at is not None:
            job.timeout_at = resolved_timeout_at

    @staticmethod
    def _lease_is_expired(job: JobInfo, *, now: datetime | None = None) -> bool:
        if job.lease_expires_at is None:
            return False
        return job.lease_expires_at <= (now or datetime.now())

    def _can_claim_execution_locked(
        self,
        job: JobInfo,
        *,
        lease_owner: str,
        now: datetime | None = None,
    ) -> bool:
        if job.cancel_requested_at is not None:
            return False

        current_owner = job.lease_owner
        if current_owner is None or current_owner == lease_owner:
            return True

        return self._lease_is_expired(job, now=now)

    @staticmethod
    def _can_heartbeat_execution_locked(job: JobInfo, *, lease_owner: str) -> bool:
        current_owner = job.lease_owner
        return current_owner == lease_owner

    def _clear_execution_claim_locked(self, job: JobInfo) -> None:
        job.lease_owner = None
        job.lease_expires_at = None

    def _mark_cancel_requested_locked(
        self,
        job: JobInfo,
        *,
        reason: str | None = None,
        now: datetime | None = None,
    ) -> None:
        if job.cancel_requested_at is None:
            job.cancel_requested_at = now or datetime.now()
        if reason is not None:
            job.cancel_reason = reason

    def _hydrate_job_from_row(self, row: Any) -> tuple[JobInfo, bool]:
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
        job.progress = getattr(row, "progress", None)
        job.message = getattr(row, "message", None)
        job.error = getattr(row, "error", None)
        job.created_at = self._parse_datetime(getattr(row, "created_at", None)) or datetime.now()
        job.started_at = self._parse_datetime(getattr(row, "started_at", None))
        job.completed_at = self._parse_datetime(getattr(row, "completed_at", None))
        job.updated_at = self._parse_datetime(getattr(row, "updated_at", None))
        job.run_spec = self._deserialize_model(
            getattr(row, "run_spec_json", None),
            RunSpec,
        )
        job.run_metadata = self._deserialize_model(
            getattr(row, "run_metadata_json", None),
            RunMetadata,
        )
        job.result = self._deserialize_summary(getattr(row, "result_json", None))
        job.raw_result = self._deserialize_json(getattr(row, "raw_result_json", None))
        job.canonical_result = self._deserialize_model(
            getattr(row, "canonical_result_json", None),
            CanonicalExecutionResult,
        )
        job.artifact_index = self._deserialize_model(
            getattr(row, "artifact_index_json", None),
            ArtifactIndex,
        )
        job.html_path = getattr(row, "html_path", None)
        job.dataset_name = getattr(row, "dataset_name", None)
        job.execution_time = getattr(row, "execution_time", None)
        job.lease_owner = getattr(row, "lease_owner", None)
        job.lease_expires_at = self._parse_datetime(getattr(row, "lease_expires_at", None))
        job.last_heartbeat_at = self._parse_datetime(getattr(row, "last_heartbeat_at", None))
        job.cancel_requested_at = self._parse_datetime(getattr(row, "cancel_requested_at", None))
        job.cancel_reason = getattr(row, "cancel_reason", None)
        job.timeout_at = self._parse_datetime(getattr(row, "timeout_at", None))
        job.best_score = getattr(row, "best_score", None)
        job.best_params = self._deserialize_json(getattr(row, "best_params_json", None))
        job.worst_score = getattr(row, "worst_score", None)
        job.worst_params = self._deserialize_json(getattr(row, "worst_params_json", None))
        job.total_combinations = getattr(row, "total_combinations", None)

        persisted_contracts = {
            "run_spec": self._deserialize_json(getattr(row, "run_spec_json", None)),
            "run_metadata": self._deserialize_json(getattr(row, "run_metadata_json", None)),
            "canonical_result": self._deserialize_json(getattr(row, "canonical_result_json", None)),
            "artifact_index": self._deserialize_json(getattr(row, "artifact_index_json", None)),
        }

        self._refresh_execution_contracts(job)

        refreshed_contracts = {
            "run_spec": job.run_spec.model_dump(mode="json") if job.run_spec is not None else None,
            "run_metadata": (
                job.run_metadata.model_dump(mode="json") if job.run_metadata is not None else None
            ),
            "canonical_result": (
                job.canonical_result.model_dump(mode="json")
                if job.canonical_result is not None
                else None
            ),
            "artifact_index": (
                job.artifact_index.model_dump(mode="json") if job.artifact_index is not None else None
            ),
        }

        return job, refreshed_contracts != persisted_contracts

    def _refresh_execution_contracts(self, job: JobInfo) -> None:
        refresh_job_execution_contracts(job)

    def refresh_job_contracts(self, job: JobInfo) -> None:
        """Refresh engine-neutral contracts for a job and persist them when possible."""
        self._persist_job(job)

    def _persist_job(self, job: JobInfo) -> None:
        self._refresh_execution_contracts(job)
        job.updated_at = datetime.now()

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
                run_spec_json=self._serialize_model(job.run_spec),
                run_metadata_json=self._serialize_model(job.run_metadata),
                result_json=self._serialize_summary(job.result),
                raw_result_json=self._serialize_json(job.raw_result),
                canonical_result_json=self._serialize_model(job.canonical_result),
                artifact_index_json=self._serialize_model(job.artifact_index),
                html_path=job.html_path,
                dataset_name=job.dataset_name,
                execution_time=job.execution_time,
                best_score=job.best_score,
                best_params_json=self._serialize_json(job.best_params),
                worst_score=job.worst_score,
                worst_params_json=self._serialize_json(job.worst_params),
                total_combinations=job.total_combinations,
                updated_at=job.updated_at.isoformat(),
                lease_owner=job.lease_owner,
                lease_expires_at=(
                    job.lease_expires_at.isoformat() if job.lease_expires_at else None
                ),
                last_heartbeat_at=(
                    job.last_heartbeat_at.isoformat() if job.last_heartbeat_at else None
                ),
                cancel_requested_at=(
                    job.cancel_requested_at.isoformat() if job.cancel_requested_at else None
                ),
                cancel_reason=job.cancel_reason,
                timeout_at=job.timeout_at.isoformat() if job.timeout_at else None,
            )
        except Exception as e:
            logger.warning(f"ジョブ永続化に失敗: {job.job_id}, error={e}")

    def create_job(
        self,
        strategy_name: str,
        job_type: str = "backtest",
        *,
        run_spec: RunSpec | None = None,
        parent_run_id: str | None = None,
    ) -> str:
        """
        新しいジョブを作成

        Args:
            strategy_name: 戦略名
            job_type: ジョブタイプ（backtest or optimization）
            run_spec: Engine-neutral run specification
            parent_run_id: Parent run identifier for lineage

        Returns:
            ジョブID
        """
        job_id = str(uuid.uuid4())
        job = JobInfo(job_id=job_id, strategy_name=strategy_name, job_type=job_type)
        effective_run_spec = run_spec or build_default_run_spec(job_type, strategy_name)
        if parent_run_id is not None:
            effective_run_spec.parent_run_id = parent_run_id
        job.run_spec = effective_run_spec
        job.run_metadata = build_run_metadata_from_spec(job_id, effective_run_spec)
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
        if self._portfolio_db is None:
            return self._jobs.get(job_id)

        row = self._portfolio_db.get_job_row(job_id)
        if row is None:
            return self._jobs.get(job_id)

        hydrated, contracts_backfilled = self._cache_hydrated_job(
            row,
            existing_job=self._jobs.get(job_id),
        )
        if contracts_backfilled:
            self._persist_job(hydrated)
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
                loaded, contracts_backfilled = self._cache_hydrated_job(
                    row,
                    existing_job=self._jobs.get(row.job_id),
                )
                if contracts_backfilled:
                    self._persist_job(loaded)
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

            if status == JobStatus.RUNNING:
                self._claim_execution_locked(job)
            elif status in _TERMINAL_STATUSES:
                job.completed_at = datetime.now()
                self._clear_execution_claim_locked(job)
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
        html_path: str | None,
        dataset_name: str,
        execution_time: float,
    ) -> None:
        """
        ジョブ結果を設定

        Args:
            job_id: ジョブID
            result_summary: 結果サマリー
            raw_result: 生の結果データ
            html_path: HTMLファイルパス（未生成時はNone）
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

    async def set_job_optimization_result(
        self,
        job_id: str,
        *,
        raw_result: dict[str, Any],
        best_score: float | None,
        best_params: dict[str, Any] | None,
        worst_score: float | None,
        worst_params: dict[str, Any] | None,
        total_combinations: int | None,
        html_path: str | None,
    ) -> None:
        """最適化ジョブの durable result を保存する。"""
        async with self._lock:
            job = self._resolve_job(job_id)
            if job is None:
                return
            job.raw_result = raw_result
            job.best_score = best_score
            job.best_params = best_params
            job.worst_score = worst_score
            job.worst_params = worst_params
            job.total_combinations = total_combinations
            job.html_path = html_path
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

    async def claim_job_execution(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int | None = None,
        timeout_seconds: int | None = None,
        message: str | None = None,
        progress: float | None = None,
    ) -> JobInfo | None:
        """ジョブ実行 lease を取得し RUNNING へ遷移させる。"""
        async with self._lock:
            job = self._resolve_job(job_id)
            if job is None or job.status in _TERMINAL_STATUSES:
                return None
            if not self._can_claim_execution_locked(job, lease_owner=lease_owner):
                return None

            self._claim_execution_locked(
                job,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
                timeout_seconds=timeout_seconds,
            )
            job.status = JobStatus.RUNNING
            if message is not None:
                job.message = message
            if progress is not None:
                job.progress = progress
            self._persist_job(job)
            return job

    async def heartbeat_job_execution(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int | None = None,
    ) -> JobInfo | None:
        """ジョブ実行中の heartbeat を durable に更新する。"""
        async with self._lock:
            job = self._resolve_job(job_id)
            if job is None or job.status in _TERMINAL_STATUSES:
                return None
            if not self._can_heartbeat_execution_locked(job, lease_owner=lease_owner):
                return None
            self._claim_execution_locked(
                job,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
            )
            self._persist_job(job)
            return job

    async def _await_cancelled_tasks(
        self,
        tasks: list[asyncio.Task[None]],
        *,
        timeout_seconds: float | None,
    ) -> None:
        pending = [task for task in tasks if not task.done()]
        if not pending:
            return

        done, still_pending = await asyncio.wait(
            pending,
            timeout=timeout_seconds,
        )

        for task in done:
            with suppress(asyncio.CancelledError):
                exc = task.exception()
                if exc is not None:
                    logger.warning(f"shutdown中のジョブタスクが例外終了: {exc}")

        if still_pending:
            pending_job_ids = [
                job.job_id
                for job in self._jobs.values()
                if job.task in still_pending
            ]
            logger.warning(
                "shutdown待機後も完了していないジョブタスクがあります: "
                f"count={len(still_pending)}, jobs={pending_job_ids}"
            )

    async def reload_job_from_storage(
        self,
        job_id: str,
        *,
        notify: bool = False,
    ) -> JobInfo | None:
        """portfolio.db からジョブ状態を再読込し、必要に応じて SSE を通知する。"""
        if self._portfolio_db is None:
            return self._jobs.get(job_id)

        event: SSEJobEvent | None = None
        terminal_event = False
        reloaded_job: JobInfo | None = None

        async with self._lock:
            row = self._portfolio_db.get_job_row(job_id)
            if row is None:
                return self._jobs.get(job_id)

            previous = self._jobs.get(job_id)
            reloaded_job, contracts_backfilled = self._cache_hydrated_job(
                row,
                existing_job=previous,
            )
            if contracts_backfilled:
                self._persist_job(reloaded_job)

            if (
                notify
                and previous is not None
                and (
                    previous.status != reloaded_job.status
                    or previous.progress != reloaded_job.progress
                    or previous.message != reloaded_job.message
                )
            ):
                event = SSEJobEvent(
                    job_id=job_id,
                    status=reloaded_job.status.value,
                    progress=reloaded_job.progress,
                    message=reloaded_job.message,
                )
                terminal_event = reloaded_job.status in _TERMINAL_STATUSES

        if event is not None:
            await self._notify_subscribers(job_id, event)
            if terminal_event:
                await self._notify_subscribers(job_id, None)

        return reloaded_job

    async def request_job_cancel(
        self,
        job_id: str,
        *,
        reason: str = "user_requested",
    ) -> JobInfo | None:
        """キャンセル要求だけを durable に記録する。"""
        task_to_cancel: asyncio.Task[None] | None = None
        async with self._lock:
            job = self._resolve_job(job_id)
            if job is None:
                return None
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                return None
            self._mark_cancel_requested_locked(job, reason=reason)
            task_to_cancel = job.task
            self._persist_job(job)
        if task_to_cancel is not None and not task_to_cancel.done():
            task_to_cancel.cancel()
        return job

    def is_cancel_requested(self, job_id: str) -> bool:
        """ジョブに cancel intent があるか。"""
        job = self._resolve_job(job_id)
        return job is not None and job.cancel_requested_at is not None

    async def reconcile_orphaned_jobs(
        self,
        *,
        job_types: set[str] | None = None,
        limit: int = 10_000,
        reason: str = "process_restart",
        stale_after_seconds: int = 0,
    ) -> list[str]:
        """再起動時に孤立した incomplete job を terminal 状態へ回収する。"""
        now = datetime.now()
        reconciled_job_ids: list[str] = []
        jobs = self.list_jobs(limit=limit, job_types=job_types)

        for job in jobs:
            if job.status not in _INCOMPLETE_STATUSES:
                continue
            if job.task is not None and not job.task.done():
                continue

            reference_time = (
                job.last_heartbeat_at
                or job.updated_at
                or job.started_at
                or job.created_at
            )
            if (
                job.cancel_requested_at is None
                and (now - reference_time).total_seconds() < stale_after_seconds
            ):
                continue

            if job.cancel_requested_at is not None:
                await self.update_job_status(
                    job.job_id,
                    JobStatus.CANCELLED,
                    message="キャンセル要求済みジョブを再起動時に回収しました",
                )
            else:
                await self.update_job_status(
                    job.job_id,
                    JobStatus.FAILED,
                    message="ジョブは再起動後に孤立状態として回収されました",
                    error=f"orphaned_after_{reason}",
                )
            reconciled_job_ids.append(job.job_id)

        return reconciled_job_ids

    async def shutdown(
        self,
        *,
        job_types: set[str] | None = None,
        limit: int = 10_000,
        reason: str = "process_shutdown",
        task_timeout_seconds: float | None = 5.0,
    ) -> int:
        """アクティブジョブに durable cancel intent を残す。"""
        requested = 0
        tasks_to_wait: list[asyncio.Task[None]] = []
        for job in self.list_jobs(limit=limit, job_types=job_types):
            if job.status not in _INCOMPLETE_STATUSES:
                continue
            result = await self.request_job_cancel(job.job_id, reason=reason)
            if result is not None:
                requested += 1
                if result.task is not None and not result.task.done():
                    tasks_to_wait.append(result.task)
        await self._await_cancelled_tasks(
            tasks_to_wait,
            timeout_seconds=task_timeout_seconds,
        )
        return requested

    async def cancel_job(self, job_id: str, *, reason: str = "user_requested") -> JobInfo | None:
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
                if job.cancel_requested_at is None:
                    self._mark_cancel_requested_locked(job, reason=reason)
                    self._persist_job(job)
                return job

            # terminal状態（COMPLETED/FAILED）からは遷移不可
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                return None

            self._mark_cancel_requested_locked(job, reason=reason)
            # ステータスをCANCELLEDに変更
            cancel_message = "ジョブがキャンセルされました"
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.now()
            job.message = cancel_message
            self._clear_execution_claim_locked(job)
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
