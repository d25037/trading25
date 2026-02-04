"""
SSE Manager

Server-Sent Eventsのイベントジェネレーター管理
"""

import asyncio
from collections.abc import AsyncGenerator

from loguru import logger

from src.server.schemas.common import SSEJobEvent
from src.server.services.job_manager import JobManager, _TERMINAL_STATUSES, job_manager


class SSEManager:
    """SSEイベント管理"""

    def __init__(self, manager: JobManager | None = None) -> None:
        self._manager = manager or job_manager

    async def job_event_generator(self, job_id: str) -> AsyncGenerator[dict[str, str], None]:
        """
        ジョブのSSEイベントジェネレーター

        既に完了済みなら現在状態を1回送信して終了。
        実行中ならsubscribe → Queueからイベント受信 → yield。
        30秒タイムアウトでheartbeat送信（接続維持）。

        Args:
            job_id: ジョブID

        Yields:
            SSEイベント辞書 (event, data)
        """
        job = self._manager.get_job(job_id)
        if job is None:
            yield {
                "event": "error",
                "data": SSEJobEvent(
                    job_id=job_id,
                    status="error",
                    message="ジョブが見つかりません",
                ).model_dump_json(),
            }
            return

        # 既に完了済みの場合、現在状態を送信して終了
        if job.status in _TERMINAL_STATUSES:
            yield {
                "event": job.status.value,
                "data": SSEJobEvent(
                    job_id=job_id,
                    status=job.status.value,
                    progress=job.progress,
                    message=job.message,
                ).model_dump_json(),
            }
            return

        # サブスクリプション開始
        queue = self._manager.subscribe(job_id)
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    # Heartbeat送信（接続維持）
                    yield {
                        "event": "heartbeat",
                        "data": "{}",
                    }
                    continue

                # 終了シグナル
                if event is None:
                    return

                yield {
                    "event": event.status,
                    "data": event.model_dump_json(),
                }

        except asyncio.CancelledError:
            logger.debug(f"SSEストリームがキャンセルされました: {job_id}")
        finally:
            self._manager.unsubscribe(job_id, queue)


# グローバルインスタンス
sse_manager = SSEManager()
