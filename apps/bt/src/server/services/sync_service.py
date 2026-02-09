"""
Sync Service

DB Sync のオーケストレーション。
GenericJobManager を使用してバックグラウンド同期を管理する。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

from loguru import logger

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.lib.market_db.market_db import METADATA_KEYS, MarketDb
from src.server.schemas.db import SyncProgress, SyncResult
from src.server.services.generic_job_manager import GenericJobManager, JobInfo
from src.server.services.sync_strategies import (
    SyncContext,
    get_strategy,
)


class SyncMode(str, Enum):
    AUTO = "auto"
    INITIAL = "initial"
    INCREMENTAL = "incremental"
    INDICES_ONLY = "indices-only"


@dataclass
class SyncJobData:
    mode: SyncMode
    resolved_mode: str = ""


# Module-level manager instance
sync_job_manager: GenericJobManager[SyncJobData, SyncProgress, SyncResult] = GenericJobManager()


def _resolve_mode(mode: SyncMode, market_db: MarketDb) -> str:
    """auto モードを実際の戦略に解決"""
    if mode != SyncMode.AUTO:
        return mode.value
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    return "incremental" if last_sync else "initial"


async def start_sync(
    mode: SyncMode,
    market_db: MarketDb,
    jquants_client: JQuantsAsyncClient,
) -> JobInfo[SyncJobData, SyncProgress, SyncResult] | None:
    """Sync ジョブを作成して開始。アクティブジョブがある場合は None。"""
    resolved_mode = _resolve_mode(mode, market_db)
    data = SyncJobData(mode=mode, resolved_mode=resolved_mode)
    job = await sync_job_manager.create_job(data)
    if job is None:
        return None

    strategy = get_strategy(resolved_mode)
    job.data.resolved_mode = resolved_mode

    async def _run() -> None:
        def on_progress(stage: str, current: int, total: int, message: str) -> None:
            pct = (current / total * 100) if total > 0 else 0
            sync_job_manager.update_progress(
                job.job_id,
                SyncProgress(stage=stage, current=current, total=total, percentage=pct, message=message),
            )

        ctx = SyncContext(
            client=jquants_client,
            market_db=market_db,
            cancelled=job.cancelled,
            on_progress=on_progress,
        )
        try:
            result = await asyncio.wait_for(strategy.execute(ctx), timeout=35 * 60)
            if sync_job_manager.is_cancelled(job.job_id):
                return
            sync_job_manager.complete_job(job.job_id, result)
        except asyncio.TimeoutError:
            sync_job_manager.fail_job(job.job_id, "Sync timed out after 35 minutes")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Sync job {job.job_id} failed: {e}")
            sync_job_manager.fail_job(job.job_id, str(e))

    task = asyncio.create_task(_run())
    job.task = task

    return job
