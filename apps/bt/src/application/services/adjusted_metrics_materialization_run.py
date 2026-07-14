"""Joined lifecycle for cooperative adjusted-metrics materialization."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import threading

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsBuildResult,
    AdjustedMetricsMaterializer,
)


class MaterializationCancellationToken:
    """Thread-safe cooperative cancellation checked at code boundaries."""

    def __init__(self) -> None:
        self._requested = threading.Event()

    def request_cancel(self) -> None:
        self._requested.set()

    def is_cancel_requested(self) -> bool:
        return self._requested.is_set()


@dataclass(frozen=True)
class MaterializationProgress:
    stage: str
    completed_codes: int
    total_codes: int
    current_code: str | None
    published_basis_count: int


async def run_shielded_materialization(
    materializer: AdjustedMetricsMaterializer,
    *,
    timeout_seconds: float,
    on_progress: Callable[[MaterializationProgress], None],
) -> AdjustedMetricsBuildResult:
    """Run materialization in a worker and always join it after cancellation."""
    token = MaterializationCancellationToken()
    loop = asyncio.get_running_loop()

    def report_progress(
        completed_codes: int,
        total_codes: int,
        current_code: str | None,
        published_basis_count: int,
    ) -> None:
        delivered = threading.Event()
        callback_error: list[BaseException] = []

        def deliver() -> None:
            try:
                on_progress(
                    MaterializationProgress(
                        stage="adjusted_metrics_pit",
                        completed_codes=completed_codes,
                        total_codes=total_codes,
                        current_code=current_code,
                        published_basis_count=published_basis_count,
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - re-raise in worker
                callback_error.append(exc)
            finally:
                delivered.set()

        loop.call_soon_threadsafe(deliver)
        delivered.wait()
        if callback_error:
            raise callback_error[0]

    worker = asyncio.create_task(
        asyncio.to_thread(
            materializer.reconcile,
            cancel_requested=token.is_cancel_requested,
            on_progress=report_progress,
        )
    )

    async def join_worker() -> None:
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if worker.done() and not worker.cancelled():
            worker.exception()

    try:
        return await asyncio.wait_for(
            asyncio.shield(worker),
            timeout=timeout_seconds,
        )
    except (asyncio.CancelledError, TimeoutError):
        token.request_cancel()
        await join_worker()
        raise
