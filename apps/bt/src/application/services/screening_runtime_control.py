"""Runtime control helpers for screening execution."""

from __future__ import annotations

import os
from collections.abc import Callable
from time import perf_counter
from typing import Any

from loguru import logger


def resolve_strategy_workers(strategy_count: int) -> int:
    """戦略並列数を自動決定する。"""
    return resolve_parallel_workers(
        work_count=strategy_count,
        env_names=("BT_SCREENING_MAX_STRATEGY_WORKERS",),
    )


def resolve_stock_workers(stock_count: int) -> int:
    """銘柄並列数を自動決定する。"""
    return resolve_parallel_workers(
        work_count=stock_count,
        env_names=(
            "BT_SCREENING_MAX_STOCK_WORKERS",
            "BT_SCREENING_MAX_STRATEGY_WORKERS",
        ),
    )


def resolve_parallel_workers(
    *,
    work_count: int,
    env_names: tuple[str, ...],
) -> int:
    if work_count <= 1:
        return 1

    auto_workers = min(work_count, os.cpu_count() or 1)
    configured_name: str | None = None
    configured: str | None = None
    for env_name in env_names:
        raw = os.getenv(env_name)
        if raw is None:
            continue
        configured_name = env_name
        configured = raw
        break

    if configured is None:
        return max(1, auto_workers)

    try:
        configured_workers = int(configured)
        if configured_workers <= 0:
            raise ValueError("must be > 0")
        return max(1, min(auto_workers, configured_workers))
    except ValueError:
        logger.warning(
            f"Invalid {configured_name}. Fallback to auto workers.",
            value=configured,
        )
        return max(1, auto_workers)


def emit_progress(
    progress_callback: Callable[[int, int], None] | None,
    completed: int,
    total: int,
) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(completed, total)
    except Exception as exc:
        logger.warning(f"screening progress callback failed: {exc}")


def log_stage_timing(
    stage: str,
    started_at: float,
    **extra: Any,
) -> None:
    duration_ms = round((perf_counter() - started_at) * 1000, 2)
    logger.bind(
        event="screening_stage_timing",
        stage=stage,
        duration_ms=duration_ms,
        **extra,
    ).info("screening stage completed")
