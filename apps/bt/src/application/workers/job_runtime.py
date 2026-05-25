"""Shared helpers for external job workers."""

from __future__ import annotations

from datetime import datetime
from time import perf_counter


def duration_ms_for_job(now: datetime, *, started_at: datetime | None, created_at: datetime | None) -> float:
    reference = started_at or created_at or now
    return round(max((now - reference).total_seconds(), 0.0) * 1000, 2)


def elapsed_ms_since(started_at: float) -> float:
    return round((perf_counter() - started_at) * 1000, 2)
