"""Shared helpers for external job workers."""

from __future__ import annotations

import json
from datetime import datetime
from time import perf_counter
from typing import Any

from src.shared.observability.metrics import metrics_recorder


def duration_ms_for_job(now: datetime, *, started_at: datetime | None, created_at: datetime | None) -> float:
    reference = started_at or created_at or now
    return round(max((now - reference).total_seconds(), 0.0) * 1000, 2)


def elapsed_ms_since(started_at: float) -> float:
    return round((perf_counter() - started_at) * 1000, 2)


def record_job_duration(job_type: str, status: str, duration_ms: float) -> None:
    metrics_recorder.record_job_duration(job_type, status, duration_ms)


def record_elapsed_job_duration(job_type: str, status: str, *, started_at: float) -> float:
    duration_ms = elapsed_ms_since(started_at)
    record_job_duration(job_type, status, duration_ms)
    return duration_ms


def external_worker_lifecycle_fields(
    job_type: str,
    job_id: str,
    status: str,
    *,
    lease_owner: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "event": "job_lifecycle",
        "jobType": job_type,
        "jobId": job_id,
        "status": status,
        "leaseOwner": lease_owner,
        "executionMode": "external_worker",
        **extra,
    }


def parse_json_object_arg(raw: str, *, label: str) -> dict[str, Any]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be a JSON object")
    return parsed
