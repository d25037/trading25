"""Shared helpers for external job workers."""

from __future__ import annotations

import json
import os
import socket
from datetime import datetime
from time import perf_counter
from typing import Any

from src.shared.observability.metrics import metrics_recorder

DEFAULT_HEARTBEAT_SECONDS = 5.0
WORKER_TIMED_OUT_ERROR = "worker_timed_out"


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


def worker_lease_owner(worker_name: str) -> str:
    return f"{worker_name}:{socket.gethostname()}:{os.getpid()}"


def job_lifecycle_fields(
    job_type: str,
    job_id: str,
    status: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "event": "job_lifecycle",
        "jobType": job_type,
        "jobId": job_id,
        "status": status,
        **extra,
    }


def external_worker_lifecycle_fields(
    job_type: str,
    job_id: str,
    status: str,
    *,
    lease_owner: str,
    **extra: Any,
) -> dict[str, Any]:
    return job_lifecycle_fields(
        job_type,
        job_id,
        status,
        leaseOwner=lease_owner,
        executionMode="external_worker",
        **extra,
    )


def parse_json_object_arg(raw: str, *, label: str) -> dict[str, Any]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be a JSON object")
    return parsed
