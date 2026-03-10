"""Helpers for consistent job response payloads."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.domains.backtest.contracts import RunMetadata
from src.entrypoints.http.schemas.common import JobExecutionControl


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _optional_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


def build_run_metadata(job: Any) -> RunMetadata | None:
    """Safely extract RunMetadata from a job-like object."""
    value = getattr(job, "run_metadata", None)
    return value if isinstance(value, RunMetadata) else None


def build_job_execution_control(job: Any) -> JobExecutionControl:
    """Build durable execution control payload from a job-like object."""
    cancel_requested_at = _optional_datetime(getattr(job, "cancel_requested_at", None))
    return JobExecutionControl(
        lease_owner=_optional_str(getattr(job, "lease_owner", None)),
        lease_expires_at=_optional_datetime(getattr(job, "lease_expires_at", None)),
        last_heartbeat_at=_optional_datetime(getattr(job, "last_heartbeat_at", None)),
        cancel_requested=cancel_requested_at is not None,
        cancel_requested_at=cancel_requested_at,
        cancel_reason=_optional_str(getattr(job, "cancel_reason", None)),
        timeout_at=_optional_datetime(getattr(job, "timeout_at", None)),
    )
