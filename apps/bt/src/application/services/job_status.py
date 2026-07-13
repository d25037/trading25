"""Shared job status groups for async job orchestration."""

from __future__ import annotations

from src.application.contracts.jobs import JobStatus

TERMINAL_JOB_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
INCOMPLETE_JOB_STATUSES = (JobStatus.PENDING, JobStatus.RUNNING)
