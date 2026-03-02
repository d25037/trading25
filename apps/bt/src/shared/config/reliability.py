"""Reliability defaults (timeout/retry/backoff) by feature."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int
    initial_backoff_seconds: float

    def backoff_seconds(self, attempt_index: int) -> float:
        return self.initial_backoff_seconds * (2**attempt_index)


JQUANTS_RETRY_POLICY = RetryPolicy(max_retries=3, initial_backoff_seconds=1.0)
SYNC_JOB_TIMEOUT_MINUTES = 35
DATASET_BUILD_TIMEOUT_MINUTES = 35
