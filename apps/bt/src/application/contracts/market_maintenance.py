"""Public contract for durable Market maintenance evidence."""

from __future__ import annotations

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict


class MaintenanceEvidenceStatus(StrEnum):
    NEVER_RUN = "never_run"
    VALID = "valid"
    INVALID = "invalid"


class MaintenanceOutcome(StrEnum):
    NEVER_RUN = "never_run"
    PASSED = "passed"
    FAILED = "failed"
    INVALID = "invalid"


class MarketOperationOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class MarketMaintenanceRecord(BaseModel):
    """Strict sidecar payload and API summary for the latest maintenance run."""

    model_config = ConfigDict(extra="forbid")

    schemaVersion: int = 1
    evidenceStatus: MaintenanceEvidenceStatus
    outcome: MaintenanceOutcome
    operation: str | None = None
    recordedAt: str | None = None
    compacted: bool | None = None
    trigger: str | None = None
    beforeBytes: int | None = None
    afterBytes: int | None = None
    durationMs: float | None = None
    validation: str | None = None
    schemaFingerprint: str | None = None
    tableCounts: dict[str, int] | None = None
    semanticDigests: dict[str, str] | None = None
    error: str | None = None
    recoveryCommand: str | None = None

    @classmethod
    def never_run(cls) -> Self:
        return cls(
            evidenceStatus=MaintenanceEvidenceStatus.NEVER_RUN,
            outcome=MaintenanceOutcome.NEVER_RUN,
        )

    @classmethod
    def invalid(cls, error: str) -> Self:
        return cls(
            evidenceStatus=MaintenanceEvidenceStatus.INVALID,
            outcome=MaintenanceOutcome.INVALID,
            error=error,
            recoveryCommand="uv run bt market-maintain",
        )

    @classmethod
    def failed(cls, *, operation: str, recorded_at: str, error: str) -> Self:
        return cls(
            evidenceStatus=MaintenanceEvidenceStatus.VALID,
            outcome=MaintenanceOutcome.FAILED,
            operation=operation,
            recordedAt=recorded_at,
            error=error,
            recoveryCommand="uv run bt market-maintain",
        )
