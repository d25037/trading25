"""Public contract for durable Market maintenance evidence."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator


RECOVERY_COMMAND = "uv run bt market-maintain"
_PASSED_REQUIRED_FIELDS = (
    "operation",
    "recordedAt",
    "compacted",
    "trigger",
    "beforeBytes",
    "afterBytes",
    "durationMs",
    "validation",
    "schemaFingerprint",
    "tableCounts",
    "semanticDigests",
)
_SUCCESS_EVIDENCE_FIELDS = (
    "compacted",
    "trigger",
    "beforeBytes",
    "afterBytes",
    "durationMs",
    "validation",
    "schemaFingerprint",
    "tableCounts",
    "semanticDigests",
)


def _is_nonblank(value: str | None) -> bool:
    return value is not None and bool(value.strip())


def _is_timestamp(value: str | None) -> bool:
    if value is None or not value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.tzinfo is not None


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

    @model_validator(mode="after")
    def validate_outcome_contract(self) -> Self:
        if self.schemaVersion != 1:
            raise ValueError("schemaVersion must be 1")

        if self.evidenceStatus is MaintenanceEvidenceStatus.NEVER_RUN:
            if self.outcome is not MaintenanceOutcome.NEVER_RUN:
                raise ValueError("never_run evidence requires never_run outcome")
            self._require_no_run_details("never_run")
            return self

        if self.evidenceStatus is MaintenanceEvidenceStatus.INVALID:
            if self.outcome is not MaintenanceOutcome.INVALID:
                raise ValueError("invalid evidence requires invalid outcome")
            if not _is_nonblank(self.error):
                raise ValueError("invalid evidence requires an error")
            if self.recoveryCommand != RECOVERY_COMMAND:
                raise ValueError(
                    "invalid evidence requires the canonical recovery command"
                )
            return self

        if self.evidenceStatus is not MaintenanceEvidenceStatus.VALID:
            raise ValueError("unsupported maintenance evidence status")

        if self.outcome is MaintenanceOutcome.PASSED:
            missing = [
                field
                for field in _PASSED_REQUIRED_FIELDS
                if getattr(self, field) is None
            ]
            if missing:
                raise ValueError("passed evidence is incomplete: " + ", ".join(missing))
            if not _is_nonblank(self.operation):
                raise ValueError("passed evidence requires a non-empty operation")
            if not _is_timestamp(self.recordedAt):
                raise ValueError("passed evidence requires an offset-aware timestamp")
            if not _is_nonblank(self.trigger) or not _is_nonblank(
                self.schemaFingerprint
            ):
                raise ValueError(
                    "passed evidence requires trigger and schema fingerprint"
                )
            if self.validation != "passed":
                raise ValueError("passed evidence requires passed validation")
            if self.error is not None or self.recoveryCommand is not None:
                raise ValueError("passed evidence cannot contain recovery details")
            return self

        if self.outcome is MaintenanceOutcome.FAILED:
            if (
                not _is_nonblank(self.operation)
                or not _is_timestamp(self.recordedAt)
                or not _is_nonblank(self.error)
            ):
                raise ValueError(
                    "failed evidence requires operation, offset-aware recordedAt, and error"
                )
            if self.recoveryCommand != RECOVERY_COMMAND:
                raise ValueError(
                    "failed evidence requires the canonical recovery command"
                )
            contradictory = [
                field
                for field in _SUCCESS_EVIDENCE_FIELDS
                if getattr(self, field) is not None
            ]
            if contradictory:
                raise ValueError(
                    "failed evidence contains success-only fields: "
                    + ", ".join(contradictory)
                )
            return self

        raise ValueError("valid evidence requires passed or failed outcome")

    def _require_no_run_details(self, label: str) -> None:
        details = (
            "operation",
            "recordedAt",
            *_SUCCESS_EVIDENCE_FIELDS,
            "error",
            "recoveryCommand",
        )
        contradictory = [field for field in details if getattr(self, field) is not None]
        if contradictory:
            raise ValueError(
                f"{label} evidence contains run details: " + ", ".join(contradictory)
            )

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
            recoveryCommand=RECOVERY_COMMAND,
        )

    @classmethod
    def failed(cls, *, operation: str, recorded_at: str, error: str) -> Self:
        return cls(
            evidenceStatus=MaintenanceEvidenceStatus.VALID,
            outcome=MaintenanceOutcome.FAILED,
            operation=operation,
            recordedAt=recorded_at,
            error=error,
            recoveryCommand=RECOVERY_COMMAND,
        )
