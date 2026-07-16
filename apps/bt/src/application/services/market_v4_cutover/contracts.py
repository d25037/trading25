"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import json
from pathlib import Path
from typing import cast, ContextManager, Protocol

from src.infrastructure.db.market import managed_root as _managed_root


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


class PromotionState(StrEnum):
    """Durable states in the retained Market promotion transaction."""

    VALIDATED = "validated"
    RUNTIMES_DETACHED = "runtimes_detached"
    PREPARED = "prepared"
    EXCHANGED = "exchanged"
    QUARANTINED = "quarantined"
    ACTIVE_SMOKE_PASSED = "active_smoke_passed"
    CLEANUP_STAGED = "cleanup_staged"
    REPORT_PERSISTED = "report_persisted"
    COMMITTED = "committed"
    EXCHANGED_BACK = "exchanged_back"
    ROLLED_BACK = "rolled_back"
    ROLLBACK_DEFERRED = "rollback_deferred_with_lease_held"


@dataclass(frozen=True)
class PromotionIdentityEvidence:
    """Exact immutable and current-location identities for one journal state."""

    active_before_directory: dict[str, int]
    active_before_payload: dict[str, object]
    retained_v4_directory: dict[str, int]
    retained_v4_payload: dict[str, object]
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    active_current: dict[str, object] | None
    retained_current: dict[str, object] | None
    quarantine_current: dict[str, object] | None
    holding_current: dict[str, object] | None
    detached_runtime_names: tuple[str, ...]
    detached_artifacts: tuple[dict[str, object], ...] = ()
    rollback_mode: str | None = None
    promotion_report_sha256: str | None = None


@dataclass(frozen=True)
class PromotionJournalRecord:
    """Typed representation of one validated durable journal record."""

    sequence: int
    state: PromotionState
    operation_id: str
    identities: PromotionIdentityEvidence
    created_at: str


class PromotionAppendStatus(StrEnum):
    """Exact durability outcome for one promotion journal append attempt."""

    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True)
class PromotionAppendResult:
    """Result of a promotion journal append or same-ID recovery."""

    status: PromotionAppendStatus
    record: PromotionJournalRecord | None
    attempt_id: str


@dataclass(frozen=True)
class _PromotionJournalAuthorization:  # pyright: ignore[reportUnusedClass]
    secret: object
    operation_id: str
    attempt_id: str
    sequence: int
    candidate_sha256: str
    resolution_sha256: str
    record_directory: tuple[int, int]
    control_directory: tuple[int, int]
    record_files: tuple[tuple[str, int, int, int, str], ...]
    control_files: tuple[tuple[str, int, int, int, str], ...]


@dataclass(frozen=True)
class MarketSourceMetadata:
    schema_version: int | None
    adjustment_mode: str | None
    adjusted_metrics_ready: bool = True


@dataclass(frozen=True)
class BackupResult:
    backup_id: str


@dataclass(frozen=True)
class RestoreResult:
    backup_id: str
    quarantine_path: str | None


@dataclass(frozen=True)
class SmokeConfig:
    symbol: str
    strategy: str
    dataset_preset: str


@dataclass(frozen=True)
class SmokeResult:
    schema_version: int
    adjustment_mode: str
    checks: tuple[str, ...]
    api_paths: tuple[str, ...]
    lineage: dict[str, int]


@dataclass(frozen=True)
class OperationResult:
    report_id: str
    report_path: str


@dataclass(frozen=True)
class RetainedPromotionEligibility:
    """Immutable evidence gathered before retained promotion may mutate disk."""

    retained_report_id: str
    retained_report_sha256: str
    source_report_id: str
    source_report_sha256: str
    retained_root: Path
    source_market_identity: dict[str, object]
    active_market_identity: dict[str, object]
    target_root_fingerprint: str
    configuration_fingerprint: str


@dataclass(frozen=True)
class DetachedArtifactEvidence:
    """Exact descriptor-derived identity for one held promotion artifact."""

    name: str
    kind: str
    identity: dict[str, object]
    directories: dict[str, dict[str, int]]
    files: dict[str, dict[str, object]]

    def to_mapping(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": self.kind,
            "identity": self.identity,
            "directories": self.directories,
            "files": self.files,
        }


@dataclass(frozen=True)
class RetainedPromotionPreparation:
    """Durable evidence produced before a retained Market exchange."""

    eligibility: RetainedPromotionEligibility
    backup_id: str
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    backup_payload_identity: dict[str, object]
    holding_root: Path
    holding_directory_identity: dict[str, int]
    detached_runtime_names: tuple[str, ...]
    detached_artifacts: tuple[DetachedArtifactEvidence, ...]


@dataclass(frozen=True)
class RetainedPromotionReportExpectation:
    """Authoritative, independently constructed promotion report contract."""

    report_id: str
    created_at: str
    code_version: str
    retained_report: dict[str, object]
    source_report: dict[str, object]
    fingerprints: dict[str, object]
    payload_identities: dict[str, object]
    filesystem_evidence: dict[str, object]
    backup_id: str
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    backup_evidence: dict[str, object]
    journal: dict[str, object]
    quarantine_path: str
    runtime_cleanup: dict[str, object]
    no_sync: bool
    no_jquants: bool
    api_checks: tuple[str, ...]
    server_process_joined: bool
    worker_process_joined: bool
    semantic_smoke: dict[str, object]
    source_consumed: dict[str, object]
    rollback_instructions: str

    def to_report(self) -> dict[str, object]:
        report = {
            "schemaVersion": 1,
            "reportId": self.report_id,
            "phase": "promotion",
            "status": "passed",
            "activationMode": "retained_atomic_exchange",
            "createdAt": self.created_at,
            "codeVersion": self.code_version,
            "retainedReport": self.retained_report,
            "sourceReport": self.source_report,
            "fingerprints": self.fingerprints,
            "payloadIdentities": self.payload_identities,
            "filesystemEvidence": self.filesystem_evidence,
            "backupId": self.backup_id,
            "backupManifestSha256": self.backup_manifest_sha256,
            "backupFileSetSha256": self.backup_file_set_sha256,
            "backupEvidence": self.backup_evidence,
            "journal": self.journal,
            "quarantinePath": self.quarantine_path,
            "runtimeCleanup": self.runtime_cleanup,
            "noSync": self.no_sync,
            "noJQuants": self.no_jquants,
            "apiChecks": list(self.api_checks),
            "serverProcessJoined": self.server_process_joined,
            "workerProcessJoined": self.worker_process_joined,
            "semanticSmoke": self.semantic_smoke,
            "sourceConsumed": self.source_consumed,
            "rollbackInstructions": self.rollback_instructions,
        }
        # The report candidate must never share mutable nested mappings with the
        # independently assembled expectation used to validate it.
        return cast(
            dict[str, object],
            json.loads(_canonical_json(report)),
        )


class AtomicExchange(Protocol):
    """Capability for atomically exchanging two managed directories."""

    def exchange(
        self,
        managed_root: _managed_root.ManagedRootFd,
        left: Path,
        right: Path,
    ) -> None: ...


class DuckDbAdapter(Protocol):
    """Exclusive DuckDB operations used by the workflow."""

    def checkpoint_exclusive(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata: ...

    def checkpoint_snapshot(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> ContextManager[MarketSourceMetadata]: ...

    def inspect(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata: ...


class RuntimeAdapter(Protocol):
    """Owned server process and HTTP operations used by the workflow."""

    def assert_quiescent(self, data_root: Path) -> None: ...

    def start(
        self,
        *,
        root_fd: int,
        market_fd: int,
        lease_fd: int,
        retained_lease_fd: int | None = None,
        environment: dict[str, str],
        log_path: Path,
        log_fd: int,
    ) -> ApiAdapter: ...

    def cancel_owned_work(self, api: ApiAdapter) -> None: ...

    def stop(self, api: ApiAdapter) -> None: ...


class ApiAdapter(Protocol):
    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]: ...
