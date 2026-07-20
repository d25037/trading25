"""Public facade for Market v5 full-rebuild cutover workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from .backup import MarketBackupService
from .activation import MarketActivationService
from .contracts import (
    AtomicExchange,
    BackupResult,
    DuckDbAdapter,
    MarketSourceMetadata,
    OperationResult,
    RestoreResult,
    RuntimeAdapter,
    ApiAdapter,
    SmokeConfig,
    SmokeResult,
)
from .evidence import MarketEvidence
from .duckdb_service import MarketIdentityService
from .full_rehearsal import FullRebuildRehearsalService
from .reports import CutoverReportRepository
from .smoke import RuntimeSmokeService
from .workspace import CutoverWorkspace


class MarketV4CutoverService:
    """Explicit facade over focused cutover collaborators."""

    def __init__(
        self,
        data_root: Path,
        *,
        duckdb: DuckDbAdapter,
        runtime: RuntimeAdapter,
        disk_free_bytes: Callable[[Path], int],
        now: Callable[[], str],
        code_version: Callable[[], str],
        atomic_exchange: AtomicExchange | None = None,
    ) -> None:
        self._workspace = CutoverWorkspace(
            data_root,
            duckdb=duckdb,
            runtime=runtime,
            disk_free_bytes=disk_free_bytes,
            now=now,
            code_version=code_version,
            atomic_exchange=atomic_exchange,
        )
        self._evidence = MarketEvidence(self._workspace)
        self._market_identity = MarketIdentityService(
            self._workspace,
            self._evidence,
        )
        self._reports = CutoverReportRepository(self._workspace, self._evidence)
        self._backups = MarketBackupService(self._workspace, self._evidence)
        self._runtime_smoke = RuntimeSmokeService(self._workspace)
        self._full_rehearsal = FullRebuildRehearsalService(
            self._workspace,
            self._evidence,
            self._reports,
            self._runtime_smoke,
        )
        self._activation = MarketActivationService(
            self._workspace,
            self._evidence,
            self._market_identity,
            self._reports,
            self._runtime_smoke,
            self._backups,
        )

    def preflight(self) -> MarketSourceMetadata:
        return self._backups.preflight()

    def backup(self, backup_id: str) -> BackupResult:
        return self._backups.backup(backup_id)

    def verify_backup(self, backup_id: str) -> BackupResult:
        return self._backups.verify_backup(backup_id)

    def restore(self, backup_id: str | None) -> RestoreResult:
        return self._backups.restore(backup_id)

    def smoke(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        *,
        operation_id: str,
    ) -> SmokeResult:
        return self._runtime_smoke.smoke(
            api,
            config,
            operation_id=operation_id,
        )

    def cutover(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        return self._activation.cutover(
            report_id,
            rehearsal_report_id=rehearsal_report_id,
            backup_id=backup_id,
            config=config,
            inherited_environment=inherited_environment,
        )

    def rehearse(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        return self._full_rehearsal.rehearse(
            report_id,
            config,
            inherited_environment=inherited_environment,
        )

    def configuration_fingerprint(self, root: Path) -> str:
        return self._evidence.configuration_fingerprint(root)

    def root_fingerprint(self, root: Path) -> str:
        return self._evidence.root_fingerprint(root)
