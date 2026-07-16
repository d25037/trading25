"""Full-rebuild rehearsal entry orchestration."""

from __future__ import annotations

from pathlib import Path

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import OperationResult, SmokeConfig
from .errors import RetainedMarketMutationError


class FullRehearsalMixin:
    def _unchanged_market_tree_identity(
        self,
        root_fd: int,
        identity_before: dict[str, object] | None,
    ) -> dict[str, object]:
        identity_after = self._market_tree_identity(root_fd)
        if identity_before != identity_after:
            raise RetainedMarketMutationError(
                "retained Market tree changed during smoke"
            )
        return identity_after

    def _operation_result(self, report_id: str, report_path: Path) -> OperationResult:
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    def _rehearse_managed(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        report_id = self._validate_id(report_id, label="report")
        code_version = self._require_code_identity()
        self._validate_active_roots()
        target_root_fingerprint = self.root_fingerprint(self.data_root)
        source_configuration_fingerprint = self.configuration_fingerprint(
            self.data_root
        )
        rehearsal_dir = self.operations_root / "rehearsals" / report_id
        self._prepare_managed_directory(rehearsal_dir.parent, exist_ok=True)
        if rehearsal_dir.exists() or rehearsal_dir.is_symlink():
            raise _managed_root.CutoverSafetyError(
                "Rehearsal destination already exists"
            )
        self._prepare_managed_directory(rehearsal_dir, exist_ok=False)
        rehearsal_root = rehearsal_dir / "root"
        runtime_name = f".cutover-runtime-{report_id}"
        self._prepare_isolated_root(rehearsal_root, runtime_name=runtime_name)
        if (
            self.configuration_fingerprint(rehearsal_root)
            != source_configuration_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Rehearsal configuration snapshot mismatch"
            )
        with _market_operation_lease.MarketOperationLease.acquire(
            rehearsal_root,
            exclusive=True,
        ) as lease:
            return self._rehearse_under_lease(
                report_id,
                config,
                inherited_environment=inherited_environment,
                rehearsal_dir=rehearsal_dir,
                rehearsal_root=rehearsal_root,
                lease=lease,
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
            )
