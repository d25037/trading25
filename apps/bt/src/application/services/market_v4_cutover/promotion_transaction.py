"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterator

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import (
    ApiAdapter,
    OperationResult,
    PromotionIdentityEvidence,
    PromotionState,
    RetainedPromotionEligibility,
    RetainedPromotionPreparation,
    SmokeConfig,
    SmokeResult,
)
from .errors import RetainedMarketMutationError, RuntimeStopError
from .filesystem import _DIR_OPEN_FLAGS
from .journal import PromotionJournal


class PromotionTransactionMixin:
    def _promote_retained_under_leases_unchecked(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        journal: PromotionJournal,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        if self._active_lease is None or self._retained_lease is None:
            raise _managed_root.CutoverSafetyError(
                "Active and retained Market operation leases are required"
            )
        operation_id = journal.operation_id
        eligibility = preparation.eligibility
        records = journal.read_validated()
        if not records or records[-1].state is not PromotionState.PREPARED:
            raise _managed_root.CutoverSafetyError("Promotion journal must be prepared")
        base = records[-1].identities
        active_relative = Path("market-timeseries")
        retained_market = eligibility.retained_root / "market-timeseries"
        retained_relative = self._managed_relative(retained_market)
        quarantine = self.operations_root / "quarantine" / operation_id
        runtime_name = f".cutover-runtime-{operation_id}"
        report_dir = self.operations_root / "reports" / operation_id
        log_path = report_dir / "active-smoke.log"
        code_version = self._active_code_version
        if code_version is None:
            raise _managed_root.CutoverSafetyError(
                "Operation code identity is unavailable"
            )

        active_location, quarantine, quarantine_location = (
            self._exchange_and_quarantine_retained_market(
                operation_id=operation_id,
                eligibility=eligibility,
                journal=journal,
                base=base,
                retained_market=retained_market,
                active_relative=active_relative,
                retained_relative=retained_relative,
            )
        )
        smoke_result, active_after = self._run_promoted_market_smoke(
            operation_id=operation_id,
            eligibility=eligibility,
            journal=journal,
            base=base,
            preparation=preparation,
            quarantine_location=quarantine_location,
            runtime_name=runtime_name,
            report_dir=report_dir,
            log_path=log_path,
            code_version=code_version,
            inherited_environment=inherited_environment,
            config=config,
        )
        return self._commit_retained_promotion(
            operation_id=operation_id,
            eligibility=eligibility,
            journal=journal,
            base=base,
            preparation=preparation,
            active_location=active_location,
            active_after=active_after,
            quarantine=quarantine,
            quarantine_location=quarantine_location,
            smoke_result=smoke_result,
            code_version=code_version,
        )

    def _exchange_and_quarantine_retained_market(
        self,
        *,
        operation_id: str,
        eligibility: RetainedPromotionEligibility,
        journal: PromotionJournal,
        base: PromotionIdentityEvidence,
        retained_market: Path,
        active_relative: Path,
        retained_relative: Path,
    ) -> tuple[dict[str, object], Path, dict[str, object]]:
        quarantine = self.operations_root / "quarantine" / operation_id
        self.atomic_exchange.exchange(
            self._managed(),
            active_relative,
            retained_relative,
        )
        self._promotion_boundary_hook("exchange_fsynced")
        active_location = self._market_location_identity(self._active_lease_fd_root())
        retained_location = self._market_location_identity(
            self._retained_lease_fd_root()
        )
        if (
            active_location["directory"] != base.retained_v4_directory
            or active_location["payload"] != base.retained_v4_payload
            or retained_location["directory"] != base.active_before_directory
            or retained_location["payload"] != base.active_before_payload
        ):
            raise _managed_root.CutoverSafetyError(
                "Atomic promotion exchange identity mismatch"
            )
        exchanged = self._promotion_identities(
            base,
            active_current=active_location,
            retained_current=retained_location,
            quarantine_current=None,
            holding_current=base.holding_current,
        )
        self._append_preparation_state(journal, PromotionState.EXCHANGED, exchanged)
        self._promotion_boundary_hook("exchanged_journaled")

        self._prepare_managed_directory(quarantine.parent, exist_ok=True)
        self._secure_rename(retained_market, quarantine)
        self._promotion_boundary_hook("quarantine_fsynced")
        quarantine_location = self._payload_location_identity(quarantine)
        if (
            quarantine_location["directory"] != base.active_before_directory
            or quarantine_location["payload"] != base.active_before_payload
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion quarantine identity mismatch"
            )
        quarantined = self._promotion_identities(
            base,
            active_current=active_location,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=base.holding_current,
        )
        self._append_preparation_state(
            journal,
            PromotionState.QUARANTINED,
            quarantined,
        )
        self._promotion_boundary_hook("quarantined_journaled")
        return active_location, quarantine, quarantine_location

    def _run_promoted_market_smoke(
        self,
        *,
        operation_id: str,
        eligibility: RetainedPromotionEligibility,
        journal: PromotionJournal,
        base: PromotionIdentityEvidence,
        preparation: RetainedPromotionPreparation,
        quarantine_location: dict[str, object],
        runtime_name: str,
        report_dir: Path,
        log_path: Path,
        code_version: str,
        inherited_environment: dict[str, str],
        config: SmokeConfig,
    ) -> tuple[SmokeResult, dict[str, object]]:
        self._prepare_retained_runtime(
            self.data_root,
            runtime_name=runtime_name,
            root_fd=self._active_lease.root_fd,
        )
        self._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._prepare_managed_directory(report_dir, exist_ok=False)
        self._require_unchanged_code_identity(code_version)
        environment = self._promotion_runtime_environment(
            inherited_environment,
            lease_fd=self._active_lease.fd,
            root_fd=self._active_lease.root_fd,
            runtime_name=runtime_name,
        )
        market_fd = os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=self._active_lease.root_fd,
        )
        api: ApiAdapter | None = None
        log_fd = self._managed().open_regular(
            self._managed_relative(log_path),
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
        )
        try:
            api = self.runtime.start(
                root_fd=self._active_lease.root_fd,
                market_fd=market_fd,
                lease_fd=self._active_lease.fd,
                retained_lease_fd=self._retained_lease.fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
        finally:
            os.close(log_fd)
        try:
            try:
                smoke_result = self.smoke(
                    api,
                    config,
                    operation_id=f"{operation_id}.active",
                    market_directory_fd=market_fd,
                    guard_lease_fd=self._active_lease.fd,
                )
            except Exception as smoke_error:
                try:
                    self.runtime.cancel_owned_work(api)
                finally:
                    try:
                        self.runtime.stop(api)
                    except RuntimeStopError as stop_error:
                        raise stop_error from smoke_error
                api = None
                raise
            self.runtime.stop(api)
            api = None
        finally:
            os.close(market_fd)
        self._promotion_boundary_hook("smoke_joined")
        active_runtime_market_fd = self._managed().open_dir(Path("market-timeseries"))
        try:
            self._remove_market_runtime(active_runtime_market_fd, runtime_name)
        finally:
            os.close(active_runtime_market_fd)
        log_bytes = self._managed().read_bytes(self._managed_relative(log_path))
        if b"jquants_fetch" in log_bytes.lower():
            raise _managed_root.CutoverSafetyError(
                "Promotion smoke observed a J-Quants fetch"
            )
        forbidden_paths = (
            "/api/db/sync",
            "/api/db/adjusted-metrics/materialize",
            "/api/db/stocks/refresh",
            "/api/db/intraday/sync",
        )
        if any(
            path.startswith(forbidden)
            for path in smoke_result.api_paths
            for forbidden in forbidden_paths
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion smoke used a forbidden mutation API"
            )
        active_after = self._market_location_identity(self._active_lease_fd_root())
        if active_after["payload"] != eligibility.source_market_identity:
            raise RetainedMarketMutationError(
                "Active retained payload changed during promotion smoke"
            )
        smoke_passed = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=base.holding_current,
        )
        self._append_preparation_state(
            journal,
            PromotionState.ACTIVE_SMOKE_PASSED,
            smoke_passed,
        )
        self._promotion_boundary_hook("smoke_journaled")
        return smoke_result, active_after

    def _commit_retained_promotion(
        self,
        *,
        operation_id: str,
        eligibility: RetainedPromotionEligibility,
        journal: PromotionJournal,
        base: PromotionIdentityEvidence,
        preparation: RetainedPromotionPreparation,
        active_location: dict[str, object],
        active_after: dict[str, object],
        quarantine: Path,
        quarantine_location: dict[str, object],
        smoke_result: SmokeResult,
        code_version: str,
    ) -> OperationResult:
        self._require_unchanged_code_identity(code_version)
        cleanup_staging = self._stage_held_promotion_artifacts(
            preparation,
            operation_id=operation_id,
        )
        self._promotion_boundary_hook("held_cleanup_fsynced")
        cleanup_location = {
            "directory": preparation.holding_directory_identity,
            "payload": base.retained_v4_payload,
        }
        cleanup_staged = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=cleanup_location,
        )
        self._append_preparation_state(
            journal,
            PromotionState.CLEANUP_STAGED,
            cleanup_staged,
        )
        active_market_fd = self._managed().open_dir(Path("market-timeseries"))
        try:
            self._validate_canonical_market_payload(active_market_fd)
        finally:
            os.close(active_market_fd)

        created_at = self.now()
        expectation = self._retained_promotion_report_expectation(
            operation_id=operation_id,
            created_at=created_at,
            code_version=code_version,
            preparation=preparation,
            base=base,
            active_location=active_location,
            active_after=active_after,
            quarantine=quarantine,
            quarantine_location=quarantine_location,
            smoke_result=smoke_result,
        )
        report = self._build_retained_promotion_report(expectation)

        def final_validator() -> None:
            current_active = self._market_location_identity(
                self._active_lease_fd_root()
            )
            current_quarantine = self._payload_location_identity(quarantine)
            current_expectation = self._retained_promotion_report_expectation(
                operation_id=operation_id,
                created_at=created_at,
                code_version=code_version,
                preparation=preparation,
                base=base,
                active_location=active_location,
                active_after=current_active,
                quarantine=quarantine,
                quarantine_location=current_quarantine,
                smoke_result=smoke_result,
            )
            if not self._retained_promotion_report_contract_valid(
                report,
                expectation=current_expectation,
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion report contract changed"
                )
            self._require_unchanged_code_identity(code_version)
            if current_active != active_after:
                raise RetainedMarketMutationError(
                    "Active retained payload changed during report publication"
                )
            if current_quarantine != quarantine_location:
                raise _managed_root.CutoverSafetyError(
                    "Promotion quarantine changed during report publication"
                )
            staging_fd = self._managed().open_dir(
                self._managed_relative(cleanup_staging)
            )
            try:
                if (
                    self._directory_identity_evidence(staging_fd)
                    != preparation.holding_directory_identity
                    or self._held_artifacts_evidence(staging_fd)
                    != preparation.detached_artifacts
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion cleanup staging changed during report publication"
                    )
            finally:
                os.close(staging_fd)

        report_path = self._write_report(
            operation_id,
            report,
            final_validator=final_validator,
        )
        self._promotion_boundary_hook("report_fsynced")
        persisted = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=cleanup_location,
        )
        self._append_preparation_state(
            journal,
            PromotionState.REPORT_PERSISTED,
            persisted,
        )
        self._promotion_boundary_hook("report_journaled")
        self._write_source_consumed_marker(
            retained_report_id=eligibility.retained_report_id,
            operation_id=operation_id,
            promotion_report_sha256=self._sha256(report_path),
        )
        self._promotion_boundary_hook("consumed_marker_fsynced")
        committed = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=cleanup_location,
            promotion_report_sha256=self._sha256(report_path),
        )
        self._append_preparation_state(
            journal,
            PromotionState.COMMITTED,
            committed,
        )
        self._promotion_boundary_hook("committed_journaled")
        self._complete_committed_promotion_cleanup(
            preparation,
            operation_id=operation_id,
            report_sha256=self._sha256(report_path),
        )
        return OperationResult(
            operation_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    @contextmanager
    def _retained_promotion_eligibility_scope(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        config: SmokeConfig,
    ) -> Iterator[RetainedPromotionEligibility]:
        with self._existing_exclusive_operation() as code_version:
            retained, retained_sha256, retained_stat = self._promotion_report_snapshot(
                retained_report_id
            )
            source_report_value = retained.get("sourceRehearsalReportId")
            if not isinstance(source_report_value, str):
                raise _managed_root.CutoverSafetyError(
                    "Retained report provenance is invalid"
                )
            source_report_id = self._validate_id(
                source_report_value, label="source rehearsal report"
            )
            retained_root = self._retained_rehearsal_root(source_report_id)
            with _market_operation_lease.MarketOperationLease.acquire_existing(
                retained_root, exclusive=True
            ) as retained_lease:
                self._retained_lease = retained_lease
                try:
                    eligibility = (
                        self._validate_retained_promotion_eligibility_under_leases(
                            report_id=report_id,
                            retained_report_id=retained_report_id,
                            backup_id=backup_id,
                            config=config,
                            code_version=code_version,
                            retained_lease=retained_lease,
                        )
                    )
                    final_retained, final_retained_sha256, final_retained_stat = (
                        self._promotion_report_snapshot(retained_report_id)
                    )
                    final_source, final_source_sha256, _final_source_stat = (
                        self._promotion_report_snapshot(eligibility.source_report_id)
                    )
                    if (
                        eligibility.retained_report_sha256 != retained_sha256
                        or final_retained_sha256 != eligibility.retained_report_sha256
                        or final_retained_stat != retained_stat
                        or final_retained.get("reportId") != retained_report_id
                        or final_source_sha256 != eligibility.source_report_sha256
                        or final_source.get("reportId") != eligibility.source_report_id
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Retained promotion report changed"
                        )
                    if (
                        self._market_tree_identity(self._active_lease_fd_root())
                        != eligibility.active_market_identity
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Active Market payload identity changed during validation"
                        )
                    if (
                        self._market_tree_identity(retained_lease.root_fd)
                        != eligibility.source_market_identity
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Retained Market payload identity changed during validation"
                        )
                    self._require_unchanged_code_identity(code_version)
                    self._assert_retained_root_identity(
                        eligibility.retained_root, retained_lease.root_fd
                    )
                    yield eligibility
                finally:
                    self._retained_lease = None
