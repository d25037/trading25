"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    PromotionIdentityEvidence,
    PromotionJournalRecord,
    PromotionState,
    RetainedPromotionPreparation,
)
from .journal import PromotionJournal
from .promotion_contracts import RetainedPromotionContext


class PromotionRollbackMixin:
    def _atomic_exchange_parent_identities(
        self,
        left: Path,
        right: Path,
    ) -> tuple[tuple[int, int], tuple[int, int]]:
        identities: list[tuple[int, int]] = []
        for relative in (left, right):
            parent_fd, _name = self._managed().open_parent(relative)
            try:
                parent_stat = os.fstat(parent_fd)
                if not stat.S_ISDIR(parent_stat.st_mode):
                    raise _managed_root.CutoverSafetyError(
                        "Atomic exchange parent is not a directory"
                    )
                identities.append((parent_stat.st_dev, parent_stat.st_ino))
            finally:
                os.close(parent_fd)
        return identities[0], identities[1]

    def _prove_atomic_exchange_parent_durability(
        self,
        left: Path,
        right: Path,
        *,
        expected: tuple[tuple[int, int], tuple[int, int]],
    ) -> None:
        if self._active_lease is None or self._retained_lease is None:
            raise _managed_root.CutoverSafetyError(
                "Promotion exchange durability requires both held leases"
            )
        parent_fds: list[int] = []
        failures: list[Exception] = []
        try:
            for relative, expected_identity in zip(
                (left, right), expected, strict=True
            ):
                try:
                    parent_fd, _name = self._managed().open_parent(relative)
                    parent_fds.append(parent_fd)
                    parent_stat = os.fstat(parent_fd)
                    if (
                        not stat.S_ISDIR(parent_stat.st_mode)
                        or (parent_stat.st_dev, parent_stat.st_ino) != expected_identity
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Atomic exchange parent identity changed during durability proof"
                        )
                except Exception as exc:
                    failures.append(exc)
            if not failures:
                for parent_fd in parent_fds:
                    try:
                        os.fsync(parent_fd)
                    except OSError as exc:
                        failures.append(exc)
        finally:
            for parent_fd in parent_fds:
                os.close(parent_fd)
        if failures:
            self._fence_promotion_leases()
            raise _managed_root.CutoverSafetyError(
                "Promotion exchange durability could not be proven; both leases remain fenced"
            ) from failures[0]

    def _rollback_retained_promotion(
        self,
        context: RetainedPromotionContext,
        *,
        processes_joined: bool,
    ) -> None:
        """Restore v3 exactly, or durably fence both roots when cleanup is unsafe."""

        if self._active_lease is None or self._retained_lease is None:
            raise _managed_root.CutoverSafetyError(
                "Both promotion leases are required for rollback"
            )
        preparation = context.preparation
        journal = context.journal
        records = journal.read_validated()
        if not records:
            raise _managed_root.CutoverSafetyError(
                "Promotion rollback journal is empty"
            )
        base = records[-1].identities
        if base.detached_artifacts != tuple(
            artifact.to_mapping() for artifact in preparation.detached_artifacts
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion rollback detached artifact evidence mismatch"
            )
        retained_market = preparation.eligibility.retained_root / "market-timeseries"
        quarantine = self.operations_root / "quarantine" / journal.operation_id
        active = self._market_location_identity(self._active_lease_fd_root())
        retained = self._promotion_location_if_present(retained_market)
        quarantined = self._promotion_location_if_present(quarantine)
        if self._rollback_validated_promotion(
            preparation=preparation,
            journal=journal,
            records=records,
            base=base,
            active=active,
            retained=retained,
            quarantined=quarantined,
            retained_market=retained_market,
            processes_joined=processes_joined,
        ):
            return
        holding = self._rollback_holding_identity(preparation, journal, base)

        if not processes_joined:
            self._defer_retained_promotion_rollback(
                journal=journal,
                base=base,
                active=active,
                retained=retained,
                quarantined=quarantined,
                holding=holding,
            )

        if self._finish_exchanged_back_rollback(
            preparation=preparation,
            journal=journal,
            records=records,
            base=base,
            active=active,
            retained=retained,
            quarantined=quarantined,
        ):
            return

        self._complete_retained_promotion_rollback(
            preparation=preparation,
            journal=journal,
            records=records,
            base=base,
            retained_market=retained_market,
            quarantine=quarantine,
            active=active,
            retained=retained,
            quarantined=quarantined,
            holding=holding,
        )

    def _complete_retained_promotion_rollback(
        self,
        *,
        preparation: RetainedPromotionPreparation,
        journal: PromotionJournal,
        records: tuple[PromotionJournalRecord, ...],
        base: PromotionIdentityEvidence,
        retained_market: Path,
        quarantine: Path,
        active: dict[str, object],
        retained: dict[str, object] | None,
        quarantined: dict[str, object] | None,
        holding: dict[str, object] | None,
    ) -> None:
        active_is_v3 = self._location_matches(
            active,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        )
        active_is_v4 = self._location_matches(
            active,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        )
        retained_is_v3 = self._location_matches(
            retained,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        )
        retained_is_v4 = self._location_matches(
            retained,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        )
        quarantine_is_v3 = self._location_matches(
            quarantined,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        )
        layout_exchangeable = active_is_v4 and (
            (retained_is_v3 and quarantined is None)
            or (quarantine_is_v3 and retained is None)
        )
        layout_already_restored = (
            active_is_v3 and retained_is_v4 and quarantined is None
        )
        if not (layout_exchangeable or layout_already_restored):
            raise _managed_root.CutoverSafetyError(
                "Promotion rollback filesystem identity is ambiguous"
            )
        if layout_already_restored and records[-1].state in {
            PromotionState.VALIDATED,
            PromotionState.RUNTIMES_DETACHED,
            PromotionState.PREPARED,
        }:
            self._restore_held_promotion_artifacts(preparation)
            self._remove_incomplete_consumed_marker(
                retained_report_id=preparation.eligibility.retained_report_id,
                operation_id=journal.operation_id,
            )
            rolled_back = self._promotion_identities(
                base,
                active_current=active,
                retained_current=retained,
                quarantine_current=None,
                holding_current=None,
            )
            self._append_preparation_state(
                journal, PromotionState.ROLLED_BACK, rolled_back
            )
            return
        backup_fallback = False
        exchange_error: Exception | None = None
        if layout_exchangeable:
            exchange_target = retained_market if retained_is_v3 else quarantine
            exchange_target_relative = self._managed_relative(exchange_target)
            exchange_parent_identities = self._atomic_exchange_parent_identities(
                Path("market-timeseries"), exchange_target_relative
            )
            try:
                self.atomic_exchange.exchange(
                    self._managed(),
                    Path("market-timeseries"),
                    exchange_target_relative,
                )
            except Exception as exc:
                exchange_error = exc
            active = self._market_location_identity(self._active_lease_fd_root())
            retained = self._promotion_location_if_present(retained_market)
            quarantined = self._promotion_location_if_present(quarantine)
            active_is_v3 = self._location_matches(
                active,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            active_is_v4 = self._location_matches(
                active,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            retained_is_v3 = self._location_matches(
                retained,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            retained_is_v4 = self._location_matches(
                retained,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            quarantine_is_v3 = self._location_matches(
                quarantined,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            quarantine_is_v4 = self._location_matches(
                quarantined,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            if active_is_v3 and retained_is_v4 and quarantined is None:
                if exchange_error is not None:
                    self._prove_atomic_exchange_parent_durability(
                        Path("market-timeseries"),
                        exchange_target_relative,
                        expected=exchange_parent_identities,
                    )
                exchange_error = None
            elif active_is_v3 and quarantine_is_v4 and retained is None:
                if exchange_error is not None:
                    self._prove_atomic_exchange_parent_durability(
                        Path("market-timeseries"),
                        exchange_target_relative,
                        expected=exchange_parent_identities,
                    )
                self._secure_rename(quarantine, retained_market)
                exchange_error = None
            elif not (
                exchange_error is not None
                and active_is_v4
                and (
                    (retained_is_v3 and quarantined is None)
                    or (quarantine_is_v3 and retained is None)
                )
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion exchange-back result is ambiguous"
                )

        backup_fallback = self._restore_rollback_backup_if_needed(
            preparation=preparation,
            base=base,
            retained_market=retained_market,
            quarantine=quarantine,
            retained_is_v3=retained_is_v3,
            exchange_error=exchange_error,
        )
        self._finalize_retained_promotion_rollback(
            preparation=preparation,
            journal=journal,
            records=records,
            base=base,
            retained_market=retained_market,
            quarantine=quarantine,
            holding=holding,
            backup_fallback=backup_fallback,
        )

    def _restore_rollback_backup_if_needed(
        self,
        *,
        preparation: RetainedPromotionPreparation,
        base: PromotionIdentityEvidence,
        retained_market: Path,
        quarantine: Path,
        retained_is_v3: bool,
        exchange_error: Exception | None,
    ) -> bool:
        backup_fallback = False
        if exchange_error is not None:
            try:
                self._verified_backup_evidence(
                    preparation.backup_id,
                    expected_payload=base.active_before_payload,
                )
                if retained_is_v3:
                    self._prepare_managed_directory(quarantine.parent, exist_ok=True)
                    self._assert_managed_target_absent(quarantine)
                    self._secure_rename(retained_market, quarantine)
                restored = self._restore_under_lease(preparation.backup_id)
                if restored.quarantine_path is None:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion backup fallback did not retain displaced v4"
                    )
                displaced = self.data_root / restored.quarantine_path
                displaced_location = self._payload_location_identity(displaced)
                if displaced_location["payload"] != base.retained_v4_payload:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion backup fallback displaced identity mismatch"
                    )
                if self._promotion_location_if_present(retained_market) is not None:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion backup fallback retained destination is occupied"
                    )
                self._secure_rename(displaced, retained_market)
                backup_fallback = True
            except Exception as restore_error:
                raise _managed_root.CutoverSafetyError(
                    "Terminal promotion recovery failure: exchange-back and "
                    "verified backup restore both failed"
                ) from restore_error
        return backup_fallback

    def _finalize_retained_promotion_rollback(
        self,
        *,
        preparation: RetainedPromotionPreparation,
        journal: PromotionJournal,
        records: tuple[PromotionJournalRecord, ...],
        base: PromotionIdentityEvidence,
        retained_market: Path,
        quarantine: Path,
        holding: dict[str, object] | None,
        backup_fallback: bool,
    ) -> None:
        active = self._market_location_identity(self._active_lease_fd_root())
        retained = self._payload_location_identity(retained_market)
        quarantined = self._promotion_location_if_present(quarantine)
        active_payload_valid = (
            self._payload_manifest_entries(cast(dict[str, object], active["payload"]))
            == self._payload_manifest_entries(base.active_before_payload)
            if backup_fallback
            else active["payload"] == base.active_before_payload
        )
        if not active_payload_valid or not self._location_matches(
            retained,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion rollback identity verification failed"
            )
        exchanged_back = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=quarantined if backup_fallback else None,
            holding_current=holding,
            rollback_mode=("backup_restore" if backup_fallback else "atomic_exchange"),
        )
        self._append_preparation_state(
            journal, PromotionState.EXCHANGED_BACK, exchanged_back
        )
        self._promotion_boundary_hook("exchanged_back_journaled")
        recovery_records = journal.read_validated()
        self._restore_held_promotion_artifacts(
            preparation,
            owned_temp_collision_recovery_records=recovery_records,
        )
        self._remove_incomplete_consumed_marker(
            retained_report_id=preparation.eligibility.retained_report_id,
            operation_id=journal.operation_id,
        )
        retained = self._payload_location_identity(retained_market)
        rolled_back = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=quarantined if backup_fallback else None,
            holding_current=None,
            rollback_mode=("backup_restore" if backup_fallback else "atomic_exchange"),
        )
        self._append_preparation_state(journal, PromotionState.ROLLED_BACK, rolled_back)

    def _rollback_validated_promotion(
        self,
        *,
        preparation: RetainedPromotionPreparation,
        journal: PromotionJournal,
        records: tuple[PromotionJournalRecord, ...],
        base: PromotionIdentityEvidence,
        active: dict[str, object],
        retained: dict[str, object] | None,
        quarantined: dict[str, object] | None,
        retained_market: Path,
        processes_joined: bool,
    ) -> bool:
        if records[-1].state is not PromotionState.VALIDATED:
            return False
        if not processes_joined:
            raise _managed_root.CutoverSafetyError(
                "Validated promotion cannot defer without child ownership"
            )
        if not (
            self._location_matches(
                active,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            and self._location_matches(
                retained,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            and quarantined is None
        ):
            raise _managed_root.CutoverSafetyError(
                "Validated promotion filesystem identity is ambiguous"
            )
        self._restore_held_promotion_artifacts(preparation)
        retained = self._promotion_location_if_present(retained_market)
        if not self._location_matches(
            retained,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        ):
            raise _managed_root.CutoverSafetyError(
                "Validated promotion retained restoration is incomplete"
            )
        rolled_back = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=None,
            holding_current=None,
        )
        self._append_preparation_state(journal, PromotionState.ROLLED_BACK, rolled_back)
        return True

    def _rollback_holding_identity(
        self,
        preparation: RetainedPromotionPreparation,
        journal: PromotionJournal,
        base: PromotionIdentityEvidence,
    ) -> dict[str, object] | None:
        staging_candidates = (
            preparation.holding_root,
            self._cleanup_staging_root(journal.operation_id),
        )
        staging_fds: list[int] = []
        for candidate in staging_candidates:
            try:
                staging_fds.append(
                    self._managed().open_dir(self._managed_relative(candidate))
                )
            except FileNotFoundError:
                continue
        if len(staging_fds) > 1:
            for fd in staging_fds:
                os.close(fd)
            raise _managed_root.CutoverSafetyError(
                "Promotion artifact staging is ambiguous"
            )
        if not staging_fds:
            return None
        holding_fd = staging_fds[0]
        try:
            if (
                self._directory_identity_evidence(holding_fd)
                != preparation.holding_directory_identity
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion holding directory identity changed"
                )
            return base.holding_current
        finally:
            os.close(holding_fd)

    def _defer_retained_promotion_rollback(
        self,
        *,
        journal: PromotionJournal,
        base: PromotionIdentityEvidence,
        active: dict[str, object],
        retained: dict[str, object] | None,
        quarantined: dict[str, object] | None,
        holding: dict[str, object] | None,
    ) -> None:
        deferred = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=quarantined,
            holding_current=holding,
        )
        self._append_preparation_state(
            journal, PromotionState.ROLLBACK_DEFERRED, deferred
        )
        for lease in (self._active_lease, self._retained_lease):
            lease.unlock_on_release = False
            lease.owns_fd = False
        raise _managed_root.CutoverSafetyError(
            "Retained promotion rollback deferred with both leases held"
        )

    def _finish_exchanged_back_rollback(
        self,
        *,
        preparation: RetainedPromotionPreparation,
        journal: PromotionJournal,
        records: tuple[PromotionJournalRecord, ...],
        base: PromotionIdentityEvidence,
        active: dict[str, object],
        retained: dict[str, object] | None,
        quarantined: dict[str, object] | None,
    ) -> bool:
        if records[-1].state is not PromotionState.EXCHANGED_BACK:
            return False
        if base.rollback_mode == "atomic_exchange":
            valid_layout = (
                self._location_matches(
                    active,
                    directory=base.active_before_directory,
                    payload=base.active_before_payload,
                )
                and self._location_matches(
                    retained,
                    directory=base.retained_v4_directory,
                    payload=base.retained_v4_payload,
                )
                and quarantined is None
            )
        elif base.rollback_mode == "backup_restore":
            valid_layout = (
                active == base.active_current
                and retained == base.retained_current
                and quarantined == base.quarantine_current
                and quarantined is not None
                and quarantined["directory"] == base.active_before_directory
                and self._payload_manifest_entries(
                    cast(dict[str, object], active["payload"])
                )
                == self._payload_manifest_entries(base.active_before_payload)
                and self._location_matches(
                    retained,
                    directory=base.retained_v4_directory,
                    payload=base.retained_v4_payload,
                )
            )
        else:
            raise _managed_root.CutoverSafetyError(
                "EXCHANGED_BACK rollback mode is missing or invalid"
            )
        if not valid_layout:
            raise _managed_root.CutoverSafetyError(
                "EXCHANGED_BACK promotion filesystem identity mismatch"
            )
        self._restore_held_promotion_artifacts(
            preparation,
            owned_temp_collision_recovery_records=records,
        )
        self._remove_incomplete_consumed_marker(
            retained_report_id=preparation.eligibility.retained_report_id,
            operation_id=journal.operation_id,
        )
        rolled_back = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=quarantined,
            holding_current=None,
        )
        self._append_preparation_state(journal, PromotionState.ROLLED_BACK, rolled_back)
        return True
