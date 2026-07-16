"""Market v4 cutover promotion recovery tests."""

from __future__ import annotations

import errno
import os
from pathlib import Path
import shutil
from typing import cast

import pytest

from src.application.services.market_v4_cutover.contracts import (
    PromotionState,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.services.market_v4_cutover_test_support import (
    _market_root,
    _retained_promotion_source,
    _filesystem_identity_snapshot,
    _owned_temp_collision_records,
)


@pytest.mark.parametrize(
    "rejection",
    [
        "nonempty_duplicate",
        "symlink_duplicate",
        "special_duplicate",
        "wrong_order",
        "missing_state",
        "wrong_rollback_mode",
        "missing_staged_evidence",
        "wrong_staged_evidence",
        "additional_duplicate",
        "unexpected_artifact",
    ],
)
def test_owned_temp_collision_recovery_rejection_has_zero_mutation(
    tmp_path: Path,
    rejection: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    original_temp = retained_root / "market-timeseries/duckdb-tmp"
    original_temp.mkdir()

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._workspace.managed(), report_id, now=service._workspace.now)
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        duplicate = retained_root / "market-timeseries/duckdb-tmp"
        duplicate.mkdir()
        if rejection == "nonempty_duplicate":
            (duplicate / "payload").write_bytes(b"not-empty")
        elif rejection == "symlink_duplicate":
            duplicate.rmdir()
            duplicate.symlink_to(tmp_path, target_is_directory=True)
        elif rejection == "special_duplicate":
            duplicate.rmdir()
            os.mkfifo(duplicate)
        elif rejection == "unexpected_artifact":
            (retained_root / "market-timeseries/unexpected").write_bytes(b"extra")
        elif rejection == "additional_duplicate":
            other = next(
                artifact
                for artifact in preparation.detached_artifacts
                if artifact.name != "duckdb-tmp"
            )
            source = preparation.holding_root / other.name
            target = retained_root / "market-timeseries" / other.name
            if other.kind == "directory":
                shutil.copytree(source, target)
            else:
                shutil.copy2(source, target)

        base = journal.read_validated()[-1].identities
        states = (
            (
                PromotionState.CLEANUP_STAGED,
                PromotionState.ACTIVE_SMOKE_PASSED,
                PromotionState.EXCHANGED_BACK,
            )
            if rejection == "wrong_order"
            else (
                (PromotionState.CLEANUP_STAGED, PromotionState.EXCHANGED_BACK)
                if rejection == "missing_state"
                else (
                    PromotionState.ACTIVE_SMOKE_PASSED,
                    PromotionState.CLEANUP_STAGED,
                    PromotionState.EXCHANGED_BACK,
                )
            )
        )
        rollback_mode = (
            "backup_restore"
            if rejection == "wrong_rollback_mode"
            else "atomic_exchange"
        )
        detached = tuple(
            artifact.to_mapping() for artifact in preparation.detached_artifacts
        )
        if rejection == "missing_staged_evidence":
            detached = tuple(
                artifact for artifact in detached if artifact["name"] != "duckdb-tmp"
            )
        elif rejection == "wrong_staged_evidence":
            detached = tuple(
                {
                    **artifact,
                    "identity": {
                        **cast(dict[str, object], artifact["identity"]),
                        "inode": 999_999_999,
                    },
                }
                if artifact["name"] == "duckdb-tmp"
                else artifact
                for artifact in detached
            )
        records = _owned_temp_collision_records(
            preparation,
            base,
            states=states,
            rollback_mode=rollback_mode,
            detached_artifacts=detached,
        )
        before = _filesystem_identity_snapshot(data_root)

        with pytest.raises(CutoverSafetyError):
            service._promotion._cleanup._restore_held_promotion_artifacts(
                preparation,
                owned_temp_collision_recovery_records=records,
            )

        assert _filesystem_identity_snapshot(data_root) == before
        assert journal.read_validated()[-1].state is PromotionState.PREPARED


def test_owned_temp_collision_parent_fsync_failure_fences_without_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    original_temp = retained_root / "market-timeseries/duckdb-tmp"
    original_temp.mkdir()
    original_inode = original_temp.stat().st_ino

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._workspace.managed(), report_id, now=service._workspace.now)
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        duplicate = retained_root / "market-timeseries/duckdb-tmp"
        duplicate.mkdir()
        records = _owned_temp_collision_records(
            preparation,
            journal.read_validated()[-1].identities,
        )
        original_fsync = os.fsync
        calls = 0

        def fail_first_fsync(fd: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError(errno.EIO, "injected parent fsync failure")
            original_fsync(fd)

        monkeypatch.setattr(os, "fsync", fail_first_fsync)
        with pytest.raises(CutoverSafetyError, match="not durable"):
            service._promotion._cleanup._restore_held_promotion_artifacts(
                preparation,
                owned_temp_collision_recovery_records=records,
            )

        assert not duplicate.exists()
        staged = preparation.holding_root / "duckdb-tmp"
        assert staged.stat().st_ino == original_inode
        assert journal.read_validated()[-1].state is PromotionState.PREPARED
        assert service._workspace._active_lease is not None
        assert service._workspace._retained_lease is not None
        assert service._workspace._active_lease.unlock_on_release is False
        assert service._workspace._retained_lease.unlock_on_release is False
        assert service._workspace._active_lease.owns_fd is False
        assert service._workspace._retained_lease.owns_fd is False

        # Test-only cleanup of deliberately fail-stop descriptors.
        monkeypatch.setattr(os, "fsync", original_fsync)
        for lease in (service._workspace._active_lease, service._workspace._retained_lease):
            assert lease is not None
            os.close(lease.fd)
            os.close(lease.root_fd)
            lease.fd = -1
            lease.root_fd = -1


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "drift", "extra"])
def test_restore_held_artifacts_preflight_failure_has_zero_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    (retained_root / "market-timeseries/market.duckdb.wal").touch()

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._workspace.managed(), report_id, now=service._workspace.now)
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        later_artifact = preparation.detached_artifacts[-1]
        assert later_artifact.kind == "regular"
        staged_later = preparation.holding_root / later_artifact.name
        retained_later = retained_root / "market-timeseries" / later_artifact.name
        if mutation == "missing":
            staged_later.rename(preparation.holding_root.parent / "missing-later")
        elif mutation == "duplicate":
            os.link(staged_later, retained_later)
        elif mutation == "drift":
            staged_later.write_bytes(b"drift")
        else:
            (preparation.holding_root / "unexpected-extra").write_bytes(b"extra")

        before = _filesystem_identity_snapshot(data_root)
        with pytest.raises(CutoverSafetyError):
            service._promotion._cleanup._restore_held_promotion_artifacts(preparation)
        assert _filesystem_identity_snapshot(data_root) == before
