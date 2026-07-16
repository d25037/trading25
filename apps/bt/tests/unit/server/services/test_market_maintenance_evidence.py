from __future__ import annotations

import json
import os
from pathlib import Path
import stat

from src.application.contracts.market_maintenance import (
    MaintenanceEvidenceStatus,
    MaintenanceOutcome,
    MarketMaintenanceRecord,
)
from src.infrastructure.db.market.market_maintenance_evidence import (
    read_market_maintenance_evidence,
    write_market_maintenance_evidence,
)


def _record() -> MarketMaintenanceRecord:
    return MarketMaintenanceRecord(
        schemaVersion=1,
        evidenceStatus=MaintenanceEvidenceStatus.VALID,
        outcome=MaintenanceOutcome.PASSED,
        operation="incremental_sync",
        recordedAt="2026-07-16T00:00:00+00:00",
        compacted=False,
        trigger="none",
        beforeBytes=1024,
        afterBytes=1024,
        durationMs=1.25,
        validation="passed",
    )


def test_missing_maintenance_sidecar_is_never_run(tmp_path: Path) -> None:
    snapshot = read_market_maintenance_evidence(tmp_path)

    assert snapshot.evidenceStatus is MaintenanceEvidenceStatus.NEVER_RUN
    assert snapshot.outcome is MaintenanceOutcome.NEVER_RUN


def test_maintenance_sidecar_round_trips_strict_valid_record(tmp_path: Path) -> None:
    write_market_maintenance_evidence(tmp_path, _record())

    snapshot = read_market_maintenance_evidence(tmp_path)

    assert snapshot == _record()
    assert not list(tmp_path.glob(".maintenance.v1.json.*.tmp"))


def test_malformed_or_extra_field_sidecar_is_invalid(tmp_path: Path) -> None:
    (tmp_path / "maintenance.v1.json").write_text(
        json.dumps({**_record().model_dump(mode="json"), "unexpected": True}),
        encoding="utf-8",
    )

    snapshot = read_market_maintenance_evidence(tmp_path)

    assert snapshot.evidenceStatus is MaintenanceEvidenceStatus.INVALID
    assert snapshot.outcome is MaintenanceOutcome.INVALID
    assert snapshot.error is not None


def test_failed_maintenance_is_valid_evidence_with_actionable_retry(
    tmp_path: Path,
) -> None:
    failed = MarketMaintenanceRecord.failed(
        operation="stock_refresh",
        recorded_at="2026-07-16T00:00:00+00:00",
        error="hard cap remains exceeded",
    )

    write_market_maintenance_evidence(tmp_path, failed)
    snapshot = read_market_maintenance_evidence(tmp_path)

    assert snapshot.evidenceStatus is MaintenanceEvidenceStatus.VALID
    assert snapshot.outcome is MaintenanceOutcome.FAILED
    assert snapshot.recoveryCommand == "uv run bt market-maintain"
    assert snapshot.error == "hard cap remains exceeded"


def test_sidecar_symlink_is_not_followed_on_read_or_atomic_replace(
    tmp_path: Path,
) -> None:
    target = tmp_path / "outside.json"
    target.write_text("outside", encoding="utf-8")
    (tmp_path / "maintenance.v1.json").symlink_to(target)

    invalid = read_market_maintenance_evidence(tmp_path)
    assert invalid.evidenceStatus is MaintenanceEvidenceStatus.INVALID

    write_market_maintenance_evidence(tmp_path, _record())
    assert target.read_text(encoding="utf-8") == "outside"
    assert not (tmp_path / "maintenance.v1.json").is_symlink()
    assert read_market_maintenance_evidence(tmp_path) == _record()


def test_atomic_sidecar_fsyncs_file_then_parent_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fsynced_modes: list[int] = []
    original_fsync = os.fsync

    def observe(fd: int) -> None:
        fsynced_modes.append(os.fstat(fd).st_mode)
        original_fsync(fd)

    monkeypatch.setattr(os, "fsync", observe)
    write_market_maintenance_evidence(tmp_path, _record())

    assert len(fsynced_modes) == 2
    assert stat.S_ISREG(fsynced_modes[0])
    assert stat.S_ISDIR(fsynced_modes[1])


def test_non_utf8_sidecar_is_invalid(tmp_path: Path) -> None:
    (tmp_path / "maintenance.v1.json").write_bytes(b"\xff\xfe")

    snapshot = read_market_maintenance_evidence(tmp_path)

    assert snapshot.evidenceStatus is MaintenanceEvidenceStatus.INVALID
