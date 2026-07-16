"""Atomic, descriptor-confined Market maintenance evidence sidecar."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
from uuid import uuid4

from pydantic import ValidationError

from src.application.contracts.market_maintenance import (
    MaintenanceEvidenceStatus,
    MarketMaintenanceRecord,
)


SIDECAR_NAME = "maintenance.v1.json"


def _open_market_root(market_root: Path) -> int:
    root_stat = market_root.lstat()
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError("Market maintenance evidence root must be a real directory")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    return os.open(market_root, flags)


def read_market_maintenance_evidence(market_root: Path) -> MarketMaintenanceRecord:
    try:
        root_fd = _open_market_root(market_root)
    except OSError as exc:
        return MarketMaintenanceRecord.invalid(f"Evidence root is unavailable: {exc}")
    try:
        try:
            fd = os.open(
                SIDECAR_NAME,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
        except FileNotFoundError:
            return MarketMaintenanceRecord.never_run()
        try:
            file_stat = os.fstat(fd)
            if not stat.S_ISREG(file_stat.st_mode):
                return MarketMaintenanceRecord.invalid(
                    "Maintenance evidence is not a regular file"
                )
            chunks: list[bytes] = []
            while chunk := os.read(fd, 64 * 1024):
                chunks.append(chunk)
        finally:
            os.close(fd)
    except (OSError, UnicodeError) as exc:
        return MarketMaintenanceRecord.invalid(
            f"Maintenance evidence read failed: {exc}"
        )
    finally:
        os.close(root_fd)

    try:
        payload = json.loads(b"".join(chunks).decode("utf-8", errors="strict"))
        record = MarketMaintenanceRecord.model_validate(payload)
        if (
            record.schemaVersion != 1
            or record.evidenceStatus is not MaintenanceEvidenceStatus.VALID
        ):
            raise ValueError("Maintenance evidence schema or status is invalid")
        return record
    except (UnicodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
        return MarketMaintenanceRecord.invalid(
            f"Maintenance evidence is invalid: {exc}"
        )


def write_market_maintenance_evidence(
    market_root: Path,
    record: MarketMaintenanceRecord,
) -> None:
    if (
        record.schemaVersion != 1
        or record.evidenceStatus is not MaintenanceEvidenceStatus.VALID
    ):
        raise ValueError("Only valid schema v1 maintenance evidence may be persisted")
    encoded = (
        json.dumps(
            record.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    root_fd = _open_market_root(market_root)
    temp_name = f".{SIDECAR_NAME}.{uuid4().hex}.tmp"
    temp_created = False
    try:
        fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        temp_created = True
        try:
            offset = 0
            while offset < len(encoded):
                offset += os.write(fd, encoded[offset:])
            os.fsync(fd)
        finally:
            os.close(fd)
        os.rename(temp_name, SIDECAR_NAME, src_dir_fd=root_fd, dst_dir_fd=root_fd)
        temp_created = False
        os.fsync(root_fd)
    finally:
        if temp_created:
            try:
                os.unlink(temp_name, dir_fd=root_fd)
                os.fsync(root_fd)
            except FileNotFoundError:
                pass
        os.close(root_fd)
