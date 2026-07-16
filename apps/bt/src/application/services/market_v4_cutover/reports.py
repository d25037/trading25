"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import stat
from typing import Callable

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    SmokeConfig,
)
from .filesystem import _FILE_NOFOLLOW
from .evidence import MarketEvidence
from .workspace import CutoverWorkspace


class CutoverReportRepository:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: MarketEvidence,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence

    def _operation_report(
        self,
        *,
        report_id: str,
        phase: str,
        status: str,
        duration_seconds: float,
        api_checks: tuple[str, ...],
        server_log: str,
        evidence: dict[str, object] | None,
        phases: tuple[dict[str, object], ...],
        config: SmokeConfig,
        code_version: str,
        backup_id: str | None = None,
        rehearsal_report_id: str | None = None,
        rehearsal_mode: str | None = None,
        source_rehearsal_report_id: str | None = None,
        source_rehearsal_code_version: str | None = None,
        source_retained_root_fingerprint: str | None = None,
        source_market_identity_before: dict[str, object] | None = None,
        source_market_identity_after: dict[str, object] | None = None,
        error: str | None = None,
        error_message: str | None = None,
        cleanup_error: str | None = None,
        stop_error: str | None = None,
        restore_error: str | None = None,
        server_process_joined: bool | None = None,
        worker_process_joined: bool | None = None,
        target_root_fingerprint: str | None = None,
    ) -> dict[str, object]:
        report: dict[str, object] = {
            "reportId": report_id,
            "phase": phase,
            "status": status,
            "createdAt": self._workspace.now(),
            "durationSeconds": round(duration_seconds, 6),
            "codeVersion": code_version,
            "targetRootFingerprint": (
                target_root_fingerprint
                if target_root_fingerprint is not None
                else self._evidence.root_fingerprint(self._workspace.data_root)
            ),
            "command": [
                "python",
                "-m",
                "uvicorn",
                "src.entrypoints.http.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                "<reserved>",
            ],
            "apiChecks": list(api_checks),
            "serverLog": server_log,
            "schemaCoverage": evidence,
            "phases": list(phases),
            "smokeConfig": {
                "symbol": config.symbol,
                "strategy": config.strategy,
                "datasetPreset": config.dataset_preset,
            },
        }
        if backup_id is not None:
            report["backupId"] = backup_id
            report["backupManifest"] = f"backups/{backup_id}/manifest.json"
        if rehearsal_report_id is not None:
            report["rehearsalReportId"] = rehearsal_report_id
        if rehearsal_mode is not None:
            report["rehearsalMode"] = rehearsal_mode
        if source_rehearsal_report_id is not None:
            report["sourceRehearsalReportId"] = source_rehearsal_report_id
        if source_rehearsal_code_version is not None:
            report["sourceRehearsalCodeVersion"] = source_rehearsal_code_version
        if source_retained_root_fingerprint is not None:
            report["sourceRetainedRootFingerprint"] = source_retained_root_fingerprint
        if source_market_identity_before is not None:
            report["sourceMarketIdentityBefore"] = source_market_identity_before
        if source_market_identity_after is not None:
            report["sourceMarketIdentityAfter"] = source_market_identity_after
        if error is not None:
            report["errorType"] = error
        if error_message is not None:
            report["errorMessage"] = error_message
        if cleanup_error is not None:
            report["cleanupErrorType"] = cleanup_error
        if stop_error is not None:
            report["stopErrorType"] = stop_error
        if restore_error is not None:
            report["restoreErrorType"] = restore_error
        if server_process_joined is not None:
            report["serverProcessJoined"] = server_process_joined
        if worker_process_joined is not None:
            report["workerProcessJoined"] = worker_process_joined
        return report

    def _redact_diagnostic(
        self,
        message: str,
        environment: dict[str, str],
        *,
        max_chars: int = 1_024,
    ) -> str:
        redacted = message.replace(str(self._workspace.data_root), "<data-root>")
        path_keys = {
            "XDG_DATA_HOME",
            "TRADING25_DATA_DIR",
            "MARKET_TIMESERIES_DIR",
            "MARKET_DB_PATH",
            "DATASET_BASE_PATH",
            "PORTFOLIO_DB_PATH",
            "TRADING25_STRATEGIES_DIR",
            "TRADING25_BACKTEST_DIR",
            "TRADING25_DEFAULT_CONFIG_PATH",
        }
        for key, value in environment.items():
            if not value:
                continue
            upper = key.upper()
            if key in path_keys:
                if Path(value).is_absolute():
                    redacted = redacted.replace(value, f"<{key.lower()}>")
            elif any(
                token in upper for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")
            ):
                redacted = redacted.replace(value, "<redacted-secret>")
        redacted = redacted.replace(str(Path.home()), "<home>")
        if len(redacted) > max_chars:
            return redacted[: max_chars - 3] + "..."
        return redacted

    def _write_report(
        self,
        report_id: str,
        report: dict[str, object],
        *,
        expected_root_fingerprint: str | None = None,
        final_validator: Callable[[], None] | None = None,
    ) -> Path:
        if (
            expected_root_fingerprint is not None
            and self._evidence.root_fingerprint(self._workspace.data_root)
            != expected_root_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Active configuration changed before report write"
            )
        report_dir = self._workspace.operations_root / "reports" / report_id
        self._workspace._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._workspace._prepare_managed_directory(report_dir, exist_ok=True)
        report_path = report_dir / "report.json"
        self._workspace._assert_managed_target_absent(report_path)
        report_relative = self._workspace._managed_relative(report_path)
        report_dir_relative = report_relative.parent
        report_dir_fd = self._workspace.managed().open_dir(report_dir_relative)
        temporary_name = f".report.json.{secrets.token_hex(8)}.tmp"
        published = False
        temporary_created = False
        try:
            self._workspace._managed_mutation_hook("write")
            path_report_dir_fd = self._workspace.managed().open_dir(report_dir_relative)
            try:
                retained_stat = os.fstat(report_dir_fd)
                path_stat = os.fstat(path_report_dir_fd)
                if (retained_stat.st_dev, retained_stat.st_ino) != (
                    path_stat.st_dev,
                    path_stat.st_ino,
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Report directory identity changed"
                    )
            finally:
                os.close(path_report_dir_fd)
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW
            temporary_fd = os.open(temporary_name, flags, 0o600, dir_fd=report_dir_fd)
            temporary_created = True
            try:
                payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
                view = memoryview(payload)
                while view:
                    written = os.write(temporary_fd, view)
                    view = view[written:]
                os.fsync(temporary_fd)
            finally:
                os.close(temporary_fd)
            self._workspace._report_publish_hook("after_temp_fsync")
            if final_validator is not None:
                final_validator()
            os.link(
                temporary_name,
                "report.json",
                src_dir_fd=report_dir_fd,
                dst_dir_fd=report_dir_fd,
                follow_symlinks=False,
            )
            published = True
            self._workspace._report_publish_hook("after_publish")
            os.fsync(report_dir_fd)
            if final_validator is not None:
                final_validator()
            if (
                expected_root_fingerprint is not None
                and self._evidence.root_fingerprint(self._workspace.data_root)
                != expected_root_fingerprint
            ):
                raise _managed_root.CutoverSafetyError(
                    "Active configuration changed during report write"
                )
            os.unlink(temporary_name, dir_fd=report_dir_fd)
            temporary_created = False
            os.fsync(report_dir_fd)
        except Exception:
            if published:
                try:
                    os.unlink("report.json", dir_fd=report_dir_fd)
                except FileNotFoundError:
                    pass
            if temporary_created:
                try:
                    os.unlink(temporary_name, dir_fd=report_dir_fd)
                except FileNotFoundError:
                    pass
            os.fsync(report_dir_fd)
            raise
        finally:
            os.close(report_dir_fd)
        return report_path

    def _try_write_report(self, report_id: str, report: dict[str, object]) -> None:
        try:
            self._write_report(report_id, report)
        except Exception:
            pass

    def _read_report(self, report_id: str) -> dict[str, object]:
        path = self._workspace.operations_root / "reports" / report_id / "report.json"
        relative = self._workspace._managed_relative(path)
        try:
            report_mode = self._workspace.managed().stat(relative).st_mode
        except FileNotFoundError:
            raise _managed_root.CutoverSafetyError(
                "An exact passing rehearsal report is required"
            )
        if stat.S_ISLNK(report_mode) or not stat.S_ISREG(report_mode):
            raise _managed_root.CutoverSafetyError("Rehearsal report is invalid")
        try:
            value = json.loads(
                self._workspace.managed().read_bytes(relative).decode("utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Rehearsal report is unreadable"
            ) from exc
        if not isinstance(value, dict):
            raise _managed_root.CutoverSafetyError("Rehearsal report is invalid")
        return value
