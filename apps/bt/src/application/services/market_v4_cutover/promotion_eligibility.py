"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import quote

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import SmokeConfig
from .duckdb_service import MarketIdentityService
from .filesystem import _DIR_OPEN_FLAGS, DarwinAtomicExchange
from .workspace import CutoverWorkspace


class PromotionEligibilityService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        market_identity: MarketIdentityService,
    ) -> None:
        self._workspace = workspace
        self._market_identity = market_identity

    @staticmethod
    def _market_identity_evidence_valid(value: object) -> bool:
        if not isinstance(value, dict) or set(value) != {
            "marketDuckdb",
            "parquetSha256",
        }:
            return False

        def file_identity_valid(identity: object) -> bool:
            return (
                isinstance(identity, dict)
                and set(identity) == {"device", "inode", "size", "sha256"}
                and all(
                    isinstance(identity.get(key), int) and int(identity[key]) >= 0
                    for key in ("device", "inode", "size")
                )
                and isinstance(identity.get("sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", identity["sha256"]) is not None
            )

        parquet = value.get("parquetSha256")
        return (
            file_identity_valid(value.get("marketDuckdb"))
            and isinstance(parquet, dict)
            and bool(parquet)
            and all(
                isinstance(path, str)
                and bool(path)
                and not Path(path).is_absolute()
                and ".." not in Path(path).parts
                and file_identity_valid(identity)
                for path, identity in parquet.items()
            )
        )

    @staticmethod
    def _retained_report_contract_valid(
        report: dict[str, object],
        *,
        report_id: str,
        config: SmokeConfig,
    ) -> bool:
        api_checks = report.get("apiChecks")
        required_api_checks = {
            "/api/db/stats",
            "/api/db/validate",
            f"/api/analytics/fundamentals/{quote(config.symbol, safe='')}",
            "/api/fundamentals/compute",
            "/api/analytics/screening/jobs",
            "/api/analytics/fundamental-ranking",
            "/api/dataset",
        }
        if (
            not isinstance(api_checks, list)
            or not all(isinstance(path, str) for path in api_checks)
            or not required_api_checks.issubset(set(api_checks))
            or not any(
                path.startswith("/api/analytics/screening/jobs/") for path in api_checks
            )
            or not any(
                "/api/analytics/screening/result/" in path for path in api_checks
            )
            or not any(path.startswith("/api/dataset/jobs/") for path in api_checks)
            or not any(path.endswith("/info") for path in api_checks)
            or not any("/sample?count=1" in path for path in api_checks)
            or any(
                forbidden in path
                for path in api_checks
                for forbidden in (
                    "/api/db/sync",
                    "materialize",
                    "stocks/refresh",
                    "intraday/sync",
                )
            )
        ):
            return False
        coverage = report.get("schemaCoverage")
        lineage_keys = {
            "sourceStatementKeyCount",
            "expectedAdjustedStatementRows",
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "missingDailyValuationRows",
            "extraDailyValuationRows",
            "wrongBasisDailyValuationRows",
            "currentBasisStatementCount",
            "dailyValuationRows",
            "readyProviderWindowCount",
        }
        if not isinstance(coverage, dict) or set(coverage) != {
            "schemaVersion",
            "stockPriceAdjustmentMode",
            "providerVintage",
        }:
            return False
        lineage = coverage.get("providerVintage")
        if (
            coverage.get("schemaVersion") != 5
            or coverage.get("stockPriceAdjustmentMode")
            != "provider_adjusted_v1"
            or not isinstance(lineage, dict)
            or set(lineage) != lineage_keys
            or any(not isinstance(value, int) for value in lineage.values())
            or any(
                lineage[key] <= 0
                for key in (
                    "sourceStatementKeyCount",
                    "expectedAdjustedStatementRows",
                    "currentBasisStatementCount",
                    "dailyValuationRows",
                    "readyProviderWindowCount",
                )
            )
            or any(
                lineage[key] != 0
                for key in lineage_keys
                - {
                    "sourceStatementKeyCount",
                    "expectedAdjustedStatementRows",
                    "currentBasisStatementCount",
                    "dailyValuationRows",
                    "readyProviderWindowCount",
                }
            )
        ):
            return False
        phases = report.get("phases")
        if not isinstance(phases, list) or not any(
            isinstance(phase, dict)
            and phase.get("name") == "retained_market_smoke"
            and phase.get("status") == "passed"
            and isinstance(phase.get("durationSeconds"), (int, float))
            and float(phase["durationSeconds"]) >= 0
            for phase in phases
        ):
            return False
        before = report.get("sourceMarketIdentityBefore")
        return (
            report.get("reportId") == report_id
            and PromotionEligibilityService._market_identity_evidence_valid(before)
            and before == report.get("sourceMarketIdentityAfter")
        )

    @staticmethod
    def _full_rebuild_report_contract_valid(
        report: dict[str, object],
        *,
        config: SmokeConfig,
    ) -> bool:
        api_checks = report.get("apiChecks")
        if (
            not isinstance(api_checks, list)
            or not all(isinstance(path, str) for path in api_checks)
            or "/api/db/sync" not in api_checks
            or not any(path.startswith("/api/db/sync/jobs/") for path in api_checks)
        ):
            return False
        synthetic_identity = {
            "marketDuckdb": {
                "device": 0,
                "inode": 0,
                "size": 0,
                "sha256": "0" * 64,
            },
            "parquetSha256": {
                "evidence.parquet": {
                    "device": 0,
                    "inode": 0,
                    "size": 0,
                    "sha256": "0" * 64,
                }
            },
        }
        semantic_report = {
            **report,
            "apiChecks": [
                path for path in api_checks if not path.startswith("/api/db/sync")
            ],
            "phases": [
                {
                    "name": "retained_market_smoke",
                    "status": "passed",
                    "durationSeconds": 0,
                }
            ],
            "sourceMarketIdentityBefore": synthetic_identity,
            "sourceMarketIdentityAfter": synthetic_identity,
        }
        if not PromotionEligibilityService._retained_report_contract_valid(
            semantic_report,
            report_id=str(report.get("reportId", "")),
            config=config,
        ):
            return False
        phases = report.get("phases")
        required_phases = {
            "initial_sync_and_provider_vintage",
            "semantic_smoke",
        }
        return isinstance(phases, list) and required_phases == {
            str(phase.get("name"))
            for phase in phases
            if isinstance(phase, dict)
            and phase.get("status") == "passed"
            and isinstance(phase.get("durationSeconds"), (int, float))
            and float(phase["durationSeconds"]) >= 0
        }

    def _promotion_report_snapshot(
        self,
        report_id: str,
    ) -> tuple[dict[str, object], str, tuple[int, int, int, int, int]]:
        report_id = self._workspace._validate_id(report_id, label="rehearsal report")
        relative = (
            Path("operations/market-v4-cutover/reports") / report_id / ("report.json")
        )
        try:
            metadata, digest = self._market_identity.regular_file_identity(
                self._workspace.managed(), relative
            )
            payload = self._workspace.managed().read_bytes(relative)
            current, current_digest = self._market_identity.regular_file_identity(
                self._workspace.managed(), relative
            )
        except FileNotFoundError as exc:
            raise _managed_root.CutoverSafetyError(
                "An exact passing retained rehearsal report is required"
            ) from exc
        identity = (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        )
        if (
            identity
            != (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
                current.st_ctime_ns,
            )
            or digest != current_digest
            or hashlib.sha256(payload).hexdigest() != digest
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion report changed during validation"
            )
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Promotion report is unreadable"
            ) from exc
        if not isinstance(value, dict):
            raise _managed_root.CutoverSafetyError("Promotion report is invalid")
        return value, digest, identity

    def _assert_promotion_destination_absent(self, relative: Path) -> None:
        try:
            self._workspace.managed().stat(relative)
        except FileNotFoundError:
            return
        raise _managed_root.CutoverSafetyError("Promotion destination already exists")

    @staticmethod
    def _assert_empty_directory(directory_fd: int, name: str) -> None:
        child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
        try:
            if os.listdir(child):
                raise _managed_root.CutoverSafetyError(
                    "Retained Market temporary artifact is not empty"
                )
        finally:
            os.close(child)

    def _validate_retained_market_allowlist(
        self,
        root_fd: int,
        *,
        proven_runtime_names: tuple[str, ...],
    ) -> None:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            allowed_runtime_names = set(proven_runtime_names)
            for name in os.listdir(market_fd):
                entry = os.stat(name, dir_fd=market_fd, follow_symlinks=False)
                if name in {"market.duckdb", "parquet"}:
                    continue
                if name == "market.duckdb.wal":
                    if not stat.S_ISREG(entry.st_mode) or entry.st_size != 0:
                        raise _managed_root.CutoverSafetyError(
                            "Nonempty or invalid retained DuckDB WAL"
                        )
                    continue
                if name == "maintenance.v1.json":
                    if not stat.S_ISREG(entry.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Retained Market maintenance evidence is invalid"
                        )
                    continue
                if name == "duckdb-tmp" or name in allowed_runtime_names:
                    if not stat.S_ISDIR(entry.st_mode) or stat.S_ISLNK(entry.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Retained Market artifact must be a real directory"
                        )
                    if name == "duckdb-tmp":
                        PromotionEligibilityService._assert_empty_directory(
                            market_fd, name
                        )
                    continue
                raise _managed_root.CutoverSafetyError(
                    "Retained Market contains an unexpected artifact"
                )
        finally:
            os.close(market_fd)

    def _proven_retained_runtime_names(
        self,
        root_fd: int,
        *,
        source_report_id: str,
        retained_report_id: str,
        source_report_code_version: str,
        source_market_identity: dict[str, object],
        retained_root_fingerprint: str,
        target_root_fingerprint: str,
    ) -> tuple[str, ...]:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            runtime_names = sorted(
                name
                for name in os.listdir(market_fd)
                if name.startswith(".cutover-runtime-")
            )
        finally:
            os.close(market_fd)
        source_runtime = f".cutover-runtime-{source_report_id}"
        selected_runtime = f".cutover-runtime-{retained_report_id}"
        proven = {source_runtime, selected_runtime}
        prefix = ".cutover-runtime-"
        for runtime_name in runtime_names:
            if runtime_name in proven:
                continue
            report_id = runtime_name.removeprefix(prefix)
            try:
                report_id = self._workspace._validate_id(
                    report_id, label="retained report"
                )
                report, _sha256, _identity = self._promotion_report_snapshot(report_id)
            except _managed_root.CutoverSafetyError:
                continue
            if (
                report.get("reportId") == report_id
                and report.get("phase") == "rehearsal"
                and report.get("status") in {"passed", "failed"}
                and report.get("rehearsalMode") == "retained_market_smoke"
                and report.get("sourceRehearsalReportId") == source_report_id
                and report.get("sourceRehearsalCodeVersion")
                == source_report_code_version
                and report.get("sourceRetainedRootFingerprint")
                == retained_root_fingerprint
                and report.get("targetRootFingerprint") == target_root_fingerprint
                and report.get("serverProcessJoined") is True
                and report.get("workerProcessJoined") is True
                and report.get("sourceMarketIdentityBefore") == source_market_identity
                and report.get("sourceMarketIdentityAfter") == source_market_identity
            ):
                proven.add(runtime_name)
        ordered = [source_runtime]
        ordered.extend(sorted(proven - {source_runtime, selected_runtime}))
        if selected_runtime != source_runtime:
            ordered.append(selected_runtime)
        return tuple(ordered)

    def _assert_promotion_exchange_capability(
        self,
        retained_lease: _market_operation_lease.MarketOperationLease,
    ) -> None:
        active_market = self._workspace.managed().open_dir(Path("market-timeseries"))
        retained_market = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_lease.root_fd
        )
        try:
            devices = {
                os.fstat(self._workspace.managed().fd).st_dev,
                os.fstat(active_market).st_dev,
                os.fstat(retained_lease.root_fd).st_dev,
                os.fstat(retained_market).st_dev,
            }
            if len(devices) != 1:
                raise _managed_root.CutoverSafetyError(
                    "Atomic exchange directories must be on the same device"
                )
        finally:
            os.close(active_market)
            os.close(retained_market)
        if isinstance(self._workspace.atomic_exchange, DarwinAtomicExchange):
            self._workspace.atomic_exchange.require_capability()
        else:
            capability = getattr(
                self._workspace.atomic_exchange, "require_capability", None
            )
            if capability is not None:
                capability()
