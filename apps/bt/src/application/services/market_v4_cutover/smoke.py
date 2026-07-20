"""Focused Market v5 cutover semantic smoke."""

from __future__ import annotations

import hashlib
import os
from datetime import date
from pathlib import Path
import re
import time
from typing import cast
from urllib.parse import quote

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import (
    ApiAdapter,
    MarketSourceMetadata,
    SmokeConfig,
    SmokeResult,
)
from .errors import WorkerShutdownError
from .project_paths import (
    repository_cutover_smoke_strategy_path,
    repository_default_config_path,
)
from .workspace import CutoverWorkspace

_CREATE_JOB_RESPONSE_FIELDS: dict[str, tuple[str, str]] = {
    "/api/db/sync": ("sync", "jobId"),
    "/api/analytics/screening/jobs": ("screening", "job_id"),
    "/api/dataset": ("dataset", "jobId"),
}


def overlay_repository_cutover_smoke_strategy(
    workspace: CutoverWorkspace,
    runtime_root: Path,
) -> str:
    """Overlay the canonical smoke strategy inside an operation-owned runtime."""

    smoke_strategy_source = repository_cutover_smoke_strategy_path()
    if not smoke_strategy_source.is_file():
        raise _managed_root.CutoverSafetyError(
            "Repository cutover smoke strategy is missing"
        )
    source_sha256 = hashlib.sha256(smoke_strategy_source.read_bytes()).hexdigest()
    runtime_production = runtime_root / "strategies" / "production"
    workspace._prepare_managed_directory(runtime_production, exist_ok=True)
    runtime_smoke_strategy = runtime_production / "cutover_smoke.yaml"
    workspace.managed().unlink(
        workspace._managed_relative(runtime_smoke_strategy),
        missing_ok=True,
    )
    workspace._copy_regular_to_managed(
        smoke_strategy_source,
        runtime_smoke_strategy,
    )
    return source_sha256


class RuntimeSmokeService:
    """Run semantic smoke checks against one explicitly selected market root."""

    def __init__(self, workspace: CutoverWorkspace) -> None:
        self._workspace = workspace

    def prepare_isolated_root(self, root: Path, *, runtime_name: str) -> None:
        workspace = self._workspace
        workspace._prepare_managed_directory(root, exist_ok=False)
        for relative in ("market-timeseries", "datasets", "backtest", "config"):
            workspace._prepare_managed_directory(root / relative, exist_ok=False)
        source_config = workspace.data_root / "config" / "default.yaml"
        if not source_config.is_file():
            source_config = repository_default_config_path()
        if not source_config.is_file():
            raise _managed_root.CutoverSafetyError(
                "Repository default configuration is missing"
            )
        config_target = root / "config" / "default.yaml"
        workspace._assert_managed_target_absent(config_target)
        workspace._copy_regular_to_managed(source_config, config_target)
        try:
            active_strategies_fd = workspace.managed().open_dir(Path("strategies"))
        except FileNotFoundError:
            workspace._prepare_managed_directory(
                root / "strategies",
                exist_ok=False,
            )
        else:
            os.close(active_strategies_fd)
            workspace.managed().copy_tree_create(
                Path("strategies"),
                workspace._managed_relative(root / "strategies"),
            )
        runtime_root = root / "market-timeseries" / runtime_name
        workspace._prepare_managed_directory(runtime_root, exist_ok=False)
        for relative in ("datasets", "backtest", "config"):
            workspace._prepare_managed_directory(
                runtime_root / relative,
                exist_ok=False,
            )
        runtime_config = runtime_root / "config" / "default.yaml"
        workspace._assert_managed_target_absent(runtime_config)
        workspace._copy_regular_to_managed(config_target, runtime_config)
        workspace.managed().copy_tree_create(
            workspace._managed_relative(root / "strategies"),
            workspace._managed_relative(runtime_root / "strategies"),
        )
        overlay_repository_cutover_smoke_strategy(
            workspace,
            runtime_root,
        )

    @staticmethod
    def isolated_environment(
        inherited: dict[str, str],
        *,
        lease_fd: int,
        root_fd: int,
        runtime_name: str,
    ) -> dict[str, str]:
        environment = dict(inherited)
        environment.pop("TRADING25_RUNTIME_CAPABILITY", None)
        environment.update(
            {
                "XDG_DATA_HOME": f"{runtime_name}/xdg-data-home",
                "TRADING25_DATA_DIR": runtime_name,
                "MARKET_TIMESERIES_DIR": ".",
                "MARKET_DB_PATH": "market.duckdb",
                "TRADING25_DUCKDB_TEMP_DIR": f"{runtime_name}/duckdb-tmp",
                "DATASET_BASE_PATH": f"{runtime_name}/datasets",
                "PORTFOLIO_DB_PATH": f"{runtime_name}/portfolio.db",
                "TRADING25_STRATEGIES_DIR": f"{runtime_name}/strategies",
                "TRADING25_BACKTEST_DIR": f"{runtime_name}/backtest",
                "TRADING25_DEFAULT_CONFIG_PATH": (
                    f"{runtime_name}/config/default.yaml"
                ),
                "TRADING25_MARKET_OPERATION_LOCK_FD": str(lease_fd),
                "TRADING25_DATA_ROOT_FD": str(root_fd),
            }
        )
        return environment

    def run_rebuild(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        root: Path,
        operation_id: str,
        *,
        market_directory_fd: int,
        guard_lease_fd: int,
    ) -> tuple[
        tuple[str, ...],
        dict[str, object],
        tuple[dict[str, object], ...],
        os.stat_result,
    ]:
        sync_started = time.monotonic()
        sync = api.request(
            "POST",
            "/api/db/sync",
            {
                "mode": "initial",
                "resetBeforeSync": False,
                "enforceBulkForStockData": True,
            },
        )
        job_id = self._require_job_id(sync, "/api/db/sync")
        self._poll_api_job(
            api,
            f"/api/db/sync/jobs/{quote(job_id, safe='')}",
            "sync",
        )
        sync_duration = time.monotonic() - sync_started
        smoke_started = time.monotonic()
        market_identity = os.fstat(market_directory_fd)
        result = self.smoke(
            api,
            config,
            operation_id=operation_id,
            market_root=root / "market-timeseries",
            market_directory_fd=market_directory_fd,
            guard_lease_fd=guard_lease_fd,
        )
        smoke_duration = time.monotonic() - smoke_started
        return (
            (
                "/api/db/sync",
                f"/api/db/sync/jobs/{job_id}",
                *result.api_paths,
            ),
            {
                "schemaVersion": result.schema_version,
                "stockPriceAdjustmentMode": result.adjustment_mode,
                "providerVintage": result.lineage,
            },
            (
                {
                    "name": "initial_sync_and_provider_vintage",
                    "status": "passed",
                    "durationSeconds": round(sync_duration, 6),
                },
                {
                    "name": "semantic_smoke",
                    "status": "passed",
                    "durationSeconds": round(smoke_duration, 6),
                },
            ),
            market_identity,
        )

    def smoke(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        *,
        operation_id: str,
        market_root: Path | None = None,
        market_directory_fd: int | None = None,
        guard_lease_fd: int | None = None,
    ) -> SmokeResult:
        if guard_lease_fd is None:
            with _market_operation_lease.MarketOperationLease.acquire(
                self._workspace.data_root,
                exclusive=False,
            ) as smoke_lease:
                try:
                    return self.smoke(
                        api,
                        config,
                        operation_id=operation_id,
                        market_root=market_root,
                        market_directory_fd=market_directory_fd,
                        guard_lease_fd=smoke_lease.fd,
                    )
                except WorkerShutdownError as exc:
                    if not exc.process_joined:
                        smoke_lease.unlock_on_release = False
                    raise
        operation_id = self._workspace._validate_id(operation_id, label="operation")
        inspected_root = market_root or self._workspace.market_root
        metadata = self._inspect_smoke_metadata(
            inspected_root=inspected_root,
            market_directory_fd=market_directory_fd,
            guard_lease_fd=guard_lease_fd,
        )
        stats_adjusted, adjusted, zero_counters = self._validate_smoke_lineage(api)
        symbol, screening_job_id = self._smoke_analytics(api, config)
        dataset_name, dataset_job_id = self._smoke_dataset(
            api,
            config,
            operation_id=operation_id,
            provider_vintage=stats_adjusted,
        )
        assert metadata.schema_version is not None
        assert metadata.adjustment_mode is not None
        return SmokeResult(
            schema_version=metadata.schema_version,
            adjustment_mode=metadata.adjustment_mode,
            checks=(
                "market_metadata",
                "adjusted_metrics_lineage",
                "fundamentals_parity",
                "screening",
                "fundamental_ranking",
                "dataset_create_info_open",
            ),
            api_paths=(
                "/api/db/stats",
                "/api/db/validate",
                f"/api/analytics/fundamentals/{symbol}",
                "/api/fundamentals/compute",
                "/api/analytics/screening/jobs",
                f"/api/analytics/screening/jobs/{screening_job_id}",
                f"/api/analytics/screening/result/{screening_job_id}",
                "/api/analytics/fundamental-ranking",
                "/api/dataset",
                f"/api/dataset/jobs/{dataset_job_id}",
                f"/api/dataset/{dataset_name}/info",
                f"/api/dataset/{dataset_name}/sample?count=1",
            ),
            lineage={
                **stats_adjusted,
                **{
                    key: int(cast(int | str, adjusted[key]))
                    for key in (
                        "sourceStatementKeyCount",
                        "expectedAdjustedStatementRows",
                        *zero_counters,
                    )
                },
                "currentBasisStatementCount": int(
                    cast(int | str, stats_adjusted["currentBasisStatementCount"])
                ),
                "currentBasisStateCount": int(
                    cast(int | str, stats_adjusted["currentBasisStateCount"])
                ),
                "providerWindowCount": int(
                    cast(int | str, stats_adjusted["providerWindowCount"])
                ),
                "readyProviderWindowCount": int(
                    cast(int | str, stats_adjusted["readyProviderWindowCount"])
                ),
            },
        )

    def _inspect_smoke_metadata(
        self,
        *,
        inspected_root: Path,
        market_directory_fd: int | None,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        if market_directory_fd is not None:
            inspected_fd = os.dup(market_directory_fd)
            try:
                metadata = self._workspace.duckdb.inspect(
                    inspected_fd,
                    "market.duckdb",
                    guard_lease_fd=guard_lease_fd,
                )
            finally:
                os.close(inspected_fd)
        else:
            with self._workspace.managed_root_scope():
                inspected_fd = self._workspace.managed().open_dir(
                    self._workspace._managed_relative(inspected_root)
                )
                try:
                    metadata = self._workspace.duckdb.inspect(
                        inspected_fd,
                        "market.duckdb",
                        guard_lease_fd=guard_lease_fd,
                    )
                finally:
                    os.close(inspected_fd)
        if metadata.schema_version != 5:
            raise _managed_root.CutoverSafetyError(
                f"Market schema must be exactly 5, got {metadata.schema_version!r}"
            )
        if metadata.adjustment_mode != "provider_adjusted_v1":
            raise _managed_root.CutoverSafetyError(
                "Market adjustment mode must be provider_adjusted_v1"
            )
        return metadata

    @staticmethod
    def _is_iso_date(value: object) -> bool:
        if not isinstance(value, str):
            return False
        try:
            return date.fromisoformat(value).isoformat() == value
        except ValueError:
            return False

    @staticmethod
    def _is_ready_provider_vintage(payload: object) -> bool:
        if not isinstance(payload, dict):
            return False
        provider_plan = payload.get("providerPlan")
        provider_as_of = payload.get("providerAsOf")
        provider_as_of_range = payload.get("providerAsOfRange")
        effective_coverage = payload.get("effectiveCoverage")
        source_fingerprint = payload.get("sourceFingerprint")
        if (
            payload.get("status") != "ready"
            or payload.get("recoveryStage") is not None
            or not isinstance(provider_plan, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", provider_plan)
            or not isinstance(source_fingerprint, str)
            or not re.fullmatch(r"[0-9a-f]{64}", source_fingerprint)
            or not isinstance(provider_as_of_range, dict)
            or not isinstance(effective_coverage, dict)
        ):
            return False
        as_of_min = provider_as_of_range.get("min")
        as_of_max = provider_as_of_range.get("max")
        coverage_min = effective_coverage.get("min")
        coverage_max = effective_coverage.get("max")
        if not all(
            RuntimeSmokeService._is_iso_date(value)
            for value in (as_of_min, as_of_max, coverage_min, coverage_max)
        ):
            return False
        assert isinstance(as_of_min, str)
        assert isinstance(as_of_max, str)
        assert isinstance(coverage_min, str)
        assert isinstance(coverage_max, str)
        if (
            as_of_min > as_of_max
            or coverage_min > coverage_max
            or as_of_min < coverage_max
            or provider_as_of != (as_of_min if as_of_min == as_of_max else None)
        ):
            return False
        integer_fields = (
            "providerWindowCount",
            "readyProviderWindowCount",
            "providerWindowFingerprintCount",
            "invalidProviderWindowCount",
            "adjustmentEventCount",
            "adjustmentEventFingerprintCount",
            "invalidAdjustmentEventCount",
            "providerAdjustedMismatchCount",
            "currentBasisStatementCount",
            "currentBasisStateCount",
            "invalidCurrentBasisStateCount",
            "pendingCurrentBasisCodeCount",
            "sourceStatementKeyCount",
            "expectedAdjustedStatementRows",
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "orphanAdjustedStatementRows",
        )
        if any(type(payload.get(field)) is not int for field in integer_fields):
            return False
        provider_window_count = int(cast(int, payload["providerWindowCount"]))
        adjustment_event_count = int(cast(int, payload["adjustmentEventCount"]))
        zero_counters = (
            "invalidProviderWindowCount",
            "invalidAdjustmentEventCount",
            "providerAdjustedMismatchCount",
            "invalidCurrentBasisStateCount",
            "pendingCurrentBasisCodeCount",
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "orphanAdjustedStatementRows",
        )
        return bool(
            payload.get("providerWindowCoherent") is True
            and provider_window_count > 0
            and adjustment_event_count >= 0
            and payload["readyProviderWindowCount"] == provider_window_count
            and payload["providerWindowFingerprintCount"] == provider_window_count
            and payload["adjustmentEventFingerprintCount"] == adjustment_event_count
            and int(cast(int, payload["currentBasisStatementCount"])) > 0
            and int(cast(int, payload["currentBasisStateCount"])) > 0
            and int(cast(int, payload["sourceStatementKeyCount"])) > 0
            and int(cast(int, payload["expectedAdjustedStatementRows"])) > 0
            and RuntimeSmokeService._is_iso_date(
                payload.get("fundamentalsAdjustmentBasisDate")
            )
            and all(payload[counter] == 0 for counter in zero_counters)
        )

    @staticmethod
    def _validate_smoke_lineage(
        api: ApiAdapter,
    ) -> tuple[dict[str, object], dict[str, object], tuple[str, ...]]:
        stats = api.request("GET", "/api/db/stats")
        schema = stats.get("schema")
        if not isinstance(schema, dict) or schema != {
            "version": 5,
            "requiredVersion": 5,
            "current": True,
        }:
            raise _managed_root.CutoverSafetyError("Market stats schema v5 gate failed")
        stats_adjusted = stats.get("providerVintage")
        if not isinstance(
            stats_adjusted, dict
        ) or not RuntimeSmokeService._is_ready_provider_vintage(stats_adjusted):
            raise _managed_root.CutoverSafetyError(
                "Market provider-vintage coverage is not ready"
            )

        validation = api.request("GET", "/api/db/validate")
        if validation.get("status") != "healthy":
            raise _managed_root.CutoverSafetyError(
                "Market validation did not report healthy"
            )
        adjusted = validation.get("providerVintage")
        if not isinstance(adjusted, dict):
            raise _managed_root.CutoverSafetyError(
                "Validation omitted provider-vintage lineage"
            )
        zero_counters = (
            "invalidProviderWindowCount",
            "invalidAdjustmentEventCount",
            "providerAdjustedMismatchCount",
            "invalidCurrentBasisStateCount",
            "pendingCurrentBasisCodeCount",
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "orphanAdjustedStatementRows",
        )
        if (
            not RuntimeSmokeService._is_ready_provider_vintage(adjusted)
            or any(adjusted.get(counter) != 0 for counter in zero_counters)
        ):
            raise _managed_root.CutoverSafetyError(
                "Exact provider-vintage lineage validation failed"
            )
        return stats_adjusted, adjusted, zero_counters

    def _smoke_analytics(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
    ) -> tuple[str, str]:
        symbol = quote(config.symbol, safe="")
        get_fundamentals = api.request("GET", f"/api/analytics/fundamentals/{symbol}")
        post_fundamentals = api.request(
            "POST", "/api/fundamentals/compute", {"symbol": config.symbol}
        )
        semantic_keys = ("asOfDate", "data", "latestMetrics")
        if any(
            get_fundamentals.get(key) != post_fundamentals.get(key)
            for key in semantic_keys
        ):
            raise _managed_root.CutoverSafetyError(
                "Fundamentals GET/POST parity failed"
            )
        if not get_fundamentals.get("data"):
            raise _managed_root.CutoverSafetyError(
                "Fundamentals smoke returned no data"
            )

        screening = api.request(
            "POST",
            "/api/analytics/screening/jobs",
            {
                "strategies": config.strategy,
                "recentDays": 10,
                "sortBy": "matchedDate",
                "order": "desc",
            },
        )
        screening_job_id = self._require_job_id(
            screening,
            "/api/analytics/screening/jobs",
        )
        self._poll_api_job(
            api,
            f"/api/analytics/screening/jobs/{quote(screening_job_id, safe='')}",
            "screening",
        )
        screening_result = api.request(
            "GET",
            f"/api/analytics/screening/result/{quote(screening_job_id, safe='')}",
        )
        if not isinstance(screening_result.get("results"), list):
            raise _managed_root.CutoverSafetyError(
                "Screening result payload is invalid"
            )

        ranking = api.request("GET", "/api/analytics/fundamental-ranking")
        if not isinstance(ranking.get("rankings"), dict):
            raise _managed_root.CutoverSafetyError(
                "Fundamental ranking payload is invalid"
            )
        return symbol, screening_job_id

    def _smoke_dataset(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        *,
        operation_id: str,
        provider_vintage: dict[str, object],
    ) -> tuple[str, str]:
        dataset_name = f"cutover-smoke-{operation_id.replace('.', '-')}"
        dataset = api.request(
            "POST",
            "/api/dataset",
            {
                "name": dataset_name,
                "preset": config.dataset_preset,
                "overwrite": False,
            },
        )
        dataset_job_id = self._require_job_id(dataset, "/api/dataset")
        self._poll_api_job(
            api,
            f"/api/dataset/jobs/{quote(dataset_job_id, safe='')}",
            "dataset",
        )
        dataset_info = api.request("GET", f"/api/dataset/{dataset_name}/info")
        snapshot = dataset_info.get("snapshot")
        dataset_validation = dataset_info.get("validation")
        effective_coverage = provider_vintage.get("effectiveCoverage")
        expected_snapshot = {
            "schemaVersion": 4,
            "sourceMarketSchemaVersion": 5,
            "stockPriceAdjustmentMode": "provider_adjusted_v1",
            "providerPlan": provider_vintage.get("providerPlan"),
            "providerAsOf": provider_vintage.get("providerAsOf"),
            "providerCoverageStart": (
                effective_coverage.get("min")
                if isinstance(effective_coverage, dict)
                else None
            ),
            "providerCoverageEnd": (
                effective_coverage.get("max")
                if isinstance(effective_coverage, dict)
                else None
            ),
            "providerSourceFingerprint": provider_vintage.get("sourceFingerprint"),
            "fundamentalsAdjustmentBasisDate": provider_vintage.get(
                "fundamentalsAdjustmentBasisDate"
            ),
        }
        if not isinstance(snapshot, dict) or any(
            snapshot.get(key) != value for key, value in expected_snapshot.items()
        ):
            raise _managed_root.CutoverSafetyError(
                "Dataset provider-vintage gate failed"
            )
        if (
            not isinstance(dataset_validation, dict)
            or dataset_validation.get("isValid") is not True
        ):
            raise _managed_root.CutoverSafetyError("Dataset validation failed")
        opened = api.request("GET", f"/api/dataset/{dataset_name}/sample?count=1")
        if not isinstance(opened.get("codes"), list) or not opened["codes"]:
            raise _managed_root.CutoverSafetyError(
                "Dataset sample smoke returned no codes"
            )
        return dataset_name, dataset_job_id

    @staticmethod
    def _require_job_id(payload: dict[str, object], endpoint: str) -> str:
        try:
            label, job_id_field = _CREATE_JOB_RESPONSE_FIELDS[endpoint]
        except KeyError as exc:
            raise _managed_root.CutoverSafetyError(
                f"Unsupported job-creating endpoint: {endpoint}"
            ) from exc
        job_id = payload.get(job_id_field)
        if not isinstance(job_id, str) or not job_id:
            raise _managed_root.CutoverSafetyError(f"{label} did not return a job ID")
        return job_id

    @staticmethod
    def _poll_api_job(
        api: ApiAdapter,
        path: str,
        label: str,
        *,
        attempts: int = 21_600,
        poll_interval_seconds: float = 2.0,
    ) -> dict[str, object]:
        for _ in range(attempts):
            job = api.request("GET", path)
            status = job.get("status")
            if status == "completed":
                return job
            if status in {"failed", "cancelled"}:
                progress = job.get("progress")
                result = job.get("result")
                details: list[str] = []
                if isinstance(progress, dict):
                    for key in ("stage", "message"):
                        value = progress.get(key)
                        if isinstance(value, str) and value:
                            details.append(f"{key}={value}")
                if isinstance(result, dict):
                    errors = result.get("errors")
                    if isinstance(errors, list):
                        result_errors = [
                            value
                            for value in errors
                            if isinstance(value, str) and value
                        ]
                        if result_errors:
                            details.append(f"errors={' | '.join(result_errors)}")
                error = job.get("error")
                if isinstance(error, str) and error:
                    details.append(f"error={error}")
                suffix = f"; {'; '.join(details)}" if details else ""
                raise _managed_root.CutoverSafetyError(
                    f"{label} job ended with status {status}{suffix}"
                )
            time.sleep(poll_interval_seconds)
        raise _managed_root.CutoverSafetyError(f"{label} job polling timed out")
