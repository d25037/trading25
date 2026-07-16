"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
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
from .project_paths import repository_default_config_path

_CREATE_JOB_RESPONSE_FIELDS: dict[str, tuple[str, str]] = {
    "/api/db/sync": ("sync", "jobId"),
    "/api/db/adjusted-metrics/materialize": ("materialize", "jobId"),
    "/api/analytics/screening/jobs": ("screening", "job_id"),
    "/api/dataset": ("dataset", "jobId"),
}


class SmokeMixin:
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
                self.data_root,
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
        operation_id = self._validate_id(operation_id, label="operation")
        inspected_root = market_root or self.market_root
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
                **{
                    key: int(cast(int | str, adjusted[key]))
                    for key in (
                        "sourceStatementKeyCount",
                        "expectedAdjustedStatementRows",
                        *zero_counters,
                    )
                },
                "statementRows": int(cast(int | str, stats_adjusted["statementRows"])),
                "dailyValuationRows": int(
                    cast(int | str, stats_adjusted["dailyValuationRows"])
                ),
                "readyBasisCount": int(
                    cast(int | str, stats_adjusted["readyBasisCount"])
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
                metadata = self.duckdb.inspect(
                    inspected_fd,
                    "market.duckdb",
                    guard_lease_fd=guard_lease_fd,
                )
            finally:
                os.close(inspected_fd)
        else:
            with self._managed_root_scope():
                inspected_fd = self._managed().open_dir(
                    self._managed_relative(inspected_root)
                )
                try:
                    metadata = self.duckdb.inspect(
                        inspected_fd,
                        "market.duckdb",
                        guard_lease_fd=guard_lease_fd,
                    )
                finally:
                    os.close(inspected_fd)
        if metadata.schema_version != 4:
            raise _managed_root.CutoverSafetyError(
                f"Market schema must be exactly 4, got {metadata.schema_version!r}"
            )
        if metadata.adjustment_mode != "local_projection_v2_event_time":
            raise _managed_root.CutoverSafetyError(
                "Market adjustment mode must be local_projection_v2_event_time"
            )
        return metadata

    @staticmethod
    def _validate_smoke_lineage(
        api: ApiAdapter,
    ) -> tuple[dict[str, object], dict[str, object], tuple[str, ...]]:
        stats = api.request("GET", "/api/db/stats")
        schema = stats.get("schema")
        if not isinstance(schema, dict) or schema != {
            "version": 4,
            "requiredVersion": 4,
            "current": True,
        }:
            raise _managed_root.CutoverSafetyError("Market stats schema v4 gate failed")
        stats_adjusted = stats.get("adjustedMetrics")
        if (
            not isinstance(stats_adjusted, dict)
            or stats_adjusted.get("status") != "ready"
            or not isinstance(stats_adjusted.get("statementRows"), int)
            or int(stats_adjusted["statementRows"]) <= 0
            or not isinstance(stats_adjusted.get("dailyValuationRows"), int)
            or int(stats_adjusted["dailyValuationRows"]) <= 0
            or not isinstance(stats_adjusted.get("readyBasisCount"), int)
            or int(stats_adjusted["readyBasisCount"]) <= 0
        ):
            raise _managed_root.CutoverSafetyError(
                "Market adjusted-metric coverage is not ready"
            )

        validation = api.request("GET", "/api/db/validate")
        if validation.get("status") != "healthy":
            raise _managed_root.CutoverSafetyError(
                "Market validation did not report healthy"
            )
        adjusted = validation.get("adjustedMetrics")
        if not isinstance(adjusted, dict):
            raise _managed_root.CutoverSafetyError(
                "Validation omitted adjusted-metric lineage"
            )
        zero_counters = (
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "missingDailyValuationRows",
            "extraDailyValuationRows",
            "wrongBasisDailyValuationRows",
        )
        if (
            adjusted.get("status") != "ready"
            or not isinstance(adjusted.get("sourceStatementKeyCount"), int)
            or int(adjusted["sourceStatementKeyCount"]) <= 0
            or not isinstance(adjusted.get("expectedAdjustedStatementRows"), int)
            or int(adjusted["expectedAdjustedStatementRows"]) <= 0
            or any(adjusted.get(counter) != 0 for counter in zero_counters)
        ):
            raise _managed_root.CutoverSafetyError(
                "Exact adjusted-metric lineage validation failed"
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
        if not isinstance(snapshot, dict) or snapshot != {
            **snapshot,
            "schemaVersion": 3,
            "sourceMarketSchemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        }:
            raise _managed_root.CutoverSafetyError(
                "Dataset event-time lineage gate failed"
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

    def configuration_fingerprint(self, root: Path) -> str:
        root = _managed_root.lexical_absolute(root)
        if self._managed_root_fd is not None:
            try:
                root_relative = root.relative_to(self.data_root)
            except ValueError:
                pass
            else:
                digest = hashlib.sha256()
                config_relative = root_relative / "config" / "default.yaml"
                try:
                    config_stat = self._managed().stat(config_relative)
                except FileNotFoundError:
                    repository_config = self._repository_default_config_path()
                    config_sha = self._sha256(repository_config)
                else:
                    if not stat.S_ISREG(config_stat.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Fingerprint config is not regular"
                        )
                    config_sha = self._managed().sha256(config_relative)
                digest.update(b"config/default.yaml\0")
                digest.update(config_sha.encode())
                digest.update(b"\n")
                strategies_relative = root_relative / "strategies"
                try:
                    strategy_files = self._managed().regular_files(strategies_relative)
                except FileNotFoundError:
                    strategy_files = []
                for relative, _entry_stat in strategy_files:
                    label = f"strategies/{relative.as_posix()}"
                    digest.update(label.encode())
                    digest.update(b"\0")
                    digest.update(
                        self._managed().sha256(strategies_relative / relative).encode()
                    )
                    digest.update(b"\n")
                return digest.hexdigest()
        _managed_root.assert_real_directory(root, "Fingerprint root")
        _managed_root.assert_safe_directory_chain(root)
        digest = hashlib.sha256()
        candidates: list[tuple[str, Path]] = []
        config = root / "config" / "default.yaml"
        if not config.is_file():
            config = self._repository_default_config_path()
        candidates.append(("config/default.yaml", config))
        strategies = root / "strategies"
        if strategies.exists():
            _managed_root.assert_real_directory(strategies, "Strategies root")
            for path in sorted(strategies.rglob("*")):
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise _managed_root.CutoverSafetyError(
                        "Strategy fingerprint source contains symlink"
                    )
                if stat.S_ISDIR(mode):
                    continue
                if not stat.S_ISREG(mode):
                    raise _managed_root.CutoverSafetyError(
                        "Strategy fingerprint source contains special file"
                    )
                candidates.append(
                    (f"strategies/{path.relative_to(strategies).as_posix()}", path)
                )
        for label, path in candidates:
            if path.is_symlink() or not path.is_file():
                raise _managed_root.CutoverSafetyError(
                    f"Fingerprint source is invalid: {label}"
                )
            digest.update(label.encode())
            digest.update(b"\0")
            digest.update(self._sha256(path).encode())
            digest.update(b"\n")
        return digest.hexdigest()

    @staticmethod
    def _repository_default_config_path() -> Path:
        config = repository_default_config_path()
        try:
            config_stat = config.lstat()
        except FileNotFoundError as exc:
            raise _managed_root.CutoverSafetyError(
                "Repository default configuration is missing"
            ) from exc
        if stat.S_ISLNK(config_stat.st_mode) or not stat.S_ISREG(config_stat.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Repository default configuration must be a regular file"
            )
        return config

    def root_fingerprint(self, root: Path) -> str:
        root = _managed_root.lexical_absolute(root)
        if self._managed_root_fd is not None:
            try:
                relative = root.relative_to(self.data_root)
            except ValueError:
                relative = None
            if relative is not None:
                root_fd = (
                    os.dup(self._managed().fd)
                    if not relative.parts
                    else self._managed().open_dir(relative)
                )
                try:
                    root_stat = os.fstat(root_fd)
                finally:
                    os.close(root_fd)
            else:
                _managed_root.assert_safe_directory_chain(root)
                _managed_root.assert_real_directory(root, "Fingerprint root")
                root_stat = root.lstat()
        else:
            _managed_root.assert_safe_directory_chain(root)
            _managed_root.assert_real_directory(root, "Fingerprint root")
            root_stat = root.lstat()
        digest = hashlib.sha256(
            f"dev={root_stat.st_dev};ino={root_stat.st_ino}\n".encode()
        )
        digest.update(self.configuration_fingerprint(root).encode())
        digest.update(b"\n")
        return digest.hexdigest()
