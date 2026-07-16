"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import os
from pathlib import Path
import time
from urllib.parse import quote

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    ApiAdapter,
    SmokeConfig,
)
from .project_paths import repository_default_config_path


class RebuildMixin:
    def _prepare_isolated_root(self, root: Path, *, runtime_name: str) -> None:
        self._prepare_managed_directory(root, exist_ok=False)
        for relative in (
            "market-timeseries",
            "datasets",
            "backtest",
            "config",
        ):
            self._prepare_managed_directory(root / relative, exist_ok=False)
        source_config = self.data_root / "config" / "default.yaml"
        if not source_config.is_file():
            source_config = repository_default_config_path()
        if not source_config.is_file():
            raise _managed_root.CutoverSafetyError(
                "Repository default configuration is missing"
            )
        config_target = root / "config" / "default.yaml"
        self._assert_managed_target_absent(config_target)
        self._copy_regular_to_managed(source_config, config_target)
        try:
            active_strategies_fd = self._managed().open_dir(Path("strategies"))
        except FileNotFoundError:
            self._prepare_managed_directory(root / "strategies", exist_ok=False)
        else:
            os.close(active_strategies_fd)
            self._managed().copy_tree_create(
                Path("strategies"),
                self._managed_relative(root / "strategies"),
            )
        runtime_root = root / "market-timeseries" / runtime_name
        self._prepare_managed_directory(runtime_root, exist_ok=False)
        for relative in ("datasets", "backtest", "config"):
            self._prepare_managed_directory(runtime_root / relative, exist_ok=False)
        runtime_config = runtime_root / "config" / "default.yaml"
        self._assert_managed_target_absent(runtime_config)
        self._copy_regular_to_managed(config_target, runtime_config)
        self._managed().copy_tree_create(
            self._managed_relative(root / "strategies"),
            self._managed_relative(runtime_root / "strategies"),
        )

    @staticmethod
    def _isolated_environment(
        inherited: dict[str, str],
        *,
        lease_fd: int,
        root_fd: int,
        runtime_name: str,
    ) -> dict[str, str]:
        environment = dict(inherited)
        environment.pop("TRADING25_RUNTIME_CAPABILITY", None)
        overrides = {
            "XDG_DATA_HOME": f"{runtime_name}/xdg-data-home",
            "TRADING25_DATA_DIR": runtime_name,
            "MARKET_TIMESERIES_DIR": ".",
            "MARKET_DB_PATH": "market.duckdb",
            "TRADING25_DUCKDB_TEMP_DIR": f"{runtime_name}/duckdb-tmp",
            "DATASET_BASE_PATH": f"{runtime_name}/datasets",
            "PORTFOLIO_DB_PATH": f"{runtime_name}/portfolio.db",
            "TRADING25_STRATEGIES_DIR": f"{runtime_name}/strategies",
            "TRADING25_BACKTEST_DIR": f"{runtime_name}/backtest",
            "TRADING25_DEFAULT_CONFIG_PATH": f"{runtime_name}/config/default.yaml",
            "TRADING25_MARKET_OPERATION_LOCK_FD": str(lease_fd),
            "TRADING25_DATA_ROOT_FD": str(root_fd),
        }
        environment.update(overrides)
        return environment

    def _run_rebuild(
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
                "adjustedMetrics": result.lineage,
            },
            (
                {
                    "name": "initial_sync_and_adjusted_metrics_pit",
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
