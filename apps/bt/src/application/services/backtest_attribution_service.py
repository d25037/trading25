"""
Backtest Signal Attribution Service.

Runs signal attribution as an async background job.
"""

import asyncio
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.shared.config.settings import get_settings
from src.domains.backtest.core.runner import BacktestRunner
from src.domains.backtest.core.signal_attribution import (
    SignalAttributionAnalyzer,
    SignalAttributionCancelled,
)
from src.shared.paths import find_strategy_path, get_backtest_attribution_dir
from src.entrypoints.http.schemas.backtest import JobStatus
from src.application.services.job_manager import JobManager, job_manager

_SAFE_PATH_SEGMENT = re.compile(r"[^A-Za-z0-9._-]+")


class BacktestAttributionService:
    """Signal attribution execution service."""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 1,
    ) -> None:
        self._manager = manager or job_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._runner = BacktestRunner()

    @staticmethod
    def _sanitize_strategy_path(strategy_name: str) -> Path:
        """Convert strategy name into safe relative path segments."""
        parts = [part for part in strategy_name.split("/") if part not in ("", ".", "..")]
        safe_parts: list[str] = []
        for part in parts:
            normalized = _SAFE_PATH_SEGMENT.sub("_", part).strip("._")
            safe_parts.append(normalized or "unknown")

        if not safe_parts:
            safe_parts.append("unknown")
        return Path(*safe_parts)

    @staticmethod
    def _db_entry(path_value: str) -> dict[str, str]:
        if not path_value:
            return {"name": "", "path": ""}
        path = Path(path_value)
        return {"name": path.name, "path": str(path)}

    def _build_persistence_payload(
        self,
        *,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        settings = get_settings()
        now = datetime.now(UTC)
        strategy_path = find_strategy_path(strategy_name)

        strategy_yaml: dict[str, Any] | None = None
        try:
            strategy_yaml = self._runner.config_loader.load_strategy_config(strategy_name)
        except Exception as e:
            logger.warning(f"戦略YAMLの読み込みに失敗（保存メタのみ継続）: {e}")

        effective_parameters: dict[str, Any] | None = None
        try:
            effective_parameters = self._runner.build_parameters_for_strategy(
                strategy=strategy_name,
                config_override=config_override,
            )
        except Exception as e:
            logger.warning(f"戦略パラメータ構築に失敗（保存メタのみ継続）: {e}")

        shared_config = (
            effective_parameters.get("shared_config", {})
            if isinstance(effective_parameters, dict)
            else {}
        )
        dataset_name = (
            shared_config.get("dataset", "")
            if isinstance(shared_config, dict)
            else ""
        )

        job = self._manager.get_job(job_id)
        created_at = getattr(job, "created_at", None) if job is not None else None
        started_at = getattr(job, "started_at", None) if job is not None else None
        job_status = getattr(job, "status", None) if job is not None else None
        status_raw = getattr(job_status, "value", job_status)
        status_value = str(status_raw) if status_raw else ""

        return {
            "saved_at": now.isoformat(),
            "job": {
                "job_id": job_id,
                "created_at": created_at.isoformat() if created_at else None,
                "started_at": started_at.isoformat() if started_at else None,
                "saved_status": status_value or None,
            },
            "strategy": {
                "name": strategy_name,
                "yaml_path": str(strategy_path) if strategy_path else None,
                "yaml": strategy_yaml,
                "effective_parameters": effective_parameters,
                "config_override": config_override,
            },
            "runtime": {
                "shapley_top_n": shapley_top_n,
                "shapley_permutations": shapley_permutations,
                "random_seed": random_seed,
            },
            "databases": {
                "market_db": self._db_entry(settings.market_db_path),
                "portfolio_db": self._db_entry(settings.portfolio_db_path),
                "dataset_base_dir": self._db_entry(settings.dataset_base_path),
                "dataset_name": dataset_name,
            },
            "result": result,
        }

    def _persist_attribution_artifact(
        self,
        *,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
        result: dict[str, Any],
    ) -> Path:
        now = datetime.now(UTC)
        strategy_rel_path = self._sanitize_strategy_path(strategy_name)
        output_dir = get_backtest_attribution_dir() / strategy_rel_path
        output_dir.mkdir(parents=True, exist_ok=True)

        payload = self._build_persistence_payload(
            job_id=job_id,
            strategy_name=strategy_name,
            config_override=config_override,
            shapley_top_n=shapley_top_n,
            shapley_permutations=shapley_permutations,
            random_seed=random_seed,
            result=result,
        )

        filename = f"attribution_{now.strftime('%Y%m%d_%H%M%S')}_{job_id}.json"
        output_path = output_dir / filename
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return output_path

    async def submit_attribution(
        self,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
        shapley_top_n: int = 5,
        shapley_permutations: int = 128,
        random_seed: int | None = None,
    ) -> str:
        """Submit a signal attribution job."""
        job_id = self._manager.create_job(
            strategy_name=strategy_name,
            job_type="backtest_attribution",
        )
        cancel_event = threading.Event()

        task = asyncio.create_task(
            self._run_attribution(
                job_id=job_id,
                strategy_name=strategy_name,
                config_override=config_override,
                shapley_top_n=shapley_top_n,
                shapley_permutations=shapley_permutations,
                random_seed=random_seed,
                cancel_event=cancel_event,
            )
        )
        await self._manager.set_job_task(job_id, task)
        return job_id

    async def _run_attribution(
        self,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        run_cancel_event = cancel_event or threading.Event()
        try:
            await self._manager.acquire_slot()
            await self._manager.update_job_status(
                job_id,
                JobStatus.RUNNING,
                message="シグナル寄与分析を開始しています...",
                progress=0.0,
            )

            logger.info(f"シグナル寄与分析開始: {job_id} (戦略: {strategy_name})")
            loop = asyncio.get_running_loop()

            result = await loop.run_in_executor(
                self._executor,
                self._execute_attribution_sync,
                job_id,
                strategy_name,
                loop,
                config_override,
                shapley_top_n,
                shapley_permutations,
                random_seed,
                run_cancel_event,
            )

            job = self._manager.get_job(job_id)
            if job is not None:
                job.raw_result = result

            try:
                artifact_path = self._persist_attribution_artifact(
                    job_id=job_id,
                    strategy_name=strategy_name,
                    config_override=config_override,
                    shapley_top_n=shapley_top_n,
                    shapley_permutations=shapley_permutations,
                    random_seed=random_seed,
                    result=result,
                )
                logger.info(f"シグナル寄与分析結果を保存: {artifact_path}")
            except Exception as e:
                logger.warning(f"シグナル寄与分析結果の保存に失敗: {e}")

            await self._manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                message="シグナル寄与分析完了",
                progress=1.0,
            )
            logger.info(f"シグナル寄与分析完了: {job_id}")
        except asyncio.CancelledError:
            run_cancel_event.set()
            logger.info(f"シグナル寄与分析がキャンセルされました: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                message="シグナル寄与分析がキャンセルされました",
            )
        except SignalAttributionCancelled:
            run_cancel_event.set()
            logger.info(f"シグナル寄与分析がキャンセルされました: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                message="シグナル寄与分析がキャンセルされました",
            )
        except Exception as e:
            logger.exception(f"シグナル寄与分析エラー: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="シグナル寄与分析に失敗しました",
                error=str(e),
            )
        finally:
            self._manager.release_slot()

    def _execute_attribution_sync(
        self,
        job_id: str,
        strategy_name: str,
        loop: asyncio.AbstractEventLoop,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        run_cancel_event = cancel_event or threading.Event()

        def progress_callback(message: str, progress: float) -> None:
            if run_cancel_event.is_set():
                return
            asyncio.run_coroutine_threadsafe(
                self._manager.update_job_status(
                    job_id,
                    JobStatus.RUNNING,
                    message=message,
                    progress=progress,
                ),
                loop,
            )

        analyzer = SignalAttributionAnalyzer(
            strategy_name=strategy_name,
            config_override=config_override,
            shapley_top_n=shapley_top_n,
            shapley_permutations=shapley_permutations,
            random_seed=random_seed,
            cancel_check=run_cancel_event.is_set,
        )
        return analyzer.run(progress_callback=progress_callback)


backtest_attribution_service = BacktestAttributionService()
