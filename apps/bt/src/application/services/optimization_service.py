"""
Optimization Service

ParameterOptimizationEngineの非同期ラッパー（Grid Search）
"""

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from loguru import logger

from src.domains.backtest.contracts import EnginePolicy
from src.domains.optimization.grid_validation import (
    format_grid_validation_issues,
)
from src.domains.optimization.strategy_spec import analyze_saved_strategy_optimization
from src.domains.strategy.runtime.loader import ConfigLoader
from src.entrypoints.http.schemas.backtest import JobStatus
from src.application.services.job_manager import JobManager, job_manager
from src.application.services.run_contracts import build_strategy_run_spec
from src.shared.config.settings import get_settings

_WORKER_MODULE = "src.application.workers.optimization_worker"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class OptimizationService:
    """最適化実行サービス"""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 1,
        worker_poll_interval_seconds: float = 0.5,
        worker_timeout_seconds: int | None = None,
    ) -> None:
        self._manager = manager or job_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._config_loader = ConfigLoader()
        self._worker_poll_interval_seconds = max(worker_poll_interval_seconds, 0.1)
        self._worker_timeout_seconds = (
            worker_timeout_seconds
            if worker_timeout_seconds is not None
            else get_settings().optimization_job_timeout_seconds
        )

    # ============================================
    # Grid Search Optimization
    # ============================================

    async def submit_optimization(
        self,
        strategy_name: str,
        engine_policy: EnginePolicy | None = None,
    ) -> str:
        """
        グリッドサーチ最適化をサブミット

        Args:
            strategy_name: 戦略名

        Returns:
            ジョブID
        """
        self._validate_grid_ready(strategy_name)
        resolved_engine_policy = engine_policy or EnginePolicy()
        run_spec = build_strategy_run_spec(
            "optimization",
            strategy_name,
            parameters={
                "optimization_mode": "grid_search",
                "engine_policy": resolved_engine_policy.model_dump(mode="json"),
            },
            config_loader=self._config_loader,
        )
        job_id = self._manager.create_job(
            strategy_name,
            job_type="optimization",
            run_spec=run_spec,
        )

        task = asyncio.create_task(
            self._run_optimization(
                job_id,
                strategy_name,
                engine_policy=resolved_engine_policy,
            )
        )
        await self._manager.set_job_task(job_id, task)

        return job_id

    def _validate_grid_ready(self, strategy_name: str) -> None:
        strategy_config = self._config_loader.load_strategy_config(strategy_name)
        analysis = analyze_saved_strategy_optimization(strategy_config)
        if analysis.optimization is None:
            raise ValueError(
                "Strategy optimization spec is missing. Save an optimization block on the strategy first."
            )
        if not analysis.valid:
            raise ValueError(
                "Strategy optimization validation failed: "
                f"{format_grid_validation_issues(analysis.errors)}"
            )
        if not analysis.ready_to_run:
            warning_text = (
                format_grid_validation_issues(analysis.warnings)
                if analysis.warnings
                else "no parameter candidate lists were found"
            )
            raise ValueError(f"Strategy optimization is not ready to run: {warning_text}")

    async def _run_optimization(
        self,
        job_id: str,
        strategy_name: str,
        *,
        engine_policy: EnginePolicy | None = None,
    ) -> None:
        """グリッドサーチ最適化を実行（バックグラウンド）"""
        process: asyncio.subprocess.Process | None = None
        try:
            await self._manager.acquire_slot()

            await self._manager.update_job_status(
                job_id,
                JobStatus.PENDING,
                message="最適化 worker を起動しています...",
                progress=0.0,
            )

            logger.info(f"最適化開始: {job_id} (戦略: {strategy_name})")

            if engine_policy is None:
                process = await self._start_worker_process(job_id, strategy_name)
            else:
                process = await self._start_worker_process(
                    job_id,
                    strategy_name,
                    engine_policy=engine_policy,
                )
            exit_code = await self._wait_for_worker_completion(job_id, process)
            job = await self._manager.reload_job_from_storage(job_id, notify=True)
            if job is None or job.status in (
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
            ):
                return
            if exit_code == 0:
                await self._manager.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    message="最適化 worker が結果を保存せず終了しました",
                    error="worker_exited_without_terminal_state",
                )
                return
            if self._manager.is_cancel_requested(job_id):
                logger.info(f"最適化 worker を停止しました: {job_id}")
                return
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="最適化 worker が異常終了しました",
                error=f"worker_exit_code={exit_code}",
            )

        except asyncio.CancelledError:
            logger.info(f"最適化がキャンセルされました: {job_id}")
            if process is not None:
                await self._terminate_worker_process(process)
            await self._manager.reload_job_from_storage(job_id, notify=True)
            if not self._manager.is_cancel_requested(job_id):
                await self._manager.update_job_status(
                    job_id,
                    JobStatus.CANCELLED,
                    message="最適化がキャンセルされました",
                )

        except Exception as e:
            logger.exception(f"最適化エラー: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="最適化に失敗しました",
                error=str(e),
            )

        finally:
            self._manager.release_slot()

    async def _start_worker_process(
        self,
        job_id: str,
        strategy_name: str,
        *,
        engine_policy: EnginePolicy | None = None,
    ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            *self._build_worker_command(
                job_id,
                strategy_name,
                engine_policy=engine_policy,
            ),
            cwd=str(_PROJECT_ROOT),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

    def _build_worker_command(
        self,
        job_id: str,
        strategy_name: str,
        *,
        engine_policy: EnginePolicy | None = None,
    ) -> list[str]:
        resolved_engine_policy = engine_policy or EnginePolicy()
        return [
            sys.executable,
            "-m",
            _WORKER_MODULE,
            "--job-id",
            job_id,
            "--strategy-name",
            strategy_name,
            "--engine-policy-json",
            resolved_engine_policy.model_dump_json(),
            "--timeout-seconds",
            str(self._worker_timeout_seconds),
        ]

    async def _wait_for_worker_completion(
        self,
        job_id: str,
        process: asyncio.subprocess.Process,
    ) -> int:
        while True:
            try:
                exit_code = await asyncio.wait_for(
                    process.wait(),
                    timeout=self._worker_poll_interval_seconds,
                )
                await self._manager.reload_job_from_storage(job_id, notify=True)
                return exit_code
            except asyncio.TimeoutError:
                await self._manager.reload_job_from_storage(job_id, notify=True)

    async def _terminate_worker_process(
        self,
        process: asyncio.subprocess.Process,
        *,
        timeout_seconds: float = 3.0,
    ) -> None:
        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    def _execute_optimization_sync(self, strategy_name: str) -> dict[str, Any]:
        """
        同期的にグリッドサーチ最適化を実行

        Returns:
            結果辞書
        """
        from src.domains.optimization.engine import ParameterOptimizationEngine

        engine = ParameterOptimizationEngine(strategy_name=strategy_name)
        opt_result = engine.optimize()

        best_score = opt_result.best_score
        best_params = opt_result.best_params
        total_combinations = len(opt_result.all_results) if opt_result.all_results else 0
        worst_result = opt_result.all_results[-1] if opt_result.all_results else None
        worst_score = worst_result.get("score") if worst_result else None
        worst_params = worst_result.get("params") if worst_result else None
        html_path = opt_result.html_path if opt_result.html_path else None

        return {
            "best_score": best_score,
            "best_params": best_params,
            "worst_score": worst_score,
            "worst_params": worst_params,
            "total_combinations": total_combinations,
            "html_path": html_path,
        }


# グローバルインスタンス
optimization_service = OptimizationService()
