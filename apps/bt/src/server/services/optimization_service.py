"""
Optimization Service

ParameterOptimizationEngineの非同期ラッパー（Grid Search）
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from loguru import logger

from src.server.schemas.backtest import JobStatus
from src.server.services.job_manager import JobManager, job_manager


class OptimizationService:
    """最適化実行サービス"""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 1,
    ) -> None:
        self._manager = manager or job_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    # ============================================
    # Grid Search Optimization
    # ============================================

    async def submit_optimization(self, strategy_name: str) -> str:
        """
        グリッドサーチ最適化をサブミット

        Args:
            strategy_name: 戦略名

        Returns:
            ジョブID
        """
        job_id = self._manager.create_job(strategy_name, job_type="optimization")

        task = asyncio.create_task(self._run_optimization(job_id, strategy_name))
        await self._manager.set_job_task(job_id, task)

        return job_id

    async def _run_optimization(self, job_id: str, strategy_name: str) -> None:
        """グリッドサーチ最適化を実行（バックグラウンド）"""
        try:
            await self._manager.acquire_slot()

            await self._manager.update_job_status(
                job_id,
                JobStatus.RUNNING,
                message="最適化を開始しています...",
                progress=0.0,
            )

            logger.info(f"最適化開始: {job_id} (戦略: {strategy_name})")

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._executor,
                self._execute_optimization_sync,
                strategy_name,
            )

            # 結果をJobInfoに格納
            job = self._manager.get_job(job_id)
            if job is not None:
                job.best_score = result.get("best_score")
                job.total_combinations = result.get("total_combinations")
                job.notebook_path = result.get("notebook_path")

            await self._manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                message="最適化完了",
                progress=1.0,
            )

            logger.info(f"最適化完了: {job_id}")

        except asyncio.CancelledError:
            logger.info(f"最適化がキャンセルされました: {job_id}")
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

    def _execute_optimization_sync(self, strategy_name: str) -> dict[str, Any]:
        """
        同期的にグリッドサーチ最適化を実行

        Returns:
            結果辞書 (best_score, total_combinations, notebook_path)
        """
        from src.optimization.engine import ParameterOptimizationEngine

        engine = ParameterOptimizationEngine(strategy_name=strategy_name)
        opt_result = engine.optimize()

        best_score = opt_result.best_score
        total_combinations = len(opt_result.all_results) if opt_result.all_results else 0
        notebook_path = opt_result.notebook_path if opt_result.notebook_path else None

        return {
            "best_score": best_score,
            "total_combinations": total_combinations,
            "notebook_path": notebook_path,
        }


# グローバルインスタンス
optimization_service = OptimizationService()
