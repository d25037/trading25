"""
Backtest Service

BacktestRunnerの非同期ラッパー
"""

import asyncio
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from loguru import logger

from src.domains.backtest.core.runner import BacktestResult, BacktestRunner
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.application.services.backtest_result_summary import resolve_backtest_result_summary
from src.application.services.job_manager import JobManager, job_manager
from src.application.services.run_contracts import build_strategy_run_spec, normalize_config_override
from src.shared.config.settings import get_settings

_WORKER_MODULE = "src.application.workers.backtest_worker"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class BacktestService:
    """バックテスト実行サービス"""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 2,
        worker_poll_interval_seconds: float = 0.5,
        worker_timeout_seconds: int | None = None,
    ) -> None:
        """
        初期化

        Args:
            manager: ジョブマネージャー（省略時はグローバルインスタンス使用）
            max_workers: スレッドプールのワーカー数
        """
        self._manager = manager or job_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._runner = BacktestRunner()
        self._worker_poll_interval_seconds = max(worker_poll_interval_seconds, 0.1)
        self._worker_timeout_seconds = (
            worker_timeout_seconds
            if worker_timeout_seconds is not None
            else get_settings().backtest_job_timeout_seconds
        )

    async def submit_backtest(
        self,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
    ) -> str:
        """
        バックテストをサブミット

        Args:
            strategy_name: 戦略名
            config_override: 設定オーバーライド（shared_config/entry_filter_params/exit_trigger_paramsのdeep merge）

        Returns:
            ジョブID
        """
        normalized_config_override = normalize_config_override(config_override)
        run_spec = build_strategy_run_spec(
            "backtest",
            strategy_name,
            config_override=normalized_config_override,
            config_loader=self._runner.config_loader,
        )
        job_id = self._manager.create_job(strategy_name, run_spec=run_spec)

        # バックグラウンドタスクとして実行
        task = asyncio.create_task(
            self._run_backtest(job_id, strategy_name, normalized_config_override)
        )
        await self._manager.set_job_task(job_id, task)

        return job_id

    async def _run_backtest(
        self,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
    ) -> None:
        """
        バックテストを実行（バックグラウンド）

        Args:
            job_id: ジョブID
            strategy_name: 戦略名
        """
        process: asyncio.subprocess.Process | None = None
        try:
            # スロット取得（同時実行数制限）
            await self._manager.acquire_slot()
            await self._manager.update_job_status(
                job_id,
                JobStatus.PENDING,
                message="バックテスト worker を起動しています...",
                progress=0.0,
            )

            logger.info(f"バックテスト開始: {job_id} (戦略: {strategy_name})")

            process = await self._start_worker_process(
                job_id,
                strategy_name,
                config_override,
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
                    message="バックテスト worker が結果を保存せず終了しました",
                    error="worker_exited_without_terminal_state",
                )
                return
            if self._manager.is_cancel_requested(job_id):
                logger.info(f"バックテスト worker を停止しました: {job_id}")
                return
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="バックテスト worker が異常終了しました",
                error=f"worker_exit_code={exit_code}",
            )

        except asyncio.CancelledError:
            logger.info(f"バックテストがキャンセルされました: {job_id}")
            if process is not None:
                await self._terminate_worker_process(process)
            await self._manager.reload_job_from_storage(job_id, notify=True)
            if not self._manager.is_cancel_requested(job_id):
                await self._manager.update_job_status(
                    job_id,
                    JobStatus.CANCELLED,
                    message="バックテストがキャンセルされました",
                )

        except Exception as e:
            logger.exception(f"バックテストエラー: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="バックテストに失敗しました",
                error=str(e),
            )

        finally:
            self._manager.release_slot()

    async def _start_worker_process(
        self,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
    ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            *self._build_worker_command(job_id, strategy_name, config_override),
            cwd=str(_PROJECT_ROOT),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

    def _build_worker_command(
        self,
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            _WORKER_MODULE,
            "--job-id",
            job_id,
            "--strategy-name",
            strategy_name,
            "--timeout-seconds",
            str(self._worker_timeout_seconds),
        ]
        if config_override is not None:
            command.extend(
                [
                    "--config-override-json",
                    json.dumps(config_override, ensure_ascii=False),
                ]
            )
        return command

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

    def _execute_backtest_sync(
        self,
        job_id: str,
        strategy_name: str,
        loop: asyncio.AbstractEventLoop,
        config_override: dict[str, Any] | None = None,
    ) -> BacktestResult:
        """
        同期的にバックテストを実行

        Args:
            job_id: ジョブID
            strategy_name: 戦略名
            loop: イベントループ（進捗コールバック用）
            config_override: 設定オーバーライド

        Returns:
            バックテスト結果
        """
        def progress_callback(status: str, elapsed: float) -> None:
            """進捗コールバック — SSE通知を発火"""
            asyncio.run_coroutine_threadsafe(
                self._manager.update_job_status(
                    job_id, JobStatus.RUNNING, message=status
                ),
                loop,
            )

        return self._runner.execute(
            strategy=strategy_name,
            progress_callback=progress_callback,
            config_override=config_override,
        )

    def _extract_result_summary(self, result: BacktestResult) -> BacktestResultSummary:
        """
        BacktestResultからサマリーを抽出

        HTML成果物セット（HTML + *.metrics.json）を優先し、
        抽出できない場合は summary dict をフォールバックとして使用する。

        Args:
            result: バックテスト結果

        Returns:
            結果サマリー
        """
        summary = resolve_backtest_result_summary(
            html_path=result.html_path,
            fallback=result.summary,
        )
        if summary is not None:
            return summary
        # 通常は到達しないが、型上の安全性のためにゼロ値で返す
        return BacktestResultSummary(
            total_return=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=None,
            calmar_ratio=0.0,
            max_drawdown=0.0,
            win_rate=0.0,
            trade_count=0,
            html_path=str(result.html_path),
        )

    def get_execution_info(self, strategy_name: str) -> dict[str, Any]:
        """
        戦略の実行情報を取得

        Args:
            strategy_name: 戦略名

        Returns:
            実行情報
        """
        return self._runner.get_execution_info(strategy_name)


# グローバルインスタンス
backtest_service = BacktestService()
