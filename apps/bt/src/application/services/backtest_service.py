"""
Backtest Service

BacktestRunnerの非同期ラッパー
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from loguru import logger

from src.domains.backtest.core.runner import BacktestResult, BacktestRunner
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.application.services.backtest_result_summary import resolve_backtest_result_summary
from src.application.services.job_manager import JobManager, job_manager


class BacktestService:
    """バックテスト実行サービス"""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 2,
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
        job_id = self._manager.create_job(strategy_name)

        # バックグラウンドタスクとして実行
        task = asyncio.create_task(
            self._run_backtest(job_id, strategy_name, config_override)
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
        try:
            # スロット取得（同時実行数制限）
            await self._manager.acquire_slot()

            await self._manager.update_job_status(
                job_id,
                JobStatus.RUNNING,
                message="バックテストを開始しています...",
                progress=0.0,
            )

            logger.info(f"バックテスト開始: {job_id} (戦略: {strategy_name})")

            # 同期的なBacktestRunnerをスレッドプールで実行
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._executor,
                self._execute_backtest_sync,
                job_id,
                strategy_name,
                loop,
                config_override,
            )

            # 結果を抽出してサマリー作成
            summary = self._extract_result_summary(result)

            await self._manager.set_job_result(
                job_id=job_id,
                result_summary=summary,
                raw_result=result.summary,
                html_path=str(result.html_path),
                dataset_name=result.dataset_name,
                execution_time=result.elapsed_time,
            )

            await self._manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                message="バックテスト完了",
                progress=1.0,
            )

            logger.info(
                f"バックテスト完了: {job_id} ({result.elapsed_time:.1f}秒)"
            )

        except asyncio.CancelledError:
            logger.info(f"バックテストがキャンセルされました: {job_id}")
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
