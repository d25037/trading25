"""
Lab Service

戦略自動生成・GA進化・Optuna最適化・戦略改善の非同期サービス
"""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from loguru import logger

from src.domains.lab_agent.models import LabStructureMode, LabTargetScope, SignalCategory
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.lab import (
    EvolutionHistoryItem,
    GenerateResultItem,
    ImprovementItem,
    OptimizeTrialItem,
)
from src.application.services.job_manager import JobManager, job_manager

_INTERNAL_JOB_MESSAGE_KEY = "_job_message"
_EVOLVE_COMPLETE_MESSAGE = "GA進化完了"
_EVOLVE_BASE_BEST_MESSAGE = "GA進化完了（ベース戦略が最良のためパラメータ変更なし）"


def _resolve_target_scope(
    target_scope: LabTargetScope,
    entry_filter_only: bool,
) -> LabTargetScope:
    if target_scope == "exit_trigger_only" and entry_filter_only:
        raise ValueError(
            "entry_filter_only=true と target_scope=exit_trigger_only は同時指定できません"
        )
    if target_scope == "both" and entry_filter_only:
        return "entry_filter_only"
    return target_scope


class LabService:
    """Lab実行サービス"""

    def __init__(
        self,
        manager: JobManager | None = None,
        max_workers: int = 1,
    ) -> None:
        self._manager = manager or job_manager
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    # ============================================
    # Common Job Runner
    # ============================================

    async def _run_job(
        self,
        job_id: str,
        lab_type: str,
        start_message: str,
        complete_message: str,
        cancel_message: str,
        fail_message: str,
        log_detail: str,
        sync_fn: Callable[..., dict[str, Any]],
        sync_args: tuple[Any, ...],
    ) -> None:
        """共通のバックグラウンドジョブ実行処理"""
        try:
            await self._manager.acquire_slot()

            await self._manager.update_job_status(
                job_id, JobStatus.RUNNING, message=start_message, progress=0.0
            )

            logger.info(f"Lab {lab_type} 開始: {job_id} ({log_detail})")

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._executor, sync_fn, *sync_args
            )
            complete_message_for_job = complete_message
            if isinstance(result, dict):
                raw_message = result.pop(_INTERNAL_JOB_MESSAGE_KEY, None)
                if isinstance(raw_message, str) and raw_message.strip():
                    complete_message_for_job = raw_message

            job = self._manager.get_job(job_id)
            if job is not None:
                job.raw_result = result

            await self._manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                message=complete_message_for_job,
                progress=1.0,
            )

            logger.info(f"Lab {lab_type} 完了: {job_id}")

        except asyncio.CancelledError:
            logger.info(f"Lab {lab_type} キャンセル: {job_id}")
            await self._manager.update_job_status(
                job_id, JobStatus.CANCELLED, message=cancel_message
            )

        except Exception as e:
            logger.exception(f"Lab {lab_type} エラー: {job_id}")
            await self._manager.update_job_status(
                job_id, JobStatus.FAILED, message=fail_message, error=str(e)
            )

        finally:
            self._manager.release_slot()

    async def _submit_job(
        self,
        strategy_name: str,
        job_type: str,
        lab_type: str,
        start_message: str,
        complete_message: str,
        cancel_message: str,
        fail_message: str,
        log_detail: str,
        sync_fn: Callable[..., dict[str, Any]],
        sync_args: tuple[Any, ...],
    ) -> str:
        """共通のジョブサブミット処理"""
        job_id = self._manager.create_job(strategy_name, job_type=job_type)

        task = asyncio.create_task(
            self._run_job(
                job_id,
                lab_type,
                start_message,
                complete_message,
                cancel_message,
                fail_message,
                log_detail,
                sync_fn,
                sync_args,
            )
        )
        await self._manager.set_job_task(job_id, task)

        return job_id

    # ============================================
    # Generate
    # ============================================

    async def submit_generate(
        self,
        count: int = 100,
        top: int = 10,
        seed: int | None = None,
        save: bool = True,
        direction: str = "longonly",
        timeframe: str = "daily",
        dataset: str = "primeExTopix500",
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
    ) -> str:
        """戦略自動生成ジョブをサブミット"""
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        return await self._submit_job(
            strategy_name=f"generate(n={count},top={top})",
            job_type="lab_generate",
            lab_type="generate",
            start_message="戦略を生成しています...",
            complete_message="戦略生成完了",
            cancel_message="戦略生成がキャンセルされました",
            fail_message="戦略生成に失敗しました",
            log_detail=f"count={count}, top={top}",
            sync_fn=self._execute_generate_sync,
            sync_args=(
                count,
                top,
                seed,
                save,
                direction,
                timeframe,
                dataset,
                entry_filter_only,
                resolved_categories,
            ),
        )

    def _execute_generate_sync(
        self,
        count: int,
        top: int,
        seed: int | None,
        save: bool,
        direction: str,
        timeframe: str,
        dataset: str,
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
    ) -> dict[str, Any]:
        """同期的に戦略生成を実行"""
        from src.domains.lab_agent.evaluator import StrategyEvaluator
        from src.domains.lab_agent.models import GeneratorConfig
        from src.domains.lab_agent.strategy_generator import StrategyGenerator
        from src.domains.lab_agent.yaml_updater import YamlUpdater

        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        config = GeneratorConfig(
            n_strategies=count,
            seed=seed,
            entry_filter_only=entry_filter_only,
            allowed_categories=resolved_categories,
        )
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        evaluator = StrategyEvaluator(
            shared_config_dict={
                "direction": direction,
                "timeframe": timeframe,
                "dataset": dataset,
            },
            n_jobs=-1,
        )
        results = evaluator.evaluate_batch(candidates, top_k=top)

        result_items = [
            GenerateResultItem(
                strategy_id=r.candidate.strategy_id,
                score=r.score,
                sharpe_ratio=r.sharpe_ratio,
                calmar_ratio=r.calmar_ratio,
                total_return=r.total_return,
                max_drawdown=r.max_drawdown,
                win_rate=r.win_rate,
                trade_count=r.trade_count,
                entry_signals=list(r.candidate.entry_filter_params.keys()),
                exit_signals=list(r.candidate.exit_trigger_params.keys()),
            ).model_dump()
            for r in results
            if r.success
        ]

        saved_path: str | None = None
        if save and results:
            best = results[0]
            yaml_updater = YamlUpdater()
            saved_path = yaml_updater.save_candidate(best.candidate)

        return {
            "lab_type": "generate",
            "results": result_items,
            "total_generated": count,
            "saved_strategy_path": saved_path,
        }

    # ============================================
    # Evolve
    # ============================================

    async def submit_evolve(
        self,
        strategy_name: str,
        generations: int = 20,
        population: int = 50,
        structure_mode: LabStructureMode = "params_only",
        random_add_entry_signals: int = 1,
        random_add_exit_signals: int = 1,
        seed: int | None = None,
        save: bool = True,
        entry_filter_only: bool = False,
        target_scope: LabTargetScope = "both",
        allowed_categories: list[SignalCategory] | None = None,
    ) -> str:
        """GA進化ジョブをサブミット"""
        resolved_target_scope = _resolve_target_scope(target_scope, entry_filter_only)
        effective_entry_filter_only = resolved_target_scope == "entry_filter_only"
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        return await self._submit_job(
            strategy_name=strategy_name,
            job_type="lab_evolve",
            lab_type="evolve",
            start_message="GA進化を開始しています...",
            complete_message="GA進化完了",
            cancel_message="GA進化がキャンセルされました",
            fail_message="GA進化に失敗しました",
            log_detail=(
                f"戦略: {strategy_name}, generations={generations}, population={population}, "
                f"structure_mode={structure_mode}, target_scope={resolved_target_scope}"
            ),
            sync_fn=self._execute_evolve_sync,
            sync_args=(
                strategy_name,
                generations,
                population,
                structure_mode,
                random_add_entry_signals,
                random_add_exit_signals,
                seed,
                save,
                effective_entry_filter_only,
                resolved_categories,
                resolved_target_scope,
            ),
        )

    def _execute_evolve_sync(
        self,
        strategy_name: str,
        generations: int,
        population: int,
        structure_mode: LabStructureMode,
        random_add_entry_signals: int,
        random_add_exit_signals: int,
        seed: int | None,
        save: bool,
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
        target_scope: LabTargetScope = "both",
    ) -> dict[str, Any]:
        """同期的にGA進化を実行"""
        from src.domains.lab_agent.models import EvolutionConfig
        from src.domains.lab_agent.parameter_evolver import ParameterEvolver
        from src.domains.lab_agent.yaml_updater import YamlUpdater

        resolved_target_scope = _resolve_target_scope(target_scope, entry_filter_only)
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        config = EvolutionConfig(
            population_size=population,
            generations=generations,
            n_jobs=-1,
            entry_filter_only=resolved_target_scope == "entry_filter_only",
            target_scope=resolved_target_scope,
            allowed_categories=resolved_categories,
            structure_mode=structure_mode,
            random_add_entry_signals=random_add_entry_signals,
            random_add_exit_signals=random_add_exit_signals,
            seed=seed,
        )
        evolver = ParameterEvolver(config=config)
        best_candidate, _ = evolver.evolve(strategy_name)
        history = evolver.get_evolution_history()
        best_is_base_strategy = (
            best_candidate.strategy_id == f"base_{strategy_name}"
        )
        if best_is_base_strategy:
            logger.info(
                "Lab evolve selected base strategy as best candidate: "
                f"strategy={strategy_name}"
            )

        history_items = [
            EvolutionHistoryItem(
                generation=h.get("generation", i),
                best_score=h.get("best_score", 0.0),
                avg_score=h.get("avg_score", 0.0),
                worst_score=h.get("worst_score", 0.0),
            ).model_dump()
            for i, h in enumerate(history)
        ]

        saved_strategy_path: str | None = None
        saved_history_path: str | None = None
        if save:
            yaml_updater = YamlUpdater()
            saved_strategy_path, saved_history_path = yaml_updater.save_evolution_result(
                best_candidate, history, base_strategy_name=strategy_name
            )

        return {
            "lab_type": "evolve",
            "best_strategy_id": best_candidate.strategy_id,
            "best_score": history[-1]["best_score"] if history else 0.0,
            "history": history_items,
            "saved_strategy_path": saved_strategy_path,
            "saved_history_path": saved_history_path,
            _INTERNAL_JOB_MESSAGE_KEY: (
                _EVOLVE_BASE_BEST_MESSAGE
                if best_is_base_strategy
                else _EVOLVE_COMPLETE_MESSAGE
            ),
        }

    # ============================================
    # Optimize
    # ============================================

    async def submit_optimize(
        self,
        strategy_name: str,
        trials: int = 100,
        sampler: str = "tpe",
        structure_mode: LabStructureMode = "params_only",
        random_add_entry_signals: int = 1,
        random_add_exit_signals: int = 1,
        seed: int | None = None,
        save: bool = True,
        entry_filter_only: bool = False,
        target_scope: LabTargetScope = "both",
        allowed_categories: list[SignalCategory] | None = None,
        scoring_weights: dict[str, float] | None = None,
    ) -> str:
        """Optuna最適化ジョブをサブミット"""
        job_id = self._manager.create_job(strategy_name, job_type="lab_optimize")

        resolved_target_scope = _resolve_target_scope(target_scope, entry_filter_only)
        effective_entry_filter_only = resolved_target_scope == "entry_filter_only"
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        task = asyncio.create_task(
            self._run_optimize(
                job_id,
                strategy_name,
                trials,
                sampler,
                structure_mode,
                random_add_entry_signals,
                random_add_exit_signals,
                seed,
                save,
                effective_entry_filter_only,
                resolved_target_scope,
                resolved_categories,
                scoring_weights,
            )
        )
        await self._manager.set_job_task(job_id, task)

        return job_id

    async def _run_optimize(
        self,
        job_id: str,
        strategy_name: str,
        trials: int,
        sampler: str,
        structure_mode: LabStructureMode,
        random_add_entry_signals: int,
        random_add_exit_signals: int,
        seed: int | None,
        save: bool,
        entry_filter_only: bool,
        target_scope: LabTargetScope = "both",
        allowed_categories: list[SignalCategory] | None = None,
        scoring_weights: dict[str, float] | None = None,
    ) -> None:
        """Optuna最適化を実行（バックグラウンド）

        optimize はプログレスコールバックにイベントループが必要なため、
        共通の _run_job ではなく専用の実装を維持する。
        """
        try:
            await self._manager.acquire_slot()

            await self._manager.update_job_status(
                job_id, JobStatus.RUNNING,
                message="Optuna最適化を開始しています...", progress=0.0,
            )

            logger.info(
                f"Lab optimize 開始: {job_id} (戦略: {strategy_name}, "
                f"trials={trials}, sampler={sampler}, structure_mode={structure_mode}, "
                f"target_scope={target_scope})"
            )

            loop = asyncio.get_running_loop()

            def progress_callback(completed: int, total: int, best_score: float) -> None:
                """Optunaトライアル完了時のコールバック"""
                progress = completed / total if total > 0 else 0.0
                message = f"Trial {completed}/{total} 完了 (best: {best_score:.4f})"

                asyncio.run_coroutine_threadsafe(
                    self._manager.update_job_status(
                        job_id, JobStatus.RUNNING,
                        message=message, progress=progress,
                    ),
                    loop,
                )

            result = await loop.run_in_executor(
                self._executor,
                self._execute_optimize_sync,
                strategy_name,
                trials,
                sampler,
                structure_mode,
                random_add_entry_signals,
                random_add_exit_signals,
                seed,
                save,
                entry_filter_only,
                list(allowed_categories or []),
                scoring_weights,
                progress_callback,
                target_scope,
            )

            job = self._manager.get_job(job_id)
            if job is not None:
                job.raw_result = result

            await self._manager.update_job_status(
                job_id, JobStatus.COMPLETED,
                message="Optuna最適化完了", progress=1.0,
            )

            logger.info(f"Lab optimize 完了: {job_id}")

        except asyncio.CancelledError:
            logger.info(f"Lab optimize キャンセル: {job_id}")
            await self._manager.update_job_status(
                job_id, JobStatus.CANCELLED,
                message="Optuna最適化がキャンセルされました",
            )

        except Exception as e:
            logger.exception(f"Lab optimize エラー: {job_id}")
            await self._manager.update_job_status(
                job_id, JobStatus.FAILED,
                message="Optuna最適化に失敗しました", error=str(e),
            )

        finally:
            self._manager.release_slot()

    def _execute_optimize_sync(
        self,
        strategy_name: str,
        trials: int,
        sampler: str,
        structure_mode: LabStructureMode,
        random_add_entry_signals: int,
        random_add_exit_signals: int,
        seed: int | None,
        save: bool,
        entry_filter_only: bool,
        allowed_categories: list[SignalCategory],
        scoring_weights: dict[str, float] | None,
        progress_callback: Any,
        target_scope: LabTargetScope = "both",
    ) -> dict[str, Any]:
        """同期的にOptuna最適化を実行"""
        from src.domains.lab_agent.models import OptunaConfig
        from src.domains.lab_agent.optuna_optimizer import OptunaOptimizer
        from src.domains.lab_agent.yaml_updater import YamlUpdater

        resolved_target_scope = _resolve_target_scope(target_scope, entry_filter_only)
        config = OptunaConfig(
            n_trials=trials,
            sampler=sampler,
            n_jobs=-1,
            entry_filter_only=resolved_target_scope == "entry_filter_only",
            target_scope=resolved_target_scope,
            allowed_categories=allowed_categories,
            structure_mode=structure_mode,
            random_add_entry_signals=random_add_entry_signals,
            random_add_exit_signals=random_add_exit_signals,
            seed=seed,
        )

        optimizer = OptunaOptimizer(
            config=config,
            scoring_weights=scoring_weights,
        )

        best_candidate, study = optimizer.optimize(
            base_strategy=strategy_name,
            progress_callback=progress_callback,
        )

        history = optimizer.get_optimization_history(study)

        history_items = [
            OptimizeTrialItem(
                trial=h.get("trial", i),
                score=h.get("score", 0.0),
                params=h.get("params", {}),
            ).model_dump()
            for i, h in enumerate(history)
        ]

        best_params: dict[str, Any] = {}
        if study.best_trial:
            best_params = dict(study.best_trial.params)

        saved_strategy_path: str | None = None
        saved_history_path: str | None = None
        if save:
            yaml_updater = YamlUpdater()
            saved_strategy_path, saved_history_path = yaml_updater.save_optuna_result(
                best_candidate, history, base_strategy_name=strategy_name
            )

        return {
            "lab_type": "optimize",
            "best_score": study.best_value if study.best_trial else 0.0,
            "best_params": best_params,
            "total_trials": len(study.trials),
            "history": history_items,
            "saved_strategy_path": saved_strategy_path,
            "saved_history_path": saved_history_path,
        }

    # ============================================
    # Improve
    # ============================================

    async def submit_improve(
        self,
        strategy_name: str,
        auto_apply: bool = True,
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
    ) -> str:
        """戦略改善ジョブをサブミット"""
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        return await self._submit_job(
            strategy_name=strategy_name,
            job_type="lab_improve",
            lab_type="improve",
            start_message="戦略を分析しています...",
            complete_message="戦略改善完了",
            cancel_message="戦略改善がキャンセルされました",
            fail_message="戦略改善に失敗しました",
            log_detail=f"戦略: {strategy_name}",
            sync_fn=self._execute_improve_sync,
            sync_args=(
                strategy_name,
                auto_apply,
                entry_filter_only,
                resolved_categories,
            ),
        )

    def _execute_improve_sync(
        self,
        strategy_name: str,
        auto_apply: bool,
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
    ) -> dict[str, Any]:
        """同期的に戦略改善を実行"""
        from src.domains.lab_agent.strategy_improver import StrategyImprover
        from src.domains.lab_agent.yaml_updater import YamlUpdater
        from src.domains.strategy.runtime.loader import ConfigLoader

        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        improver = StrategyImprover()
        report = improver.analyze(
            strategy_name,
            entry_filter_only=entry_filter_only,
            allowed_categories=resolved_categories,
        )

        config_loader = ConfigLoader()
        strategy_config = config_loader.load_strategy_config(strategy_name)
        improvements = improver.suggest_improvements(
            report,
            strategy_config,
            entry_filter_only=entry_filter_only,
            allowed_categories=resolved_categories,
        )

        improvement_items = [
            ImprovementItem(
                improvement_type=imp.improvement_type,
                target=imp.target,
                signal_name=imp.signal_name,
                changes=imp.changes,
                reason=imp.reason,
                expected_impact=imp.expected_impact,
            ).model_dump()
            for imp in improvements
        ]

        saved_path: str | None = None
        if auto_apply and improvements:
            yaml_updater = YamlUpdater()
            saved_path = yaml_updater.apply_improvements(strategy_name, improvements)

        return {
            "lab_type": "improve",
            "strategy_name": strategy_name,
            "max_drawdown": report.max_drawdown,
            "max_drawdown_duration_days": report.max_drawdown_duration_days,
            "suggested_improvements": report.suggested_improvements,
            "improvements": improvement_items,
            "saved_strategy_path": saved_path,
        }


# グローバルインスタンス
lab_service = LabService()
