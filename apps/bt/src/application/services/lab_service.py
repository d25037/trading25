"""
Lab Service

戦略自動生成・GA進化・Optuna最適化・戦略改善の非同期サービス
"""

import asyncio
from copy import deepcopy
import json
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from loguru import logger

from src.application.services.verification_orchestrator import (
    INTERNAL_VERIFICATION_CANDIDATES_KEY,
    INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY,
    build_canonical_metrics,
    build_verification_seed,
)
from src.domains.backtest.contracts import EnginePolicy, RunSpec
from src.domains.lab_agent.evaluator import load_default_shared_config
from src.domains.lab_agent.models import (
    LabStructureMode,
    LabTargetScope,
    SignalCategory,
    StrategyCandidate,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.lab import (
    EvolutionHistoryItem,
    GenerateResultItem,
    ImprovementItem,
    OptimizeTrialItem,
)
from src.application.services.job_manager import JobManager, job_manager
from src.application.services.run_contracts import build_parameterized_run_spec, build_strategy_run_spec
from src.shared.config.settings import get_settings

_INTERNAL_JOB_MESSAGE_KEY = "_job_message"
_EVOLVE_COMPLETE_MESSAGE = "GA進化完了"
_EVOLVE_BASE_BEST_MESSAGE = "GA進化完了（ベース戦略が最良のためパラメータ変更なし）"
_WORKER_MODULE = "src.application.workers.lab_worker"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_LAB_JOB_MESSAGES: dict[str, dict[str, str]] = {
    "generate": {
        "start": "戦略を生成しています...",
        "complete": "戦略生成完了",
        "cancel": "戦略生成がキャンセルされました",
        "fail": "戦略生成に失敗しました",
        "timeout": "戦略生成がタイムアウトしました",
        "worker_start": "戦略生成 worker を起動しています...",
    },
    "evolve": {
        "start": "GA進化を開始しています...",
        "complete": _EVOLVE_COMPLETE_MESSAGE,
        "cancel": "GA進化がキャンセルされました",
        "fail": "GA進化に失敗しました",
        "timeout": "GA進化がタイムアウトしました",
        "worker_start": "GA進化 worker を起動しています...",
    },
    "optimize": {
        "start": "Optuna最適化を開始しています...",
        "complete": "Optuna最適化完了",
        "cancel": "Optuna最適化がキャンセルされました",
        "fail": "Optuna最適化に失敗しました",
        "timeout": "Optuna最適化がタイムアウトしました",
        "worker_start": "Optuna最適化 worker を起動しています...",
    },
    "improve": {
        "start": "戦略を分析しています...",
        "complete": "戦略改善完了",
        "cancel": "戦略改善がキャンセルされました",
        "fail": "戦略改善に失敗しました",
        "timeout": "戦略改善がタイムアウトしました",
        "worker_start": "戦略改善 worker を起動しています...",
    },
}


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


def _normalize_universe_preset(universe_preset: str | None) -> str | None:
    if universe_preset is None:
        return None

    normalized = universe_preset.strip()
    return normalized or None


def _resolve_generate_universe_default(universe_preset: str | None) -> str | None:
    normalized_preset = _normalize_universe_preset(universe_preset)
    if normalized_preset is not None:
        return normalized_preset

    default_preset = load_default_shared_config().get("universe_preset")
    if not isinstance(default_preset, str):
        return None

    return _normalize_universe_preset(default_preset)


def _resolve_generate_universe_preset(
    universe_preset: str | None,
) -> str | None:
    return _resolve_generate_universe_default(universe_preset)


def _merge_generate_shared_config(
    candidate_shared_config: dict[str, Any] | None,
    *,
    direction: str,
    timeframe: str,
    universe_preset: str | None,
) -> dict[str, Any]:
    merged_shared_config = dict(candidate_shared_config or {})
    merged_shared_config["direction"] = direction
    merged_shared_config["timeframe"] = timeframe
    merged_shared_config["data_source"] = "market"
    merged_shared_config.pop("dataset", None)
    if universe_preset is None:
        merged_shared_config.pop("universe_preset", None)
    else:
        merged_shared_config["universe_preset"] = universe_preset
    return merged_shared_config


def _candidate_to_config_override(
    candidate: StrategyCandidate,
    *,
    shared_config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged_shared_config = dict(shared_config_override or {})
    merged_shared_config.update(candidate.shared_config or {})
    return {
        "shared_config": merged_shared_config,
        "entry_filter_params": deepcopy(candidate.entry_filter_params),
        "exit_trigger_params": deepcopy(candidate.exit_trigger_params),
    }


class LabService:
    """Lab実行サービス"""

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
            else get_settings().lab_job_timeout_seconds
        )

    async def _run_worker_job(
        self,
        job_id: str,
        payload: dict[str, Any],
    ) -> None:
        """外部 worker を起動して durable state を監視する。"""
        lab_type = str(payload.get("lab_type", "lab"))
        messages = _LAB_JOB_MESSAGES.get(lab_type, {})
        process: asyncio.subprocess.Process | None = None
        try:
            await self._manager.acquire_slot()
            await self._manager.update_job_status(
                job_id,
                JobStatus.PENDING,
                message=messages.get("worker_start", "Lab worker を起動しています..."),
                progress=0.0,
            )

            logger.info(f"Lab {lab_type} worker 開始: {job_id}")
            process = await self._start_worker_process(job_id, payload)
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
                    message=messages.get("fail", "Lab worker が結果を保存せず終了しました"),
                    error="worker_exited_without_terminal_state",
                )
                return
            if self._manager.is_cancel_requested(job_id):
                logger.info(f"Lab {lab_type} worker を停止しました: {job_id}")
                return
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message=messages.get("fail", "Lab worker が異常終了しました"),
                error=f"worker_exit_code={exit_code}",
            )
        except asyncio.CancelledError:
            logger.info(f"Lab {lab_type} watcher キャンセル: {job_id}")
            if process is not None:
                await self._terminate_worker_process(process)
            await self._manager.reload_job_from_storage(job_id, notify=True)
            if not self._manager.is_cancel_requested(job_id):
                await self._manager.update_job_status(
                    job_id,
                    JobStatus.CANCELLED,
                    message=messages.get("cancel", "Labジョブがキャンセルされました"),
                )
        except Exception as e:
            logger.exception(f"Lab {lab_type} watcher エラー: {job_id}")
            await self._manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message=messages.get("fail", "Labジョブに失敗しました"),
                error=str(e),
            )
        finally:
            self._manager.release_slot()

    async def _submit_worker_job(
        self,
        strategy_name: str,
        job_type: str,
        run_spec: RunSpec | None,
        payload: dict[str, Any],
    ) -> str:
        job_id = self._manager.create_job(
            strategy_name,
            job_type=job_type,
            run_spec=run_spec,
        )
        task = asyncio.create_task(self._run_worker_job(job_id, payload))
        await self._manager.set_job_task(job_id, task)
        return job_id

    async def _start_worker_process(
        self,
        job_id: str,
        payload: dict[str, Any],
    ) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            *self._build_worker_command(job_id, payload),
            cwd=str(_PROJECT_ROOT),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

    def _build_worker_command(
        self,
        job_id: str,
        payload: dict[str, Any],
    ) -> list[str]:
        return [
            sys.executable,
            "-m",
            _WORKER_MODULE,
            "--job-id",
            job_id,
            "--payload-json",
            json.dumps(payload, ensure_ascii=False),
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
        run_spec: RunSpec | None,
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
        job_id = self._manager.create_job(
            strategy_name,
            job_type=job_type,
            run_spec=run_spec,
        )

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
        universe_preset: str | None = None,
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
        engine_policy: EnginePolicy | None = None,
    ) -> str:
        """戦略自動生成ジョブをサブミット"""
        resolved_engine_policy = engine_policy or EnginePolicy()
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        resolved_universe_preset = _resolve_generate_universe_preset(universe_preset)
        parameters: dict[str, Any] = {
            "count": count,
            "top": top,
            "seed": seed,
            "save": save,
            "direction": direction,
            "timeframe": timeframe,
            "entry_filter_only": entry_filter_only,
            "allowed_categories": resolved_categories,
            "engine_policy": resolved_engine_policy.model_dump(mode="json"),
        }
        if resolved_universe_preset is not None:
            parameters["universe_preset"] = resolved_universe_preset
        run_spec = build_parameterized_run_spec(
            "lab_generate",
            f"generate(n={count},top={top})",
            dataset_name=resolved_universe_preset,
            parameters=parameters,
        )
        payload: dict[str, Any] = {
            "lab_type": "generate",
            "count": count,
            "top": top,
            "seed": seed,
            "save": save,
            "direction": direction,
            "timeframe": timeframe,
            "entry_filter_only": entry_filter_only,
            "allowed_categories": resolved_categories,
            "engine_policy": resolved_engine_policy.model_dump(mode="json"),
        }
        if resolved_universe_preset is not None:
            payload["universe_preset"] = resolved_universe_preset
        return await self._submit_worker_job(
            strategy_name=f"generate(n={count},top={top})",
            job_type="lab_generate",
            run_spec=run_spec,
            payload=payload,
        )

    def _execute_generate_sync(
        self,
        count: int,
        top: int,
        seed: int | None,
        save: bool,
        direction: str,
        timeframe: str,
        universe_preset: str | None,
        entry_filter_only: bool = False,
        allowed_categories: list[SignalCategory] | None = None,
    ) -> dict[str, Any]:
        """同期的に戦略生成を実行"""
        from src.domains.lab_agent.evaluator import StrategyEvaluator
        from src.domains.lab_agent.models import GeneratorConfig
        from src.domains.lab_agent.strategy_generator import StrategyGenerator
        from src.domains.lab_agent.yaml_updater import YamlUpdater

        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        resolved_universe_preset = _resolve_generate_universe_preset(universe_preset)
        shared_config_override = _merge_generate_shared_config(
            None,
            direction=direction,
            timeframe=timeframe,
            universe_preset=resolved_universe_preset,
        )
        config = GeneratorConfig(
            n_strategies=count,
            seed=seed,
            entry_filter_only=entry_filter_only,
            allowed_categories=resolved_categories,
        )
        generator = StrategyGenerator(config=config)
        candidates = generator.generate()

        evaluator = StrategyEvaluator(
            shared_config_dict=shared_config_override,
            n_jobs=-1,
        )
        results = evaluator.evaluate_batch(candidates, top_k=top)
        successful_results = [result for result in results if result.success]
        verification_candidates = []

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
        for rank, result in enumerate(successful_results, start=1):
            fast_metrics = build_canonical_metrics(
                {
                    "total_return": result.total_return,
                    "sharpe_ratio": result.sharpe_ratio,
                    "calmar_ratio": result.calmar_ratio,
                    "max_drawdown": result.max_drawdown,
                    "win_rate": result.win_rate,
                    "trade_count": result.trade_count,
                }
            )
            verification_candidates.append(
                build_verification_seed(
                    candidate_id=result.candidate.strategy_id,
                    fast_rank=rank,
                    fast_score=result.score,
                    fast_metrics=fast_metrics,
                    strategy_name="reference/strategy_template",
                    config_override={
                        "shared_config": _merge_generate_shared_config(
                            result.candidate.shared_config,
                            direction=direction,
                            timeframe=timeframe,
                            universe_preset=resolved_universe_preset,
                        ),
                        "entry_filter_params": deepcopy(result.candidate.entry_filter_params),
                        "exit_trigger_params": deepcopy(result.candidate.exit_trigger_params),
                    },
                    strategy_candidate=result.candidate,
                ).model_dump(mode="json")
            )

        saved_path: str | None = None
        if save and successful_results:
            best = successful_results[0]
            best.candidate.shared_config = _merge_generate_shared_config(
                best.candidate.shared_config,
                direction=direction,
                timeframe=timeframe,
                universe_preset=resolved_universe_preset,
            )
            yaml_updater = YamlUpdater()
            saved_path = yaml_updater.save_candidate(best.candidate)

        return {
            "lab_type": "generate",
            "results": result_items,
            "total_generated": count,
            "saved_strategy_path": saved_path,
            INTERNAL_VERIFICATION_CANDIDATES_KEY: verification_candidates,
            INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY: dict(evaluator.scoring_weights),
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
        engine_policy: EnginePolicy | None = None,
    ) -> str:
        """GA進化ジョブをサブミット"""
        resolved_engine_policy = engine_policy or EnginePolicy()
        resolved_target_scope = _resolve_target_scope(target_scope, entry_filter_only)
        effective_entry_filter_only = resolved_target_scope == "entry_filter_only"
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        run_spec = build_strategy_run_spec(
            "lab_evolve",
            strategy_name,
            parameters={
                "generations": generations,
                "population": population,
                "structure_mode": structure_mode,
                "random_add_entry_signals": random_add_entry_signals,
                "random_add_exit_signals": random_add_exit_signals,
                "seed": seed,
                "save": save,
                "entry_filter_only": effective_entry_filter_only,
                "target_scope": resolved_target_scope,
                "allowed_categories": resolved_categories,
                "engine_policy": resolved_engine_policy.model_dump(mode="json"),
            },
            config_loader=self._config_loader,
        )
        return await self._submit_worker_job(
            strategy_name=strategy_name,
            job_type="lab_evolve",
            run_spec=run_spec,
            payload={
                "lab_type": "evolve",
                "strategy_name": strategy_name,
                "generations": generations,
                "population": population,
                "structure_mode": structure_mode,
                "random_add_entry_signals": random_add_entry_signals,
                "random_add_exit_signals": random_add_exit_signals,
                "seed": seed,
                "save": save,
                "entry_filter_only": effective_entry_filter_only,
                "allowed_categories": resolved_categories,
                "target_scope": resolved_target_scope,
                "engine_policy": resolved_engine_policy.model_dump(mode="json"),
            },
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
        best_candidate, all_results = evolver.evolve(strategy_name)
        history = evolver.get_evolution_history()
        successful_results = sorted(
            [result for result in all_results if result.success],
            key=lambda result: result.score,
            reverse=True,
        )
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
        fast_candidates = []
        verification_candidates = []
        for rank, result in enumerate(successful_results, start=1):
            candidate_id = f"evolve_{rank:04d}"
            fast_metrics = build_canonical_metrics(
                {
                    "total_return": result.total_return,
                    "sharpe_ratio": result.sharpe_ratio,
                    "calmar_ratio": result.calmar_ratio,
                    "max_drawdown": result.max_drawdown,
                    "win_rate": result.win_rate,
                    "trade_count": result.trade_count,
                }
            )
            if rank <= 10:
                fast_candidates.append(
                    {
                        "candidate_id": candidate_id,
                        "rank": rank,
                        "score": result.score,
                        "metrics": fast_metrics.model_dump(mode="json") if fast_metrics is not None else None,
                    }
                )
            verification_candidates.append(
                build_verification_seed(
                    candidate_id=candidate_id,
                    fast_rank=rank,
                    fast_score=result.score,
                    fast_metrics=fast_metrics,
                    strategy_name=strategy_name,
                    config_override=_candidate_to_config_override(result.candidate),
                    strategy_candidate=result.candidate,
                ).model_dump(mode="json")
            )

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
            "fast_candidates": fast_candidates,
            "saved_strategy_path": saved_strategy_path,
            "saved_history_path": saved_history_path,
            INTERNAL_VERIFICATION_CANDIDATES_KEY: verification_candidates,
            INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY: dict(evolver.scoring_weights),
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
        engine_policy: EnginePolicy | None = None,
    ) -> str:
        """Optuna最適化ジョブをサブミット"""
        resolved_engine_policy = engine_policy or EnginePolicy()
        resolved_target_scope = _resolve_target_scope(target_scope, entry_filter_only)
        effective_entry_filter_only = resolved_target_scope == "entry_filter_only"
        resolved_categories: list[SignalCategory] = list(allowed_categories or [])
        run_spec = build_strategy_run_spec(
            "lab_optimize",
            strategy_name,
            parameters={
                "trials": trials,
                "sampler": sampler,
                "structure_mode": structure_mode,
                "random_add_entry_signals": random_add_entry_signals,
                "random_add_exit_signals": random_add_exit_signals,
                "seed": seed,
                "save": save,
                "entry_filter_only": effective_entry_filter_only,
                "target_scope": resolved_target_scope,
                "allowed_categories": resolved_categories,
                "scoring_weights": scoring_weights,
                "engine_policy": resolved_engine_policy.model_dump(mode="json"),
            },
            config_loader=self._config_loader,
        )
        return await self._submit_worker_job(
            strategy_name=strategy_name,
            job_type="lab_optimize",
            run_spec=run_spec,
            payload={
                "lab_type": "optimize",
                "strategy_name": strategy_name,
                "trials": trials,
                "sampler": sampler,
                "structure_mode": structure_mode,
                "random_add_entry_signals": random_add_entry_signals,
                "random_add_exit_signals": random_add_exit_signals,
                "seed": seed,
                "save": save,
                "entry_filter_only": effective_entry_filter_only,
                "target_scope": resolved_target_scope,
                "allowed_categories": resolved_categories,
                "scoring_weights": scoring_weights,
                "engine_policy": resolved_engine_policy.model_dump(mode="json"),
            },
        )

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
        sorted_history = sorted(
            history,
            key=lambda trial_result: float(trial_result.get("score", 0.0)),
            reverse=True,
        )

        history_items = [
            OptimizeTrialItem(
                trial=h.get("trial", i),
                score=h.get("score", 0.0),
                params=h.get("params", {}),
            ).model_dump()
            for i, h in enumerate(history)
        ]
        fast_candidates = []
        verification_candidates = []
        for rank, trial_result in enumerate(sorted_history, start=1):
            candidate_id = f"trial_{int(trial_result.get('trial', rank))}"
            fast_metrics = build_canonical_metrics(trial_result)
            strategy_candidate = optimizer.build_candidate_from_params(
                dict(trial_result.get("params") or {}),
                strategy_id=candidate_id,
            )
            if rank <= 10:
                fast_candidates.append(
                    {
                        "candidate_id": candidate_id,
                        "rank": rank,
                        "score": float(trial_result.get("score", 0.0)),
                        "metrics": fast_metrics.model_dump(mode="json") if fast_metrics is not None else None,
                    }
                )
            verification_candidates.append(
                build_verification_seed(
                    candidate_id=candidate_id,
                    fast_rank=rank,
                    fast_score=float(trial_result.get("score", 0.0)),
                    fast_metrics=fast_metrics,
                    strategy_name=strategy_name,
                    config_override=_candidate_to_config_override(strategy_candidate),
                    strategy_candidate=strategy_candidate,
                ).model_dump(mode="json")
            )

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
            "fast_candidates": fast_candidates,
            "saved_strategy_path": saved_strategy_path,
            "saved_history_path": saved_history_path,
            INTERNAL_VERIFICATION_CANDIDATES_KEY: verification_candidates,
            INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY: dict(optimizer.scoring_weights),
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
        run_spec = build_strategy_run_spec(
            "lab_improve",
            strategy_name,
            parameters={
                "auto_apply": auto_apply,
                "entry_filter_only": entry_filter_only,
                "allowed_categories": resolved_categories,
            },
            config_loader=self._config_loader,
        )
        return await self._submit_worker_job(
            strategy_name=strategy_name,
            job_type="lab_improve",
            run_spec=run_spec,
            payload={
                "lab_type": "improve",
                "strategy_name": strategy_name,
                "auto_apply": auto_apply,
                "entry_filter_only": entry_filter_only,
                "allowed_categories": resolved_categories,
            },
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
