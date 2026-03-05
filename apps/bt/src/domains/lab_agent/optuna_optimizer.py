"""
Optunaによるベイズ最適化モジュール

TPE（Tree-structured Parzen Estimator）を使用した効率的なパラメータ探索
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import copy
import numpy as np
import pandas as pd
from loguru import logger
import random

# 型チェック時のみoptunaをインポート
if TYPE_CHECKING:
    import optuna
    from optuna.pruners import BasePruner
    from optuna.samplers import BaseSampler

from src.infrastructure.data_access.mode import data_access_mode_context
from src.infrastructure.data_access.loaders.data_preparation import prepare_multi_data
from src.infrastructure.data_access.loaders.index_loaders import load_topix_data
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.optimization.scoring import calculate_weighted_score_from_metrics

from .models import LabTargetScope, OptunaConfig, SignalCategory, StrategyCandidate
from .signal_filters import is_signal_allowed
from .signal_augmentation import apply_random_add_structure
from .signal_search_space import CATEGORICAL_PARAMS, PARAM_RANGES, ParamType

# ランタイムではtry-exceptでインポート
try:
    import optuna as optuna_runtime
    from optuna.pruners import MedianPruner, NopPruner
    from optuna.samplers import CmaEsSampler, RandomSampler, TPESampler

    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False
    optuna_runtime: Any | None = None
    TPESampler: Any | None = None
    RandomSampler: Any | None = None
    CmaEsSampler: Any | None = None
    NopPruner: Any | None = None
    MedianPruner: Any | None = None


class OptunaOptimizer:
    """
    Optunaによるベイズ最適化

    TPEサンプラーを使用してパラメータ空間を効率的に探索
    """

    # Backward-compatible alias.
    PARAM_RANGES = PARAM_RANGES
    MIN_ACCEPTABLE_RETURN_RATIO = 0.95
    MIN_TRIALS_FOR_TWO_STAGE = 40
    TWO_STAGE_STAGE1_RATIO = 0.6
    MIN_STAGE2_TRIALS = 10
    LOCAL_SEARCH_TOP_K = 8
    LOCAL_SEARCH_MARGIN_RATIO = 0.15

    def __init__(
        self,
        config: OptunaConfig | None = None,
        shared_config_dict: dict[str, Any] | None = None,
        scoring_weights: dict[str, float] | None = None,
    ):
        """
        初期化

        Args:
            config: Optuna設定
            shared_config_dict: 共有設定辞書
            scoring_weights: スコアリング重み
        """
        if not OPTUNA_AVAILABLE:
            raise ImportError(
                "Optuna is not installed. Please install with: uv add optuna"
            )

        self.config = config or OptunaConfig()
        self.shared_config_dict = shared_config_dict
        self.scoring_weights = scoring_weights or {
            "sharpe_ratio": 0.5,
            "calmar_ratio": 0.3,
            "total_return": 0.2,
        }
        self.allowed_category_set: set[SignalCategory] = set(self.config.allowed_categories)

        # ベース戦略パラメータ（後で設定）
        self.base_entry_params: dict[str, Any] = {}
        self.base_exit_params: dict[str, Any] = {}
        self.base_shared_config: dict[str, Any] = {}
        self._shared_config: SharedConfig | None = None
        self._prefetched_multi_data: dict[str, dict[str, pd.DataFrame]] | None = None
        self._prefetched_benchmark_data: pd.DataFrame | None = None
        self._baseline_score: float | None = None
        self._baseline_total_return: float | None = None
        self._param_specs: dict[str, tuple[float, float, ParamType]] = {}
        self._active_param_overrides: dict[str, tuple[float, float, ParamType]] = {}

    def _is_usage_targeted(self, usage_type: str) -> bool:
        """target_scope に基づき対象サイドか判定する。"""
        if self.config.target_scope == "both":
            return True
        if self.config.target_scope == "entry_filter_only":
            return usage_type == "entry"
        return usage_type == "exit"

    def _effective_random_add_counts(self) -> tuple[int, int]:
        """target_scope を反映した random_add 数を返す。"""
        add_entry = (
            self.config.random_add_entry_signals
            if self._is_usage_targeted("entry")
            else 0
        )
        add_exit = (
            self.config.random_add_exit_signals
            if self._is_usage_targeted("exit")
            else 0
        )
        return add_entry, add_exit

    @staticmethod
    def recommend_trials_from_dimension_count(
        dimension_count: int,
    ) -> dict[str, int]:
        """探索次元数に対する試行回数の推奨値を返す。"""
        dims = max(1, int(dimension_count))
        minimum_trials = min(1000, max(40, dims * 8))
        recommended_trials = min(1000, max(60, dims * 15))
        high_quality_trials = min(1000, max(100, dims * 25))
        return {
            "minimum_trials": minimum_trials,
            "recommended_trials": recommended_trials,
            "high_quality_trials": high_quality_trials,
        }

    @classmethod
    def estimate_trial_recommendation(
        cls,
        strategy_name: str,
        target_scope: LabTargetScope = "both",
        allowed_categories: list[SignalCategory] | None = None,
    ) -> dict[str, int]:
        """戦略の探索次元数を推定し、試行回数の推奨値を返す。"""
        config = OptunaConfig(
            n_trials=100,
            target_scope=target_scope,
            entry_filter_only=target_scope == "entry_filter_only",
            allowed_categories=list(allowed_categories or []),
        )
        estimator = cls(config=config)
        base_candidate = estimator._load_base_strategy(strategy_name)
        estimator.base_entry_params = base_candidate.entry_filter_params
        estimator.base_exit_params = base_candidate.exit_trigger_params
        flat_params = estimator._build_optuna_param_dict(
            estimator.base_entry_params,
            estimator.base_exit_params,
        )
        dimension_count = len(flat_params)
        recommendation = cls.recommend_trials_from_dimension_count(dimension_count)
        recommendation["dimension_count"] = dimension_count
        return recommendation

    def optimize(
        self,
        base_strategy: str | StrategyCandidate,
        progress_callback: Callable[[int, int, float], None] | None = None,
    ) -> tuple[StrategyCandidate, optuna.Study]:
        """
        Optunaで戦略パラメータを最適化

        Args:
            base_strategy: ベース戦略名またはStrategyCandidate
            progress_callback: 進捗コールバック (completed, total, best_score)

        Returns:
            (最良戦略候補, Optuna Study)
        """
        # ベース戦略の読み込み
        base_candidate = self._load_base_strategy(base_strategy)

        # random_add モード: ベース戦略に新しいシグナルを追加してから最適化
        add_entry_signals, add_exit_signals = self._effective_random_add_counts()
        should_random_add = (
            self.config.structure_mode == "random_add"
            and (add_entry_signals > 0 or add_exit_signals > 0)
        )
        if should_random_add:
            rng = (
                random.Random(self.config.seed)
                if self.config.seed is not None
                else random.Random()
            )
            base_entry = _extract_enabled_signal_names(base_candidate.entry_filter_params)
            base_exit = _extract_enabled_signal_names(base_candidate.exit_trigger_params)
            base_candidate, added = apply_random_add_structure(
                base_candidate,
                rng=rng,
                add_entry_signals=add_entry_signals,
                add_exit_signals=add_exit_signals,
                base_entry_signals=base_entry,
                base_exit_signals=base_exit,
                allowed_categories=self.allowed_category_set,
            )
            logger.info(
                "Optuna random-add applied: "
                f"entry_added={added['entry']}, exit_added={added['exit']}"
            )

        self.base_entry_params = base_candidate.entry_filter_params
        self.base_exit_params = base_candidate.exit_trigger_params
        self.base_shared_config = base_candidate.shared_config or {}
        self._param_specs = self._build_param_specs(
            self.base_entry_params,
            self.base_exit_params,
        )
        self._active_param_overrides = {}
        if self.shared_config_dict is None:
            self.shared_config_dict = {}
        self._prepare_prefetched_data()

        logger.info(
            f"Starting Optuna optimization: n_trials={self.config.n_trials}, "
            f"sampler={self.config.sampler}, dimensions={len(self._param_specs)}"
        )

        self._evaluate_baseline_candidate(base_candidate)

        # サンプラー選択
        sampler = self._create_sampler()
        optuna_rt = cast(Any, optuna_runtime)

        # Study作成
        study = optuna_rt.create_study(
            study_name=self.config.study_name,
            storage=f"sqlite:///{self.config.storage_path}"
            if self.config.storage_path
            else None,
            direction="maximize",
            sampler=sampler,
            pruner=self._create_pruner(),
            load_if_exists=True,
        )

        base_trial_params = self._build_optuna_param_dict(
            self.base_entry_params,
            self.base_exit_params,
        )
        if base_trial_params:
            study.enqueue_trial(base_trial_params)

        # Optunaコールバック（trial完了時に外部通知）
        callbacks = []
        if progress_callback is not None:
            n_trials = self.config.n_trials

            def _optuna_callback(
                study: optuna.Study, trial: optuna.trial.FrozenTrial
            ) -> None:
                completed = len([
                    t for t in study.trials
                    if t.state == optuna_rt.trial.TrialState.COMPLETE
                ])
                best_score = study.best_value if study.best_trial else 0.0
                progress_callback(completed, n_trials, best_score)

            callbacks.append(_optuna_callback)

        # 最適化実行（2段階探索）
        stage1_trials, stage2_trials = self._build_two_stage_plan(self.config.n_trials)
        logger.info(
            "Optuna stage plan: "
            f"stage1={stage1_trials}, stage2={stage2_trials}, total={self.config.n_trials}"
        )

        self._active_param_overrides = {}
        self._optimize_study(study, stage1_trials, callbacks)

        if stage2_trials > 0:
            stage2_overrides, seed_trials = self._build_stage2_local_search_space(study)
            if stage2_overrides:
                self._active_param_overrides = stage2_overrides
                self._enqueue_stage2_seed_trials(study, seed_trials, stage2_overrides)
                logger.info(
                    "Optuna stage2 local search enabled: "
                    f"narrowed_dims={len(stage2_overrides)}"
                )
            else:
                self._active_param_overrides = {}
                logger.info(
                    "Optuna stage2 local search skipped: "
                    "insufficient complete trials for narrowing"
                )
            self._optimize_study(study, stage2_trials, callbacks)

        self._active_param_overrides = {}

        # 最良パラメータで戦略候補を構築
        best_candidate = self._resolve_best_candidate(study, base_candidate)

        logger.info(
            f"Optimization complete: best_score={study.best_value:.4f}, "
            f"n_trials={len(study.trials)}"
        )

        return best_candidate, study

    def _optimize_study(
        self,
        study: optuna.Study,
        n_trials: int,
        callbacks: list[Callable[[optuna.Study, optuna.trial.FrozenTrial], None]],
    ) -> None:
        """指定試行数で Study を最適化する。"""
        if n_trials <= 0:
            return
        study.optimize(
            self._objective,
            n_trials=n_trials,
            n_jobs=self.config.n_jobs,
            show_progress_bar=True,
            callbacks=callbacks,
        )

    def _build_two_stage_plan(self, total_trials: int) -> tuple[int, int]:
        """2段階探索の試行数配分を返す。"""
        if total_trials < self.MIN_TRIALS_FOR_TWO_STAGE:
            return total_trials, 0

        stage1_trials = int(total_trials * self.TWO_STAGE_STAGE1_RATIO)
        stage1_trials = max(1, min(total_trials, stage1_trials))
        stage2_trials = total_trials - stage1_trials

        if stage2_trials < self.MIN_STAGE2_TRIALS:
            return total_trials, 0
        return stage1_trials, stage2_trials

    def _build_stage2_local_search_space(
        self,
        study: optuna.Study,
    ) -> tuple[dict[str, tuple[float, float, ParamType]], list[optuna.trial.FrozenTrial]]:
        """stage1 上位trialを元に stage2 の局所探索レンジを構築する。"""
        optuna_rt = cast(Any, optuna_runtime)
        completed_trials = [
            trial for trial in study.trials
            if trial.state == optuna_rt.trial.TrialState.COMPLETE
            and trial.value is not None
        ]
        if len(completed_trials) < 3:
            return {}, []

        top_k = min(self.LOCAL_SEARCH_TOP_K, len(completed_trials))
        top_trials = sorted(
            completed_trials,
            key=lambda trial: float(cast(float, trial.value)),
            reverse=True,
        )[:top_k]

        overrides: dict[str, tuple[float, float, ParamType]] = {}
        for suggest_name, (global_min, global_max, param_type) in self._param_specs.items():
            trial_values = [
                float(trial.params[suggest_name])
                for trial in top_trials
                if suggest_name in trial.params
            ]
            if not trial_values:
                continue

            local_min = min(trial_values)
            local_max = max(trial_values)
            full_span = float(global_max - global_min)
            if full_span <= 0:
                continue

            margin = full_span * self.LOCAL_SEARCH_MARGIN_RATIO
            if param_type == "int":
                margin = max(margin, 1.0)

            narrowed_min = max(global_min, local_min - margin)
            narrowed_max = min(global_max, local_max + margin)

            if param_type == "int":
                min_int = max(int(global_min), int(np.floor(narrowed_min)))
                max_int = min(int(global_max), int(np.ceil(narrowed_max)))
                if min_int >= max_int:
                    center = int(round(float(np.mean(trial_values))))
                    min_int = max(int(global_min), center - 1)
                    max_int = min(int(global_max), center + 1)
                    if min_int >= max_int:
                        min_int = max_int = int(np.clip(center, global_min, global_max))
                overrides[suggest_name] = (float(min_int), float(max_int), "int")
                continue

            if narrowed_min >= narrowed_max:
                center = float(np.mean(trial_values))
                epsilon = max(full_span * 0.01, 1e-6)
                narrowed_min = max(global_min, center - epsilon)
                narrowed_max = min(global_max, center + epsilon)
                if narrowed_min >= narrowed_max:
                    narrowed_max = float(np.nextafter(narrowed_min, float("inf")))

            overrides[suggest_name] = (float(narrowed_min), float(narrowed_max), "float")

        return overrides, top_trials

    def _enqueue_stage2_seed_trials(
        self,
        study: optuna.Study,
        top_trials: list[optuna.trial.FrozenTrial],
        overrides: dict[str, tuple[float, float, ParamType]],
    ) -> None:
        """局所探索レンジに合わせて上位trialを stage2 の初期点として enqueue する。"""
        for trial in top_trials[:3]:
            seeded: dict[str, float | int] = {}
            for suggest_name, value in trial.params.items():
                if suggest_name not in overrides:
                    continue
                min_val, max_val, param_type = overrides[suggest_name]
                if param_type == "int":
                    clamped = int(np.clip(int(round(float(value))), int(min_val), int(max_val)))
                    seeded[suggest_name] = clamped
                else:
                    clamped = float(np.clip(float(value), min_val, max_val))
                    seeded[suggest_name] = clamped
            if seeded:
                study.enqueue_trial(seeded)

    def _resolve_best_candidate(
        self,
        study: optuna.Study,
        base_candidate: StrategyCandidate,
    ) -> StrategyCandidate:
        """非退行ガードを適用した最良候補を返す。"""
        if not study.best_trial:
            return base_candidate

        best_score = float(study.best_trial.value or 0.0)
        if self._baseline_score is not None and best_score < self._baseline_score:
            logger.warning(
                "Optuna best score is below baseline; falling back to base strategy "
                f"(best={best_score:.4f}, baseline={self._baseline_score:.4f})"
            )
            return base_candidate

        best_return_raw = study.best_trial.user_attrs.get("total_return")
        best_return = self._safe_metric(best_return_raw)
        if (
            self._baseline_total_return is not None
            and self._baseline_total_return > 0.0
            and best_return
            < self._baseline_total_return * self.MIN_ACCEPTABLE_RETURN_RATIO
        ):
            logger.warning(
                "Optuna best return is below acceptable baseline ratio; "
                "falling back to base strategy "
                f"(best={best_return:.4f}, baseline={self._baseline_total_return:.4f}, "
                f"ratio={self.MIN_ACCEPTABLE_RETURN_RATIO:.2f})"
            )
            return base_candidate

        return self._build_candidate_from_params(study.best_params)

    def _evaluate_baseline_candidate(self, base_candidate: StrategyCandidate) -> None:
        """ベース戦略を1回評価し、非退行ガードの基準値を保存する。"""
        self._baseline_score = None
        self._baseline_total_return = None

        try:
            entry_signal_params = SignalParams(**base_candidate.entry_filter_params)
            exit_signal_params = SignalParams(**base_candidate.exit_trigger_params)
            with data_access_mode_context("direct"):
                if self._shared_config is not None:
                    shared_config = self._shared_config
                else:
                    shared_config = SharedConfig(**(self.shared_config_dict or {}))
                    self._shared_config = shared_config

                strategy = YamlConfigurableStrategy(
                    shared_config=shared_config,
                    entry_filter_params=entry_signal_params,
                    exit_trigger_params=exit_signal_params,
                )

                if self._prefetched_multi_data is not None:
                    strategy.multi_data_dict = self._prefetched_multi_data
                if self._prefetched_benchmark_data is not None:
                    strategy.benchmark_data = self._prefetched_benchmark_data

                _, portfolio, _, _, _ = strategy.run_optimized_backtest_kelly(
                    kelly_fraction=shared_config.kelly_fraction,
                    min_allocation=shared_config.min_allocation,
                    max_allocation=shared_config.max_allocation,
                )

            sharpe, calmar, total_return = self._extract_metrics(portfolio)
            self._baseline_total_return = total_return
            self._baseline_score = self._calculate_weighted_score(
                sharpe,
                calmar,
                total_return,
            )
            logger.info(
                "Optuna baseline evaluated: "
                f"score={self._baseline_score:.4f}, total_return={total_return:.4f}"
            )
        except Exception as e:
            logger.warning(f"Failed to evaluate baseline strategy for guardrail: {e}")

    def _build_optuna_param_dict(
        self,
        entry_params: dict[str, Any],
        exit_params: dict[str, Any],
    ) -> dict[str, Any]:
        """ベース戦略パラメータを Optuna enqueue 用フォーマットへ変換する。"""
        flattened: dict[str, Any] = {}
        self._collect_optuna_params("entry", entry_params, flattened)
        self._collect_optuna_params("exit", exit_params, flattened)
        return flattened

    def _collect_optuna_params(
        self,
        usage_type: str,
        base_params: dict[str, Any],
        flattened: dict[str, Any],
    ) -> None:
        for signal_name, params in base_params.items():
            if not isinstance(params, dict):
                continue
            if not self._is_signal_optimization_allowed(signal_name, usage_type):
                continue
            ranges = self.PARAM_RANGES.get(signal_name, {})
            self._collect_nested_optuna_params(
                usage_type=usage_type,
                signal_name=signal_name,
                params=params,
                ranges=ranges,
                flattened=flattened,
            )

    def _collect_nested_optuna_params(
        self,
        usage_type: str,
        signal_name: str,
        params: dict[str, Any],
        ranges: dict[str, tuple[float, float, ParamType]],
        flattened: dict[str, Any],
        prefix: str = "",
    ) -> None:
        for key, value in params.items():
            param_name = f"{prefix}.{key}" if prefix else key
            if key in CATEGORICAL_PARAMS or param_name in CATEGORICAL_PARAMS:
                continue

            if isinstance(value, dict):
                self._collect_nested_optuna_params(
                    usage_type=usage_type,
                    signal_name=signal_name,
                    params=value,
                    ranges=ranges,
                    flattened=flattened,
                    prefix=param_name,
                )
                continue

            if param_name not in ranges or isinstance(value, bool):
                continue

            suggest_suffix = param_name.replace(".", "__")
            suggest_name = f"{usage_type}_{signal_name}_{suggest_suffix}"
            flattened[suggest_name] = value

    def _build_param_specs(
        self,
        entry_params: dict[str, Any],
        exit_params: dict[str, Any],
    ) -> dict[str, tuple[float, float, ParamType]]:
        """最適化対象パラメータの suggest_name -> range/spec マップを構築する。"""
        specs: dict[str, tuple[float, float, ParamType]] = {}
        self._collect_param_specs("entry", entry_params, specs)
        self._collect_param_specs("exit", exit_params, specs)
        return specs

    def _collect_param_specs(
        self,
        usage_type: str,
        base_params: dict[str, Any],
        specs: dict[str, tuple[float, float, ParamType]],
    ) -> None:
        for signal_name, params in base_params.items():
            if not isinstance(params, dict):
                continue
            if not self._is_signal_optimization_allowed(signal_name, usage_type):
                continue
            ranges = self.PARAM_RANGES.get(signal_name, {})
            self._collect_nested_param_specs(
                usage_type=usage_type,
                signal_name=signal_name,
                params=params,
                ranges=ranges,
                specs=specs,
            )

    def _collect_nested_param_specs(
        self,
        usage_type: str,
        signal_name: str,
        params: dict[str, Any],
        ranges: dict[str, tuple[float, float, ParamType]],
        specs: dict[str, tuple[float, float, ParamType]],
        prefix: str = "",
    ) -> None:
        for key, value in params.items():
            param_name = f"{prefix}.{key}" if prefix else key
            if key in CATEGORICAL_PARAMS or param_name in CATEGORICAL_PARAMS:
                continue

            if isinstance(value, dict):
                self._collect_nested_param_specs(
                    usage_type=usage_type,
                    signal_name=signal_name,
                    params=value,
                    ranges=ranges,
                    specs=specs,
                    prefix=param_name,
                )
                continue

            if param_name not in ranges or isinstance(value, bool):
                continue

            suggest_suffix = param_name.replace(".", "__")
            suggest_name = f"{usage_type}_{signal_name}_{suggest_suffix}"
            specs[suggest_name] = ranges[param_name]

    def _load_base_strategy(
        self, base_strategy: str | StrategyCandidate
    ) -> StrategyCandidate:
        """
        ベース戦略をロード

        Args:
            base_strategy: 戦略名またはStrategyCandidate

        Returns:
            StrategyCandidate
        """
        if isinstance(base_strategy, StrategyCandidate):
            return base_strategy

        # 戦略名からロード
        config_loader = ConfigLoader()
        strategy_config = config_loader.load_strategy_config(base_strategy)

        # shared_configをマージ
        if self.shared_config_dict is None:
            self.shared_config_dict = config_loader.merge_shared_config(strategy_config)

        return StrategyCandidate(
            strategy_id=f"base_{base_strategy}",
            entry_filter_params=strategy_config.get("entry_filter_params", {}),
            exit_trigger_params=strategy_config.get("exit_trigger_params", {}),
            shared_config=strategy_config.get("shared_config", {}),
            metadata={"base_strategy": base_strategy},
        )

    def _create_sampler(self) -> BaseSampler:
        """
        サンプラーを作成

        Returns:
            Optunaサンプラー
        """
        if not OPTUNA_AVAILABLE:
            raise ImportError("Optuna is not available")
        seed = self.config.seed
        if TPESampler is None or RandomSampler is None or CmaEsSampler is None:
            raise ImportError("Optuna samplers are unavailable")
        if self.config.sampler == "tpe":
            return cast(Any, TPESampler(seed=seed))
        elif self.config.sampler == "random":
            return cast(Any, RandomSampler(seed=seed))
        elif self.config.sampler == "cmaes":
            return cast(Any, CmaEsSampler(seed=seed))
        else:
            return cast(Any, TPESampler(seed=seed))

    def _create_pruner(self) -> BasePruner:
        """Pruner を作成する。"""
        if not OPTUNA_AVAILABLE:
            raise ImportError("Optuna is not available")
        if NopPruner is None or MedianPruner is None:
            raise ImportError("Optuna pruners are unavailable")
        if not self.config.pruning:
            return cast(Any, NopPruner())

        startup_trials = max(5, min(20, self.config.n_trials // 10))
        return cast(
            Any,
            MedianPruner(
                n_startup_trials=startup_trials,
                n_warmup_steps=0,
                interval_steps=1,
            ),
        )

    def _prepare_prefetched_data(self) -> None:
        """Trial 間で再利用する OHLCV / benchmark データを先読みする。"""
        self._prefetched_multi_data = None
        self._prefetched_benchmark_data = None

        if self.shared_config_dict is None:
            return

        try:
            with data_access_mode_context("direct"):
                shared_config = SharedConfig(**self.shared_config_dict)
                self._shared_config = shared_config

                probe_strategy = YamlConfigurableStrategy(
                    shared_config=shared_config,
                    entry_filter_params=SignalParams(**self.base_entry_params),
                    exit_trigger_params=SignalParams(**self.base_exit_params),
                )

                should_load_margin = bool(
                    cast(Any, probe_strategy)._should_load_margin_data()
                )
                should_load_statements = bool(
                    cast(Any, probe_strategy)._should_load_statements_data()
                )
                include_forecast_revision = bool(
                    cast(Any, probe_strategy)._should_include_forecast_revision()
                )
                period_type = cast(Any, probe_strategy)._resolve_period_type()

                self._prefetched_multi_data = prepare_multi_data(
                    dataset=shared_config.dataset,
                    stock_codes=shared_config.stock_codes,
                    start_date=shared_config.start_date,
                    end_date=shared_config.end_date,
                    include_margin_data=(
                        shared_config.include_margin_data and should_load_margin
                    ),
                    include_statements_data=(
                        shared_config.include_statements_data and should_load_statements
                    ),
                    timeframe=shared_config.timeframe,
                    period_type=period_type,
                    include_forecast_revision=include_forecast_revision,
                )

                if bool(cast(Any, probe_strategy)._should_load_benchmark()):
                    self._prefetched_benchmark_data = load_topix_data(
                        shared_config.dataset,
                        shared_config.start_date,
                        shared_config.end_date,
                    )

                logger.info(
                    "Optuna prefetch completed: "
                    f"stocks={len(self._prefetched_multi_data or {})}, "
                    f"benchmark={'yes' if self._prefetched_benchmark_data is not None else 'no'}"
                )
        except Exception as e:
            logger.warning(f"Optuna prefetch skipped due to error: {e}")

    @staticmethod
    def _safe_metric(value: Any) -> float:
        """NaN/Inf を安全に 0.0 へフォールバックする。"""
        try:
            scalar = float(value)
        except (TypeError, ValueError):
            return 0.0
        return scalar if np.isfinite(scalar) else 0.0

    def _extract_metrics(self, portfolio: Any) -> tuple[float, float, float]:
        """ポートフォリオから評価メトリクスを抽出する。"""
        sharpe = self._safe_metric(portfolio.sharpe_ratio())
        calmar = self._safe_metric(portfolio.calmar_ratio())
        total_return = self._safe_metric(portfolio.total_return())
        return sharpe, calmar, total_return

    def _calculate_weighted_score(
        self,
        sharpe: float,
        calmar: float,
        total_return: float,
    ) -> float:
        """重み付きスコアを計算する。"""
        return calculate_weighted_score_from_metrics(
            {
                "sharpe_ratio": sharpe,
                "calmar_ratio": calmar,
                "total_return": total_return,
            },
            self.scoring_weights,
        )

    def _run_backtest_for_trial(
        self,
        strategy: YamlConfigurableStrategy,
        shared_config: SharedConfig,
        trial: optuna.Trial,
    ) -> Any:
        """1 trial 分のバックテストを実行し、最終ポートフォリオを返す。"""
        if not self.config.pruning:
            _, portfolio, _, _, _ = strategy.run_optimized_backtest_kelly(
                kelly_fraction=shared_config.kelly_fraction,
                min_allocation=shared_config.min_allocation,
                max_allocation=shared_config.max_allocation,
            )
            return portfolio

        # pruning 有効時は第1段階の暫定スコアで枝刈り判定
        run_multi_backtest = cast(Any, strategy).run_multi_backtest
        initial_portfolio, _ = run_multi_backtest()
        if strategy.group_by:
            strategy.combined_portfolio = initial_portfolio
        else:
            strategy.portfolio = initial_portfolio

        provisional_sharpe, provisional_calmar, provisional_return = self._extract_metrics(
            initial_portfolio
        )
        provisional_score = self._calculate_weighted_score(
            provisional_sharpe,
            provisional_calmar,
            provisional_return,
        )
        trial.report(provisional_score, step=0)
        trial.set_user_attr("provisional_score", provisional_score)

        should_prune = trial.should_prune()
        if isinstance(should_prune, bool) and should_prune:
            logger.info(
                f"Trial {trial.number} pruned at stage-1: "
                f"provisional_score={provisional_score:.4f}"
            )
            optuna_rt = cast(Any, optuna_runtime)
            raise optuna_rt.TrialPruned()

        optimize_allocation_kelly = cast(Any, strategy).optimize_allocation_kelly
        optimized_allocation, _ = optimize_allocation_kelly(
            initial_portfolio,
            kelly_fraction=shared_config.kelly_fraction,
            min_allocation=shared_config.min_allocation,
            max_allocation=shared_config.max_allocation,
        )

        if strategy.group_by and hasattr(strategy, "run_multi_backtest_from_cached_signals"):
            try:
                run_cached = cast(Any, strategy).run_multi_backtest_from_cached_signals
                return run_cached(
                    optimized_allocation
                )
            except Exception as e:
                logger.debug(
                    "Failed to reuse cached grouped signals; fallback to regular run: "
                    f"{e}"
                )

        portfolio, _ = run_multi_backtest(allocation_pct=optimized_allocation)
        return portfolio

    def _objective(self, trial: optuna.Trial) -> float:
        """
        Optuna目的関数

        Args:
            trial: Optunaトライアル

        Returns:
            最適化スコア
        """
        try:
            # パラメータをサンプリング
            entry_params = self._sample_params(trial, self.base_entry_params, "entry")
            exit_params = self._sample_params(trial, self.base_exit_params, "exit")

            # SignalParams構築
            entry_signal_params = SignalParams(**entry_params)
            exit_signal_params = SignalParams(**exit_params)

            with data_access_mode_context("direct"):
                # SharedConfig構築（optimize 開始時のコンテキストを再利用）
                if self._shared_config is not None:
                    shared_config = self._shared_config
                else:
                    shared_config = SharedConfig(**(self.shared_config_dict or {}))
                    self._shared_config = shared_config

                # 戦略インスタンス作成
                strategy = YamlConfigurableStrategy(
                    shared_config=shared_config,
                    entry_filter_params=entry_signal_params,
                    exit_trigger_params=exit_signal_params,
                )

                if self._prefetched_multi_data is not None:
                    strategy.multi_data_dict = self._prefetched_multi_data
                if self._prefetched_benchmark_data is not None:
                    strategy.benchmark_data = self._prefetched_benchmark_data

                portfolio = self._run_backtest_for_trial(strategy, shared_config, trial)

            # メトリクス抽出
            sharpe, calmar, total_return = self._extract_metrics(portfolio)
            score = self._calculate_weighted_score(sharpe, calmar, total_return)

            # 中間結果をログ
            trial.set_user_attr("sharpe_ratio", sharpe)
            trial.set_user_attr("calmar_ratio", calmar)
            trial.set_user_attr("total_return", total_return)

            return score

        except Exception as e:
            optuna_rt = cast(Any, optuna_runtime)
            if OPTUNA_AVAILABLE and isinstance(e, optuna_rt.TrialPruned):
                raise
            logger.warning(f"Trial {trial.number} failed: {e}")
            return -999.0

    def _sample_params(
        self,
        trial: optuna.Trial,
        base_params: dict[str, Any],
        usage_type: str,
    ) -> dict[str, Any]:
        """
        パラメータをサンプリング

        Args:
            trial: Optunaトライアル
            base_params: ベースパラメータ
            usage_type: "entry" or "exit"

        Returns:
            サンプリングされたパラメータ
        """
        sampled = {}

        for signal_name, params in base_params.items():
            if not isinstance(params, dict):
                continue

            sampled_signal = copy.deepcopy(params)
            if not self._is_signal_optimization_allowed(signal_name, usage_type):
                sampled[signal_name] = sampled_signal
                continue

            ranges = self.PARAM_RANGES.get(signal_name, {})
            self._sample_nested_params(
                trial=trial,
                usage_type=usage_type,
                signal_name=signal_name,
                params=sampled_signal,
                ranges=ranges,
            )

            sampled[signal_name] = sampled_signal

        return sampled

    def _sample_nested_params(
        self,
        trial: optuna.Trial,
        usage_type: str,
        signal_name: str,
        params: dict[str, Any],
        ranges: dict[str, tuple[float, float, ParamType]],
        prefix: str = "",
    ) -> None:
        for key, value in params.items():
            param_name = f"{prefix}.{key}" if prefix else key

            if key in CATEGORICAL_PARAMS or param_name in CATEGORICAL_PARAMS:
                continue

            if isinstance(value, dict):
                self._sample_nested_params(
                    trial=trial,
                    usage_type=usage_type,
                    signal_name=signal_name,
                    params=value,
                    ranges=ranges,
                    prefix=param_name,
                )
                continue

            if param_name not in ranges or isinstance(value, bool):
                continue

            min_val, max_val, param_type = ranges[param_name]
            suggest_suffix = param_name.replace(".", "__")
            suggest_name = f"{usage_type}_{signal_name}_{suggest_suffix}"
            if suggest_name in self._active_param_overrides:
                min_val, max_val, param_type = self._active_param_overrides[suggest_name]

            min_val, max_val = self._apply_param_dependency_constraints(
                key=key,
                min_val=min_val,
                max_val=max_val,
                param_type=param_type,
                sibling_params=params,
            )

            if param_type == "int":
                min_int = int(np.ceil(min_val))
                max_int = int(np.floor(max_val))
                if min_int > max_int:
                    min_int = max_int
                params[key] = trial.suggest_int(suggest_name, min_int, max_int)
            else:
                min_float = float(min_val)
                max_float = float(max_val)
                if min_float >= max_float:
                    max_float = float(np.nextafter(min_float, float("inf")))
                params[key] = trial.suggest_float(suggest_name, min_float, max_float)

    def _apply_param_dependency_constraints(
        self,
        key: str,
        min_val: float,
        max_val: float,
        param_type: ParamType,
        sibling_params: dict[str, Any],
    ) -> tuple[float, float]:
        """相互依存パラメータの制約を sampling range に反映する。"""
        lower_bound = min_val
        upper_bound = max_val

        if key == "long_period" and "short_period" in sibling_params:
            lower_bound = max(lower_bound, float(sibling_params["short_period"]) + 1.0)
        elif key == "short_period" and "long_period" in sibling_params:
            upper_bound = min(upper_bound, float(sibling_params["long_period"]) - 1.0)
        elif key == "slow_period" and "fast_period" in sibling_params:
            lower_bound = max(lower_bound, float(sibling_params["fast_period"]) + 1.0)
        elif key == "fast_period" and "slow_period" in sibling_params:
            upper_bound = min(upper_bound, float(sibling_params["slow_period"]) - 1.0)
        elif key == "max_threshold" and "min_threshold" in sibling_params:
            lower_bound = max(lower_bound, float(sibling_params["min_threshold"]) + 1e-6)
        elif key == "min_threshold" and "max_threshold" in sibling_params:
            upper_bound = min(upper_bound, float(sibling_params["max_threshold"]) - 1e-6)
        elif key == "max_beta" and "min_beta" in sibling_params:
            lower_bound = max(lower_bound, float(sibling_params["min_beta"]) + 1e-6)
        elif key == "min_beta" and "max_beta" in sibling_params:
            upper_bound = min(upper_bound, float(sibling_params["max_beta"]) - 1e-6)

        if param_type == "int":
            lower_bound = float(int(np.ceil(lower_bound)))
            upper_bound = float(int(np.floor(upper_bound)))
            if lower_bound > upper_bound:
                lower_bound = upper_bound
            return lower_bound, upper_bound

        if lower_bound >= upper_bound:
            upper_bound = float(np.nextafter(lower_bound, float("inf")))
        return lower_bound, upper_bound

    def _is_signal_optimization_allowed(self, signal_name: str, usage_type: str) -> bool:
        """制約に基づいて最適化対象に含めるか判定する。"""
        if not self._is_usage_targeted(usage_type):
            return False
        return is_signal_allowed(signal_name, self.allowed_category_set)

    def _build_candidate_from_params(
        self, params: dict[str, Any]
    ) -> StrategyCandidate:
        """
        Optunaパラメータから戦略候補を構築

        Args:
            params: Optunaの最良パラメータ

        Returns:
            戦略候補
        """
        entry_params = {}
        exit_params = {}

        # ベースパラメータをコピー
        for signal_name, signal_params in self.base_entry_params.items():
            if isinstance(signal_params, dict):
                entry_params[signal_name] = copy.deepcopy(signal_params)

        for signal_name, signal_params in self.base_exit_params.items():
            if isinstance(signal_params, dict):
                exit_params[signal_name] = copy.deepcopy(signal_params)

        # Optunaパラメータで上書き
        for param_name, value in params.items():
            parts = param_name.split("_", 2)  # entry_signal_param
            if len(parts) == 3:
                usage_type = parts[0]
                signal_name = parts[1]
                signal_param = "_".join(parts[2:])

                # signal_nameに複数の_が含まれる場合を処理
                # (例: entry_trading_value_range_period)
                for known_signal in sorted(
                    self.PARAM_RANGES.keys(), key=len, reverse=True
                ):
                    if param_name.startswith(f"{usage_type}_{known_signal}_"):
                        signal_name = known_signal
                        signal_param = param_name[len(f"{usage_type}_{known_signal}_"):]
                        break

                signal_param = signal_param.replace("__", ".")

                if usage_type == "entry" and signal_name in entry_params:
                    self._set_nested_param(entry_params[signal_name], signal_param, value)
                elif usage_type == "exit" and signal_name in exit_params:
                    self._set_nested_param(exit_params[signal_name], signal_param, value)

        return StrategyCandidate(
            strategy_id="optuna_best",
            entry_filter_params=entry_params,
            exit_trigger_params=exit_params,
            shared_config=copy.deepcopy(self.base_shared_config),
            metadata={"optimization_method": "optuna"},
        )

    def _set_nested_param(
        self,
        params: dict[str, Any],
        path: str,
        value: Any,
    ) -> None:
        parts = path.split(".")
        current = params

        for part in parts[:-1]:
            child = current.get(part)
            if not isinstance(child, dict):
                child = {}
                current[part] = child
            current = child

        current[parts[-1]] = value

    def get_optimization_history(self, study: optuna.Study) -> list[dict[str, Any]]:
        """
        最適化履歴を取得

        Args:
            study: Optuna Study

        Returns:
            トライアルごとの結果リスト
        """
        history = []
        optuna_rt = cast(Any, optuna_runtime)
        for trial in study.trials:
            if trial.state == optuna_rt.trial.TrialState.COMPLETE:
                history.append(
                    {
                        "trial": trial.number,
                        "score": trial.value,
                        "sharpe_ratio": trial.user_attrs.get("sharpe_ratio", 0),
                        "calmar_ratio": trial.user_attrs.get("calmar_ratio", 0),
                        "total_return": trial.user_attrs.get("total_return", 0),
                        "params": trial.params,
                    }
                )
        return history


def _extract_enabled_signal_names(params_dict: dict[str, Any]) -> set[str]:
    enabled: set[str] = set()
    for name, value in (params_dict or {}).items():
        if not isinstance(value, dict):
            enabled.add(name)
            continue
        if value.get("enabled", True):
            enabled.add(name)
    return enabled
