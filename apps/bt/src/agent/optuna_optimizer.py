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

# ランタイムではtry-exceptでインポート
try:
    import optuna as optuna_runtime
    from optuna.pruners import MedianPruner, NopPruner
    from optuna.samplers import CmaEsSampler, RandomSampler, TPESampler

    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False
    optuna_runtime = None  # type: ignore

from src.data.access.mode import data_access_mode_context
from src.data.loaders.data_preparation import prepare_multi_data
from src.data.loaders.index_loaders import load_topix_data
from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.lib.strategy_runtime.loader import ConfigLoader

from .models import OptunaConfig, SignalCategory, StrategyCandidate
from .signal_filters import is_signal_allowed
from .signal_augmentation import apply_random_add_structure
from .signal_search_space import CATEGORICAL_PARAMS, PARAM_RANGES, ParamType


class OptunaOptimizer:
    """
    Optunaによるベイズ最適化

    TPEサンプラーを使用してパラメータ空間を効率的に探索
    """

    # Backward-compatible alias.
    PARAM_RANGES = PARAM_RANGES

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
        if self.shared_config_dict is None:
            self.shared_config_dict = {}
        self._prepare_prefetched_data()

        logger.info(
            f"Starting Optuna optimization: n_trials={self.config.n_trials}, "
            f"sampler={self.config.sampler}"
        )

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

        # 最適化実行
        study.optimize(
            self._objective,
            n_trials=self.config.n_trials,
            n_jobs=self.config.n_jobs,
            show_progress_bar=True,
            callbacks=callbacks,
        )

        # 最良パラメータで戦略候補を構築
        best_params = study.best_params
        best_candidate = self._build_candidate_from_params(best_params)

        logger.info(
            f"Optimization complete: best_score={study.best_value:.4f}, "
            f"n_trials={len(study.trials)}"
        )

        return best_candidate, study

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
        if self.config.sampler == "tpe":
            return TPESampler()  # type: ignore[possibly-unbound]
        elif self.config.sampler == "random":
            return RandomSampler()  # type: ignore[possibly-unbound]
        elif self.config.sampler == "cmaes":
            return CmaEsSampler()  # type: ignore[possibly-unbound]
        else:
            return TPESampler()  # type: ignore[possibly-unbound]

    def _create_pruner(self) -> BasePruner:
        """Pruner を作成する。"""
        if not OPTUNA_AVAILABLE:
            raise ImportError("Optuna is not available")
        if not self.config.pruning:
            return NopPruner()  # type: ignore[possibly-unbound]

        startup_trials = max(5, min(20, self.config.n_trials // 10))
        return MedianPruner(  # type: ignore[possibly-unbound]
            n_startup_trials=startup_trials,
            n_warmup_steps=0,
            interval_steps=1,
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
        return float(value) if pd.notna(value) and np.isfinite(value) else 0.0

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
        score = 0.0
        if "sharpe_ratio" in self.scoring_weights:
            score += self.scoring_weights["sharpe_ratio"] * sharpe
        if "calmar_ratio" in self.scoring_weights:
            score += self.scoring_weights["calmar_ratio"] * calmar
        if "total_return" in self.scoring_weights:
            score += self.scoring_weights["total_return"] * total_return
        return score

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

            if param_type == "int":
                params[key] = trial.suggest_int(
                    suggest_name, int(min_val), int(max_val)
                )
            else:
                params[key] = trial.suggest_float(
                    suggest_name, min_val, max_val
                )

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
