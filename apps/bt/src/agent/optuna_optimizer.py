"""
Optunaによるベイズ最適化モジュール

TPE（Tree-structured Parzen Estimator）を使用した効率的なパラメータ探索
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import copy
import numpy as np
import pandas as pd
from loguru import logger
import random

# 型チェック時のみoptunaをインポート
if TYPE_CHECKING:
    import optuna
    from optuna.samplers import BaseSampler

# ランタイムではtry-exceptでインポート
try:
    import optuna as optuna_runtime
    from optuna.samplers import CmaEsSampler, RandomSampler, TPESampler

    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False
    optuna_runtime = None  # type: ignore

from src.data.access.mode import data_access_mode_context
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
            )
            logger.info(
                "Optuna random-add applied: "
                f"entry_added={added['entry']}, exit_added={added['exit']}"
            )

        self.base_entry_params = base_candidate.entry_filter_params
        self.base_exit_params = base_candidate.exit_trigger_params
        self.base_shared_config = base_candidate.shared_config or {}

        logger.info(
            f"Starting Optuna optimization: n_trials={self.config.n_trials}, "
            f"sampler={self.config.sampler}"
        )

        # サンプラー選択
        sampler = self._create_sampler()

        # Study作成
        study = optuna_runtime.create_study(
            study_name=self.config.study_name,
            storage=f"sqlite:///{self.config.storage_path}"
            if self.config.storage_path
            else None,
            direction="maximize",
            sampler=sampler,
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
                    if t.state == optuna_runtime.trial.TrialState.COMPLETE
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
                # SharedConfig構築
                shared_config = SharedConfig(**self.shared_config_dict)

                # 戦略インスタンス作成
                strategy = YamlConfigurableStrategy(
                    shared_config=shared_config,
                    entry_filter_params=entry_signal_params,
                    exit_trigger_params=exit_signal_params,
                )

                # Kelly基準バックテスト実行
                _, portfolio, _, _, _ = strategy.run_optimized_backtest_kelly(
                    kelly_fraction=shared_config.kelly_fraction,
                    min_allocation=shared_config.min_allocation,
                    max_allocation=shared_config.max_allocation,
                )

            # メトリクス抽出
            sharpe = portfolio.sharpe_ratio()
            calmar = portfolio.calmar_ratio()
            total_return = portfolio.total_return()

            # NaN/Infチェック
            sharpe = float(sharpe) if pd.notna(sharpe) and np.isfinite(sharpe) else 0.0
            calmar = float(calmar) if pd.notna(calmar) and np.isfinite(calmar) else 0.0
            total_return = (
                float(total_return)
                if pd.notna(total_return) and np.isfinite(total_return)
                else 0.0
            )

            # 複合スコア計算
            score = 0.0
            if "sharpe_ratio" in self.scoring_weights:
                score += self.scoring_weights["sharpe_ratio"] * sharpe
            if "calmar_ratio" in self.scoring_weights:
                score += self.scoring_weights["calmar_ratio"] * calmar
            if "total_return" in self.scoring_weights:
                score += self.scoring_weights["total_return"] * total_return

            # 中間結果をログ
            trial.set_user_attr("sharpe_ratio", sharpe)
            trial.set_user_attr("calmar_ratio", calmar)
            trial.set_user_attr("total_return", total_return)

            return score

        except Exception as e:
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
        for trial in study.trials:
            if trial.state == optuna_runtime.trial.TrialState.COMPLETE:
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
