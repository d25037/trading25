"""
Optunaによるベイズ最適化モジュール

TPE（Tree-structured Parzen Estimator）を使用した効率的なパラメータ探索
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from loguru import logger

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

from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.lib.strategy_runtime.loader import ConfigLoader

from .models import OptunaConfig, StrategyCandidate


class OptunaOptimizer:
    """
    Optunaによるベイズ最適化

    TPEサンプラーを使用してパラメータ空間を効率的に探索
    """

    # パラメータ範囲定義（ParameterEvolverと同じ）
    PARAM_RANGES: dict[str, dict[str, tuple[float, float, str]]] = {
        "period_breakout": {
            "period": (20, 500, "int"),
            "lookback_days": (1, 30, "int"),
        },
        "ma_breakout": {
            "period": (20, 500, "int"),
            "lookback_days": (1, 30, "int"),
        },
        "crossover": {
            "fast_period": (5, 50, "int"),
            "slow_period": (20, 200, "int"),
            "signal_period": (5, 20, "int"),
            "lookback_days": (1, 10, "int"),
        },
        "mean_reversion": {
            "baseline_period": (10, 100, "int"),
            "deviation_threshold": (0.05, 0.5, "float"),
        },
        "bollinger_bands": {
            "window": (10, 100, "int"),
            "alpha": (1.0, 4.0, "float"),
        },
        "atr_support_break": {
            "lookback_period": (10, 100, "int"),
            "atr_multiplier": (1.0, 10.0, "float"),
        },
        "rsi_threshold": {
            "period": (5, 50, "int"),
            "threshold": (10.0, 90.0, "float"),
        },
        "rsi_spread": {
            "fast_period": (5, 20, "int"),
            "slow_period": (10, 50, "int"),
            "threshold": (5.0, 30.0, "float"),
        },
        "volume": {
            "threshold": (0.3, 3.0, "float"),
            "short_period": (10, 100, "int"),
            "long_period": (50, 300, "int"),
        },
        "trading_value": {
            "period": (5, 50, "int"),
            "threshold_value": (0.1, 100.0, "float"),
        },
        "trading_value_range": {
            "period": (5, 50, "int"),
            "min_threshold": (0.1, 50.0, "float"),
            "max_threshold": (10.0, 500.0, "float"),
        },
        "beta": {
            "lookback_period": (20, 200, "int"),
            "min_beta": (-1.0, 2.0, "float"),
            "max_beta": (0.5, 5.0, "float"),
        },
        "margin": {
            "lookback_period": (50, 300, "int"),
            "percentile_threshold": (0.1, 0.9, "float"),
        },
        "index_daily_change": {
            "max_daily_change_pct": (0.5, 3.0, "float"),
        },
        "index_macd_histogram": {
            "fast_period": (5, 20, "int"),
            "slow_period": (15, 50, "int"),
            "signal_period": (5, 15, "int"),
        },
    }

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

        # ベース戦略パラメータ（後で設定）
        self.base_entry_params: dict[str, Any] = {}
        self.base_exit_params: dict[str, Any] = {}

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
        self.base_entry_params = base_candidate.entry_filter_params
        self.base_exit_params = base_candidate.exit_trigger_params

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
            n_jobs=1,  # バックテストは内部で並列化しているため
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

            sampled_signal = params.copy()
            ranges = self.PARAM_RANGES.get(signal_name, {})

            # カテゴリカルパラメータ（スキップ対象）
            categorical_params = [
                "enabled", "direction", "condition", "type", "ma_type",
                "position", "baseline_type", "recovery_price",
                "recovery_direction", "deviation_direction", "price_column",
            ]

            for param_name in params.keys():
                if param_name in categorical_params:
                    # カテゴリカルパラメータはそのまま
                    continue

                if param_name in ranges:
                    min_val, max_val, param_type = ranges[param_name]
                    suggest_name = f"{usage_type}_{signal_name}_{param_name}"

                    if param_type == "int":
                        sampled_signal[param_name] = trial.suggest_int(
                            suggest_name, int(min_val), int(max_val)
                        )
                    else:
                        sampled_signal[param_name] = trial.suggest_float(
                            suggest_name, min_val, max_val
                        )

            sampled[signal_name] = sampled_signal

        return sampled

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
                entry_params[signal_name] = signal_params.copy()

        for signal_name, signal_params in self.base_exit_params.items():
            if isinstance(signal_params, dict):
                exit_params[signal_name] = signal_params.copy()

        # Optunaパラメータで上書き
        for param_name, value in params.items():
            parts = param_name.split("_", 2)  # entry_signal_param
            if len(parts) == 3:
                usage_type = parts[0]
                signal_name = parts[1]
                signal_param = "_".join(parts[2:])

                # signal_nameに複数の_が含まれる場合を処理
                # (例: entry_trading_value_range_period)
                for known_signal in self.PARAM_RANGES.keys():
                    if param_name.startswith(f"{usage_type}_{known_signal}_"):
                        signal_name = known_signal
                        signal_param = param_name[len(f"{usage_type}_{known_signal}_"):]
                        break

                if usage_type == "entry" and signal_name in entry_params:
                    entry_params[signal_name][signal_param] = value
                elif usage_type == "exit" and signal_name in exit_params:
                    exit_params[signal_name][signal_param] = value

        return StrategyCandidate(
            strategy_id="optuna_best",
            entry_filter_params=entry_params,
            exit_trigger_params=exit_params,
            shared_config={},
            metadata={"optimization_method": "optuna"},
        )

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
