"""
遺伝的アルゴリズムによるパラメータ最適化モジュール

戦略パラメータを進化的に最適化
"""

import copy
import random
from typing import Any

from loguru import logger

from src.lib.strategy_runtime.loader import ConfigLoader

from .models import EvolutionConfig, EvaluationResult, StrategyCandidate
from .strategy_evaluator import StrategyEvaluator


class ParameterEvolver:
    """
    遺伝的アルゴリズムによるパラメータ最適化

    戦略のパラメータを進化的に最適化し、最良の組み合わせを探索
    """

    # パラメータ範囲定義（各シグナルパラメータの有効範囲）
    PARAM_RANGES: dict[str, dict[str, tuple[float, float, str]]] = {
        # シグナル名: {パラメータ名: (min, max, type)}
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
        config: EvolutionConfig | None = None,
        shared_config_dict: dict[str, Any] | None = None,
        scoring_weights: dict[str, float] | None = None,
    ):
        """
        初期化

        Args:
            config: 進化設定
            shared_config_dict: 共有設定辞書
            scoring_weights: スコアリング重み
        """
        self.config = config or EvolutionConfig()
        self.shared_config_dict = shared_config_dict
        self.scoring_weights = scoring_weights

        # 乱数シード設定
        random.seed(42)

        # 評価器
        self.evaluator = StrategyEvaluator(
            shared_config_dict=shared_config_dict,
            scoring_weights=scoring_weights,
            n_jobs=self.config.n_jobs,
            timeout_seconds=self.config.timeout_seconds,
        )

        # 進化履歴
        self.history: list[dict[str, Any]] = []

    def evolve(
        self,
        base_strategy: str | StrategyCandidate,
    ) -> tuple[StrategyCandidate, list[EvaluationResult]]:
        """
        遺伝的アルゴリズムで戦略パラメータを最適化

        Args:
            base_strategy: ベース戦略名またはStrategyCandidate

        Returns:
            (最良戦略候補, 全世代の評価結果リスト)
        """
        # ベース戦略の読み込み
        base_candidate = self._load_base_strategy(base_strategy)

        logger.info(
            f"Starting evolution: population={self.config.population_size}, "
            f"generations={self.config.generations}"
        )

        # 初期集団生成
        population = self._initialize_population(base_candidate)

        # 最良個体追跡
        best_result: EvaluationResult | None = None
        all_results: list[EvaluationResult] = []

        for generation in range(self.config.generations):
            logger.info(f"Generation {generation + 1}/{self.config.generations}")

            # 評価
            results = self.evaluator.evaluate_batch(population)
            all_results.extend(results)

            # 成功した結果のみ抽出
            successful = [r for r in results if r.success]

            if not successful:
                logger.warning(
                    f"Generation {generation + 1}: No successful evaluations"
                )
                continue

            # 最良個体更新
            gen_best = max(successful, key=lambda x: x.score)
            if best_result is None or gen_best.score > best_result.score:
                best_result = gen_best
                logger.info(
                    f"New best: score={gen_best.score:.4f}, "
                    f"sharpe={gen_best.sharpe_ratio:.4f}, "
                    f"strategy={gen_best.candidate.strategy_id}"
                )

            # 履歴記録
            self.history.append(
                {
                    "generation": generation + 1,
                    "best_score": gen_best.score,
                    "avg_score": sum(r.score for r in successful) / len(successful),
                    "population_size": len(successful),
                }
            )

            # 最終世代でなければ次世代を生成
            if generation < self.config.generations - 1:
                population = self._evolve_population(successful)

        if best_result is None:
            raise RuntimeError("Evolution failed: no successful evaluations")

        logger.info(
            f"Evolution complete: best_score={best_result.score:.4f}, "
            f"sharpe={best_result.sharpe_ratio:.4f}"
        )

        return best_result.candidate, all_results

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
            self.evaluator.shared_config_dict = self.shared_config_dict

        return StrategyCandidate(
            strategy_id=f"base_{base_strategy}",
            entry_filter_params=strategy_config.get("entry_filter_params", {}),
            exit_trigger_params=strategy_config.get("exit_trigger_params", {}),
            shared_config=strategy_config.get("shared_config", {}),
            metadata={"base_strategy": base_strategy},
        )

    def _initialize_population(
        self, base_candidate: StrategyCandidate
    ) -> list[StrategyCandidate]:
        """
        初期集団を生成

        Args:
            base_candidate: ベース戦略候補

        Returns:
            初期集団（population_size個）
        """
        population: list[StrategyCandidate] = []

        # ベース戦略をそのまま1個追加
        population.append(base_candidate)

        # 残りは変異させて生成
        for i in range(self.config.population_size - 1):
            mutated = self._mutate(base_candidate, mutation_strength=0.3)
            mutated.strategy_id = f"init_{i}"
            mutated.metadata["generation"] = 0
            population.append(mutated)

        return population

    def _evolve_population(
        self, results: list[EvaluationResult]
    ) -> list[StrategyCandidate]:
        """
        次世代を生成

        Args:
            results: 現世代の評価結果

        Returns:
            次世代集団
        """
        next_population: list[StrategyCandidate] = []

        # スコア順にソート
        sorted_results = sorted(results, key=lambda x: x.score, reverse=True)

        # エリート保存
        n_elite = max(1, int(len(sorted_results) * self.config.elite_ratio))
        for i, result in enumerate(sorted_results[:n_elite]):
            elite = copy.deepcopy(result.candidate)
            elite.strategy_id = f"elite_{i}"
            elite.metadata["elite"] = True
            next_population.append(elite)

        # 残りを交叉・突然変異で生成
        while len(next_population) < self.config.population_size:
            # トーナメント選択で2親を選択
            parent1 = self._tournament_select(sorted_results)
            parent2 = self._tournament_select(sorted_results)

            # 交叉
            if random.random() < self.config.crossover_rate:
                child = self._crossover(parent1.candidate, parent2.candidate)
            else:
                child = copy.deepcopy(parent1.candidate)

            # 突然変異
            if random.random() < self.config.mutation_rate:
                child = self._mutate(child)

            child.strategy_id = f"gen_{len(next_population)}"
            child.metadata["elite"] = False
            next_population.append(child)

        return next_population

    def _tournament_select(
        self, results: list[EvaluationResult]
    ) -> EvaluationResult:
        """
        トーナメント選択

        Args:
            results: 評価結果リスト

        Returns:
            選択された個体
        """
        tournament = random.sample(
            results, min(self.config.tournament_size, len(results))
        )
        return max(tournament, key=lambda x: x.score)

    def _crossover(
        self, parent1: StrategyCandidate, parent2: StrategyCandidate
    ) -> StrategyCandidate:
        """
        一様交叉

        Args:
            parent1: 親1
            parent2: 親2

        Returns:
            子個体
        """
        child_entry = {}
        child_exit = {}

        # Entry params の交叉
        all_entry_signals = set(parent1.entry_filter_params.keys()) | set(
            parent2.entry_filter_params.keys()
        )
        for signal in all_entry_signals:
            if random.random() < 0.5:
                if signal in parent1.entry_filter_params:
                    child_entry[signal] = copy.deepcopy(
                        parent1.entry_filter_params[signal]
                    )
            else:
                if signal in parent2.entry_filter_params:
                    child_entry[signal] = copy.deepcopy(
                        parent2.entry_filter_params[signal]
                    )

        # Exit params の交叉
        all_exit_signals = set(parent1.exit_trigger_params.keys()) | set(
            parent2.exit_trigger_params.keys()
        )
        for signal in all_exit_signals:
            if random.random() < 0.5:
                if signal in parent1.exit_trigger_params:
                    child_exit[signal] = copy.deepcopy(
                        parent1.exit_trigger_params[signal]
                    )
            else:
                if signal in parent2.exit_trigger_params:
                    child_exit[signal] = copy.deepcopy(
                        parent2.exit_trigger_params[signal]
                    )

        return StrategyCandidate(
            strategy_id="child",
            entry_filter_params=child_entry,
            exit_trigger_params=child_exit,
            shared_config=parent1.shared_config,
            metadata={"crossover": True},
        )

    def _mutate(
        self,
        candidate: StrategyCandidate,
        mutation_strength: float = 0.2,
    ) -> StrategyCandidate:
        """
        突然変異（ガウシアン摂動）

        Args:
            candidate: 変異対象
            mutation_strength: 変異強度（0-1）

        Returns:
            変異後の個体
        """
        mutated = copy.deepcopy(candidate)

        # Entry params の変異
        for signal_name, params in mutated.entry_filter_params.items():
            if isinstance(params, dict):
                mutated.entry_filter_params[signal_name] = self._mutate_signal_params(
                    signal_name, params, mutation_strength
                )

        # Exit params の変異
        for signal_name, params in mutated.exit_trigger_params.items():
            if isinstance(params, dict):
                mutated.exit_trigger_params[signal_name] = self._mutate_signal_params(
                    signal_name, params, mutation_strength
                )

        mutated.metadata["mutated"] = True
        return mutated

    def _mutate_signal_params(
        self,
        signal_name: str,
        params: dict[str, Any],
        mutation_strength: float,
    ) -> dict[str, Any]:
        """
        シグナルパラメータを変異

        Args:
            signal_name: シグナル名
            params: パラメータ辞書
            mutation_strength: 変異強度

        Returns:
            変異後のパラメータ
        """
        mutated = params.copy()
        ranges = self.PARAM_RANGES.get(signal_name, {})

        # カテゴリカルパラメータ（スキップ対象）
        categorical_params = [
            "enabled", "direction", "condition", "type", "ma_type",
            "position", "baseline_type", "recovery_price",
            "recovery_direction", "deviation_direction", "price_column",
        ]

        for param_name, value in params.items():
            if param_name in categorical_params:
                # カテゴリカルパラメータはスキップ
                continue

            if param_name in ranges:
                min_val, max_val, param_type = ranges[param_name]

                if random.random() < mutation_strength:
                    if param_type == "int":
                        # ガウシアン摂動（整数）
                        delta = int((max_val - min_val) * 0.2 * random.gauss(0, 1))
                        new_value = int(value) + delta
                        new_value = max(int(min_val), min(int(max_val), new_value))
                        mutated[param_name] = new_value
                    else:
                        # ガウシアン摂動（浮動小数点）
                        delta = (max_val - min_val) * 0.2 * random.gauss(0, 1)
                        new_value = float(value) + delta
                        mutated[param_name] = max(min_val, min(max_val, new_value))

        return mutated

    def get_evolution_history(self) -> list[dict[str, Any]]:
        """
        進化履歴を取得

        Returns:
            世代ごとの統計情報リスト
        """
        return self.history
