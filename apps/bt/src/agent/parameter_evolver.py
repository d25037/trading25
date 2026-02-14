"""
遺伝的アルゴリズムによるパラメータ最適化モジュール

戦略パラメータを進化的に最適化
"""

import copy
import random
from typing import Any

from loguru import logger

from src.lib.strategy_runtime.loader import ConfigLoader

from .models import EvolutionConfig, EvaluationResult, SignalCategory, StrategyCandidate
from .signal_filters import is_signal_allowed
from .signal_augmentation import apply_random_add_structure
from .signal_search_space import CATEGORICAL_PARAMS, PARAM_RANGES
from .strategy_evaluator import StrategyEvaluator


class ParameterEvolver:
    """
    遺伝的アルゴリズムによるパラメータ最適化

    戦略のパラメータを進化的に最適化し、最良の組み合わせを探索
    """

    # Backward-compatible alias (used by older internal callers).
    PARAM_RANGES = PARAM_RANGES

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
        self.allowed_category_set: set[SignalCategory] = set(self.config.allowed_categories)

        # 乱数シード設定
        random.seed(self.config.seed if self.config.seed is not None else 42)

        # 評価器
        self.evaluator = StrategyEvaluator(
            shared_config_dict=shared_config_dict,
            scoring_weights=scoring_weights,
            n_jobs=self.config.n_jobs,
            timeout_seconds=self.config.timeout_seconds,
        )

        # 進化履歴
        self.history: list[dict[str, Any]] = []
        self._base_entry_signals: set[str] = set()
        self._base_exit_signals: set[str] = set()

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

        # random_add モード用: ベース戦略のシグナル集合を記録
        self._base_entry_signals = _extract_enabled_signal_names(
            base_candidate.entry_filter_params
        )
        self._base_exit_signals = _extract_enabled_signal_names(
            base_candidate.exit_trigger_params
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

        is_random_add_mode = self.config.structure_mode == "random_add"
        should_add_signals = is_random_add_mode and (
            self.config.random_add_entry_signals > 0
            or self.config.random_add_exit_signals > 0
        )

        # random_add モード: ベース + 追加シグナル版も入れて多様性を確保
        if should_add_signals and len(population) < self.config.population_size:
            augmented, _ = apply_random_add_structure(
                base_candidate,
                rng=random,
                add_entry_signals=self.config.random_add_entry_signals,
                add_exit_signals=self.config.random_add_exit_signals,
                base_entry_signals=self._base_entry_signals,
                base_exit_signals=self._base_exit_signals,
            )
            augmented.strategy_id = "base_augmented"
            population.append(augmented)

        # 残りは変異させて生成
        for i in range(self.config.population_size - len(population)):
            mutated = self._mutate(base_candidate, mutation_strength=0.3)
            if is_random_add_mode:
                mutated, _ = apply_random_add_structure(
                    mutated,
                    rng=random,
                    add_entry_signals=self.config.random_add_entry_signals,
                    add_exit_signals=self.config.random_add_exit_signals,
                    base_entry_signals=self._base_entry_signals,
                    base_exit_signals=self._base_exit_signals,
                )
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
        is_random_add_mode = self.config.structure_mode == "random_add"
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

            # random_add モード: 子個体のシグナル数を正規化（追加・過剰分トリム）
            if is_random_add_mode:
                child, _ = apply_random_add_structure(
                    child,
                    rng=random,
                    add_entry_signals=self.config.random_add_entry_signals,
                    add_exit_signals=self.config.random_add_exit_signals,
                    base_entry_signals=self._base_entry_signals,
                    base_exit_signals=self._base_exit_signals,
                )

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
            if not self._is_signal_mutation_allowed(signal_name, usage_type="entry"):
                continue
            if isinstance(params, dict):
                mutated.entry_filter_params[signal_name] = self._mutate_signal_params(
                    signal_name, params, mutation_strength
                )

        # Exit params の変異
        for signal_name, params in mutated.exit_trigger_params.items():
            if not self._is_signal_mutation_allowed(signal_name, usage_type="exit"):
                continue
            if isinstance(params, dict):
                mutated.exit_trigger_params[signal_name] = self._mutate_signal_params(
                    signal_name, params, mutation_strength
                )

        mutated.metadata["mutated"] = True

        # random_add モード: 突然変異後も追加シグナル数を維持
        if self.config.structure_mode == "random_add":
            mutated, _ = apply_random_add_structure(
                mutated,
                rng=random,
                add_entry_signals=self.config.random_add_entry_signals,
                add_exit_signals=self.config.random_add_exit_signals,
                base_entry_signals=self._base_entry_signals,
                base_exit_signals=self._base_exit_signals,
            )
        return mutated

    def _is_signal_mutation_allowed(self, signal_name: str, usage_type: str) -> bool:
        """制約に基づいて変異対象に含めるか判定する。"""
        if usage_type == "exit" and self.config.entry_filter_only:
            return False
        return is_signal_allowed(signal_name, self.allowed_category_set)

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

        for param_name, value in params.items():
            if param_name in CATEGORICAL_PARAMS:
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


def _extract_enabled_signal_names(params_dict: dict[str, Any]) -> set[str]:
    enabled: set[str] = set()
    for name, value in (params_dict or {}).items():
        if not isinstance(value, dict):
            enabled.add(name)
            continue
        if value.get("enabled", True):
            enabled.add(name)
    return enabled
