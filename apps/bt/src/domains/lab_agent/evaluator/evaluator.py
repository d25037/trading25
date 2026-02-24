"""
メイン評価クラス

StrategyEvaluatorクラスの実装
"""

from typing import Any

from loguru import logger

from src.shared.constants import DEFAULT_SCORING_WEIGHTS, EVALUATION_TIMEOUT_SECONDS
from src.infrastructure.data_access.mode import data_access_mode_context
from src.infrastructure.data_access.loaders.cache import DataCache

from ..models import EvaluationResult, StrategyCandidate
from .batch_executor import (
    execute_batch_evaluation,
    get_max_workers,
    prepare_batch_data,
)
from .candidate_processor import evaluate_single_candidate
from .data_preparation import load_default_shared_config
from .score_normalizer import normalize_scores


class StrategyEvaluator:
    """
    戦略評価クラス

    生成された戦略候補をバックテストで評価し、スコアリングする
    """

    def __init__(
        self,
        shared_config_dict: dict[str, Any] | None = None,
        scoring_weights: dict[str, float] | None = None,
        n_jobs: int = -1,
        timeout_seconds: int = EVALUATION_TIMEOUT_SECONDS,
    ):
        """
        初期化

        Args:
            shared_config_dict: 共有設定辞書
            scoring_weights: スコアリング重み
            n_jobs: 並列ワーカー数（-1で全CPU）
            timeout_seconds: 評価タイムアウト（秒）
        """
        # デフォルト共有設定（config/default.yaml から読み込み、引数で上書き）
        default_config = load_default_shared_config()
        if shared_config_dict:
            default_config.update(shared_config_dict)
        self.shared_config_dict = default_config

        # デフォルトスコアリング重み
        self.scoring_weights = scoring_weights or DEFAULT_SCORING_WEIGHTS.copy()

        self.n_jobs = n_jobs
        self.timeout_seconds = timeout_seconds

    def evaluate_single(self, candidate: StrategyCandidate) -> EvaluationResult:
        """
        単一候補を評価

        Args:
            candidate: 戦略候補

        Returns:
            評価結果
        """
        with data_access_mode_context("direct"):
            return evaluate_single_candidate(
                candidate, self.shared_config_dict, self.scoring_weights
            )

    def evaluate_batch(
        self,
        candidates: list[StrategyCandidate],
        top_k: int | None = None,
        enable_cache: bool = True,
    ) -> list[EvaluationResult]:
        """
        複数候補をバッチ評価

        Args:
            candidates: 戦略候補リスト
            top_k: 上位K件のみ返す（省略時は全件）
            enable_cache: データキャッシュを有効化（デフォルト: True）

        Returns:
            評価結果リスト（スコア降順）

        Note:
            OHLCVデータはメインプロセスで1回だけバッチ取得し、
            各ワーカープロセスに渡すことでAPIコールを削減します。
            100戦略×50銘柄の評価でも、API呼び出しは1回のみです。
        """
        if not candidates:
            return []

        with data_access_mode_context("direct"):
            # キャッシュ有効化（バッチ評価中のAPIコール削減）
            if enable_cache:
                DataCache.enable()
                logger.debug("DataCache enabled for batch evaluation")

            try:
                results = self._evaluate_batch_internal(candidates, top_k)
            finally:
                # キャッシュ無効化・クリア
                if enable_cache:
                    stats = DataCache.get_instance().get_stats()
                    DataCache.disable()
                    hits, misses = stats["hits"], stats["misses"]
                    logger.debug(f"DataCache disabled: hits={hits}, misses={misses}")

            return results

    def _evaluate_batch_internal(
        self,
        candidates: list[StrategyCandidate],
        top_k: int | None = None,
    ) -> list[EvaluationResult]:
        """バッチ評価の内部実装"""
        max_workers = get_max_workers(self.n_jobs)
        prepared_data = prepare_batch_data(self.shared_config_dict, candidates)
        results = execute_batch_evaluation(
            candidates,
            max_workers,
            prepared_data,
            self.shared_config_dict,
            self.scoring_weights,
            self.timeout_seconds,
        )
        return self._finalize_batch_results(results, top_k)

    def _finalize_batch_results(
        self,
        results: list[EvaluationResult],
        top_k: int | None,
    ) -> list[EvaluationResult]:
        """
        バッチ評価結果を最終処理

        Args:
            results: 評価結果リスト
            top_k: 上位K件のみ返す（省略時は全件）

        Returns:
            正規化・ソート済み評価結果リスト
        """
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]

        normalized_results = normalize_scores(successful_results, self.scoring_weights)
        sorted_results = sorted(normalized_results, key=lambda x: x.score, reverse=True)
        sorted_results.extend(failed_results)

        if top_k is not None and top_k > 0:
            sorted_results = sorted_results[:top_k]

        return sorted_results
