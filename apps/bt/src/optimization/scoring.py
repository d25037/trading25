"""
最適化スコアリング共通関数

Min-Max正規化と複合スコア計算を提供
"""

from typing import Any

import numpy as np


def is_valid_metric(value: Any) -> bool:
    """
    メトリクス値がNaN/Infでないかを検証

    Args:
        value: 検証する値

    Returns:
        bool: 有効な数値の場合True、NaN/Infの場合False
    """
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def calculate_composite_score(
    portfolio: Any,
    weights: dict[str, float],
) -> float:
    """
    VectorBTポートフォリオから複合スコアを計算

    Args:
        portfolio: VectorBTポートフォリオオブジェクト
        weights: 指標名と重みの辞書
            例: {"sharpe_ratio": 0.4, "calmar_ratio": 0.3, "total_return": 0.3}

    Returns:
        float: 複合スコア（各指標の重み付け合計）
    """
    score = 0.0

    if "sharpe_ratio" in weights:
        try:
            sharpe = float(portfolio.sharpe_ratio())
            if is_valid_metric(sharpe):
                score += weights["sharpe_ratio"] * sharpe
        except Exception:
            pass

    if "calmar_ratio" in weights:
        try:
            calmar = float(portfolio.calmar_ratio())
            if is_valid_metric(calmar):
                score += weights["calmar_ratio"] * calmar
        except Exception:
            pass

    if "total_return" in weights:
        try:
            total_return = float(portfolio.total_return())
            if is_valid_metric(total_return):
                score += weights["total_return"] * total_return
        except Exception:
            pass

    return score


def normalize_and_recalculate_scores(
    results: list[dict[str, Any]],
    scoring_weights: dict[str, float],
) -> list[dict[str, Any]]:
    """
    全結果の指標を正規化し、複合スコアを再計算

    Min-Max正規化: normalized = (value - min) / (max - min)

    これにより異なるスケールの指標（Sharpe 0-3 vs Return 0-100%）を
    0-1の範囲に統一し、重み付けを適切に機能させる。

    Args:
        results: 最適化結果リスト（各要素は metric_values を含む）
        scoring_weights: 各指標の重み（例: {"sharpe_ratio": 0.4, "total_return": 0.3, ...}）

    Returns:
        List[Dict[str, Any]]: 正規化後の結果リスト
            各要素に normalized_metrics と更新された score が追加される
    """
    if not results:
        return results

    # 1. 各指標の最小値・最大値を収集
    metric_ranges: dict[str, dict[str, float]] = {}
    for metric in scoring_weights.keys():
        values = [r["metric_values"][metric] for r in results]
        metric_ranges[metric] = {"min": min(values), "max": max(values)}

    # 2. 各結果を正規化し、複合スコアを再計算
    normalized_results = []
    for result in results:
        normalized_metrics = {}
        composite_score = 0.0

        for metric, weight in scoring_weights.items():
            raw_value = result["metric_values"][metric]
            min_val = metric_ranges[metric]["min"]
            max_val = metric_ranges[metric]["max"]

            # Min-Max正規化（0-1範囲）
            if max_val - min_val > 1e-10:  # ゼロ除算対策
                normalized_value = (raw_value - min_val) / (max_val - min_val)
            else:
                # 全て同じ値の場合は0.5とする
                normalized_value = 0.5

            normalized_metrics[metric] = normalized_value
            composite_score += weight * normalized_value

        # 結果を更新
        result["normalized_metrics"] = normalized_metrics
        result["score"] = composite_score
        normalized_results.append(result)

    return normalized_results
