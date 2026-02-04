"""
スコア正規化モジュール

評価結果のMin-Max正規化処理
"""

from ..models import EvaluationResult


def normalize_scores(
    results: list[EvaluationResult],
    scoring_weights: dict[str, float],
) -> list[EvaluationResult]:
    """
    スコアを正規化（Min-Max正規化）

    Args:
        results: 評価結果リスト
        scoring_weights: スコアリング重み

    Returns:
        正規化後の評価結果リスト
    """
    if not results:
        return results

    # 各メトリクスの範囲を計算
    sharpe_values = [r.sharpe_ratio for r in results]
    calmar_values = [r.calmar_ratio for r in results]
    return_values = [r.total_return for r in results]

    sharpe_min, sharpe_max = min(sharpe_values), max(sharpe_values)
    calmar_min, calmar_max = min(calmar_values), max(calmar_values)
    return_min, return_max = min(return_values), max(return_values)

    # 正規化してスコア再計算
    normalized_results = []
    for result in results:
        # Min-Max正規化
        sharpe_norm = normalize_value(result.sharpe_ratio, sharpe_min, sharpe_max)
        calmar_norm = normalize_value(result.calmar_ratio, calmar_min, calmar_max)
        return_norm = normalize_value(result.total_return, return_min, return_max)

        # 複合スコア再計算
        metrics = {"sharpe_ratio": sharpe_norm, "calmar_ratio": calmar_norm, "total_return": return_norm}
        score = sum(
            scoring_weights.get(key, 0.0) * value for key, value in metrics.items()
        )

        # 新しいEvaluationResultを作成（scoreを更新）
        normalized_results.append(
            EvaluationResult(
                candidate=result.candidate,
                score=score,
                sharpe_ratio=result.sharpe_ratio,
                calmar_ratio=result.calmar_ratio,
                total_return=result.total_return,
                max_drawdown=result.max_drawdown,
                win_rate=result.win_rate,
                trade_count=result.trade_count,
                success=result.success,
                error_message=result.error_message,
            )
        )

    return normalized_results


def normalize_value(value: float, min_val: float, max_val: float) -> float:
    """
    値を0-1に正規化

    Args:
        value: 正規化対象値
        min_val: 最小値
        max_val: 最大値

    Returns:
        正規化値（0-1）
    """
    if max_val - min_val > 1e-10:
        return (value - min_val) / (max_val - min_val)
    return 0.5  # 全て同じ値の場合
