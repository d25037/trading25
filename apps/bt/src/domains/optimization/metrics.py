"""
最適化用メトリクス収集ユーティリティ
"""

from typing import Any, Mapping

from .scoring import is_valid_metric


def extract_trade_count(portfolio: Any) -> int:
    """
    ポートフォリオからclosed trades件数を取得

    Args:
        portfolio: VectorBTポートフォリオオブジェクト

    Returns:
        int: トレード件数（取得不能時は0）
    """
    try:
        trades = portfolio.trades
    except Exception:
        return 0

    # 優先: VectorBTのcount()（Seriesの可能性があるためsumで集約）
    try:
        count = trades.count()
        count_value = count.sum() if hasattr(count, "sum") else count
        if is_valid_metric(count_value):
            return max(0, int(float(count_value)))
    except Exception:
        pass

    # フォールバック: records_readableの行数
    try:
        if hasattr(trades, "records_readable"):
            return max(0, int(len(trades.records_readable)))
    except Exception:
        pass

    return 0


def collect_metrics(
    portfolio: Any, scoring_weights: Mapping[str, float]
) -> dict[str, float | int]:
    """
    ポートフォリオから最適化メトリクスを収集

    Args:
        portfolio: VectorBTポートフォリオオブジェクト
        scoring_weights: スコア計算対象メトリクスの重み

    Returns:
        dict[str, float | int]: メトリクス名と値のマッピング
    """
    metric_values: dict[str, float | int] = {}

    for metric in scoring_weights.keys():
        if metric == "sharpe_ratio":
            value = portfolio.sharpe_ratio()
        elif metric == "calmar_ratio":
            value = portfolio.calmar_ratio()
        elif metric == "total_return":
            value = portfolio.total_return()
        else:
            continue

        metric_values[metric] = float(value) if is_valid_metric(value) else 0.0

    # 表示用途: closed trades件数（スコア計算には使用しない）
    metric_values["trade_count"] = extract_trade_count(portfolio)

    return metric_values
