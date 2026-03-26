"""
最適化用メトリクス収集ユーティリティ
"""

from typing import Any, Mapping

from src.domains.backtest.vectorbt_adapter import (
    ExecutionPortfolioProtocol,
    canonical_metrics_from_portfolio,
)

from .scoring import is_valid_metric


def _coerce_metric_value(value: float | None) -> float:
    if value is None or not is_valid_metric(value):
        return 0.0
    return float(value)


def extract_trade_count(portfolio: ExecutionPortfolioProtocol | Any) -> int:
    """
    ポートフォリオからclosed trades件数を取得

    Args:
        portfolio: 実行ポートフォリオ

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
    portfolio: ExecutionPortfolioProtocol | Any,
    scoring_weights: Mapping[str, float],
) -> dict[str, float | int]:
    """
    ポートフォリオから最適化メトリクスを収集

    Args:
        portfolio: 実行ポートフォリオ
        scoring_weights: スコア計算対象メトリクスの重み

    Returns:
        dict[str, float | int]: メトリクス名と値のマッピング
    """
    metric_values: dict[str, float | int] = {}
    canonical_metrics = canonical_metrics_from_portfolio(portfolio)
    metric_lookup = {
        "sharpe_ratio": (
            canonical_metrics.sharpe_ratio if canonical_metrics is not None else None
        ),
        "calmar_ratio": (
            canonical_metrics.calmar_ratio if canonical_metrics is not None else None
        ),
        "total_return": (
            canonical_metrics.total_return if canonical_metrics is not None else None
        ),
        "sortino_ratio": (
            canonical_metrics.sortino_ratio if canonical_metrics is not None else None
        ),
        "max_drawdown": (
            canonical_metrics.max_drawdown if canonical_metrics is not None else None
        ),
        "win_rate": (
            canonical_metrics.win_rate if canonical_metrics is not None else None
        ),
    }

    for metric in scoring_weights.keys():
        if metric not in metric_lookup:
            continue
        value = metric_lookup.get(metric)
        metric_values[metric] = _coerce_metric_value(value)

    for metric in ("sortino_ratio", "max_drawdown", "win_rate"):
        if metric in metric_values:
            continue
        value = metric_lookup.get(metric)
        if value is None:
            continue
        metric_values[metric] = _coerce_metric_value(value)

    # 表示用途: closed trades件数（スコア計算には使用しない）
    trade_count = (
        canonical_metrics.trade_count if canonical_metrics is not None else None
    )
    metric_values["trade_count"] = (
        int(trade_count) if trade_count is not None else extract_trade_count(portfolio)
    )

    return metric_values
