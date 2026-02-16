"""
Backtest Result Summary Resolver

バックテスト結果サマリーを HTML 成果物セット（HTML + *.metrics.json）から解決する。
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loguru import logger

from src.data.metrics_extractor import extract_metrics_from_html
from src.server.schemas.backtest import BacktestResultSummary


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _to_required_float(value: Any, *, default: float = 0.0) -> float:
    parsed = _to_optional_float(value)
    return default if parsed is None else parsed


def _to_required_int(value: Any, *, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _fallback_to_mapping(
    fallback: Mapping[str, Any] | BacktestResultSummary | None,
) -> Mapping[str, Any]:
    if fallback is None:
        return {}
    if isinstance(fallback, BacktestResultSummary):
        return fallback.model_dump()
    if isinstance(fallback, Mapping):
        return fallback
    return {}


def _pick_value(
    artifact_values: Mapping[str, Any],
    fallback_values: Mapping[str, Any],
    key: str,
) -> Any:
    artifact = artifact_values.get(key)
    if artifact is not None:
        return artifact
    return fallback_values.get(key)


def resolve_backtest_result_summary(
    html_path: str | Path | None,
    fallback: Mapping[str, Any] | BacktestResultSummary | None = None,
) -> BacktestResultSummary | None:
    """
    BacktestResultSummary を成果物セットから解決する。

    優先順位:
    1. html_path の *.metrics.json / HTML から抽出された値
    2. fallback（raw_result または既存 summary）
    """
    fallback_values = _fallback_to_mapping(fallback)
    resolved_html_path: str | None = str(html_path) if html_path else None

    artifact_values: dict[str, Any] = {}
    if html_path:
        path = Path(html_path)
        if path.exists():
            try:
                metrics = extract_metrics_from_html(path)
                artifact_values = {
                    "total_return": metrics.total_return,
                    "max_drawdown": metrics.max_drawdown,
                    "sharpe_ratio": metrics.sharpe_ratio,
                    "sortino_ratio": metrics.sortino_ratio,
                    "calmar_ratio": metrics.calmar_ratio,
                    "win_rate": metrics.win_rate,
                    "trade_count": metrics.total_trades,
                }
            except Exception as e:
                logger.warning(f"成果物メトリクス抽出失敗: {e}")

    if not artifact_values and not fallback_values:
        return None

    fallback_trade_count = fallback_values.get("trade_count", fallback_values.get("total_trades"))
    fallback_html_path = fallback_values.get("html_path")
    if resolved_html_path is None and isinstance(fallback_html_path, str):
        resolved_html_path = fallback_html_path
    trade_count_value = (
        artifact_values.get("trade_count")
        if artifact_values.get("trade_count") is not None
        else fallback_trade_count
    )

    return BacktestResultSummary(
        total_return=_to_required_float(
            _pick_value(artifact_values, fallback_values, "total_return"),
            default=0.0,
        ),
        sharpe_ratio=_to_required_float(
            _pick_value(artifact_values, fallback_values, "sharpe_ratio"),
            default=0.0,
        ),
        sortino_ratio=_to_optional_float(
            _pick_value(artifact_values, fallback_values, "sortino_ratio")
        ),
        calmar_ratio=_to_required_float(
            _pick_value(artifact_values, fallback_values, "calmar_ratio"),
            default=0.0,
        ),
        max_drawdown=_to_required_float(
            _pick_value(artifact_values, fallback_values, "max_drawdown"),
            default=0.0,
        ),
        win_rate=_to_required_float(
            _pick_value(artifact_values, fallback_values, "win_rate"),
            default=0.0,
        ),
        trade_count=_to_required_int(trade_count_value, default=0),
        html_path=resolved_html_path,
    )
