"""Backtest metric loading for screening strategy summaries."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol


class StrategyMetricLike(Protocol):
    @property
    def basename(self) -> str:
        ...

    @property
    def response_name(self) -> str:
        ...


def load_strategy_scores(
    strategies: Sequence[StrategyMetricLike],
    *,
    load_latest_metric_fn,
) -> tuple[dict[str, float | None], list[str], list[str]]:
    """各戦略の最新バックテスト指標を取得する。"""
    scores: dict[str, float | None] = {}
    missing: list[str] = []
    warnings: list[str] = []

    for strategy in strategies:
        score, warning = load_latest_metric_fn(strategy.basename)
        scores[strategy.response_name] = score
        if score is None:
            missing.append(strategy.response_name)
        if warning:
            warnings.append(f"{strategy.response_name}: {warning}")

    return scores, missing, warnings


def load_latest_metric(
    strategy_basename: str,
    *,
    metric_name: str,
    get_backtest_results_dir,
) -> tuple[float | None, str | None]:
    """戦略ディレクトリ内の最新*.metrics.jsonから metric_name を取得する。"""
    strategy_dir: Path = get_backtest_results_dir(strategy_basename)
    if not strategy_dir.exists():
        return None, None

    metric_files = sorted(
        strategy_dir.glob("*.metrics.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not metric_files:
        return None, None

    latest = metric_files[0]

    try:
        payload = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"failed to read metrics ({latest.name}: {exc})"

    value = payload.get(metric_name)
    if value is None:
        return None, None

    if isinstance(value, (int, float)):
        return float(value), None

    try:
        return float(value), None
    except (TypeError, ValueError):
        return None, f"metric {metric_name} is not numeric in {latest.name}"
