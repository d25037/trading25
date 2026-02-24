"""
Metrics Extractor

HTMLバックテスト結果からメトリクスを抽出するユーティリティ。
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path


TH_METRIC_FIELDS: tuple[tuple[str, str], ...] = (
    ("total_return", "Total Return [%]"),
    ("max_drawdown", "Max Drawdown [%]"),
    ("sharpe_ratio", "Sharpe Ratio"),
    ("sortino_ratio", "Sortino Ratio"),
    ("calmar_ratio", "Calmar Ratio"),
    ("win_rate", "Win Rate [%]"),
    ("profit_factor", "Profit Factor"),
)


@dataclass
class BacktestMetrics:
    """バックテスト結果のメトリクス"""

    total_return: float | None = None
    max_drawdown: float | None = None
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    calmar_ratio: float | None = None
    kelly_fraction: float | None = None
    win_rate: float | None = None
    profit_factor: float | None = None
    total_trades: int | None = None
    optimal_allocation: float | None = None  # 最適配分率 (0.0-1.0)


def _decode_unicode_escapes(content: str) -> str:
    """HTMLコンテンツ内の \\uXXXX エスケープと \\n をデコード"""
    decoded = re.sub(
        r"\\u([0-9a-fA-F]{4})",
        lambda m: chr(int(m.group(1), 16)),
        content,
    )
    return decoded.replace("\\n", "\n")


def _extract_float_from_html(
    html_content: str,
    metric_name: str,
    *,
    use_th: bool = True,
) -> float | None:
    """
    HTMLテーブルからメトリクス値をfloatで抽出

    Args:
        html_content: HTMLコンテンツ（Unicodeエスケープ含む可能性あり）
        metric_name: メトリクス名（例: "Total Return [%]", "最適配分率"）
        use_th: True の場合 <th>name</th><td>value</td> パターン、
                False の場合 <td>name</td><td>value</td> パターン

    Returns:
        メトリクス値（float）またはNone
    """
    decoded = _decode_unicode_escapes(html_content)

    if use_th:
        pattern = rf"<th>{re.escape(metric_name)}</th>\s*<td>([^<]+)</td>"
    else:
        pattern = rf"<td[^>]*>{re.escape(metric_name)}</td>\s*<td[^>]*>([^<]+)</td>"

    match = re.search(pattern, decoded, re.IGNORECASE | re.DOTALL)
    if not match:
        return None

    value_str = match.group(1).strip().replace("%", "").replace("件", "")
    try:
        return float(value_str)
    except ValueError:
        return None


def extract_metrics_from_html(html_path: Path) -> BacktestMetrics:
    """
    HTMLファイルからバックテストメトリクスを抽出

    Args:
        html_path: HTMLファイルパス

    Returns:
        BacktestMetrics インスタンス
    """
    metrics = BacktestMetrics()

    metrics_json_path = html_path.with_suffix(".metrics.json")
    if metrics_json_path.exists():
        try:
            data = json.loads(metrics_json_path.read_text(encoding="utf-8"))
            metrics.total_return = data.get("total_return")
            metrics.max_drawdown = data.get("max_drawdown")
            metrics.sharpe_ratio = data.get("sharpe_ratio")
            metrics.sortino_ratio = data.get("sortino_ratio")
            metrics.calmar_ratio = data.get("calmar_ratio")
            metrics.kelly_fraction = data.get("kelly_fraction")
            metrics.win_rate = data.get("win_rate")
            metrics.profit_factor = data.get("profit_factor")
            metrics.total_trades = data.get("total_trades")
            metrics.optimal_allocation = data.get("optimal_allocation")
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    should_fallback_to_html = any(
        getattr(metrics, field_name) is None for field_name, _ in TH_METRIC_FIELDS
    )
    should_fallback_to_html = (
        should_fallback_to_html
        or metrics.total_trades is None
        or metrics.optimal_allocation is None
    )
    if not should_fallback_to_html:
        return metrics

    try:
        content = html_path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        return metrics

    # Final Portfolio Statistics テーブル (<th>/<td> 形式)
    for field_name, html_metric_name in TH_METRIC_FIELDS:
        if getattr(metrics, field_name) is None:
            setattr(
                metrics,
                field_name,
                _extract_float_from_html(content, html_metric_name),
            )

    if metrics.total_trades is None:
        total_trades = _extract_float_from_html(content, "Total Trades")
        if total_trades is not None:
            metrics.total_trades = int(total_trades)

    # Kelly Allocation Info テーブル (<td>/<td> 形式)
    if metrics.optimal_allocation is None:
        allocation = _extract_float_from_html(content, "最適配分率", use_th=False)
        if allocation is not None:
            metrics.optimal_allocation = allocation / 100  # パーセント → 小数

    return metrics
