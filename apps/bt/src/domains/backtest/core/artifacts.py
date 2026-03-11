"""Backtest artifact planning and persistence helpers."""

from __future__ import annotations

import json
import math
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.backtest.vectorbt_adapter import canonical_metrics_from_portfolio


@dataclass(frozen=True, slots=True)
class BacktestArtifactPaths:
    """Filesystem paths for a single backtest run."""

    html_path: Path
    metrics_path: Path
    manifest_path: Path
    simulation_payload_path: Path
    report_payload_path: Path | None = None


class BacktestArtifactWriter:
    """Persist and describe backtest artifacts independent from rendering."""

    @staticmethod
    def artifact_paths_for_html(html_path: Path) -> BacktestArtifactPaths:
        return BacktestArtifactPaths(
            html_path=html_path,
            metrics_path=html_path.with_suffix(".metrics.json"),
            manifest_path=html_path.with_suffix(".manifest.json"),
            simulation_payload_path=html_path.with_suffix(".simulation.pkl"),
            report_payload_path=html_path.with_suffix(".report.json"),
        )

    def write_metrics(self, *, metrics_path: Path, metrics_payload: dict[str, Any]) -> Path:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(
            json.dumps(
                _sanitize_json_payload(metrics_payload),
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            ),
            encoding="utf-8",
        )
        return metrics_path

    def write_manifest(
        self,
        *,
        html_path: Path | None,
        manifest_path: Path | None = None,
        metrics_path: Path | None = None,
        simulation_payload_path: Path | None = None,
        report_payload_path: Path | None = None,
        parameters: dict[str, Any],
        strategy_name: str,
        dataset_name: str,
        elapsed_time: float,
        total_elapsed_time: float | None = None,
        walk_forward: dict[str, Any] | None = None,
        report_status: str = "completed",
        report_render_time: float | None = None,
        render_error: str | None = None,
    ) -> Path:
        """Persist a manifest compatible with the existing bt-046 layout."""

        resolved_manifest_path = manifest_path
        if resolved_manifest_path is None:
            if html_path is None:
                raise ValueError("manifest_path is required when html_path is not available")
            resolved_manifest_path = html_path.with_suffix(".manifest.json")

        resolved_execution_time = (
            total_elapsed_time if total_elapsed_time is not None else elapsed_time
        )

        manifest = {
            "generated_at": datetime.now().isoformat(),
            "strategy_name": strategy_name,
            "dataset_name": dataset_name,
            "html_path": str(html_path) if html_path else None,
            "metrics_path": str(metrics_path) if metrics_path else None,
            "simulation_payload_path": (
                str(simulation_payload_path) if simulation_payload_path else None
            ),
            "report_payload_path": (
                str(report_payload_path) if report_payload_path else None
            ),
            "execution_time": resolved_execution_time,
            "simulation_elapsed_time": elapsed_time,
            "total_elapsed_time": total_elapsed_time,
            "parameters": parameters,
            "simulation": {
                "status": "completed",
                "execution_time": elapsed_time,
                "metrics_path": str(metrics_path) if metrics_path else None,
                "simulation_payload_path": (
                    str(simulation_payload_path) if simulation_payload_path else None
                ),
                "report_payload_path": (
                    str(report_payload_path) if report_payload_path else None
                ),
            },
            "report": {
                "renderer": "marimo_html",
                "status": report_status,
                "html_path": str(html_path) if html_path else None,
                "render_time": report_render_time,
                "error": render_error,
                "report_payload_path": (
                    str(report_payload_path) if report_payload_path else None
                ),
            },
            "versions": {
                "python": self._get_package_version("python"),
                "vectorbt": self._get_package_version("vectorbt"),
                "marimo": self._get_package_version("marimo"),
                "pydantic": self._get_package_version("pydantic"),
            },
            "git_commit": self._get_git_commit(),
        }
        if walk_forward:
            manifest["walk_forward"] = walk_forward

        resolved_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_manifest_path.write_text(
            json.dumps(
                _sanitize_json_payload(manifest),
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            ),
            encoding="utf-8",
        )
        return resolved_manifest_path

    @staticmethod
    def _get_package_version(package: str) -> str | None:
        if package == "python":
            return sys.version.split()[0]
        try:
            from importlib.metadata import version

            return version(package)
        except Exception:
            return None

    @staticmethod
    def _get_git_commit() -> str | None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() or None
        except Exception:
            return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    return coerced if math.isfinite(coerced) else None


def _sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _extract_stat(stats: Any, key: str) -> float | None:
    if stats is None:
        return None
    try:
        if hasattr(stats, "get"):
            direct_value = stats.get(key)
            parsed = _coerce_float(direct_value)
            if parsed is not None:
                return parsed
    except Exception:
        pass
    if isinstance(stats, pd.Series):
        return _coerce_float(stats.get(key))
    if isinstance(stats, pd.DataFrame) and key in stats.index:
        row = stats.loc[key]
        if hasattr(row, "mean"):
            return _coerce_float(row.mean())
        if hasattr(row, "iloc"):
            return _coerce_float(row.iloc[0])
    return None


def _extract_optimal_allocation(allocation_info: Any) -> float | None:
    if hasattr(allocation_info, "allocation"):
        return _coerce_float(getattr(allocation_info, "allocation"))
    return _coerce_float(allocation_info)


def build_metrics_payload(
    *,
    portfolio: Any,
    allocation_info: Any = None,
) -> dict[str, Any]:
    """Build the canonical metrics payload stored beside each result."""

    stats = None
    if portfolio is not None:
        try:
            stats = portfolio.stats()
        except Exception:
            stats = None
    summary_metrics = canonical_metrics_from_portfolio(portfolio)

    metrics_payload: dict[str, Any] = {
        "total_return": _extract_stat(stats, "Total Return [%]"),
        "max_drawdown": _extract_stat(stats, "Max Drawdown [%]"),
        "sharpe_ratio": _extract_stat(stats, "Sharpe Ratio"),
        "sortino_ratio": _extract_stat(stats, "Sortino Ratio"),
        "calmar_ratio": _extract_stat(stats, "Calmar Ratio"),
        "win_rate": _extract_stat(stats, "Win Rate [%]"),
        "profit_factor": _extract_stat(stats, "Profit Factor"),
    }
    total_trades = _extract_stat(stats, "Total Trades")
    metrics_payload["total_trades"] = int(total_trades) if total_trades is not None else None
    metrics_payload["trade_count"] = metrics_payload["total_trades"]

    if summary_metrics is not None:
        metrics_payload["total_return"] = _prefer_metric_value(
            metrics_payload["total_return"],
            summary_metrics.total_return,
        )
        metrics_payload["max_drawdown"] = _prefer_metric_value(
            metrics_payload["max_drawdown"],
            summary_metrics.max_drawdown,
        )
        metrics_payload["sharpe_ratio"] = _prefer_metric_value(
            metrics_payload["sharpe_ratio"],
            summary_metrics.sharpe_ratio,
        )
        metrics_payload["sortino_ratio"] = _prefer_metric_value(
            metrics_payload["sortino_ratio"],
            summary_metrics.sortino_ratio,
        )
        metrics_payload["calmar_ratio"] = _prefer_metric_value(
            metrics_payload["calmar_ratio"],
            summary_metrics.calmar_ratio,
        )
        metrics_payload["win_rate"] = _prefer_metric_value(
            metrics_payload["win_rate"],
            summary_metrics.win_rate,
        )
        if metrics_payload["trade_count"] is None:
            metrics_payload["trade_count"] = summary_metrics.trade_count
        if metrics_payload["total_trades"] is None:
            metrics_payload["total_trades"] = summary_metrics.trade_count

    metrics_payload["optimal_allocation"] = _extract_optimal_allocation(allocation_info)
    metrics_payload["generated_at"] = datetime.now().isoformat()
    return metrics_payload


def _prefer_metric_value(primary: Any, fallback: Any) -> Any:
    return primary if primary is not None else fallback
