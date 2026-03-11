"""Backtest artifact path and manifest helpers."""

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


@dataclass(frozen=True)
class BacktestArtifactPaths:
    """Filesystem paths for a single backtest run's core artifacts."""

    html_path: Path
    metrics_path: Path
    manifest_path: Path
    report_data_path: Path


class BacktestArtifactWriter:
    """Build and persist backtest artifact metadata independent from rendering."""

    @staticmethod
    def artifact_paths_for_html(html_path: Path) -> BacktestArtifactPaths:
        return BacktestArtifactPaths(
            html_path=html_path,
            metrics_path=html_path.with_suffix(".metrics.json"),
            manifest_path=html_path.with_suffix(".manifest.json"),
            report_data_path=html_path.with_suffix(".report.json"),
        )

    def write_manifest(
        self,
        *,
        html_path: Path,
        parameters: dict[str, Any],
        strategy_name: str,
        dataset_name: str,
        elapsed_time: float,
        walk_forward: dict[str, Any] | None = None,
        metrics_path: Path | None = None,
        html_generated: bool | None = None,
        render_error: str | None = None,
        render_status: str | None = None,
        report_data_path: Path | None = None,
    ) -> Path:
        """Persist the run manifest beside the expected HTML artifact path."""

        artifact_paths = self.artifact_paths_for_html(html_path)
        html_available = html_generated if html_generated is not None else html_path.exists()
        core_artifacts = {
            "manifest_json": {
                "path": str(artifact_paths.manifest_path),
                "role": "artifact_catalog",
                "available": True,
            },
            "metrics_json": {
                "path": str(metrics_path) if metrics_path is not None else None,
                "role": "canonical_summary",
                "available": metrics_path.exists() if metrics_path is not None else False,
            },
            "report_data_json": {
                "path": str(report_data_path) if report_data_path is not None else None,
                "role": "renderer_input",
                "available": (
                    report_data_path.exists() if report_data_path is not None else False
                ),
            },
        }
        presentation_artifacts = {
            "result_html": {
                "path": str(html_path),
                "role": "presentation_only",
                "available": html_available,
                "render_status": render_status,
                "render_error": render_error,
            }
        }
        manifest = {
            "generated_at": datetime.now().isoformat(),
            "strategy_name": strategy_name,
            "dataset_name": dataset_name,
            "html_path": str(html_path),
            "execution_time": elapsed_time,
            "parameters": parameters,
            "artifact_contract": {
                "version": 2,
                "canonical_result_source": "metrics.json",
                "artifact_catalog_source": "manifest.json",
                "presentation_source": "result.html",
            },
            "core_artifacts": core_artifacts,
            "presentation_artifacts": presentation_artifacts,
            "versions": {
                "python": self._get_package_version("python"),
                "vectorbt": self._get_package_version("vectorbt"),
                "marimo": self._get_package_version("marimo"),
                "pydantic": self._get_package_version("pydantic"),
            },
            "git_commit": self._get_git_commit(),
            "report_artifacts": {
                "html_generated": html_available,
                "metrics_path": core_artifacts["metrics_json"]["path"],
                "manifest_path": core_artifacts["manifest_json"]["path"],
                "report_data_path": core_artifacts["report_data_json"]["path"],
                "render_status": render_status,
                "render_error": render_error,
            },
        }
        if walk_forward:
            manifest["walk_forward"] = walk_forward

        artifact_paths.manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return artifact_paths.manifest_path

    def write_metrics(
        self,
        *,
        html_path: Path,
        metrics_payload: dict[str, Any],
    ) -> Path:
        """Persist canonical scalar metrics beside the expected HTML artifact path."""

        artifact_paths = self.artifact_paths_for_html(html_path)
        artifact_paths.metrics_path.write_text(
            json.dumps(metrics_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return artifact_paths.metrics_path

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


def _extract_stat(stats: Any, key: str) -> float | None:
    if stats is None:
        return None
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
    """Build the canonical metrics payload used by artifact-first backtest reads."""

    metrics_payload: dict[str, Any] = {}
    summary_metrics = canonical_metrics_from_portfolio(portfolio)
    if summary_metrics is not None:
        if summary_metrics.total_return is not None:
            metrics_payload["total_return"] = summary_metrics.total_return
        if summary_metrics.max_drawdown is not None:
            metrics_payload["max_drawdown"] = summary_metrics.max_drawdown
        if summary_metrics.sharpe_ratio is not None:
            metrics_payload["sharpe_ratio"] = summary_metrics.sharpe_ratio
        if summary_metrics.sortino_ratio is not None:
            metrics_payload["sortino_ratio"] = summary_metrics.sortino_ratio
        if summary_metrics.calmar_ratio is not None:
            metrics_payload["calmar_ratio"] = summary_metrics.calmar_ratio
        if summary_metrics.win_rate is not None:
            metrics_payload["win_rate"] = summary_metrics.win_rate
        if summary_metrics.trade_count is not None:
            metrics_payload["total_trades"] = int(summary_metrics.trade_count)

    stats = None
    if portfolio is not None:
        try:
            stats = portfolio.stats()
        except Exception:
            stats = None

    profit_factor = _extract_stat(stats, "Profit Factor")
    if profit_factor is not None:
        metrics_payload["profit_factor"] = profit_factor

    optimal_allocation = _extract_optimal_allocation(allocation_info)
    if optimal_allocation is not None:
        metrics_payload["optimal_allocation"] = optimal_allocation

    return metrics_payload
