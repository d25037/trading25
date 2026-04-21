"""Static backtest report path planning and HTML rendering."""

from __future__ import annotations

import json
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.backtest.core.artifacts import BacktestArtifactPaths, BacktestArtifactWriter
from src.domains.backtest.core.report_payload import (
    BacktestReportRenderContext,
    SerializedPortfolioView,
    load_backtest_report_payload,
)
from src.shared.utils.snapshot_ids import canonicalize_dataset_snapshot_id

BacktestReportPaths = BacktestArtifactPaths


class BacktestReportPathPlanner:
    """Plan report artifact paths without depending on a notebook runtime."""

    def __init__(self, output_dir: str | Path | None = None) -> None:
        if output_dir is None:
            from src.shared.paths import get_backtest_results_dir

            self.output_dir = get_backtest_results_dir()
        else:
            self._validate_output_directory(str(output_dir))
            self.output_dir = Path(output_dir)

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _validate_output_directory(self, output_dir: str) -> None:
        dangerous_patterns = [
            "../",
            "..\\",
            "/etc",
            "/var/log",
            "/usr",
            "/bin",
            "/sbin",
        ]
        if any(pattern in output_dir for pattern in dangerous_patterns):
            raise ValueError(f"Invalid output directory path: {output_dir}")

        try:
            from src.shared.paths import get_data_dir

            output_path = Path(output_dir).expanduser().resolve()
            current_dir = Path.cwd().resolve()
            data_dir = get_data_dir().resolve()

            if "/tmp" in str(output_path) or "tmp" in str(output_path).lower():
                return
            if str(output_path).startswith(str(current_dir)):
                return
            if str(output_path).startswith(str(data_dir)):
                return

            raise ValueError(
                "Output directory must be inside the project or "
                f"~/.local/share/trading25: {output_dir}"
            )
        except ValueError:
            raise
        except Exception as exc:
            if "/tmp" in output_dir or "tmp" in output_dir.lower():
                return
            raise ValueError(f"Output directory validation failed: {exc}") from exc

    def _validate_filename(self, filename: str, extension: str = ".html") -> None:
        pattern = rf"^[a-zA-Z0-9._-]+\{extension}$"
        if not re.match(pattern, filename):
            raise ValueError(
                f"Invalid filename: {filename} "
                f"({extension} files may only use letters, digits, _, -, and .)"
            )

        dangerous_patterns = ["..", "/", "\\", "~", ":", "*", "?", '"', "<", ">", "|"]
        if any(pattern in filename for pattern in dangerous_patterns):
            raise ValueError(f"Filename contains invalid characters: {filename}")
        if len(filename) > 100:
            raise ValueError(f"Filename is too long: {filename} (max 100 chars)")

    def _generate_output_filename(
        self,
        parameters: dict[str, Any],
        strategy_name: str | None = None,
    ) -> tuple[str, str]:
        strategy_name = strategy_name or "unknown"
        shared_config = parameters.get("shared_config", {})
        dataset = shared_config.get("dataset", "")
        dataset_name = canonicalize_dataset_snapshot_id(dataset) or "unknown"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        from src.shared.paths import get_backtest_results_dir

        external_dir = get_backtest_results_dir()
        if str(self.output_dir).startswith(str(external_dir)):
            strategy_dir_path = strategy_name
        else:
            strategy_dir_path = f"backtest/{strategy_name}"

        return strategy_dir_path, f"{dataset_name}_{timestamp}"

    def _resolve_html_path(
        self,
        *,
        parameters: dict[str, Any],
        strategy_name: str | None = None,
        output_filename: str | None = None,
    ) -> Path:
        if output_filename is None:
            strategy_dir_path, base_filename = self._generate_output_filename(
                parameters,
                strategy_name,
            )
        else:
            strategy_dir_path = f"backtest/{strategy_name}" if strategy_name else ""
            base_filename = output_filename.replace(".html", "")

        strategy_output_dir = self.output_dir / strategy_dir_path
        strategy_output_dir.mkdir(parents=True, exist_ok=True)

        html_filename = f"{base_filename}.html"
        self._validate_filename(html_filename, ".html")
        html_path = strategy_output_dir / html_filename

        html_path_resolved = html_path.resolve()
        output_dir_resolved = self.output_dir.resolve()
        if not str(html_path_resolved).startswith(str(output_dir_resolved)):
            raise ValueError(f"Output file path escapes output directory: {html_filename}")

        return html_path

    def plan_report_paths(
        self,
        parameters: dict[str, Any],
        strategy_name: str | None = None,
        output_filename: str | None = None,
    ) -> BacktestReportPaths:
        html_path = self._resolve_html_path(
            parameters=parameters,
            strategy_name=strategy_name,
            output_filename=output_filename,
        )
        return BacktestArtifactWriter.artifact_paths_for_html(html_path)


class StaticHtmlReportRenderer(BacktestReportPathPlanner):
    """Render a presentation HTML file from a serialized report payload."""

    renderer_name = "static_html"

    def render_report(
        self,
        *,
        report_payload_path: Path,
        html_path: Path,
        strategy_name: str,
        dataset_name: str,
        metrics_path: Path | None = None,
        manifest_path: Path | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> Path:
        context = load_backtest_report_payload(report_payload_path)
        metrics = _load_json(metrics_path)
        html = _build_report_html(
            context=context,
            strategy_name=strategy_name,
            dataset_name=dataset_name,
            metrics=metrics,
            metrics_path=metrics_path,
            manifest_path=manifest_path,
            report_payload_path=report_payload_path,
            parameters=parameters or {},
        )
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html, encoding="utf-8")
        return html_path


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _format_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.4f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _table_from_mapping(title: str, values: dict[str, Any]) -> str:
    if not values:
        return ""
    rows = "\n".join(
        "<tr>"
        f"<th>{escape(str(key))}</th>"
        f"<td>{escape(_format_value(value))}</td>"
        "</tr>"
        for key, value in values.items()
    )
    return f"<section><h2>{escape(title)}</h2><table>{rows}</table></section>"


def _table_from_series(title: str, series: pd.Series) -> str:
    if series.empty:
        return ""
    rows = "\n".join(
        "<tr>"
        f"<th>{escape(str(index))}</th>"
        f"<td>{escape(_format_value(value))}</td>"
        "</tr>"
        for index, value in series.items()
    )
    return f"<section><h2>{escape(title)}</h2><table>{rows}</table></section>"


def _table_from_dataframe(title: str, frame: pd.DataFrame, *, limit: int = 25) -> str:
    if frame.empty:
        return ""
    limited = frame.head(limit)
    headers = "".join(f"<th>{escape(str(column))}</th>" for column in limited.columns)
    body = "\n".join(
        "<tr>"
        + "".join(f"<td>{escape(_format_value(value))}</td>" for value in row)
        + "</tr>"
        for row in limited.itertuples(index=False, name=None)
    )
    note = ""
    if len(frame) > limit:
        note = f"<p class=\"note\">Showing first {limit:,} of {len(frame):,} rows.</p>"
    return (
        f"<section><h2>{escape(title)}</h2>{note}"
        f"<table><thead><tr>{headers}</tr></thead><tbody>{body}</tbody></table></section>"
    )


def _portfolio_sections(title: str, portfolio: SerializedPortfolioView | None) -> str:
    if portfolio is None:
        return ""
    return "\n".join(
        section
        for section in [
            _table_from_series(f"{title} Stats", portfolio.stats()),
            _table_from_series(f"{title} Trade Stats", portfolio.trades.stats()),
            _table_from_mapping(f"{title} Risk Metrics", portfolio.risk_metrics),
            _table_from_dataframe(f"{title} Trades", portfolio.trades.records_readable),
        ]
        if section
    )


def _build_report_html(
    *,
    context: BacktestReportRenderContext,
    strategy_name: str,
    dataset_name: str,
    metrics: dict[str, Any],
    metrics_path: Path | None,
    manifest_path: Path | None,
    report_payload_path: Path,
    parameters: dict[str, Any],
) -> str:
    shared_config = parameters.get("shared_config", {}) if isinstance(parameters, dict) else {}
    generated_at = datetime.now().isoformat(timespec="seconds")
    artifact_rows = {
        "metrics": str(metrics_path) if metrics_path else None,
        "manifest": str(manifest_path) if manifest_path else None,
        "report payload": str(report_payload_path),
    }
    summary = {
        "strategy": strategy_name,
        "dataset": dataset_name,
        "generated at": generated_at,
        "stock universe": shared_config.get("stock_codes"),
        "initial cash": shared_config.get("initial_cash"),
        "fees": shared_config.get("fees"),
    }
    sections = [
        _table_from_mapping("Run Summary", summary),
        _table_from_mapping("Canonical Metrics", metrics),
        _table_from_mapping("Artifacts", artifact_rows),
        _portfolio_sections("Kelly Portfolio", context.kelly_portfolio),
        _portfolio_sections("Initial Portfolio", context.initial_portfolio),
    ]
    if context.all_entries is not None:
        sections.append(_table_from_dataframe("Entry Signal Counts", context.all_entries))
    body = "\n".join(section for section in sections if section)
    title = f"{strategy_name} Backtest Report"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{ color-scheme: light; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    body {{ margin: 0; background: #f7f8fa; color: #1f2933; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 32px 24px 48px; }}
    header {{ margin-bottom: 24px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; font-weight: 700; }}
    h2 {{ margin: 0 0 12px; font-size: 18px; }}
    p {{ margin: 0; color: #52606d; }}
    section {{ background: #ffffff; border: 1px solid #d9e2ec; border-radius: 6px; margin: 16px 0; padding: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e4e7eb; padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ width: 260px; color: #334e68; font-weight: 600; }}
    thead th {{ width: auto; background: #f0f4f8; }}
    .note {{ margin: 0 0 8px; font-size: 12px; color: #627d98; }}
  </style>
</head>
<body>
  <main data-report-renderer="{StaticHtmlReportRenderer.renderer_name}">
    <header>
      <h1>{escape(title)}</h1>
      <p>Generated from canonical bt report payload without a notebook runtime.</p>
    </header>
    {body}
  </main>
</body>
</html>
"""


__all__ = [
    "BacktestReportPathPlanner",
    "BacktestReportPaths",
    "StaticHtmlReportRenderer",
]
