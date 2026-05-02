"""Static backtest report path planning and HTML rendering."""

from __future__ import annotations

import json
import math
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
        data_scope_name = _resolve_report_data_scope_name(shared_config)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        from src.shared.paths import get_backtest_results_dir

        external_dir = get_backtest_results_dir()
        if str(self.output_dir).startswith(str(external_dir)):
            strategy_dir_path = strategy_name
        else:
            strategy_dir_path = f"backtest/{strategy_name}"

        return strategy_dir_path, f"{data_scope_name}_{timestamp}"

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


def _resolve_report_data_scope_name(shared_config: dict[str, Any]) -> str:
    if not isinstance(shared_config, dict):
        return "unknown"
    if shared_config.get("data_source") == "dataset_snapshot":
        snapshot_id = canonicalize_dataset_snapshot_id(shared_config.get("dataset_snapshot"))
        return snapshot_id or "unknown"
    universe_preset = shared_config.get("universe_preset")
    if isinstance(universe_preset, str) and universe_preset.strip():
        return universe_preset.strip()
    legacy_dataset = canonicalize_dataset_snapshot_id(shared_config.get("dataset"))
    return legacy_dataset or "unknown"


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


def _entry_signal_counts_section(frame: pd.DataFrame, *, limit: int = 25) -> str:
    if frame.empty:
        return ""
    if "signal_count" not in frame.columns:
        return _table_from_dataframe("Entry Signal Counts", frame, limit=limit)

    counts = pd.to_numeric(frame["signal_count"], errors="coerce").fillna(0).astype(float)
    total_days = int(len(counts))
    nonzero_days = int((counts > 0).sum())
    zero_days = total_days - nonzero_days
    total_signals = float(counts.sum())
    summary = {
        "Total Days": total_days,
        "Total Signals": int(total_signals) if total_signals.is_integer() else total_signals,
        "Non-zero Days": nonzero_days,
        "Zero Days": zero_days,
        "Non-zero Day Rate": _format_percent(nonzero_days / total_days),
        "Mean per Day": float(counts.mean()),
        "Median per Day": float(counts.median()),
        "P75 per Day": float(counts.quantile(0.75)),
        "P90 per Day": float(counts.quantile(0.90)),
        "P95 per Day": float(counts.quantile(0.95)),
        "Max per Day": int(counts.max()) if float(counts.max()).is_integer() else float(counts.max()),
    }

    frequency = counts.value_counts().sort_index()
    limited_frequency = frequency.head(limit)
    if len(frequency) > limit:
        note = (
            f'<p class="note">Showing first {limit:,} signal-count buckets '
            f'of {len(frequency):,}.</p>'
        )
    else:
        note = f'<p class="note">Showing {len(frequency):,} signal-count buckets.</p>'

    summary_rows = "\n".join(
        "<tr>"
        f"<th>{escape(str(metric))}</th>"
        f"<td>{escape(_format_value(value))}</td>"
        "</tr>"
        for metric, value in summary.items()
    )
    summary_table = (
        "<h3>Daily Signal Count Statistics</h3>"
        "<table><thead><tr><th>Metric</th><th>Value</th></tr></thead>"
        f"<tbody>{summary_rows}</tbody></table>"
    )
    rows = "\n".join(
        "<tr>"
        f"<td>{_format_signal_count_bucket(signal_count)}</td>"
        f"<td>{int(day_count):,}</td>"
        f"<td>{_format_percent(int(day_count) / total_days)}</td>"
        "</tr>"
        for signal_count, day_count in limited_frequency.items()
    )
    distribution_table = (
        f"<h3>Daily Signal Count Distribution</h3>{note}"
        "<table><thead><tr><th>Signal Count</th><th>Days</th><th>Share</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )
    return f"<section><h2>Entry Signal Counts</h2>{summary_table}{distribution_table}</section>"


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def _format_signal_count_bucket(value: Any) -> str:
    coerced = float(value)
    if coerced.is_integer():
        return f"{int(coerced):,}"
    return _format_value(coerced)


def _numeric_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return pd.Series(dtype=float)
    return numeric.astype(float)


def _chart_y(value: float, *, min_value: float, max_value: float, top: float, height: float) -> float:
    value_range = max_value - min_value
    if math.isclose(value_range, 0.0):
        return top + height / 2
    return top + ((max_value - value) / value_range) * height


def _chart_tick_values(values: list[float], *, max_ticks: int = 5) -> list[float]:
    min_value = min(values)
    max_value = max(values)
    value_range = max_value - min_value
    if math.isclose(value_range, 0.0):
        return [min_value]
    return [
        min_value + (value_range * index / (max_ticks - 1))
        for index in range(max_ticks)
    ]


def _chart_tick_indices(length: int, *, max_ticks: int = 5) -> list[int]:
    if length <= 0:
        return []
    if length <= max_ticks:
        return list(range(length))
    denominator = max_ticks - 1
    return sorted(
        {
            round(index * (length - 1) / denominator)
            for index in range(max_ticks)
        }
    )


def _chart_axis_value_label(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.1f}K"
    if abs_value >= 100:
        return f"{value:.0f}"
    if abs_value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _chart_axis_label(value: Any) -> str:
    try:
        timestamp = pd.to_datetime(value, errors="coerce")
    except Exception:
        timestamp = pd.NaT
    if pd.notna(timestamp):
        return f"{timestamp.year:04d}/{timestamp.month:02d}"
    return str(value)


def _series_chart_section(title: str, series: pd.Series, *, accent: str) -> str:
    numeric = _numeric_series(series)
    if numeric.empty:
        return ""

    width = 760
    height = 260
    left = 74
    right = 28
    top = 28
    bottom = 40
    plot_width = width - left - right
    plot_height = height - top - bottom
    values = numeric.tolist()
    min_value = min(values)
    max_value = max(values)

    points: list[str] = []
    denominator = max(len(values) - 1, 1)
    for index, value in enumerate(values):
        x = left + (index / denominator) * plot_width
        y = _chart_y(
            value,
            min_value=min_value,
            max_value=max_value,
            top=top,
            height=plot_height,
        )
        points.append(f"{x:.2f},{y:.2f}")

    y_ticks = "\n".join(
        f'    <line class="chart-grid" x1="{left}" y1="{y:.2f}" '
        f'x2="{width - right}" y2="{y:.2f}"></line>\n'
        f'    <text class="chart-y-label" x="{left - 10}" y="{y + 4:.2f}">'
        f"{escape(_chart_axis_value_label(value))}</text>"
        for value in reversed(_chart_tick_values(values))
        for y in [
            _chart_y(
                value,
                min_value=min_value,
                max_value=max_value,
                top=top,
                height=plot_height,
            )
        ]
    )
    x_tick_indices = _chart_tick_indices(len(values), max_ticks=9)
    x_grid = "\n".join(
        f'    <line class="chart-x-grid" x1="{x:.2f}" y1="{top}" '
        f'x2="{x:.2f}" y2="{height - bottom}"></line>'
        for index in x_tick_indices
        for x in [left + (index / denominator) * plot_width]
    )
    x_ticks = "\n".join(
        f'    <text class="chart-x-label" x="{x:.2f}" y="{height - 8}">'
        f"{escape(_chart_axis_label(numeric.index[index]))}</text>"
        for index in x_tick_indices
        for x in [left + (index / denominator) * plot_width]
    )
    summary = (
        f"Start {escape(_format_value(values[0]))} | "
        f"End {escape(_format_value(values[-1]))} | "
        f"Min {escape(_format_value(min_value))} | "
        f"Max {escape(_format_value(max_value))}"
    )
    points_attr = " ".join(points)
    return f"""
<section class="chart-section">
  <h2>{escape(title)}</h2>
  <p class="note">{summary}</p>
  <svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">
{y_ticks}
{x_grid}
    <line class="chart-axis" x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}"></line>
    <line class="chart-axis" x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}"></line>
    <polyline class="chart-line" stroke="{accent}" points="{points_attr}"></polyline>
{x_ticks}
  </svg>
</section>"""


def _kelly_chart_sections(portfolio: SerializedPortfolioView | None) -> str:
    if portfolio is None:
        return ""
    return "\n".join(
        section
        for section in [
            _series_chart_section(
                "Kelly Portfolio Equity Curve",
                portfolio.value(),
                accent="#1d4ed8",
            ),
            _series_chart_section(
                "Kelly Portfolio Drawdown",
                portfolio.drawdown(),
                accent="#b42318",
            ),
        ]
        if section
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
        _kelly_chart_sections(context.kelly_portfolio),
        _portfolio_sections("Kelly Portfolio", context.kelly_portfolio),
        _portfolio_sections("Initial Portfolio", context.initial_portfolio),
    ]
    if context.all_entries is not None:
        sections.append(_entry_signal_counts_section(context.all_entries))
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
    h3 {{ margin: 18px 0 10px; font-size: 15px; }}
    p {{ margin: 0; color: #52606d; }}
    section {{ background: #ffffff; border: 1px solid #d9e2ec; border-radius: 6px; margin: 16px 0; padding: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e4e7eb; padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ width: 260px; color: #334e68; font-weight: 600; }}
    thead th {{ width: auto; background: #f0f4f8; }}
    .note {{ margin: 0 0 8px; font-size: 12px; color: #627d98; }}
    svg {{ display: block; width: 100%; height: auto; }}
    .chart-section {{ overflow: hidden; }}
    .chart-grid {{ stroke: #e4e7eb; stroke-width: 1; }}
    .chart-x-grid {{ stroke: #edf1f5; stroke-width: 1; }}
    .chart-axis {{ stroke: #cbd5e1; stroke-width: 1; }}
    .chart-line {{ fill: none; stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; }}
    .chart-y-label {{ fill: #627d98; font-size: 11px; text-anchor: end; }}
    .chart-x-label {{ fill: #627d98; font-size: 11px; text-anchor: middle; }}
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
