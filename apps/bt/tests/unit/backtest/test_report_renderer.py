"""Static HTML report renderer tests."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.domains.backtest.core.report_renderer import (
    StaticHtmlReportRenderer,
    _chart_axis_label,
    _chart_axis_value_label,
    _chart_tick_indices,
    _chart_tick_values,
    _entry_signal_counts_section,
    _load_json,
    _numeric_series,
    _resolve_report_data_scope_name,
    _series_chart_section,
)


def _allowed_tmp_output_dir(tmp_path: Path) -> Path:
    output_dir = Path("/tmp") / f"trading25-report-renderer-{tmp_path.name}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def test_static_html_report_renderer_plans_artifact_paths(tmp_path: Path) -> None:
    output_dir = _allowed_tmp_output_dir(tmp_path)
    renderer = StaticHtmlReportRenderer(output_dir)

    paths = renderer.plan_report_paths(
        {"shared_config": {"dataset": "demo-dataset"}},
        strategy_name="demo_strategy",
        output_filename="demo_result",
    )

    assert paths.html_path == output_dir / "backtest/demo_strategy/demo_result.html"
    assert paths.metrics_path == paths.html_path.with_suffix(".metrics.json")
    assert paths.manifest_path == paths.html_path.with_suffix(".manifest.json")
    assert paths.simulation_payload_path == paths.html_path.with_suffix(".simulation.pkl")
    assert paths.report_payload_path == paths.html_path.with_suffix(".report.json")


def test_static_html_report_renderer_uses_universe_preset_for_market_result_filename(
    tmp_path: Path,
) -> None:
    output_dir = _allowed_tmp_output_dir(tmp_path)
    renderer = StaticHtmlReportRenderer(output_dir)

    paths = renderer.plan_report_paths(
        {"shared_config": {"data_source": "market", "universe_preset": "primeExTopix500"}},
        strategy_name="demo_strategy",
    )

    assert paths.html_path.parent == output_dir / "backtest/demo_strategy"
    assert paths.html_path.name.startswith("primeExTopix500_")
    assert paths.html_path.suffix == ".html"


def test_static_html_report_renderer_resolves_dataset_snapshot_result_filename(
    tmp_path: Path,
) -> None:
    output_dir = _allowed_tmp_output_dir(tmp_path)
    renderer = StaticHtmlReportRenderer(output_dir)

    paths = renderer.plan_report_paths(
        {
            "shared_config": {
                "data_source": "dataset_snapshot",
                "dataset_snapshot": "dataset/demo-snapshot.db",
            }
        },
        strategy_name="demo_strategy",
    )

    assert paths.html_path.name.startswith("demo-snapshot_")


def test_static_html_report_renderer_rejects_invalid_paths_and_filenames(
    tmp_path: Path,
) -> None:
    renderer = StaticHtmlReportRenderer(_allowed_tmp_output_dir(tmp_path))

    with pytest.raises(ValueError, match="Invalid output directory path"):
        StaticHtmlReportRenderer("../unsafe")
    with pytest.raises(ValueError, match="Invalid filename"):
        renderer.plan_report_paths({}, strategy_name="demo", output_filename="bad name")
    with pytest.raises(ValueError, match="Filename contains invalid characters"):
        renderer.plan_report_paths({}, strategy_name="demo", output_filename="bad..name")
    with pytest.raises(ValueError, match="Filename is too long"):
        renderer.plan_report_paths({}, strategy_name="demo", output_filename="a" * 101)


def test_static_html_report_renderer_scope_name_fallbacks() -> None:
    assert _resolve_report_data_scope_name({}) == "unknown"
    assert _resolve_report_data_scope_name({"dataset": "dataset/legacy.db"}) == "legacy"
    assert _resolve_report_data_scope_name("invalid") == "unknown"  # type: ignore[arg-type]


def test_static_html_report_renderer_load_json_ignores_invalid_sources(tmp_path: Path) -> None:
    corrupt_path = tmp_path / "corrupt.json"
    corrupt_path.write_text("{", encoding="utf-8")
    list_path = tmp_path / "list.json"
    list_path.write_text("[1]", encoding="utf-8")

    assert _load_json(None) == {}
    assert _load_json(corrupt_path) == {}
    assert _load_json(list_path) == {}


def test_static_html_report_renderer_writes_payload_driven_html(tmp_path: Path) -> None:
    output_dir = _allowed_tmp_output_dir(tmp_path)
    renderer = StaticHtmlReportRenderer(output_dir)
    report_payload_path = output_dir / "result.report.json"
    metrics_path = output_dir / "result.metrics.json"
    manifest_path = output_dir / "result.manifest.json"
    html_path = output_dir / "result.html"
    chart_index = [
        "2026-01-01T00:00:00",
        "2026-02-01T00:00:00",
        "2026-03-01T00:00:00",
        "2026-04-01T00:00:00",
        "2026-05-01T00:00:00",
        "2026-06-01T00:00:00",
        "2026-07-01T00:00:00",
        "2026-08-01T00:00:00",
        "2026-09-01T00:00:00",
    ]

    report_payload_path.write_text(
        json.dumps(
            {
                "allocation_info": {"kind": "scalar", "payload": 0.5},
                "entry_signal_counts": {
                    "index": ["2026-01-01"],
                    "values": [3],
                },
                "kelly_portfolio": {
                    "value_series": {
                        "index": chart_index,
                        "values": [
                            10_000_000.0,
                            12_500_000.0,
                            18_200_000.0,
                            25_000_000.0,
                            42_000_000.0,
                            70_000_000.0,
                            105_000_000.0,
                            177_875_095.8411,
                            152_906_444.5238,
                        ],
                    },
                    "drawdown_series": {
                        "index": chart_index,
                        "values": [0.0, -0.2, -2.3, -4.1, -7.5, -12.0, -8.5, -3.0, -6.2],
                    },
                    "returns_series": {
                        "index": chart_index,
                        "values": [0.0, 0.035, -0.022, 0.05, 0.08, 0.12, -0.04, 0.18, -0.03],
                    },
                    "trade_records": [{"symbol": "7203", "pnl": 12.5}],
                    "trade_stats": [{"metric": "Total Trades", "value": 1}],
                    "final_stats": [{"metric": "Total Return [%]", "value": 12.3}],
                    "risk_metrics": {"sharpe_ratio": 1.4},
                },
                "initial_portfolio": None,
            }
        ),
        encoding="utf-8",
    )
    metrics_path.write_text(
        json.dumps({"total_return": 12.3, "trade_count": 1}),
        encoding="utf-8",
    )

    rendered_path = renderer.render_report(
        report_payload_path=report_payload_path,
        html_path=html_path,
        strategy_name="demo_strategy",
        dataset_name="demo-dataset",
        metrics_path=metrics_path,
        manifest_path=manifest_path,
        parameters={"shared_config": {"initial_cash": 1_000_000}},
    )

    html = rendered_path.read_text(encoding="utf-8")
    assert rendered_path == html_path
    assert 'data-report-renderer="static_html"' in html
    assert "demo_strategy Backtest Report" in html
    assert "Canonical Metrics" in html
    assert "Kelly Portfolio Stats" in html
    assert "Kelly Portfolio Equity Curve" in html
    assert "Kelly Portfolio Drawdown" in html
    assert '<svg viewBox="0 0 760 260"' in html
    assert 'class="chart-y-label"' in html
    assert 'class="chart-x-grid"' in html
    assert ">177.9M</text>" in html
    assert html.count('class="chart-x-label"') >= 8
    assert "2026-01-01T00:00:00" not in html
    assert "2026/01" in html
    assert "2026/09" in html
    assert "7203" in html


def test_static_html_report_renderer_entry_signal_counts_show_distribution_stats(
    tmp_path: Path,
) -> None:
    output_dir = _allowed_tmp_output_dir(tmp_path)
    renderer = StaticHtmlReportRenderer(output_dir)
    report_payload_path = output_dir / "entry-counts.report.json"
    html_path = output_dir / "entry-counts.html"
    dates = [f"2026-01-{day:02d}T00:00:00" for day in range(1, 31)]
    values = [0] * 25 + [3, 0, 7, 0, 0]

    report_payload_path.write_text(
        json.dumps(
            {
                "allocation_info": {"kind": "scalar", "payload": 0.5},
                "entry_signal_counts": {"index": dates, "values": values},
                "kelly_portfolio": None,
                "initial_portfolio": None,
            }
        ),
        encoding="utf-8",
    )

    rendered_path = renderer.render_report(
        report_payload_path=report_payload_path,
        html_path=html_path,
        strategy_name="demo_strategy",
        dataset_name="demo-dataset",
        parameters={},
    )

    html = rendered_path.read_text(encoding="utf-8")
    assert "Entry Signal Counts" in html
    assert "Daily Signal Count Statistics" in html
    assert "<th>Metric</th><th>Value</th>" in html
    assert "<th>Total Days</th><td>30</td>" in html
    assert "<th>Total Signals</th><td>10</td>" in html
    assert "<th>Non-zero Days</th><td>2</td>" in html
    assert "<th>Zero Days</th><td>28</td>" in html
    assert "<th>Mean per Day</th><td>0.3333</td>" in html
    assert "<th>Median per Day</th><td>0.0000</td>" in html
    assert "<th>Max per Day</th><td>7</td>" in html
    assert "Daily Signal Count Distribution" in html
    assert "<th>Signal Count</th><th>Days</th><th>Share</th>" in html
    assert "<td>0</td><td>28</td><td>93.33%</td>" in html
    assert "<td>3</td><td>1</td><td>3.33%</td>" in html
    assert "<td>7</td><td>1</td><td>3.33%</td>" in html
    assert "2026-01-26T00:00:00" not in html
    assert "Showing first 25 of 30 rows" not in html


def test_static_html_report_renderer_entry_signal_counts_empty_and_fallbacks() -> None:
    assert _entry_signal_counts_section(pd.DataFrame()) == ""
    zero_only = _entry_signal_counts_section(
        pd.DataFrame({"signal_count": [0, 0]}, index=["2026-01-01", "2026-01-02"])
    )
    assert "<th>Total Days</th><td>2</td>" in zero_only
    assert "<th>Total Signals</th><td>0</td>" in zero_only
    assert "<td>0</td><td>2</td><td>100.00%</td>" in zero_only

    fallback = _entry_signal_counts_section(
        pd.DataFrame({"other_count": [1, 2, 3]}, index=["a", "b", "c"]),
        limit=2,
    )

    assert "Showing first 2 of 3 rows" in fallback
    assert "other_count" in fallback


def test_static_html_report_renderer_chart_helper_edge_cases() -> None:
    assert _numeric_series(pd.Series(dtype=float)).empty
    assert _numeric_series(pd.Series(["x", None])).empty
    assert _series_chart_section("Empty", pd.Series(dtype=float), accent="#000") == ""
    assert "5.0" in _series_chart_section(
        "Constant",
        pd.Series([5.0], index=["not-a-date"]),
        accent="#000",
    )

    assert _chart_tick_values([5.0]) == [5.0]
    assert _chart_tick_indices(0) == []
    assert _chart_tick_indices(3) == [0, 1, 2]
    assert _chart_axis_value_label(1_200_000_000) == "1.2B"
    assert _chart_axis_value_label(12_300) == "12.3K"
    assert _chart_axis_value_label(120) == "120"
    assert _chart_axis_value_label(12.34) == "12.3"
    assert _chart_axis_value_label(1.20) == "1.2"
    assert _chart_axis_label("not-a-date") == "not-a-date"
