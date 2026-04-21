"""Static HTML report renderer tests."""

from __future__ import annotations

import json
from pathlib import Path

from src.domains.backtest.core.report_renderer import StaticHtmlReportRenderer


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


def test_static_html_report_renderer_writes_payload_driven_html(tmp_path: Path) -> None:
    output_dir = _allowed_tmp_output_dir(tmp_path)
    renderer = StaticHtmlReportRenderer(output_dir)
    report_payload_path = output_dir / "result.report.json"
    metrics_path = output_dir / "result.metrics.json"
    manifest_path = output_dir / "result.manifest.json"
    html_path = output_dir / "result.html"

    report_payload_path.write_text(
        json.dumps(
            {
                "allocation_info": {"kind": "scalar", "payload": 0.5},
                "entry_signal_counts": {
                    "index": ["2026-01-01"],
                    "values": [3],
                },
                "kelly_portfolio": {
                    "value_series": {"index": ["2026-01-01"], "values": [100.0]},
                    "drawdown_series": {"index": ["2026-01-01"], "values": [0.0]},
                    "returns_series": {"index": ["2026-01-01"], "values": [0.01]},
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
    assert "7203" in html
