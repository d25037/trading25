"""
Backtest manifest tests
"""

import json
from pathlib import Path

from src.domains.backtest.core.runner import BacktestRunner


def test_write_manifest_creates_file(tmp_path: Path):
    runner = BacktestRunner()
    html_path = tmp_path / "result.html"
    html_path.write_text("<html></html>", encoding="utf-8")
    metrics_path = tmp_path / "result.metrics.json"
    metrics_path.write_text("{}", encoding="utf-8")
    report_data_path = tmp_path / "result.report.json"
    report_data_path.write_text("{}", encoding="utf-8")

    params = {"shared_config": {"dataset": "sample"}}

    manifest_path = runner._write_manifest(
        html_path=html_path,
        parameters=params,
        strategy_name="test_strategy",
        dataset_name="sample",
        elapsed_time=1.23,
        metrics_path=metrics_path,
        html_generated=True,
        render_status="completed",
        report_data_path=report_data_path,
    )

    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert data["strategy_name"] == "test_strategy"
    assert data["dataset_name"] == "sample"
    assert data["html_path"] == str(html_path)
    assert data["parameters"] == params
    assert data["artifact_contract"]["canonical_result_source"] == "metrics.json"
    assert data["artifact_contract"]["artifact_catalog_source"] == "manifest.json"
    assert data["artifact_contract"]["presentation_source"] == "result.html"
    assert data["core_artifacts"]["manifest_json"]["path"] == str(manifest_path)
    assert data["core_artifacts"]["metrics_json"] == {
        "path": str(metrics_path),
        "role": "canonical_summary",
        "available": True,
    }
    assert data["core_artifacts"]["report_data_json"] == {
        "path": str(report_data_path),
        "role": "renderer_input",
        "available": True,
    }
    assert data["presentation_artifacts"]["result_html"] == {
        "path": str(html_path),
        "role": "presentation_only",
        "available": True,
        "render_status": "completed",
        "render_error": None,
    }
