"""
Backtest manifest tests
"""

import json
from pathlib import Path

from src.backtest.runner import BacktestRunner


def test_write_manifest_creates_file(tmp_path: Path):
    runner = BacktestRunner()
    html_path = tmp_path / "result.html"
    html_path.write_text("<html></html>", encoding="utf-8")

    params = {"shared_config": {"dataset": "sample"}}

    manifest_path = runner._write_manifest(
        html_path=html_path,
        parameters=params,
        strategy_name="test_strategy",
        dataset_name="sample",
        elapsed_time=1.23,
    )

    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert data["strategy_name"] == "test_strategy"
    assert data["dataset_name"] == "sample"
    assert data["html_path"] == str(html_path)
    assert data["parameters"] == params
