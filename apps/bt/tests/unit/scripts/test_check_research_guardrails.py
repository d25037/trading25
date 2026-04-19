"""Tests for scripts/check-research-guardrails.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "check-research-guardrails.py"
    spec = importlib.util.spec_from_file_location("check_research_guardrails", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load check_research_guardrails module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_research_guardrails"] = module
    spec.loader.exec_module(module)
    return module


def test_find_guardrail_findings_detects_viewer_only_regressions(tmp_path: Path) -> None:
    module = _load_module()
    relative_path = Path("apps/bt/notebooks/playground/demo_playground.py")
    text = """
import marimo

@app.cell
def _():
    from src.domains.analytics.demo import run_demo_research
    result = run_demo_research("/tmp/market.duckdb")
    return result
""".strip()

    findings = module.find_guardrail_findings_in_text(
        tmp_path,
        relative_path,
        text,
    )

    rule_names = {finding.rule_name for finding in findings}
    assert "missing-shared-viewer-import" in rule_names
    assert "missing-runner-path" in rule_names
    assert "direct-research-import" in rule_names
    assert "direct-research-call" in rule_names


def test_scan_playground_files_detects_missing_runner_script(tmp_path: Path) -> None:
    module = _load_module()
    notebook = tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    notebook.parent.mkdir(parents=True)
    notebook.write_text(
        """
import marimo

@app.cell
def _():
    from src.shared.research_notebook_viewer import build_bundle_viewer_controls
    return build_bundle_viewer_controls

@app.cell
def _(build_bundle_viewer_controls, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id="",
        latest_bundle_path_str="",
        runner_path="apps/bt/scripts/research/run_missing.py",
    )
    return bundle_path, run_id
""".strip()
    )

    findings = module.scan_playground_files(
        tmp_path,
        [Path("apps/bt/notebooks/playground/demo_playground.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "missing-runner-script"


def test_scan_playground_files_detects_missing_docs_readme(tmp_path: Path) -> None:
    module = _load_module()
    runner = tmp_path / "apps" / "bt" / "scripts" / "research" / "run_demo.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("#!/usr/bin/env python3\n")

    notebook = tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    notebook.parent.mkdir(parents=True, exist_ok=True)
    notebook.write_text(
        """
import marimo

@app.cell
def _():
    from src.shared.research_notebook_viewer import build_bundle_viewer_controls
    return build_bundle_viewer_controls

@app.cell
def _(build_bundle_viewer_controls, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id="",
        latest_bundle_path_str="",
        runner_path="apps/bt/scripts/research/run_demo.py",
        docs_readme_path="apps/bt/docs/experiments/demo/README.md",
    )
    return bundle_path, run_id
""".strip()
    )

    findings = module.scan_playground_files(
        tmp_path,
        [Path("apps/bt/notebooks/playground/demo_playground.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "missing-docs-readme"


def test_main_accepts_clean_viewer_only_notebook(tmp_path: Path, capsys) -> None:
    module = _load_module()
    runner = tmp_path / "apps" / "bt" / "scripts" / "research" / "run_demo.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("#!/usr/bin/env python3\n")

    notebook = tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    notebook.parent.mkdir(parents=True, exist_ok=True)
    notebook.write_text(
        """
import marimo

@app.cell
def _():
    from src.shared.research_notebook_viewer import build_bundle_viewer_controls
    return build_bundle_viewer_controls

@app.cell
def _(build_bundle_viewer_controls, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id="",
        latest_bundle_path_str="",
        runner_path="apps/bt/scripts/research/run_demo.py",
    )
    return bundle_path, run_id
""".strip()
    )

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[research-guardrails] OK" in captured.out
