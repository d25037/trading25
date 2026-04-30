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


def test_scan_research_files_detects_legacy_playground_file(tmp_path: Path) -> None:
    module = _load_module()
    notebook = tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    notebook.parent.mkdir(parents=True)
    notebook.write_text("# legacy notebook\n", encoding="utf-8")

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/notebooks/playground/demo_playground.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "legacy-playground-file"


def test_scan_research_files_detects_legacy_notebook_doc_reference(tmp_path: Path) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

```bash
uv run --project apps/bt marimo edit \\
  apps/bt/notebooks/playground/demo_playground.py
```
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/docs/experiments/demo/README.md")],
    )

    rule_names = {finding.rule_name for finding in findings}
    assert "legacy-notebook-reference" in rule_names
    assert "legacy-marimo-command" in rule_names


def test_main_accepts_clean_runner_bundle_docs(tmp_path: Path, capsys) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Reproduce

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_demo.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。
""".strip(),
        encoding="utf-8",
    )

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[research-guardrails] OK" in captured.out


def test_scan_research_files_detects_incomplete_published_readout(tmp_path: Path) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision
- Keep as context.

### Main Findings
- Finding with numbers.
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/docs/experiments/demo/README.md")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "incomplete-published-readout"
    assert "caveats" in findings[0].message


def test_main_accepts_complete_published_readout(tmp_path: Path, capsys) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision
- Keep as context.

### Main Findings
- Finding with numbers.

### Interpretation
- This is observational.

### Production Implication
- Use only as a research lens.

### Caveats
- PIT-safe inputs still need cost checks.

### Source Artifacts
- `results.duckdb`
""".strip(),
        encoding="utf-8",
    )

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[research-guardrails] OK" in captured.out


def test_scan_research_files_requires_published_readout_section_content(tmp_path: Path) -> None:
    module = _load_module()
    readme = tmp_path / "apps" / "bt" / "docs" / "experiments" / "demo" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
# Demo

## Published Readout

### Decision

### Main Findings
- Finding with numbers.

### Interpretation
- This is observational.

### Production Implication
- Use only as a research lens.

### Caveats
- PIT-safe inputs still need cost checks.

### Source Artifacts
- `results.duckdb`
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/docs/experiments/demo/README.md")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "incomplete-published-readout"
    assert "decision" in findings[0].message


def test_scan_research_files_rejects_new_published_summary_without_reason(
    tmp_path: Path,
) -> None:
    module = _load_module()
    analytics_module = (
        tmp_path
        / "apps"
        / "bt"
        / "src"
        / "domains"
        / "analytics"
        / "demo.py"
    )
    analytics_module.parent.mkdir(parents=True)
    analytics_module.write_text(
        """
def write_demo_bundle():
    return write_research_bundle(
        summary_markdown="# Demo",
        published_summary=_build_published_summary(),
    )
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/src/domains/analytics/demo.py")],
    )

    assert len(findings) == 1
    assert findings[0].rule_name == "published-summary-without-fallback-reason"


def test_scan_research_files_accepts_published_summary_with_fallback_reason(
    tmp_path: Path,
) -> None:
    module = _load_module()
    analytics_module = (
        tmp_path
        / "apps"
        / "bt"
        / "src"
        / "domains"
        / "analytics"
        / "demo.py"
    )
    analytics_module.parent.mkdir(parents=True)
    analytics_module.write_text(
        """
def write_demo_bundle():
    return write_research_bundle(
        summary_markdown="# Demo",
        # bundle-structured-fallback: keep machine-readable payload for old bundles.
        published_summary=_build_published_summary(),
    )
""".strip(),
        encoding="utf-8",
    )

    findings = module.scan_research_files(
        tmp_path,
        [Path("apps/bt/src/domains/analytics/demo.py")],
    )

    assert findings == []


def test_main_detects_legacy_playground_by_default(tmp_path: Path, capsys) -> None:
    module = _load_module()
    notebook = tmp_path / "apps" / "bt" / "notebooks" / "playground" / "demo_playground.py"
    notebook.parent.mkdir(parents=True)
    notebook.write_text("# legacy notebook\n", encoding="utf-8")

    exit_code = module.main(["--root", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "legacy-playground-file" in captured.err
