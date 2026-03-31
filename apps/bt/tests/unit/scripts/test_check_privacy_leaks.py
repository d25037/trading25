"""Tests for scripts/check-privacy-leaks.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "check-privacy-leaks.py"
    spec = importlib.util.spec_from_file_location("check_privacy_leaks", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load check_privacy_leaks module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_privacy_leaks"] = module
    spec.loader.exec_module(module)
    return module


def test_find_privacy_leaks_detects_repo_and_local_data_paths() -> None:
    module = _load_module()
    text = "\n".join(
        [
            'note = "/Users/alice/dev/trading25/apps/bt/notebooks/playground/demo.py"',
            'db = "/Users/alice/.local/share/trading25/market-timeseries/market.duckdb"',
            'agent = "/Users/alice/.codex/worktrees/ab12/trading25/apps/bt/tests/unit/test_x.py"',
        ]
    )

    findings = module.find_privacy_leaks_in_text(
        Path("docs/example.md"),
        text,
    )

    assert len(findings) == 3
    assert findings[0].pattern_name == "repo-local-path"
    assert findings[1].pattern_name == "trading25-data-home-path"
    assert findings[2].pattern_name == "codex-or-agents-home-path"


def test_find_privacy_leaks_ignores_portable_and_generic_example_paths() -> None:
    module = _load_module()
    text = "\n".join(
        [
            'db = "~/.local/share/trading25/market-timeseries/market.duckdb"',
            'sample = "/Users/john/project/file.db"',
            'skill = ".codex/skills/bt-marimo-playground/SKILL.md"',
        ]
    )

    findings = module.find_privacy_leaks_in_text(
        Path("docs/example.md"),
        text,
    )

    assert findings == []


def test_scan_files_skips_checker_source_and_test_files(tmp_path: Path) -> None:
    module = _load_module()
    script_file = tmp_path / "scripts" / "check-privacy-leaks.py"
    script_file.parent.mkdir(parents=True)
    script_file.write_text('PATTERN = "/Users/alice/dev/trading25"')

    test_file = tmp_path / "apps" / "bt" / "tests" / "unit" / "scripts" / "test_check_privacy_leaks.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text('EXAMPLE = "/Users/alice/.codex/worktrees/ab12/trading25"')

    findings = module.scan_files(
        tmp_path,
        [
            Path("scripts/check-privacy-leaks.py"),
            Path("apps/bt/tests/unit/scripts/test_check_privacy_leaks.py"),
        ],
    )

    assert findings == []


def test_main_reports_findings_for_explicit_files(tmp_path: Path, capsys) -> None:
    module = _load_module()
    target = tmp_path / "issues" / "example.md"
    target.parent.mkdir(parents=True)
    target.write_text('ref: "/Users/alice/dev/trading25/apps/bt/src/example.py"\n')

    exit_code = module.main(["--root", str(tmp_path), "issues/example.md"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "privacy-leak-check" in captured.err
    assert "issues/example.md:1" in captured.err
    assert "/Users/alice/dev/trading25/apps/bt/src/example.py" in captured.err
