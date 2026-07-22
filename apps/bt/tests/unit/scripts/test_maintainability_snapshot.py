"""Tests for the repository maintainability snapshot gate."""

from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType

import pytest
from ruamel.yaml import YAML


REPO_ROOT = Path(__file__).resolve().parents[5]
SCRIPT_PATH = REPO_ROOT / "scripts" / "maintainability_snapshot.py"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PREPUSH_SCRIPT = REPO_ROOT / "scripts" / "prepush-ci.sh"
RECOVERY_COMMAND = (
    "uv run --project apps/bt python scripts/maintainability_snapshot.py ..."
)


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "maintainability_snapshot_under_test",
        SCRIPT_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _init_repo(root: Path, source: bytes = b"def example():\n    return 1\n") -> None:
    source_path = root / "apps" / "bt" / "src" / "example.py"
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(source)
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", source_path], cwd=root, check=True)


def _run_script(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--root", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_python_version_guard_exits_two_with_exact_recovery_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()

    with pytest.raises(SystemExit) as error:
        module.require_supported_python((3, 9, 6))

    assert error.value.code == 2
    assert RECOVERY_COMMAND in capsys.readouterr().err


def test_python_parser_receives_relative_filename_and_python_312_feature_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_module()
    calls: list[tuple[str, str, tuple[int, int]]] = []
    real_parse = ast.parse

    def recording_parse(
        source: str,
        filename: str,
        mode: str = "exec",
        *,
        type_comments: bool = False,
        feature_version: tuple[int, int] | None = None,
    ) -> ast.AST:
        calls.append((filename, mode, feature_version))
        return real_parse(
            source,
            filename=filename,
            mode=mode,
            type_comments=type_comments,
            feature_version=feature_version,
        )

    monkeypatch.setattr(module.ast, "parse", recording_parse)

    module.collect_python_functions(
        Path("/repo/apps/bt/src/example.py"),
        "apps/bt/src/example.py",
        "def example():\n    return 1\n",
    )

    assert calls == [("apps/bt/src/example.py", "exec", (3, 12))]


def test_python_syntax_error_is_not_swallowed(tmp_path: Path) -> None:
    _init_repo(tmp_path, b"def broken(:\n    pass\n")

    result = _run_script(tmp_path)

    assert result.returncode == 1
    assert "SyntaxError" in result.stderr
    assert "apps/bt/src/example.py" in result.stderr


def test_invalid_utf8_source_fails_instead_of_being_replaced(tmp_path: Path) -> None:
    _init_repo(tmp_path, b"def example():\n    return '\xff'\n")

    result = _run_script(tmp_path)

    assert result.returncode == 1
    assert "UnicodeDecodeError" in result.stderr
    assert "apps/bt/src/example.py" in result.stderr


def test_worktree_collection_skips_deleted_tracked_and_includes_untracked(
    tmp_path: Path,
) -> None:
    _init_repo(tmp_path)
    tracked = tmp_path / "apps" / "bt" / "src" / "example.py"
    tracked.unlink()
    untracked = tmp_path / "apps" / "bt" / "src" / "replacement.py"
    untracked.write_text("def replacement():\n    return 2\n", encoding="utf-8")

    module = _load_module()

    assert module.git_worktree_source_files(tmp_path) == [untracked]


def test_check_mode_passes_without_rewriting_artifacts(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    json_path = tmp_path / "snapshot.json"
    markdown_path = tmp_path / "snapshot.md"
    generated = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
    )
    assert generated.returncode == 0, generated.stderr
    original_json = json_path.read_bytes()
    original_markdown = markdown_path.read_bytes()

    result = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
        "--check",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert json_path.read_bytes() == original_json
    assert markdown_path.read_bytes() == original_markdown


def test_check_mode_rejects_coordinated_json_and_markdown_date_drift(
    tmp_path: Path,
) -> None:
    _init_repo(tmp_path)
    json_path = tmp_path / "snapshot.json"
    markdown_path = tmp_path / "snapshot.md"
    generated = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
    )
    assert generated.returncode == 0, generated.stderr
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    payload["generated_on"] = "1999-12-31"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_lines = markdown_path.read_text(encoding="utf-8").splitlines()
    markdown_lines[0] = "# Maintainability Snapshot 1999-12-31"
    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    original_json = json_path.read_bytes()
    original_markdown = markdown_path.read_bytes()

    result = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
        "--check",
    )

    assert result.returncode == 1
    assert "maintainability snapshot drift" in result.stderr
    assert json_path.read_bytes() == original_json
    assert markdown_path.read_bytes() == original_markdown


def test_generated_artifacts_have_no_wall_clock_metadata(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    json_path = tmp_path / "snapshot.json"
    markdown_path = tmp_path / "snapshot.md"

    result = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
    )

    assert result.returncode == 0, result.stderr
    assert "generated_on" not in json.loads(json_path.read_text(encoding="utf-8"))
    assert (
        markdown_path.read_text(encoding="utf-8").splitlines()[0]
        == "# Maintainability Snapshot"
    )


def test_check_mode_reports_drift_and_does_not_rewrite_baseline(
    tmp_path: Path,
) -> None:
    _init_repo(tmp_path)
    json_path = tmp_path / "snapshot.json"
    markdown_path = tmp_path / "snapshot.md"
    generated = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
    )
    assert generated.returncode == 0, generated.stderr
    original_json = json_path.read_bytes()
    original_markdown = markdown_path.read_bytes()
    source_path = tmp_path / "apps" / "bt" / "src" / "example.py"
    source_path.write_text(
        "def example():\n    if True:\n        return 2\n",
        encoding="utf-8",
    )

    result = _run_script(
        tmp_path,
        "--json-out",
        str(json_path),
        "--md-out",
        str(markdown_path),
        "--check",
    )

    assert result.returncode == 1
    assert "maintainability snapshot drift" in result.stderr
    assert str(json_path) in result.stderr
    assert str(markdown_path) in result.stderr
    assert json_path.read_bytes() == original_json
    assert markdown_path.read_bytes() == original_markdown


def test_maintainability_snapshot_is_a_local_guardrail_not_a_ci_job() -> None:
    with CI_WORKFLOW.open(encoding="utf-8") as workflow_file:
        jobs = YAML(typ="safe").load(workflow_file)["jobs"]

    assert "maintainability" not in jobs
    assert "maintainability-python39" not in jobs


def test_prepush_runs_maintainability_check_through_bt_python() -> None:
    source = PREPUSH_SCRIPT.read_text(encoding="utf-8")

    assert '"quality:maintainability-snapshot"' in source
    assert (
        'uv run --project "${repo_root}/apps/bt" python '
        '"${repo_root}/scripts/maintainability_snapshot.py"'
    ) in source
    assert (
        '--json-out "${repo_root}/docs/maintainability-snapshot-latest.json"'
    ) in source
    assert '--check' in source
    assert source.index("  run_maintainability_guardrail\n") < source.index(
        "if ${docs_only}"
    )
