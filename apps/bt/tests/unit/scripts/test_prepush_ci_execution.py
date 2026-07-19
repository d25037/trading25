"""Executable regression coverage for forced pre-push CI tiers."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[5]


def _write_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _build_prepush_harness(
    tmp_path: Path,
) -> tuple[Path, str, Path, dict[str, str]]:
    repo = tmp_path / "repo"
    scripts_dir = repo / "scripts"
    ci_dir = scripts_dir / "ci"
    (repo / "apps" / "bt").mkdir(parents=True)
    shutil.copytree(
        REPO_ROOT / "scripts",
        scripts_dir,
        ignore=shutil.ignore_patterns("*"),
    )
    ci_dir.mkdir(parents=True, exist_ok=True)
    for relative_path in (
        "scripts/prepush-ci.sh",
        "scripts/ci/changed-scope.py",
        "scripts/ci/test_taxonomy.py",
    ):
        source = REPO_ROOT / relative_path
        destination = repo / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    _write_executable(
        scripts_dir / "check-privacy-leaks.py",
        "#!/usr/bin/env python3\n",
    )
    _write_executable(
        scripts_dir / "skills" / "audit_skills.py",
        "#!/usr/bin/env python3\n",
    )
    _write_executable(
        ci_dir / "test_targets.py",
        "#!/usr/bin/env python3\n"
        "print('tests/unit/domains/analytics/test_research_core.py')\n",
    )
    _write_executable(
        ci_dir / "research-test-targets.py",
        "#!/usr/bin/env python3\n",
    )
    _write_executable(
        scripts_dir / "bt-pytest.sh",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"${PREPUSH_TRACE_FILE}\"\n",
    )

    trace_path = tmp_path / "prepush-trace.log"
    trace_path.touch()
    bin_dir = tmp_path / "bin"
    _write_executable(
        bin_dir / "uv",
        "#!/usr/bin/env bash\nprintf 'uv %s\\n' \"$*\" >>\"${PREPUSH_TRACE_FILE}\"\n",
    )
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    (home_dir / ".bash_profile").write_text(
        f'export PATH="{bin_dir}:$PATH"\n', encoding="utf-8"
    )

    docs_note = repo / "docs" / "note.md"
    docs_note.parent.mkdir(parents=True, exist_ok=True)
    docs_note.write_text("baseline\n", encoding="utf-8")
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "prepush-ci@example.test"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Pre-push CI"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", "docs: add note"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    docs_note.write_text("modified\n", encoding="utf-8")

    env = os.environ.copy()
    env.update(
        {
            "PREPUSH_BASE_REF": base_sha,
            "PREPUSH_TRACE_FILE": str(trace_path),
            "UV_CACHE_DIR": str(tmp_path / "uv-cache"),
            "PATH": f"{bin_dir}{os.pathsep}{env['PATH']}",
            "HOME": str(home_dir),
        }
    )
    return repo, base_sha, trace_path, env


def test_docs_only_research_flag_prepares_and_runs_research_suite(
    tmp_path: Path,
) -> None:
    repo, base_sha, trace_path, env = _build_prepush_harness(tmp_path)
    result = subprocess.run(
        ["bash", "scripts/prepush-ci.sh", "--research"],
        cwd=repo,
        env=env | {"PREPUSH_BASE_REF": base_sha},
        capture_output=True,
        text=True,
        check=False,
    )

    trace = trace_path.read_text(encoding="utf-8")
    assert result.returncode == 0, result.stderr
    assert "docs-only change; no local CI tiers selected" not in result.stdout
    assert "==> [quality:research-guardrails]" in result.stdout
    assert "==> [bt-research-tests:fast]" in result.stdout
    assert "uv sync --locked" in trace
    assert "test_research_core.py" in trace


def test_plain_docs_only_change_keeps_early_pass(tmp_path: Path) -> None:
    repo, base_sha, trace_path, env = _build_prepush_harness(tmp_path)
    result = subprocess.run(
        ["bash", "scripts/prepush-ci.sh", "--skip-install"],
        cwd=repo,
        env=env | {"PREPUSH_BASE_REF": base_sha},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "docs-only change; no local CI tiers selected" in result.stdout
    assert "quality:research-guardrails" not in result.stdout
    assert "test_research_core.py" not in trace_path.read_text(encoding="utf-8")
