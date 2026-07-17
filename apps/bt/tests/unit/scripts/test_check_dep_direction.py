from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def test_allowlist_lookup_does_not_use_early_closing_pipe() -> None:
    repo_root = Path(__file__).resolve().parents[5]
    script = (repo_root / "scripts" / "check-dep-direction.sh").read_text()

    assert 'echo "$allowlist_clean" | grep -qxF' not in script


def test_large_allowlist_does_not_misclassify_allowed_file(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[5]
    script_source = repo_root / "scripts" / "check-dep-direction.sh"
    script_target = tmp_path / "scripts" / "check-dep-direction.sh"
    script_target.parent.mkdir(parents=True)
    shutil.copy2(script_source, script_target)

    allowlist_entries: list[str] = []
    for index in range(500):
        relative_path = f"apps/ts/packages/contracts/generated/allowed-contract-reference-{index:04d}.ts"
        target = tmp_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("export type Contract = BacktestClient;\n")
        allowlist_entries.append(relative_path)

    (tmp_path / "scripts" / "dep-direction-allowlist.txt").write_text(
        "\n".join(allowlist_entries) + "\n"
    )

    result = subprocess.run(
        ["bash", str(script_target)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "500 allowed, 0 violations, 0 stale" in result.stdout
