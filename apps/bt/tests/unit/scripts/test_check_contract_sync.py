"""Behavior and contract tests for scripts/check-contract-sync.sh."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


REPO_ROOT = Path(__file__).resolve().parents[5]
SCRIPT = REPO_ROOT / "scripts" / "check-contract-sync.sh"


def _script_text() -> str:
    return SCRIPT.read_text()


def _write_executable(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")
    path.chmod(0o755)


@dataclass(frozen=True)
class ContractSyncHarness:
    repo_root: Path
    script: Path
    tmp_root: Path
    tool_log: Path
    generated_types: Path
    env: dict[str, str]

    def run(self, **env_overrides: str) -> subprocess.CompletedProcess[str]:
        env = self.env | env_overrides
        return subprocess.run(
            [str(self.script)],
            cwd=self.repo_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def events(self) -> list[dict[str, object]]:
        if not self.tool_log.exists():
            return []
        return [
            json.loads(line)
            for line in self.tool_log.read_text(encoding="utf-8").splitlines()
        ]


@pytest.fixture
def contract_sync_harness(tmp_path: Path) -> ContractSyncHarness:
    repo_root = tmp_path / "repo"
    script = repo_root / "scripts" / "check-contract-sync.sh"
    script.parent.mkdir(parents=True)
    shutil.copy2(SCRIPT, script)

    (repo_root / "apps" / "bt").mkdir(parents=True)
    snapshot = (
        repo_root
        / "apps"
        / "ts"
        / "packages"
        / "contracts"
        / "openapi"
        / "bt-openapi.json"
    )
    snapshot.parent.mkdir(parents=True)
    snapshot.write_text(
        '{"info":{"title":"fixture"},"openapi":"3.1.0"}\n',
        encoding="utf-8",
    )
    generated_types = (
        repo_root
        / "apps"
        / "ts"
        / "packages"
        / "contracts"
        / "src"
        / "clients"
        / "backtest"
        / "generated"
        / "bt-api-types.ts"
    )
    generated_types.parent.mkdir(parents=True)
    generated_types.write_text("// tracked fixture\n", encoding="utf-8")

    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    tmp_root = tmp_path / "tmp"
    tmp_root.mkdir()
    tool_log = tmp_path / "tool-events.jsonl"

    _write_executable(
        fake_bin / "mktemp",
        """
        #!/usr/bin/env python3
        import json
        import os
        from pathlib import Path
        import sys
        import tempfile

        args = sys.argv[1:]
        if len(args) != 2 or args[0] != "-d":
            print("fake BSD mktemp requires: mktemp -d TEMPLATE", file=sys.stderr)
            raise SystemExit(64)
        template = args[1]
        if not template.endswith("XXXXXX"):
            print(f"fake BSD mktemp requires trailing Xs: {template}", file=sys.stderr)
            raise SystemExit(65)
        prefix_path = Path(template[:-6])
        created = Path(
            tempfile.mkdtemp(prefix=prefix_path.name, dir=prefix_path.parent)
        )
        with open(os.environ["FAKE_TOOL_LOG"], "a", encoding="utf-8") as log:
            log.write(json.dumps({"tool": "mktemp", "path": str(created)}) + "\\n")
        print(created)
        """,
    )
    _write_executable(
        fake_bin / "uv",
        """
        #!/usr/bin/env python3
        import json
        import os
        from pathlib import Path
        import sys
        import time

        output = Path(sys.argv[sys.argv.index("--output") + 1])
        with open(os.environ["FAKE_TOOL_LOG"], "a", encoding="utf-8") as log:
            log.write(json.dumps({"tool": "uv", "output": str(output)}) + "\\n")
        if os.environ.get("FAKE_UV_FAIL") == "1":
            print("fake uv export failure", file=sys.stderr)
            raise SystemExit(71)
        time.sleep(float(os.environ.get("FAKE_UV_DELAY", "0")))
        output.write_text(
            '{"openapi":"3.1.0","info":{"title":"fixture"}}\\n',
            encoding="utf-8",
        )
        """,
    )
    _write_executable(
        fake_bin / "bun",
        """
        #!/usr/bin/env python3
        import json
        import os
        import sys

        with open(os.environ["FAKE_TOOL_LOG"], "a", encoding="utf-8") as log:
            log.write(json.dumps({"tool": "bun", "argv": sys.argv[1:]}) + "\\n")
        if os.environ.get("FAKE_BUN_FAIL") == "1":
            print("fake bun check failure", file=sys.stderr)
            raise SystemExit(72)
        """,
    )
    _write_executable(
        fake_bin / "git",
        """
        #!/usr/bin/env python3
        import json
        import os
        import sys

        with open(os.environ["FAKE_TOOL_LOG"], "a", encoding="utf-8") as log:
            log.write(json.dumps({"tool": "git", "argv": sys.argv[1:]}) + "\\n")
        print("contract check must not invoke git", file=sys.stderr)
        raise SystemExit(73)
        """,
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
            "TMPDIR": str(tmp_root),
            "FAKE_TOOL_LOG": str(tool_log),
        }
    )
    return ContractSyncHarness(
        repo_root=repo_root,
        script=script,
        tmp_root=tmp_root,
        tool_log=tool_log,
        generated_types=generated_types,
        env=env,
    )


def _generated_state(harness: ContractSyncHarness) -> tuple[str, int]:
    return (
        harness.generated_types.read_text(encoding="utf-8"),
        harness.generated_types.stat().st_mtime_ns,
    )


def _assert_tmp_root_empty(harness: ContractSyncHarness) -> None:
    assert list(harness.tmp_root.iterdir()) == []


def test_contract_sync_uses_one_portable_run_local_directory() -> None:
    script = _script_text()

    assert 'mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX"' in script
    assert 'trap \'rm -rf "${tmp_dir}"\' EXIT' in script
    assert 'tmp_openapi="${tmp_dir}/bt-openapi.json"' in script
    assert 'tmp_openapi_norm="${tmp_dir}/bt-openapi-normalized.json"' in script
    assert 'tmp_snapshot_norm="${tmp_dir}/bt-openapi-snapshot-normalized.json"' in script
    assert "XXXXXX.json" not in script


def test_contract_sync_checks_types_without_writing_generated_file() -> None:
    script = _script_text()

    assert "bt:generate-types -- --check" in script
    assert "generated_types_path=" not in script
    assert 'git -C "${repo_root}" diff --exit-code' not in script
    assert "Run: bun run --filter @trading25/contracts bt:sync" in script


def test_contract_sync_runs_with_bsd_mktemp_and_preserves_generated_file(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    before = _generated_state(contract_sync_harness)

    result = contract_sync_harness.run()

    assert result.returncode == 0, result.stderr
    assert "[contract] PASS" in result.stdout
    events = contract_sync_harness.events()
    mktemp_events = [event for event in events if event["tool"] == "mktemp"]
    assert len(mktemp_events) == 1
    assert str(mktemp_events[0]["path"]).startswith(
        str(contract_sync_harness.tmp_root / "bt-contract-sync.")
    )
    bun_events = [event for event in events if event["tool"] == "bun"]
    assert bun_events == [
        {
            "tool": "bun",
            "argv": [
                "run",
                "--filter",
                "@trading25/contracts",
                "bt:generate-types",
                "--",
                "--check",
            ],
        }
    ]
    assert all(event["tool"] != "git" for event in events)
    assert _generated_state(contract_sync_harness) == before
    _assert_tmp_root_empty(contract_sync_harness)


def test_contract_sync_isolates_two_overlapping_runs(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    before = _generated_state(contract_sync_harness)
    env = contract_sync_harness.env | {"FAKE_UV_DELAY": "0.2"}

    first = subprocess.Popen(
        [str(contract_sync_harness.script)],
        cwd=contract_sync_harness.repo_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    second = subprocess.Popen(
        [str(contract_sync_harness.script)],
        cwd=contract_sync_harness.repo_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    first_stdout, first_stderr = first.communicate(timeout=30)
    second_stdout, second_stderr = second.communicate(timeout=30)

    assert first.returncode == 0, first_stderr
    assert second.returncode == 0, second_stderr
    assert "[contract] PASS" in first_stdout
    assert "[contract] PASS" in second_stdout
    mktemp_paths = {
        str(event["path"])
        for event in contract_sync_harness.events()
        if event["tool"] == "mktemp"
    }
    assert len(mktemp_paths) == 2
    assert _generated_state(contract_sync_harness) == before
    _assert_tmp_root_empty(contract_sync_harness)


def test_contract_sync_cleans_run_directory_when_export_fails(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    before = _generated_state(contract_sync_harness)

    result = contract_sync_harness.run(FAKE_UV_FAIL="1")

    assert result.returncode == 71
    assert "fake uv export failure" in result.stderr
    assert all(event["tool"] != "bun" for event in contract_sync_harness.events())
    assert _generated_state(contract_sync_harness) == before
    _assert_tmp_root_empty(contract_sync_harness)


def test_contract_sync_propagates_bun_check_failure_without_mutation(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    before = _generated_state(contract_sync_harness)

    result = contract_sync_harness.run(FAKE_BUN_FAIL="1")

    assert result.returncode == 1
    assert "fake bun check failure" in result.stderr
    assert "Generated types are not up to date" in result.stderr
    bun_events = [
        event for event in contract_sync_harness.events() if event["tool"] == "bun"
    ]
    assert bun_events[0]["argv"][-2:] == ["--", "--check"]
    assert _generated_state(contract_sync_harness) == before
    _assert_tmp_root_empty(contract_sync_harness)
