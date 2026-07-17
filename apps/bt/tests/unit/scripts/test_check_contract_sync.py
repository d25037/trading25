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
TS_PACKAGE = REPO_ROOT / "apps" / "ts" / "package.json"
CONTRACTS_PACKAGE = (
    REPO_ROOT / "apps" / "ts" / "packages" / "contracts" / "package.json"
)
CHECK_TYPES_SCRIPT = (
    REPO_ROOT
    / "apps"
    / "ts"
    / "packages"
    / "contracts"
    / "scripts"
    / "check-bt-types.ts"
)


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
    shutil.copy2(REPO_ROOT / "scripts" / "check-ts-wire-contracts.py", script.parent)

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
    for relative_path in (
        "apps/ts/packages/contracts/src/types/api-response-types.ts",
        "apps/ts/packages/contracts/src/types/api-types.ts",
        "apps/ts/packages/api-clients/src/analytics/types.ts",
        "apps/ts/packages/api-clients/src/backtest/types.ts",
        "apps/ts/packages/api-clients/src/backtest/fundamentals-types.ts",
    ):
        fixture = repo_root / relative_path
        fixture.parent.mkdir(parents=True, exist_ok=True)
        fixture.write_text("// no duplicate wire declarations\n", encoding="utf-8")

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
            log.write(json.dumps({
                "tool": "uv",
                "argv": sys.argv[1:],
                "output": str(output),
                "research_api": os.environ.get("BT_ENABLE_RESEARCH_API"),
                "uv_cache_dir": os.environ.get("UV_CACHE_DIR"),
            }) + "\\n")
        if os.environ.get("FAKE_UV_FAIL") == "1":
            print("fake uv export failure", file=sys.stderr)
            raise SystemExit(71)
        time.sleep(float(os.environ.get("FAKE_UV_DELAY", "0")))
        output.write_text(
            os.environ.get(
                "FAKE_OPENAPI_JSON",
                '{"openapi":"3.1.0","info":{"title":"fixture"}}\\n',
            ),
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


def _assert_no_contract_sync_run_directories(harness: ContractSyncHarness) -> None:
    assert list(harness.tmp_root.glob("bt-contract-sync.*")) == []


def test_contract_sync_uses_one_portable_run_local_directory() -> None:
    script = _script_text()

    assert 'mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX"' in script
    assert "trap 'rm -rf \"${tmp_dir}\"' EXIT" in script
    assert 'tmp_openapi="${tmp_dir}/bt-openapi.json"' in script
    assert 'tmp_openapi_norm="${tmp_dir}/bt-openapi-normalized.json"' in script
    assert (
        'tmp_snapshot_norm="${tmp_dir}/bt-openapi-snapshot-normalized.json"' in script
    )
    assert "XXXXXX.json" not in script


def test_contract_sync_checks_types_without_writing_generated_file() -> None:
    script = _script_text()

    assert "bt:generate-types -- --check" in script
    assert "generated_types_path=" not in script
    assert 'git -C "${repo_root}" diff --exit-code' not in script
    assert "Run: bun run --filter @trading25/contracts bt:sync" in script


def test_contract_sync_runs_optional_compatibility_gate_after_drift_checks() -> None:
    script = _script_text()

    compat_call = 'python3 "${repo_root}/scripts/openapi_compat.py"'
    assert 'if [[ -n "${OPENAPI_BASE_SNAPSHOT:-}" ]]; then' in script
    assert '--base "${OPENAPI_BASE_SNAPSHOT}"' in script
    assert '--candidate "${tmp_openapi}"' in script
    assert (
        '--approvals "${repo_root}/contracts/openapi-breaking-approvals.json"' in script
    )
    assert script.index("bt:generate-types -- --check") < script.index(compat_call)


def test_contract_sync_rejects_handwritten_wire_contract_duplicates() -> None:
    script = _script_text()

    detector_call = 'python3 "${repo_root}/scripts/check-ts-wire-contracts.py"'
    assert detector_call in script
    assert '--openapi "${snapshot_path}"' in script
    assert '--contracts "${repo_root}/apps/ts/packages/contracts/src"' in script
    assert "contracts/src/types/api-response-types.ts" not in script
    assert "contracts/src/types/api-types.ts" not in script
    assert '--api-clients "${repo_root}/apps/ts/packages/api-clients/src"' in script
    assert "api-clients/src/analytics/types.ts" not in script
    assert script.index("bt:generate-types -- --check") < script.index(detector_call)


def test_contract_package_exposes_strict_sync_check_and_offline_generation() -> None:
    package = json.loads(CONTRACTS_PACKAGE.read_text(encoding="utf-8"))
    scripts = package["scripts"]

    assert scripts["bt:sync"] == (
        "bun run bt:fetch-schema && bun run bt:generate-offline"
    )
    assert scripts["bt:generate-offline"] == scripts["bt:generate-types"]
    assert scripts["bt:check"] == "bun scripts/check-bt-types.ts"
    assert package["devDependencies"]["openapi-typescript"] == "7.13.0"


def test_workspace_exposes_contract_check_and_offline_generation() -> None:
    package = json.loads(TS_PACKAGE.read_text(encoding="utf-8"))
    scripts = package["scripts"]

    assert scripts["contracts:check:bt"] == (
        "bun run --filter @trading25/contracts bt:check"
    )
    assert scripts["contracts:generate-offline:bt"] == (
        "bun run --filter @trading25/contracts bt:generate-offline"
    )


def test_bt_check_delegates_to_repository_non_destructive_drift_gate() -> None:
    check_script = CHECK_TYPES_SCRIPT.read_text(encoding="utf-8")

    assert "check-contract-sync.sh" in check_script
    assert "bt-api-types.ts" not in check_script


def test_contract_sync_runs_with_bsd_mktemp_and_preserves_generated_file(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    before = _generated_state(contract_sync_harness)
    unrelated_tmp_artifact = contract_sync_harness.tmp_root / "xcrun_db"
    unrelated_tmp_artifact.mkdir()

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
    assert unrelated_tmp_artifact.is_dir()
    _assert_no_contract_sync_run_directories(contract_sync_harness)


def test_contract_sync_forces_canonical_research_api_and_preserves_uv_cache(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    custom_uv_cache = str(contract_sync_harness.tmp_root / "custom-uv-cache")

    result = contract_sync_harness.run(
        BT_ENABLE_RESEARCH_API="0",
        UV_CACHE_DIR=custom_uv_cache,
    )

    assert result.returncode == 0, result.stderr
    uv_events = [
        event for event in contract_sync_harness.events() if event["tool"] == "uv"
    ]
    assert len(uv_events) == 1
    assert uv_events[0]["argv"] == [
        "run",
        "--locked",
        "python",
        "scripts/export_openapi.py",
        "--output",
        uv_events[0]["output"],
    ]
    assert uv_events[0]["research_api"] == "1"
    assert uv_events[0]["uv_cache_dir"] == custom_uv_cache


def test_contract_sync_rejects_collision_in_nested_contract_source(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    snapshot = (
        contract_sync_harness.repo_root
        / "apps/ts/packages/contracts/openapi/bt-openapi.json"
    )
    openapi_payload = json.dumps(
        {
            "openapi": "3.1.0",
            "info": {"title": "fixture"},
            "components": {"schemas": {"WireResponse": {"type": "object"}}},
        }
    )
    snapshot.write_text(openapi_payload, encoding="utf-8")
    nested_contract = (
        contract_sync_harness.repo_root
        / "apps/ts/packages/contracts/src/nested/wire-types.ts"
    )
    nested_contract.parent.mkdir(parents=True)
    nested_contract.write_text(
        "export interface WireResponse { value: string }\n",
        encoding="utf-8",
    )

    result = contract_sync_harness.run(FAKE_OPENAPI_JSON=openapi_payload)

    assert result.returncode == 1
    assert f"{nested_contract}:1: WireResponse" in result.stderr


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
    _assert_no_contract_sync_run_directories(contract_sync_harness)


def test_contract_sync_cleans_run_directory_when_export_fails(
    contract_sync_harness: ContractSyncHarness,
) -> None:
    before = _generated_state(contract_sync_harness)

    result = contract_sync_harness.run(FAKE_UV_FAIL="1")

    assert result.returncode == 71
    assert "fake uv export failure" in result.stderr
    assert all(event["tool"] != "bun" for event in contract_sync_harness.events())
    assert _generated_state(contract_sync_harness) == before
    _assert_no_contract_sync_run_directories(contract_sync_harness)


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
    _assert_no_contract_sync_run_directories(contract_sync_harness)
