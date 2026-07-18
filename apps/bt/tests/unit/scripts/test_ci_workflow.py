import json
import os
from pathlib import Path
import re
import subprocess
import tomllib
from typing import Any

import pytest
from ruamel.yaml import YAML


REPO_ROOT = Path(__file__).resolve().parents[5]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
NAUTILUS_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nautilus-smoke.yml"
GITLEAKS_CONFIG = REPO_ROOT / ".gitleaks.toml"
ACTION_PIN_PATTERN = re.compile(
    r"^\s*- uses: [\w.-]+/[\w.-]+@[0-9a-f]{40}\s+# v\S+\s*$",
    re.MULTILINE,
)
EXPECTED_GATE_NEEDS = {
    "changes",
    "maintainability",
    "maintainability-python39",
    "repo-guardrails",
    "quality",
    "contract-tests",
    "golden-dataset-regression",
    "coverage-gate",
    "package-unit-tests",
    "market-v4-darwin-capabilities",
    "app-integration-tests",
    "ts-tests",
    "secret-scan",
    "dependency-vulnerability-audit",
    "bt-research-tests",
    "web-e2e",
}


def _jobs() -> dict[str, Any]:
    with CI_WORKFLOW.open(encoding="utf-8") as workflow_file:
        workflow = YAML(typ="safe").load(workflow_file)
    return workflow["jobs"]


def _workflow(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as workflow_file:
        return YAML(typ="safe").load(workflow_file)


def _needs(*, product_ci: str, event_name: str) -> dict[str, Any]:
    product_enabled = product_ci == "true"
    outputs = {
        "product_ci": product_ci,
        "research_ci": "true" if product_enabled else "false",
        "contracts_ci": "true" if product_enabled else "false",
        "security_ci": "true" if product_enabled else "false",
        "docs_only": "false" if product_enabled else "true",
    }
    needs = {
        name: {"result": "success", "outputs": {}} for name in EXPECTED_GATE_NEEDS
    }
    needs["changes"]["outputs"] = outputs

    if not product_enabled:
        for name in EXPECTED_GATE_NEEDS - {
            "changes",
            "maintainability",
            "maintainability-python39",
            "repo-guardrails",
            "secret-scan",
        }:
            needs[name]["result"] = "skipped"
    elif event_name != "pull_request":
        needs["web-e2e"]["result"] = "skipped"
    return needs


def _run_gate(needs: dict[str, Any], *, event_name: str) -> subprocess.CompletedProcess[str]:
    gate_script = _jobs()["ci-gate"]["steps"][0]["run"]
    env = os.environ.copy()
    env.update(
        {
            "NEEDS_JSON": json.dumps(needs),
            "GITHUB_EVENT_NAME": event_name,
        }
    )
    return subprocess.run(
        ["bash", "-c", gate_script],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def test_change_classification_pipeline_fails_closed() -> None:
    classify_step = next(
        step
        for step in _jobs()["changes"]["steps"]
        if step.get("name") == "Classify CI scope"
    )

    assert "set -o pipefail" in classify_step["run"]


@pytest.mark.parametrize("workflow_path", [CI_WORKFLOW, NAUTILUS_WORKFLOW])
def test_workflow_declares_read_only_repository_permissions(
    workflow_path: Path,
) -> None:
    assert _workflow(workflow_path)["permissions"] == {"contents": "read"}


@pytest.mark.parametrize("workflow_path", [CI_WORKFLOW, NAUTILUS_WORKFLOW])
def test_all_checkout_steps_disable_credential_persistence(
    workflow_path: Path,
) -> None:
    workflow = _workflow(workflow_path)
    checkout_steps = [
        step
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/checkout@")
    ]

    assert checkout_steps
    assert all(
        step.get("with", {}).get("persist-credentials") is False
        for step in checkout_steps
    )


@pytest.mark.parametrize("workflow_path", [CI_WORKFLOW, NAUTILUS_WORKFLOW])
def test_all_actions_use_immutable_shas_with_version_comments(
    workflow_path: Path,
) -> None:
    source = workflow_path.read_text(encoding="utf-8")
    uses_lines = [line for line in source.splitlines() if "- uses:" in line]

    assert uses_lines
    assert len(ACTION_PIN_PATTERN.findall(source)) == len(uses_lines)


def test_ci_tool_versions_are_centrally_fixed() -> None:
    workflow = _workflow(CI_WORKFLOW)

    assert workflow["env"] == {
        "BUN_VERSION": "1.3.14",
        "BUN_ARCHIVE_SHA256": (
            "951ee2aee855f08595aeec6225226a298d3fea83a3dcd6465c09cbccdf7e848f"
        ),
        "UV_VERSION": "0.11.29",
        "GITLEAKS_VERSION": "8.30.1",
        "GITLEAKS_IMAGE_DIGEST": (
            "sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f"
        ),
    }
    source = CI_WORKFLOW.read_text(encoding="utf-8")
    assert source.count("version: ${{ env.UV_VERSION }}") == 10
    assert 'version: "latest"' not in source


def test_bun_installation_verifies_the_exact_official_release_artifact() -> None:
    install_steps = [
        step
        for job in _jobs().values()
        for step in job["steps"]
        if step.get("name") == "Install Bun"
    ]

    assert len(install_steps) == 7
    for step in install_steps:
        command = step["run"]
        assert (
            "https://github.com/oven-sh/bun/releases/download/"
            "bun-v${BUN_VERSION}/bun-linux-x64.zip"
        ) in command
        assert (
            'echo "${BUN_ARCHIVE_SHA256}  ${bun_archive}" '
            "| sha256sum -c -"
        ) in command
        assert 'BUN_INSTALL="${RUNNER_TEMP}/bun-install"' in command
        assert 'echo "${BUN_INSTALL}/bin" >> "$GITHUB_PATH"' in command
        assert '"${BUN_INSTALL}/bin/bun" --version' in command
        assert "https://bun.com/install" not in command
        assert "bash -s" not in command


def test_gitleaks_container_is_pinned_to_the_verified_oci_digest() -> None:
    secret_command = next(
        step["run"]
        for step in _jobs()["secret-scan"]["steps"]
        if step.get("name") == "Run secret scan (gitleaks)"
    )

    assert (
        '"ghcr.io/gitleaks/gitleaks:v${GITLEAKS_VERSION}'
        '@${GITLEAKS_IMAGE_DIGEST}"'
    ) in secret_command
    assert 'gitleaks/gitleaks:${GITLEAKS_VERSION}"' not in secret_command


def test_nautilus_tool_version_is_fixed() -> None:
    workflow = _workflow(NAUTILUS_WORKFLOW)

    assert workflow["env"] == {"UV_VERSION": "0.11.29"}
    assert "version: ${{ env.UV_VERSION }}" in NAUTILUS_WORKFLOW.read_text(
        encoding="utf-8"
    )


def test_security_jobs_always_run_and_secret_scan_is_git_aware() -> None:
    jobs = _jobs()
    privacy_job = jobs["repo-guardrails"]
    secret_job = jobs["secret-scan"]
    secret_command = next(
        step["run"]
        for step in secret_job["steps"]
        if step.get("name") == "Run secret scan (gitleaks)"
    )

    assert "if" not in privacy_job
    assert "if" not in secret_job
    assert (
        "gitleaks/gitleaks:v${GITLEAKS_VERSION}@${GITLEAKS_IMAGE_DIGEST}"
        in secret_command
    )
    assert " git " in secret_command
    assert "--log-opts" in secret_command
    assert "github.event.pull_request.base.sha" in secret_command
    assert "github.event.before" in secret_command
    assert 'log_opts="-1 ${{ github.sha }}"' in secret_command
    assert 'log_opts="--all"' not in secret_command
    assert 'git "/repo"' in secret_command
    assert "--source" not in secret_command
    assert "--no-git" not in secret_command


def test_nautilus_pull_requests_are_scoped_without_narrowing_main_pushes() -> None:
    workflow = _workflow(NAUTILUS_WORKFLOW)
    triggers = workflow["on"]

    assert triggers["push"] == {"branches": ["main"]}
    assert triggers["pull_request"] == {
        "paths": [
            ".github/workflows/nautilus-smoke.yml",
            "apps/bt/pyproject.toml",
            "apps/bt/uv.lock",
            "apps/bt/src/**",
            "apps/bt/tests/conftest.py",
            "apps/bt/tests/smoke/test_nautilus_runtime_smoke.py",
            "scripts/bt-run.sh",
            "scripts/test-nautilus-smoke.sh",
        ]
    }
    assert triggers["workflow_dispatch"] is None


def test_synthetic_indicator_allowlist_is_rule_path_and_line_scoped() -> None:
    config = tomllib.loads(GITLEAKS_CONFIG.read_text(encoding="utf-8"))
    synthetic = next(
        allowlist
        for allowlist in config["allowlists"]
        if "synthetic SMA/ATR" in allowlist["description"]
    )

    assert "allowlist" not in config
    assert synthetic == {
        "description": "Allow only the synthetic SMA/ATR indicator registry key.",
        "targetRules": ["generic-api-key"],
        "condition": "AND",
        "regexTarget": "line",
        "paths": [
            r"^apps/bt/tests/unit/server/services/test_indicator_service\.py$"
        ],
        "regexes": [r'^\s*assert key == "sma_atr_bands_2_3_1\.0"\s*$'],
    }


def test_every_gitleaks_allowlist_is_rule_path_and_line_scoped() -> None:
    config = tomllib.loads(GITLEAKS_CONFIG.read_text(encoding="utf-8"))

    for allowlist in config["allowlists"]:
        assert allowlist.get("targetRules"), allowlist["description"]
        assert allowlist.get("paths"), allowlist["description"]
        assert allowlist.get("condition") == "AND", allowlist["description"]
        assert allowlist.get("regexTarget") == "line", allowlist["description"]
        assert allowlist.get("regexes"), allowlist["description"]


def test_product_ci_unconditionally_runs_contract_tests() -> None:
    jobs = _jobs()

    assert jobs["contract-tests"]["if"] == (
        "needs.changes.outputs.product_ci == 'true'"
    )

    needs = _needs(product_ci="true", event_name="push")
    needs["changes"]["outputs"]["contracts_ci"] = "false"
    needs["contract-tests"]["result"] = "skipped"

    result = _run_gate(needs, event_name="push")

    assert result.returncode != 0


def test_pull_request_contract_job_checks_base_snapshot_compatibility() -> None:
    contract_job = _jobs()["contract-tests"]
    steps = contract_job["steps"]
    materialize = next(
        step for step in steps if step.get("name") == "Materialize base OpenAPI snapshot"
    )
    contract_check = next(
        step for step in steps if step.get("name") == "Run contract sync checks"
    )

    assert materialize["if"] == "github.event_name == 'pull_request'"
    assert materialize["env"] == {
        "BASE_SHA": "${{ github.event.pull_request.base.sha }}"
    }
    assert 'git show "${BASE_SHA}:apps/ts/packages/contracts/openapi/bt-openapi.json"' in (
        materialize["run"]
    )
    assert steps.index(materialize) < steps.index(contract_check)
    assert contract_check["env"]["OPENAPI_BASE_SNAPSHOT"] == (
        "${{ github.event_name == 'pull_request' && '/tmp/bt-openapi-base.json' || '' }}"
    )


def test_contract_job_runs_wire_duplicate_detector_tests() -> None:
    contract_job = _jobs()["contract-tests"]
    detector_tests = next(
        step
        for step in contract_job["steps"]
        if step.get("name") == "Test TypeScript wire duplicate gate"
    )

    assert detector_tests["working-directory"] == "apps/bt"
    assert detector_tests["run"] == (
        "uv run pytest tests/unit/scripts/test_check_ts_wire_contracts.py -q"
    )


def test_typescript_workspace_tests_are_a_dedicated_required_job() -> None:
    jobs = _jobs()
    ts_tests = jobs["ts-tests"]

    workspace_build = {
        "name": "Build TypeScript workspace",
        "working-directory": "apps/ts",
        "run": "bun run workspace:build",
    }
    extension_build = {
        "name": "Build Shikiho extension",
        "working-directory": "apps/ts",
        "run": "bun run extension:build",
    }
    workspace_tests = {
        "name": "Run TypeScript workspace tests",
        "working-directory": "apps/ts",
        "run": "bun run workspace:test",
    }
    assert workspace_build in ts_tests["steps"]
    assert extension_build in ts_tests["steps"]
    assert workspace_tests in ts_tests["steps"]
    assert ts_tests["steps"].index(workspace_build) < ts_tests["steps"].index(
        workspace_tests
    )
    assert ts_tests["steps"].index(extension_build) < ts_tests["steps"].index(
        workspace_tests
    )
    assert "ts-tests" in jobs["web-e2e"]["needs"]
    assert jobs["ci-gate"]["if"] == "always()"
    assert set(jobs["ci-gate"]["needs"]) == EXPECTED_GATE_NEEDS

    serialized_job = str(ts_tests)
    assert "bun-v${BUN_VERSION}" in serialized_job
    assert "bun install --frozen-lockfile" in serialized_job
    assert "SKIP_TS_TESTS" not in serialized_job


def test_market_v4_darwin_capabilities_run_on_required_macos_job() -> None:
    jobs = _jobs()
    capability_job = jobs["market-v4-darwin-capabilities"]

    assert capability_job["runs-on"] == "macos-latest"
    assert (
        capability_job["steps"][-1]["run"]
        == "uv run pytest -m darwin_capability"
    )
    assert "market-v4-darwin-capabilities" in jobs["web-e2e"]["needs"]
    assert "market-v4-darwin-capabilities" in jobs["ci-gate"]["needs"]


@pytest.mark.parametrize("event_name", ["push", "pull_request"])
def test_ci_gate_accepts_complete_product_ci(event_name: str) -> None:
    result = _run_gate(
        _needs(product_ci="true", event_name=event_name), event_name=event_name
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_ci_gate_accepts_intentional_non_product_skip() -> None:
    result = _run_gate(
        _needs(product_ci="false", event_name="pull_request"),
        event_name="pull_request",
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("job_name", ["repo-guardrails", "secret-scan"])
def test_ci_gate_requires_security_checks_for_docs_only_changes(
    job_name: str,
) -> None:
    needs = _needs(product_ci="false", event_name="pull_request")
    needs[job_name]["result"] = "skipped"

    result = _run_gate(needs, event_name="pull_request")

    assert result.returncode != 0


def test_ci_gate_always_requires_maintainability_snapshot() -> None:
    needs = _needs(product_ci="false", event_name="pull_request")
    needs["maintainability"]["result"] = "skipped"

    result = _run_gate(needs, event_name="pull_request")

    assert result.returncode != 0


def test_ci_gate_always_requires_real_python_39_guard() -> None:
    needs = _needs(product_ci="false", event_name="pull_request")
    needs["maintainability-python39"]["result"] = "skipped"

    result = _run_gate(needs, event_name="pull_request")

    assert result.returncode != 0


@pytest.mark.parametrize("product_ci", ["", "unexpected"])
def test_ci_gate_rejects_missing_or_malformed_product_scope(product_ci: str) -> None:
    result = _run_gate(
        _needs(product_ci=product_ci, event_name="push"), event_name="push"
    )

    assert result.returncode != 0


@pytest.mark.parametrize("result_name", ["failure", "cancelled", "skipped"])
def test_ci_gate_rejects_non_success_required_product_jobs(result_name: str) -> None:
    needs = _needs(product_ci="true", event_name="push")
    needs["quality"]["result"] = result_name

    result = _run_gate(needs, event_name="push")

    assert result.returncode != 0


@pytest.mark.parametrize("job_name", ["repo-guardrails", "secret-scan"])
def test_ci_gate_requires_product_guardrails(job_name: str) -> None:
    needs = _needs(product_ci="true", event_name="push")
    needs[job_name]["result"] = "skipped"

    result = _run_gate(needs, event_name="push")

    assert result.returncode != 0


def test_ci_gate_rejects_skipped_e2e_for_product_pull_request() -> None:
    needs = _needs(product_ci="true", event_name="pull_request")
    needs["web-e2e"]["result"] = "skipped"

    result = _run_gate(needs, event_name="pull_request")

    assert result.returncode != 0
