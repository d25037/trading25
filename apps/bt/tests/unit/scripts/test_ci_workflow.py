import json
import os
from pathlib import Path
import subprocess
from typing import Any

import pytest
from ruamel.yaml import YAML


REPO_ROOT = Path(__file__).resolve().parents[5]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
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


def test_typescript_workspace_tests_are_a_dedicated_required_job() -> None:
    jobs = _jobs()
    ts_tests = jobs["ts-tests"]

    assert ts_tests["steps"][-1] == {
        "name": "Run TypeScript workspace tests",
        "working-directory": "apps/ts",
        "run": "bun run workspace:test",
    }
    assert "ts-tests" in jobs["web-e2e"]["needs"]
    assert jobs["ci-gate"]["if"] == "always()"
    assert set(jobs["ci-gate"]["needs"]) == EXPECTED_GATE_NEEDS

    serialized_job = str(ts_tests)
    assert "bun-v1.3.8" in serialized_job
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
