from pathlib import Path

from ruamel.yaml import YAML


REPO_ROOT = Path(__file__).resolve().parents[5]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _jobs() -> dict[str, object]:
    with CI_WORKFLOW.open(encoding="utf-8") as workflow_file:
        workflow = YAML(typ="safe").load(workflow_file)
    return workflow["jobs"]


def test_typescript_workspace_tests_are_a_dedicated_required_job() -> None:
    jobs = _jobs()
    ts_tests = jobs["ts-tests"]

    assert ts_tests["steps"][-1]["run"] == "bun run workspace:test"
    assert "ts-tests" in jobs["ci-gate"]["needs"]

    serialized_job = str(ts_tests)
    assert "bun-v1.3.8" in serialized_job
    assert "bun install --frozen-lockfile" in serialized_job
    assert "SKIP_TS_TESTS" not in serialized_job


def test_ci_gate_always_rejects_failed_or_cancelled_dependencies() -> None:
    jobs = _jobs()
    ci_gate = jobs["ci-gate"]

    assert ci_gate["if"] == "always()"
    assert {
        "changes",
        "quality",
        "golden-dataset-regression",
        "coverage-gate",
        "package-unit-tests",
        "app-integration-tests",
        "ts-tests",
    } <= set(ci_gate["needs"])

    serialized_job = str(ci_gate)
    assert "product_ci" in serialized_job
    assert "failure" in serialized_job
    assert "cancelled" in serialized_job
