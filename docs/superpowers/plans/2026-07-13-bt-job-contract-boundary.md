# bt Job Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move job lifecycle contracts into the application layer, delete all legacy HTTP-schema definitions and import paths for those contracts, and shrink the application-to-HTTP-schema ratchet without changing the HTTP wire contract.

**Architecture:** `src.application.contracts.jobs` becomes the only definition site for `JobStatus`, `JobProgress`, and `JobEvent`. Application services, workers, routes, schemas, and tests import that canonical module directly; HTTP schemas may reference it only through a module-qualified inward dependency and must not re-export legacy names.

**Tech Stack:** Python 3.12, Pydantic 2, FastAPI, pytest, Ruff, Pyright, OpenAPI contract sync

## Global Constraints

- Internal Python compatibility aliases and transitional re-exports are forbidden.
- `JobStatus` values remain exactly `pending`, `running`, `completed`, `failed`, and `cancelled`.
- Persisted job status values remain lowercase strings.
- `JobEvent` JSON fields remain exactly `job_id`, `status`, `progress`, `message`, and `data`; event progress remains 0.0–1.0.
- `JobProgress` fields remain exactly `stage`, `current`, `total`, `percentage`, and `message`; percentage remains 0–100.
- The SSE queue terminal sentinel remains `None`.
- FastAPI/OpenAPI and generated TypeScript contracts must have no semantic change.
- Remove only baseline entries whose complete HTTP-schema module import disappears; mixed imports for unrelated DTOs remain.
- Every production change follows a witnessed RED → GREEN test cycle.

---

### Task 1: Add Canonical Application Job Contracts

**Files:**
- Create: `apps/bt/src/application/contracts/__init__.py`
- Create: `apps/bt/src/application/contracts/jobs.py`
- Create: `apps/bt/tests/unit/application/contracts/test_jobs.py`

**Interfaces:**
- Consumes: Pydantic `BaseModel` and `Field`; Python `str, Enum`.
- Produces: `JobStatus`, `JobProgress`, and `JobEvent` from `src.application.contracts.jobs` with the exact shapes in Global Constraints.

- [ ] **Step 1: Write the failing canonical-contract tests**

```python
from src.application.contracts.jobs import JobEvent, JobProgress, JobStatus


def test_job_status_values_are_stable() -> None:
    assert [status.value for status in JobStatus] == [
        "pending",
        "running",
        "completed",
        "failed",
        "cancelled",
    ]


def test_job_event_serialization_is_stable() -> None:
    event = JobEvent(
        job_id="job-1",
        status="running",
        progress=0.25,
        message="running",
        data={"stage": "load"},
    )
    assert event.model_dump(mode="json") == {
        "job_id": "job-1",
        "status": "running",
        "progress": 0.25,
        "message": "running",
        "data": {"stage": "load"},
    }


def test_job_progress_serialization_is_stable() -> None:
    progress = JobProgress(
        stage="copy",
        current=1,
        total=4,
        percentage=25.0,
        message="copying",
    )
    assert progress.model_dump(mode="json") == {
        "stage": "copy",
        "current": 1,
        "total": 4,
        "percentage": 25.0,
        "message": "copying",
    }
```

- [ ] **Step 2: Run the tests and witness RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/application/contracts/test_jobs.py -q
```

Expected: collection fails with `ModuleNotFoundError: No module named 'src.application.contracts'`.

- [ ] **Step 3: Implement the canonical contracts**

Create an intentionally empty package initializer except for a package docstring. Define the contracts in `jobs.py`:

```python
"""Application-owned job lifecycle contracts."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobProgress(BaseModel):
    stage: str = Field(description="Current stage name")
    current: int = Field(description="Current step number")
    total: int = Field(description="Total steps")
    percentage: float = Field(description="Progress percentage 0-100")
    message: str = Field(description="Human-readable progress message")


class JobEvent(BaseModel):
    job_id: str = Field(description="ジョブID")
    status: str = Field(description="ジョブステータス")
    progress: float | None = Field(default=None, description="進捗（0.0 - 1.0）")
    message: str | None = Field(default=None, description="ステータスメッセージ")
    data: dict[str, Any] | None = Field(default=None, description="追加データ")
```

- [ ] **Step 4: Run GREEN verification**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/application/contracts/test_jobs.py -q
uv run --directory apps/bt ruff check src/application/contracts tests/unit/application/contracts
uv run --directory apps/bt pyright src/application/contracts
```

Expected: 3 tests pass; Ruff and Pyright exit 0.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/contracts apps/bt/tests/unit/application/contracts
git commit -m "feat(bt): add canonical job contracts"
```

### Task 2: Migrate Application Services and Workers

**Files:**
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Modify: `apps/bt/src/application/services/backtest_attribution_service.py`
- Modify: `apps/bt/src/application/services/backtest_service.py`
- Modify: `apps/bt/src/application/services/dataset_builder_copy_stages.py`
- Modify: `apps/bt/src/application/services/dataset_builder_service.py`
- Modify: `apps/bt/src/application/services/generic_job_manager.py`
- Modify: `apps/bt/src/application/services/job_manager.py`
- Modify: `apps/bt/src/application/services/job_status.py`
- Modify: `apps/bt/src/application/services/lab_service.py`
- Modify: `apps/bt/src/application/services/optimization_service.py`
- Modify: `apps/bt/src/application/services/run_contracts.py`
- Modify: `apps/bt/src/application/services/screening_job_service.py`
- Modify: `apps/bt/src/application/services/sse_manager.py`
- Modify: `apps/bt/src/application/services/verification_orchestrator.py`
- Modify: `apps/bt/src/application/workers/backtest_worker.py`
- Modify: `apps/bt/src/application/workers/job_runtime.py`
- Modify: `apps/bt/src/application/workers/lab_worker.py`
- Modify: `apps/bt/src/application/workers/optimization_worker.py`

**Interfaces:**
- Consumes: canonical contracts from Task 1.
- Produces: application services/workers with no HTTP-schema imports for `JobStatus`, `JobProgress`, or `SSEJobEvent`; `SSEJobEvent` usages become `JobEvent`.

- [ ] **Step 1: Add a failing architecture assertion for forbidden application imports**

Add a reusable helper that retains imported symbol names and the application-scoped test:

```python
LEGACY_JOB_SCHEMA_NAMES = {"JobStatus", "JobProgress", "SSEJobEvent"}


def _legacy_job_schema_imports(*roots: Path) -> list[str]:
    violations: list[str] = []
    for root in roots:
        for py_file in root.rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module_name = _resolve_import_from_module(py_file, node)
                if module_name is None or not module_name.startswith(
                    APPLICATION_HTTP_SCHEMA_PREFIX
                ):
                    continue
                imported = LEGACY_JOB_SCHEMA_NAMES.intersection(
                    alias.name for alias in node.names
                )
                if imported:
                    relative = py_file.relative_to(PROJECT_ROOT)
                    violations.append(
                        f"{relative}:{node.lineno} imports {sorted(imported)} from {module_name}"
                    )
    return sorted(violations)


def test_application_job_contracts_do_not_import_http_schemas() -> None:
    violations = _legacy_job_schema_imports(SRC_ROOT / "application")
    assert not violations, "Application job contracts must be application-owned:\n" + "\n".join(
        violations
    )
```

- [ ] **Step 2: Run the architecture test and witness RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/architecture/test_layer_boundaries.py::test_application_job_contracts_do_not_import_http_schemas -q
```

Expected: failure listing the current 17 application files.

- [ ] **Step 3: Migrate application imports and names**

For every listed application service and worker, import the needed names from:

```python
from src.application.contracts.jobs import JobEvent, JobProgress, JobStatus
```

Import only the symbols each file uses. Replace every `SSEJobEvent` annotation and constructor with `JobEvent`. Preserve unrelated imports such as `BacktestResultSummary` from HTTP schemas until their own future migration.

- [ ] **Step 4: Shrink the exact baseline**

Remove exactly these 15 lines because those entire module imports disappear:

```text
application/services/backtest_attribution_service.py|src.entrypoints.http.schemas.backtest
application/services/dataset_builder_copy_stages.py|src.entrypoints.http.schemas.job
application/services/dataset_builder_service.py|src.entrypoints.http.schemas.job
application/services/generic_job_manager.py|src.entrypoints.http.schemas.job
application/services/job_manager.py|src.entrypoints.http.schemas.common
application/services/job_status.py|src.entrypoints.http.schemas.backtest
application/services/lab_service.py|src.entrypoints.http.schemas.backtest
application/services/optimization_service.py|src.entrypoints.http.schemas.backtest
application/services/run_contracts.py|src.entrypoints.http.schemas.backtest
application/services/screening_job_service.py|src.entrypoints.http.schemas.backtest
application/services/sse_manager.py|src.entrypoints.http.schemas.common
application/services/verification_orchestrator.py|src.entrypoints.http.schemas.backtest
application/workers/job_runtime.py|src.entrypoints.http.schemas.common
application/workers/lab_worker.py|src.entrypoints.http.schemas.backtest
application/workers/optimization_worker.py|src.entrypoints.http.schemas.backtest
```

Retain the `backtest` baseline entries for `backtest_service.py`, `job_manager.py`, and `backtest_worker.py` because they still import `BacktestResultSummary`.

- [ ] **Step 5: Run GREEN verification for application migration**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/architecture \
  tests/server/test_job_manager.py \
  tests/server/test_sse.py \
  tests/unit/server/test_generic_job_manager.py \
  tests/unit/server/services/test_backtest_service.py \
  tests/unit/server/services/test_lab_service_worker.py \
  tests/unit/server/services/test_optimization_service.py \
  tests/unit/server/services/test_screening_job_service.py \
  tests/unit/server/services/test_verification_orchestrator.py \
  tests/unit/server/test_backtest_worker.py \
  tests/unit/server/test_lab_worker.py \
  tests/unit/server/test_optimization_worker.py -q
uv run --directory apps/bt ruff check src/application tests/unit/architecture
uv run --directory apps/bt pyright src/application
```

Expected: all selected tests pass; Ruff and Pyright exit 0.

- [ ] **Step 6: Commit**

```bash
git add apps/bt/src/application apps/bt/tests/unit/architecture
git commit -m "refactor(bt): move job contracts out of http schemas"
```

### Task 3: Delete Legacy HTTP Schema Paths and Migrate All Consumers

**Files:**
- Modify: `apps/bt/src/entrypoints/http/schemas/common.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/job.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/backtest.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/__init__.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Modify: `apps/bt/src/entrypoints/http/routes/backtest.py`
- Modify: `apps/bt/src/entrypoints/http/routes/dataset.py`
- Modify: `apps/bt/src/entrypoints/http/routes/db.py`
- Modify: `apps/bt/tests/server/routes/test_lab.py`
- Modify: `apps/bt/tests/server/test_job_manager.py`
- Modify: `apps/bt/tests/server/test_routes.py`
- Modify: `apps/bt/tests/server/test_schemas.py`
- Modify: `apps/bt/tests/server/test_sse.py`
- Modify: `apps/bt/tests/unit/server/routes/test_analytics_complex.py`
- Modify: `apps/bt/tests/unit/server/routes/test_backtest.py`
- Modify: `apps/bt/tests/unit/server/routes/test_optimize.py`
- Modify: `apps/bt/tests/unit/server/services/test_backtest_attribution_service.py`
- Modify: `apps/bt/tests/unit/server/services/test_backtest_service.py`
- Modify: `apps/bt/tests/unit/server/services/test_job_manager_cancel.py`
- Modify: `apps/bt/tests/unit/server/services/test_lab_service_worker.py`
- Modify: `apps/bt/tests/unit/server/services/test_optimization_service.py`
- Modify: `apps/bt/tests/unit/server/services/test_screening_job_service.py`
- Modify: `apps/bt/tests/unit/server/services/test_verification_orchestrator.py`
- Modify: `apps/bt/tests/unit/server/test_backtest_worker.py`
- Modify: `apps/bt/tests/unit/server/test_dataset_builder_service_branches.py`
- Modify: `apps/bt/tests/unit/server/test_generic_job_manager.py`
- Modify: `apps/bt/tests/unit/server/test_lab_worker.py`
- Modify: `apps/bt/tests/unit/server/test_optimization_worker.py`
- Modify: `apps/bt/tests/unit/server/test_routes_dataset_jobs.py`
- Modify: `apps/bt/tests/unit/server/test_routes_db_sync.py`
- Modify: `apps/bt/tests/unit/server/test_run_contracts.py`
- Modify: `apps/bt/tests/unit/server/test_run_registry.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `docs/bt-src-layering-guide.md`

**Interfaces:**
- Consumes: migrated application code from Task 2.
- Produces: no legacy schema definition, alias, export, or repository import; HTTP `BaseJobResponse.status` uses `job_contracts.JobStatus`; all event usages use `JobEvent`.

- [ ] **Step 1: Add a failing legacy-removal architecture test**

Add a test that imports the schema modules and verifies the removed names are absent:

```python
def test_http_schemas_do_not_export_legacy_job_contracts() -> None:
    from src.entrypoints.http import schemas
    from src.entrypoints.http.schemas import backtest, common, job

    assert not hasattr(common, "JobStatus")
    assert not hasattr(common, "SSEJobEvent")
    assert not hasattr(job, "JobStatus")
    assert not hasattr(job, "JobProgress")
    assert not hasattr(backtest, "JobStatus")
    assert not hasattr(schemas, "JobStatus")
```

Add a repository scan assertion which fails if any Python file outside the canonical contract imports those three names from an HTTP schema module.

```python
def test_repository_does_not_import_legacy_job_contract_paths() -> None:
    violations = _legacy_job_schema_imports(SRC_ROOT, PROJECT_ROOT / "tests")
    assert not violations, "Legacy job contract imports found:\n" + "\n".join(
        violations
    )
```

- [ ] **Step 2: Run the removal tests and witness RED**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/architecture/test_layer_boundaries.py::test_http_schemas_do_not_export_legacy_job_contracts \
  tests/unit/architecture/test_layer_boundaries.py::test_repository_does_not_import_legacy_job_contract_paths -q
```

Expected: both tests fail and report current schema exports/imports.

- [ ] **Step 3: Remove HTTP definitions and exports**

- In `common.py`, delete the local `JobStatus` and `SSEJobEvent`; import the module as `from src.application.contracts import jobs as job_contracts`; annotate `BaseJobResponse.status` as `job_contracts.JobStatus`.
- In `job.py`, delete the local `JobStatus` and `JobProgress`; leave `CancelJobResponse` as the only schema in this file.
- In `backtest.py`, stop importing/re-exporting `JobStatus`; import only `BaseJobResponse` from `common.py`; remove the compatibility `__all__` declaration.
- In `schemas/__init__.py`, remove the `JobStatus` import and `__all__` entry.

- [ ] **Step 4: Migrate routes and tests to canonical imports**

Routes and tests import directly from:

```python
from src.application.contracts.jobs import JobEvent, JobStatus
```

Rename all `SSEJobEvent` annotations and constructors to `JobEvent`. Do not introduce aliases such as `JobEvent as SSEJobEvent`.

Use this scan until it returns no matches:

```bash
rg -n "from src\.entrypoints\.http\.schemas\..* import .*\b(JobStatus|JobProgress|SSEJobEvent)\b" apps/bt/src apps/bt/tests
```

- [ ] **Step 5: Document the canonical boundary**

Add a concise example to `docs/bt-src-layering-guide.md` stating that job lifecycle contracts live in `src.application.contracts.jobs`, HTTP schemas depend inward on them, and legacy HTTP schema re-exports are forbidden.

- [ ] **Step 6: Run focused GREEN verification**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/application/contracts/test_jobs.py \
  tests/unit/architecture \
  tests/server/test_schemas.py \
  tests/server/test_job_manager.py \
  tests/server/test_sse.py \
  tests/server/test_routes.py \
  tests/server/routes/test_lab.py \
  tests/unit/server/routes/test_analytics_complex.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/server/routes/test_optimize.py \
  tests/unit/server/test_routes_dataset_jobs.py \
  tests/unit/server/test_routes_db_sync.py \
  tests/unit/server/test_dataset_builder_service_branches.py \
  tests/unit/server/test_generic_job_manager.py \
  tests/unit/server/test_run_contracts.py \
  tests/unit/server/test_run_registry.py -q
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
./scripts/check-contract-sync.sh
git diff --check
```

Expected: all selected tests pass; Ruff, Pyright, contract sync, and diff check exit 0. `check-contract-sync.sh` leaves no generated TypeScript diff.

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/entrypoints apps/bt/tests docs/bt-src-layering-guide.md
git commit -m "refactor(bt): remove legacy job schema exports"
```

### Task 4: Whole-Slice Verification

**Files:**
- Verify only; modify files only if a failing test exposes a regression, and add a regression test before its fix.

**Interfaces:**
- Consumes: completed Tasks 1–3.
- Produces: fresh evidence that the slice preserves behavior and contracts.

- [ ] **Step 1: Confirm no forbidden imports or exports remain**

Run:

```bash
rg -n "from src\.entrypoints\.http\.schemas\..* import .*\b(JobStatus|JobProgress|SSEJobEvent)\b" apps/bt/src apps/bt/tests
```

Expected: no output and exit code 1 from `rg` because there are no matches.

- [ ] **Step 2: Run complete affected verification**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/application/contracts \
  tests/unit/architecture \
  tests/server/test_schemas.py \
  tests/server/test_job_manager.py \
  tests/server/test_sse.py \
  tests/server/test_routes.py \
  tests/server/routes/test_lab.py \
  tests/unit/server -q
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
git diff --check
git status --short
```

Expected: all tests and checks exit 0; status shows no uncommitted files.

- [ ] **Step 3: Request whole-branch review**

Generate a review package from the branch merge base through `HEAD`. The reviewer must verify the design completion criteria, no legacy aliases, exact baseline shrinkage, and wire-contract preservation. Fix every Critical or Important finding with a regression test, re-run its covering tests, and request re-review.
