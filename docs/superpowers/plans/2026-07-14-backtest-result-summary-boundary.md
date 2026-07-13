# Backtest Result Summary Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `src.application.contracts.backtest.BacktestResultSummary` the only model definition and remove the obsolete HTTP-schema ownership/import path without changing the wire contract.

**Architecture:** Introduce the canonical Pydantic model in the application contract package. Application code, routes, and tests consume it directly; HTTP response schemas refer to it through a module-qualified inward import and do not re-export it. Generalize the existing static boundary guard so both job contracts and the result summary cannot return to the HTTP schema layer.

**Tech Stack:** Python 3.12, Pydantic 2, FastAPI, pytest, Ruff, Pyright, OpenAPI, generated TypeScript contracts

## Global Constraints

- Preserve all eight `BacktestResultSummary` fields, types, requiredness, defaults, and descriptions exactly.
- Preserve Pydantic default coercion, extra-field behavior, `model_dump(mode="json")`, and `model_validate` behavior.
- Preserve the OpenAPI component name `BacktestResultSummary` and every endpoint reference to it.
- OpenAPI snapshot and generated TypeScript output must have zero semantic and textual diff.
- Compatibility aliases, schema-layer re-exports, wrappers, subclasses, and transitional imports are forbidden.
- Do not move `SignalAttributionResult` or unrelated request/response DTOs.
- The exact application-to-HTTP-schema baseline must decrease from 54 to 50 entries.
- Every production change follows a witnessed RED → GREEN test cycle.

---

### Task 1: Add the Canonical Application Contract

**Files:**

- Create: `apps/bt/src/application/contracts/backtest.py`
- Create: `apps/bt/tests/unit/application/contracts/test_backtest.py`

**Interfaces:**

- Produces: `src.application.contracts.backtest.BacktestResultSummary`
- Consumes: Pydantic `BaseModel` and `Field`
- Preserves: current eight-field JSON schema and serialization behavior

- [ ] **Step 1: Write the failing canonical-contract tests**

Create `test_backtest.py` with tests that import the not-yet-existing model and assert the complete serialized payload and JSON-schema contract:

```python
from src.application.contracts.backtest import BacktestResultSummary


def _summary() -> BacktestResultSummary:
    return BacktestResultSummary(
        total_return=12.5,
        sharpe_ratio=1.2,
        sortino_ratio=1.4,
        calmar_ratio=0.8,
        max_drawdown=-9.5,
        win_rate=54.0,
        trade_count=42,
        html_path="/tmp/result.html",
    )


def test_backtest_result_summary_serialization_is_stable() -> None:
    assert _summary().model_dump(mode="json") == {
        "total_return": 12.5,
        "sharpe_ratio": 1.2,
        "sortino_ratio": 1.4,
        "calmar_ratio": 0.8,
        "max_drawdown": -9.5,
        "win_rate": 54.0,
        "trade_count": 42,
        "html_path": "/tmp/result.html",
    }


def test_backtest_result_summary_schema_is_stable() -> None:
    schema = BacktestResultSummary.model_json_schema()

    assert schema["title"] == "BacktestResultSummary"
    assert schema["required"] == [
        "total_return",
        "sharpe_ratio",
        "calmar_ratio",
        "max_drawdown",
        "win_rate",
        "trade_count",
    ]
    assert set(schema["properties"]) == {
        "total_return",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "max_drawdown",
        "win_rate",
        "trade_count",
        "html_path",
    }


def test_backtest_result_summary_optional_fields_default_to_none() -> None:
    summary = BacktestResultSummary(
        total_return=0.0,
        sharpe_ratio=0.0,
        calmar_ratio=0.0,
        max_drawdown=0.0,
        win_rate=0.0,
        trade_count=0,
    )

    assert summary.sortino_ratio is None
    assert summary.html_path is None
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_backtest.py
```

Expected: collection fails with `ModuleNotFoundError: No module named 'src.application.contracts.backtest'`.

- [ ] **Step 3: Implement the canonical model**

Create `src/application/contracts/backtest.py`:

```python
"""Application-owned backtest result contracts."""

from pydantic import BaseModel, Field


class BacktestResultSummary(BaseModel):
    """バックテスト結果サマリー"""

    total_return: float = Field(description="トータルリターン (%)")
    sharpe_ratio: float = Field(description="シャープレシオ")
    sortino_ratio: float | None = Field(default=None, description="ソルティノレシオ")
    calmar_ratio: float = Field(description="カルマーレシオ")
    max_drawdown: float = Field(description="最大ドローダウン (%)")
    win_rate: float = Field(description="勝率 (%)")
    trade_count: int = Field(description="取引回数")
    html_path: str | None = Field(default=None, description="結果HTMLファイルのパス")
```

- [ ] **Step 4: Run GREEN verification**

Run:

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_backtest.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src/application/contracts/backtest.py tests/unit/application/contracts/test_backtest.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src/application/contracts/backtest.py
```

Expected: 3 tests pass; Ruff and Pyright exit 0.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/contracts/backtest.py apps/bt/tests/unit/application/contracts/test_backtest.py
git commit -m "feat(bt): add canonical backtest result summary"
```

---

### Task 2: Delete the HTTP Ownership and Migrate Every Consumer

**Files:**

- Delete: `apps/bt/tests/unit/architecture/job_contract_boundary_guard.py`
- Create: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Modify: `apps/bt/src/entrypoints/http/schemas/backtest.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/__init__.py`
- Modify: `apps/bt/src/entrypoints/http/routes/backtest.py`
- Modify: `apps/bt/src/application/services/backtest_result_summary.py`
- Modify: `apps/bt/src/application/services/backtest_service.py`
- Modify: `apps/bt/src/application/services/job_manager.py`
- Modify: `apps/bt/src/application/services/run_registry.py`
- Modify: `apps/bt/src/application/workers/backtest_worker.py`
- Modify: `apps/bt/tests/server/test_schemas.py`
- Modify: `apps/bt/tests/server/test_job_manager.py`
- Modify: `apps/bt/tests/unit/server/test_run_contracts.py`
- Modify: `apps/bt/tests/unit/server/test_run_registry.py`
- Modify: `apps/bt/tests/unit/server/services/test_backtest_result_summary.py`
- Modify: `apps/bt/tests/unit/server/services/test_verification_orchestrator.py`
- Modify: `apps/bt/tests/unit/server/routes/test_backtest.py`

**Interfaces:**

- Consumes: `src.application.contracts.backtest.BacktestResultSummary` from Task 1
- Produces: HTTP response schemas containing `backtest_contracts.BacktestResultSummary`
- Produces: `forbidden_http_application_contract_references(...) -> list[str]` in the generalized test guard
- Preserves: `SignalAttributionResult` in the HTTP schema layer and the mixed `run_registry.py` baseline entry

- [ ] **Step 1: Generalize the architecture guard and add the new forbidden contract**

Rename the guard module to `application_contract_boundary_guard.py`. Rename:

```python
FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
```

to:

```python
FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES = {
    "JobStatus",
    "JobProgress",
    "JobEvent",
    "SSEJobEvent",
    "BacktestResultSummary",
}
```

Rename `forbidden_http_job_contract_references` to `forbidden_http_application_contract_references`, update diagnostic text from “job contracts” to “application contracts”, and update `test_layer_boundaries.py` imports, helper names, and test names. Keep all existing AST coverage for direct imports, schema bindings, aliases, control-flow bindings, and `__all__` exports.

- [ ] **Step 2: Run the guard and verify RED against the current tree**

Run:

```bash
./scripts/bt-pytest.sh tests/unit/architecture/test_layer_boundaries.py
```

Expected: failure listing the current HTTP-layer `BacktestResultSummary` class/export and application imports. The failure must be caused by the newly forbidden contract, while existing job-contract guard cases remain green.

- [ ] **Step 3: Move all production consumers to the canonical contract**

Apply these exact ownership rules:

```python
# application services, worker, route
from src.application.contracts.backtest import BacktestResultSummary
```

In `run_registry.py`, split the imports:

```python
from src.application.contracts.backtest import BacktestResultSummary
from src.entrypoints.http.schemas.backtest import SignalAttributionResult
```

In `schemas/backtest.py`, delete the class definition and add:

```python
from src.application.contracts import backtest as backtest_contracts
```

Use the module-qualified type in both response fields:

```python
result: backtest_contracts.BacktestResultSummary | None
summary: backtest_contracts.BacktestResultSummary
```

Do not assign `BacktestResultSummary = backtest_contracts.BacktestResultSummary`.

Remove `BacktestResultSummary` from the imports and `__all__` in `schemas/__init__.py`. Change every test that constructs or annotates this model to import it directly from `src.application.contracts.backtest`.

- [ ] **Step 4: Shrink the exact dependency baseline**

Delete exactly these four lines from `application_http_schema_imports.txt`:

```text
application/services/backtest_result_summary.py|src.entrypoints.http.schemas.backtest
application/services/backtest_service.py|src.entrypoints.http.schemas.backtest
application/services/job_manager.py|src.entrypoints.http.schemas.backtest
application/workers/backtest_worker.py|src.entrypoints.http.schemas.backtest
```

Retain:

```text
application/services/run_registry.py|src.entrypoints.http.schemas.backtest
```

- [ ] **Step 5: Verify no legacy ownership or import surface remains**

Run:

```bash
rg -n "from src\.entrypoints\.http\.schemas(?:\.backtest)? import .*BacktestResultSummary|class BacktestResultSummary|\"BacktestResultSummary\"" apps/bt/src/entrypoints/http apps/bt/src/application
```

Expected: no class, import, assignment, or package export of the old path. Module-qualified response annotations may contain the class name and are allowed.

Run:

```bash
./scripts/bt-pytest.sh tests/unit/architecture/test_layer_boundaries.py
```

Expected: all architecture tests pass and the exact baseline has 50 entries.

- [ ] **Step 6: Run focused behavior verification**

Run:

```bash
./scripts/bt-pytest.sh \
  tests/unit/application/contracts/test_backtest.py \
  tests/server/test_schemas.py \
  tests/server/test_job_manager.py \
  tests/unit/server/services/test_backtest_result_summary.py \
  tests/unit/server/services/test_verification_orchestrator.py \
  tests/unit/server/test_run_registry.py \
  tests/unit/server/test_run_contracts.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/server/test_backtest_worker.py
```

Expected: all selected tests pass with unchanged serialization, persistence, artifact fallback, route responses, and worker behavior.

- [ ] **Step 7: Run static and contract verification**

Run:

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src/application src/entrypoints/http tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src/application src/entrypoints/http
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
```

Expected: Ruff and Pyright pass, dependency direction has zero violations, contract sync passes, and the generated contract files have no diff.

- [ ] **Step 8: Commit**

```bash
git add -A apps/bt/src apps/bt/tests
git commit -m "refactor(bt): move backtest result summary to application"
```

---

### Task 3: Whole-Slice Verification and Review

**Files:**

- Verify only; no planned production changes

**Interfaces:**

- Consumes: canonical model and migrated consumers from Tasks 1–2
- Produces: evidence that the slice is ready for the next DTO migration

- [ ] **Step 1: Run the complete relevant test set from a clean HEAD**

Run:

```bash
./scripts/bt-pytest.sh \
  tests/unit/application/contracts \
  tests/unit/architecture \
  tests/server/test_schemas.py \
  tests/server/test_job_manager.py \
  tests/unit/server/services/test_backtest_result_summary.py \
  tests/unit/server/services/test_verification_orchestrator.py \
  tests/unit/server/test_run_registry.py \
  tests/unit/server/test_run_contracts.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/server/test_backtest_worker.py
```

Expected: all tests pass.

- [ ] **Step 2: Run repository boundary and contract checks**

Run:

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src/application src/entrypoints/http tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src/application src/entrypoints/http
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
python3 scripts/skills/refresh_skill_references.py --check
git diff --check
git status --short
```

Expected: every check exits 0 and the worktree is clean.

- [ ] **Step 3: Request whole-branch review**

Generate a review package from the pre-slice commit through `HEAD`. The reviewer must verify the approved design, absence of compatibility paths, exact baseline shrinkage, persistence semantics, OpenAPI identity, and test evidence. Any Critical or Important finding must be fixed and re-reviewed before completion.

