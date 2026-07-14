# Signal Reference Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the complete signal-reference DTO graph from HTTP schemas to application contracts, delete the legacy HTTP module without compatibility surfaces, and reduce the application-to-HTTP dependency baseline from 37 to 35.

**Architecture:** `src.application.contracts.signal_reference` becomes the sole owner of two serialized literal aliases and seven Pydantic models. Compiler availability enums remain domain-owned, while application services and HTTP adapters consume the canonical contract module through qualified references.

**Tech Stack:** Python 3.12, Pydantic v2, FastAPI/OpenAPI, pytest, Ruff, Pyright, Bun/openapi-typescript

## Global Constraints

- Preserve every class name, docstring, field name, annotation, default, default factory, constraint, serialized payload, forward-reference relationship, and OpenAPI component identity.
- Preserve literal value order for `SignalFieldTypeValue` and `SignalExecutionSemantics`.
- Keep `CompiledSignalScope` and `CompiledSignalAvailability` domain-owned.
- Delete `src/entrypoints/http/schemas/signal_reference.py` after migration.
- Do not add compatibility aliases, HTTP re-exports, forwarding modules, subclasses, duplicate models, or conversion wrappers.
- HTTP modules must use qualified application-contract references and must not bind any of the nine canonical names.
- Delete exactly two signal-reference dependency-baseline rows; the final non-comment count is exactly 35.
- Do not change signal registry behavior, authoring copy, computation endpoints, or execution-policy semantics.
- Regenerated OpenAPI and TypeScript contract files must have zero diff.

---

### Task 1: Add Canonical Signal Reference Contracts

**Files:**

- Create: `apps/bt/src/application/contracts/signal_reference.py`
- Create: `apps/bt/tests/unit/application/contracts/test_signal_reference.py`

**Interfaces:**

- Produces: `SignalFieldTypeValue`, `SignalExecutionSemantics`, `FieldConstraints`, `SignalFieldSchema`, `SignalChartCapability`, `SignalReferenceSchema`, `SignalAvailabilityProfile`, `SignalCategorySchema`, and `SignalReferenceResponse`
- Consumes: `CompiledSignalScope` and `CompiledSignalAvailability` from `src.domains.strategy.runtime.compiler`
- Preserves: all schemas and serialization currently owned by `entrypoints.http.schemas.signal_reference`

- [ ] **Step 1: Write failing canonical-contract tests**

Create `test_signal_reference.py` with imports from the missing application
module and tests for literal order, complete nested serialization, fresh mutable
defaults, forward-reference resolution, compiler enum validation, schema titles,
properties, and required fields. The core fixtures are:

```python
from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError

from src.application.contracts import signal_reference as signal_reference_contracts
from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    CompiledSignalAvailability,
    CompiledSignalScope,
)


def _availability() -> CompiledSignalAvailability:
    return CompiledSignalAvailability(
        observation_time=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        available_at=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        decision_cutoff=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        execution_session=CompiledExecutionSession.CURRENT_SESSION,
    )


def test_signal_reference_literals_are_stable() -> None:
    assert get_args(signal_reference_contracts.SignalFieldTypeValue) == (
        "boolean",
        "number",
        "string",
        "select",
    )
    assert get_args(signal_reference_contracts.SignalExecutionSemantics) == (
        "standard",
        "next_session_round_trip",
        "current_session_round_trip",
        "overnight_round_trip",
    )

    for alias, values in (
        (
            signal_reference_contracts.SignalFieldTypeValue,
            ("boolean", "number", "string", "select"),
        ),
        (
            signal_reference_contracts.SignalExecutionSemantics,
            (
                "standard",
                "next_session_round_trip",
                "current_session_round_trip",
                "overnight_round_trip",
            ),
        ),
    ):
        adapter = TypeAdapter(alias)
        for value in values:
            assert adapter.validate_python(value) == value
        with pytest.raises(ValidationError):
            adapter.validate_python("legacy")
```

Construct a complete `SignalReferenceResponse` containing one signal, one field,
one availability profile, one category, and non-default chart flags. Assert its
entire `model_dump(mode="json")` payload. Instantiate two default-valued signal
models and assert that `when_to_use`, `pitfalls`, `examples`,
`data_requirements`, `availability_profiles`, `supported_modes`, and `chart`
objects are distinct.

Encode these exact `(properties, required)` expectations so the tests remain
valid after the old HTTP module is deleted:

```python
EXPECTED_SHAPES = {
    "FieldConstraints": ({"gt", "ge", "lt", "le"}, []),
    "SignalFieldSchema": (
        {
            "name", "label", "type", "description", "default", "options",
            "constraints", "unit", "placeholder",
        },
        ["name", "type", "description"],
    ),
    "SignalChartCapability": (
        {
            "supported", "supported_modes", "supports_relative_mode",
            "requires_benchmark", "requires_sector_data", "requires_margin_data",
            "requires_statements_data",
        },
        [],
    ),
    "SignalReferenceSchema": (
        {
            "key", "signal_type", "name", "category", "description", "summary",
            "when_to_use", "pitfalls", "examples", "usage_hint", "fields",
            "yaml_snippet", "exit_disabled", "data_requirements",
            "availability_profiles", "chart",
        },
        [
            "key", "signal_type", "name", "category", "description",
            "usage_hint", "fields", "yaml_snippet",
        ],
    ),
    "SignalAvailabilityProfile": (
        {"scope", "execution_semantics", "availability"},
        ["scope", "execution_semantics", "availability"],
    ),
    "SignalCategorySchema": ({"key", "label"}, ["key", "label"]),
    "SignalReferenceResponse": (
        {"signals", "categories", "total"},
        ["signals", "categories", "total"],
    ),
}
```

For each class, assert `set(schema["properties"])` and
`schema.get("required", [])` against this mapping. Also assert:

```python
assert signal_reference_contracts.SignalReferenceSchema.model_json_schema()["title"] == "SignalReferenceSchema"
assert signal_reference_contracts.SignalReferenceSchema.__doc__ == "シグナル定義"
assert signal_reference_contracts.SignalAvailabilityProfile.model_fields[
    "scope"
].annotation is CompiledSignalScope
assert signal_reference_contracts.SignalAvailabilityProfile.model_fields[
    "availability"
].annotation is CompiledSignalAvailability
```

- [ ] **Step 2: Run RED**

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_signal_reference.py
```

Expected: collection fails with
`ModuleNotFoundError: No module named 'src.application.contracts.signal_reference'`.

- [ ] **Step 3: Add the canonical module**

Create `application/contracts/signal_reference.py` by copying the current two
aliases and seven models without semantic changes:

```python
"""Application-owned signal reference contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from src.domains.strategy.runtime.compiler import (
    CompiledSignalAvailability,
    CompiledSignalScope,
)

SignalFieldTypeValue = Literal["boolean", "number", "string", "select"]
SignalExecutionSemantics = Literal[
    "standard",
    "next_session_round_trip",
    "current_session_round_trip",
    "overnight_round_trip",
]


class FieldConstraints(BaseModel):
    """フィールド制約情報"""

    gt: float | None = None
    ge: float | None = None
    lt: float | None = None
    le: float | None = None


class SignalFieldSchema(BaseModel):
    """シグナルフィールド定義"""

    name: str
    label: str | None = Field(default=None, description="Display label")
    type: SignalFieldTypeValue
    description: str
    default: bool | int | float | str | None = None
    options: list[str] | None = None
    constraints: FieldConstraints | None = None
    unit: str | None = Field(default=None, description="Display unit")
    placeholder: str | None = Field(default=None, description="Suggested placeholder")


class SignalChartCapability(BaseModel):
    """Chart overlay capability metadata."""

    supported: bool = True
    supported_modes: list[str] = Field(default_factory=list)
    supports_relative_mode: bool = True
    requires_benchmark: bool = False
    requires_sector_data: bool = False
    requires_margin_data: bool = False
    requires_statements_data: bool = False


class SignalReferenceSchema(BaseModel):
    """シグナル定義"""

    key: str = Field(description="param_keyベースの安定スラッグ")
    signal_type: str = Field(description="chart/signal API で使用する signal type")
    name: str
    category: str
    description: str
    summary: str | None = Field(default=None, description="Short authoring summary")
    when_to_use: list[str] = Field(default_factory=list)
    pitfalls: list[str] = Field(default_factory=list)
    examples: list[str] = Field(default_factory=list)
    usage_hint: str = Field(description="entry_purpose + exit_purposeから自動合成")
    fields: list[SignalFieldSchema]
    yaml_snippet: str
    exit_disabled: bool = False
    data_requirements: list[str] = Field(default_factory=list)
    availability_profiles: list["SignalAvailabilityProfile"] = Field(default_factory=list)
    chart: SignalChartCapability = Field(default_factory=SignalChartCapability)


class SignalAvailabilityProfile(BaseModel):
    """Compiled availability profile for a signal under one execution semantic."""

    scope: CompiledSignalScope
    execution_semantics: SignalExecutionSemantics
    availability: CompiledSignalAvailability


class SignalCategorySchema(BaseModel):
    """シグナルカテゴリ定義"""

    key: str
    label: str


class SignalReferenceResponse(BaseModel):
    """シグナルリファレンス レスポンス"""

    signals: list[SignalReferenceSchema]
    categories: list[SignalCategorySchema]
    total: int
```

- [ ] **Step 4: Run GREEN and exact legacy parity**

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_signal_reference.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check \
  src/application/contracts/signal_reference.py \
  tests/unit/application/contracts/test_signal_reference.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright \
  src/application/contracts/signal_reference.py
```

Before deleting the old module, add a temporary local comparison or use an
interactive Python process with this exact logic, then discard it before the
commit:

```python
from typing import get_args
from src.application.contracts import signal_reference as new
from src.entrypoints.http.schemas import signal_reference as old

names = (
    "FieldConstraints", "SignalFieldSchema", "SignalChartCapability",
    "SignalReferenceSchema", "SignalAvailabilityProfile",
    "SignalCategorySchema", "SignalReferenceResponse",
)
for name in names:
    assert getattr(new, name).model_json_schema() == getattr(old, name).model_json_schema()
    assert getattr(new, name).__doc__ == getattr(old, name).__doc__
for name in ("SignalFieldTypeValue", "SignalExecutionSemantics"):
    assert get_args(getattr(new, name)) == get_args(getattr(old, name))
```

Expected: no assertion and exit zero.

- [ ] **Step 5: Commit Task 1**

```bash
git add apps/bt/src/application/contracts/signal_reference.py \
  apps/bt/tests/unit/application/contracts/test_signal_reference.py
git commit -m "feat(bt): add canonical signal reference contracts"
```

---

### Task 2: Delete HTTP Ownership and Migrate Every Consumer

**Files:**

- Delete: `apps/bt/src/entrypoints/http/schemas/signal_reference.py`
- Modify: `apps/bt/src/application/services/signal_reference_service.py`
- Modify: `apps/bt/src/application/services/strategy_authoring_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/signal_reference.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/strategy_authoring.py`
- Modify: signal-reference and strategy-authoring tests that reference the old module
- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`

**Interfaces:**

- Consumes: all nine names from `src.application.contracts.signal_reference`
- Produces: the unchanged `/api/signals/reference` response and strategy-authoring payloads without HTTP-owned application DTOs
- Preserves: registry reflection, copy generation, field inference, route error mapping, and OpenAPI component names

- [ ] **Step 1: Extend ownership guards before production migration**

Add these names to `FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES`:

```python
"SignalFieldTypeValue",
"SignalExecutionSemantics",
"FieldConstraints",
"SignalFieldSchema",
"SignalChartCapability",
"SignalReferenceSchema",
"SignalAvailabilityProfile",
"SignalCategorySchema",
"SignalReferenceResponse",
```

Add synthetic cases that reject a direct HTTP import, a class definition, a
literal alias assignment, and `__all__` exports. Add a qualified application
contract import to the allowed cases.

- [ ] **Step 2: Run architecture RED**

```bash
./scripts/bt-pytest.sh tests/unit/architecture/test_layer_boundaries.py
```

Expected: repository scans report the old HTTP definitions, both application
imports, HTTP schema/route bindings, and any test imports of the old module.

- [ ] **Step 3: Migrate application services**

In both services use:

```python
from src.application.contracts import signal_reference as signal_reference_contracts
```

Change `_get_field_type` to return
`signal_reference_contracts.SignalFieldTypeValue`. Replace construction and
annotations for `FieldConstraints` and `SignalCategorySchema` with qualified
application-contract names. Do not change registry traversal or returned data.

- [ ] **Step 4: Migrate HTTP adapters with qualified names**

In `routes/signal_reference.py` import the canonical module and use:

```python
@router.get(
    "/api/signals/reference",
    response_model=signal_reference_contracts.SignalReferenceResponse,
)
async def get_signal_reference() -> signal_reference_contracts.SignalReferenceResponse:
    data = build_signal_reference()
    return signal_reference_contracts.SignalReferenceResponse(**data)
```

Keep the existing exception handling around this body. In
`schemas/strategy_authoring.py`, use qualified annotations:

```python
constraints: signal_reference_contracts.FieldConstraints | None = Field(
    default=None,
    description="Numeric constraints",
)
signal_categories: list[signal_reference_contracts.SignalCategorySchema]
```

Do not bind any canonical name in an HTTP module.

- [ ] **Step 5: Delete legacy ownership and migrate tests**

Delete `entrypoints/http/schemas/signal_reference.py`. Replace every source and
test import of that path with the canonical qualified module. Do not create an
HTTP replacement module.

- [ ] **Step 6: Shrink the baseline**

Delete exactly:

```text
application/services/signal_reference_service.py|src.entrypoints.http.schemas.signal_reference
application/services/strategy_authoring_service.py|src.entrypoints.http.schemas.signal_reference
```

Assert exactly 35 non-comment entries remain.

- [ ] **Step 7: Run focused behavior tests**

```bash
./scripts/bt-pytest.sh \
  tests/unit/application/contracts/test_signal_reference.py \
  tests/unit/architecture/test_layer_boundaries.py \
  tests/server/test_signal_reference.py \
  tests/server/test_schema_sync.py \
  tests/unit/server/routes/test_signal_reference.py \
  tests/unit/server/routes/test_strategies.py \
  tests/unit/server/test_openapi.py
```

Expected: all collected tests pass. Existing warnings are acceptable only when
unchanged from baseline.

- [ ] **Step 8: Run static, dependency, and contract gates**

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check \
  src tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
```

From `apps/ts` run:

```bash
bun run --filter @trading25/contracts typecheck
bun run --filter @trading25/api-clients typecheck
```

Then verify:

```bash
test "$(awk 'NF && $1 !~ /^#/' \
  apps/bt/tests/unit/architecture/application_http_schema_imports.txt | wc -l | tr -d ' ')" = 35
git diff --exit-code -- \
  apps/ts/packages/contracts/openapi/bt-openapi.json \
  apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
git diff --check
```

- [ ] **Step 9: Commit Task 2**

```bash
git add -A apps/bt/src/application/services \
  apps/bt/src/entrypoints/http/routes/signal_reference.py \
  apps/bt/src/entrypoints/http/schemas \
  apps/bt/tests
git commit -m "refactor(bt): move signal reference contracts to application"
```

---

### Task 3: Whole-Slice Verification and Independent Review

**Files:**

- Verify only; no planned production changes

**Interfaces:**

- Consumes: Tasks 1 and 2
- Produces: fresh completion evidence and an independent final review

- [ ] **Step 1: Re-run the complete focused suite**

Run the Task 2 focused test command plus the CI contract-sync behavior tests:

```bash
./scripts/bt-pytest.sh \
  tests/unit/scripts/test_check_contract_sync.py \
  tests/unit/application/contracts/test_signal_reference.py \
  tests/unit/architecture/test_layer_boundaries.py \
  tests/server/test_signal_reference.py \
  tests/server/test_schema_sync.py \
  tests/unit/server/routes/test_signal_reference.py \
  tests/unit/server/routes/test_strategies.py \
  tests/unit/server/test_openapi.py
```

- [ ] **Step 2: Re-run all blocker gates from clean HEAD**

Run Ruff, Pyright, dependency direction, contract sync, both TS typechecks,
`python3 scripts/skills/refresh_skill_references.py --check`, baseline count 35,
generated-file zero diff, `git diff --check`, and `git status --short`.

- [ ] **Step 3: Request a history-independent final review**

Provide a fresh reviewer with the design, this plan, TDD reports, per-task
reviews, base SHA, head SHA, complete diff, and fresh verification outputs.
Require Critical/Important/Minor findings, spec compliance, quality approval,
and readiness. Fix and re-review every Critical or Important finding before
claiming completion.
