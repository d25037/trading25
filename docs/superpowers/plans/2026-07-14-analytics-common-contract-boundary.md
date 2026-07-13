# Analytics Common Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the shared analytics provenance and diagnostics contracts from the HTTP schema layer to the application layer without changing runtime payloads or generated API contracts.

**Architecture:** `src.application.contracts.analytics` becomes the only definition site for `AnalyticsSourceKind`, `ResponseDiagnostics`, and `DataProvenance`. Application services import the canonical values directly, while endpoint-specific HTTP schemas use module-qualified application annotations and retain only transport wrappers. An AST ownership guard prevents later HTTP redefinition or re-export.

**Tech Stack:** Python 3.12, Pydantic v2, FastAPI/OpenAPI, pytest, Ruff, Pyright, Bun, openapi-typescript

## Global Constraints

- Preserve every existing field name, annotation, default, default factory, class docstring, serialized payload, and OpenAPI component shape.
- Do not add compatibility aliases, re-exports, duplicate models, subclasses, wrappers, or forwarding modules for the migrated names.
- HTTP schema modules must use module-qualified `analytics_contracts.*` references and must not bind the migrated names.
- Leave market bubble footprint HTTP models in `entrypoints/http/schemas/analytics_common.py`.
- Delete exactly seven `analytics_common` dependency-baseline rows; the resulting non-comment baseline count is exactly 42.
- Regenerated OpenAPI and TypeScript contract files must have zero diff.
- Preserve all analytics calculations, endpoint paths, status codes, and response nesting.

---

### Task 1: Add Canonical Analytics Contracts

**Files:**

- Create: `apps/bt/src/application/contracts/analytics.py`
- Create: `apps/bt/tests/unit/application/contracts/test_analytics.py`

**Interfaces:**

- Produces: `AnalyticsSourceKind`, `ResponseDiagnostics`, and `DataProvenance`
- Consumes: `typing.Literal`, `pydantic.BaseModel`, and `pydantic.Field`
- Preserves: the JSON schema and serialization of the current HTTP-owned models

- [ ] **Step 1: Write failing canonical-contract tests**

Create `test_analytics.py` with the following complete contents:

```python
import pytest
from pydantic import ValidationError

from src.application.contracts.analytics import DataProvenance, ResponseDiagnostics


def test_response_diagnostics_complete_serialization_is_stable() -> None:
    diagnostics = ResponseDiagnostics(
        missing_required_data=["statements"],
        used_fields=["eps", "forecast_eps"],
        effective_period_type="FY",
        warnings=["partial history"],
    )
    assert diagnostics.model_dump(mode="json") == {
        "missing_required_data": ["statements"],
        "used_fields": ["eps", "forecast_eps"],
        "effective_period_type": "FY",
        "warnings": ["partial history"],
    }


def test_data_provenance_complete_serialization_is_stable() -> None:
    provenance = DataProvenance(
        source_kind="dataset",
        market_snapshot_id="market-1",
        dataset_snapshot_id="dataset-1",
        reference_date="2026-07-14",
        loaded_domains=["stock_data", "statements"],
        strategy_name="production/example",
        strategy_fingerprint="sha256:example",
        warnings=["snapshot warning"],
    )
    assert provenance.model_dump(mode="json") == {
        "source_kind": "dataset",
        "market_snapshot_id": "market-1",
        "dataset_snapshot_id": "dataset-1",
        "reference_date": "2026-07-14",
        "loaded_domains": ["stock_data", "statements"],
        "strategy_name": "production/example",
        "strategy_fingerprint": "sha256:example",
        "warnings": ["snapshot warning"],
    }


def test_analytics_contract_defaults_are_stable_and_independent() -> None:
    first_diagnostics = ResponseDiagnostics()
    second_diagnostics = ResponseDiagnostics()
    first_provenance = DataProvenance(source_kind="market")
    second_provenance = DataProvenance(source_kind="market")

    assert first_diagnostics.model_dump(mode="json") == {
        "missing_required_data": [],
        "used_fields": [],
        "effective_period_type": None,
        "warnings": [],
    }
    assert first_provenance.model_dump(mode="json") == {
        "source_kind": "market",
        "market_snapshot_id": None,
        "dataset_snapshot_id": None,
        "reference_date": None,
        "loaded_domains": [],
        "strategy_name": None,
        "strategy_fingerprint": None,
        "warnings": [],
    }
    assert first_diagnostics.missing_required_data is not second_diagnostics.missing_required_data
    assert first_diagnostics.used_fields is not second_diagnostics.used_fields
    assert first_diagnostics.warnings is not second_diagnostics.warnings
    assert first_provenance.loaded_domains is not second_provenance.loaded_domains
    assert first_provenance.warnings is not second_provenance.warnings


def test_analytics_contract_required_fields_and_literals_are_stable() -> None:
    with pytest.raises(ValidationError):
        DataProvenance.model_validate({})

    with pytest.raises(ValidationError):
        DataProvenance.model_validate({"source_kind": "legacy"})


def test_analytics_contract_json_schema_is_stable() -> None:
    diagnostics_schema = ResponseDiagnostics.model_json_schema()
    provenance_schema = DataProvenance.model_json_schema()

    assert diagnostics_schema["title"] == "ResponseDiagnostics"
    assert diagnostics_schema.get("required", []) == []
    assert set(diagnostics_schema["properties"]) == {
        "missing_required_data",
        "used_fields",
        "effective_period_type",
        "warnings",
    }
    assert provenance_schema["title"] == "DataProvenance"
    assert provenance_schema["required"] == ["source_kind"]
    assert set(provenance_schema["properties"]) == {
        "source_kind",
        "market_snapshot_id",
        "dataset_snapshot_id",
        "reference_date",
        "loaded_domains",
        "strategy_name",
        "strategy_fingerprint",
        "warnings",
    }
```

- [ ] **Step 2: Run the tests and witness RED**

Run:

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_analytics.py
```

Expected: collection fails with `ModuleNotFoundError` for `src.application.contracts.analytics`.

- [ ] **Step 3: Add the exact canonical definitions**

Create `analytics.py` with:

```python
"""Application-owned shared analytics contracts."""

from typing import Literal

from pydantic import BaseModel, Field


AnalyticsSourceKind = Literal["market", "dataset"]


class ResponseDiagnostics(BaseModel):
    """Common diagnostics payload for analytics-style responses."""

    missing_required_data: list[str] = Field(default_factory=list)
    used_fields: list[str] = Field(default_factory=list)
    effective_period_type: str | None = None
    warnings: list[str] = Field(default_factory=list)


class DataProvenance(BaseModel):
    """Common provenance payload for SoT-backed analytics responses."""

    source_kind: AnalyticsSourceKind
    market_snapshot_id: str | None = None
    dataset_snapshot_id: str | None = None
    reference_date: str | None = None
    loaded_domains: list[str] = Field(default_factory=list)
    strategy_name: str | None = None
    strategy_fingerprint: str | None = None
    warnings: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Run GREEN and static checks**

Run:

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_analytics.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src/application/contracts/analytics.py tests/unit/application/contracts/test_analytics.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src/application/contracts/analytics.py
```

Expected: all contract tests pass, Ruff reports no issues, and Pyright reports zero errors.

- [ ] **Step 5: Verify parity against the HTTP definitions**

Before Task 2 removes the old definitions, compare `model_json_schema()` and class docstrings for both models and assert exact equality. Confirm `AnalyticsSourceKind` accepts exactly `market` and `dataset`.

- [ ] **Step 6: Commit Task 1**

```bash
git add apps/bt/src/application/contracts/analytics.py apps/bt/tests/unit/application/contracts/test_analytics.py
git commit -m "feat(bt): add canonical analytics common contracts"
```

---

### Task 2: Delete HTTP Ownership and Migrate All Consumers

**Files:**

- Modify: `apps/bt/src/application/services/analytics_provenance.py`
- Modify: `apps/bt/src/application/services/fundamentals_service.py`
- Modify: `apps/bt/src/application/services/indicator_service.py`
- Modify: `apps/bt/src/application/services/margin_analytics_service.py`
- Modify: `apps/bt/src/application/services/roe_service.py`
- Modify: `apps/bt/src/application/services/screening_response_builder.py`
- Modify: `apps/bt/src/application/services/signal_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/analytics_common.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/analytics_margin.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/analytics_roe.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/fundamentals.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/indicators.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/screening.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/signals.py`
- Modify: `apps/bt/tests/server/routes/test_signals.py`
- Modify: `apps/bt/tests/unit/server/test_indicator_schemas.py`
- Modify: `apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py`
- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`

**Interfaces:**

- Consumes: the three canonical names from Task 1
- Produces: application-only ownership and module-qualified HTTP usage
- Preserves: endpoint response models, default factories, provenance helper behavior, and generated OpenAPI references

- [ ] **Step 1: Extend the ownership guard first**

Add the following names to `FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES`:

```python
"AnalyticsSourceKind",
"ResponseDiagnostics",
"DataProvenance",
```

Add synthetic cases that reject:

```python
from src.entrypoints.http.schemas.analytics_common import DataProvenance
class ResponseDiagnostics: ...
AnalyticsSourceKind = Literal["market", "dataset"]
__all__ = ["DataProvenance"]
```

The repository scan uses the shared forbidden-name set and therefore enforces all three names.

- [ ] **Step 2: Run the architecture tests and witness RED**

Run:

```bash
./scripts/bt-pytest.sh tests/unit/architecture/test_layer_boundaries.py
```

Expected: failures identify the old `analytics_common.py` definitions, all seven application imports, and any HTTP bindings/re-exports of the migrated names.

- [ ] **Step 3: Migrate application services**

Replace every application import from `src.entrypoints.http.schemas.analytics_common` with a direct import from `src.application.contracts.analytics`. Do not change construction, type annotations, return values, or serialization.

- [ ] **Step 4: Migrate HTTP schemas with qualified references**

In each of the six endpoint schema modules, replace the HTTP-schema import with:

```python
from src.application.contracts import analytics as analytics_contracts
```

Replace every `DataProvenance` and `ResponseDiagnostics` field annotation and default factory with the following qualified forms:

```python
provenance: analytics_contracts.DataProvenance
diagnostics: analytics_contracts.ResponseDiagnostics = Field(
    default_factory=analytics_contracts.ResponseDiagnostics
)
```

Do not bind `AnalyticsSourceKind`, `ResponseDiagnostics`, or `DataProvenance` in an HTTP schema module.

- [ ] **Step 5: Delete old ownership and migrate test imports**

Remove `AnalyticsSourceKind`, `ResponseDiagnostics`, and `DataProvenance` from `analytics_common.py`, including now-unused `Literal`. Keep its `BaseModel`, `Field`, and bubble-footprint models unchanged.

Change the three listed test modules to import `DataProvenance` and `ResponseDiagnostics` from `src.application.contracts.analytics`.

- [ ] **Step 6: Shrink the dependency baseline exactly**

Delete the seven rows ending in `src.entrypoints.http.schemas.analytics_common` for:

```text
application/services/analytics_provenance.py
application/services/fundamentals_service.py
application/services/indicator_service.py
application/services/margin_analytics_service.py
application/services/roe_service.py
application/services/screening_response_builder.py
application/services/signal_service.py
```

Confirm exactly 42 non-comment entries remain.

- [ ] **Step 7: Run focused behavior and architecture tests**

Run:

```bash
./scripts/bt-pytest.sh \
  tests/unit/application/contracts/test_analytics.py \
  tests/unit/architecture/test_layer_boundaries.py \
  tests/server/routes/test_signals.py \
  tests/unit/server/test_indicator_schemas.py \
  tests/unit/server/test_routes_analytics_fundamentals.py \
  tests/server/services/test_fundamentals_service.py \
  tests/server/services/test_signal_service.py \
  tests/server/test_indicator_service.py \
  tests/unit/server/services/test_indicator_service.py \
  tests/unit/server/services/test_roe_service.py \
  tests/unit/server/services/test_screening_service.py \
  tests/unit/server/services/test_screening_service_helpers.py \
  tests/unit/server/test_openapi.py
```

Expected: all tests pass with unchanged endpoint payloads and schemas.

- [ ] **Step 8: Run static, dependency, and contract verification**

Run:

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
```

From `apps/ts`, run:

```bash
bun run --filter @trading25/contracts typecheck
bun run --filter @trading25/api-clients typecheck
```

Then run:

```bash
git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
git diff --check
```

Expected: every command exits zero; dependency baseline has no stale rows; generated files have zero diff.

- [ ] **Step 9: Commit Task 2**

```bash
git add -A apps/bt/src apps/bt/tests
git commit -m "refactor(bt): move analytics common contracts to application"
```

---

### Task 3: Whole-Slice Verification and Review

**Files:**

- Verify only; no planned production changes

**Interfaces:**

- Consumes: Tasks 1 and 2
- Produces: fresh completion evidence and an independent whole-slice review

- [ ] **Step 1: Re-run the complete relevant suite from clean HEAD**

Run the Task 2 focused command again from the repository root. Confirm all tests pass and record the exact count and warnings.

- [ ] **Step 2: Re-run repository checks**

Run Ruff, Pyright, dependency direction, contract sync, both TypeScript typechecks, generated-file zero-diff, `python3 scripts/skills/refresh_skill_references.py --check`, `git diff --check`, baseline count, and `git status --short`.

Expected: all commands exit zero, baseline is 42, generated files are unchanged, and the worktree is clean.

- [ ] **Step 3: Request whole-slice review**

Review the design, this plan, Task 1 and Task 2 implementation reports, their independent reviews, verification evidence, and the full diff from the pre-slice commit. Verify exact three-name ownership closure, absence of compatibility surfaces, exact seven-row baseline reduction, OpenAPI identity, behavior preservation, and TDD evidence.

Fix and re-review every Critical or Important finding before completion.
