# Screening Family Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move application-owned screening results and job payloads out of HTTP schemas, eliminate duplicate screening aliases, and reduce the application-to-HTTP dependency baseline from 42 to 37.

**Architecture:** Existing domain aliases become the sole source of screening semantic values. Six Pydantic models move to `src.application.contracts.screening`; routes use those contracts directly, while the HTTP layer retains only the `BaseJobResponse`-derived `ScreeningJobResponse` transport wrapper.

**Tech Stack:** Python 3.12, Pydantic v2, FastAPI/OpenAPI, pytest, Ruff, Pyright, Bun/openapi-typescript

## Global Constraints

- Preserve every class name, docstring, field name, annotation, default, default factory, validation constraint, serialized payload, and OpenAPI component shape.
- Preserve the committed OpenAPI enum order for `ScreeningSortBy`.
- Keep `ScreeningJobPayload.response` as `dict[str, Any]`.
- Do not add compatibility aliases, HTTP re-exports, forwarding modules, duplicate models, subclasses, or conversion wrappers.
- Delete `entrypoints/http/schemas/screening.py` and the unused `ScreeningDataSource` alias.
- Keep only `ScreeningJobResponse` in `entrypoints/http/schemas/screening_job.py`.
- HTTP schema modules must use qualified application/domain references and must not bind canonical names.
- Delete exactly five stale dependency-baseline rows; the final count is exactly 37.
- Regenerated OpenAPI and TypeScript contract files must have zero diff.
- Do not change screening calculations, persistence shape, endpoint behavior, or job lifecycle.

---

### Task 1: Add Canonical Screening Contracts and Alias Order

**Files:**

- Create: `apps/bt/src/application/contracts/screening.py`
- Create: `apps/bt/tests/unit/application/contracts/test_screening.py`
- Modify: `apps/bt/src/domains/analytics/screening_results.py`

**Interfaces:**

- Produces: six application screening contracts
- Consumes: analytics application contracts and four domain semantic aliases
- Preserves: current HTTP model schemas and enum ordering

- [ ] **Step 1: Add failing canonical-contract tests**

Create tests that import all six models from the missing application module and
the four aliases from their domain modules. Build a complete
`MarketScreeningResponse` and assert its full JSON payload. Build minimal result,
summary, diagnostics, and request values and assert:

- fresh list/dict defaults are independent;
- `ScreeningJobRequest` defaults are unchanged;
- unknown fields are forbidden;
- `recentDays` rejects 0 and 91;
- `date` rejects non-`YYYY-MM-DD` values;
- `limit` rejects 0;
- all accepted/rejected alias values are unchanged;
- `get_args(ScreeningSortBy)` equals
  `("bestStrategyScore", "matchedDate", "stockCode", "matchStrategyCount")`;
- schema titles, required fields, property sets, and field constraints match the
  current HTTP-owned models.

- [ ] **Step 2: Run RED**

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_screening.py
```

Expected: collection fails because `src.application.contracts.screening` does
not exist.

- [ ] **Step 3: Reorder the domain sort alias**

Set `ScreeningSortBy` in `domains/analytics/screening_results.py` to:

```python
ScreeningSortBy = Literal[
    "bestStrategyScore",
    "matchedDate",
    "stockCode",
    "matchStrategyCount",
]
```

Do not change sorting logic.

- [ ] **Step 4: Add the canonical Pydantic graph**

Create `application/contracts/screening.py`. Copy the four models from the old
screening schema and the two job models from the old screening-job schema
without semantic changes. Import:

```python
from src.application.contracts import analytics as analytics_contracts
from src.domains.analytics.screening_results import ScreeningSortBy, SortOrder
from src.domains.strategy.runtime.screening_profile import EntryDecidability
```

The exact job models are:

```python
class MatchedStrategyItem(BaseModel):
    """同一銘柄でヒットした戦略情報"""

    strategyName: str
    matchedDate: str
    strategyScore: float | None = None


class ScreeningResultItem(BaseModel):
    """銘柄集約済みスクリーニング結果項目"""

    stockCode: str
    companyName: str
    scaleCategory: str | None = None
    sector33Name: str | None = None
    matchedDate: str
    bestStrategyName: str
    bestStrategyScore: float | None = None
    matchStrategyCount: int
    matchedStrategies: list[MatchedStrategyItem] = Field(default_factory=list)


class ScreeningSummary(BaseModel):
    """スクリーニングサマリー"""

    totalStocksScreened: int
    matchCount: int
    skippedCount: int = 0
    byStrategy: dict[str, int] = Field(default_factory=dict)
    strategiesEvaluated: list[str] = Field(default_factory=list)
    strategiesWithoutBacktestMetrics: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class MarketScreeningResponse(BaseModel):
    """マーケットスクリーニングレスポンス"""

    results: list[ScreeningResultItem]
    summary: ScreeningSummary
    entry_decidability: EntryDecidability = Field(default="pre_open_decidable")
    markets: list[str]
    scopeLabel: str | None = None
    recentDays: int
    referenceDate: str | None = None
    sortBy: ScreeningSortBy
    order: SortOrder
    lastUpdated: str
    provenance: analytics_contracts.DataProvenance
    diagnostics: analytics_contracts.ResponseDiagnostics = Field(
        default_factory=analytics_contracts.ResponseDiagnostics
    )


class ScreeningJobRequest(BaseModel):
    """Screening ジョブ作成リクエスト"""

    entry_decidability: EntryDecidability = Field(default="pre_open_decidable")
    markets: str | None = Field(default=None)
    strategies: str | None = Field(default=None)
    recentDays: int = Field(default=10, ge=1, le=90)
    date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    sortBy: ScreeningSortBy = Field(default="matchedDate")
    order: SortOrder = Field(default="desc")
    limit: int | None = Field(default=None, ge=1)

    model_config = {"extra": "forbid"}


class ScreeningJobPayload(BaseModel):
    """JobInfo.raw_result へ保持する payload"""

    response: dict[str, Any]
```

- [ ] **Step 5: Run GREEN and parity checks**

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_screening.py \
  tests/unit/domains/analytics/test_screening_results.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check \
  src/application/contracts/screening.py \
  src/domains/analytics/screening_results.py \
  tests/unit/application/contracts/test_screening.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright \
  src/application/contracts/screening.py \
  src/domains/analytics/screening_results.py
```

Before Task 2 deletes old definitions, compare `model_json_schema()` and
docstrings for all six Pydantic models and confirm exact equality. Compare the
four alias value sets and the committed OpenAPI enum order.

- [ ] **Step 6: Commit Task 1**

```bash
git add apps/bt/src/application/contracts/screening.py \
  apps/bt/src/domains/analytics/screening_results.py \
  apps/bt/tests/unit/application/contracts/test_screening.py
git commit -m "feat(bt): add canonical screening contracts"
```

---

### Task 2: Remove HTTP Screening Ownership and Migrate Consumers

**Files:**

- Delete: `apps/bt/src/entrypoints/http/schemas/screening.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/screening_job.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/strategy.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Modify: `apps/bt/src/entrypoints/http/routes/strategies.py`
- Modify: five application screening service modules from the baseline
- Modify: screening-related tests that import deleted HTTP names
- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`

**Interfaces:**

- Consumes: Task 1 contracts and domain aliases
- Produces: application/domain-only canonical ownership
- Preserves: HTTP `ScreeningJobResponse`, route signatures, request restoration, and raw-result recovery

- [ ] **Step 1: Extend ownership guards before production migration**

Add the six model names and four alias names to the forbidden HTTP ownership
set. Add synthetic direct-import, class/assignment, and `__all__` cases for the
screening contract and alias families.

- [ ] **Step 2: Run architecture RED**

```bash
./scripts/bt-pytest.sh tests/unit/architecture/test_layer_boundaries.py
```

Expected: repository-scan failures identify old screening definitions, five
application imports, HTTP schema/route bindings, and test imports.

- [ ] **Step 3: Migrate application services**

The five baseline modules import Pydantic values from
`src.application.contracts.screening` and semantic aliases from the existing
domain modules. Preserve method signatures and construction behavior.

- [ ] **Step 4: Migrate HTTP routes and schemas**

Use module-qualified imports:

```python
from src.application.contracts import screening as screening_contracts
from src.domains.analytics import screening_results
from src.domains.strategy.runtime import screening_profile
```

`analytics_complex.py` uses application contracts for request parsing,
response models, request restoration, payload validation, and result recovery.
`strategy.py` and `routes/strategies.py` use qualified `screening_profile`
aliases. `screening_job.py` retains only `ScreeningJobResponse` and uses
qualified domain aliases for its fields.

- [ ] **Step 5: Delete old ownership and migrate tests**

Delete `schemas/screening.py`. Remove `ScreeningJobRequest` and
`ScreeningJobPayload` from `screening_job.py`. Update every source and test
import of deleted names to the canonical application/domain module. Do not
create a replacement HTTP module.

- [ ] **Step 6: Shrink the baseline**

Delete exactly the five screening/screening-job rows named in the design and
confirm exactly 37 non-comment entries remain.

- [ ] **Step 7: Run focused behavior tests**

Run:

```bash
./scripts/bt-pytest.sh \
  tests/unit/application/contracts/test_screening.py \
  tests/unit/architecture/test_layer_boundaries.py \
  tests/unit/domains/analytics/test_screening_results.py \
  tests/unit/server/services/test_screening_job_service.py \
  tests/unit/server/services/test_screening_service.py \
  tests/unit/server/services/test_screening_service_helpers.py \
  tests/unit/server/services/test_screening_default_markets.py \
  tests/unit/server/services/test_screening_strategy_selection.py \
  tests/unit/server/routes/test_analytics_complex.py \
  tests/unit/server/routes/test_strategies.py \
  tests/unit/server/test_openapi.py
```

If a listed test path has a different repository name, locate the existing
test for that exact service/route with `rg --files apps/bt/tests` and use it;
do not omit coverage for that component.

- [ ] **Step 8: Run static and contract verification**

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
```

From `apps/ts`:

```bash
bun run --filter @trading25/contracts typecheck
bun run --filter @trading25/api-clients typecheck
```

Then verify generated files, baseline, and whitespace:

```bash
git diff --exit-code -- \
  apps/ts/packages/contracts/openapi/bt-openapi.json \
  apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
git diff --check
```

- [ ] **Step 9: Commit Task 2**

```bash
git add -A apps/bt/src apps/bt/tests
git commit -m "refactor(bt): move screening contracts to application"
```

---

### Task 3: Whole-Slice Verification and Review

**Files:**

- Verify only; no planned production changes

**Interfaces:**

- Consumes: Tasks 1 and 2
- Produces: completion evidence and independent whole-slice review

- [ ] **Step 1: Re-run complete screening suite**

Run the resolved Task 2 focused test list from clean HEAD and record exact test
count and warnings.

- [ ] **Step 2: Re-run repository and contract checks**

Run Ruff, Pyright, dependency direction, parallel-safe contract sync, both
TypeScript typechecks, generated-file zero diff, skill-reference check,
baseline count 37, `git diff --check`, and `git status --short`.

- [ ] **Step 3: Request whole-slice review**

Review the design, plan, TDD reports, independent task reviews, complete diff,
six-model ownership closure, four domain alias closure, deleted HTTP module,
five-row baseline reduction, job recovery semantics, OpenAPI identity, and
test evidence. Fix and re-review every Critical or Important finding.
