# Portfolio Performance Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move all portfolio-performance and watchlist-price response models out of the HTTP layer, preserve the public API exactly, and align the handwritten TypeScript facade with that API.

**Architecture:** Two focused modules under `src.application.contracts` become the only Python owners of the nine response models. Application services construct those models, FastAPI routes only expose them, and owner-independent OpenAPI normalization preserves the public `DateRange` component without depending on an internal module path. The obsolete HTTP schema module is deleted with no compatibility surface, and the existing handwritten TypeScript interfaces are corrected without changing web behavior.

**Tech Stack:** Python 3.12, Pydantic v2, FastAPI/OpenAPI 3.1, pytest, Ruff, Pyright, TypeScript, Bun, Vitest, openapi-typescript

## Global Constraints

- Delete `src.entrypoints.http.schemas.portfolio_performance`; do not add an alias, re-export, forwarding module, subclass, duplicate model, or conversion wrapper.
- Preserve all nine public component identities, field names, field order, required-field order, annotations, defaults, alias behavior, serialized payloads, route metadata, parameter defaults, and error mappings.
- Keep `DateRange.from_ = Field(alias="from")` and `model_config = {"populate_by_name": True}`.
- Keep portfolio `portfolioDescription`, `benchmark`, `benchmarkTimeSeries`, and `dateRange` optional-nullable, and `HoldingDetail.account` optional-nullable.
- Keep watchlist `prevClose` and `changePercent` optional-nullable in OpenAPI; represent them as optional-nullable in the handwritten TypeScript facade.
- Keep `GET /api/portfolio/{id}/performance` with `benchmarkCode="0000"` and `lookbackDays=252`, and keep `GET /api/watchlist/{id}/prices` unchanged.
- Derive the portfolio date-range schema from `PortfolioPerformanceResponse.properties.dateRange.anyOf[*].$ref`; do not add another module-path constant.
- Keep normalized OpenAPI and generated TypeScript at zero diff.
- Reduce `apps/bt/tests/unit/architecture/application_http_schema_imports.txt` from 35 to exactly 33 non-comment rows by removing only the two migrated service imports.
- Make no portfolio calculation, database, endpoint, web fetching, or UI behavior changes.

---

## File Structure

- `apps/bt/src/application/contracts/portfolio_performance.py`: canonical seven-model portfolio performance response family.
- `apps/bt/src/application/contracts/watchlist_prices.py`: canonical two-model watchlist latest-price response family.
- `apps/bt/tests/unit/application/contracts/test_portfolio_performance.py`: portfolio serialization, alias, required-order, and schema contract tests.
- `apps/bt/tests/unit/application/contracts/test_watchlist_prices.py`: watchlist serialization, optional fields, required-order, and schema contract tests.
- `apps/bt/src/application/services/portfolio_performance_service.py`: constructs canonical portfolio contracts.
- `apps/bt/src/application/services/watchlist_prices_service.py`: constructs canonical watchlist contracts.
- `apps/bt/src/entrypoints/http/routes/portfolio.py`: qualified route exposure of the canonical portfolio response model.
- `apps/bt/src/entrypoints/http/routes/watchlist.py`: qualified route exposure of the canonical watchlist response model.
- `apps/bt/src/entrypoints/http/openapi_config.py`: owner-independent portfolio `DateRange` component normalization.
- `apps/bt/tests/unit/server/test_openapi.py`: synthetic raw-schema and normalized public-schema regression coverage.
- `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`: forbidden HTTP ownership and portfolio-specific legacy-path checks.
- `apps/bt/tests/unit/architecture/test_layer_boundaries.py`: scanner behavior and exact baseline assertions.
- `apps/ts/packages/contracts/src/types/api-response-types.ts`: corrected handwritten portfolio/watchlist facade.
- `apps/ts/packages/contracts/src/types/api-response-types.test.ts`: required `analysisDate` and optional price-comparison fixtures.

---

### Task 1: Add the canonical application contract modules

**Files:**
- Create: `apps/bt/src/application/contracts/portfolio_performance.py`
- Create: `apps/bt/src/application/contracts/watchlist_prices.py`
- Create: `apps/bt/tests/unit/application/contracts/test_portfolio_performance.py`
- Create: `apps/bt/tests/unit/application/contracts/test_watchlist_prices.py`

**Interfaces:**
- Consumes: Pydantic `BaseModel` and `Field`; the exact public model shapes currently defined in `src.entrypoints.http.schemas.portfolio_performance`.
- Produces: `portfolio_performance.PerformanceSummary`, `HoldingDetail`, `TimeSeriesPoint`, `BenchmarkResult`, `BenchmarkTimeSeriesPoint`, `DateRange`, `PortfolioPerformanceResponse`; and `watchlist_prices.WatchlistStockPrice`, `WatchlistPricesResponse`.

- [ ] **Step 1: Write failing imports and behavior tests**

  Add tests that import both future application modules and instantiate complete responses. The portfolio fixture must include `analysisDate`, every required list, an `account=None` holding, optional-nullable response fields, and `DateRange` constructed once with `from` and once with `from_`. Assert `model_dump(by_alias=True)` contains `"from"` and never `"from_"`. The watchlist tests must cover both a complete price and a price omitting `prevClose` and `changePercent`.

  Encode these exact required orders from `model_json_schema()["required"]`:

  ```python
  assert PortfolioPerformanceResponse.model_json_schema()["required"] == [
      "portfolioId", "portfolioName", "summary", "holdings", "timeSeries",
      "analysisDate", "dataPoints", "warnings",
  ]
  assert WatchlistStockPrice.model_json_schema()["required"] == [
      "code", "close", "volume", "date",
  ]
  assert WatchlistPricesResponse.model_json_schema()["required"] == ["prices"]
  ```

  Also assert all model property orders listed in the approved design and verify that two independently-created response instances do not share list values.

- [ ] **Step 2: Run the new tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/application/contracts/test_portfolio_performance.py tests/unit/application/contracts/test_watchlist_prices.py -q
  ```

  Expected: collection fails with `ModuleNotFoundError` for the two new application contract modules.

- [ ] **Step 3: Add the two canonical modules with the exact model definitions**

  In `portfolio_performance.py`, define the seven models in dependency order and preserve this `DateRange` implementation exactly:

  ```python
  class DateRange(BaseModel):
      from_: str = Field(alias="from")
      to: str

      model_config = {"populate_by_name": True}
  ```

  Define `PortfolioPerformanceResponse` with the existing order and defaults:

  ```python
  class PortfolioPerformanceResponse(BaseModel):
      portfolioId: int
      portfolioName: str
      portfolioDescription: str | None = None
      summary: PerformanceSummary
      holdings: list[HoldingDetail]
      timeSeries: list[TimeSeriesPoint]
      benchmark: BenchmarkResult | None = None
      benchmarkTimeSeries: list[BenchmarkTimeSeriesPoint] | None = None
      analysisDate: str
      dateRange: DateRange | None = None
      dataPoints: int
      warnings: list[str]
  ```

  In `watchlist_prices.py`, define `WatchlistStockPrice` with `prevClose: float | None = None` and `changePercent: float | None = None`, followed by `WatchlistPricesResponse(prices: list[WatchlistStockPrice])`. Copy the remaining field types and ordering verbatim from the old module; do not import from or edit the old module yet.

- [ ] **Step 4: Prove legacy parity while both owners temporarily exist**

  Run a one-off comparison for each same-named old/new class and require exact equality of `model_json_schema()` and `__doc__`:

  ```bash
  cd apps/bt
  uv run python - <<'PY'
  from src.application.contracts import portfolio_performance as new_portfolio
  from src.application.contracts import watchlist_prices as new_watchlist
  from src.entrypoints.http.schemas import portfolio_performance as old

  for module, names in (
      (new_portfolio, ("PerformanceSummary", "HoldingDetail", "TimeSeriesPoint", "BenchmarkResult", "BenchmarkTimeSeriesPoint", "DateRange", "PortfolioPerformanceResponse")),
      (new_watchlist, ("WatchlistStockPrice", "WatchlistPricesResponse")),
  ):
      for name in names:
          before, after = getattr(old, name), getattr(module, name)
          assert before.model_json_schema() == after.model_json_schema(), name
          assert before.__doc__ == after.__doc__, name
  PY
  ```

  Expected: exit 0 with no output.

- [ ] **Step 5: Run focused quality gates**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/application/contracts/test_portfolio_performance.py tests/unit/application/contracts/test_watchlist_prices.py -q
  uv run ruff check src/application/contracts/portfolio_performance.py src/application/contracts/watchlist_prices.py tests/unit/application/contracts/test_portfolio_performance.py tests/unit/application/contracts/test_watchlist_prices.py
  uv run pyright src/application/contracts/portfolio_performance.py src/application/contracts/watchlist_prices.py
  ```

  Expected: all commands pass.

- [ ] **Step 6: Commit the canonical owner**

  ```bash
  git add apps/bt/src/application/contracts/portfolio_performance.py apps/bt/src/application/contracts/watchlist_prices.py apps/bt/tests/unit/application/contracts/test_portfolio_performance.py apps/bt/tests/unit/application/contracts/test_watchlist_prices.py
  git commit -m "feat(bt): add canonical portfolio performance contracts"
  ```

---

### Task 2: Cut backend ownership over atomically and preserve OpenAPI

**Files:**
- Modify: `apps/bt/src/application/services/portfolio_performance_service.py`
- Modify: `apps/bt/src/application/services/watchlist_prices_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/portfolio.py`
- Modify: `apps/bt/src/entrypoints/http/routes/watchlist.py`
- Modify: `apps/bt/src/entrypoints/http/openapi_config.py`
- Delete: `apps/bt/src/entrypoints/http/schemas/portfolio_performance.py`
- Modify: `apps/bt/tests/unit/server/test_openapi.py`
- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Test: `apps/bt/tests/unit/server/test_routes_portfolio_performance.py`
- Test: `apps/bt/tests/unit/server/test_watchlist_prices_service.py`
- Test: `apps/bt/tests/unit/server/test_routes_watchlist_prices.py`

**Interfaces:**
- Consumes: both canonical application modules from Task 1.
- Produces: unchanged endpoint payloads and normalized public OpenAPI, with no Python reference to `src.entrypoints.http.schemas.portfolio_performance` and an exact architecture baseline of 33.

- [ ] **Step 1: Add RED architecture scanner cases**

  Extend the forbidden unique-name set with these eight names:

  ```python
  PORTFOLIO_PERFORMANCE_HTTP_CONTRACT_NAMES = frozenset({
      "PerformanceSummary", "HoldingDetail", "TimeSeriesPoint",
      "BenchmarkResult", "BenchmarkTimeSeriesPoint",
      "PortfolioPerformanceResponse", "WatchlistStockPrice",
      "WatchlistPricesResponse",
  })
  ```

  Before changing the guard implementation, add parameterized synthetic tests proving that application imports from the old module and HTTP schema/route top-level bindings of each unique name are rejected. Add portfolio-specific cases proving that a replacement HTTP portfolio-performance module cannot define or directly bind `DateRange`, while `DateRange` in an unrelated HTTP schema remains allowed. Add a repository assertion that the old import-path text and file are absent.

- [ ] **Step 2: Run architecture tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/architecture/test_layer_boundaries.py -q
  ```

  Expected: new scanner cases fail because portfolio names, routes, legacy path, and portfolio-specific `DateRange` are not guarded yet.

- [ ] **Step 3: Add RED raw OpenAPI normalization tests**

  Import `_stabilize_date_range_refs` in `test_openapi.py`. Feed it a synthetic schema where `PortfolioPerformanceResponse.properties.dateRange.anyOf` references `src__application__contracts__portfolio_performance__DateRange`, whose properties are `from` and `to`. Assert after normalization that:

  ```python
  assert schemas["DateRange"] == candidate
  assert reference["$ref"] == "#/components/schemas/DateRange"
  assert "src__application__contracts__portfolio_performance__DateRange" not in schemas
  ```

  Include another synthetic module-qualified `DateRange` with different properties and assert it is not selected or removed. Extend the normalized `openapi_schema` fixture assertions to require the plain `DateRange` component and the portfolio response ref while rejecting application-qualified portfolio component keys.

- [ ] **Step 4: Run OpenAPI tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/server/test_openapi.py -q
  ```

  Expected: the synthetic owner-independent case fails because the current implementation only recognizes `_LEGACY_PORTFOLIO_PERF_DATE_RANGE`.

- [ ] **Step 5: Implement owner-independent portfolio DateRange discovery**

  Delete `_LEGACY_PORTFOLIO_PERF_DATE_RANGE`. In `_stabilize_date_range_refs`, read the portfolio response's `dateRange.anyOf`, parse component names only from refs beginning with `#/components/schemas/`, and select the referenced candidate only when `_is_from_to_date_range(candidate)` is true. Assign that candidate to `schemas["DateRange"]`, rewrite that exact ref to the plain component, and remove the selected qualified key unless it is already `DateRange`. Leave dataset, DB, factor-regression, and portfolio-factor-regression behavior unchanged.

  The selection must be driven exclusively by the response ref, not by scanning all `DateRange`-suffixed keys or hard-coding the new module path.

- [ ] **Step 6: Implement the complete service and route cutover**

  Use module-qualified imports everywhere:

  ```python
  from src.application.contracts import portfolio_performance as portfolio_performance_contracts
  from src.application.contracts import watchlist_prices as watchlist_prices_contracts
  ```

  Replace service construction and annotations with `portfolio_performance_contracts.<Model>` or `watchlist_prices_contracts.<Model>`. Replace route `response_model` and return annotations the same way. Do not leave direct class aliases in the route modules. Delete `src/entrypoints/http/schemas/portfolio_performance.py`.

- [ ] **Step 7: Complete the ownership guard and reduce the baseline**

  Apply the eight-name set to all files under `src/entrypoints/http`, not only `schemas`, so direct route bindings are rejected. Keep the existing migrated-name sets intact. Add a narrowly-scoped portfolio-module rule for `DateRange` and the deleted old module path; do not globally forbid `DateRange`. Remove exactly:

  ```text
  application/services/portfolio_performance_service.py|src.entrypoints.http.schemas.portfolio_performance
  application/services/watchlist_prices_service.py|src.entrypoints.http.schemas.portfolio_performance
  ```

  Assert the remaining non-comment baseline count is 33.

- [ ] **Step 8: Run the backend focused suite**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/application/contracts/test_portfolio_performance.py \
    tests/unit/application/contracts/test_watchlist_prices.py \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/test_openapi.py \
    tests/unit/server/test_routes_portfolio_performance.py \
    tests/unit/server/test_watchlist_prices_service.py \
    tests/unit/server/test_routes_watchlist_prices.py -q
  uv run ruff check src/application/contracts src/application/services/portfolio_performance_service.py src/application/services/watchlist_prices_service.py src/entrypoints/http/routes/portfolio.py src/entrypoints/http/routes/watchlist.py src/entrypoints/http/openapi_config.py tests/unit/application/contracts tests/unit/architecture tests/unit/server/test_openapi.py
  uv run pyright src
  ```

  Expected: all commands pass, and the architecture test reports no stale or added baseline rows.

- [ ] **Step 9: Verify the public contract produces no artifact diff**

  Run:

  ```bash
  git diff -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts > /tmp/portfolio-contract-before.diff
  cd apps/ts
  bun run --filter @trading25/contracts bt:sync
  cd ../..
  git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
  ```

  Expected: sync succeeds and the final `git diff --exit-code` exits 0.

- [ ] **Step 10: Commit the atomic backend migration**

  ```bash
  git add apps/bt/src apps/bt/tests/unit apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
  git commit -m "refactor(bt): move portfolio performance contracts to application"
  ```

---

### Task 3: Repair the handwritten TypeScript facade

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- Test: `apps/ts/packages/web/src/hooks/useWatchlist.test.tsx`

**Interfaces:**
- Consumes: unchanged backend OpenAPI from Task 2.
- Produces: `PortfolioPerformanceResponse.analysisDate: string`; `WatchlistStockPrice.prevClose?: number | null`; `WatchlistStockPrice.changePercent?: number | null`.

- [ ] **Step 1: Add TypeScript compile-time RED fixtures**

  Add `analysisDate: '2026-03-04'` to the existing typed `PortfolioPerformanceResponse` fixture and assert it. Add a second typed watchlist price that omits both comparison properties:

  ```typescript
  const priceWithoutComparison: WatchlistStockPrice = {
    code: '6758',
    close: 1000,
    volume: 100000,
    date: '2026-03-04',
  };
  ```

  Include it in `WatchlistPricesResponse.prices` and assert both omitted fields are `undefined`.

- [ ] **Step 2: Run contract typechecking and verify RED**

  Run:

  ```bash
  cd apps/ts
  bun run --filter @trading25/contracts typecheck
  ```

  Expected: TypeScript rejects the unknown `analysisDate` property and reports missing `prevClose`/`changePercent` on the omission fixture.

- [ ] **Step 3: Make the minimal interface corrections**

  Update only these declarations:

  ```typescript
  export interface PortfolioPerformanceResponse {
    portfolioId: number;
    portfolioName: string;
    portfolioDescription?: string | null;
    dateRange?: PortfolioPerformanceDateRange | null;
    dataPoints: number;
    summary: PortfolioPerformanceSummary;
    holdings: PortfolioHoldingPerformance[];
    timeSeries: PortfolioPerformancePoint[];
    benchmark?: PortfolioBenchmarkMetrics | null;
    benchmarkTimeSeries?: PortfolioBenchmarkPoint[] | null;
    analysisDate: string;
    warnings: string[];
  }

  export interface WatchlistStockPrice {
    code: string;
    close: number;
    prevClose?: number | null;
    changePercent?: number | null;
    volume: number;
    date: string;
  }
  ```

  Place `analysisDate` in the same semantic position as the backend response, after `benchmarkTimeSeries` and before `warnings` if preserving the handwritten grouping, without renaming any facade type.

- [ ] **Step 4: Run focused TypeScript tests and type gates**

  Run:

  ```bash
  cd apps/ts
  bun test packages/contracts/src/types/api-response-types.test.ts
  bun run --filter @trading25/contracts typecheck
  bun test packages/web/src/hooks/useWatchlist.test.tsx
  bun run quality:typecheck
  ```

  Expected: all commands pass.

- [ ] **Step 5: Reconfirm generated artifacts are unchanged**

  Run:

  ```bash
  git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
  ```

  Expected: exit 0.

- [ ] **Step 6: Commit the TypeScript repair**

  ```bash
  git add apps/ts/packages/contracts/src/types/api-response-types.ts apps/ts/packages/contracts/src/types/api-response-types.test.ts
  git commit -m "fix(ts): align portfolio performance response types"
  ```

---

### Task 4: Run integrated maintenance gates and independent review

**Files:**
- Verify: all files changed by Tasks 1-3
- Modify only if a gate exposes a defect: the smallest already-in-scope source or test file responsible for that defect

**Interfaces:**
- Consumes: the complete Python and TypeScript migration.
- Produces: a clean, independently-reviewed branch state with baseline 33 and no generated-contract drift.

- [ ] **Step 1: Run the complete scoped backend suite**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/application/contracts \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/test_openapi.py \
    tests/unit/server/test_routes_portfolio_performance.py \
    tests/unit/server/test_watchlist_prices_service.py \
    tests/unit/server/test_routes_watchlist_prices.py \
    tests/unit/scripts/test_check_contract_sync.py \
    tests/unit/scripts/test_audit_skills.py -q
  uv run ruff check src tests/unit/application/contracts tests/unit/architecture tests/unit/server/test_openapi.py
  uv run pyright src
  ```

  Expected: every command passes.

- [ ] **Step 2: Run repository dependency and reference gates**

  Run:

  ```bash
  python scripts/skills/audit_skills.py --strict-legacy
  ./scripts/check-contract-sync.sh
  test "$(grep -cvE '^[[:space:]]*(#|$)' apps/bt/tests/unit/architecture/application_http_schema_imports.txt)" -eq 33
  ! rg -n "src\.entrypoints\.http\.schemas\.portfolio_performance|entrypoints/http/schemas/portfolio_performance" apps/bt/src apps/bt/tests
  ```

  Expected: both scripts pass, the baseline assertion exits 0, and `rg` finds no legacy path.

- [ ] **Step 3: Run final TypeScript gates**

  Run:

  ```bash
  cd apps/ts
  bun test packages/contracts/src/types/api-response-types.test.ts packages/web/src/hooks/useWatchlist.test.tsx
  bun run quality:typecheck
  bun run quality:deps:audit
  cd ../..
  git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
  ```

  Expected: all commands pass and generated artifacts remain unchanged.

- [ ] **Step 4: Check patch integrity**

  Run:

  ```bash
  git diff --check
  git status --short
  ```

  Expected before final review: `git diff --check` exits 0; status contains only intentional in-scope review fixes, or is clean when each task commit is complete.

- [ ] **Step 5: Dispatch independent final review**

  Give a fresh reviewer the approved design, this plan, `git diff 80a29b74..HEAD`, and the exact gate outputs. Require explicit answers for: spec compliance, single canonical ownership, absence of compatibility surfaces, OpenAPI stability, TypeScript facade accuracy, test adequacy, and merge readiness. Any finding must cite a concrete file and line and be fixed with a new RED/GREEN cycle before repeating the affected gates.

- [ ] **Step 6: Commit only review-driven corrections**

  If review required changes, commit the minimal correction separately:

  ```bash
  git add \
    apps/bt/src/application/contracts/portfolio_performance.py \
    apps/bt/src/application/contracts/watchlist_prices.py \
    apps/bt/src/application/services/portfolio_performance_service.py \
    apps/bt/src/application/services/watchlist_prices_service.py \
    apps/bt/src/entrypoints/http/routes/portfolio.py \
    apps/bt/src/entrypoints/http/routes/watchlist.py \
    apps/bt/src/entrypoints/http/openapi_config.py \
    apps/bt/tests/unit/application/contracts \
    apps/bt/tests/unit/architecture/application_contract_boundary_guard.py \
    apps/bt/tests/unit/architecture/application_http_schema_imports.txt \
    apps/bt/tests/unit/architecture/test_layer_boundaries.py \
    apps/bt/tests/unit/server/test_openapi.py \
    apps/ts/packages/contracts/src/types/api-response-types.ts \
    apps/ts/packages/contracts/src/types/api-response-types.test.ts
  git commit -m "fix: address portfolio contract boundary review"
  ```

  If no changes were required, do not create an empty commit.
