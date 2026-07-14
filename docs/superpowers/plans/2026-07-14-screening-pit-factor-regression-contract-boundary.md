# Screening PIT and Factor Regression Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every current-table fallback from Screening universe resolution, make canonical application contracts trigger contract CI, and converge stock/portfolio Factor Regression responses on application-owned Python models and generated TypeScript aliases.

**Architecture:** Complete the fail-closed Screening behavior as an independently reviewed correctness slice. Then add the CI guardrail, create two canonical Factor Regression contract modules, atomically delete both HTTP schema owners, and move TypeScript consumers to distinct generated stock/portfolio shapes. Preserve all successful regression calculations, endpoints, payloads, stabilized OpenAPI components, and frontend presentation.

**Tech Stack:** Python 3.12, DuckDB, Pydantic v2, FastAPI/OpenAPI 3.1, pytest, Ruff, Pyright, TypeScript, Bun, Vitest, openapi-typescript

## Global Constraints

- Screening must never query `stocks` for universe membership or market classification.
- Screening effective reference date is exactly explicit `reference_date` or latest `stock_data` date; remove latest-stock-master and `9999-12-31` fallbacks.
- Non-empty Screening market codes require an exact `stock_master_daily` snapshot and fail with an actionable error when absent.
- Preserve the existing asynchronous Screening job API and error persistence path; do not add a synchronous or frontend fallback.
- Add `apps/bt/src/application/contracts/` to contract CI classification without changing governance-only or docs-only behavior.
- Keep stock and portfolio Factor Regression match shapes distinct; never coerce one into the other.
- Delete both old Factor Regression HTTP schema modules; do not add aliases, re-exports, forwarding modules, subclasses, duplicate response models, or compatibility wrappers.
- Preserve Factor Regression calculations, database queries, endpoint paths, query parameters, status mapping, response JSON, and stabilized OpenAPI document.
- Use module-qualified Python imports from `src.application.contracts`.
- TypeScript response and nested wire types must be generated aliases; request parameter types remain locally authored.
- Delete handwritten `Api*` and analytics Factor Regression response definitions instead of retaining deprecated aliases.
- Do not add financial calculations or business validation to TypeScript.
- Reduce the exact application-to-HTTP-schema baseline from 21 to 19.

---

## File Structure

- `apps/bt/src/application/services/screening_universe.py`: strict exact-date `stock_master_daily` universe loader.
- `apps/bt/src/application/services/screening_service.py`: market-date-only effective reference selection and strict loader wiring.
- `apps/bt/tests/unit/server/services/test_screening_service.py`: real DuckDB PIT/fail-closed regression coverage.
- `apps/bt/tests/unit/server/services/test_screening_service_helpers.py`: strict helper signature fixtures.
- `scripts/ci/test_taxonomy.py`: canonical application contracts classified as contracts.
- `apps/bt/src/application/contracts/factor_regression.py`: stock response graph.
- `apps/bt/src/application/contracts/portfolio_factor_regression.py`: portfolio response graph.
- `apps/bt/tests/unit/application/contracts/test_factor_regression.py`: exact Python contract parity and distinct-shape tests.
- `apps/bt/src/application/services/factor_regression_service.py`: stock service consumer.
- `apps/bt/src/application/services/portfolio_factor_regression_service.py`: portfolio service consumer.
- `apps/bt/src/entrypoints/http/routes/analytics_complex.py`: FastAPI wiring to canonical response models.
- `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`: exact baseline 19.
- `apps/ts/packages/contracts/src/types/api-response-types.ts`: stable generated Factor Regression aliases.
- `apps/ts/packages/api-clients/src/analytics/types.ts`: response re-exports plus local request types.
- `apps/ts/packages/api-clients/type-tests/factor-regression-contracts.ts`: stock/portfolio match-shape compile guard.
- `apps/ts/packages/web/src/hooks/useFactorRegression.ts`: canonical response import.
- `apps/ts/packages/web/src/components/Chart/FactorRegressionPanel.tsx`: canonical match import.

---

### Task 1: Make Screening universe resolution PIT fail-closed

**Files:**
- Modify: `apps/bt/src/application/services/screening_universe.py:16-61`
- Modify: `apps/bt/src/application/services/screening_service.py:175-182,302-311`
- Modify: `apps/bt/tests/unit/server/services/test_screening_service.py:81-127,430-500`
- Modify: `apps/bt/tests/unit/server/services/test_screening_service_helpers.py:194-225`

**Interfaces:**
- Produces: `load_stock_universe(reader, market_codes, *, as_of_date: str, stock_master_daily_has_date: Callable[[str], bool]) -> list[StockUniverseItem]`.
- Produces: `ScreeningService._load_stock_universe(market_codes: list[str], as_of_date: str) -> list[StockUniverseItem]`.
- Preserves: empty market codes return `[]`; exact-date results normalize/deduplicate stock codes.

- [ ] **Step 1: Add RED tests for missing snapshots and reference dates**

  Add real-DuckDB tests that place a current-only row in `stocks`, omit the requested date from `stock_master_daily`, and assert:

  ```python
  with pytest.raises(
      ValueError,
      match=(
          r"stock_master_daily snapshot is unavailable for screening "
          r"reference date 2024-01-15; run market DB sync before screening"
      ),
  ):
      service._load_stock_universe(["0111"], "2024-01-15")
  ```

  Add a missing-table test with the same expected error. Add a service test that stubs both explicit date and latest `stock_data` date as absent and asserts:

  ```python
  with pytest.raises(ValueError, match="No market date available for screening"):
      service.run_screening(reference_date=None)
  ```

  Update the deduplication helper test to pass `"2024-01-15"` and make `_stock_master_daily_has_date` return `True`; do not preserve the optional-date call.

- [ ] **Step 2: Run the focused tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/server/services/test_screening_service.py \
    tests/unit/server/services/test_screening_service_helpers.py -q
  ```

  Expected: the new missing-date test returns the row from `stocks`, and the missing-market-date test does not raise the required error.

- [ ] **Step 3: Implement strict exact-date loading**

  Replace the loader branch with:

  ```python
  def load_stock_universe(
      reader: MarketDbReadable,
      market_codes: list[str],
      *,
      as_of_date: str,
      stock_master_daily_has_date: Callable[[str], bool],
  ) -> list[StockUniverseItem]:
      if not market_codes:
          return []
      if not stock_master_daily_has_date(as_of_date):
          raise ValueError(
              "stock_master_daily snapshot is unavailable for screening "
              f"reference date {as_of_date}; run market DB sync before screening"
          )

      placeholders = ",".join("?" for _ in market_codes)
      rows = reader.query(
          f"""
          SELECT code, company_name, scale_category, sector_33_name
          FROM stock_master_daily
          WHERE date = ? AND market_code IN ({placeholders})
          ORDER BY code
          """,
          (as_of_date, *market_codes),
      )
  ```

  Keep the existing normalization/deduplication loop. Remove `get_latest_stock_master_date` and `get_latest_market_date` parameters and every `stocks` query branch.

  In `run_screening`, use:

  ```python
  effective_reference_date = reference_date or self._get_latest_market_date()
  if effective_reference_date is None:
      raise ValueError("No market date available for screening")
  ```

  Make `_load_stock_universe` require `as_of_date: str` and pass only `stock_master_daily_has_date` to the helper.

- [ ] **Step 4: Verify GREEN and scan forbidden fallbacks**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/server/services/test_screening_service.py \
    tests/unit/server/services/test_screening_service_helpers.py \
    tests/unit/server/services/test_screening_job_service.py -q
  uv run ruff check \
    src/application/services/screening_universe.py \
    src/application/services/screening_service.py \
    tests/unit/server/services/test_screening_service.py \
    tests/unit/server/services/test_screening_service_helpers.py
  uv run pyright \
    src/application/services/screening_universe.py \
    src/application/services/screening_service.py
  rg -n 'source_table = "stocks"|9999-12-31|Legacy/unit-test DBs' \
    src/application/services/screening_universe.py \
    src/application/services/screening_service.py
  ```

  Expected: tests/lint/types pass; `rg` exits 1 with no matches.

- [ ] **Step 5: Commit the PIT fix**

  ```bash
  git add \
    apps/bt/src/application/services/screening_universe.py \
    apps/bt/src/application/services/screening_service.py \
    apps/bt/tests/unit/server/services/test_screening_service.py \
    apps/bt/tests/unit/server/services/test_screening_service_helpers.py
  git commit -m "fix(bt): fail closed on missing screening universe snapshots"
  ```

---

### Task 2: Make canonical application contracts trigger contract CI

**Files:**
- Modify: `scripts/ci/test_taxonomy.py:45-55`
- Modify: `apps/bt/tests/unit/scripts/test_test_taxonomy.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`

**Interfaces:**
- Produces: `is_contract_path("apps/bt/src/application/contracts/factor_regression.py") is True`.
- Produces: application-contract-only changes select `product_ci=true`, `contracts_ci=true`, and `docs_only=false`.

- [ ] **Step 1: Add RED taxonomy and changed-scope tests**

  Add:

  ```python
  def test_application_contract_is_product_and_contract_path() -> None:
      module = _load_module()
      path = "apps/bt/src/application/contracts/factor_regression.py"

      assert module.is_product_path(path)
      assert module.is_contract_path(path)
  ```

  And:

  ```python
  def test_application_contract_change_runs_contract_and_product_ci() -> None:
      module = _load_module()
      scope = module.classify_changed_paths(
          ["apps/bt/src/application/contracts/factor_regression.py"]
      )

      assert scope.product_ci is True
      assert scope.contracts_ci is True
      assert scope.research_ci is False
      assert scope.security_ci is False
      assert scope.docs_only is False
  ```

- [ ] **Step 2: Run focused tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/scripts/test_test_taxonomy.py \
    tests/unit/scripts/test_ci_changed_scope.py -q
  ```

  Expected: both new assertions fail because the path is not in `CONTRACT_PREFIXES`.

- [ ] **Step 3: Add the canonical contract prefix**

  Add exactly this entry to `CONTRACT_PREFIXES`:

  ```python
  "apps/bt/src/application/contracts/",
  ```

  Do not add individual contract files or change the product/research/governance classifiers.

- [ ] **Step 4: Verify GREEN and real CLI output**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/scripts/test_test_taxonomy.py \
    tests/unit/scripts/test_ci_changed_scope.py -q
  cd ../..
  printf '%s\n' 'apps/bt/src/application/contracts/factor_regression.py' \
    | python3 scripts/ci/changed-scope.py
  ```

  Expected output contains `product_ci=true`, `contracts_ci=true`, and `docs_only=false`.

- [ ] **Step 5: Commit the CI guardrail**

  ```bash
  git add \
    scripts/ci/test_taxonomy.py \
    apps/bt/tests/unit/scripts/test_test_taxonomy.py \
    apps/bt/tests/unit/scripts/test_ci_changed_scope.py
  git commit -m "ci: classify application contracts as API contracts"
  ```

---

### Task 3: Add canonical stock and portfolio Factor Regression contracts

**Files:**
- Create: `apps/bt/src/application/contracts/factor_regression.py`
- Create: `apps/bt/src/application/contracts/portfolio_factor_regression.py`
- Create: `apps/bt/tests/unit/application/contracts/test_factor_regression.py`

**Interfaces:**
- Produces stock module: `DateRange`, `IndexMatch`, `FactorRegressionResponse`.
- Produces portfolio module: `StockWeight`, `ExcludedStock`, `IndexMatch`, `DateRange`, `PortfolioFactorRegressionResponse`.
- Preserves aliases: both `DateRange` models accept `from` and `from_` and serialize `from` with `by_alias=True`.

- [ ] **Step 1: Add RED import and distinct-shape tests**

  Import modules as:

  ```python
  from src.application.contracts import factor_regression as factor_contracts
  from src.application.contracts import portfolio_factor_regression as portfolio_contracts
  ```

  Add exact property assertions:

  ```python
  assert list(factor_contracts.IndexMatch.model_fields) == [
      "indexCode", "indexName", "category", "rSquared", "beta"
  ]
  assert list(portfolio_contracts.IndexMatch.model_fields) == [
      "code", "name", "rSquared"
  ]
  ```

  Instantiate complete stock and portfolio response graphs, assert literal
  `model_dump(by_alias=True)` dictionaries, property order, required order,
  docstrings, date alias behavior, and mutable `StockWeight.weight` behavior.

- [ ] **Step 2: Run the new test and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/application/contracts/test_factor_regression.py -q
  ```

  Expected: import failure because both canonical modules are absent.

- [ ] **Step 3: Move exact model definitions into two canonical modules**

  Create `factor_regression.py` with the exact three current definitions and
  `portfolio_factor_regression.py` with the exact five current definitions.
  Preserve class names, field order, annotations, aliases, configs, and
  docstrings. Neither module may import from `entrypoints`.

- [ ] **Step 4: Prove old/new parity before deletion**

  Run:

  ```bash
  cd apps/bt
  uv run python - <<'PY'
  from src.application.contracts import factor_regression as new_stock
  from src.application.contracts import portfolio_factor_regression as new_portfolio
  from src.entrypoints.http.schemas import factor_regression as old_stock
  from src.entrypoints.http.schemas import portfolio_factor_regression as old_portfolio

  for old, new, names in (
      (old_stock, new_stock, ("DateRange", "IndexMatch", "FactorRegressionResponse")),
      (
          old_portfolio,
          new_portfolio,
          ("StockWeight", "ExcludedStock", "IndexMatch", "DateRange", "PortfolioFactorRegressionResponse"),
      ),
  ):
      for name in names:
          assert getattr(old, name).model_json_schema() == getattr(new, name).model_json_schema(), name
          assert getattr(old, name).__doc__ == getattr(new, name).__doc__, name
  PY
  ```

  Expected: exit 0.

- [ ] **Step 5: Run focused quality gates and commit**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/application/contracts/test_factor_regression.py -q
  uv run ruff check \
    src/application/contracts/factor_regression.py \
    src/application/contracts/portfolio_factor_regression.py \
    tests/unit/application/contracts/test_factor_regression.py
  uv run pyright \
    src/application/contracts/factor_regression.py \
    src/application/contracts/portfolio_factor_regression.py
  ```

  Then commit:

  ```bash
  git add \
    apps/bt/src/application/contracts/factor_regression.py \
    apps/bt/src/application/contracts/portfolio_factor_regression.py \
    apps/bt/tests/unit/application/contracts/test_factor_regression.py
  git commit -m "feat(bt): add canonical factor regression contracts"
  ```

---

### Task 4: Atomically cut over the Factor Regression backend

**Files:**
- Modify: `apps/bt/src/application/services/factor_regression_service.py`
- Modify: `apps/bt/src/application/services/portfolio_factor_regression_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Delete: `apps/bt/src/entrypoints/http/schemas/factor_regression.py`
- Delete: `apps/bt/src/entrypoints/http/schemas/portfolio_factor_regression.py`
- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/server/test_openapi.py`
- Test: `apps/bt/tests/unit/server/services/test_factor_regression_service.py`
- Test: `apps/bt/tests/unit/server/routes/test_analytics_complex.py`
- Test: `apps/bt/tests/unit/server/test_routes_portfolio_factor_regression.py`

**Interfaces:**
- Consumes: both canonical modules from Task 3.
- Produces: unchanged stock and portfolio endpoints using canonical response models.
- Produces: exact architecture baseline 19 and no old schema module/path.

- [ ] **Step 1: Add RED ownership and OpenAPI parity tests**

  Extend the architecture guard with both deleted module paths and all eight
  migrated model names. Reject recreation, direct binding, top-level re-export,
  and canonical wildcard imports from either old path. Add assertions:

  ```python
  assert not (HTTP_SCHEMA_ROOT / "factor_regression.py").exists()
  assert not (HTTP_SCHEMA_ROOT / "portfolio_factor_regression.py").exists()
  assert len(_application_http_schema_baseline()) == 19
  ```

  Add OpenAPI assertions that `FactorRegressionResponse` and
  `PortfolioFactorRegressionResponse` keep their complete expected properties,
  and that portfolio `IndexMatch` is `{code,name,rSquared}` while the stabilized
  stock match component is `{indexCode,indexName,category,rSquared,beta}`.

- [ ] **Step 2: Run architecture/OpenAPI tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/test_openapi.py -q
  ```

  Expected: deleted-file and baseline assertions fail.

- [ ] **Step 3: Switch services and routes using module-qualified imports**

  Use:

  ```python
  from src.application.contracts import factor_regression as factor_contracts
  from src.application.contracts import (
      portfolio_factor_regression as portfolio_factor_contracts,
  )
  ```

  Change every annotation and constructor in each service to its module-qualified
  model. Change route `response_model` and return annotations the same way.
  Delete both old HTTP schema files. Remove exactly these baseline rows:

  ```text
  application/services/factor_regression_service.py|src.entrypoints.http.schemas.factor_regression
  application/services/portfolio_factor_regression_service.py|src.entrypoints.http.schemas.portfolio_factor_regression
  ```

- [ ] **Step 4: Compare normalized OpenAPI before contract sync**

  Export current source OpenAPI to a temporary file and compare it with the
  committed snapshot using the repository canonical JSON normalization. The
  complete documents must be equal; if module-path collision stabilization
  changes keys, update `openapi_config.py` only to preserve the existing
  committed component names, never to introduce new names.

  Run:

  ```bash
  ./scripts/check-contract-sync.sh
  ```

  Expected: `[contract] PASS` and no generated diff.

- [ ] **Step 5: Run the backend slice and compatibility scans**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/application/contracts/test_factor_regression.py \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/test_openapi.py \
    tests/unit/server/services/test_factor_regression_service.py \
    tests/unit/server/routes/test_analytics_complex.py \
    tests/unit/server/test_routes_portfolio_factor_regression.py -q
  uv run ruff check \
    src/application/contracts \
    src/application/services/factor_regression_service.py \
    src/application/services/portfolio_factor_regression_service.py \
    src/entrypoints/http/routes/analytics_complex.py \
    tests/unit/application/contracts/test_factor_regression.py \
    tests/unit/architecture
  uv run pyright src
  rg -n \
    'src\.entrypoints\.http\.schemas\.(factor_regression|portfolio_factor_regression)' \
    src
  ```

  Expected: tests/lint/types pass; `rg` exits 1.

- [ ] **Step 6: Commit the atomic backend cutover**

  ```bash
  git add \
    apps/bt/src/application/services/factor_regression_service.py \
    apps/bt/src/application/services/portfolio_factor_regression_service.py \
    apps/bt/src/entrypoints/http/routes/analytics_complex.py \
    apps/bt/src/entrypoints/http/schemas/factor_regression.py \
    apps/bt/src/entrypoints/http/schemas/portfolio_factor_regression.py \
    apps/bt/tests/unit/architecture \
    apps/bt/tests/unit/server/test_openapi.py
  git commit -m "refactor(bt): move factor regression contracts to application"
  ```

---

### Task 5: Converge TypeScript Factor Regression responses on generated aliases

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/index.ts`
- Create: `apps/ts/packages/api-clients/type-tests/factor-regression-contracts.ts`
- Modify: `apps/ts/packages/api-clients/tsconfig.type-tests.json`
- Modify: `apps/ts/packages/web/src/hooks/useFactorRegression.ts`
- Modify: `apps/ts/packages/web/src/components/Chart/FactorRegressionPanel.tsx`
- Test: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`
- Test: focused Factor Regression web tests discovered with `rg --files apps/ts/packages/web/src | rg 'FactorRegression.*test'`

**Interfaces:**
- Produces stable contracts aliases: `FactorRegressionDateRange`, `FactorRegressionIndexMatch`, `FactorRegressionResponse`, `PortfolioFactorRegressionDateRange`, `PortfolioFactorRegressionIndexMatch`, `PortfolioFactorRegressionStockWeight`, `PortfolioFactorRegressionExcludedStock`, `PortfolioFactorRegressionResponse`.
- Preserves local requests: `FactorRegressionParams`, `PortfolioFactorRegressionParams`.
- Removes handwritten: `ApiIndexMatch`, `ApiFactorRegressionResponse`, `ApiPortfolioWeight`, `ApiExcludedStock`, `ApiPortfolioFactorRegressionResponse`, and analytics response/nested interfaces.

- [ ] **Step 1: Add RED generated-identity and distinct-shape type tests**

  Add stable aliases in the test first and assert representative generated
  optionality at runtime fixtures. Create the dedicated type-test root:

  ```typescript
  import type {
    FactorRegressionIndexMatch,
    PortfolioFactorRegressionIndexMatch,
    PortfolioFactorRegressionResponse,
  } from '@trading25/contracts/types/api-response-types';

  const stockMatch: FactorRegressionIndexMatch = {
    indexCode: '0085',
    indexName: 'TOPIX-17',
    category: 'sector17',
    rSquared: 0.8,
    beta: 1.1,
  };

  const portfolioMatch: PortfolioFactorRegressionIndexMatch = {
    code: '0085',
    name: 'TOPIX-17',
    rSquared: 0.8,
  };

  declare const response: PortfolioFactorRegressionResponse;
  response.sector17Matches satisfies PortfolioFactorRegressionIndexMatch[];

  // @ts-expect-error stock match is not a portfolio match
  const invalidPortfolioMatch: PortfolioFactorRegressionIndexMatch = stockMatch;
  void portfolioMatch;
  void invalidPortfolioMatch;
  ```

  Add `type-tests/factor-regression-contracts.ts` to the dedicated tsconfig
  include if the existing glob does not already include it.

- [ ] **Step 2: Run TypeScript tests/type-tests and verify RED**

  Run:

  ```bash
  cd apps/ts
  bun run --filter @trading25/contracts test
  bun run --filter @trading25/api-clients typecheck:tests
  ```

  Expected: missing exported aliases and/or an unused `@ts-expect-error` proves
  the handwritten portfolio shape is not yet distinct.

- [ ] **Step 3: Add generated stable aliases and remove handwritten contracts**

  In `api-response-types.ts`, alias the exact generated components. Use the
  stabilized generated component keys already present in
  `bt-api-types.ts`; derive nested types from response fields where that avoids
  depending on a collision-qualified key:

  ```typescript
  export type FactorRegressionResponse = BtApiSchemas['FactorRegressionResponse'];
  export type FactorRegressionDateRange = FactorRegressionResponse['dateRange'];
  export type FactorRegressionIndexMatch = FactorRegressionResponse['sector17Matches'][number];
  export type PortfolioFactorRegressionResponse = BtApiSchemas['PortfolioFactorRegressionResponse'];
  export type PortfolioFactorRegressionDateRange = PortfolioFactorRegressionResponse['dateRange'];
  export type PortfolioFactorRegressionIndexMatch = PortfolioFactorRegressionResponse['sector17Matches'][number];
  export type PortfolioFactorRegressionStockWeight = PortfolioFactorRegressionResponse['weights'][number];
  export type PortfolioFactorRegressionExcludedStock = PortfolioFactorRegressionResponse['excludedStocks'][number];
  ```

  Delete the five handwritten `Api*` contracts from `api-types.ts`. In
  api-clients, import/re-export canonical response/nested aliases and leave only:

  ```typescript
  export interface FactorRegressionParams {
    symbol: string;
    lookbackDays?: number;
  }

  export interface PortfolioFactorRegressionParams {
    portfolioId: number;
    lookbackDays?: number;
  }
  ```

  Update web imports to canonical aliases. Do not add runtime conversion or
  field renaming.

- [ ] **Step 4: Prove the negative type test is sensitive**

  Temporarily change `PortfolioFactorRegressionIndexMatch` to the stock match
  alias, run `bun run --filter @trading25/api-clients typecheck:tests`, and
  confirm `TS2578` for the unused `@ts-expect-error`. Revert the temporary
  change and rerun successfully. Do not commit the temporary change.

- [ ] **Step 5: Run TS/web gates and commit**

  Run:

  ```bash
  cd apps/ts
  bun run --filter @trading25/contracts test
  bun run --filter @trading25/api-clients test
  bun run --filter @trading25/api-clients typecheck
  bun run quality:typecheck
  bun run quality:deps:audit
  cd packages/web
  bun run test --run \
    src/hooks/useFactorRegression.test.tsx \
    src/components/Chart/FactorRegressionPanel.test.tsx
  ```

  If the focused test filename differs, use the existing file returned by the
  discovery command in this task; do not create a duplicate test file solely to
  satisfy the command.

  Commit:

  ```bash
  git add \
    apps/ts/packages/contracts/src/types/api-response-types.ts \
    apps/ts/packages/contracts/src/types/api-response-types.test.ts \
    apps/ts/packages/contracts/src/types/api-types.ts \
    apps/ts/packages/api-clients/src/analytics \
    apps/ts/packages/api-clients/type-tests/factor-regression-contracts.ts \
    apps/ts/packages/api-clients/tsconfig.type-tests.json \
    apps/ts/packages/web/src/hooks/useFactorRegression.ts \
    apps/ts/packages/web/src/components/Chart/FactorRegressionPanel.tsx
  git commit -m "refactor(ts): converge factor regression contracts"
  ```

---

### Task 6: Run integrated verification and final compatibility audit

**Files:**
- Verify only; modify production code only if a preceding requirement is exposed as incomplete.

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: final evidence for PIT fail-closed behavior, contract sync, baseline 19, and distinct TypeScript shapes.

- [ ] **Step 1: Run the complete scoped backend suite**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/application/contracts \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/scripts/test_test_taxonomy.py \
    tests/unit/scripts/test_ci_changed_scope.py \
    tests/unit/server/test_openapi.py \
    tests/unit/server/services/test_screening_service.py \
    tests/unit/server/services/test_screening_service_helpers.py \
    tests/unit/server/services/test_screening_job_service.py \
    tests/unit/server/services/test_factor_regression_service.py \
    tests/unit/server/routes/test_analytics_complex.py \
    tests/unit/server/test_routes_portfolio_factor_regression.py -q
  uv run ruff check src tests/unit/application/contracts tests/unit/architecture
  uv run pyright src
  ```

  Expected: all commands pass.

- [ ] **Step 2: Run repository and contract gates**

  Run:

  ```bash
  cd ../..
  apps/bt/.venv/bin/python scripts/skills/audit_skills.py --strict-legacy
  python3 scripts/skills/refresh_skill_references.py --check
  ./scripts/check-contract-sync.sh
  test "$(grep -cvE '^[[:space:]]*(#|$)' apps/bt/tests/unit/architecture/application_http_schema_imports.txt)" -eq 19
  test ! -e apps/bt/src/entrypoints/http/schemas/factor_regression.py
  test ! -e apps/bt/src/entrypoints/http/schemas/portfolio_factor_regression.py
  ```

  Expected: all commands exit 0.

- [ ] **Step 3: Run final TypeScript gates**

  Run:

  ```bash
  cd apps/ts
  bun run --filter @trading25/contracts test
  bun run --filter @trading25/api-clients test
  bun run quality:typecheck
  bun run quality:deps:audit
  cd packages/web
  bun run test --run \
    src/hooks/useFactorRegression.test.tsx \
    src/components/Chart/FactorRegressionPanel.test.tsx
  ```

  Expected: all commands pass.

- [ ] **Step 4: Audit forbidden compatibility and worktree state**

  Run:

  ```bash
  cd ../../../..
  ! rg -n \
    'source_table = "stocks"|9999-12-31|Legacy/unit-test DBs' \
    apps/bt/src/application/services/screening_universe.py \
    apps/bt/src/application/services/screening_service.py
  ! rg -n \
    'src\.entrypoints\.http\.schemas\.(factor_regression|portfolio_factor_regression)' \
    apps/bt/src
  ! rg -n \
    'ApiIndexMatch|ApiFactorRegressionResponse|ApiPortfolioWeight|ApiExcludedStock|ApiPortfolioFactorRegressionResponse' \
    apps/ts/packages --glob '!**/*.test.*' --glob '!**/generated/**'
  git diff --check
  git status --short
  ```

  Expected: compatibility searches return no production matches, diff check
  passes, and the worktree is clean after the task commits.

- [ ] **Step 5: Record verification evidence**

  Write the command results and exact pass counts to
  `.superpowers/sdd/screening-pit-factor-regression-task-6-report.md`. Do not
  create an empty verification commit.
