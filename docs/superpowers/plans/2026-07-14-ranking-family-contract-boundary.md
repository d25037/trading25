# Ranking Family Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the complete Ranking response family one application owner, remove the deprecated `liquidityState` query and all compatibility logic, reduce the application-to-HTTP baseline from 33 to 21, and converge handwritten Ranking response types on generated OpenAPI types.

**Architecture:** A single flat `src.application.contracts.ranking` module owns the 12 Pydantic response models, 13 application aliases, and `normalize_sector_strength_family`. All ranking services and the analytics route use module-qualified references, while the old HTTP schema is deleted without a shim. OpenAPI is regenerated with exactly one intentional public change—the removal of `liquidityState`—then Fundamental and Value Composite TypeScript response DTOs become generated-schema aliases in a separate commit.

**Tech Stack:** Python 3.12, Pydantic v2, FastAPI/OpenAPI 3.1, pytest, Ruff, Pyright, TypeScript, Bun, Vitest, openapi-typescript

## Global Constraints

- Delete `src.entrypoints.http.schemas.ranking`; do not add an alias, re-export, forwarding module, subclass, duplicate model, or conversion wrapper.
- Task 1 may temporarily contain exact old/new definitions solely for parity verification; Task 2 must delete the old owner atomically, and no runtime consumer may switch through a compatibility surface.
- Preserve exact class docstrings, model field order, required order, annotations, Literal order, defaults, default factories, `Field` descriptions, serialization, and mutable Pydantic behavior.
- Keep all ranking calculations, PIT/as-of ordering, universe resolution, market normalization, scoring, database access, response payloads, and UI behavior unchanged.
- Remove `liquidityState`, `RankingStateFilter`, `_DEPRECATED_RANKING_RISK_STATES`, and `_normalize_ranking_state_filters` completely; do not add 410, rewrite, alias, or warning compatibility behavior.
- Keep `regimeState` and `riskState` as the only supported liquidity/risk state inputs and pass them directly to the service.
- Preserve every Ranking path, operation ID, response component, remaining parameter order/default/constraint/description, tag, summary, and error mapping.
- The only intended normalized OpenAPI/generated-TypeScript API change is removal of `liquidityState` from `GET /api/analytics/ranking`.
- Remove exactly the 12 ranking dependency rows and change the exact baseline from 33 to 21.
- Use module-qualified Python imports: `from src.application.contracts import ranking as ranking_contracts`.
- Keep TypeScript request types locally authored, but delete `MarketRankingParams.liquidityState`; response types must use generated aliases.
- Do not change frontend production code or introduce frontend ranking calculations.

---

## File Structure

- `apps/bt/src/application/contracts/ranking.py`: canonical Ranking aliases, helper, and 12 Pydantic response models.
- `apps/bt/tests/unit/application/contracts/test_ranking.py`: schema parity, serialization, default-factory, mutability, Literal, and normalizer tests.
- `apps/bt/src/application/services/ranking_*.py`: twelve application consumers of the canonical module.
- `apps/bt/src/entrypoints/http/routes/analytics_complex.py`: thin FastAPI exposure of canonical models and supported query inputs.
- `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`: migrated-name and deleted-module ownership enforcement.
- `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`: exact baseline reduced by 12 rows.
- `apps/bt/tests/unit/architecture/test_layer_boundaries.py`: scanner behavior, deleted-path checks, and count 21.
- `apps/bt/tests/unit/server/test_openapi.py`: component stability and `liquidityState` absence.
- `apps/ts/packages/contracts/openapi/bt-openapi.json`: regenerated OpenAPI snapshot with only the deprecated query removed.
- `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`: regenerated TypeScript contract.
- `apps/ts/packages/contracts/src/types/api-response-types.ts`: Fundamental/Value response aliases to generated schemas.
- `apps/ts/packages/contracts/src/types/api-response-types.test.ts`: compile/runtime fixtures for generated optional fields.
- `apps/ts/packages/api-clients/src/analytics/types.ts`: stable public response re-exports and local request types without `liquidityState`.
- `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`: request serialization without `liquidityState`.
- `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`: ranking URL contract and removed-property coverage.

---

### Task 1: Add the canonical Ranking application contract

**Files:**
- Create: `apps/bt/src/application/contracts/ranking.py`
- Create: `apps/bt/tests/unit/application/contracts/test_ranking.py`

**Interfaces:**
- Consumes: Pydantic `BaseModel` and `Field`, `typing.Literal`, and the exact definitions currently in `src.entrypoints.http.schemas.ranking`.
- Produces: 12 public Pydantic models; 13 application aliases; `normalize_sector_strength_family(value: str) -> SectorStrengthFamily`.

- [ ] **Step 1: Write failing canonical import and behavior tests**

  Import the future module as:

  ```python
  from src.application.contracts import ranking as ranking_contracts
  ```

  Add separate tests that assert:

  ```python
  assert ranking_contracts.ValueCompositeScoreMethod.__args__ == (
      "standard_pbr_tilt",
      "prime_size_tilt",
      "prime_size75_forward_per25",
      "equal_weight",
  )
  assert ranking_contracts.SectorStrengthFamily.__args__ == (
      "balanced_sector_strength",
      "long_hybrid_leadership",
  )
  assert ranking_contracts.normalize_sector_strength_family(
      "balanced_sector_strength"
  ) == "balanced_sector_strength"
  with pytest.raises(ValueError, match="Unsupported sectorStrengthFamily"):
      ranking_contracts.normalize_sector_strength_family("unknown")
  ```

  Instantiate complete daily, fundamental, and value-composite response graphs and assert `model_dump()` values. Assert default-backed lists/dicts are omitted from `model_json_schema()["required"]`, are independent between instances, and that a `RankingItem` field can be enriched after construction.

  Encode the exact 12 model names and assert their `model_json_schema()` property order and required order match an explicit expected mapping copied from the current HTTP module before production code is added.

- [ ] **Step 2: Run the new test and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/application/contracts/test_ranking.py -q
  ```

  Expected: collection fails with `ImportError` because `src.application.contracts.ranking` does not exist.

- [ ] **Step 3: Create the canonical module**

  Use `apply_patch` to create `apps/bt/src/application/contracts/ranking.py`. Move the following definitions verbatim and in their existing order:

  ```text
  ValueCompositeScoreMethod
  ValueCompositeProfileId
  ValueCompositeForwardEpsMode
  ValueCompositeScoreUnavailableReason
  LiquidityRegime
  RankingRiskFlag
  RankingTechnicalFlag
  RankingRegimeStateFilter
  RankingRiskStateFilter
  RankingTechnicalStateFilter
  RankingFundamentalStateFilter
  SectorStrengthBucket
  SectorStrengthFamily
  normalize_sector_strength_family
  RankingItem
  Rankings
  IndexPerformanceItem
  MarketRankingResponse
  MarketRankingSymbolResponse
  FundamentalRankingItem
  FundamentalRankings
  MarketFundamentalRankingResponse
  ValueCompositeTechnicalMetrics
  ValueCompositeRankingItem
  ValueCompositeRankingResponse
  ValueCompositeScoreResponse
  ```

  Do not copy `RankingStateFilter`. Do not import from the HTTP module. The new module imports only `Literal`, `BaseModel`, and `Field`.

- [ ] **Step 4: Prove exact legacy parity before deletion**

  Run:

  ```bash
  cd apps/bt
  uv run python - <<'PY'
  from src.application.contracts import ranking as new
  from src.entrypoints.http.schemas import ranking as old

  names = (
      "RankingItem", "Rankings", "IndexPerformanceItem",
      "MarketRankingResponse", "MarketRankingSymbolResponse",
      "FundamentalRankingItem", "FundamentalRankings",
      "MarketFundamentalRankingResponse", "ValueCompositeTechnicalMetrics",
      "ValueCompositeRankingItem", "ValueCompositeRankingResponse",
      "ValueCompositeScoreResponse",
  )
  for name in names:
      before, after = getattr(old, name), getattr(new, name)
      assert before.model_json_schema() == after.model_json_schema(), name
      assert before.__doc__ == after.__doc__, name

  aliases = (
      "ValueCompositeScoreMethod", "ValueCompositeProfileId",
      "ValueCompositeForwardEpsMode", "ValueCompositeScoreUnavailableReason",
      "LiquidityRegime", "RankingRiskFlag", "RankingTechnicalFlag",
      "RankingRegimeStateFilter", "RankingRiskStateFilter",
      "RankingTechnicalStateFilter", "RankingFundamentalStateFilter",
      "SectorStrengthBucket", "SectorStrengthFamily",
  )
  for name in aliases:
      assert getattr(old, name) == getattr(new, name), name
  PY
  ```

  Expected: exit 0 with no output.

- [ ] **Step 5: Run focused quality gates**

  Run:

  ```bash
  cd apps/bt
  uv run pytest tests/unit/application/contracts/test_ranking.py -q
  uv run ruff check src/application/contracts/ranking.py tests/unit/application/contracts/test_ranking.py
  uv run pyright src/application/contracts/ranking.py
  ```

  Expected: all commands pass.

- [ ] **Step 6: Commit the canonical owner**

  ```bash
  git add apps/bt/src/application/contracts/ranking.py apps/bt/tests/unit/application/contracts/test_ranking.py
  git commit -m "feat(bt): add canonical ranking contracts"
  ```

---

### Task 2: Cut over the backend, remove the deprecated query, and update OpenAPI

**Files:**
- Modify: `apps/bt/src/application/services/ranking_collection_filters.py`
- Modify: `apps/bt/src/application/services/ranking_daily_queries.py`
- Modify: `apps/bt/src/application/services/ranking_daily_technical_metrics.py`
- Modify: `apps/bt/src/application/services/ranking_index_performance.py`
- Modify: `apps/bt/src/application/services/ranking_liquidity.py`
- Modify: `apps/bt/src/application/services/ranking_response_items.py`
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Modify: `apps/bt/src/application/services/ranking_state_flags.py`
- Modify: `apps/bt/src/application/services/ranking_technical_flags.py`
- Modify: `apps/bt/src/application/services/ranking_valuation.py`
- Modify: `apps/bt/src/application/services/ranking_value_composite_config.py`
- Modify: `apps/bt/src/application/services/ranking_value_composite_metrics.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Delete: `apps/bt/src/entrypoints/http/schemas/ranking.py`
- Modify: `apps/bt/tests/unit/server/services/test_ranking_service.py`
- Modify: `apps/bt/tests/unit/server/routes/test_analytics_complex.py`
- Modify: `apps/bt/tests/unit/server/test_openapi.py`
- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Modify: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`

**Interfaces:**
- Consumes: `src.application.contracts.ranking` from Task 1.
- Produces: the same five Ranking endpoints and 12 response component schemas, without `liquidityState`; baseline 21; no legacy HTTP Ranking schema.

- [ ] **Step 1: Add RED architecture ownership tests**

  Define a ranking migrated-name set containing all 12 model names, 13 application alias names, `normalize_sector_strength_family`, and the removed legacy name `RankingStateFilter`. Extend synthetic guard tests to reject direct imports from `src.entrypoints.http.schemas.ranking`, HTTP top-level binding/re-export of every migrated or removed name, and recreation of the old module.

  Add repository assertions equivalent to:

  ```python
  assert not (HTTP_SCHEMA_ROOT / "ranking.py").exists()
  assert len(_application_http_schema_baseline()) == 21
  ```

  Assert the exact old import path is absent from production source after migration, while allowing its literal inside negative architecture tests.

- [ ] **Step 2: Add RED route and OpenAPI tests for deprecated-query deletion**

  Delete the two tests that assert `liquidityState` translation. Add tests proving supported state inputs are forwarded directly:

  ```python
  response = client.get(
      "/api/analytics/ranking?includeValuation=true"
      "&regimeState=crowded_rerating&riskState=overheat"
  )
  assert response.status_code == 200
  call_kwargs = service.get_rankings.call_args.kwargs
  assert call_kwargs["regime_state"] == "crowded_rerating"
  assert call_kwargs["risk_state"] == "overheat"
  ```

  Add an OpenAPI assertion:

  ```python
  parameters = openapi_schema["paths"]["/api/analytics/ranking"]["get"]["parameters"]
  assert "liquidityState" not in {parameter["name"] for parameter in parameters}
  ```

  Preserve and assert the ordered names of every remaining parameter exactly as listed in the design.

- [ ] **Step 3: Run architecture/route/OpenAPI tests and verify RED**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/routes/test_analytics_complex.py \
    tests/unit/server/test_openapi.py -q
  ```

  Expected: failures show baseline 33 instead of 21, the old module exists, migrated names are unguarded, and `liquidityState` remains in the route/OpenAPI.

- [ ] **Step 4: Migrate all application consumers**

  In each of the twelve `ranking_*.py` files, replace direct HTTP imports with:

  ```python
  from src.application.contracts import ranking as ranking_contracts
  ```

  Qualify every annotation, constructor, Literal alias, and helper call with `ranking_contracts.`. Do not leave module-level aliases such as `RankingItem = ranking_contracts.RankingItem`; the ownership guard must make the canonical dependency visible.

- [ ] **Step 5: Migrate the route and delete compatibility logic**

  In `analytics_complex.py`, import the canonical module and qualify all Ranking response models and supported filter/value aliases. Remove these definitions and uses completely:

  ```python
  _DEPRECATED_RANKING_RISK_STATES
  _normalize_ranking_state_filters
  RankingStateFilter
  liquidityState
  normalized_regime_state
  normalized_risk_state
  ```

  Pass `regimeState` and `riskState` directly to `RankingService.get_rankings`. Keep all other route parameters, order, defaults, Query constraints/descriptions, response models, summaries, and error handlers unchanged.

  Update direct test imports to `src.application.contracts.ranking`. Delete `src/entrypoints/http/schemas/ranking.py` with no replacement in the HTTP package.

- [ ] **Step 6: Complete the guard and exact baseline reduction**

  Apply the ranking migrated-name set throughout `src/entrypoints/http` so routes cannot bind canonical class names directly. Preserve qualified module imports. Remove exactly these twelve baseline rows:

  ```text
  application/services/ranking_collection_filters.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_daily_queries.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_daily_technical_metrics.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_index_performance.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_liquidity.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_response_items.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_service.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_state_flags.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_technical_flags.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_valuation.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_value_composite_config.py|src.entrypoints.http.schemas.ranking
  application/services/ranking_value_composite_metrics.py|src.entrypoints.http.schemas.ranking
  ```

  Change the explicit count assertion to 21. Do not alter another baseline row.

- [ ] **Step 7: Run the backend GREEN suite**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/application/contracts/test_ranking.py \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/services/test_ranking_service.py \
    tests/unit/server/routes/test_analytics_complex.py \
    tests/unit/server/test_openapi.py -q
  uv run ruff check \
    src/application/contracts/ranking.py \
    src/application/services/ranking_*.py \
    src/entrypoints/http/routes/analytics_complex.py \
    tests/unit/application/contracts/test_ranking.py \
    tests/unit/architecture \
    tests/unit/server/routes/test_analytics_complex.py \
    tests/unit/server/test_openapi.py
  uv run pyright src
  ```

  Expected: all commands pass; no application import references the HTTP Ranking schema; baseline count is 21.

- [ ] **Step 8: Prove the pre-sync OpenAPI delta is only `liquidityState`**

  Export the fresh source schema without overwriting the committed snapshot, normalize both JSON documents, remove only the `/api/analytics/ranking` GET parameter named `liquidityState` from the committed document, and compare equality:

  ```bash
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/ranking-openapi.XXXXXX")"
  trap 'rm -rf "$tmp_dir"' EXIT
  cd apps/bt
  uv run python scripts/export_openapi.py --output "$tmp_dir/current.json"
  cd ../..
  apps/bt/.venv/bin/python - "$tmp_dir/current.json" apps/ts/packages/contracts/openapi/bt-openapi.json <<'PY'
  import json
  import sys
  from pathlib import Path

  current = json.loads(Path(sys.argv[1]).read_text())
  previous = json.loads(Path(sys.argv[2]).read_text())
  parameters = previous["paths"]["/api/analytics/ranking"]["get"]["parameters"]
  previous["paths"]["/api/analytics/ranking"]["get"]["parameters"] = [
      parameter for parameter in parameters if parameter["name"] != "liquidityState"
  ]
  assert current == previous
  PY
  ```

  Expected: exit 0. Any other OpenAPI difference blocks the task.

- [ ] **Step 9: Sync and verify generated contracts**

  Run:

  ```bash
  cd apps/ts
  bun run --filter @trading25/contracts bt:sync
  bun run --filter @trading25/contracts bt:check
  cd ../..
  ! rg -n "liquidityState" \
    apps/ts/packages/contracts/openapi/bt-openapi.json \
    apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
  ./scripts/check-contract-sync.sh
  ```

  Expected: sync/check pass and the generated artifacts contain no `liquidityState`. The 12 Ranking component schemas remain unchanged.

- [ ] **Step 10: Commit the atomic backend/API migration**

  ```bash
  git add \
    apps/bt/src/application/services/ranking_*.py \
    apps/bt/src/entrypoints/http/routes/analytics_complex.py \
    apps/bt/src/entrypoints/http/schemas/ranking.py \
    apps/bt/tests/unit/architecture \
    apps/bt/tests/unit/server/services/test_ranking_service.py \
    apps/bt/tests/unit/server/routes/test_analytics_complex.py \
    apps/bt/tests/unit/server/test_openapi.py \
    apps/ts/packages/contracts/openapi/bt-openapi.json \
    apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
  git commit -m "refactor(bt): move ranking contracts to application"
  ```

---

### Task 3: Converge TypeScript response types and client inputs

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`
- Verify: `apps/ts/packages/api-clients/src/analytics/index.ts`

**Interfaces:**
- Consumes: generated `BtApiSchemas` after Task 2.
- Produces: stable public Ranking response type names backed by generated schemas; local request types without `liquidityState`.

- [ ] **Step 1: Add compile-time RED fixtures for generated-valid omissions**

  In `api-response-types.test.ts`, create typed fixtures that omit the default-backed properties:

  ```typescript
  const fundamentalRankings: FundamentalRankings = {};
  const valueCompositeRanking: ValueCompositeRankingResponse = {
    date: '2026-07-14',
    markets: ['0112'],
    metricKey: 'standard_value_composite',
    scoreMethod: 'standard_pbr_tilt',
    forwardEpsMode: 'latest',
    applyLiquidityFilter: true,
    scorePolicy: 'rank percentile',
    weights: {},
    itemCount: 0,
    lastUpdated: '2026-07-14T00:00:00Z',
  };
  const valueCompositeScore: ValueCompositeScoreResponse = {
    date: '2026-07-14',
    code: '7203',
    metricKey: 'standard_value_composite',
    forwardEpsMode: 'latest',
    universeCount: 0,
    scoreAvailable: false,
    lastUpdated: '2026-07-14T00:00:00Z',
  };
  ```

  In `AnalyticsClient.test.ts`, add a compile-only `@ts-expect-error` assertion that `MarketRankingParams` rejects `liquidityState` after the migration:

  ```typescript
  const removedLegacyParam: MarketRankingParams = {
    // @ts-expect-error liquidityState was removed; use regimeState or riskState
    liquidityState: 'crowded_rerating',
  };
  void removedLegacyParam;
  ```

- [ ] **Step 2: Run a direct test-file compile and verify RED**

  Run:

  ```bash
  cd apps/ts
  bun run quality:typecheck:root
  ```

  Expected: TypeScript reports required `ratioHigh`/`ratioLow`, `items`, or `weights`, and reports an unused `@ts-expect-error` because `liquidityState` still exists.

- [ ] **Step 3: Replace handwritten response DTOs with generated aliases**

  In `api-response-types.ts`, use these aliases:

  ```typescript
  export type FundamentalRankingItem = BtApiSchemas['FundamentalRankingItem'];
  export type FundamentalRankings = BtApiSchemas['FundamentalRankings'];
  export type MarketFundamentalRankingResponse = BtApiSchemas['MarketFundamentalRankingResponse'];
  export type ValueCompositeTechnicalMetrics = BtApiSchemas['ValueCompositeTechnicalMetrics'];
  export type ValueCompositeRankingItem = BtApiSchemas['ValueCompositeRankingItem'];
  export type ValueCompositeRankingResponse = BtApiSchemas['ValueCompositeRankingResponse'];
  export type ValueCompositeScoreResponse = BtApiSchemas['ValueCompositeScoreResponse'];
  ```

  Derive exported enum/source aliases from these generated types with indexed access and `NonNullable`; do not retain parallel Literal unions.

  In `api-clients/src/analytics/types.ts`, import and re-export the contract response aliases and keep only request parameter interfaces local. Preserve every public export name in `analytics/index.ts`.

- [ ] **Step 4: Remove the deprecated client request property**

  Delete from `MarketRankingParams`:

  ```typescript
  liquidityState?: RankingLiquidityState;
  ```

  Remove `liquidityState: params.liquidityState` from `AnalyticsClient.getMarketRanking`. Remove any now-unused `RankingLiquidityState` alias/import/export only when `rg` proves no supported consumer remains.

  Update `AnalyticsClient.test.ts` so the expected Ranking URL contains supported `regimeState`, `fundamentalState`, `riskState`, and `technicalState`, and assert the URL does not contain `liquidityState`.

- [ ] **Step 5: Run TypeScript GREEN gates**

  Run:

  ```bash
  cd apps/ts
  bun run quality:typecheck:root
  bun test packages/contracts/src/types/api-response-types.test.ts
  bun run --filter @trading25/contracts test
  bun run --filter @trading25/api-clients test
  bun run quality:typecheck
  ```

  Expected: all commands pass; response aliases resolve to generated schemas; the removed request property is rejected.

- [ ] **Step 6: Run supported web Ranking regression harnesses**

  Run:

  ```bash
  cd apps/ts/packages/web
  bun run test --run \
    src/hooks/useRanking.test.tsx \
    src/hooks/useRankingSymbolSnapshot.test.tsx \
    src/pages/RankingPage.test.tsx \
    src/components/Ranking/RankingTable.test.tsx \
    src/components/Ranking/IndexPerformanceTable.test.tsx
  ```

  Expected: every selected Vitest file passes under the web package's DOM harness.

- [ ] **Step 7: Verify scope and commit**

  Run:

  ```bash
  cd ../../../..
  ! rg -n "liquidityState|RankingStateFilter|_normalize_ranking_state_filters|_DEPRECATED_RANKING_RISK_STATES" \
    apps/bt/src apps/ts/packages/api-clients/src apps/ts/packages/contracts/src/types
  git diff --check
  ```

  Expected: no compatibility names remain in production source and no whitespace errors exist.

  Commit:

  ```bash
  git add \
    apps/ts/packages/contracts/src/types/api-response-types.ts \
    apps/ts/packages/contracts/src/types/api-response-types.test.ts \
    apps/ts/packages/api-clients/src/analytics/types.ts \
    apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts \
    apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts \
    apps/ts/packages/api-clients/src/analytics/index.ts
  git commit -m "refactor(ts): converge ranking response contracts"
  ```

---

### Task 4: Run integrated verification and final review

**Files:**
- Verify: all files changed by Tasks 1-3
- Modify only if a gate exposes an in-scope defect: the smallest responsible source/test file, with a new RED/GREEN cycle

**Interfaces:**
- Consumes: completed Python ownership migration, OpenAPI update, and TypeScript convergence.
- Produces: clean main state with baseline 21, no compatibility surfaces, and independent merge-readiness evidence.

- [ ] **Step 1: Run the complete scoped backend suite**

  Run:

  ```bash
  cd apps/bt
  uv run pytest \
    tests/unit/application/contracts \
    tests/unit/architecture/test_layer_boundaries.py \
    tests/unit/server/test_openapi.py \
    tests/unit/server/routes/test_analytics_complex.py \
    tests/unit/server/services/test_ranking_service.py \
    tests/unit/scripts/test_check_contract_sync.py \
    tests/unit/scripts/test_audit_skills.py -q
  uv run ruff check src tests/unit/application/contracts tests/unit/architecture tests/unit/server/test_openapi.py tests/unit/server/routes/test_analytics_complex.py
  uv run pyright src
  ```

  Expected: every command passes.

- [ ] **Step 2: Run architecture, compatibility-removal, and contract gates**

  Run from repository root:

  ```bash
  apps/bt/.venv/bin/python scripts/skills/audit_skills.py --strict-legacy
  ./scripts/check-contract-sync.sh
  test "$(grep -cvE '^[[:space:]]*(#|$)' apps/bt/tests/unit/architecture/application_http_schema_imports.txt)" -eq 21
  test ! -e apps/bt/src/entrypoints/http/schemas/ranking.py
  ! rg -n "src\.entrypoints\.http\.schemas\.ranking" apps/bt/src
  ! rg -n "liquidityState|RankingStateFilter|_normalize_ranking_state_filters|_DEPRECATED_RANKING_RISK_STATES" \
    apps/bt/src apps/ts/packages/api-clients/src apps/ts/packages/contracts/src/types
  ```

  Expected: audit/sync pass, baseline equals 21, the old file/path is absent, and no deprecated compatibility identifier remains in production source.

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
    src/hooks/useRanking.test.tsx \
    src/hooks/useRankingSymbolSnapshot.test.tsx \
    src/pages/RankingPage.test.tsx \
    src/components/Ranking/RankingTable.test.tsx \
    src/components/Ranking/IndexPerformanceTable.test.tsx
  cd ../../../..
  git diff --check
  git status --short
  ```

  Expected: all tests/type/dependency gates pass, `git diff --check` is clean, and status contains no uncommitted files.

- [ ] **Step 4: Dispatch independent final review**

  Give a fresh reviewer the approved design, this plan, all Task reports/reviews, verification evidence, and the full diff from `e017e85d` to `HEAD`. Require explicit verdicts on canonical ownership, compatibility removal, exact 33→21 ratchet, supported endpoint stability, OpenAPI delta scope, TS alias accuracy, PIT/runtime non-regression, test adequacy, and merge readiness.

  Critical or Important findings require one consolidated fix subagent, focused tests covering every fix, and a repeat review. Record Minor findings in the durable SDD ledger for explicit final disposition.
