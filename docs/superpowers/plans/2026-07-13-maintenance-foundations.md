# Maintenance Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore enforceable governance, make Daily Ranking use one OpenAPI-generated TypeScript contract, and prevent growth of the current bt application-to-HTTP-schema dependency debt.

**Architecture:** Extend the existing changed-file taxonomy and skill audit instead of adding a second governance system. Reverse the TypeScript package dependency so contracts is independent and api-clients consumes its generated aliases. Preserve current bt behavior while replacing the broad transitional exception with an exact checked-in dependency baseline.

**Tech Stack:** Python 3.12, pytest, Bun 1.3.x, TypeScript, OpenAPI-generated types, Vitest/Testing Library, GitHub Actions.

## Global Constraints

- Do not change FastAPI payloads or regenerate a different OpenAPI schema.
- Do not change ranking, screening, synchronization, strategy, research, or backtest behavior.
- Production code changes require a failing test first.
- Repository skill verification commands must work from the repository root.
- `.codex/skills/**` and every `AGENTS.md` must run repo guardrails without forcing unrelated product tests.
- `@trading25/contracts` must not depend on `@trading25/api-clients`.
- Existing application-to-HTTP-schema imports remain temporarily supported, but the exact set may only shrink.

---

### Task 1: Classify instruction and skill changes as governance

**Files:**
- Modify: `scripts/ci/test_taxonomy.py`
- Modify: `scripts/ci/changed-scope.py`
- Modify: `apps/bt/tests/unit/scripts/test_test_taxonomy.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`

**Interfaces:**
- Produces: `is_governance_path(path: str) -> bool`
- Produces: `CiScope` behavior where governance-only changes have every product/research/contracts/security flag false and `docs_only=false`
- Consumes: existing `normalize_path`, `is_docs_path`, and CI output format

- [ ] **Step 1: Add failing taxonomy tests**

Add these tests to `test_test_taxonomy.py`:

```python
def test_skill_markdown_is_governance_not_plain_docs() -> None:
    module = _load_module()
    path = ".codex/skills/ts-api-endpoints/SKILL.md"

    assert module.is_docs_path(path)
    assert module.is_governance_path(path)


def test_nested_agents_file_is_governance() -> None:
    module = _load_module()

    assert module.is_governance_path("apps/bt/AGENTS.md")
    assert module.is_governance_path("AGENTS.md")
```

Add these tests to `test_ci_changed_scope.py`:

```python
def test_skill_only_change_runs_guardrails_without_product_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        [".codex/skills/ts-api-endpoints/SKILL.md"]
    )

    assert scope.product_ci is False
    assert scope.research_ci is False
    assert scope.contracts_ci is False
    assert scope.security_ci is False
    assert scope.docs_only is False


def test_agents_only_change_runs_guardrails_without_product_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["apps/ts/AGENTS.md"])

    assert scope.product_ci is False
    assert scope.research_ci is False
    assert scope.contracts_ci is False
    assert scope.security_ci is False
    assert scope.docs_only is False
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_test_taxonomy.py tests/unit/scripts/test_ci_changed_scope.py -q
```

Expected: FAIL because `is_governance_path` does not exist and governance Markdown is currently docs-only.

- [ ] **Step 3: Implement the governance taxonomy**

Add to `test_taxonomy.py`:

```python
GOVERNANCE_PREFIXES = (
    ".codex/skills/",
    "scripts/skills/",
    "apps/bt/tests/unit/scripts/test_audit_skills.py",
)


def is_governance_path(path: str) -> bool:
    return path == "AGENTS.md" or path.endswith("/AGENTS.md") or path.startswith(
        GOVERNANCE_PREFIXES
    )
```

Import `is_governance_path` in `changed-scope.py`. During classification, compute `is_governance`, include it in the recognized-path condition, and calculate:

```python
docs_only = all(is_docs_path(path) and not is_governance_path(path) for path in normalized)
```

Do not set `product_ci` for governance-only changes.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the command from Step 2.

Expected: all taxonomy and changed-scope tests pass.

- [ ] **Step 5: Verify the actual CLI output**

Run:

```bash
printf '%s\n' '.codex/skills/ts-api-endpoints/SKILL.md' | python3 scripts/ci/changed-scope.py
```

Expected:

```text
product_ci=false
research_ci=false
contracts_ci=false
security_ci=false
docs_only=false
```

- [ ] **Step 6: Commit the governance classification**

```bash
git add scripts/ci/test_taxonomy.py scripts/ci/changed-scope.py apps/bt/tests/unit/scripts/test_test_taxonomy.py apps/bt/tests/unit/scripts/test_ci_changed_scope.py
git commit -m "ci: classify agent governance changes"
```

---

### Task 2: Make skill verification commands root-safe and auditable

**Files:**
- Modify: `scripts/skills/audit_skills.py`
- Modify: `apps/bt/tests/unit/scripts/test_audit_skills.py`
- Modify: `.codex/skills/*/SKILL.md`
- Modify: `AGENTS.md`
- Modify: `apps/bt/AGENTS.md`
- Modify: `apps/ts/AGENTS.md`

**Interfaces:**
- Produces: `verification_commands(content: str) -> tuple[str, ...]`
- Produces: validation errors for root-unsafe `uv run`, `bun run`, and `python` command forms inside `## Verification`
- Consumes: existing `validate_skill_file` error aggregation

- [ ] **Step 1: Add failing command-validation tests**

Add a helper fixture that creates a valid workflow skill and these tests to `test_audit_skills.py`:

```python
def test_root_unsafe_uv_verification_command_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --project apps/bt pytest tests/unit/server/routes",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe uv command" in error for error in errors)


def test_root_unsafe_bun_verification_command_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun run quality:typecheck",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe bun command" in error for error in errors)


def test_root_safe_verification_commands_pass(tmp_path: Path) -> None:
    module = _load_audit_module()
    bt_skill = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/server/routes",
    )
    ts_skill = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun --cwd apps/ts run quality:typecheck",
    )

    assert module.validate_skill_file(bt_skill, tmp_path) == []
    assert module.validate_skill_file(ts_skill, tmp_path) == []
```

The `_workflow_skill` helper must write all required headings and no unrelated path references.

- [ ] **Step 2: Run the focused audit tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_audit_skills.py -q
```

Expected: FAIL because verification commands are not parsed or validated.

- [ ] **Step 3: Parse and validate the Verification section**

Add these patterns and helper behavior to `audit_skills.py`:

```python
VERIFICATION_SECTION_PATTERN = re.compile(
    r"^## Verification\s*$([\s\S]*?)(?=^## |\Z)", re.MULTILINE
)


def verification_commands(content: str) -> tuple[str, ...]:
    match = VERIFICATION_SECTION_PATTERN.search(content)
    if match is None:
        return ()
    return tuple(CODE_SPAN_PATTERN.findall(match.group(1)))


def validate_verification_commands(content: str, skill_file: Path) -> list[str]:
    errors: list[str] = []
    for command in verification_commands(content):
        if command.startswith("uv run ") and not command.startswith(
            "uv run --directory apps/bt "
        ):
            errors.append(f"Verification must use a root-safe uv command: {skill_file} -> {command}")
        if command.startswith("bun run "):
            errors.append(f"Verification must use a root-safe bun command: {skill_file} -> {command}")
        if command.startswith("python "):
            errors.append(f"Verification must use python3: {skill_file} -> {command}")
    return errors
```

Call `validate_verification_commands` from `validate_skill_file`.

- [ ] **Step 4: Run the focused test and verify partial GREEN**

Run the Step 2 command.

Expected: the new unit tests pass; existing fixtures that intentionally use the old form may now fail and must be updated to the root-safe form without weakening their original assertion.

- [ ] **Step 5: Mechanically normalize every repository skill command**

Apply these exact transformations only inside `## Verification` sections:

```text
uv run --project apps/bt <tool> <path>  -> uv run --directory apps/bt <tool> <apps/bt-relative-path>
uv run <tool> <path>                    -> uv run --directory apps/bt <tool> <apps/bt-relative-path>
bun run <script>                        -> bun --cwd apps/ts run <script>
python <script>                         -> python3 <script>
```

Keep root-owned commands such as `python3 scripts/skills/audit_skills.py --strict-legacy` unchanged. Do not change command semantics or expand test scope.

- [ ] **Step 6: Correct directly related AGENTS drift**

Update the instruction files so they state:

```text
CI runs on pushes to main, pull requests, and workflow_dispatch.
Repository-local process and domain skills are those exposed by the current Codex skill catalog; do not assume ~/.agents/skills entries exist.
experimental, production, and legacy strategies are external/XDG categories; reference is project-owned; resolver fallback behavior must be checked before editing a shadowed name.
```

Remove the unavailable named `~/.agents/skills` inventory. Preserve the rule that repository `AGENTS.md` and `.codex/skills` take precedence.

- [ ] **Step 7: Run the strict audit and command-focused tests**

Run:

```bash
python3 scripts/skills/audit_skills.py --strict-legacy
uv run --directory apps/bt pytest tests/unit/scripts/test_audit_skills.py -q
```

Expected: both commands pass.

- [ ] **Step 8: Smoke representative commands from the repository root**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/architecture -q
bun --cwd apps/ts run --filter @trading25/contracts bt:check
python3 scripts/skills/refresh_skill_references.py --check
```

Expected: all three commands pass.

- [ ] **Step 9: Commit skill and instruction governance**

```bash
git add scripts/skills/audit_skills.py apps/bt/tests/unit/scripts/test_audit_skills.py .codex/skills AGENTS.md apps/bt/AGENTS.md apps/ts/AGENTS.md
git commit -m "chore: harden repository skill governance"
```

---

### Task 3: Make generated Ranking schemas canonical across TypeScript packages

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Move: `apps/ts/packages/contracts/src/clients/backtest/generated/type-compatibility-check.ts` to `apps/ts/packages/api-clients/src/backtest/type-compatibility-check.ts`
- Modify: `apps/ts/packages/contracts/package.json`
- Modify: `apps/ts/packages/api-clients/package.json`
- Modify: `apps/ts/packages/api-clients/src/analytics/types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/index.ts`
- Modify: `apps/ts/package.json`
- Modify: `apps/ts/bun.lock`
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingSummary.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingSummary.test.tsx`
- Test: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`

**Interfaces:**
- Produces: stable aliases backed by `components['schemas']` for Ranking response types
- Produces: `@trading25/api-clients/analytics` re-exports of those aliases
- Consumes: generated `bt-api-types.ts` and existing `AnalyticsClient` signatures

- [ ] **Step 1: Add a failing omitted-collection UI test**

Add to `RankingSummary.test.tsx`:

```tsx
it('renders empty ranking collections without throwing', () => {
  const sparseData: MarketRankingResponse = {
    date: '2026-07-13',
    markets: ['0111'],
    lookbackDays: 20,
    periodDays: 20,
    rankings: {},
    indexPerformance: [],
    lastUpdated: '2026-07-13T15:00:00+09:00',
  };

  render(<RankingSummary data={sparseData} />);

  expect(screen.getByText('Top Volume')).toBeInTheDocument();
  expect(screen.getAllByText('-').length).toBeGreaterThan(0);
});
```

- [ ] **Step 2: Run the focused component test and verify RED**

Run:

```bash
bun --cwd apps/ts test packages/web/src/components/Ranking/RankingSummary.test.tsx
```

Expected: FAIL at runtime because `data.rankings.gainers[0]` reads index zero from `undefined`.

- [ ] **Step 3: Replace handwritten contract interfaces with generated aliases**

In `api-response-types.ts`, replace handwritten backend Ranking interfaces with aliases of the existing `BtApiSchemas` mapping:

```typescript
export type RankingItem = BtApiSchemas['RankingItem'];
export type Rankings = BtApiSchemas['Rankings'];
export type IndexPerformanceItem = BtApiSchemas['IndexPerformanceItem'];
export type MarketRankingResponse = BtApiSchemas['MarketRankingResponse'];
export type MarketRankingSymbolResponse = BtApiSchemas['MarketRankingSymbolResponse'];
```

Derive the two flag element types from the generated `RankingItem` instead of maintaining separate unions:

```typescript
export type RankingRiskFlag = NonNullable<RankingItem['riskFlags']>[number];
export type RankingTechnicalFlag = NonNullable<RankingItem['technicalFlags']>[number];
```

Keep `MarketRankingParams` and other request/view state local when they are not response schemas.

- [ ] **Step 4: Reverse the package dependency and relocate compatibility checks**

Move the compatibility checker to api-clients. Change its generated import to:

```typescript
import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';
```

Remove `@trading25/api-clients` and `build:deps` from `packages/contracts/package.json`. Add this dependency to `packages/api-clients/package.json`:

```json
"@trading25/contracts": "workspace:*"
```

In root `apps/ts/package.json`, build contracts before api-clients:

```json
"workspace:build": "bun run --filter @trading25/contracts build:local && bun run api-clients:build && bun run --filter @trading25/utils build:local && bun run web:build"
```

Run `bun install` from `apps/ts` to update `bun.lock` after manifest changes.

- [ ] **Step 5: Re-export canonical types from api-clients**

Remove the duplicate Ranking response interfaces from `api-clients/src/analytics/types.ts` and add type-only imports/exports:

```typescript
import type {
  MarketRankingResponse,
  MarketRankingSymbolResponse,
  RankingItem,
  Rankings,
} from '@trading25/contracts/types/api-response-types';

export type {
  MarketRankingResponse,
  MarketRankingSymbolResponse,
  RankingItem,
  Rankings,
} from '@trading25/contracts/types/api-response-types';
```

Keep the local names available to `AnalyticsClient.ts` and preserve the public exports from `analytics/index.ts`.

- [ ] **Step 6: Make RankingSummary tolerate omitted collections**

Use empty-array fallbacks:

```typescript
const topGainer = data.rankings.gainers?.[0];
const topLoser = data.rankings.losers?.[0];
const topVolume = data.rankings.tradingValue?.[0];
```

- [ ] **Step 7: Run focused tests and typechecks**

Run:

```bash
bun --cwd apps/ts test packages/web/src/components/Ranking/RankingSummary.test.tsx
bun --cwd apps/ts run --filter @trading25/contracts typecheck
bun --cwd apps/ts run --filter @trading25/api-clients typecheck
bun --cwd apps/ts run --filter @trading25/api-clients test
bun --cwd apps/ts run --filter @trading25/web typecheck
```

Expected: all commands pass, and the sparse response test is GREEN.

- [ ] **Step 8: Verify dependency direction and contract stability**

Run:

```bash
bun --cwd apps/ts run quality:deps:audit
./scripts/check-contract-sync.sh
git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
```

Expected: dependency audit and contract check pass; OpenAPI snapshot and generated output have no semantic diff after regeneration.

- [ ] **Step 9: Commit the Ranking contract correction**

```bash
git add apps/ts/package.json apps/ts/bun.lock apps/ts/packages/contracts apps/ts/packages/api-clients apps/ts/packages/web/src/components/Ranking/RankingSummary.tsx apps/ts/packages/web/src/components/Ranking/RankingSummary.test.tsx
git commit -m "refactor(ts): make ranking contracts generated aliases"
```

---

### Task 4: Add an exact-set ratchet for application HTTP-schema imports

**Files:**
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Create: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Modify: `docs/bt-src-layering-guide.md`

**Interfaces:**
- Produces: `_application_http_schema_imports() -> set[str]`
- Produces: baseline line format `<src-relative-python-path>|<imported-module>`
- Consumes: `_iter_src_imports` and current transitional prefix

- [ ] **Step 1: Add a failing baseline-ratchet test**

Add to `test_layer_boundaries.py`:

```python
APPLICATION_HTTP_SCHEMA_BASELINE = Path(__file__).with_name(
    "application_http_schema_imports.txt"
)
APPLICATION_HTTP_SCHEMA_PREFIX = "src.entrypoints.http.schemas"


def _application_http_schema_imports() -> set[str]:
    imports: set[str] = set()
    application_root = SRC_ROOT / "application"
    for py_file in application_root.rglob("*.py"):
        for module_name, _line_no in _iter_src_imports(py_file):
            if module_name == APPLICATION_HTTP_SCHEMA_PREFIX or module_name.startswith(
                f"{APPLICATION_HTTP_SCHEMA_PREFIX}."
            ):
                relative = py_file.relative_to(SRC_ROOT).as_posix()
                imports.add(f"{relative}|{module_name}")
    return imports


def _application_http_schema_baseline() -> set[str]:
    return {
        line.strip()
        for line in APPLICATION_HTTP_SCHEMA_BASELINE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def test_application_http_schema_dependency_baseline_is_exact() -> None:
    actual = _application_http_schema_imports()
    expected = _application_http_schema_baseline()

    added = sorted(actual - expected)
    stale = sorted(expected - actual)
    assert not added and not stale, (
        "Application HTTP schema dependency baseline changed.\n"
        f"added={added}\n"
        f"stale={stale}\n"
        "New entries are forbidden; remove stale entries in the same DTO migration."
    )
```

Create the baseline with only this header initially:

```text
# Temporary application -> entrypoints.http.schemas imports.
# New entries are forbidden. Remove entries when DTO migrations delete imports.
```

- [ ] **Step 2: Run the exact test and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/architecture/test_layer_boundaries.py::test_application_http_schema_dependency_baseline_is_exact -q
```

Expected: FAIL with every current import listed under `added=`.

- [ ] **Step 3: Freeze the exact current set**

Add these exact 69 sorted entries after the header in `application_http_schema_imports.txt`. They are the current output of the AST collector and must not be expanded:

```text
application/services/analytics_provenance.py|src.entrypoints.http.schemas.analytics_common
application/services/backtest_attribution_service.py|src.entrypoints.http.schemas.backtest
application/services/backtest_result_summary.py|src.entrypoints.http.schemas.backtest
application/services/backtest_service.py|src.entrypoints.http.schemas.backtest
application/services/chart_service.py|src.entrypoints.http.schemas.chart
application/services/dataset_builder_copy_stages.py|src.entrypoints.http.schemas.job
application/services/dataset_builder_service.py|src.entrypoints.http.schemas.job
application/services/dataset_data_service.py|src.entrypoints.http.schemas.dataset_data
application/services/dataset_service.py|src.entrypoints.http.schemas.dataset
application/services/db_stats_service.py|src.entrypoints.http.schemas.db
application/services/db_validation_payloads.py|src.entrypoints.http.schemas.db
application/services/db_validation_service.py|src.entrypoints.http.schemas.db
application/services/factor_regression_service.py|src.entrypoints.http.schemas.factor_regression
application/services/fundamentals_service.py|src.entrypoints.http.schemas.analytics_common
application/services/fundamentals_service.py|src.entrypoints.http.schemas.fundamentals
application/services/generic_job_manager.py|src.entrypoints.http.schemas.job
application/services/indicator_service.py|src.entrypoints.http.schemas.analytics_common
application/services/intraday_sync_service.py|src.entrypoints.http.schemas.db
application/services/job_manager.py|src.entrypoints.http.schemas.backtest
application/services/job_manager.py|src.entrypoints.http.schemas.common
application/services/job_status.py|src.entrypoints.http.schemas.backtest
application/services/jquants_proxy_service.py|src.entrypoints.http.schemas.jquants
application/services/lab_service.py|src.entrypoints.http.schemas.backtest
application/services/lab_service.py|src.entrypoints.http.schemas.lab
application/services/margin_analytics_service.py|src.entrypoints.http.schemas.analytics_common
application/services/margin_analytics_service.py|src.entrypoints.http.schemas.analytics_margin
application/services/market_data_service.py|src.entrypoints.http.schemas.jquants
application/services/market_data_service.py|src.entrypoints.http.schemas.market_data
application/services/optimization_service.py|src.entrypoints.http.schemas.backtest
application/services/options_225.py|src.entrypoints.http.schemas.jquants
application/services/portfolio_factor_regression_service.py|src.entrypoints.http.schemas.portfolio_factor_regression
application/services/portfolio_performance_service.py|src.entrypoints.http.schemas.portfolio_performance
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
application/services/roe_service.py|src.entrypoints.http.schemas.analytics_common
application/services/roe_service.py|src.entrypoints.http.schemas.analytics_roe
application/services/run_contracts.py|src.entrypoints.http.schemas.backtest
application/services/run_registry.py|src.entrypoints.http.schemas.backtest
application/services/screening_execution.py|src.entrypoints.http.schemas.screening
application/services/screening_job_service.py|src.entrypoints.http.schemas.backtest
application/services/screening_job_service.py|src.entrypoints.http.schemas.screening_job
application/services/screening_response_builder.py|src.entrypoints.http.schemas.analytics_common
application/services/screening_response_builder.py|src.entrypoints.http.schemas.screening
application/services/screening_service.py|src.entrypoints.http.schemas.screening
application/services/screening_strategy_runtime.py|src.entrypoints.http.schemas.screening
application/services/signal_reference_service.py|src.entrypoints.http.schemas.signal_reference
application/services/signal_service.py|src.entrypoints.http.schemas.analytics_common
application/services/sse_manager.py|src.entrypoints.http.schemas.common
application/services/stock_refresh_service.py|src.entrypoints.http.schemas.db
application/services/strategy_authoring_service.py|src.entrypoints.http.schemas.signal_reference
application/services/strategy_authoring_service.py|src.entrypoints.http.schemas.strategy_authoring
application/services/sync_service.py|src.entrypoints.http.schemas.db
application/services/sync_strategies.py|src.entrypoints.http.schemas.db
application/services/verification_orchestrator.py|src.entrypoints.http.schemas.backtest
application/services/watchlist_prices_service.py|src.entrypoints.http.schemas.portfolio_performance
application/workers/backtest_worker.py|src.entrypoints.http.schemas.backtest
application/workers/job_runtime.py|src.entrypoints.http.schemas.common
application/workers/lab_worker.py|src.entrypoints.http.schemas.backtest
application/workers/optimization_worker.py|src.entrypoints.http.schemas.backtest
```

- [ ] **Step 4: Run the architecture suite and verify GREEN**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/architecture -q
```

Expected: all architecture tests pass, including the exact-set ratchet.

- [ ] **Step 5: Document the shrinking-baseline rule**

Add to `docs/bt-src-layering-guide.md` under Dependency Guardrails:

```markdown
### Transitional application schema debt

`application -> entrypoints.http.schemas` is migration debt, not an allowed design direction.
`tests/unit/architecture/application_http_schema_imports.txt` freezes the exact current set.
New entries are forbidden. A DTO migration must remove its stale baseline entries in the same change.
```

Update the validation examples in this document to use root-safe commands:

```bash
uv run --directory apps/bt pytest tests/unit/architecture
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
```

- [ ] **Step 6: Run formatting and focused checks**

Run:

```bash
uv run --directory apps/bt ruff check tests/unit/architecture/test_layer_boundaries.py
uv run --directory apps/bt pytest tests/unit/architecture -q
```

Expected: both commands pass.

- [ ] **Step 7: Commit the architecture ratchet**

```bash
git add apps/bt/tests/unit/architecture/test_layer_boundaries.py apps/bt/tests/unit/architecture/application_http_schema_imports.txt docs/bt-src-layering-guide.md
git commit -m "test(bt): ratchet application schema dependencies"
```

---

### Task 5: Run integrated maintenance verification

**Files:**
- Modify only if a verification failure exposes an in-scope defect

**Interfaces:**
- Consumes: all outputs from Tasks 1-4
- Produces: evidence that governance, contracts, and architecture checks pass together

- [ ] **Step 1: Run repository governance and contract checks**

```bash
python3 scripts/skills/audit_skills.py --strict-legacy
python3 scripts/skills/refresh_skill_references.py --check
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
```

Expected: all commands pass.

- [ ] **Step 2: Run focused Python suites**

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_test_taxonomy.py \
  tests/unit/scripts/test_ci_changed_scope.py \
  tests/unit/scripts/test_audit_skills.py \
  tests/unit/architecture -q
uv run --directory apps/bt ruff check \
  tests/unit/scripts/test_test_taxonomy.py \
  tests/unit/scripts/test_ci_changed_scope.py \
  tests/unit/scripts/test_audit_skills.py \
  tests/unit/architecture/test_layer_boundaries.py
```

Expected: all focused tests and Ruff checks pass.

- [ ] **Step 3: Run TypeScript quality checks**

```bash
bun --cwd apps/ts run --filter @trading25/contracts test
bun --cwd apps/ts run --filter @trading25/api-clients test
bun --cwd apps/ts test packages/web/src/components/Ranking/RankingSummary.test.tsx
bun --cwd apps/ts run quality:typecheck
bun --cwd apps/ts run quality:deps:audit
```

Expected: all tests, typechecks, and dependency audit pass.

- [ ] **Step 4: Inspect the final diff and worktree**

```bash
git diff --check
git status --short
git log --oneline --decorate -5
```

Expected: no whitespace errors; only intended maintenance files are modified or committed; commits are separated by the four reviewable tasks.

- [ ] **Step 5: Request whole-branch review**

Use `superpowers:requesting-code-review` against the complete branch. Address only confirmed in-scope findings, rerun the affected verification commands, and then apply `superpowers:verification-before-completion` before reporting completion.
