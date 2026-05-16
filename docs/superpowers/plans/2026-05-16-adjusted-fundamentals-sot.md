# Adjusted Fundamentals SoT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make adjusted price, adjusted per-share fundamentals, and daily valuation the canonical DB-served data for Trading25 analytics while preserving J-Quants raw data for provenance and replay.

**Architecture:** Keep `stock_data_raw` and `statements` as raw vendor provenance. Add materialized DuckDB tables for adjusted statement metrics and daily valuation, generated inside `apps/bt` from raw tables plus `stock_data_raw.adjustment_factor` events. Ranking, Symbol Workbench, Screening, and research loaders should consume the adjusted tables first, with raw statement access reserved for official-history display and diagnostics.

**Tech Stack:** Python 3.12, DuckDB, FastAPI, Pydantic, pandas, uv, pytest, ruff, pyright, OpenAPI generated TypeScript contracts.

---

## Scope Decisions

- Raw J-Quants data remains stored and inspectable. Do not delete `stock_data_raw` or raw `statements`.
- `stock_data` remains the adjusted price series SoT.
- New adjusted fundamentals must be derived from actual split/reverse-split adjustment events, not arbitrary `shares_outstanding` changes.
- Official EPS/BPS history displays can still expose raw fields, but valuation/ranking/screening/research should default to adjusted DB tables.
- This is a breaking SoT migration in behavior, but it should be implemented as additive schema first, then consumer cutover.

## File Map

- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
  - Create tables, indexes, inspect helpers, and upsert/query helpers for adjusted metrics.
- Create: `apps/bt/src/domains/fundamentals/adjusted_metrics.py`
  - Pure domain logic for statement adjustment and daily valuation generation.
- Modify: `apps/bt/src/domains/fundamentals/calculator.py`
  - Keep current calculation semantics, but delegate shared adjustment math to `adjusted_metrics.py`.
- Modify: `apps/bt/src/application/services/fundamentals_service.py`
  - Prefer adjusted DB tables for `dailyValuation` and latest valuation.
- Modify: `apps/bt/src/application/services/ranking_service.py`
  - Replace per-request valuation recomputation with adjusted DB lookup.
- Modify: `apps/bt/src/application/services/screening_market_loader.py`
  - Load adjusted fundamentals/valuation when screening needs valuation fields.
- Modify: `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
  - Copy adjusted tables into snapshot bundles.
- Modify: `apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py`
  - Expose adjusted tables from dataset snapshots for archived reproducibility.
- Modify: `apps/bt/src/entrypoints/http/schemas/fundamentals.py`
  - Add provenance/basis fields without removing existing response fields.
- Modify: `apps/bt/src/application/services/db_stats_service.py`
  - Include adjusted table row counts and basis freshness.
- Modify: `apps/bt/src/application/services/db_validation_service.py`
  - Warn when adjusted tables are stale or missing while raw source tables exist.
- Modify: `docs/architecture-sot-matrix.md`
  - Declare adjusted fundamentals and daily valuation as consumer-facing SoT.
- Modify: `docs/market-duckdb-sot-v3-plan.md`
  - Document migration behavior and reset/rebuild expectations.
- Test: `apps/bt/tests/unit/domains/fundamentals/test_adjusted_metrics.py`
- Test: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`
- Test: `apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_adjusted_valuation.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`

---

### Task 1: Lock The Adjustment Contract With Domain Tests

**Files:**
- Create: `apps/bt/tests/unit/domains/fundamentals/test_adjusted_metrics.py`
- Create: `apps/bt/src/domains/fundamentals/adjusted_metrics.py`

- [ ] **Step 1: Write failing tests for split-based per-share adjustment**

Test cases:
- 2-for-1 split event adjusts older EPS/BPS onto the current adjusted-price basis.
- Reverse split adjusts in the opposite direction.
- Plain `shares_outstanding` change with no `adjustment_factor != 1` event does not adjust EPS/BPS.
- Negative or zero EPS/BPS remains available as raw/adjusted value, but valuation ratios return `None`.

Minimal test shape:

```python
from src.domains.fundamentals.adjusted_metrics import (
    AdjustedStatementInput,
    build_adjusted_statement_metric,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


def test_split_event_adjusts_eps_without_using_share_count_change() -> None:
    metric = build_adjusted_statement_metric(
        AdjustedStatementInput(
            code="9880",
            disclosed_date="2023-05-10",
            period_end="2023-03-31",
            period_type="FY",
            eps=100.0,
            bps=1000.0,
            forecast_eps=120.0,
            dividend_fy=30.0,
            shares_outstanding=10_000_000.0,
        ),
        events=[ShareAdjustmentEvent(date="2024-01-01", adjustment_factor=0.5)],
        price_basis_date="2024-12-30",
    )

    assert metric.adjusted_eps == 50.0
    assert metric.adjusted_bps == 500.0
    assert metric.adjusted_forecast_eps == 60.0
    assert metric.adjustment_factor_cumulative == 0.5
```

- [ ] **Step 2: Run the focused test and confirm it fails**

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/fundamentals/test_adjusted_metrics.py -q
```

Expected: import failure for `src.domains.fundamentals.adjusted_metrics`.

- [ ] **Step 3: Implement the pure domain module**

Create dataclasses for:
- `AdjustedStatementInput`
- `AdjustedStatementMetric`
- `DailyValuationInput`
- `DailyValuationMetric`

Use `cumulative_adjustment_factor_after()` from `src.shared.utils.share_adjustment` as the only split adjustment source. Do not infer split ratio from `shares_outstanding`.

- [ ] **Step 4: Run the focused domain tests**

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/fundamentals/test_adjusted_metrics.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/fundamentals/adjusted_metrics.py apps/bt/tests/unit/domains/fundamentals/test_adjusted_metrics.py
git commit -m "test(bt): lock adjusted fundamentals semantics"
```

---

### Task 2: Add DuckDB Tables And Store Helpers

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Test: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`

- [ ] **Step 1: Add failing DB schema tests**

Assert a new market DB contains:
- `statement_metrics_adjusted`
- `daily_valuation`

Expected core columns:

```text
statement_metrics_adjusted:
  code, disclosed_date, period_end, period_type, price_basis_date,
  raw_eps, adjusted_eps, raw_bps, adjusted_bps,
  raw_forecast_eps, adjusted_forecast_eps,
  raw_dividend_fy, adjusted_dividend_fy,
  adjustment_factor_cumulative, basis_version, created_at

daily_valuation:
  code, date, price_basis_date, close,
  eps, bps, forward_eps, per, forward_per, pbr,
  market_cap, free_float_market_cap,
  statement_disclosed_date, forward_eps_disclosed_date,
  forward_eps_source, basis_version, created_at
```

- [ ] **Step 2: Run the schema test and confirm it fails**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Expected: missing table failure.

- [ ] **Step 3: Add table creation and indexes**

In `MarketDB._ensure_tables()`, create both tables. Use additive schema only. Keep existing v3 compatibility behavior; do not mark old DBs incompatible only because the new adjusted tables are absent.

Recommended primary keys:

```sql
PRIMARY KEY (code, disclosed_date, period_end, period_type, basis_version)
PRIMARY KEY (code, date, basis_version)
```

Recommended indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_statement_metrics_adjusted_code_disclosed
ON statement_metrics_adjusted(code, disclosed_date);

CREATE INDEX IF NOT EXISTS idx_daily_valuation_date_code
ON daily_valuation(date, code);
```

- [ ] **Step 4: Add upsert/query helpers**

Add methods to `MarketDB`:
- `upsert_statement_metrics_adjusted(rows: list[dict[str, Any]]) -> int`
- `upsert_daily_valuation(rows: list[dict[str, Any]]) -> int`
- `get_adjusted_statement_metrics(code: str, as_of_date: str | None = None) -> list[dict[str, Any]]`
- `get_daily_valuation(code: str, start: str | None = None, end: str | None = None) -> list[dict[str, Any]]`
- `get_daily_valuation_for_codes(codes: list[str], date: str) -> list[dict[str, Any]]`
- `get_adjusted_metrics_snapshot() -> dict[str, Any]`

- [ ] **Step 5: Run DB tests**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add apps/bt/src/infrastructure/db/market/market_db.py apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py
git commit -m "feat(bt): add adjusted fundamentals tables"
```

---

### Task 3: Materialize Adjusted Metrics From Raw Tables

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Create: `apps/bt/src/application/services/adjusted_metrics_materializer.py`
- Test: `apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py`

- [ ] **Step 1: Write materializer tests**

Cover:
- raw `statements` + `stock_data_raw.adjustment_factor` generate adjusted statement metrics.
- `daily_valuation` uses adjusted `stock_data.close`, not raw close.
- `daily_valuation` chooses statement rows with `disclosed_date <= valuation date`.
- future disclosed statement rows are excluded.
- rebuild is idempotent for the same `basis_version`.

- [ ] **Step 2: Implement materializer service**

Create `AdjustedMetricsMaterializer` with:

```python
class AdjustedMetricsMaterializer:
    def __init__(self, market_db: MarketDB) -> None: ...

    def rebuild_all(self) -> AdjustedMetricsBuildResult: ...

    def rebuild_codes(self, codes: list[str]) -> AdjustedMetricsBuildResult: ...
```

`basis_version` should be deterministic:

```text
adjusted-v1:{price_basis_date}
```

where `price_basis_date` is the latest available `stock_data.date`.

- [ ] **Step 3: Integrate with sync publish path**

After these sync stages publish raw/adjusted sources, rebuild affected codes:
- `stock_data`
- `statements`
- `stock_data_raw` adjustment events

Keep rebuild scoped to changed codes when possible. For `initial reset`, run full rebuild after source publish completes.

- [ ] **Step 4: Run materializer tests**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py -q
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/services/adjusted_metrics_materializer.py apps/bt/src/infrastructure/db/market/market_db.py apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py
git commit -m "feat(bt): materialize adjusted valuation data"
```

---

### Task 4: Surface Adjusted Metrics In DB Stats And Validation

**Files:**
- Modify: `apps/bt/src/application/services/db_stats_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Test: `apps/bt/tests/unit/server/services/test_db_stats_service.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`

- [ ] **Step 1: Add tests for adjusted table health**

Expected stats fields:
- `adjustedMetrics.statementRows`
- `adjustedMetrics.dailyValuationRows`
- `adjustedMetrics.priceBasisDate`
- `adjustedMetrics.basisVersion`

Expected validation behavior:
- raw source present + adjusted tables empty = actionable warning.
- adjusted basis older than latest `stock_data.date` = actionable warning.
- raw source absent + adjusted tables empty = informational diagnostic, not blocking.

- [ ] **Step 2: Implement stats and validation fields**

Use `MarketDB.get_adjusted_metrics_snapshot()` as the single source. Keep response additive so current web clients continue to work.

- [ ] **Step 3: Run focused tests**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_db_stats_service.py apps/bt/tests/unit/server/services/test_db_validation_service.py -q
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add apps/bt/src/application/services/db_stats_service.py apps/bt/src/application/services/db_validation_service.py apps/bt/src/entrypoints/http/schemas/db.py apps/bt/tests/unit/server/services/test_db_stats_service.py apps/bt/tests/unit/server/services/test_db_validation_service.py
git commit -m "feat(bt): validate adjusted metrics freshness"
```

---

### Task 5: Switch Fundamentals API To Adjusted DB SoT

**Files:**
- Modify: `apps/bt/src/application/services/fundamentals_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/fundamentals.py`
- Test: `apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py`

- [ ] **Step 1: Add tests for adjusted-table preference**

Test behavior:
- `dailyValuation` is read from `daily_valuation` when available.
- `latestMetrics.per`, `latestMetrics.pbr`, and adjusted EPS/BPS fields match adjusted DB rows.
- raw `data[].eps` remains official raw EPS for statement history.
- response includes `valuationBasisVersion` and `priceBasisDate`.

- [ ] **Step 2: Implement adjusted DB lookup**

In `FundamentalsService.compute()`, replace direct `_calculate_daily_valuation()` for normal market DB path with `MarketDB.get_daily_valuation()`. Keep calculator fallback only for tests or missing adjusted table diagnostics during transition.

- [ ] **Step 3: Keep raw official history explicit**

Do not replace `data[].eps` and `data[].bps` with adjusted values. Keep adjusted fields in `adjustedEps`, `adjustedBps`, and `adjustedForecastEps`.

- [ ] **Step 4: Run focused tests**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py apps/bt/tests/unit/server/routes/test_routes_analytics_fundamentals.py -q
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/services/fundamentals_service.py apps/bt/src/entrypoints/http/schemas/fundamentals.py apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py
git commit -m "feat(bt): serve fundamentals valuation from adjusted tables"
```

---

### Task 6: Switch Ranking And Screening Consumers

**Files:**
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Modify: `apps/bt/src/application/services/screening_market_loader.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_adjusted_valuation.py`
- Test: existing screening service tests under `apps/bt/tests/unit/server/services/`

- [ ] **Step 1: Add Ranking tests**

Cover:
- Ranking enrichment reads `daily_valuation` by `(code, target_date)`.
- `forwardEpsDisclosedDate` and `forwardEpsSource` come from adjusted valuation rows.
- Future disclosed rows are not used.
- If adjusted valuation is missing for one code, that code does not get recomputed from raw statements silently.

- [ ] **Step 2: Replace per-code valuation recompute**

In `RankingService._enrich_ranking_collections_with_valuation()` and value-composite loading, use batched `get_daily_valuation_for_codes()` instead of constructing `JQuantsStatement` objects per code.

- [ ] **Step 3: Switch screening loader for valuation requirements**

When screening strategy requirements need valuation fields, load from adjusted metrics tables. Keep raw statement loading for fundamental signals that explicitly need raw accounting fields.

- [ ] **Step 4: Run focused tests**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_ranking_adjusted_valuation.py apps/bt/tests/unit/server/services -q
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/services/ranking_service.py apps/bt/src/application/services/screening_market_loader.py apps/bt/tests/unit/server/services/test_ranking_adjusted_valuation.py
git commit -m "feat(bt): use adjusted valuation in analytics consumers"
```

---

### Task 7: Include Adjusted Tables In Dataset Snapshots

**Files:**
- Modify: `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
- Modify: `apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py`
- Test: dataset writer/reader tests under `apps/bt/tests/unit/server/db/`

- [ ] **Step 1: Add dataset copy tests**

Assert `dataset.duckdb` contains:
- `statement_metrics_adjusted`
- `daily_valuation`

when source `market.duckdb` contains them.

- [ ] **Step 2: Copy adjusted tables during dataset creation**

Add adjusted tables to the DuckDB direct-copy path. Preserve current partial snapshot behavior: if source adjusted tables are missing, dataset validation should warn rather than create synthetic adjusted data inside dataset writer.

- [ ] **Step 3: Expose reader helpers**

Add dataset reader methods mirroring market reader methods needed by archived backtests/research.

- [ ] **Step 4: Run dataset tests**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/db -q
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py apps/bt/tests/unit/server/db
git commit -m "feat(bt): include adjusted metrics in dataset snapshots"
```

---

### Task 8: Update Contracts, Web Types, And Docs

**Files:**
- Modify: `apps/bt/src/entrypoints/http/schemas/fundamentals.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Generated: `apps/ts/packages/contracts/src/generated/*`
- Modify: `docs/architecture-sot-matrix.md`
- Modify: `docs/market-duckdb-sot-v3-plan.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Add API schema fields**

Add additive response fields:
- `priceBasisDate`
- `valuationBasisVersion`
- `adjustedMetricsSource: "daily_valuation" | "computed_fallback"`

Do not remove current fields in this phase.

- [ ] **Step 2: Run OpenAPI contract generation**

```bash
bun run --cwd apps/ts --filter @trading25/contracts bt:sync
```

Expected: generated TypeScript contract diff only.

- [ ] **Step 3: Update docs**

Document:
- raw provenance SoT: `stock_data_raw`, `statements`
- consumer SoT: `stock_data`, `statement_metrics_adjusted`, `daily_valuation`
- official-history exception: raw EPS/BPS display remains allowed when explicitly labeled
- rebuild trigger: initial/incremental sync updates adjusted metrics after source publish

- [ ] **Step 4: Commit**

```bash
git add apps/bt/src/entrypoints/http/schemas/fundamentals.py apps/bt/src/entrypoints/http/schemas/db.py apps/ts/packages/contracts docs/architecture-sot-matrix.md docs/market-duckdb-sot-v3-plan.md AGENTS.md
git commit -m "docs: declare adjusted fundamentals as consumer SoT"
```

---

### Task 9: Full Validation And Live Data Rebuild

**Files:**
- No code changes expected unless validation exposes defects.

- [ ] **Step 1: Run backend quality gates**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/db apps/bt/tests/unit/server/services apps/bt/tests/unit/domains/fundamentals -q
uv run --project apps/bt ruff check src/domains/fundamentals src/infrastructure/db src/application/services src/entrypoints/http
uv run --project apps/bt pyright src/domains/fundamentals src/infrastructure/db src/application/services src/entrypoints/http
```

Expected: pass.

- [ ] **Step 2: Run TS contract checks**

```bash
bun run --cwd apps/ts quality:typecheck
bun run --cwd apps/ts quality:lint
```

Expected: pass.

- [ ] **Step 3: Rebuild local adjusted tables**

Use the implementation command added by Task 3. If no CLI command was added, trigger an `initial` or `incremental` DB sync that calls the materializer.

Expected DB validation:
- adjusted statement rows > 0
- daily valuation rows > 0
- adjusted basis date equals latest `stock_data` date
- no actionable adjusted-metrics warning

- [ ] **Step 4: Live UI smoke**

Verify:
- Market DB page shows adjusted metrics freshness.
- Ranking page still shows PER/PBR/forward PER.
- Symbol Workbench for `9880` shows raw official EPS history while valuation uses adjusted fields.

- [ ] **Step 5: Final commit if validation fixes were needed**

```bash
git add .
git commit -m "fix(bt): complete adjusted metrics migration"
```

---

## Rollout Order

1. Implement Tasks 1-4 behind additive schema and validation.
2. Rebuild local adjusted tables and compare old vs new Ranking/Fundamentals outputs.
3. Implement Tasks 5-6 consumer cutover.
4. Implement Task 7 dataset snapshot support.
5. Implement Task 8 docs/contracts.
6. Run Task 9 full validation.

## Risk Controls

- Keep raw tables untouched until adjusted consumers are stable.
- Do not infer split adjustments from `shares_outstanding`.
- Treat adjusted table absence as a warning during the additive phase; make it blocking only after consumer cutover is complete.
- Keep a small symbol regression set: `9880` for non-split share-count changes, plus at least one known split and one reverse split symbol from local `stock_data_raw.adjustment_factor` events.
- Preserve PIT ordering: filter by target/as-of date before choosing latest statement rows.

## Completion Criteria

- `daily_valuation` is the default source for PER/PBR/forward PER in Ranking and Symbol Workbench.
- `statement_metrics_adjusted` is the default source for adjusted EPS/BPS/forecast EPS in API consumers.
- raw EPS/BPS remains accessible and explicitly labeled for official statement history.
- `/api/db/stats` and `/api/db/validate` expose adjusted table freshness.
- Dataset snapshots include adjusted metrics for reproducible archived runs.
- Focused backend tests, backend lint/typecheck, TS typecheck/lint, and live UI smoke all pass.
