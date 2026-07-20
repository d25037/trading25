# Market v5 Provider-Adjusted Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Issue #490 as a breaking Market v5 full-rebuild cutover using J-Quants provider-adjusted daily prices, current-basis fundamentals, PIT valuation, and bounded immutable datasets.

**Architecture:** Preserve the established `stock_data` reader interface but populate it from provider `Adj*`; retain the complete raw provider payload and a compact factor-event ledger. Replace retained basis catalogs and materialized multi-basis valuation with current-basis statement rows and a canonical ASOF valuation view. Advance Market and Dataset contract majors and reject all older physical bundles.

**Tech Stack:** Python 3.12, DuckDB + Parquet, FastAPI/Pydantic, pytest, React 19/TypeScript/Bun, OpenAPI.

## Global Constraints

- Physical Market schema is exactly `5`; adjustment mode is exactly `provider_adjusted_v1`.
- Market v4 is rejected; there is no in-place migration, dual read, compatibility alias, or retained-v4 promotion.
- Retention is derived from `JQUANTS_PLAN` and observed provider coverage; no fixed ten-year day count is embedded.
- Fetch and validation complete before an affected code's atomic replacement; failures preserve its old rows and metadata.
- Tests are written and observed failing before production code for each behavior slice.
- OpenAPI changes require `bun run --filter @trading25/contracts bt:sync`.

---

### Task 1: Market v5 physical and JSON contracts

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/market_schema.py`
- Modify: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Modify: `apps/bt/src/infrastructure/db/market/tables.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Create: `contracts/market-db-schema-v4.json`
- Modify: `contracts/README.md`
- Test: `apps/bt/tests/unit/server/db/test_market_db.py`
- Test: `apps/bt/tests/unit/server/db/test_tables.py`

**Interfaces:** Produces Market schema 5, mode `provider_adjusted_v1`, raw/adjusted columns, provider metadata keys, `stock_adjustment_events`, current-basis statement schema, and an ASOF `daily_valuation` view.

- [ ] Write schema tests asserting v5/mode, exact raw and event-ledger columns, absence of required basis tables, current-basis keys, and v4 rejection.
- [ ] Run the focused tests and confirm they fail because v4 constants and tables are still present.
- [ ] Implement the minimal DDL/constants/JSON contract and duplicated SQLAlchemy table declarations.
- [ ] Run focused tests and schema conformance tests until green.

### Task 2: Provider daily-row normalization and atomic per-code publication

**Files:**
- Modify: `apps/bt/src/application/services/stock_data_row_builder.py`
- Modify: `apps/bt/src/application/services/sync_row_converters.py`
- Modify: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Modify: `apps/bt/src/application/services/stock_refresh_service.py`
- Create: `apps/bt/src/application/services/provider_stock_window.py`
- Test: `apps/bt/tests/unit/server/services/test_stock_data_row_builder.py`
- Test: `apps/bt/tests/unit/server/db/test_time_series_store.py`
- Test: `apps/bt/tests/unit/server/services/test_stock_refresh_service.py`

**Interfaces:** Produces normalized raw+Adj rows, `replace_stock_provider_window(code, rows, coverage, metadata)`, event fingerprints, and atomic replacement results.

- [ ] Add failing tests proving exact `Va`/`Adj*` preservation and rejection of incomplete/non-finite provider-adjusted rows.
- [ ] Add failing store tests for normal append, non-unit event ledger publication, drift detection, coverage pruning, idempotence, and rollback on validation/transaction failure.
- [ ] Implement normalization and one-transaction replacement; make `stock_data` select exact `Adj*` values rather than local cumulative projection.
- [ ] Refactor stock refresh to fetch all pages before calling the atomic replacement and update metadata only after commit.
- [ ] Run all focused row/store/refresh tests until green.

### Task 3: Incremental sync affected-code refresh and provider coverage

**Files:**
- Modify: `apps/bt/src/application/services/sync_stock_data_fetch.py`
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Modify: `apps/bt/src/application/services/sync_publish_helpers.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Modify: `apps/bt/src/application/contracts/market_data_plane.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`

**Interfaces:** Consumes Task 2 drift/event results; produces normal new-date append plus paginated full-window refresh for only affected codes, persisted provider vintage, and no full-code/all-basis materialization stage.

- [ ] Add failing tests for append-only normal dates, factor/corrected-factor/Adj drift detection, affected-code full fetch, pagination failure preservation, and dynamic provider coverage.
- [ ] Implement affected-code collection and refresh orchestration without deleting before fetch.
- [ ] Remove the initial/incremental/repair `adjusted_metrics_pit` full-code stage and publish new progress counters.
- [ ] Run sync tests and assert normal incremental never invokes all-code materialization.

### Task 4: Raw disclosure identity and current-basis fundamentals delta

**Files:**
- Modify: `apps/bt/src/application/services/fins_summary_mapper.py`
- Modify: `apps/bt/src/application/contracts/jquants.py`
- Modify: `apps/bt/src/infrastructure/external_api/clients/jquants_client.py`
- Modify: `apps/bt/src/application/services/sync_fundamentals_data.py`
- Replace behavior in: `apps/bt/src/application/services/adjusted_metrics_materializer.py`
- Modify: `apps/bt/src/domains/fundamentals/adjusted_metrics.py`
- Modify: `apps/bt/src/infrastructure/db/market/valuation_writers.py`
- Test: `apps/bt/tests/unit/server/services/test_fins_summary_mapper.py`
- Test: `apps/bt/tests/unit/domains/fundamentals/test_adjusted_metrics.py`
- Test: `apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py`

**Interfaces:** Produces stable provider disclosure identity, unmodified raw provenance, and `rebuild_current_basis(codes)` that updates only affected disclosures using ledger events after disclosure.

- [ ] Add failing mapper/store tests for disclosure number/time, same-day distinct documents, diluted EPS, period identity, and raw totals.
- [ ] Add failing adjustment tests for split/reverse split, event strictly after disclosure, event before disclosure ignored, share-count inverse adjustment, and totals/ratios unchanged.
- [ ] Implement identity-aware upsert and set-based/current-code recomputation with no retained basis rows.
- [ ] Wire changed statements and Task 2 event changes to affected-code recomputation.
- [ ] Run mapper, domain, materializer, and sync fundamentals tests until green.

### Task 5: Canonical PIT valuation and reader migration

**Files:**
- Modify: `apps/bt/src/infrastructure/data_access/fundamentals_pit_reader.py`
- Modify: `apps/bt/src/application/contracts/fundamentals_pit.py`
- Modify: `apps/bt/src/application/services/fundamentals_service.py`
- Modify: `apps/bt/src/application/services/ranking_fundamental_queries.py`
- Modify: `apps/bt/src/application/services/screening_statement_loader.py`
- Modify: `apps/bt/src/application/services/ranking_liquidity.py`
- Modify: `apps/bt/src/application/services/ranking_valuation.py`
- Modify: `apps/bt/src/application/services/ranking_value_composite_metrics.py`
- Test: `apps/bt/tests/unit/server/db/test_fundamentals_pit_reader.py`
- Test: `apps/bt/tests/unit/server/services/test_fundamentals_adjusted_sot.py`
- Test: `apps/bt/tests/unit/server/services/test_screening_market_loader.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`

**Interfaces:** Consumes the v5 valuation relation; produces current-provider-basis snapshots and preserves effective-date/knowledge-cutoff PIT semantics without `basis_id`.

- [ ] Add failing tests for ASOF valuation equality, no future disclosure, weekend/suspension resolution, and provider-adjusted price use.
- [ ] Refactor the PIT reader and contracts to provider/basis metadata rather than retained basis IDs.
- [ ] Move screening/ranking valuation joins to the canonical view/helper and delete raw×segment projection paths.
- [ ] Run fundamentals, screening, ranking, and representative analytics tests until green.

### Task 6: Dataset v4 immutable provider-vintage bundle

**Files:**
- Create: `contracts/dataset-db-schema-v4.json`
- Create: `contracts/dataset-snapshot-manifest-v4.schema.json`
- Modify: `apps/bt/src/application/services/dataset_builder_copy_stages.py`
- Modify: `apps/bt/src/application/services/dataset_builder_service.py`
- Modify: `apps/bt/src/application/services/dataset_snapshot_selection.py`
- Modify: `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
- Modify: `apps/bt/src/infrastructure/db/dataset_io/snapshot_contract.py`
- Modify: `apps/bt/src/infrastructure/db/dataset_io/pit_validation.py`
- Modify: `apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py`
- Modify: `apps/bt/src/application/contracts/dataset.py`
- Test: `apps/bt/tests/unit/server/db/test_dataset_event_time_basis_snapshot.py`
- Test: `apps/bt/tests/unit/server/test_dataset_snapshot_reader.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service_branches.py`

**Interfaces:** Produces payload schema 4 bundles bounded to pinned effective provider coverage with provider vintage/hash/mode/basis-date manifest fields; rejects v3 bundles.

- [ ] Add failing tests for exact manifest fields, lower and upper coverage bounds, no basis tables, current statement/valuation contract, immutable hashes, and old-bundle rejection.
- [ ] Implement bounded copy from the pinned Market v5 source and the new manifest/logical hash.
- [ ] Remove event-time basis graph reconstruction/validation from the v4 payload path.
- [ ] Run dataset writer, reader, builder, resolver, and API tests until green.

### Task 7: Stats, validation, HTTP, OpenAPI, and TS UI

**Files:**
- Modify: `apps/bt/src/application/services/db_stats_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/db.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Modify: `apps/bt/src/entrypoints/http/routes/fundamentals_error_mapping.py`
- Modify: `apps/ts/packages/web/src/hooks/useDbSync.ts`
- Modify: `apps/ts/packages/web/src/pages/SettingsPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Modify generated: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Modify generated: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`

**Interfaces:** Produces provider-vintage stats/validation, removes standalone adjusted-metrics job endpoints/UI, and updates generated client contracts.

- [ ] Add failing backend and web tests for provider coverage/ledger/freshness status and removal of `adjusted_metrics_pit` recovery/job UI.
- [ ] Implement API/schema/UI changes and regenerate OpenAPI with `bun run --filter @trading25/contracts bt:sync`.
- [ ] Run backend route tests, contract tests, web tests, and TypeScript typecheck until green.

### Task 8: Full-rebuild cutover, runbook, and performance evidence

**Files:**
- Modify: `apps/bt/src/application/services/market_v4_cutover/`
- Modify: `apps/bt/src/entrypoints/cli/market_cutover.py`
- Create: `docs/runbooks/market-v5-cutover.md`
- Modify: `docs/architecture-sot-matrix.md`
- Modify: `AGENTS.md`
- Create: `apps/bt/scripts/benchmark_market_v5_sync.py`
- Test: `apps/bt/tests/unit/server/services/test_market_v4_cutover_*`
- Test: `apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py`

**Interfaces:** Produces v5-only full rebuild/rehearsal/cutover/rollback validation, rejects retained-v4 promotion, and emits reproducible wall/CPU/RSS/request/storage benchmark JSON.

- [ ] Add failing cutover tests for v4 rejection, v5 smoke, immutable backup, atomic activation, exact rollback, provider coverage proof, and retained-v4 ineligibility.
- [ ] Update cutover gates/reports and add the operator runbook.
- [ ] Add a fixture-driven benchmark test showing normal incremental work scales with new rows and no all-code materializer is invoked.
- [ ] Run representative live/local benchmark when credentials and a representative DB are available; otherwise commit the reproducible harness and recorded fixture evidence.

### Task 9: Whole-repository verification and publication

- [ ] Run `uv run pytest -q` in `apps/bt` and confirm zero failures.
- [ ] Run `uv run ruff check src tests` and `uv run pyright src` in `apps/bt`.
- [ ] Run `bun run workspace:test`, `bun run quality:typecheck`, and `bun run quality:lint` in `apps/ts`.
- [ ] Run contract sync a final time and confirm a clean generated diff.
- [ ] Re-read Issue #490 and verify every acceptance criterion against tests, code, or benchmark/runbook evidence.
- [ ] Request whole-branch code review, fix all Critical/Important findings, and rerun verification.
- [ ] Commit only scoped files, push `codex/market-v5-cutover`, and open a draft PR referencing `Closes #490`.
