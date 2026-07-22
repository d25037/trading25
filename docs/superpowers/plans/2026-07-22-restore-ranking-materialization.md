# Restore Ranking Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore fast Daily Ranking reads by materializing technical metrics after market sync, retaining bounded DuckDB file growth, and removing production request-time full-history validation.

**Architecture:** `stock_data` remains the provider-adjusted consumer SoT. Initial and incremental sync explicitly reconcile `daily_technical_metrics` before terminal finalization; incremental finalization then applies the existing free-block compaction policy. Production Ranking reads bounded `stock_data` SQL plus date/code-scoped materialized technical rows, while the research-only event-time builder remains available outside the page API.

**Tech Stack:** Python 3.12, FastAPI, DuckDB, pandas, pytest, ruff, pyright

## Global Constraints

- Preserve Market v5 provider-adjusted/current-basis contracts.
- Do not restore Market v4 adjustment bases or persistent `daily_valuation` tables.
- Keep implementation minimal: no full Ranking snapshot table and no new compatibility layer.
- Use TDD and verify the real `market.duckdb` latency and free-block statistics.
- Preserve the existing semantic-delta writer and finalizer compaction safety boundary.

---

### Task 1: Restore the sync materialization stage

**Files:**
- Modify: `apps/bt/src/application/services/sync_service.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`

**Interfaces:**
- Consumes: `MarketDb.rebuild_daily_technical_metrics_from_stock_data() -> TechnicalMetricRebuildResult`
- Produces: a successful sync that materializes technical metrics before terminal finalization

- [ ] **Step 1: Write failing sync tests**

Add initial and incremental sync tests whose fake market DB records `rebuild_daily_technical_metrics_from_stock_data` exactly once after strategy execution and before finalization. Add a failure test proving materialization failure makes the sync terminal status failed.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/unit/server/services/test_sync_service.py -k technical_materialization -v`

Expected: FAIL because sync never invokes the technical materializer.

- [ ] **Step 3: Implement the explicit stage**

After successful strategy execution and before `finalize_market_operation_joined`, call the technical rebuild in `asyncio.to_thread`. Publish a `daily_technical_metrics` progress stage. Do not run it for failed/cancelled syncs.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `uv run pytest tests/unit/server/services/test_sync_service.py -k technical_materialization -v`

Expected: PASS.

### Task 2: Preserve bounded DuckDB growth

**Files:**
- Modify if required: `apps/bt/src/infrastructure/db/market/technical_metric_writers.py`
- Modify if required: `apps/bt/src/infrastructure/db/market/market_compaction.py`
- Test: `apps/bt/tests/unit/server/db/test_technical_metric_differential.py`
- Test: `apps/bt/tests/unit/server/db/test_market_compaction.py`
- Test: `apps/bt/tests/unit/server/db/test_market_growth_acceptance.py`

**Interfaces:**
- Consumes: `TechnicalMetricRebuildResult.stats` and `MarketCompactor.maintain()`
- Produces: idempotent semantic-delta materialization followed by threshold-based compaction on incremental sync

- [ ] **Step 1: Add regression coverage for repeated materialization**

Prove a second unchanged rebuild performs zero insert/update/delete mutations and preserves row counts. Prove changed prices update only affected semantic rows. Prove sync finalization observes free-block thresholds after materialization.

- [ ] **Step 2: Run tests to verify current behavior and any RED gap**

Run: `uv run pytest tests/unit/server/db/test_technical_metric_differential.py tests/unit/server/db/test_market_compaction.py tests/unit/server/db/test_market_growth_acceptance.py -v`

Expected: any new regression test that exposes a missing lifecycle link fails.

- [ ] **Step 3: Keep the historical mitigation intact**

Use the existing desired temp relation plus semantic delta rather than delete-all/insert-all. Retain the existing compaction thresholds: soft at 512 MiB and 10%, hard at 1 GiB. Ensure the rebuild occurs before writable handles close so incremental sync finalization can inspect and compact the resulting free blocks.

- [ ] **Step 4: Run tests to verify GREEN**

Run the same command and require zero failures.

### Task 3: Restore the bounded production Ranking read path

**Files:**
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Modify: `apps/bt/src/application/services/ranking_query_helpers.py`
- Modify: `apps/bt/src/application/services/ranking_daily_queries.py`
- Modify: `apps/bt/src/application/services/ranking_daily_technical_metrics.py`
- Modify: `apps/bt/src/application/services/ranking_technical_flags.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`

**Interfaces:**
- Consumes: bounded `stock_data`, `stock_master_daily`, `daily_technical_metrics`, and Market v5 `daily_valuation`
- Produces: unchanged Ranking response contract without request-time `stock_data_raw` scans, SHA256 lineage audit, pandas transfer, or temporary relation registration

- [ ] **Step 1: Write failing production-path tests**

Add tests proving `RankingService.get_rankings` never calls `query_dataframe` or `temporary_in_memory_relation`, never includes `stock_data_raw`/`sha256` in SQL, never reads after the requested date, and reads technical metrics only from the materialized table.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/unit/server/services/test_ranking_service.py -k 'bounded or materialized or event_time' -v`

Expected: FAIL on the current request-time event-time path.

- [ ] **Step 3: Restore pre-regression bounded queries**

Restore the `2557af47^` production query structure with current 5/6-digit code normalization and Market v5 `stock_master_daily` semantics. Remove only the production dependency on `event_time_signal_sql`; retain the research domain implementation.

- [ ] **Step 4: Remove technical fallback recomputation**

Make `ranking_daily_technical_metrics` perform only a target-date and selected-code lookup. Missing materialized rows remain missing and must be recovered through market sync rather than recomputed in GET.

- [ ] **Step 5: Run focused tests to verify GREEN**

Run the same focused test command and require zero failures.

### Task 4: Real-data verification

**Files:**
- No source changes

**Interfaces:**
- Consumes: local Market v5 `market.duckdb`
- Produces: measured evidence for materialization, latency, API responsiveness, and bounded free space

- [ ] **Step 1: Run backend quality gates**

Run: `uv run pytest tests/unit/server/services/test_sync_service.py tests/unit/server/services/test_ranking_service.py tests/unit/server/db/test_technical_metric_differential.py tests/unit/server/db/test_market_compaction.py -q`

Run: `uv run ruff check src tests/unit/server/services/test_sync_service.py tests/unit/server/services/test_ranking_service.py`

Run: `uv run pyright src`

- [ ] **Step 2: Materialize the local initial-sync database once**

Run the restored production materializer against the local DB, then inspect `daily_technical_metrics` row count/max date and `PRAGMA database_size` free blocks.

- [ ] **Step 3: Measure Ranking and health latency**

Start FastAPI, time `/api/analytics/ranking` with the normal page parameters, and call `/api/health` concurrently. Confirm Ranking no longer creates multi-gigabyte request memory or blocks health checks.

- [ ] **Step 4: Repeat the materializer and inspect bloat**

Run it again unchanged. Confirm semantic mutations are zero and compare file size/free blocks before and after. If free space crosses the production policy, run the normal finalizer/compactor path and verify a compacted, validated replacement.
