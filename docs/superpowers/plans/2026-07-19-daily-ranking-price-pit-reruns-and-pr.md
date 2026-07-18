# Daily Ranking Price-PIT Reruns and PR Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the two earlier Daily Ranking studies onto the same audited `stock_data_raw` event-time price path as Technical Fit v6, republish immutable canonical bundles, then push the complete branch and open a draft PR.

**Architecture:** Reuse `create_event_time_price_relations()` and inject its signal/outcome relations into `create_daily_ranking_research_panel`, ATR, long-sector leadership, and OLS consumers. Each study keeps its frozen cohort, horizon, bootstrap, and adoption thresholds; only price provenance and publication lineage change. Immutable old bundles remain untouched and the new canonical README values must come from independently validated reruns.

**Tech Stack:** Python 3.12, DuckDB, pandas, pytest, ruff, pyright, Git/GitHub CLI.

## Global Constraints

- Universe is exact signal-date Prime equivalent only: pre-reorg `0101`, post-reorg `0111`; Standard and Growth are excluded.
- Require Market v4 and `stock_price_adjustment_mode=local_projection_v2_event_time`; fail closed.
- Stock price source is `stock_data_raw`; no `stock_data` fallback is allowed for the two canonical reruns.
- Signal features use exact signal-date basis over the complete lookback; outcomes use exact completion-date basis on both endpoints.
- Reuse the Technical Fit price projection/audit implementation; do not duplicate adjustment logic.
- Frozen research cohorts, horizons, bootstrap settings, and decision thresholds do not change.
- Previous bundles remain immutable and are documented as superseded for price-lineage hardening.
- Publication SoT is each experiment README `## Published Readout`; artifact values must be verified from `results.duckdb`, `summary.md`, and `manifest.json`.

---

### Task 1: Trend Acceleration PIT Migration and Rerun

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify if needed: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes: `create_event_time_price_relations()` and its `signal_features`, `forward_outcomes`, and audit payload.
- Produces: an immutable price-PIT canonical bundle and a readout whose decision and metrics match that bundle.

- [ ] Add regression tests proving poisoned `stock_data` cannot change the study and missing/overlapping/invalid price lineage fails closed.
- [ ] Inject event-time relations into Daily Ranking Base, ATR, long leadership, and trend-slope inputs without changing frozen study semantics.
- [ ] Persist price projection policy, counts, hashes, and no-fallback evidence in manifest/summary.
- [ ] Run focused and adjacent tests, ruff, pyright, runner help, and diff checks; commit the code migration.
- [ ] Execute a new immutable bundle, validate Prime/PIT scope and all headline tables, update README/catalog with actual values, and commit publication.

### Task 2: Fixed Return Priority PIT Migration and Rerun

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md`
- Modify if needed: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes: the same audited event-time price relations and price audit contract as Task 1.
- Produces: an immutable price-PIT canonical bundle and a readout whose decision and metrics match that bundle.

- [ ] Add regression tests for poisoned convenience prices, completion-basis outcomes, and fail-closed price lineage.
- [ ] Inject event-time relations into Daily Ranking Base, ATR, long leadership, and fixed-return observation construction while preserving fixed-free membership.
- [ ] Persist price projection policy, counts, hashes, and no-fallback evidence in manifest/summary.
- [ ] Run focused and adjacent tests, ruff, pyright, runner help, and diff checks; commit the code migration.
- [ ] Execute a new immutable bundle, validate Prime/PIT scope and all headline tables, update README/catalog with actual values, and commit publication.

### Task 3: Whole-Branch Verification and Draft PR

**Files:**
- Modify only if verification finds a real defect.

**Interfaces:**
- Consumes: both newly published canonical bundles plus Technical Fit v6.
- Produces: a pushed `codex/ranking-technical-fit-score` branch and a draft PR to `main`.

- [ ] Run the full affected Daily Ranking research test set, adjacent shared-default tests, ruff, pyright, both runner `--help` commands, research guardrails, and `git diff --check`.
- [ ] Independently verify all three canonical readouts against their live immutable bundles and confirm no uncommitted work remains.
- [ ] Obtain a whole-branch Critical/Important review and fix/re-review every blocking finding.
- [ ] Inspect the complete `main...HEAD` diff and commits, push the branch to `origin`, and open a draft PR with scope, rationale, PIT root cause, results, validation, and known unrelated guardrail status.
