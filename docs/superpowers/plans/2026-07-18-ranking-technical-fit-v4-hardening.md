# Ranking Technical Fit v4 Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Technical Fit research price inputs fully Market-v4 event-time PIT, enforce the frozen same-near replication gate per OOS period, and make its publication contract hermetic in CI before publishing immutable v4.

**Architecture:** Build Technical-Fit-only raw-price projection relations keyed by exact event-time basis and inject their precomputed signal features through opt-in parameters, leaving all shared research defaults on their existing paths. Signal features use the signal-date basis across each full lookback; realized outcomes select the completion-date basis and apply it to both endpoints. Shape-slice evidence is stored as additional `segment_stability` analyses so the exact 15-table bundle contract remains unchanged.

**Tech Stack:** Python 3.12, DuckDB SQL, pandas, pytest, ruff, pyright, runner-first research bundles.

## Global Constraints

- Physical price source is `stock_data_raw`; Technical Fit must never fall back to convenience `stock_data`.
- Adjusted OHLC is raw OHLC multiplied by `cumulative_factor`; adjusted volume is `ROUND(raw.volume / cumulative_factor)`.
- Signal features use the exact basis valid on the signal date for every lookback row.
- Realized outcomes use the exact basis valid on the completion date for both signal and completion endpoints.
- Every relevant raw row must have exactly one covering segment before separately requiring a finite positive factor.
- Existing shared research defaults remain unchanged.
- The exact 15-table bundle contract and every frozen gate threshold remain unchanged.
- v1-v3 bundles remain immutable; v4 run id is `20260718_prime_pit_technical_fit_shape_v4`.
- `.superpowers/sdd/*` reports are not edited or staged.

---

### Task 1: Event-Time Price Projection and Audit

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py`
- Modify: `apps/bt/src/domains/analytics/ranking_color_evidence.py`
- Modify: `apps/bt/src/domains/analytics/daily_ranking_research_base.py`
- Modify: `apps/bt/src/domains/analytics/atr_expansion_forward_response.py`
- Modify: `apps/bt/src/domains/analytics/ranking_long_sector_leadership_horizon_decomposition.py`
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Produces `EventTimePriceRelations(signal_features, forward_outcomes)` and `EventTimePriceAudit`.
- `create_event_time_signal_price_relations(...)` materializes normalized raw, exact signal bases, `(code,basis_id,date)` projections, rolling signal features, and audit metadata.
- `create_event_time_forward_outcomes(...)` materializes candidate outcomes with completion-date basis applied to both endpoints.
- Shared builders accept optional fixed relation names; `None` preserves existing SQL.

- [ ] Add fixture `stock_data_raw`, basis/segment regimes, poisoned `stock_data`, split, missing/overlap/invalid factor, and future-basis mutations.
- [ ] Run price-focused tests and observe failures caused by current `stock_data` reads or absent projection functions.
- [ ] Implement normalized raw alias audit and exact signal-basis selection.
- [ ] Implement basis-price projection and rolling ADV/return/ATR/OLS/leadership features partitioned by `(code,basis_id)`.
- [ ] Implement completion-session outcomes using one completion basis for both endpoints.
- [ ] Add opt-in relation branches to Daily Ranking, ATR, and long leadership; rewire Technical Fit OLS/candidate outcomes.
- [ ] Persist feature/outcome row, basis, segment counts and ordered hashes plus timing policies and `no_stock_data_fallback=true`.
- [ ] Run all price regressions until GREEN.

### Task 2: Same-Near Period Shape Gate

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- `_build_oos_shape_gate_rows(daily, mapping)` emits `segment_stability` rows with `analysis=raw_shape_pair_gate`, one near ring per required OOS period.
- `mean_effect_pct` stores the minimum core/near selected-bin lift; `median_effect_pct` stores the maximum core/near severe-loss deterioration; `positive_date_rate_pct` is 100 only when lift is positive and downside is at most 1 pp.
- A score passes only when the same near ring has passing rows in both required periods.

- [ ] Add Simpson-conflict RED tests for different near winners across periods and pooled severe loss masking a +2 pp period.
- [ ] Remove misleading `oos_*` flags from `raw_shape_summary`.
- [ ] Append auditable pair-period rows to `segment_stability` without adding a table.
- [ ] Make shape classification and family adoption require the frozen same-near score gate.
- [ ] Run focused gate tests until GREEN.

### Task 3: Hermetic Publication Contract

**Files:**
- Create: `apps/bt/tests/fixtures/research/ranking_technical_fit_score_shape_evidence_published_digest.json`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Normal unit test reads only README, catalog/index, and the committed digest.
- Integration test reads `TRADING25_VERIFY_PUBLISHED_RESEARCH_ROOT`; without it, the test skips.

- [ ] Add committed v3 digest and change the standard publication test to compare README fields to it.
- [ ] Move DuckDB/manifest queries into an integration-marked opt-in test.
- [ ] Run the standard test with no local artifact path and confirm GREEN; run the opt-in test against the current v3 root.

### Task 4: Immutable v4 Publication

**Files:**
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/tests/fixtures/research/ranking_technical_fit_score_shape_evidence_published_digest.json`

**Interfaces:**
- The first commit contains Tasks 1-3 code/tests/fixture.
- The second commit points README/index/digest at the immutable v4 artifact.

- [ ] Run focused tests, adjacent tests, ruff, pyright, runner help, and diff checks; commit Tasks 1-3.
- [ ] Run immutable `20260718_prime_pit_technical_fit_shape_v4` with frozen parameters.
- [ ] Validate decision, all headline values, exact 15 non-empty tables, Prime scope, cutoffs, same-date pairing, price audit hashes/counts, and shape-gate rows.
- [ ] Publish actual v4 values, document v3 supersession for price-basis/gate/CI hardening, and update the committed digest.
- [ ] Re-run publication, focused/adjacent, static, guardrail, skill-audit, and artifact checks; commit publication.
