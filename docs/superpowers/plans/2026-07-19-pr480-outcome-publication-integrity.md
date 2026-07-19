# PR #480 Outcome and Publication Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve authoritative stock completion-date outcomes through Fixed/Trend/Technical research, align TOPIX/N225 benchmarks, republish invalid evidence immutably, and synchronize the Research catalog.

**Architecture:** The external-price Daily Ranking panel passes completion date, stock return, and TOPIX excess directly from `price_outcome_relation`; legacy `stock_data` behavior remains separate. N225 joins use the same completion date. Fixed/Trend persist these fields through observations and bundles. Publication registry fields in TOML are tested against committed hermetic fixtures/digests.

**Tech Stack:** Python 3.12, DuckDB, pandas, pytest, TOML, runner-first research bundles.

## Global Constraints

- External-price primary outcomes must use `forward_outcome_completion_date_*`, `forward_close_return_*`, and `forward_close_excess_return_*` from the authoritative relation.
- TOPIX/N225 benchmark endpoints must equal each stock horizon's authoritative completion date.
- Legacy `stock_data` Daily Ranking behavior remains unchanged.
- Fixed/Trend observation bundles retain completion date, stock return, and TOPIX excess for audit.
- Existing bundles are immutable; Trend v6 and Fixed v10 are new runs. Technical Fit v9 is created only if corrected N225 evidence changes v8.
- Catalog structured fields are `canonicalRunId`, `canonicalDecision`, and `supersededRunIds`.
- OpenAPI, production Ranking API, ts/web, and strategy contracts remain unchanged.
- Existing PR #480 is updated; no duplicate PR is created.

---

### Task 1: End-to-End Authoritative Outcome Propagation

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_color_evidence.py`
- Modify: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_color_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py`

**Interfaces:**
- Consumes: existing `EventTimePriceRelations.signal_features` and `.forward_outcomes` schemas.
- Produces: Daily Ranking/consumer rows with `forward_outcome_completion_date_{h}d`, `forward_close_return_{h}d_pct`, `forward_close_excess_return_{h}d_pct`, and completion-aligned N225 fields.

- [ ] **Step 1: Add RED source and panel boundary regressions**

Extend the three-session price fixture so TOPIX closes differ on nominal and authoritative completion dates. Assert upstream relation returns the authoritative completion date and aligned excess.

In `test_ranking_color_evidence.py`, create minimal `price_feature_relation` / `price_outcome_relation` temp tables where a 1D stock completion is two TOPIX sessions later than nominal. Invoke `create_daily_ranking_research_panel()` and assert:

```python
row = conn.execute(
    "SELECT forward_outcome_completion_date_1d, "
    "forward_close_return_1d_pct, forward_close_excess_return_1d_pct "
    "FROM daily_ranking_research_panel WHERE code = '1111'"
).fetchone()
assert row == (date(2024, 1, 8), pytest.approx(10.0), pytest.approx(expected_aligned))
assert row[2] != pytest.approx(nominal_topix_excess)
```

Run both focused modules. Expected RED: completion column is missing and/or the panel returns nominal TOPIX excess.

- [ ] **Step 2: Implement explicit external outcome expressions**

In `_create_observation_panel()`, generate separate external and legacy SQL expressions. The external branch must select relation fields into unique internal aliases, then expose authoritative names without recomputing TOPIX excess:

```python
external_outcomes = ",\n".join(
    expression
    for horizon in horizons
    for expression in (
        f"outcome.forward_outcome_completion_date_{horizon}d",
        f"outcome.forward_close_return_{horizon}d_pct AS authoritative_forward_close_return_{horizon}d_pct",
        f"outcome.forward_close_excess_return_{horizon}d_pct AS authoritative_forward_close_excess_return_{horizon}d_pct",
    )
)
```

For external mode, `return_exprs` aliases the authoritative stock return and `excess_exprs` aliases authoritative TOPIX excess. Legacy mode retains synthetic `lead()` calculations.

- [ ] **Step 3: Align N225 to completion dates**

For external mode, join N225 signal close on `date` and one completion close per horizon on `forward_outcome_completion_date_{h}d`; calculate:

```sql
CASE WHEN n225_signal.close > 0 AND n225_completion.close > 0
THEN (n225_completion.close / n225_signal.close - 1.0) * 100.0 END
```

Use this value for `n225_close_return_{h}d_pct` and stock return minus it for `forward_close_n225_excess_return_{h}d_pct`. Preserve all-null behavior when the index table is absent. Do not use nominal N225 `lead(horizon)` in external mode.

- [ ] **Step 4: Add consumer-level RED regressions**

In Fixed and Trend fixtures, remove one candidate stock raw session while retaining TOPIX/N225 sessions. Assert final `observation_sample_df` contains authoritative completion date, stock return, and TOPIX excess and differs from nominal benchmark result. Assert bundle write/load round-trip retains these columns.

In Technical Fit, add an N225 regression where completion date differs from nominal lead and assert aligned N225 excess.

Expected RED: Fixed/Trend observation queries omit completion/stock return and/or retain nominal excess; Technical N225 uses nominal lead.

- [ ] **Step 5: Propagate fields through consumers**

Update Fixed `_query_fixed_free_observations()` and Trend `_build_candidate_observations()` horizon column lists to select all three authoritative fields. Ensure `observation_sample_df` and bundle schemas retain datetime/float columns. Update Technical N225 computation to join by authoritative completion date.

- [ ] **Step 6: Verify GREEN and commit**

Run:

```bash
uv run --project apps/bt python -m pytest -q \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_color_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
```

Expected: all pass, with only existing opt-in live-artifact skip/warnings.

Commit:

```bash
git add apps/bt/src/domains/analytics apps/bt/tests/unit/domains/analytics
git commit -m "fix(bt): preserve authoritative ranking outcomes"
```

### Task 2: Corrected Immutable Publications

**Files:**
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md`
- Modify conditionally: `apps/bt/docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md`
- Modify conditionally: `apps/bt/tests/fixtures/research/ranking_technical_fit_score_shape_evidence_published_digest.json`

**Interfaces:**
- Consumes: corrected runners and current Market v4.
- Produces: immutable Trend v6, Fixed v10, and only-if-changed Technical v9 bundles/readouts.

- [ ] **Step 1: Run pre-publication endpoint audit**

Execute a read-only audit over corrected runner observations and record completion mismatch counts/deltas by horizon. Verify Trend v5 mismatch sample is reproduced before rerun and Fixed v9 full observation count remains 4,762.

- [ ] **Step 2: Publish Trend v6 and Fixed v10**

Run existing runner CLI with new immutable IDs:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py \
  --run-id 20260719_prime_price_pit_conditional_lift_v6
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py \
  --run-id 20260719_prime_price_pit_fixed_return_priority_v10
```

Use the runners' existing canonical arguments/defaults; do not overwrite prior bundles.

- [ ] **Step 3: Audit Technical N225 and conditionally publish v9**

Run the corrected Technical research in a candidate immutable bundle or equivalent full observation audit. Compare v8 tables/digest. If any published table/gate/digest changes, publish `20260719_prime_pit_technical_fit_shape_v9`; otherwise retain v8 and record zero-difference N225 audit in tests/readout without a new canonical run.

- [ ] **Step 4: Verify bundles and update readouts**

For each new canonical bundle, verify manifest, `results.duckdb`, `summary.md`, table schemas/counts, completion fields, decision gates, and hashes. Update Published Readout, Source Artifacts, exact decisions/numbers, and supersession lineage. Do not copy old numbers when corrected results differ.

- [ ] **Step 5: Run publication tests and commit**

Run the three experiment suites, runner `--help`, guardrails, and any live bundle verification flags used by existing tests. Commit only canonical docs/digest changes:

```bash
git add apps/bt/docs/experiments/market-behavior apps/bt/tests/fixtures/research
git commit -m "docs(bt): publish aligned ranking outcomes"
```

### Task 3: Canonical Catalog Consistency

**Files:**
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`
- Modify: `apps/bt/docs/experiments/README.md`
- Create: `apps/bt/tests/fixtures/research/ranking_publication_registry.json`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_publication_registry.py`
- Modify if routing requires: `scripts/ci/research-test-targets.py`, `scripts/ci/test_targets.py`

**Interfaces:**
- Consumes: final run IDs/decisions from Task 2 and Technical digest.
- Produces: structured hermetic publication registry synchronized with catalog metadata.

- [ ] **Step 1: Write RED registry consistency test**

Create a fixture mapping the three experiment IDs to exact `canonicalRunId`, `canonicalDecision`, and ordered `supersededRunIds`. Parse TOML with `tomllib` and assert each entry exactly equals the fixture. For Technical, assert fixture run/decision also equals the existing/new published digest.

Expected RED: structured fields are absent and stale prose versions remain.

- [ ] **Step 2: Update catalog and index**

Add exact structured fields to all three TOML entries and update `decision` prose to final Trend/Fixed/Technical versions and supersession ranges. Update the top-level index Technical description and any versioned Trend/Fixed text. Values must use the actual Task 2 run IDs and decisions, not planned placeholders.

- [ ] **Step 3: Route the new hermetic test**

Ensure changed registry fixture and test map to the fast research contract group or mapped local research suite without moving heavy publication tests into GitHub Actions. Add routing regression first if any target file is initially uncovered.

- [ ] **Step 4: Verify and commit**

Run the new registry test, catalog service tests, research target/taxonomy tests, research guardrails, and privacy check. Commit:

```bash
git add apps/bt/docs/experiments apps/bt/tests/fixtures/research \
  apps/bt/tests/unit/domains/analytics/test_ranking_publication_registry.py scripts/ci
git commit -m "docs(bt): sync ranking publication catalog"
```

### Task 4: Final Verification and PR Update

**Files:**
- Modify only generated evidence if a canonical guardrail requires it: `docs/maintainability-snapshot-latest.json`, `docs/maintainability-snapshot-latest.md`.

- [ ] **Step 1: Run complete local verification**

Run focused tests, mapped fast/heavy research, Ruff, repository typecheck, research guardrails, skill/privacy audits, `git diff --check`, and:

```bash
UV_CACHE_DIR=/tmp/uv-cache scripts/prepush-ci.sh --research --skip-install
```

Refresh maintainability snapshots canonically if tracked source changes require it, then rerun from the start.

- [ ] **Step 2: Whole-branch review**

Generate a review package from `git merge-base origin/main HEAD` to HEAD. Require no unresolved Critical/Important findings, specifically checking endpoint propagation, publication numbers, catalog versions, and CI routing.

- [ ] **Step 3: Push and update PR #480**

Push `codex/ranking-technical-fit-score`, update the existing PR body with final versions and verification counts, reply to and resolve addressed review threads, and wait for GitHub CI/Nautilus success. Do not create a duplicate PR or merge it in this task.

- [ ] **Step 4: Completion audit**

Verify authoritative outcome endpoint tests, immutable bundles/readouts/catalog, clean worktree, remote head, resolved findings, and successful CI. Only then mark the goal complete.
