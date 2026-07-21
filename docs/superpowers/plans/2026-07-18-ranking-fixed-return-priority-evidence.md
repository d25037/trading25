# Ranking Fixed Return Priority Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a PIT Prime-only experiment that decides whether fixed 20D/60D returns deserve Ranking priority inside independently selected long candidates.

**Architecture:** Add one focused analytics module that reuses the Daily Ranking PIT panel and existing value/leadership/ATR calculations, but freezes two mutually exclusive candidate families before fixed-return ranking. A runner writes an eleven-table research bundle; the canonical README publishes a decision-first Japanese readout. No production/API/UI behavior changes.

**Tech Stack:** Python 3.12, pandas, NumPy, DuckDB, pytest, ruff, pyright, existing bt research-bundle utilities.

## Global Constraints

- Exact-date Prime-equivalent membership only: market codes `0101` and `0111`; Standard/Growth excluded.
- Market schema v4 and `stock_price_adjustment_mode=local_projection_v2_event_time`; fail closed with no latest fallback.
- Candidate predicates are fixed-return-free and future-return-free.
- Primary outcome is 20D TOPIX-excess return; 5D and 60D are diagnostics.
- Frozen gates and 2,000-resample moving-block bootstrap; no post-result threshold tuning.
- Preserve the user-owned `.gitignore` modification and do not stage it.

---

### Task 1: Pure Feature, Ranking, and Gate Contracts

**Files:**
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Create: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`

**Interfaces:**
- Produces: `classify_fixed_return_quadrant(return_20d_pct, return_60d_pct) -> str`, `add_prime_date_percentiles(frame) -> DataFrame`, `moving_block_bootstrap_ci(...)`, `build_decision_gate_df(...)`.
- Produces constants `SCAFFOLD_REGISTRY`, `SEGMENTS`, `PRIME_EQUIVALENT_MARKET_CODES`, and `REQUIRED_BUNDLE_TABLES`.

- [ ] **Step 1: Write failing boundary and predicate tests**

```python
@pytest.mark.parametrize(("r20", "r60", "expected"), [
    (1.0, 2.0, "++"), (1.0, -2.0, "+-"),
    (-1.0, 2.0, "-+"), (-1.0, -2.0, "--"),
    (0.0, 1.0, "zero"), (None, 1.0, "missing"),
])
def test_fixed_return_quadrant_boundaries(r20, r60, expected):
    assert classify_fixed_return_quadrant(r20, r60) == expected

def test_scaffold_predicates_are_fixed_and_future_free():
    forbidden = ("return_20", "return_60", "momentum", "neutral_rerating",
                 "crowded_rerating", "distribution_stress", "ex_overheat",
                 "sector_strength", "forward_", "future_")
    for scaffold in SCAFFOLD_REGISTRY:
        assert not any(token in scaffold.predicate.lower() for token in forbidden)
```

- [ ] **Step 2: Verify RED**

Run: `cd apps/bt && uv run pytest tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py -q`

Expected: collection fails because the new module does not exist.

- [ ] **Step 3: Implement constants, dataclasses, quadrant classification, Prime-date percentiles, bootstrap, and frozen gate aggregation**

Use strict `>0/<0`; return `zero` when either finite value equals zero and `missing` when either value is null/non-finite. Rank 20D and 60D returns against all exact-date Prime rows before filtering to scaffold candidates, then set `fixed_equal_priority = (fixed20_priority + fixed60_priority) / 2`.

- [ ] **Step 4: Verify GREEN**

Run the Task 1 test file and expect all pure-function tests to pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py
git commit -m "feat(bt): add fixed return priority contracts"
```

### Task 2: PIT Observation Panel and Fixed-Free Scaffolds

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`

**Interfaces:**
- Produces: `run_ranking_fixed_return_priority_evidence_research(db_path, ...) -> RankingFixedReturnPriorityEvidenceResult`.
- Consumes the existing Daily Ranking research panel, long leadership 120/252/504 calculations, raw ATR20 acceleration, Deep Value, and equal-weight value composite.

- [ ] **Step 1: Write failing integration tests**

Build the established mixed-market fixture and assert observation rows contain only `0101/0111`, both codes appear across the reorganization boundary, `0112/0113` never appear, family assignment is mutually exclusive, and appending a future Growth row does not alter earlier features or assignments.

- [ ] **Step 2: Verify RED**

Run the two integration tests; expect missing runner/panel functions.

- [ ] **Step 3: Implement the read-only PIT pipeline**

Call `assert_daily_ranking_research_tables`, create the Prime-only Daily Ranking panel, create long-leadership and raw-ATR inputs, materialize a fixed-free candidate base, then join Prime-date return percentiles and forward excess returns. Validate schema-v4 provenance through the existing readers and reject missing exact-date membership.

- [ ] **Step 4: Verify GREEN**

Run the full new test file and expect PIT/membership/append tests to pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py
git commit -m "feat(bt): build fixed-free Ranking scaffold panel"
```

### Task 3: Evidence Tables and Bundle

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Create: `apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py`

**Interfaces:**
- Produces all eleven DataFrames named in `REQUIRED_BUNDLE_TABLES`.
- Produces `write_ranking_fixed_return_priority_evidence_bundle(result, run_id, output_root) -> ResearchBundleInfo` and `build_summary_markdown(result) -> str`.

- [ ] **Step 1: Write failing table-contract tests**

Assert daily sides require two candidates, percentiles are date-local Prime percentiles, incomplete outcomes are absent, bootstrap is seed-stable, both families are required for a pass, and a written bundle contains exactly:

```python
{
 "coverage_attrition", "scaffold_registry", "continuous_priority_lift",
 "fixed_2x2_daily", "fixed_incremental_contrast", "topk_priority_lift",
 "segment_stability", "bootstrap_effect_ci", "regression_sensitivity",
 "decision_gate", "observation_sample",
}
```

- [ ] **Step 2: Verify RED**

Run the table-contract tests; expect absent builders and bundle writer.

- [ ] **Step 3: Implement evidence builders and CLI**

Build family/date continuous top-bottom and IC rows for all three variants, strict 2×2 rows and four contrasts, combined/family/leave-one-out K5/K10 rows, segment/year summaries, bootstrap CIs, z/deep-pullback/sector-equal/bank/N225/boundary/regression sensitivities, and one frozen recommendation row. The CLI accepts database path, date range, run id, bootstrap controls, and output root.

- [ ] **Step 4: Verify GREEN and smoke CLI help**

Run:

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py -q
uv run python scripts/research/run_ranking_fixed_return_priority_evidence.py --help
```

Expected: tests pass and help exits 0.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py
git commit -m "feat(bt): add fixed return priority research bundle"
```

### Task 4: Catalog, Run, and Published Readout

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes the durable bundle generated by Task 3.
- Produces the canonical Japanese `## Published Readout` and catalog metadata.

- [ ] **Step 1: Add a failing catalog/bundle discovery assertion**

Extend the new test to assert the experiment id is registered and the README contains `## Published Readout`, Prime/PIT scope, the recommendation, both family names, effect sizes, uncertainty, downside, segments, and bundle provenance.

- [ ] **Step 2: Verify RED**

Run the new test and expect missing catalog/readout failure.

- [ ] **Step 3: Run the frozen research**

```bash
cd apps/bt
uv run python scripts/research/run_ranking_fixed_return_priority_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --run-id 20260718_prime_pit_fixed_return_priority_v1
```

Do not alter gates after the run. Validate manifest provenance and eleven DuckDB tables before interpreting results.

- [ ] **Step 4: Write the decision-first README and catalog entries**

Start with one plain Japanese conclusion, explain what population was tested, show 20D/60D/composite and badge gates in a compact table, then Top-K/downside/segments, limitations, and follow-on. Explicitly state that 2024+ is not a holdout and observations are not portfolio performance.

- [ ] **Step 5: Verify GREEN and commit**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py -q
git add docs/experiments/README.md docs/experiments/research-catalog-metadata.toml docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md
git commit -m "docs(bt): publish fixed return priority evidence"
```

### Task 5: Final Verification and Review

**Files:**
- Verify all files changed in Tasks 1-4.

**Interfaces:**
- Produces a clean, evidence-backed handoff; `.gitignore` remains unstaged and user-owned.

- [ ] **Step 1: Run focused and adjacent tests**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q
```

- [ ] **Step 2: Run quality checks**

```bash
cd apps/bt
uv run ruff check src/domains/analytics/ranking_fixed_return_priority_evidence.py scripts/research/run_ranking_fixed_return_priority_evidence.py tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py
uv run pyright src/domains/analytics/ranking_fixed_return_priority_evidence.py
```

- [ ] **Step 3: Verify artifact and repository state**

Inspect `manifest.json`, query `results.duckdb` table names/counts, run `git diff --check`, and confirm `git status --short` contains no uncommitted research files and only the pre-existing `.gitignore` modification.

- [ ] **Step 4: Review against the spec**

Check every gate uses both primary families, primary tables exclude Standard/Growth and circular predicates, all sensitivity-only analyses are labeled, and the README conclusion matches `decision_gate` exactly.

- [ ] **Step 5: Commit any review fixes**

Stage only research files and use `git commit -m "fix(bt): harden fixed return priority evidence"` if review changes are necessary.
