# Ranking Technical Fit Score Shape Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a PIT Prime-only experiment that learns a prior-only nonlinear Technical Fit Score and decides whether fixed endpoint returns or OLS fitted moves provide the better third Ranking score inside high Value/Long-Hybrid candidates.

**Architecture:** A dedicated analytics module reuses the Daily Ranking PIT panel, value composite, long-horizon leadership, and shared OLS helper. It materializes three fixed-free candidate rings first, attaches Prime-wide fixed/OLS percentiles, learns five-bin expectancy curves using prior years only, and evaluates out-of-time Fit Scores by ring and combined Top-K. A runner writes a fixed fifteen-table bundle and a canonical Japanese readout; production surfaces remain unchanged.

**Tech Stack:** Python 3.12, pandas, NumPy, DuckDB, pytest, ruff, pyright, existing bt research-bundle utilities.

## Global Constraints

- Exact signal-date Prime-equivalent membership only: `0101` and `0111`; Standard/Growth excluded.
- Market schema v4 and `stock_price_adjustment_mode=local_projection_v2_event_time`; fail closed with no latest fallback.
- Candidate rings use only Value Score and Long Hybrid Score and are materialized before technical/outcome joins.
- Primary outcome is 20D TOPIX-excess return; 5D and 60D are supporting diagnostics.
- Five fixed raw bins, prior-only walk-forward mapping, no sweet-spot or weight optimization.
- 2,000-resample fixed-seed moving-block bootstrap for published inference.
- Preserve the user-owned `.gitignore` modification and never stage it.

---

### Task 1: Ring, Bin, Mapping, and Decision Contracts

**Files:**
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Create: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Produces `classify_candidate_ring(value_score, leadership_score) -> str`.
- Produces `classify_raw_level_bin(level) -> str`.
- Produces `build_walkforward_mapping(training, evaluation_year, ...) -> DataFrame`.
- Produces `apply_walkforward_mapping(frame, mapping, ...) -> DataFrame`.
- Produces `classify_shape(...) -> str` and `build_decision_gate_df(...) -> DataFrame`.

- [ ] **Step 1: Write failing pure-contract tests**

```python
@pytest.mark.parametrize(("value", "leadership", "expected"), [
    (0.8, 0.8, "core_high_high"),
    (0.7, 0.8, "near_high_high_1"),
    (0.7, 0.7, "near_high_high_1"),
    (0.6, 0.7, "near_high_high_2"),
    (0.59, 0.9, "outside"),
    (None, 0.9, "missing"),
])
def test_ring_boundaries(value, leadership, expected):
    assert classify_candidate_ring(value, leadership) == expected

@pytest.mark.parametrize(("level", "expected"), [
    (0.0, "q1"), (0.2, "q2"), (0.4, "q3"),
    (0.6, "q4"), (0.8, "q5"), (1.0, "q5"), (None, "missing"),
])
def test_raw_bin_boundaries(level, expected):
    assert classify_raw_level_bin(level) == expected
```

Add registry-token rejection, flat mapping (`0.5`), prior-year-only training, per-bin `200/50` insufficiency, interpolation, fixed win, OLS win, tie, neither, and insufficiency-precedence tests.

- [ ] **Step 2: Verify RED**

Run: `cd apps/bt && uv run pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -q`

Expected: collection fails because the new module does not exist.

- [ ] **Step 3: Implement minimal pure functions and frozen registries**

Define `RING_REGISTRY`, `RAW_SCORE_REGISTRY`, `REQUIRED_BUNDLE_TABLES`, five bin boundaries, prior-only mapping normalization, piecewise interpolation, shape states, and decision precedence exactly as specified.

- [ ] **Step 4: Verify GREEN and commit**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -q
git add src/domains/analytics/ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "feat(bt): add technical fit score contracts"
```

### Task 2: PIT Candidate and Raw Technical Panel

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Produces `run_ranking_technical_fit_score_shape_evidence_research(db_path, ...) -> RankingTechnicalFitScoreShapeEvidenceResult`.
- Produces candidate observations with ring, fixed20/60/equal levels, OLS20/60/equal levels, diagnostics, and 5D/20D/60D outcomes.

- [ ] **Step 1: Write failing PIT integration tests**

Use the established mixed-market fixture, add valid v4 metadata, and assert exact-date codes are a subset of `0101/0111`, Standard/Growth are absent, candidate keys are unique, ring predicates contain no forbidden tokens, incompatible metadata raises, and appending future rows does not change earlier rings/raw levels.

- [ ] **Step 2: Verify RED**

Run the PIT integration tests and expect missing runner/panel builders.

- [ ] **Step 3: Implement the read-only panel**

Call `require_market_v4_compatibility`, create the Prime-only Daily Ranking panel, create long leadership and value composite inputs, materialize a keys-plus-ring-flags table, compute shared rolling OLS moves, rank fixed and OLS inputs across all Prime members per date, then join rings and completed outcomes. Do not import frontend or production Ranking code.

- [ ] **Step 4: Verify GREEN and commit**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -q
git add src/domains/analytics/ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "feat(bt): build PIT technical fit candidate panel"
```

### Task 3: Walk-Forward Shape and OOS Evaluation

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Produces `raw_shape_daily_df`, `raw_shape_summary_df`, `walkforward_mapping_df`, `oos_fit_score_lift_df`, `fixed_vs_ols_paired_df`, `topk_operational_lift_df`, `overheat_negative_diagnostics_df`, `segment_stability_df`, `annual_stability_df`, and `bootstrap_effect_ci_df`.

- [ ] **Step 1: Write failing analysis tests**

Add synthetic mountain, monotonic, flat, unstable, and insufficient curves; assert evaluation year never enters training; assert each daily comparison requires ten candidates and three per side; assert fixed/OLS paired rows use identical eligible dates; assert incomplete outcomes are dropped; assert fixed-seed bootstrap repeats exactly.

- [ ] **Step 2: Verify RED**

Run the new analysis tests and expect missing evidence builders.

- [ ] **Step 3: Implement date-equal shape, mapping, OOS, paired, Top-K, and diagnostic builders**

Use 2017–2021 as first training, expanding prior-only years thereafter. Learn one union-ring mapping per raw score family/year, apply it separately to each ring, and keep component scores attribution-only. Build sector-equal, bank-excluded, N225, z-band, negative-return, overheat, OLS R²/acceleration, conflict, and date-fixed-effect sensitivities without altering primary gates.

- [ ] **Step 4: Verify GREEN and commit**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -q
git add src/domains/analytics/ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "feat(bt): evaluate walk-forward technical fit scores"
```

### Task 4: Bundle and Runner

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Create: `apps/bt/scripts/research/run_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Produces `write_ranking_technical_fit_score_shape_evidence_bundle(...) -> ResearchBundleInfo`.
- Produces `build_summary_markdown(result) -> str` and CLI options for DB, dates, horizons, training minimums, bootstrap, sample limit, output root, and run id.

- [ ] **Step 1: Write failing bundle-contract tests**

Assert the durable DuckDB contains exactly the fifteen names in the spec, empty tables remain writable with explicit schemas, manifest records `0101/0111`, `fixed_return_free` rings, prior-only walk-forward timing, and summary decision matches `decision_gate`.

- [ ] **Step 2: Verify RED**

Run the bundle tests and expect absent writer/runner functionality.

- [ ] **Step 3: Implement writer, summary, and runner**

Use `write_research_bundle` with exact table names and fail on table-contract drift. The CLI default start is `2017-01-01`, default horizons are `5,20,60`, resamples `2000`, seed fixed in the module, and training minimums `200` observations/`50` dates.

- [ ] **Step 4: Verify GREEN and CLI help, then commit**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -q
uv run python scripts/research/run_ranking_technical_fit_score_shape_evidence.py --help
git add src/domains/analytics/ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py scripts/research/run_ranking_technical_fit_score_shape_evidence.py
git commit -m "feat(bt): add technical fit score research bundle"
```

### Task 5: Execute Research and Publish Readout

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Consumes final durable bundle.
- Produces canonical Japanese `## Published Readout` and catalog/index registration.

- [ ] **Step 1: Add failing publication test**

Assert the README begins with a decision, contains fixed-versus-OLS winner, shape state, Ranking implication, `20D<0`/overheat treatment, all three rings, `0101/0111`, final run id, and exact agreement with `decision_gate`.

- [ ] **Step 2: Verify RED**

Run the publication test and expect missing README/catalog failure.

- [ ] **Step 3: Run the frozen research**

```bash
cd apps/bt
uv run python scripts/research/run_ranking_technical_fit_score_shape_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --run-id 20260718_prime_pit_technical_fit_shape_v1
```

Validate manifest provenance, fifteen tables, Prime-only rows, mapping year cutoffs, and decision consistency before interpretation. Never alter gates after the run.

- [ ] **Step 4: Write decision-first README and catalog/index entries**

Report training evidence separately from 2022–2023 walk-forward and 2024+ hypothesis-origin evidence. Show every ring, raw five-bin shape, OOS Fit lift, paired fixed-minus-OLS result, Top-K/downside/concentration, limitations, and production implication.

- [ ] **Step 5: Verify and commit**

```bash
cd apps/bt
uv run pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -q
git add docs/experiments/README.md docs/experiments/research-catalog-metadata.toml docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "docs(bt): publish technical fit score shape evidence"
```

### Task 6: Final Verification and Independent Review

**Files:**
- Verify every file changed in Tasks 1–5.

**Interfaces:**
- Produces a reviewed handoff with only the pre-existing `.gitignore` change left dirty.

- [ ] **Step 1: Run focused and adjacent tests**

```bash
cd apps/bt
uv run pytest \
  tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py \
  tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py \
  tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py -q
```

- [ ] **Step 2: Run quality and guardrail checks**

```bash
cd apps/bt
uv run ruff check src/domains/analytics/ranking_technical_fit_score_shape_evidence.py scripts/research/run_ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
uv run pyright src/domains/analytics/ranking_technical_fit_score_shape_evidence.py
cd ../..
python3 scripts/check-research-guardrails.py
git diff --check
```

- [ ] **Step 3: Verify artifact and repository state**

Check the manifest, query all fifteen result tables and row counts, assert no non-Prime sample rows, compare README headline with `decision_gate`, and confirm `git status --short` contains only `.gitignore`.

- [ ] **Step 4: Request independent code/research review**

Review the implementation against the design spec, focusing on PIT safety, candidate-before-technical order, prior-only mappings, sample insufficiency semantics, fixed/OLS fairness, table/readout consistency, and post-result threshold drift. Fix every Critical/Important issue and rerun Steps 1–3.
