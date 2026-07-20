# PR #480 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove future-outcome selection leakage, enforce Market v4 basis integrity, prevent near-flat Technical Fit amplification, and preserve the fast-CI/heavy-local boundary before updating PR #480.

**Architecture:** Each research top-k builder will separate signal-time membership from outcome evaluation and expose explicit coverage/status fields. Price projection will reject basis rows whose adjustment frontier differs from their event date, while Technical Fit freezes a 1 bp flatness threshold. CI will run only dedicated lightweight contract surfaces; full experiment and publication verification remains in the local research workflow.

**Tech Stack:** Python 3.12, pandas, NumPy, DuckDB, pytest, GitHub Actions YAML, repository Python CI routing scripts.

## Global Constraints

- Top-k membership must be determined without reading forward-outcome availability.
- Missing outcomes must never backfill selection; incomplete rows remain auditable and all outcome-derived metrics are `NaN`.
- Stability, bootstrap, and decision/adoption gates consume only `outcome_status == "complete"` rows.
- `candidate_count` is the pre-outcome signal-time universe size.
- Market v4 signal and completion bases require `adjustment_through_date = valid_from`; no latest/current fallback or local recomputation is allowed.
- Technical Fit expectancy spreads of at most `0.01` percentage point are flat and map to score `0.5`.
- GitHub Actions must not execute full experiment suites or live publication reruns; those remain local.
- Existing PR #480 is updated and made Ready for review; it is not replaced with a new PR number.

---

### Task 1: Selection-First Top-k Contracts

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_research_selection_contract.py`
- Modify: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_research_selection_contract.py`

**Interfaces:**
- Consumes: existing top-k builders and their stability/bootstrap/gate consumers.
- Produces: `FrozenTopKSelection` / `select_frozen_topk()` plus top-k rows with `candidate_outcome_count`, `candidate_outcome_coverage_pct`, `selected_outcome_count`, `selected_outcome_coverage_pct`, and `outcome_status`.

- [ ] **Step 1: Write adversarial missing-outcome tests**

First add a lightweight test for the wished-for API:

```python
selection = select_frozen_topk(
    frame,
    score_columns=("score",),
    outcome_column="outcome_pct",
    k=5,
    ascending=(False,),
)
assert selection.candidate_count == 20
assert selection.selected["code"].tolist() == ["00", "01", "02", "03", "04"]
assert selection.selected_outcome_count == 4
assert selection.outcome_status == "incomplete_outcomes"
```

For each builder, construct at least 20 deterministic candidates, set the highest-ranked candidate outcome to `NaN`, and assert that `candidate_count == 20`, `selected_outcome_count == k - 1`, `outcome_status == "incomplete_outcomes"`, and outcome-derived metrics are `NaN`. Assert the shared selection's code list to prove rank `k + 1` did not replace the missing top-k member.

- [ ] **Step 2: Run the three focused tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest -q \
  tests/unit/domains/analytics/test_ranking_research_selection_contract.py \
  tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py -k "missing_outcome" \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -k "missing_outcome" \
  tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -k "missing_outcome"
```

Expected: failures show the current pre-selection outcome filtering/backfill behavior or missing coverage columns.

- [ ] **Step 3: Implement selection-first evaluation**

Implement the shared boundary with these fields:

```python
@dataclass(frozen=True)
class FrozenTopKSelection:
    candidates: pd.DataFrame
    selected: pd.DataFrame
    candidate_outcomes: pd.Series
    selected_outcomes: pd.Series
    candidate_count: int
    candidate_outcome_count: int
    candidate_outcome_coverage_pct: float
    selected_outcome_count: int
    selected_outcome_coverage_pct: float
    outcome_status: str
```

`select_frozen_topk()` must validate `k > 0`, matching sort/ascending lengths, required columns, and deterministic `code` tie-breaking. In each builder, use this ordering:

```python
candidates = group.dropna(subset=[score_column]).drop_duplicates(["date", "code"])
ranked = candidates.sort_values([score_column, "code"], ascending=[False, True])
selected = ranked.head(k)
candidate_outcomes = pd.to_numeric(ranked[outcome_column], errors="coerce")
selected_outcomes = pd.to_numeric(selected[outcome_column], errors="coerce")
complete = bool(candidate_outcomes.notna().all() and selected_outcomes.notna().all())
```

Always emit coverage fields. When `complete` is false, set every outcome-derived metric to `float("nan")`; do not replace rows in `selected`. Keep signal-time-only metrics such as selected membership concentration and turnover when they do not require outcomes.

- [ ] **Step 4: Filter downstream statistical consumers**

Before stability, bootstrap, and gate calculations, use:

```python
complete_rows = frame.loc[frame["outcome_status"].eq("complete")].copy()
```

Update Technical Fit `_BUNDLE_EMPTY_SCHEMAS["topk_operational_lift"]` with exact nullable integer/float/string types for the new fields.

- [ ] **Step 5: Verify GREEN**

Run all three affected modules:

```bash
uv run --directory apps/bt pytest -q \
  tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py \
  tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
```

Expected: all pass, with only the existing opt-in live-artifact skip.

- [ ] **Step 6: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_* apps/bt/tests/unit/domains/analytics/test_ranking_*
git commit -m "fix(bt): remove future outcome selection leakage"
```

### Task 2: Event-time Basis Integrity Contract

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Consumes: `create_event_time_price_relations()` and its existing fail-closed request-count checks.
- Produces: a small independently collectable contract module covering successful projection and signal/completion adjustment-frontier rejection.

- [ ] **Step 1: Extract the minimal DuckDB projection fixture**

Move or share only the fixture setup required by projection contract tests. Keep large end-to-end experiment fixtures in the existing score-shape module.

- [ ] **Step 2: Write signal and completion mismatch regressions**

Mutate a valid basis with:

```sql
UPDATE stock_adjustment_bases
SET adjustment_through_date = valid_from + INTERVAL 1 DAY
WHERE basis_id = ?
```

Assert signal mismatch raises the signal readiness error and completion mismatch raises the completion readiness error.

- [ ] **Step 3: Run the new contract tests and verify RED**

```bash
uv run --directory apps/bt pytest -q tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py
```

Expected: both mismatch tests fail because the basis is currently accepted.

- [ ] **Step 4: Add integrity predicates**

Add this predicate to both signal and completion ready/materialized basis selection queries:

```sql
AND CAST(basis.adjustment_through_date AS DATE)
    = CAST(basis.valid_from AS DATE)
```

Do not change interval cardinality semantics.

- [ ] **Step 5: Verify GREEN and commit**

```bash
uv run --directory apps/bt pytest -q tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py
git add apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "fix(bt): enforce event-time adjustment frontier"
```

### Task 3: Technical Fit Flatness Threshold

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Consumes: `classify_shape(expectancies, ...)` and `build_walkforward_mapping()`.
- Produces: `DEFAULT_FLAT_EXPECTANCY_TOLERANCE_PCT = 0.01` and optional `flat_tolerance_pct` validation.

- [ ] **Step 1: Write threshold tests**

Test spreads below and exactly `0.01` as flat with five `0.5` scores, a spread above `0.01` as non-flat/min-max mapped, and a negative tolerance override as `ValueError`.

- [ ] **Step 2: Verify RED**

```bash
uv run --directory apps/bt pytest -q tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -k "flat or tolerance"
```

- [ ] **Step 3: Implement and propagate the frozen threshold**

```python
DEFAULT_FLAT_EXPECTANCY_TOLERANCE_PCT = 0.01

def classify_shape(
    expectancies: Sequence[float | None],
    *,
    flat_tolerance_pct: float = DEFAULT_FLAT_EXPECTANCY_TOLERANCE_PCT,
    ...,
) -> str:
    if flat_tolerance_pct < 0:
        raise ValueError("flat_tolerance_pct must be non-negative")
    ...
    if max(finite_values) - min(finite_values) <= flat_tolerance_pct:
        return "flat"
```

Ensure mapping calls this contract and records `mapping_status="flat"` with score `0.5`.

- [ ] **Step 4: Verify GREEN and commit**

```bash
uv run --directory apps/bt pytest -q tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -k "flat or tolerance or walkforward_mapping"
git add apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "fix(bt): bound near-flat technical fit mapping"
```

### Task 4: Fast CI and Heavy-local Routing

**Files:**
- Modify: `scripts/ci/test_taxonomy.py`
- Modify: `scripts/ci/research-test-targets.py`
- Modify: `scripts/ci/test_targets.py`
- Modify: `apps/bt/tests/unit/scripts/test_test_taxonomy.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`
- Modify: `apps/bt/tests/unit/scripts/test_research_test_targets.py`
- Modify: `apps/bt/tests/unit/scripts/test_test_targets.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Verify: `.github/workflows/ci.yml`
- Verify: `scripts/prepush-ci.sh`

**Interfaces:**
- Consumes: `is_research_path()`, `pytest_targets_for_research_changes()`, `BT_FAST_RESEARCH_TESTS`.
- Produces: correct research classification/local mapping while Actions remains `--mode fast-pytest` only.

- [ ] **Step 1: Write routing tests**

Assert research fixture paths classify as research; analytics research test paths classify as research while production analytics tests retain product semantics; changed research tests map to themselves; the published Technical Fit digest maps to its consumer test; and the fast target list contains the dedicated projection contract module.

- [ ] **Step 2: Verify RED**

```bash
uv run --directory apps/bt pytest -q tests/unit/scripts/test_test_taxonomy.py tests/unit/scripts/test_ci_changed_scope.py tests/unit/scripts/test_research_test_targets.py tests/unit/scripts/test_test_targets.py tests/unit/scripts/test_ci_workflow.py
```

- [ ] **Step 3: Implement taxonomy and local mappings**

Add `apps/bt/tests/fixtures/research/` to research prefixes. For `apps/bt/tests/unit/domains/analytics/test_*.py`, derive the corresponding module name and apply existing production analytics exclusions. In `research-test-targets.py`, map changed research tests to their `tests/...` path and map the published digest fixture to `tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`.

Add only `tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py` and `tests/unit/domains/analytics/test_ranking_research_selection_contract.py` to `BT_FAST_RESEARCH_TESTS`. Do not add any full 700+ or 2,000+ line experiment module.

- [ ] **Step 4: Verify Actions/local boundary**

Assert `.github/workflows/ci.yml` continues to call `research-test-targets.py --mode fast-pytest` and never invokes mapped experiment targets. Assert `scripts/prepush-ci.sh` continues to run both fast and mapped research targets locally.

- [ ] **Step 5: Verify GREEN and commit**

```bash
uv run --directory apps/bt pytest -q tests/unit/scripts/test_test_taxonomy.py tests/unit/scripts/test_ci_changed_scope.py tests/unit/scripts/test_research_test_targets.py tests/unit/scripts/test_test_targets.py tests/unit/scripts/test_ci_workflow.py
git add scripts/ci apps/bt/tests/unit/scripts
git commit -m "ci: enforce fast research contract boundary"
```

### Task 5: Local Research Rerun and Publication Update

**Files:**
- Modify as generated/required: `apps/bt/docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md`
- Modify as generated/required: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify as generated/required: `apps/bt/docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md`
- Modify as generated/required: `apps/bt/tests/fixtures/research/ranking_technical_fit_score_shape_evidence_published_digest.json`

**Interfaces:**
- Consumes: the three runner CLIs and the shared XDG Market v4 data plane.
- Produces: immutable new bundle versions, updated Japanese Published Readouts, and matching committed digest provenance.

- [ ] **Step 1: Verify runner interfaces**

```bash
uv run --directory apps/bt python scripts/research/run_ranking_fixed_return_priority_evidence.py --help
uv run --directory apps/bt python scripts/research/run_ranking_trend_acceleration_conditional_lift.py --help
uv run --directory apps/bt python scripts/research/run_ranking_technical_fit_score_shape_evidence.py --help
```

- [ ] **Step 2: Run all three studies locally**

Use each runner's canonical arguments from its README/source. Write new versioned bundle directories; do not overwrite historical immutable bundles.

- [ ] **Step 3: Update publication readouts and digest**

Update Decision/Main Findings/Interpretation/Production Implication/Caveats/Source Artifacts in Japanese from the new bundles. Regenerate the Technical Fit published digest from the new canonical artifact.

- [ ] **Step 4: Verify publication**

```bash
python3 scripts/check-research-guardrails.py
uv run --directory apps/bt pytest -q \
  tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py \
  tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
```

- [ ] **Step 5: Commit**

```bash
git add apps/bt/docs/experiments apps/bt/tests/fixtures/research
git commit -m "docs(bt): republish leakage-safe ranking evidence"
```

### Task 6: Whole-branch Verification and PR Update

**Files:**
- Verify all PR files against `origin/main`.

**Interfaces:**
- Consumes: all task commits.
- Produces: pushed branch and Ready-for-review PR #480 with green required checks.

- [ ] **Step 1: Run local verification**

```bash
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
python3 scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
scripts/prepush-ci.sh --research --skip-install
```

- [ ] **Step 2: Inspect scope and final diff**

```bash
git status --short
git diff --check origin/main...HEAD
git log --oneline origin/main..HEAD
```

- [ ] **Step 3: Push and update PR**

```bash
git push origin codex/ranking-technical-fit-score
gh pr ready 480
```

- [ ] **Step 4: Wait for required checks**

```bash
gh pr checks 480 --watch
gh pr view 480 --json state,isDraft,mergeStateStatus,reviewDecision,statusCheckRollup
```

Expected: PR remains open, is not draft, and required checks succeed. Do not merge in this task.
