# PR #480 Final Review Blockers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align Technical Fit feature/outcome session counting and make docs-only `prepush-ci.sh --research` execute the complete forced research path before updating PR #480.

**Architecture:** The price projection keeps the canonical raw relation intact for physical provenance, but applies one identical valid-bar predicate to both projected feature windows and the raw session calendar used by forward `lead()`. The pre-push script treats `research_ci || include_research` as one selection contract at command, dependency, early-return, and execution boundaries, verified through an isolated subprocess harness.

**Tech Stack:** Python 3.12, DuckDB, pytest, Bash, git, uv, GitHub pull requests.

## Global Constraints

- Valid feature/outcome sessions require `open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0`.
- `canonical_raw_row_count` continues to count every normalized physical raw row, including invalid bars.
- Existing event-time basis/segment fail-closed checks and TOPIX endpoint semantics remain unchanged.
- `--research` forces command validation, bt dependency preparation unless `--skip-install`, and research suite execution even for plain docs-only changes.
- Plain docs-only changes without force flags retain the early PASS path.
- Production Ranking API, OpenAPI, ts/web, strategy contracts, and decision gates remain unchanged.
- Existing PR #480 is updated; no duplicate PR is created.

---

### Task 1: Valid Raw-Session Price Projection

**Files:**
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py`
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py:316-326,404-413`

**Interfaces:**
- Consumes: `create_event_time_price_relations(conn, query_start, query_end, analysis_start_date, analysis_end_date, horizons)`.
- Produces: unchanged `EventTimePriceRelations` and `EventTimePriceAudit`; horizon completion dates count only valid raw sessions.

- [ ] **Step 1: Extend the minimal fixture for a three-session invalid-bar case**

Add a helper option to `_build_price_projection_db()` that replaces the two raw rows with:

```python
raw_rows = [
    ("1111", "2024-01-04", 100.0, 101.0, 99.0, 100.0, 1_000, 1.0),
    ("1111", "2024-01-05", 0.0, 0.0, 0.0, 0.0, -1, 1.0),
    ("1111", "2024-01-08", 110.0, 111.0, 109.0, 110.0, 1_100, 1.0),
]
```

For the invalid-bar case, insert exact `stock_master_daily`, `daily_valuation`, ready event-time basis, basis segments, and TOPIX rows through `2024-01-08`. The signal date remains `2024-01-04`; the completion basis is valid from `2024-01-08` and covers both endpoints through its segments.

- [ ] **Step 2: Write the failing horizon regression**

Add:

```python
def test_event_time_price_projection_skips_invalid_raw_bars_when_counting_horizon(
    tmp_path: Path,
) -> None:
    db_path = _build_price_projection_db(
        tmp_path / "market.duckdb",
        include_invalid_intermediate_bar=True,
    )
    conn = duckdb.connect(str(db_path))
    try:
        relations, audit = create_event_time_price_relations(
            conn,
            query_start="2024-01-04",
            query_end="2024-01-08",
            analysis_start_date="2024-01-04",
            analysis_end_date="2024-01-04",
            horizons=(1,),
        )
        outcome = conn.execute(
            f"SELECT forward_outcome_completion_date_1d, "
            f"forward_close_return_1d_pct FROM {relations.forward_outcomes}"
        ).fetchone()
    finally:
        conn.close()

    assert str(outcome[0]) == "2024-01-08"
    assert outcome[1] == pytest.approx(10.0)
    assert audit.canonical_raw_row_count == 3
    assert audit.completed_outcome_row_count == 1
```

- [ ] **Step 3: Run the regression and verify RED**

Run:

```bash
uv run --project apps/bt python -m pytest -q \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py \
  -k skips_invalid_raw_bars_when_counting_horizon
```

Expected: FAIL because the current `lead()` chooses `2024-01-05`, producing the invalid completion date and a null return while counting one completed outcome row.

- [ ] **Step 4: Apply the valid-bar predicate to outcome session input**

Define the SQL fragment once inside `create_event_time_price_relations()`:

```python
valid_raw_bar_predicate = (
    "open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0"
)
```

Use it in both SQL relations:

```python
f"""
FROM ranking_technical_fit_basis_prices
WHERE {valid_raw_bar_predicate}
"""
```

and:

```python
f"""
CREATE OR REPLACE TEMP TABLE ranking_technical_fit_raw_sessions AS
SELECT code, date, {lead_exprs}
FROM ranking_technical_fit_normalized_raw
WHERE {valid_raw_bar_predicate}
"""
```

Do not filter `ranking_technical_fit_normalized_raw` itself or change `canonical_raw_row_count`.

- [ ] **Step 5: Verify GREEN for the complete price contract module**

Run:

```bash
uv run --project apps/bt python -m pytest -q \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py
```

Expected: all tests PASS, including the existing poisoned `stock_data`, basis-frontier, and segment-integrity cases.

- [ ] **Step 6: Commit the price fix**

```bash
git add \
  apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py
git commit -m "fix(bt): align technical fit outcome sessions"
```

### Task 2: Forced Research Pre-push Execution

**Files:**
- Create: `apps/bt/tests/unit/scripts/test_prepush_ci_execution.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Modify: `scripts/prepush-ci.sh:114-132,182-195,329-350`

**Interfaces:**
- Consumes: CLI flags `--research`, `--skip-install`, `PREPUSH_BASE_REF`, and changed-file classification.
- Produces: unchanged CLI surface; docs-only `--research` prepares bt dependencies and executes `run_research_suite()`.

- [ ] **Step 1: Build an isolated executable pre-push harness**

Create `test_prepush_ci_execution.py` with a helper that:

1. Copies `scripts/prepush-ci.sh`, `scripts/ci/changed-scope.py`, and `scripts/ci/test_taxonomy.py` into `tmp_path / "repo"`.
2. Creates executable no-op `scripts/check-privacy-leaks.py` and `scripts/skills/audit_skills.py`.
3. Creates `scripts/ci/test_targets.py` that prints `tests/unit/domains/analytics/test_research_core.py` and `scripts/ci/research-test-targets.py` that exits without targets.
4. Creates executable `scripts/bt-pytest.sh` that appends its arguments to `$PREPUSH_TRACE_FILE`.
5. Creates a stub `uv` earlier in `PATH` that appends `uv $*` to `$PREPUSH_TRACE_FILE` and exits zero.
6. Initializes git, commits `docs/note.md`, records the base SHA, then modifies `docs/note.md` so classification is docs-only.

Return `(repo, base_sha, trace_path, env)` where `env` sets `PREPUSH_BASE_REF`, `PREPUSH_TRACE_FILE`, `UV_CACHE_DIR`, and the stubbed `PATH`.

- [ ] **Step 2: Write forced and unforced docs-only regressions**

Add:

```python
def test_docs_only_research_flag_prepares_and_runs_research_suite(tmp_path: Path) -> None:
    repo, base_sha, trace_path, env = _build_prepush_harness(tmp_path)
    result = subprocess.run(
        ["bash", "scripts/prepush-ci.sh", "--research"],
        cwd=repo,
        env=env | {"PREPUSH_BASE_REF": base_sha},
        capture_output=True,
        text=True,
        check=False,
    )

    trace = trace_path.read_text(encoding="utf-8")
    assert result.returncode == 0, result.stderr
    assert "docs-only change; no local CI tiers selected" not in result.stdout
    assert "==> [quality:research-guardrails]" in result.stdout
    assert "==> [bt-research-tests:fast]" in result.stdout
    assert "uv sync --locked" in trace
    assert "test_research_core.py" in trace


def test_plain_docs_only_change_keeps_early_pass(tmp_path: Path) -> None:
    repo, base_sha, trace_path, env = _build_prepush_harness(tmp_path)
    result = subprocess.run(
        ["bash", "scripts/prepush-ci.sh", "--skip-install"],
        cwd=repo,
        env=env | {"PREPUSH_BASE_REF": base_sha},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "docs-only change; no local CI tiers selected" in result.stdout
    assert "quality:research-guardrails" not in result.stdout
    assert "test_research_core.py" not in trace_path.read_text(encoding="utf-8")
```

- [ ] **Step 3: Add a source contract for all selection boundaries**

In `test_ci_workflow.py`, assert the script contains all three forced-research decisions:

```python
def test_prepush_forced_research_is_honored_at_every_selection_boundary() -> None:
    source = PREPUSH_CI.read_text(encoding="utf-8")

    assert source.count("${research_ci} || ${include_research}") >= 3
    assert (
        "if ${docs_only} && ! ${include_research} "
        "&& ! ${include_security} && ! ${include_web_e2e}; then"
    ) in source
```

- [ ] **Step 4: Run the regressions and verify RED**

Run:

```bash
uv run --project apps/bt python -m pytest -q \
  apps/bt/tests/unit/scripts/test_prepush_ci_execution.py \
  apps/bt/tests/unit/scripts/test_ci_workflow.py \
  -k "docs_only or forced_research"
```

Expected: forced-research execution stops at the current docs-only early PASS; the source contract also fails because `include_research` is absent from the early return and selection boundaries. Plain docs-only remains PASS.

- [ ] **Step 5: Align command, dependency, early-return, and execution conditions**

In `scripts/prepush-ci.sh`:

```bash
if ${product_ci} || ${research_ci} || ${include_research} || ${contracts_ci} || ${security_ci} || ${include_security} || ${include_web_e2e}; then
  install_bt_deps
fi
```

Use the same `research_ci || include_research` selection in `ensure_commands_for_scope()`. Change the early return to:

```bash
if ${docs_only} && ! ${include_research} && ! ${include_security} && ! ${include_web_e2e}; then
```

Keep execution as:

```bash
if ${research_ci} || ${include_research}; then
  run_research_suite
fi
```

- [ ] **Step 6: Verify GREEN for CI routing tests**

Run:

```bash
uv run --project apps/bt python -m pytest -q \
  apps/bt/tests/unit/scripts/test_prepush_ci_execution.py \
  apps/bt/tests/unit/scripts/test_ci_workflow.py \
  apps/bt/tests/unit/scripts/test_ci_changed_scope.py \
  apps/bt/tests/unit/scripts/test_research_test_targets.py \
  apps/bt/tests/unit/scripts/test_test_taxonomy.py
```

Expected: all tests PASS.

- [ ] **Step 7: Commit the pre-push fix**

```bash
git add \
  scripts/prepush-ci.sh \
  apps/bt/tests/unit/scripts/test_prepush_ci_execution.py \
  apps/bt/tests/unit/scripts/test_ci_workflow.py
git commit -m "fix(ci): honor forced research checks"
```

### Task 3: Completion Audit and PR Update

**Files:**
- Inspect: `apps/bt/docs/experiments/market-behavior/ranking-fixed-return-priority-evidence/README.md`
- Inspect: `apps/bt/docs/experiments/market-behavior/ranking-technical-fit-score-shape-evidence/README.md`
- Inspect: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify only if evidence changes: publication README/digest artifacts generated by existing runners.

**Interfaces:**
- Consumes: current Market v4 `stock_data_raw`, affected test target routing, existing PR #480 head.
- Produces: verified branch pushed to `origin/codex/ranking-technical-fit-score` and updated PR #480.

- [ ] **Step 1: Inspect current Market v4 for invalid raw bars**

Resolve the canonical market path using the existing XDG helpers or repository configuration, then run a read-only DuckDB query equivalent to:

```sql
SELECT count(*) AS invalid_rows
FROM stock_data_raw
WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume < 0;
```

If `invalid_rows = 0`, record that existing publication outcomes/digests are unchanged by construction. If nonzero rows intersect any research query interval, rerun all three existing publication runners with their canonical arguments and verify immutable new bundles/readouts before committing generated docs.

- [ ] **Step 2: Run focused and mapped verification**

Run:

```bash
uv run --project apps/bt python -m pytest -q \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py \
  apps/bt/tests/unit/scripts/test_prepush_ci_execution.py \
  apps/bt/tests/unit/scripts/test_ci_workflow.py \
  apps/bt/tests/unit/scripts/test_ci_changed_scope.py \
  apps/bt/tests/unit/scripts/test_research_test_targets.py \
  apps/bt/tests/unit/scripts/test_test_taxonomy.py
```

Then run the mapped fast and heavy research targets identified by `scripts/ci/research-test-targets.py`, plus:

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py \
  apps/bt/tests/unit/scripts/test_prepush_ci_execution.py \
  apps/bt/tests/unit/scripts/test_ci_workflow.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py
uv run --project apps/bt python scripts/check-research-guardrails.py
uv run --project apps/bt python scripts/skills/audit_skills.py --strict-legacy
git diff --check
```

Expected: all commands exit zero; no unrelated files are modified.

- [ ] **Step 3: Verify the forced path end-to-end locally**

Run:

```bash
UV_CACHE_DIR=/tmp/uv-cache scripts/prepush-ci.sh --research --skip-install
```

Expected: PASS after `quality:research-guardrails`, fast research tests, and mapped local research tests. It must not report the docs-only early-return message.

- [ ] **Step 4: Review the exact final diff and commits**

Run:

```bash
git status --short --branch
git diff origin/main...HEAD --check
git log --oneline origin/codex/ranking-technical-fit-score..HEAD
git diff --stat origin/codex/ranking-technical-fit-score...HEAD
```

Expected: only the design/plan, two fixes, regression tests, and evidence-required publication files are new relative to the remote PR branch.

- [ ] **Step 5: Push and verify PR #480**

```bash
git push origin codex/ranking-technical-fit-score
```

Use the GitHub connector to verify PR #480 points to the pushed head, remains open, and its latest workflow runs complete successfully. Resolve or reply to the two blocker threads with the exact regression evidence when permitted; do not create a new PR number.

- [ ] **Step 6: Final completion audit**

Confirm each requirement against authoritative evidence:

- invalid intermediate bars do not consume outcome sessions;
- physical canonical row count remains intact;
- docs-only `--research` prepares dependencies and runs research checks;
- plain docs-only keeps early PASS;
- all affected tests/guardrails pass;
- PR #480 contains the commits and is reviewable.

Only then mark the active goal complete.
