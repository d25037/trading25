# Ranking Trend Acceleration Conditional Lift Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build, execute, and publish a PIT-safe Prime-equivalent research experiment that decides whether OLS trend acceleration adds priority ordering inside multiple existing positive-expectation long-candidate groups.

**Architecture:** Extract the existing rolling log-price OLS calculation into a shared analytics helper, then create an independent research domain and runner that reuse the Daily Ranking PIT panel. Candidate membership is frozen from existing Ranking semantics before OLS features are evaluated. Results are aggregated as same-day conditional spreads with deterministic moving-block bootstrap confidence intervals and published through the canonical experiment README.

**Tech Stack:** Python 3.12, pandas, NumPy, DuckDB, pytest, ruff, bt research bundle utilities.

## Global Constraints

- Market scope is PIT Prime-equivalent only: `0111` after the 2022 reorganization and `0101` before it, resolved through `market_code_alias.py` and exact-date `stock_master_daily`.
- Standard and Growth must not appear in defaults, decision gates, or published evidence.
- Signal date `X` may use data through `X` close; forward outcomes begin after `X`.
- Candidate selection must not reference OLS features, R², moving-average slopes, or future-return fields.
- Existing fixed 20D/60D Ranking semantics remain unchanged.
- Publication SoT is the canonical README `## Published Readout`; do not create `summary.json` or legacy digest fields.
- Preserve the user's unrelated `.gitignore` change.

---

### Task 1: Shared Rolling OLS Feature Helper

**Files:**
- Create: `apps/bt/src/domains/analytics/trend_slope_features.py`
- Modify: `apps/bt/src/domains/analytics/ranking_trend_slope_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_trend_slope_features.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py`

**Interfaces:**
- Produces: `rolling_log_slope_features(values: np.ndarray, *, window: int) -> tuple[np.ndarray, np.ndarray]`.
- Preserves: `price_lr_slope_N_pct = (exp(beta * (N - 1)) - 1) * 100` and `R² = corr(x, log(close))²`.

- [ ] **Step 1: Write the failing helper tests**

```python
def test_rolling_log_slope_features_recovers_known_fitted_move() -> None:
    beta = 0.01
    values = np.log(100.0) + beta * np.arange(20, dtype=float)
    slopes, r2 = rolling_log_slope_features(values, window=20)
    assert slopes[-1] == pytest.approx((np.exp(beta * 19) - 1) * 100)
    assert r2[-1] == pytest.approx(1.0)


def test_rolling_log_slope_features_returns_zero_for_flat_window() -> None:
    slopes, r2 = rolling_log_slope_features(np.ones(20), window=20)
    assert slopes[-1] == 0.0
    assert r2[-1] == 0.0
```

- [ ] **Step 2: Run tests and verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_trend_slope_features.py -q`

Expected: FAIL because `trend_slope_features` does not exist.

- [ ] **Step 3: Implement the shared helper and switch the old research to it**

```python
def rolling_log_slope_features(
    values: np.ndarray,
    *,
    window: int,
) -> tuple[np.ndarray, np.ndarray]:
    if window <= 1:
        raise ValueError("window must be greater than 1")
    # Preserve the loop and numerical semantics from the existing research.
```

Remove the private duplicate from `ranking_trend_slope_evidence.py` and import the shared function with the local alias used by its feature builder.

- [ ] **Step 4: Run helper and old-research tests and verify GREEN**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_trend_slope_features.py tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/trend_slope_features.py \
  apps/bt/src/domains/analytics/ranking_trend_slope_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_trend_slope_features.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py
git commit -m "refactor(bt): share trend slope feature calculation"
```

### Task 2: Conditional-Lift Research Domain

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py`

**Interfaces:**
- Produces: `RankingTrendAccelerationConditionalLiftResult` with the ten DataFrames listed in the design.
- Produces: `run_ranking_trend_acceleration_conditional_lift_research(db_path, *, start_date, end_date, horizons, min_observations, bootstrap_resamples, bootstrap_seed, observation_sample_limit)`.
- Produces: `write_ranking_trend_acceleration_conditional_lift_bundle(result, *, output_root, run_id, notes)`.
- Produces: `build_summary_markdown(result) -> str`.

- [ ] **Step 1: Write RED tests for feature boundaries and candidate purity**

```python
@pytest.mark.parametrize(
    ("s20", "s60", "expected"),
    [(2.0, 1.0, True), (1.0, 1.0, False), (1.0, 0.0, False), (-1.0, -2.0, False), (None, 1.0, False)],
)
def test_trend_acceleration_triple_boundaries(s20, s60, expected) -> None:
    assert classify_trend_acceleration_triple(s20, s60) is expected


def test_candidate_predicates_do_not_reference_trend_or_future_columns() -> None:
    forbidden = ("slope", "r2", "forward_", "future_")
    for candidate in CANDIDATE_REGISTRY:
        assert not any(token in candidate.predicate.lower() for token in forbidden)
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement immutable definitions and validation**

```python
@dataclass(frozen=True)
class CandidateDefinition:
    name: str
    predicate: str
    role: Literal["primary", "broad_sensitivity", "nested_sensitivity", "secondary_20d"]


SEGMENTS = (
    ("historical_pre_reorg", date(2017, 1, 1), date(2021, 12, 31)),
    ("historical_post_reorg", date(2022, 1, 1), date(2023, 12, 31)),
    ("recent_hypothesis_origin", date(2024, 1, 1), None),
)
```

Use explicit SQL predicates matching the approved design and validate that only `market_scope='prime'` is constructed.

- [ ] **Step 4: Add RED tests for PIT membership, exclusive slices, and future append stability**

Build a fixture containing `0101`, `0111`, `0112`, and `0113` rows across signal dates. Assert that only exact-date `0101/0111` rows enter the panel, each observation has one exclusive slice, and appending later prices/master rows leaves the earlier cutoff result unchanged.

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q`

Expected: FAIL because the panel and result builder are incomplete.

- [ ] **Step 5: Implement the PIT panel and candidate registry**

Reuse `create_daily_ranking_research_panel`, the existing sector/leadership/ATR builders where semantics match, and `rolling_log_slope_features`. Materialize candidate flags before trend flags. Create the named and mutually exclusive candidate rows with `(code, date, candidate_group)` uniqueness.

- [ ] **Step 6: Add RED tests for same-day pairing, candidate-local ranks, and incomplete forwards**

```python
def test_binary_lift_requires_two_symbols_on_both_sides_same_day() -> None:
    result = run_fixture_research(...)
    assert set(result.conditional_binary_lift_df["paired_date"]) == {"2024-03-05"}


def test_continuous_percentiles_are_candidate_date_local() -> None:
    result = run_fixture_research(...)
    assert result.observation_sample_df.groupby(["candidate_group", "date"])["acceleration_percentile"].max().eq(1.0).all()
```

Run the focused test and verify it fails for the missing aggregations.

- [ ] **Step 7: Implement aggregations and deterministic block bootstrap**

Implement:

```python
def moving_block_bootstrap_ci(
    values: np.ndarray,
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> tuple[float, float, float]:
    # Sample circular contiguous blocks until len(values), truncate, aggregate,
    # and return point estimate plus 2.5/97.5 percentiles.
```

Build binary, fixed 2x2, quintile/IC, top-K, segment/year, bootstrap, and decision-gate tables. Exclude incomplete forward outcomes and enforce the approved minimum group sizes.

- [ ] **Step 8: Add RED then GREEN bundle tests**

Assert `results.duckdb` contains exactly the ten required tables and `summary.md` contains every result section. Run the focused test before and after implementing bundle serialization.

- [ ] **Step 9: Run the complete new-domain test**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q`

Expected: all tests pass.

- [ ] **Step 10: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py
git commit -m "feat(bt): add ranking trend acceleration conditional lift domain"
```

### Task 3: Runner and Experiment Surfaces

**Files:**
- Create: `apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py`
- Create: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`
- Modify: `docs/research/daily-ranking-research-base.md`

**Interfaces:**
- CLI defaults: `--start-date 2017-01-01`, horizons `5,20,60`, Prime-only with no market override.
- CLI controls: `--db-path`, `--start-date`, `--end-date`, `--min-observations`, `--bootstrap-resamples`, `--bootstrap-seed`, `--observation-sample-limit`, and standard bundle-output arguments.

- [ ] **Step 1: Write the runner with a Prime-only interface**

Do not add `--markets`. Pass the fixed `prime` scope through the domain API.

- [ ] **Step 2: Verify runner help**

Run: `uv run --directory apps/bt python scripts/research/run_ranking_trend_acceleration_conditional_lift.py --help`

Expected: exit 0; output contains bootstrap arguments and does not contain `--markets`.

- [ ] **Step 3: Add the canonical README skeleton and indexes**

The README must contain Japanese `Decision`, `Main Findings`, `Interpretation`, `Production Implication`, `Caveats`, and `Source Artifacts`. Before the real run it must state `Decision: 実行結果のpublication待ち`; this line must be replaced during Task 4, not left in the final state.

- [ ] **Step 4: Run guardrails and focused tests**

Run:

```bash
python3 scripts/check-research-guardrails.py
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_trend_slope_features.py \
  tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q
```

Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py \
  apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md \
  apps/bt/docs/experiments/README.md \
  apps/bt/docs/experiments/research-catalog-metadata.toml \
  docs/research/daily-ranking-research-base.md
git commit -m "feat(bt): add ranking trend acceleration research runner"
```

### Task 4: Execute Research and Publish the Readout

**Files:**
- Modify: `apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Durable bundle root: default XDG research root.
- Run ID: `20260718_prime_pit_conditional_lift_v1`.

- [ ] **Step 1: Execute the full research**

Run:

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_trend_acceleration_conditional_lift.py \
  --start-date 2017-01-01 \
  --bootstrap-resamples 2000 \
  --bootstrap-seed 20260718 \
  --run-id 20260718_prime_pit_conditional_lift_v1
```

Expected: exit 0 and emitted paths for `manifest.json`, `results.duckdb`, and `summary.md`.

- [ ] **Step 2: Inspect every decision-gate input from `results.duckdb`**

Use DuckDB queries against `coverage_diagnostics_df`, `conditional_binary_lift_df`, `continuous_rank_lift_df`, `segment_stability_df`, `bootstrap_effect_ci_df`, and `decision_gate_df`. Confirm Prime-equivalent-only coverage and reconcile the gate verdict with its component rows.

- [ ] **Step 3: Replace the README placeholder with the Japanese Published Readout**

Publish the actual decision, compact evidence tables, interpretation, production implication, caveats, exact bundle path, result-table names, and runner command. Do not claim portfolio performance.

- [ ] **Step 4: Update catalog metadata to match the Published Readout**

Set status, decision, risk flags, and related experiments consistently with the README.

- [ ] **Step 5: Commit publication**

```bash
git add apps/bt/docs/experiments/market-behavior/ranking-trend-acceleration-conditional-lift/README.md \
  apps/bt/docs/experiments/research-catalog-metadata.toml
git commit -m "docs(bt): publish ranking trend acceleration readout"
```

### Task 5: Final Verification

**Files:**
- Verify only; modify files only to correct observed failures.

**Interfaces:**
- Confirms research reproducibility, code quality, publication contract, and preservation of unrelated worktree changes.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_trend_slope_features.py \
  tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q
```

- [ ] **Step 2: Run lint and type checks on affected Python files**

Run:

```bash
uv run --directory apps/bt ruff check \
  src/domains/analytics/trend_slope_features.py \
  src/domains/analytics/ranking_trend_slope_evidence.py \
  src/domains/analytics/ranking_trend_acceleration_conditional_lift.py \
  scripts/research/run_ranking_trend_acceleration_conditional_lift.py \
  tests/unit/domains/analytics/test_trend_slope_features.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py
uv run --directory apps/bt pyright \
  src/domains/analytics/trend_slope_features.py \
  src/domains/analytics/ranking_trend_acceleration_conditional_lift.py
```

- [ ] **Step 3: Run research and skill guardrails**

Run:

```bash
python3 scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
```

- [ ] **Step 4: Verify bundle and worktree state**

Confirm the durable bundle files exist, query the result table list, run `git diff --check`, and verify `.gitignore` remains modified only by the user.

- [ ] **Step 5: Report evidence-backed completion**

Report the research verdict, the decision-gate evidence, files changed, bundle path, and every verification command outcome. Do not recommend production API/UI implementation unless the applicable gate passed.
