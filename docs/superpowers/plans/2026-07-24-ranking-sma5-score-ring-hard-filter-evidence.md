# Daily Ranking SMA5 Score-Ring Hard-Filter Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build, run, and publish a Market v5 research bundle that decides whether SMA5 entry and exit hard gates add statistically and operationally significant value inside frozen Value × Long Hybrid score rings.

**Architecture:** Reuse the generation-bound Daily Ranking Market v5 panel and feature builders, then convert frozen score-ring and SMA5 predicates into same-Close vectorbt entry/exit matrices. Keep feature construction, execution accounting, evidence aggregation, and bundle orchestration behind separate functions in one focused analytics module; the CLI only parses inputs and writes the canonical bundle.

**Tech Stack:** Python 3.12, DuckDB, pandas, NumPy, vectorbt through `VectorbtAdapter`, pytest, runner-first research bundles.

## Global Constraints

- Physical `market.duckdb` schema v5 and `stock_price_adjustment_mode=provider_adjusted_v1` are mandatory.
- Universe membership is exact signal-date Prime (`0101`, `0111`); latest/current membership fallback is forbidden.
- Candidate rings are frozen before outcome attachment: both scores `>=0.8`, `>=0.7`, and `>=0.6`.
- The primary ring is `core_high_high`; near rings are robustness checks only.
- Execution policy is `close_proxy_same_session`: the signal and fill both use the same Close, first PnL begins in the next Close-to-Close interval, and the optimistic assumption must be disclosed.
- Entry and exit families are tested separately; a combined confirmatory variant is eligible only when both component families pass.
- Discovery is 2018–2021, validation is 2022–2024, and final holdout is 2025 through the available final date.
- Primary cap is 60 sessions; 20 sessions is a robustness check.
- Publication requires `manifest.json + results.duckdb + summary.md` and a Japanese `## Published Readout`; do not create `summary.json` or `_build_published_summary()`.

---

### Task 1: Pure score-ring and position-state semantics

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py`

**Interfaces:**
- Produces: `classify_score_ring(value_score: object, leadership_score: object) -> str`
- Produces: `entry_rule_matches(row: Mapping[str, object], rule_id: str) -> bool`
- Produces: `exit_rule_matches(row: Mapping[str, object], rule_id: str) -> bool`
- Produces: `build_position_signal_frames(feature_df: pd.DataFrame, *, ring_id: str, entry_rule_id: str, exit_rule_id: str, max_holding_sessions: int) -> PositionSignalFrames`
- `PositionSignalFrames` contains aligned `close`, `entries`, `exits`, `held_intervals`, and `state_events` frames.

- [ ] **Step 1: Write failing classification and rule tests**

Add parameterized tests covering exact score boundaries, missing values, all E0–E4 rules, and all X0–X4 rules:

```python
@pytest.mark.parametrize(
    ("value", "leadership", "expected"),
    [
        (0.80, 0.80, "core_high_high"),
        (0.79, 0.80, "near_high_high_1"),
        (0.70, 0.70, "near_high_high_1"),
        (0.69, 0.70, "near_high_high_2"),
        (0.60, 0.60, "near_high_high_2"),
        (0.59, 0.90, "outside"),
        (None, 0.90, "missing"),
    ],
)
def test_classify_score_ring(value: object, leadership: object, expected: str) -> None:
    assert classify_score_ring(value, leadership) == expected


def test_frozen_entry_and_exit_predicates() -> None:
    row = {
        "close": 101.0,
        "sma5": 100.0,
        "sma5_above_count_5d": 2,
        "sma5_below_streak": 0,
        "sma5_atr20_deviation": 0.75,
    }
    assert entry_rule_matches(row, "E1_close_above_sma5")
    assert entry_rule_matches(row, "E2_count_ge_2")
    assert entry_rule_matches(row, "E3_avoid_atr20_chase")
    assert entry_rule_matches(row, "E4_count_ge_2_and_avoid_chase")
    assert not exit_rule_matches(row, "X1_close_below_sma5")
```

- [ ] **Step 2: Run the focused tests and confirm missing imports fail**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py \
  -q
```

Expected: collection fails because the new module does not exist.

- [ ] **Step 3: Implement the frozen registries and predicate helpers**

Create immutable registries and fail on unknown rule IDs:

```python
SCORE_RING_THRESHOLDS = {
    "core_high_high": 0.80,
    "near_high_high_1": 0.70,
    "near_high_high_2": 0.60,
}
ENTRY_RULE_IDS = (
    "E0_no_sma5_filter",
    "E1_close_above_sma5",
    "E2_count_ge_2",
    "E3_avoid_atr20_chase",
    "E4_count_ge_2_and_avoid_chase",
)
EXIT_RULE_IDS = (
    "X0_no_sma5_exit",
    "X1_close_below_sma5",
    "X2_count_le_1",
    "X3_below_streak_ge_3",
    "X4_atr20_below_le_neg1",
)


def classify_score_ring(value_score: object, leadership_score: object) -> str:
    value = finite_float_or_none(value_score)
    leadership = finite_float_or_none(leadership_score)
    if value is None or leadership is None:
        return "missing"
    if value >= 0.80 and leadership >= 0.80:
        return "core_high_high"
    if value >= 0.70 and leadership >= 0.70:
        return "near_high_high_1"
    if value >= 0.60 and leadership >= 0.60:
        return "near_high_high_2"
    return "outside"
```

Implement entry and exit predicates with explicit numeric-null checks. `E0` is true, `X0` is false, and an active technical rule with missing inputs is false rather than silently passing.

- [ ] **Step 4: Write failing state-transition tests**

Build a two-code synthetic panel and assert:

```python
def test_position_state_enters_on_false_to_true_and_does_not_same_day_reenter() -> None:
    feature_df = _synthetic_feature_frame()
    frames = build_position_signal_frames(
        feature_df,
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )
    events = frames.state_events
    assert events.loc[events["event_type"].eq("entry"), "date"].tolist() == [
        pd.Timestamp("2025-01-02"),
        pd.Timestamp("2025-01-07"),
    ]
    assert not (
        events.groupby(["date", "code"])["event_type"]
        .agg(list)
        .map(lambda values: "entry" in values and "exit" in values)
        .any()
    )


def test_entry_day_return_is_not_booked_and_exit_day_closes_exposure() -> None:
    frames = build_position_signal_frames(
        _synthetic_feature_frame(),
        ring_id="core_high_high",
        entry_rule_id="E2_count_ge_2",
        exit_rule_id="X2_count_le_1",
        max_holding_sessions=60,
    )
    assert not frames.held_intervals.loc[pd.Timestamp("2025-01-02"), "1001"]
    assert frames.held_intervals.loc[pd.Timestamp("2025-01-03"), "1001"]
    assert not frames.held_intervals.loc[pd.Timestamp("2025-01-06"), "1001"]
```

Also cover ring exit, 20/60-session cap, terminal open positions, middle-row missing prices, and re-arm only after eligibility becomes false.

- [ ] **Step 5: Implement deterministic state transitions and aligned matrices**

Use one sorted pass per code and variant. Emit entries and exits at the signal Close, keep `held_intervals` shifted so entry-day return is excluded, and assign exit precedence:

```python
EXIT_PRECEDENCE = ("ring_exit", "sma5_exit", "time_exit", "terminal_exit")

@dataclass(frozen=True)
class PositionSignalFrames:
    close: pd.DataFrame
    entries: pd.DataFrame
    exits: pd.DataFrame
    held_intervals: pd.DataFrame
    state_events: pd.DataFrame
```

Do not forward-fill prices across missing trading rows. Terminal positions close on their last finite Close with `terminal_exit`.

- [ ] **Step 6: Run state tests**

Run the Task 1 test file. Expected: all classification, rule, and state-transition tests pass.

- [ ] **Step 7: Commit Task 1**

```bash
git add \
  apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py
git commit -m "feat(bt): add SMA5 score-ring position semantics"
```

### Task 2: Market v5 feature panel and vectorbt execution

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py`

**Interfaces:**
- Consumes Task 1 `PositionSignalFrames`.
- Produces: `build_score_ring_feature_panel(conn: Any, relations: DailyRankingResearchRelations) -> pd.DataFrame`
- Produces: `execute_variant(feature_df: pd.DataFrame, variant: ResearchVariant, *, fee_bps: float) -> VariantExecution`
- `VariantExecution` contains vectorbt trade records plus normalized daily portfolio returns and state events.

- [ ] **Step 1: Write a failing Market v5 integration test**

Reuse `daily_ranking_market_v5_fixture` and the existing SMA5 fixture builders. Assert:

```python
def test_market_v5_panel_contains_frozen_scores_and_sma_features(tmp_path: Path) -> None:
    db_path = _build_hard_filter_market_v5_db(tmp_path / "market.duckdb")
    result = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        bootstrap_resamples=100,
        min_trades=1,
        min_signal_dates=1,
    )
    assert result.pit_lineage.stock_price_adjustment_mode == "provider_adjusted_v1"
    assert {
        "value_composite_equal_score",
        "long_hybrid_leadership_score",
        "sma5",
        "sma5_above_count_5d",
        "sma5_below_streak",
        "sma5_atr20_deviation",
    }.issubset(result.observation_sample_df.columns)
```

Add fail-closed assertions for schema v4 and a non-provider-adjusted sync mode.

- [ ] **Step 2: Run the integration test and confirm it fails**

Expected: `run_ranking_sma5_score_ring_hard_filter_research` is undefined.

- [ ] **Step 3: Implement Market v5 orchestration**

Follow the established sequence:

```python
relations = build_daily_ranking_research_base(
    conn,
    DailyRankingPanelRequest(
        namespace="sma5_score_ring_hard_filter",
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        horizons=(1,),
        market_scopes=("prime",),
        include_liquidity=True,
        percentile_features=(),
    ),
)
signal_source = relations.ranked_signals
atr = build_atr_features(
    conn,
    AtrFeaturesRequest(source=signal_source, namespace="hard_filter_atr"),
)
short = build_short_scaffold_features(
    conn,
    ShortScaffoldFeaturesRequest(
        source=signal_source,
        atr_features=atr,
        namespace="hard_filter_short",
    ),
)
sector = build_sector_strength_features(
    conn,
    SectorStrengthFeaturesRequest(
        source=signal_source,
        population_source=signal_source,
        namespace="hard_filter_sector",
    ),
)
leadership = build_long_leadership_features(
    conn,
    LongLeadershipFeaturesRequest(
        source=signal_source,
        sector_features=sector,
        namespace="hard_filter_leadership",
        leadership_windows=(120, 252, 504),
    ),
)
sma = build_sma_features(
    conn,
    SmaFeaturesRequest(
        source=signal_source,
        price_history=relations.price_history,
        namespace="hard_filter_sma",
    ),
)
long_scaffold = build_long_scaffold_features(
    conn,
    LongScaffoldFeaturesRequest(
        source=signal_source,
        leadership_features=leadership,
        short_scaffold_features=short,
        namespace="hard_filter_long",
    ),
)
composed = compose_daily_ranking_signal_features(
    conn,
    source=relations.ranked_signals,
    features=(long_scaffold, sma),
    namespace="sma5_score_ring_hard_filter",
)
```

Use the exact builder names and request types already exposed by `daily_ranking_feature_builders.py`; do not duplicate score formulas in pandas.

- [ ] **Step 4: Write a failing vectorbt accounting test**

Mock or run `VectorbtAdapter(engine="numba")` on the synthetic matrices. Assert exact entry/exit timestamps, trade count, and that a 10bps round trip reduces a 10% gross trade to the vectorbt-computed net return rather than a post-hoc row edit.

- [ ] **Step 5: Implement vectorbt execution**

Call vectorbt once per code without shared cash so that its trade ledger remains
the authoritative fill and fee account:

```python
portfolio = VectorbtAdapter(engine="numba").create_signal_portfolio(
    close=frames.close,
    entries=frames.entries,
    exits=frames.exits,
    direction="longonly",
    init_cash=1_000_000.0,
    fees=fee_bps / 20_000.0,
    slippage=0.0,
    cash_sharing=False,
    group_by=False,
    accumulate=False,
    size=1.0,
    size_type="percent",
    freq="D",
)
```

Normalize `portfolio.trades.records_readable` and reconcile every vectorbt trade
to one state event pair. Build the approved date-level equal-weight active
portfolio by averaging the vectorbt per-code returns only across
`held_intervals=true`; charge entry/exit fees on their event dates. Preserve the
independently generated state events so exit reasons remain auditable. Do not
use a shared-cash first-come fill order, because it would turn the result into a
code-order-dependent top-N portfolio.

- [ ] **Step 6: Run focused integration and execution tests**

Expected: Market v5 lineage, feature coverage, same-Close trade timestamps, and fee accounting all pass.

- [ ] **Step 7: Commit Task 2**

```bash
git add \
  apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py
git commit -m "feat(bt): execute score-ring SMA5 variants on Market v5"
```

### Task 3: Evidence tables, bootstrap, Holm correction, and adoption decision

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py`

**Interfaces:**
- Produces: `moving_block_bootstrap_delta_ci(candidate: pd.Series, baseline: pd.Series, *, block_length: int, resamples: int, seed: int) -> BootstrapInterval`
- Produces: `holm_adjust(p_values: Sequence[float | None]) -> list[float | None]`
- Produces: `build_decision_gate_df(evidence_df: pd.DataFrame, annual_stability_df: pd.DataFrame, cost_sensitivity_df: pd.DataFrame) -> pd.DataFrame`
- Produces result tables named exactly as the approved bundle contract.

- [ ] **Step 1: Write failing deterministic statistics tests**

Use fixed synthetic paired return series:

```python
def test_moving_block_bootstrap_is_paired_and_reproducible() -> None:
    baseline = pd.Series([0.0, -0.01, 0.0, -0.01] * 20)
    candidate = baseline + 0.002
    first = moving_block_bootstrap_delta_ci(
        candidate, baseline, block_length=4, resamples=500, seed=20260724
    )
    second = moving_block_bootstrap_delta_ci(
        candidate, baseline, block_length=4, resamples=500, seed=20260724
    )
    assert first == second
    assert first.lower > 0.0


def test_holm_adjustment_preserves_original_order() -> None:
    assert holm_adjust([0.01, 0.04, 0.03, None]) == pytest.approx(
        [0.03, 0.06, 0.06, None]
    )
```

- [ ] **Step 2: Run statistics tests and confirm they fail**

Expected: the new helper functions are undefined.

- [ ] **Step 3: Implement evidence aggregation**

Create the approved tables:

```python
@dataclass(frozen=True)
class RankingSma5ScoreRingHardFilterResult:
    db_path: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    pit_lineage: PitLineageAudit
    rule_registry_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    trade_ledger_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    entry_rule_evidence_df: pd.DataFrame
    exit_rule_evidence_df: pd.DataFrame
    combined_rule_evidence_df: pd.DataFrame
    annual_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame
    cost_sensitivity_df: pd.DataFrame
    decision_gate_df: pd.DataFrame
    observation_sample_df: pd.DataFrame
```

Define a local immutable `HardFilterPitLineage` with
`market_schema_version`, `stock_price_adjustment_mode`, `market_source`, and
`source_mode`; do not couple this experiment to the Technical Fit module's
private lineage dataclass.

For every ring, cap, period, family, and variant, calculate trade count, signal-date count, gross/net mean and median return, annualized IR, max drawdown, 5% expected shortfall, turnover, and paired deltas against the correct baseline.

- [ ] **Step 4: Implement paired moving-block bootstrap and Holm adjustment**

Align candidate and baseline on the union of trading dates, fill inactive strategy return with zero cash return, bootstrap paired deltas with circular moving blocks, and derive a two-sided empirical p-value. Apply Holm independently within the entry and exit primary families.

- [ ] **Step 5: Write failing decision-boundary tests**

Build table fixtures that independently fail each gate: CI crosses zero, adjusted p equals `0.05`, 199 trades, 99 dates, IR lift `0.149`, tail improvement `9.99%`, turnover ratio `1.501`, cost reversal, minority positive years, and holdout sign reversal. Assert `production_candidate` only when every strict boundary passes.

- [ ] **Step 6: Implement the decision gate**

Emit one row per primary variant with boolean columns for every requirement and:

```python
decision = (
    "production_candidate"
    if all(required_gate_values)
    else "insufficient_evidence"
)
```

Emit a separate family outcome (`entry`, `exit`, `combined`) and keep combined `not_evaluated` unless one entry and one exit rule independently pass before holdout evaluation.

- [ ] **Step 7: Run the full domain test file**

Expected: all pure, integration, execution, statistics, and decision tests pass.

- [ ] **Step 8: Commit Task 3**

```bash
git add \
  apps/bt/src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py
git commit -m "feat(bt): evaluate SMA5 hard-filter adoption gates"
```

### Task 4: Runner and bundle contract

**Files:**
- Create: `apps/bt/scripts/research/run_ranking_sma5_score_ring_hard_filter_evidence.py`
- Create: `apps/bt/tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py`

**Interfaces:**
- Produces CLI `main(argv: list[str] | None = None) -> int`.
- Produces `write_ranking_sma5_score_ring_hard_filter_bundle(result, *, output_root, run_id, notes) -> ResearchBundleInfo`.
- Bundle contains the twelve approved result tables with stable names.

- [ ] **Step 1: Write failing CLI parse and bundle tests**

Assert defaults and explicit overrides for database path, dates, block length, resamples, seed, minimum trades/dates, cost levels, and output arguments. Run the domain result into a temporary bundle and assert exact table names through DuckDB:

```python
expected_tables = {
    "rule_registry_df",
    "coverage_diagnostics_df",
    "trade_ledger_df",
    "portfolio_daily_df",
    "entry_rule_evidence_df",
    "exit_rule_evidence_df",
    "combined_rule_evidence_df",
    "annual_stability_df",
    "bootstrap_effect_ci_df",
    "cost_sensitivity_df",
    "decision_gate_df",
    "observation_sample_df",
}
```

- [ ] **Step 2: Run runner tests and confirm missing module failure**

Expected: runner import fails because the file does not exist.

- [ ] **Step 3: Implement CLI and bundle writer**

Follow `run_ranking_sma5_position_state_evidence.py` and `scripts/research/common.py`. Default dates and gates must match the approved design. Store in manifest:

```python
result_metadata={
    "execution_policy": "close_proxy_same_session",
    "execution_is_optimistic": True,
    "stock_price_adjustment_mode": "provider_adjusted_v1",
    "primary_ring": "core_high_high",
    "primary_holding_cap": 60,
    "robustness_holding_cap": 20,
    "discovery_period": ["2018-01-01", "2021-12-31"],
    "validation_period": ["2022-01-01", "2024-12-31"],
    "holdout_period": ["2025-01-01", result.analysis_end_date],
}
```

- [ ] **Step 4: Run runner/bundle tests**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py \
  tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py
```

Expected: tests pass and the temporary bundle contains no `summary.json` or
digest fallback.

- [ ] **Step 5: Commit Task 4**

```bash
git add \
  apps/bt/scripts/research/run_ranking_sma5_score_ring_hard_filter_evidence.py \
  apps/bt/tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py
git commit -m "feat(bt): add SMA5 hard-filter research runner"
```

### Task 5: Execute Market v5 research and publish the readout

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-hard-filter-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes the completed runner.
- Produces one immutable bundle under `~/.local/share/trading25/research/market-behavior/ranking-sma5-score-ring-hard-filter-evidence/<run_id>/`.
- Produces the canonical Japanese Published Readout.

- [ ] **Step 1: Run a bounded smoke bundle**

Run a short 2024 slice with 100 bootstrap resamples and a distinct smoke run ID. Confirm all tables are non-empty where applicable and the manifest records Market v5/provider-adjusted provenance.

- [ ] **Step 2: Run the canonical full bundle**

Run:

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_sma5_score_ring_hard_filter_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --start-date 2018-01-01 \
  --end-date 2026-07-21 \
  --bootstrap-resamples 2000 \
  --bootstrap-seed 20260724 \
  --run-id 20260724_prime_v5_sma5_score_ring_hard_filter_v1
```

Expected: the command emits JSON paths for `manifest.json`, `results.duckdb`, and `summary.md`.

- [ ] **Step 3: Independently inspect result tables**

Use read-only DuckDB queries to compare `decision_gate_df`, `bootstrap_effect_ci_df`, `entry_rule_evidence_df`, `exit_rule_evidence_df`, `annual_stability_df`, and `cost_sensitivity_df`. Verify sample coverage and that no holdout-driven rule mutation occurred.

- [ ] **Step 4: Write the Japanese Published Readout**

Replace `pending_run` with the exact run ID and evidence-backed conclusion. Include:

- `Decision`: entry, exit, and combined family decisions.
- `Main Findings`: one `#### 結論` plus a pipe table per distinct conclusion.
- `Interpretation`: distinguish weak-state avoidance, chase avoidance, and early-exit winner truncation.
- `Production Implication`: no automatic strategy/UI change; identify only rules that passed both gates.
- `Caveats`: same-Close optimistic fill, equal-weight portfolio approximation, costs, capacity, incomplete 2026.
- `Source Artifacts`: runner, module, tests, bundle path, and exact result table names.

- [ ] **Step 5: Verify publication and full research suite**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py \
  tests/unit/scripts/test_run_ranking_sma5_score_ring_hard_filter_evidence.py
python3 scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
scripts/prepush-ci.sh --research
```

Expected: all commands pass.

- [ ] **Step 6: Commit the canonical publication**

```bash
git add \
  apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-hard-filter-evidence/README.md \
  apps/bt/docs/experiments/research-catalog-metadata.toml
git commit -m "docs(bt): publish SMA5 hard-filter evidence"
```

### Task 6: Final verification and handoff

**Files:**
- Read-only verification of all files and bundle artifacts from Tasks 1–5.

**Interfaces:**
- Produces a final evidence-backed handoff with exact decisions and artifact paths.

- [ ] **Step 1: Inspect repository state and commit scope**

Run `git status --short`, `git log -6 --oneline`, and `git diff HEAD~5 --stat`. Confirm no unrelated user changes were included.

- [ ] **Step 2: Recompute key published metrics**

Read `results.duckdb` directly and independently recompute the metrics quoted in Published Readout. Confirm they match exactly.

- [ ] **Step 3: Apply verification-before-completion**

Review fresh output from the focused tests, research guardrails, skill audit, and research pre-push suite. Do not claim completion from earlier cached output.

- [ ] **Step 4: Report outcome**

Lead with whether any entry or exit rule passed both the statistical and operational gates. Link the canonical README and implementation files, name the canonical bundle, disclose same-Close optimism, and summarize tests.
