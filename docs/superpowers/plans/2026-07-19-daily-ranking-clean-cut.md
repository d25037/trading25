# Daily Ranking Clean-Cut Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unsafe Daily Ranking research facade and duplicated production semantics with a shared, PIT-safe, reusable, deterministic, and faster Daily Ranking core, migrate every consumer, and verify all affected publications.

**Architecture:** A pure `daily_ranking_core.py` owns signal-date policies shared by production and research. A generic Market v4 event-time price module supplies signal-only relations to production and separate signal/outcome relations to the typed research base. Research selection is frozen before outcomes are attached, and all legacy fixed temp relations and cross-experiment private builder dependencies are removed after consumer migration.

**Tech Stack:** Python 3.12, DuckDB, pandas, NumPy, Pydantic v2 contracts, FastAPI, pytest, Ruff, Pyright, Bun/TypeScript OpenAPI contracts.

## Global Constraints

- Preserve production Daily Ranking request and response fields and frontend behavior.
- Add only the standardized HTTP 409 `adjusted_metrics_pit` recovery response to the endpoint contract.
- Production code must never import research outcome or bundle modules.
- Canonical research price input is Market v4 `stock_data_raw` plus ready event-time bases and segments; no `stock_data` fallback exists.
- Signal relations must contain no `forward_*` columns.
- Candidate membership is frozen from signal columns before outcomes are attached.
- Missing selected outcomes never backfill and fail closed for effect metrics and decision gates.
- Percentile ties preserve equal values; fixed-size selections use normalized code as the final tie-breaker.
- Relation schemas use explicit columns and `(code, date)` keys are unique.
- Optional features do no work when disabled.
- Existing user changes outside the PR worktree are untouched.

---

## File Structure

### New modules

- `apps/bt/src/domains/analytics/daily_ranking_core.py`: pure valuation, liquidity, technical, percentile, and scope semantics.
- `apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py`: reusable Market v4 signal-price and forward-outcome SQL/materialization.
- `apps/bt/src/domains/analytics/daily_ranking_feature_builders.py`: public ATR, sector, PSR, SMA, ROE, long/short scaffold overlay builders extracted from experiment modules.
- `apps/bt/tests/unit/domains/analytics/test_daily_ranking_core.py`: shared-core boundary and SQL/Python conformance tests.
- `apps/bt/tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py`: Market v4 projection, lineage, and cardinality tests.
- `apps/bt/tests/unit/domains/analytics/test_daily_ranking_research_base.py`: typed relation, lifecycle, and schema tests.
- `apps/bt/tests/unit/domains/analytics/test_daily_ranking_production_research_conformance.py`: production/research adapter parity tests.
- `apps/bt/scripts/research/publish_ranking_research.py`: generated immutable publication digest command.
- `apps/bt/tests/unit/scripts/test_publish_ranking_research.py`: digest generation and verification tests.

### Replaced ownership

- `apps/bt/src/domains/analytics/daily_ranking_research_base.py`: owns typed request/result and canonical research relation orchestration.
- `apps/bt/src/domains/analytics/ranking_research_selection_contract.py`: owns frozen percentile, tails, and top-K selection plus outcome evaluation.
- `apps/bt/src/domains/analytics/ranking_color_evidence.py`: becomes a consumer; no shared panel builder remains in this file.
- `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py`: retains only Technical Fit compatibility exports until all imports migrate, then is deleted or reduced to experiment-specific audit code.

---

### Task 1: Shared Signal Core and Conformance Contract

**Files:**
- Create: `apps/bt/src/domains/analytics/daily_ranking_core.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_core.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_production_research_conformance.py`
- Modify: `apps/bt/src/application/services/ranking_liquidity.py`
- Modify: `apps/bt/src/application/services/ranking_collection_filters.py`
- Modify: `apps/bt/src/application/services/ranking_state_flags.py`
- Modify: `apps/bt/src/domains/analytics/daily_ranking_research_base.py`

**Interfaces:**
- Produces `DailyRankingValuationMetrics`, `DailyRankingValuationState`, `DailyRankingLiquidityInputs`, `DailyRankingLiquidityState`, `DailyRankingTechnicalInputs`, `DailyRankingTechnicalState`.
- Produces `classify_valuation_state()`, `classify_liquidity_state()`, `classify_technical_state()`, `percent_rank_sql()`, and `normalize_percentile_population()`.
- Production adapters map `atr20_acceleration_ex_overheat` back to the existing API value `atr20_acceleration`.

- [ ] **Step 1: Write failing pure-policy boundary tests**

```python
def test_liquidity_policy_uses_production_boundaries() -> None:
    crowded = classify_liquidity_state(
        DailyRankingLiquidityInputs(
            residual_z=1.0,
            recent_return_20d_pct=0.01,
            recent_return_60d_pct=0.01,
        )
    )
    zero_boundary = classify_liquidity_state(
        DailyRankingLiquidityInputs(
            residual_z=1.0,
            recent_return_20d_pct=0.0,
            recent_return_60d_pct=1.0,
        )
    )
    assert crowded.regime == "crowded_rerating"
    assert zero_boundary.regime == "distribution_stress"


def test_technical_state_has_canonical_internal_name() -> None:
    state = classify_technical_state(
        DailyRankingTechnicalInputs(
            atr20_change_20d_pct=2.0,
            recent_return_20d_pct=29.99,
            recent_return_60d_percentile=0.9,
            recent_return_20d_percentile=0.9,
        )
    )
    assert state.atr20_acceleration_ex_overheat
    assert state.api_flags == ("atr20_acceleration", "momentum_20_60_top20")
```

- [ ] **Step 2: Run the new tests and verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_core.py -q`

Expected: collection fails because `daily_ranking_core` and its public types do not exist.

- [ ] **Step 3: Implement immutable policy types and pure classifiers**

```python
@dataclass(frozen=True)
class DailyRankingLiquidityInputs:
    residual_z: float | None
    recent_return_20d_pct: float | None
    recent_return_60d_pct: float | None


@dataclass(frozen=True)
class DailyRankingLiquidityState:
    regime: LiquidityRegime


def classify_liquidity_state(inputs: DailyRankingLiquidityInputs) -> DailyRankingLiquidityState:
    returns = (inputs.recent_return_20d_pct, inputs.recent_return_60d_pct)
    complete = all(value is not None and math.isfinite(value) for value in returns)
    runup = complete and all(cast(float, value) > 0.0 for value in returns)
    if inputs.residual_z is None or not math.isfinite(inputs.residual_z):
        return DailyRankingLiquidityState("missing")
    if inputs.residual_z >= 1.0 and complete:
        return DailyRankingLiquidityState(
            "crowded_rerating" if runup else "distribution_stress"
        )
    if inputs.residual_z <= -1.0:
        return DailyRankingLiquidityState("stale_liquidity")
    if -1.0 < inputs.residual_z < 1.0 and runup:
        return DailyRankingLiquidityState("neutral_rerating")
    return DailyRankingLiquidityState("neutral")
```

- [ ] **Step 4: Add SQL/Python conformance fixtures**

Create a DuckDB `VALUES` table containing null, threshold, and tie cases. Generate SQL expressions from the policy, fetch SQL states, calculate Python states from the same rows, and assert exact equality for valuation, liquidity, and technical outputs.

- [ ] **Step 5: Run shared-core and existing production classifier tests**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_core.py tests/unit/server/services/test_ranking_service.py -k 'liquidity or collection_filter or technical_flag' -q`

Expected: all tests pass with no warning.

- [ ] **Step 6: Migrate production classifiers to core adapters**

Replace local threshold implementations with calls to the pure classifiers. Keep response strings and Pydantic fields unchanged.

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/domains/analytics/daily_ranking_core.py apps/bt/src/domains/analytics/daily_ranking_research_base.py apps/bt/src/application/services/ranking_liquidity.py apps/bt/src/application/services/ranking_collection_filters.py apps/bt/src/application/services/ranking_state_flags.py apps/bt/tests/unit/domains/analytics/test_daily_ranking_core.py apps/bt/tests/unit/domains/analytics/test_daily_ranking_production_research_conformance.py
git commit -m "refactor(bt): unify daily ranking signal semantics"
```

### Task 2: Production Event-Time Signal Data

**Files:**
- Create: `apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py`
- Modify: `apps/bt/src/application/services/ranking_query_helpers.py`
- Modify: `apps/bt/src/application/services/ranking_daily_queries.py`
- Modify: `apps/bt/src/application/services/ranking_daily_technical_metrics.py`
- Modify: `apps/bt/src/application/services/ranking_technical_flags.py`
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`
- Test: `apps/bt/tests/unit/server/routes/test_analytics_complex.py`

**Interfaces:**
- Produces `EventTimeSignalRequest`, `EventTimeSignalSql`, and `build_event_time_signal_sql(request)`.
- The SQL result contains normalized code, date, raw projected OHLCV, lagged closes, recent returns, and consumed signal basis ID; it has no outcome columns.

- [ ] **Step 1: Add a failing poisoned-convenience-row production test**

```python
def test_historical_rankings_ignore_poisoned_stock_data_convenience_rows(
    ranking_db: str,
) -> None:
    baseline = RankingService(MarketDbReader(ranking_db)).get_rankings(
        date="2024-06-20", markets="prime", limit=0
    )
    with duckdb.connect(ranking_db) as conn:
        conn.execute("UPDATE stock_data SET close = close * 99 WHERE date <= '2024-06-20'")
    rerun = RankingService(MarketDbReader(ranking_db)).get_rankings(
        date="2024-06-20", markets="prime", limit=0
    )
    assert ranking_payload_without_timestamp(rerun) == ranking_payload_without_timestamp(baseline)
```

- [ ] **Step 2: Verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py -k poisoned_stock_data -q`

Expected: ranking values differ because production queries read `stock_data`.

- [ ] **Step 3: Extract signal-only raw projection SQL**

The builder must normalize aliases, reject conflicting duplicates, resolve exactly one ready signal-date basis, require one covering segment per raw date, and project OHLCV without constructing completion requests.

- [ ] **Step 4: Use the event-time CTE in all five production daily query families and technical loaders**

`ranking_by_trading_value`, `ranking_by_trading_value_average`, price change, period high/low, daily technical metrics, and technical flags receive projected signal rows through shared SQL rather than `stock_data_dedup_cte`.

- [ ] **Step 5: Add lineage failure route tests**

```python
def test_ranking_returns_adjusted_metrics_pit_recovery_for_missing_basis(client) -> None:
    delete_ready_basis_for("1111", "2024-06-20")
    response = client.get("/api/analytics/ranking?date=2024-06-20&limit=20")
    assert response.status_code == 409
    assert response.json()["details"][0]["recoveryStage"] == "adjusted_metrics_pit"
```

- [ ] **Step 6: Run production ranking tests**

Run: `uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py tests/unit/server/routes/test_analytics_complex.py -q`

Expected: all pass; request/response payload fields are unchanged.

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py apps/bt/src/application/services/ranking_query_helpers.py apps/bt/src/application/services/ranking_daily_queries.py apps/bt/src/application/services/ranking_daily_technical_metrics.py apps/bt/src/application/services/ranking_technical_flags.py apps/bt/src/application/services/ranking_service.py apps/bt/tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py apps/bt/tests/unit/server/services/test_ranking_service.py apps/bt/tests/unit/server/routes/test_analytics_complex.py
git commit -m "refactor(bt): make daily ranking prices event-time safe"
```

### Task 3: Generic Forward Outcome Projection

**Files:**
- Modify: `apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py`
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py`

**Interfaces:**
- Produces `DailyRankingPriceRelations(signal_features, forward_outcomes, lineage, diagnostics)`.
- Produces `build_daily_ranking_event_time_prices(conn, request) -> DailyRankingPriceRelations`.

- [ ] **Step 1: Add failing sparse-session and basis-lineage tests**

Create a fixture where the stock skips a TOPIX session, completion uses a later basis, N225 has both dates, and `stock_data` is poisoned. Assert stock, TOPIX, and N225 all terminate at the authoritative stock completion date and each endpoint uses its required basis.

- [ ] **Step 2: Verify RED against the new generic API**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py -q`

Expected: failure because the forward builder and result types do not exist.

- [ ] **Step 3: Move the tested generic projection from the Technical Fit module**

Preserve its proven basis/segment validation, replace Technical-specific relation names with validated namespace names, compute diagnostics in one aggregate query per materialized relation, and expose explicit schemas.

- [ ] **Step 4: Add cardinality invariants**

Assert:

```python
assert diagnostics.signal_feature_rows == diagnostics.signal_request_rows
assert diagnostics.outcome_request_rows == diagnostics.signal_request_rows * len(request.horizons)
assert diagnostics.endpoint_rows == 2 * diagnostics.completed_request_rows
assert diagnostics.forward_outcome_rows <= diagnostics.signal_request_rows
```

- [ ] **Step 5: Migrate Technical Fit imports and run projection suites**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py -q`

Expected: all pass, including alias conflict, split, reverse split, future append, and sparse completion tests.

- [ ] **Step 6: Commit**

```bash
git add apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py apps/bt/tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py
git commit -m "refactor(bt): generalize ranking event-time outcomes"
```

### Task 4: Typed Namespaced Research Base

**Files:**
- Replace: `apps/bt/src/domains/analytics/daily_ranking_research_base.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_research_base.py`
- Modify: `apps/bt/src/domains/analytics/ranking_color_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_color_evidence.py`

**Interfaces:**
- Produces `RelationRef`, `DailyRankingPanelRequest`, `DailyRankingResearchRelations`, `DailyRankingLineageAudit`, and `DailyRankingBuildDiagnostics` exactly as specified in the design.
- Produces `build_daily_ranking_research_base(conn, request) -> DailyRankingResearchRelations`.
- Produces `materialize_daily_ranking_signal_cohort(conn, relations, *, source, name, columns, predicate, derived_columns, order_by, limit) -> RelationRef`; the source must be a capability-bearing signal `RelationRef` returned by the same build and predicate/projection expressions are validated. `attach_daily_ranking_outcomes(conn, cohort, relations, *, name) -> RelationRef` accepts only a registered frozen cohort from that build.

- [ ] **Step 1: Write failing relation lifecycle and schema tests**

Tests must prove two namespaces coexist, optional liquidity is `None`, rebuilding without liquidity cannot resolve stale state, every published relation has an exact column/type schema, all dates are `DATE`, all keys are unique, and no signal relation column starts with `forward_`.

- [ ] **Step 2: Verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_research_base.py -q`

Expected: import failure for the new typed API.

- [ ] **Step 3: Implement request validation and explicit relation schemas**

```python
_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class RelationRef:
    name: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    row_count: int

    def __post_init__(self) -> None:
        if not _NAMESPACE_RE.fullmatch(self.name):
            raise ValueError(f"invalid DuckDB relation name: {self.name}")
```

- [ ] **Step 4: Build signal panel and ranked signals without outcomes**

Use exact-date market membership, cutoff-valid valuation basis, one percentile window stage, the shared core SQL policies, and an optional liquidity stage. Join outcomes only in `attach_daily_ranking_outcomes` after a consumer cohort relation exists.

- [ ] **Step 5: Make Ranking Color a normal consumer**

Its runner builds the base, freezes the full signal universe and valuation buckets from `ranked_signals`, then attaches outcomes. Delete the private shared builder functions from `ranking_color_evidence.py` as their callers migrate.

- [ ] **Step 6: Run base and Ranking Color suites**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_research_base.py tests/unit/domains/analytics/test_ranking_color_evidence.py -q`

Expected: all pass, including SQL/Python conformance and future append stability.

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/domains/analytics/daily_ranking_research_base.py apps/bt/src/domains/analytics/ranking_color_evidence.py apps/bt/tests/unit/domains/analytics/test_daily_ranking_research_base.py apps/bt/tests/unit/domains/analytics/test_ranking_color_evidence.py
git commit -m "refactor(bt): replace daily ranking research base"
```

### Task 5: General Selection-First API

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_research_selection_contract.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_research_selection_contract.py`

**Interfaces:**
- Produces `FrozenSignalSelection` and `EvaluatedSignalSelection`.
- Produces `freeze_signal_topk()`, `freeze_signal_tails()`, `freeze_signal_percentile_buckets()`, and `evaluate_frozen_selection()`.
- Selection functions accept no outcome parameter.

- [ ] **Step 1: Add failing permutation, tie, and missing-outcome tests**

```python
def test_frozen_tails_do_not_backfill_missing_best_outcome() -> None:
    frozen = freeze_signal_tails(
        frame,
        group_columns=("date",),
        score_columns=("score",),
        fraction=0.2,
        min_side=2,
    )
    evaluated = evaluate_frozen_selection(frozen, outcomes, outcome_column="outcome")
    assert tuple(frozen.top["code"]) == ("0009", "0010")
    assert evaluated.outcome_status == "incomplete"
    assert evaluated.effect_metrics is None
```

- [ ] **Step 2: Verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_research_selection_contract.py -q`

Expected: new APIs are missing or existing Top-K behavior cannot satisfy tails/percentile cases.

- [ ] **Step 3: Implement selection-only ranking and separate evaluation**

Stable-sort by group, score columns, and normalized code; compute membership before merging the unique outcome frame. Reject duplicate keys and any score/group column whose name begins with `forward_` or equals the declared outcome column.

- [ ] **Step 4: Run contract tests**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_research_selection_contract.py -q`

Expected: all pass for shuffled input, all-score ties, incomplete outcome, and duplicate-key rejection.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_research_selection_contract.py apps/bt/tests/unit/domains/analytics/test_ranking_research_selection_contract.py
git commit -m "refactor(bt): enforce selection-first ranking research"
```

### Task 6: Fix All Known Outcome-Dependent Selections

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_daily_triage_lens.py`
- Modify: `apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py`
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Modify: `apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py`
- Test: corresponding four `apps/bt/tests/unit/domains/analytics/test_*.py` files

**Interfaces:**
- Consumes the Task 5 frozen selection API.
- Produces unchanged result-table column contracts plus explicit coverage/status fields where absent.

- [ ] **Step 1: Add one failing highest-score-missing-outcome test per consumer**

Each test records the selected code set before outcomes, removes the highest selected outcome, and asserts no lower-ranked code appears. Technical Fit's existing “drop incomplete outcomes” test is rewritten to require preserved membership and fail-closed metrics.

- [ ] **Step 2: Verify all four tests fail for the expected backfill/re-ranking reason**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_daily_triage_lens.py tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -k 'missing_outcome or selection' -q`

- [ ] **Step 3: Replace Daily Triage and Fixed tail selection**

Daily Triage freezes date-local Top-K from `triage_score`. Fixed continuous, TOPIX/N225 sensitivity, and sector-equal sensitivity freeze 20% tails from the signal priority before evaluating each outcome.

- [ ] **Step 4: Replace Technical and Trend selection**

Technical OOS comparison freezes candidate count and 30% sides before outcomes. Trend uses the already signal-time `acceleration_percentile`; it does not recompute percentiles from complete outcomes.

- [ ] **Step 5: Ensure downstream bootstrap/gates consume complete evaluated rows only**

Coverage rows remain published, but incomplete effect rows are excluded from stability, bootstrap, and decisions by `outcome_status == "complete"`.

- [ ] **Step 6: Run all four full suites**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_daily_triage_lens.py tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py -q`

Expected: all pass; no test encodes outcome-first membership.

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_daily_triage_lens.py apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py apps/bt/src/domains/analytics/ranking_trend_acceleration_conditional_lift.py apps/bt/tests/unit/domains/analytics/test_ranking_daily_triage_lens.py apps/bt/tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py
git commit -m "fix(bt): freeze all ranking cohorts before outcomes"
```

### Task 7: Public Feature Builders

**Files:**
- Create: `apps/bt/src/domains/analytics/daily_ranking_feature_builders.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_feature_builders.py`
- Modify source owners: `atr_expansion_forward_response.py`, `ranking_sector_strength_evidence.py`, `ranking_short_red_evidence.py`, `ranking_long_scaffold_value_composite_evidence.py`, `ranking_psr_valuation_evidence.py`, `ranking_roe_quality_evidence.py`, and SMA evidence modules.

**Interfaces:**
- Produces named builders `build_atr_features`, `build_sector_strength_features`, `build_psr_features`, `build_sma_features`, `build_roe_features`, `build_long_scaffold_features`, and `build_short_scaffold_features`.
- Every builder consumes `RelationRef`, creates a namespaced explicit-schema relation, and returns `RelationRef`.

- [ ] **Step 1: Add failing architecture and builder contract tests**

The test scans AST imports and rejects any `ranking_*` experiment importing an underscore-prefixed symbol from another experiment. Builder fixtures assert key uniqueness, explicit columns, and absence of outcome columns.

- [ ] **Step 2: Verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_daily_ranking_feature_builders.py tests/unit/architecture/test_layer_boundaries.py -q`

Expected: current private cross-experiment imports are reported.

- [ ] **Step 3: Extract builders without changing formulas**

Move one feature family at a time, replace fixed temp names with the request namespace, and retain the original experiment helper only until its last caller migrates in Tasks 8–10.

- [ ] **Step 4: Verify feature parity**

For each extracted builder, run old and new implementations on the same deterministic fixture and assert identical sorted rows and schema before deleting the old implementation.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/daily_ranking_feature_builders.py apps/bt/tests/unit/domains/analytics/test_daily_ranking_feature_builders.py apps/bt/src/domains/analytics/atr_expansion_forward_response.py apps/bt/src/domains/analytics/ranking_sector_strength_evidence.py apps/bt/src/domains/analytics/ranking_short_red_evidence.py apps/bt/src/domains/analytics/ranking_long_scaffold_value_composite_evidence.py apps/bt/src/domains/analytics/ranking_psr_valuation_evidence.py apps/bt/src/domains/analytics/ranking_roe_quality_evidence.py
git commit -m "refactor(bt): publish daily ranking feature builders"
```

### Task 8: Migrate Valuation and Liquidity Research Consumers

**Files:**
- Modify: `ranking_crowded_long_tail_evidence.py`, `ranking_daily_triage_lens.py`, `ranking_forecast_operating_profit_growth_evidence.py`, `ranking_liquidity_z_long_evidence.py`, `ranking_long_scaffold_value_composite_evidence.py`, `ranking_psr_valuation_evidence.py`, `ranking_roe_quality_evidence.py`, `ranking_short_red_evidence.py`, `ranking_short_value_composite_evidence.py` under `apps/bt/src/domains/analytics/`.
- Modify corresponding unit tests under `apps/bt/tests/unit/domains/analytics/`.

**Interfaces:**
- Consumers call `build_daily_ranking_research_base` and public feature builders.
- Only Forecast Operating Profit Growth requests relation percentiles.

- [ ] **Step 1: Parameterize consumer contract tests over this file group**

Assert each runner uses a unique namespace, builds signal cohorts before outcomes, and contains no `ranking_color_*` relation literal or call to the removed query-bound helpers.

- [ ] **Step 2: Verify RED against current consumers**

Run the parameterized architecture test and confirm it reports every unmigrated file in the group.

- [ ] **Step 3: Migrate each runner to typed relations**

Request only the features it consumes. Replace local PSR/valuation percentile duplication with the public builder or core percentile expression. Preserve bundle table schemas unless the selection-first status contract requires additive coverage columns.

- [ ] **Step 4: Run all group tests**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_crowded_long_tail_evidence.py \
  tests/unit/domains/analytics/test_ranking_daily_triage_lens.py \
  tests/unit/domains/analytics/test_ranking_forecast_operating_profit_growth_evidence.py \
  tests/unit/domains/analytics/test_ranking_liquidity_z_long_evidence.py \
  tests/unit/domains/analytics/test_ranking_long_scaffold_value_composite_evidence.py \
  tests/unit/domains/analytics/test_ranking_psr_valuation_evidence.py \
  tests/unit/domains/analytics/test_ranking_roe_quality_evidence.py \
  tests/unit/domains/analytics/test_ranking_short_red_evidence.py \
  tests/unit/domains/analytics/test_ranking_short_value_composite_evidence.py -q
```

Expected: all group tests pass and the architecture parameterization reports zero legacy dependencies for this group.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics apps/bt/tests/unit/domains/analytics
git commit -m "refactor(bt): migrate ranking value and liquidity research"
```

Before staging, verify `git diff --name-only` contains only the listed consumer and test files plus shared files already changed by this task.

### Task 9: Migrate Sector, Trend, and Benchmark Consumers

**Files:**
- Modify under `apps/bt/src/domains/analytics/`: `ranking_sector_strength_evidence.py`, `ranking_short_sector_strength_evidence.py`, `ranking_long_scaffold_factor_cross_evidence.py`, `ranking_n225_neutral_rerating_benchmark.py`, `ranking_trend_slope_evidence.py`, `ranking_trend_acceleration_conditional_lift.py`, `ranking_fixed_return_priority_evidence.py`, `ranking_technical_fit_score_shape_evidence.py`, `ranking_core_factor_regime_breakdown.py`, `ranking_core_sector_neutral_value_regime_breakdown.py`, `ranking_core_sector_relative_value_evidence.py`, `ranking_long_sector_leadership_horizon_decomposition.py`.
- Modify corresponding tests.

**Interfaces:**
- Uses typed base and public sector/ATR/scaffold builders.
- N225, Fixed, Trend Acceleration, and Technical consume authoritative completion-aligned outcomes.

- [ ] **Step 1: Add a failing group architecture test**

Reject legacy `ranking_color_ranked`, `ranking_color_liquidity_ranked`, raw table strings, unnecessary relation percentile requests, and direct imports from Technical Fit price projection.

- [ ] **Step 2: Verify RED and migrate the twelve consumers**

Replace legacy aliases with returned relation names, request only used capabilities, and attach outcomes after each signal cohort is materialized.

- [ ] **Step 3: Run all twelve full suites and the event-time projection suite**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sector_strength_evidence.py \
  tests/unit/domains/analytics/test_ranking_short_sector_strength_evidence.py \
  tests/unit/domains/analytics/test_ranking_long_scaffold_factor_cross_evidence.py \
  tests/unit/domains/analytics/test_ranking_n225_neutral_rerating_benchmark.py \
  tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py \
  tests/unit/domains/analytics/test_ranking_trend_acceleration_conditional_lift.py \
  tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py \
  tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py \
  tests/unit/domains/analytics/test_ranking_core_factor_regime_breakdown.py \
  tests/unit/domains/analytics/test_ranking_core_sector_neutral_value_regime_breakdown.py \
  tests/unit/domains/analytics/test_ranking_core_sector_relative_value_evidence.py \
  tests/unit/domains/analytics/test_ranking_long_sector_leadership_horizon_decomposition.py \
  tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py -q
```

Expected: all pass; N225 completion and basis lineage tests remain green.

- [ ] **Step 4: Commit**

```bash
git add apps/bt/src/domains/analytics apps/bt/tests/unit/domains/analytics
git commit -m "refactor(bt): migrate ranking trend and sector research"
```

Before staging, verify the diff is restricted to the twelve consumers, their tests, and shared builder files changed by the task.

### Task 10: Migrate SMA and Price-Action Consumers

**Files:**
- Modify under `apps/bt/src/domains/analytics/`: `ranking_moving_average_replacement_evidence.py`, `ranking_sma5_atr_deviation_evidence.py`, `ranking_sma5_below_streak_evidence.py`, `ranking_sma5_count_long_evidence.py`, `ranking_sma5_count_short_evidence.py`, `ranking_sma5_deviation_evidence.py`, `ranking_sma5_position_state_evidence.py`, `ranking_liquidity_price_action_recomposition.py`, `atr_expansion_forward_response.py`.
- Modify corresponding tests.

**Interfaces:**
- Uses event-time signal prices for all SMA/EMA/ATR features.
- `ranking_sma5_position_state_evidence` uses the common authoritative next-session outcome relation instead of rebuilding outcomes from `stock_data`.

- [ ] **Step 1: Add failing poisoned-`stock_data` and sparse-session tests for each distinct SMA path**

The tests mutate only `stock_data` and append future raw/basis rows, then assert earlier SMA states, candidate membership, and next-session results remain unchanged.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_moving_average_replacement_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_atr_deviation_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_below_streak_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_count_long_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_count_short_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_deviation_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_position_state_evidence.py \
  tests/unit/domains/analytics/test_ranking_liquidity_price_action_recomposition.py \
  tests/unit/domains/analytics/test_atr_expansion_forward_response.py \
  -k 'poisoned or sparse or future' -q
```

Confirm current paths consume convenience data or rebuild independent completion dates.

- [ ] **Step 3: Migrate SMA and next-session calculations**

Compute all rolling features from event-time signal prices and obtain next-session endpoints from the common outcome builder. Remove raw public table string formatting.

- [ ] **Step 4: Run all nine full suites**

Run the same command as Step 2 without the `-k` expression.

Expected: all pass with no use of `stock_data` in cutoff-aware research code.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics apps/bt/tests/unit/domains/analytics
git commit -m "refactor(bt): migrate ranking price action research"
```

Before staging, verify only the nine consumers, their tests, and shared price/feature modules are present.

### Task 11: Technical Fit Narrow-Long Performance Refactor

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py`

**Interfaces:**
- Produces one fixed-schema narrow long frame with `date`, `code`, `ring`, `sector_33_code`, `raw_score_name`, `family`, `role`, `horizon`, `technical_fit_score`, `outcome_pct`, `n225_outcome_pct`, and declared diagnostics.

- [ ] **Step 1: Add failing column and amplification tests**

Assert the long frame column tuple exactly matches the allowlist and its row count does not exceed `oos_rows * available_score_count * horizon_count`. Patch `DataFrame.copy` only to count copied cells, not to alter behavior, and assert wide source copies are absent.

- [ ] **Step 2: Verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py -k 'narrow or amplification' -q`

Expected: current frame contains the full wide schema and copy amplification exceeds the contract.

- [ ] **Step 3: Build the narrow frame once**

Select the exact allowlist before melting scores/horizons, avoid `_finite_rows` copies of unrelated columns, and reuse grouped views for raw shape, OOS lift, bootstrap, and decision inputs.

- [ ] **Step 4: Run Technical Fit tests and record a local benchmark**

Run the full Technical Fit suite and the runner on the existing immutable input with output directed to a temporary directory. Record wall-clock, peak RSS, input rows, long rows, and relation diagnostics in the plan's execution notes; do not gate on machine time.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py apps/bt/tests/unit/domains/analytics/test_ranking_technical_fit_score_shape_evidence.py
git commit -m "perf(bt): narrow technical fit evaluation frames"
```

### Task 12: Delete Legacy Infrastructure and Add Architecture Ratchets

**Files:**
- Modify: `apps/bt/src/domains/analytics/ranking_color_evidence.py`
- Delete or reduce: `apps/bt/src/domains/analytics/ranking_technical_fit_price_projection.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/scripts/test_removed_future_leak_surfaces.py`
- Modify: `scripts/check-research-guardrails.py`

**Interfaces:**
- No compatibility interface remains for fixed `ranking_color_*` shared infrastructure or unsafe base arguments.

- [ ] **Step 1: Add failing source-search ratchets**

The ratchets reject:

```python
FORBIDDEN = (
    "DAILY_RANKING_RESEARCH_RANKED_TABLE",
    "DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE",
    "event_time_basis_only=",
    "price_feature_relation=",
    "price_outcome_relation=",
)
```

They also reject cross-experiment underscore imports and `stock_data` references in Daily Ranking research modules.

The ratchet derives both bridge paths with AST rather than textual search: exactly
25 modules call `create_daily_ranking_research_panel` directly, exactly five call
it indirectly through `_create_observation_panel`, and the union is exactly 30.

- [ ] **Step 2: Verify RED, delete old builders/aliases, and rerun ratchets**

Remove `_create_observation_panel`, `_create_percentile_view`, fixed shared relation aliases, duplicated valuation SQL, query padding helpers, and unused constants only after `rg` proves no consumer remains.

- [ ] **Step 3: Run architecture and guardrail tests**

Run: `uv run --directory apps/bt pytest tests/unit/architecture tests/unit/scripts/test_removed_future_leak_surfaces.py -q`

Run: `uv run --project apps/bt python scripts/check-research-guardrails.py`

Expected: all pass and guardrails print `[research-guardrails] OK`.

- [ ] **Step 4: Commit**

```bash
git add -A apps/bt/src/domains/analytics apps/bt/tests/unit/architecture apps/bt/tests/unit/scripts scripts/check-research-guardrails.py
git commit -m "refactor(bt): remove legacy daily ranking research paths"
```

### Task 13: Cloud CI Runs Mapped Research Tests

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/ci/research-test-targets.py`
- Modify: `scripts/ci/test_targets.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Modify: `apps/bt/tests/unit/scripts/test_research_test_targets.py`

**Interfaces:**
- The research job executes one de-duplicated target list containing fixed fast tests and mapped changed-file tests.

- [ ] **Step 1: Replace the current test that requires fast-only CI with a failing mapped-routing test**

```python
def test_actions_research_job_runs_fast_and_changed_mapped_targets() -> None:
    workflow = CI_WORKFLOW.read_text()
    assert "--mode fast-pytest" in workflow
    assert "research-test-targets.py < /tmp/research-changed-files.txt" in workflow
    assert "sort -u" in workflow
```

- [ ] **Step 2: Verify RED**

Run: `uv run --directory apps/bt pytest tests/unit/scripts/test_ci_workflow.py tests/unit/scripts/test_research_test_targets.py -q`

- [ ] **Step 3: Merge fast and mapped targets in the existing job**

Write both lists, concatenate and sort uniquely, pass the result to one `bt-pytest.sh` invocation, and restore a timeout that covers the mapped Technical Fit suite.

- [ ] **Step 4: Run CI self-tests and target the full PR diff**

Pipe `git diff --name-only origin/main...HEAD` to the mapper and assert the output contains every directly changed research experiment test.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml scripts/ci/research-test-targets.py scripts/ci/test_targets.py apps/bt/tests/unit/scripts/test_ci_workflow.py apps/bt/tests/unit/scripts/test_research_test_targets.py
git commit -m "ci: run mapped daily ranking research tests"
```

### Task 14: Immutable Publication Digests and Reruns

**Files:**
- Create: `apps/bt/scripts/research/publish_ranking_research.py`
- Create: `apps/bt/tests/unit/scripts/test_publish_ranking_research.py`
- Modify: `apps/bt/tests/fixtures/research/ranking_publication_registry.json`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`
- Modify affected canonical experiment README files.
- Modify/add committed generated digest files under `apps/bt/tests/fixtures/research/`.

**Interfaces:**
- Produces `build_publication_digest(bundle_dir, source_commit, dirty) -> dict[str, object]`.
- Produces CLI modes `publish` and `verify`; `verify` performs no writes.

- [ ] **Step 1: Add failing digest completeness and tamper tests**

Assert the digest contains run decision, source identity, dirty state, manifest/results/summary SHA-256, every table schema/count, projection hashes, selection hashes, and decision metrics. Mutating any artifact or README identity must fail verification.

- [ ] **Step 2: Verify RED and implement deterministic digest generation**

Sort mapping keys, schemas, table names, and metric rows before JSON serialization. Refuse dirty source for a published canonical digest and refuse an existing run ID.

- [ ] **Step 3: Audit every registered Daily Ranking publication**

For each registry entry, compare its current digest and canonical README to the new semantics. Keep it only when exact equivalence is proven; otherwise record invalidation or execute its runner against the active Market v4 database and publish a new immutable run.

- [ ] **Step 4: Rerun PR #480's three studies**

Run from the repository root while the source tree is clean:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_trend_acceleration_conditional_lift.py --run-id 20260719_prime_price_pit_conditional_lift_v8
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py --run-id 20260719_prime_price_pit_fixed_return_priority_v11
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_technical_fit_score_shape_evidence.py --run-id 20260719_prime_pit_technical_fit_shape_v12
```

Publish new immutable bundles, regenerate digests, update Japanese `Published Readout` sections, catalog, and registry, and preserve superseded lineage.

- [ ] **Step 5: Run publication tests and opt-in bundle verification**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_publish_ranking_research.py tests/unit/domains/analytics/test_ranking_publication_registry.py -q
uv run --project apps/bt python apps/bt/scripts/research/publish_ranking_research.py verify
```

Expected: README, catalog, registry, digest, and bundle agree exactly for every published run available in the shared XDG root.

- [ ] **Step 6: Commit**

```bash
git add apps/bt/scripts/research/publish_ranking_research.py apps/bt/tests/unit/scripts/test_publish_ranking_research.py apps/bt/tests/fixtures/research apps/bt/docs/experiments apps/bt/docs/experiments/research-catalog-metadata.toml
git commit -m "docs(bt): republish event-time daily ranking research"
```

### Task 15: OpenAPI, Full Verification, Independent Review, and PR Handoff

**Files:**
- Modify generated OpenAPI TypeScript contracts if the additive 409 response changes generation output.
- Modify `docs/maintainability-snapshot-latest.json` and `.md` only through the repository snapshot command.
- Modify the PR description after push.

**Interfaces:**
- No new internal API; this task proves all prior deliverables.

- [ ] **Step 1: Regenerate and verify bt/ts contracts**

Run: `bun run --filter @trading25/contracts bt:sync` from `apps/ts`.

Run contract tests and confirm only the standardized 409 response is additive.

- [ ] **Step 2: Run focused and full Python verification**

```bash
uv run --directory apps/bt pytest tests/unit/domains/analytics tests/unit/server/services tests/unit/server/routes tests/unit/application/contracts tests/unit/architecture tests/unit/scripts
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
```

- [ ] **Step 3: Run repository guardrails and TypeScript verification**

```bash
uv run --project apps/bt python scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
bun --cwd apps/ts run workspace:test
bun --cwd apps/ts run quality:typecheck
bun --cwd apps/ts run quality:lint
```

- [ ] **Step 4: Run full pre-push research verification**

Run: `scripts/prepush-ci.sh --research --skip-install`

Expected: all product, research, contract, coverage, maintainability, privacy, and guardrail stages pass.

- [ ] **Step 5: Refresh the maintainability snapshot and rerun its check**

Run:

```bash
uv run --project apps/bt python scripts/maintainability_snapshot.py --root . --json-out docs/maintainability-snapshot-latest.json --md-out docs/maintainability-snapshot-latest.md
uv run --project apps/bt python scripts/maintainability_snapshot.py --root . --json-out docs/maintainability-snapshot-latest.json --md-out docs/maintainability-snapshot-latest.md --check
```

Stage only the generated latest JSON/Markdown files.

- [ ] **Step 6: Perform an independent requirements and security-minded review**

Review the complete `origin/main...HEAD` diff against the design completion criteria. Confirm no unsafe fallback, future-outcome selection, SQL identifier injection, stale relation, unbounded amplification, or API payload regression remains.

- [ ] **Step 7: Commit final generated artifacts**

```bash
git add apps/ts/packages/contracts docs/maintainability-snapshot-latest.json docs/maintainability-snapshot-latest.md
git commit -m "chore: verify daily ranking clean cut"
```

Skip this commit when regeneration produces no diff.

- [ ] **Step 8: Push, observe GitHub merge-ref CI, and update PR #480**

Push the branch, update the PR body with architecture, corrected publications, benchmark evidence, and verification counts, and wait for all required checks on the current head.

- [ ] **Step 9: Merge and update local main only after a clean final review**

Confirm PR #480 is mergeable and approved, merge it, fetch `origin/main`, verify the user's root worktree `.gitignore` modification is preserved, and update local `main` without discarding its existing commits.

---

## Plan Self-Review Checklist

- [ ] Every design goal maps to at least one task.
- [ ] Production and research adapters share only signal semantics.
- [ ] Production has no forward outcome dependency.
- [ ] All 30 current consumers (25 direct and five indirect) appear in Tasks 8–10.
- [ ] Daily Triage, Fixed, Technical, and Trend leakage regressions appear in Task 6.
- [ ] PIT lineage, performance cardinality, CI routing, and publication integrity have direct tests.
- [ ] Legacy deletion occurs only after all consumers migrate.
- [ ] Full verification includes Python, TypeScript, contracts, guardrails, publications, and merge-ref CI.
