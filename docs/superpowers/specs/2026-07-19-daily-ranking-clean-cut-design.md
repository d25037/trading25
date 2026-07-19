# Daily Ranking Clean-Cut Design

## Status

- Date: 2026-07-19
- Decision: clean cut
- Scope: production Daily Ranking semantics, reusable research snapshots, all current research consumers, CI, and publication integrity
- Compatibility: preserve request/response field contracts and UI behavior; allow an additive standardized 409 recovery response for invalid PIT lineage; remove internal unsafe and legacy research contracts

## Problem

`daily_ranking_research_base.py` is currently a public-looking facade over private builders in the 2,228-line `ranking_color_evidence.py`. The dependency direction is backwards: the reusable base imports research-specific private functions, while exactly 30 research modules consume fixed temp-table names and unstable `SELECT *` schemas: 25 call the bridge directly and five call it indirectly through `_create_observation_panel`.

The current structure has correctness and performance defects:

- the default research path reads the current `stock_data` projection and does not require event-time valuation bases;
- signal features and forward outcomes are mixed in one wide relation, so consumers can filter on future outcome availability before selecting a signal-time cohort;
- fixed temp names cannot safely coexist and optional relations can remain stale after a rebuild;
- production and research duplicate valuation and liquidity semantics and already disagree at boundary conditions;
- wide temp tables are repeatedly materialized and Technical Fit expands a wide frame across score and horizon dimensions;
- GitHub CI runs fixed lightweight research tests but not the changed-file mapped experiment tests;
- publication checks can prove committed files agree with one another without proving they agree with the immutable bundle.

The known outcome-selection bugs in Trend Acceleration, Fixed Return, Technical Fit, and Daily Triage are consequences of this architecture. Fixing only those call sites would leave the same unsafe operation available elsewhere.

## Goals

1. Make one reusable Daily Ranking signal-semantic core the source of truth for production and research.
2. Make the canonical research path Market v4 event-time safe and fail closed.
3. Make it structurally difficult to use forward outcome availability during signal-time selection.
4. Replace fixed, implicit temp-table contracts with typed, namespaced relation contracts.
5. Migrate every current `daily_ranking_research_base` consumer and remove legacy aliases and private cross-research APIs.
6. Reduce unnecessary scans, window functions, table copies, and pandas frame amplification.
7. Keep the production `/api/analytics/ranking` HTTP/OpenAPI/UI contract stable.
8. Make CI and publication verification cover the code and artifacts that determine published decisions.

## Non-goals

- Forward outcomes will never be added to the production Ranking API.
- Research bundle data will not be served by `RankingService`.
- This work will not add a new ranking feature, sort field, badge, or UI control.
- Wall-clock timing on shared CI runners will not be a merge gate.
- Old unsafe temp-table names will not remain as a permanent compatibility layer.

## Considered Approaches

### 1. Clean cut — selected

Introduce a shared signal core, rebuild the research base around event-time inputs, migrate all consumers, and delete legacy internal contracts in the same change series. This is the only approach that removes rather than hides the unsafe architecture.

### 2. Long-lived compatibility adapter — rejected

Keeping `ranking_color_*` aliases and the current `stock_data` fallback would reduce migration work, but it would preserve two sources of truth and allow new code to select the unsafe path.

### 3. Patch PR #480 consumers only — rejected

This would address the four known selection bugs but would not fix the unsafe default base, production/research semantic divergence, stale relations, or CI gaps.

## Architecture

```text
Market v4 Data Plane
  stock_data_raw + stock adjustment bases/segments
  stock_master_daily
  daily_valuation + event-time basis
  topix_data + indices_data
                     │
                     ▼
daily_ranking_core.py
  pure policies and deterministic signal semantics
  no I/O, no forward outcomes
           │                         │
           ▼                         ▼
production services          daily_ranking_research_base.py
RankingService               namespaced PIT signal/outcome relations
no forward outcomes          selection-first research workflow
```

### Shared signal core

Create `apps/bt/src/domains/analytics/daily_ranking_core.py`. It owns only pure, signal-time semantics:

- normalized market-scope policy;
- valuation percentile and classification thresholds;
- liquidity regime classification;
- deterministic percentile semantics;
- deterministic ordinal ordering and tie-breaking;
- value/risk flag predicates used by both production and research.

The core exposes Python functions and SQL-expression builders generated from the same immutable policy objects. SQL and Python implementations must pass conformance tests over boundary and missing-value cases.

The core must not import application services, DuckDB readers, research bundle code, or any module that knows a forward outcome column.

### Production boundary

`RankingService` continues to own the live endpoint orchestration. Existing query modules continue to load current or requested signal-date data, but duplicated classification logic moves to `daily_ranking_core.py`.

Production signal-date prices, lookbacks, technical flags, and historical ranking queries migrate from `stock_data` convenience rows to the same raw event-time projection policy used by research. The production adapter requests signal features only and never constructs forward outcomes. Latest-date results and explicitly requested historical dates therefore use the basis valid at their own signal date rather than a later current materialization.

Production policy is canonical where production and research currently disagree:

- liquidity regression requires at least 100 valid observations, a finite positive slope and residual standard deviation, uses `sqrt(SSE / (n - 2))`, and treats a persistent run-up as both 20-day and 60-day returns strictly greater than zero;
- valuation percentile population is explicit in the request and defaults to exact-date Prime for the existing valuation filters;
- multi-market technical percentiles declare whether they rank across the requested union or separately by market instead of inheriting an implicit SQL partition;
- the internal technical state is named `atr20_acceleration_ex_overheat`; the existing API string `atr20_acceleration` remains an adapter mapping;
- unsupported non-Prime valuation filters fail explicitly instead of interpreting all-null percentiles as `no_earnings`.

The following remain production-only:

- trading-value, gainer/loser, and period-high/low queries;
- response item construction and ranking response contracts;
- endpoint filtering, pagination, and enrichment orchestration;
- latest-date resolution and index-performance response construction.

Production must not import `daily_ranking_research_base.py`, event-time forward outcome builders, or research selection helpers. The API query parameters, response fields, and frontend behavior remain unchanged. Invalid or unavailable PIT lineage maps to the unified error body with HTTP 409 and recovery stage `adjusted_metrics_pit`; this response is added to OpenAPI and regenerated TypeScript contracts.

### Event-time price inputs

Move the generic parts of `ranking_technical_fit_price_projection.py` into `apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py`.

The builder consumes only:

- `stock_data_raw`;
- `stock_adjustment_bases`;
- `stock_adjustment_basis_segments`;
- `stock_master_daily`;
- `topix_data`;
- `indices_data` when N225 is available.

It returns two separate, namespaced relations:

1. `signal_prices`: OHLCV, lagged signal features, liquidity inputs, and consumed basis lineage through the signal date;
2. `forward_outcomes`: one row per `(code, signal_date)` with a completion date and stock/TOPIX/N225 returns for every requested horizon.

The builder validates Market v4 compatibility, normalized-code uniqueness, basis cardinality, basis status, materialization frontier, exact segment coverage, finite positive factors, completion-date alignment, and relation cardinality before publishing either relation.

There is no canonical `stock_data` fallback and no `event_time_basis_only` boolean.

### Research base request and result

Replace the boolean-heavy function with typed inputs:

```python
@dataclass(frozen=True)
class DailyRankingPanelRequest:
    namespace: str
    analysis_start_date: date | None
    analysis_end_date: date | None
    horizons: tuple[int, ...]
    market_scopes: tuple[MarketScope, ...]
    include_liquidity: bool = True
    percentile_features: tuple[DailyRankingPercentileFeature, ...] = ()


@dataclass(frozen=True)
class DailyRankingResearchRelations:
    signal_prices: RelationRef
    forward_outcomes: RelationRef
    signal_panel: RelationRef
    ranked_signals: RelationRef
    liquidity_ranked_signals: RelationRef | None
    lineage: DailyRankingLineageAudit
    diagnostics: DailyRankingBuildDiagnostics
```

`RelationRef` contains a validated DuckDB identifier, an exact ordered column/type schema, key columns, row count, generation, relation kind, and an unforgeable build capability. Namespace values accept only lowercase ASCII letters, digits, and underscores. Optional relations are represented by `None`, never by a name that might refer to stale state. Cohort selection accepts only signal refs returned by the same build plus validated predicate/projection expressions; relation-name prefixes or caller-authored `SELECT` text are not authority.

The builder owns query-bound resolution. Consumers provide analysis dates and session horizons, not calendar-padding guesses. Insufficient history or outcome frontier is recorded in diagnostics; lineage inconsistency fails the build.

### Relation model

The canonical base exposes separate relations:

- `signal_panel`: one row per `(code, date, market_scope)` containing identity, signal-date price features, valuation, liquidity, and lineage; no `forward_*` columns;
- `ranked_signals`: signal panel plus explicitly requested signal-time percentiles and classifications; no outcomes;
- `forward_outcomes`: one row per `(code, date)` containing only completion dates and forward result columns;
- `liquidity_ranked_signals`: optional view of ranked signals; no duplicated materialization;
- consumer-owned frozen cohort relations;
- consumer-owned evaluated relations created only after a cohort is frozen.

There is no generic full-universe relation that mixes unrestricted signal selection with outcome columns.

All schemas use explicit columns. Date columns are `DATE`, normalized codes are canonical four-character equity codes, and `(code, date)` uniqueness is checked at every source boundary.

Until Tasks 8–10 complete, the deprecated fixed aliases may remain as a bridge,
but `DailyRankingResearchPanelSpec` returns generation-specific relation names.
Publishing the complete alias set is one transaction so a failed rebuild cannot
expose mixed generations or leave generation orphans.

### Selection-first contract

Extend `ranking_research_selection_contract.py` into the only supported cohort-selection API.

It provides two distinct ranking concepts:

1. value percentile: equal values receive the same standard `percent_rank` value;
2. operational order: rows are ordered by score columns followed by normalized `code`, producing deterministic fixed-size top-K or tails.

The workflow is mandatory:

```text
rank signal rows
→ freeze membership using signal columns only
→ attach outcomes by (code, signal_date)
→ compute coverage
→ calculate metrics only when the declared completeness policy passes
```

Selection functions do not accept an outcome column. Outcome evaluation functions accept an already-frozen selection object or relation. Missing selected outcomes never backfill from lower-ranked candidates.

The default publication policy is fail closed: an incomplete selected cohort emits coverage and `outcome_status="incomplete"`, while effect metrics remain null and do not enter bootstrap, stability, or decision gates.

### Consumer migration

All 30 current consumers of `create_daily_ranking_research_panel` migrate in this change. Migration includes the 25 direct callers, the five indirect `_create_observation_panel` callers, direct references to `ranking_color_*`, raw public table strings, and cross-research private helpers.

The four known selection-first violations receive explicit regression tests and migration:

- Daily Triage top-K;
- Fixed Return continuous and sensitivity tails;
- Technical Fit OOS top/bottom comparison;
- Trend Acceleration continuous percentile buckets.

The remaining consumers use the typed relation result and feature capabilities they actually need. Relation percentiles are enabled only for Forecast Operating Profit Growth unless another consumer demonstrates a direct column dependency.

Common overlays such as ATR, sector strength, PSR, SMA, long/short scaffold, and ROE move behind named public feature-builder functions. A research module must not import another experiment module's underscore-prefixed builder or depend on its fixed temp-table names.

When the last consumer is migrated, remove:

- the old `create_daily_ranking_research_panel` signature;
- `DAILY_RANKING_RESEARCH_*_TABLE` fixed-name constants;
- `ranking_color_*` compatibility aliases used as shared infrastructure;
- the unsafe internal price mode;
- duplicated query start/end helpers;
- private cross-experiment helper imports replaced by public builders.

`ranking_color_evidence.py` becomes a normal consumer of the base rather than its implementation owner.

## Correctness Decisions

### Point-in-time data

- Exact-date `stock_master_daily` membership is mandatory.
- Signal price features use the basis valid at the signal date.
- Completion price uses the basis valid at the authoritative stock completion date.
- TOPIX and N225 outcomes terminate on that stock completion date, not on independent `lead(horizon)` dates.
- Valuation rows must carry the cutoff-valid ready basis and pass the same lineage audit.
- Future rows appended after an analysis cutoff must not change earlier signal features, cohorts, percentiles, or decisions.

### Missing outcomes

Missing outcomes are data coverage, not a signal feature. They may exclude a metric from evaluation after membership is frozen; they may not change membership, side counts, percentiles, or minimum-candidate eligibility.

### Ties

Percentile ties preserve equal values. Fixed-size operational selections break score ties by normalized code. Input row order cannot change selection or artifact hashes.

### Production/research conformance

Given identical signal-date valuation percentiles, liquidity residuals, and momentum inputs, production Python classification and research DuckDB classification must produce identical:

- valuation signal;
- value confirmations and warnings;
- liquidity regime;
- risk flags covered by the common policy.

Any current divergence is resolved in favor of the documented common policy and called out in release notes. No response field is added or removed.

## Performance Design

- Push date, market, and required-column projection into the earliest DuckDB relations.
- Materialize `signal_panel` and `ranked_signals` once; use views for optional scopes when they do not reduce repeated computation.
- Compute all requested percentiles in one window stage.
- Do not calculate liquidity regression when `include_liquidity=False`.
- Do not calculate relation percentiles unless explicitly requested.
- Reuse one N225 source relation and one event-time price projection per build.
- Replace Technical Fit's repeated wide DataFrame copies with one narrow long frame containing only identity, score, horizon, outcome, and required diagnostics.
- Replace repeated audit scans with a single diagnostics query per materialized relation.
- Keep large operations in DuckDB until the final narrow result frame is required.

CI performance gates use deterministic cardinality and amplification invariants rather than wall-clock timing:

- one signal row per request;
- one outcome request per signal row and horizon;
- at most two benchmark endpoints per completed request;
- unique `(code, date)` signal and outcome keys;
- no relation exceeds its declared theoretical row amplification;
- Technical Fit long-frame columns match a fixed narrow allowlist.

Wall-clock, peak RSS, temp bytes, and phase scan counts are recorded by a repeatable local benchmark command and compared manually against the pre-refactor baseline.

## Error Handling

The build fails before exposing canonical relations when it encounters:

- unsupported Market schema or adjustment mode;
- invalid namespace or relation identifier;
- missing required tables or columns;
- duplicate or conflicting normalized rows;
- missing, overlapping, non-ready, or invalid adjustment bases/segments;
- valuation basis mismatch;
- relation schema or key-cardinality drift.

Ordinary incomplete forward outcomes do not fail the base. They are represented in coverage diagnostics and handled by the consumer's declared completeness policy after selection.

## CI and Publication

The `bt-research-tests` job runs both:

1. the fixed lightweight contract suite;
2. changed-file mapped research suites, de-duplicated into one pytest invocation.

The job timeout reflects the mapped suites rather than forcing them out of cloud CI. Workflow contract tests assert that changed-file input reaches pytest selection.

Publication uses a generated committed digest containing:

- immutable run ID and decision;
- source commit and clean/dirty state;
- manifest, results, and summary hashes;
- table schemas and row counts;
- price/basis/selection lineage hashes;
- decision-gate metrics used by the README.

Normal CI verifies README, catalog, registry, and digest agreement. An opt-in artifact verifier regenerates the complete digest from the immutable bundle and requires exact equality.

Any published study whose membership or metrics change is not silently retained. PR #480's Trend Acceleration, Fixed Return, and Technical Fit studies are rerun and republished. Other registered Daily Ranking publications are either proven equivalent by digest/conformance checks, rerun, or explicitly invalidated with lineage recorded in their canonical README and catalog.

## Testing Strategy

### Shared core

- boundary tables for every valuation and liquidity classification;
- SQL/Python conformance over null, zero, threshold, and finite values;
- deterministic percentile ties and ordinal code tie-breaks;
- input-row permutation invariance.

### Event-time inputs

- Market v4 compatibility rejection;
- basis cardinality, overlap, status, frontier, and factor failures;
- alias duplicate equality acceptance and conflicting duplicate rejection;
- future raw/basis/valuation/universe append stability;
- split and reverse-split signal/completion basis correctness;
- sparse stock sessions with TOPIX/N225 completion alignment;
- missing N225 represented by stable nullable columns.

### Research relations

- stable explicit schema across horizons and optional features;
- no outcome columns in signal relations;
- namespaced builds coexist on one connection;
- optional relation is `None` and cannot resolve stale state;
- disabled liquidity and percentiles skip their operators;
- source key uniqueness and relation row-count invariants.

### Selection-first regressions

- highest-ranked missing outcome never backfills;
- missing outcome does not change percentile membership or side counts;
- incomplete selected cohort emits null effect metrics and fails closed;
- Daily Triage, Fixed Return, Technical Fit, and Trend Acceleration each carry a direct regression;
- bootstrap, stability, and decision gates consume complete evaluated rows only.

### Production

- existing RankingService API and contract tests remain green;
- production/research classification conformance tests use identical fixtures;
- historical-date enrichment remains point-in-time stable;
- historical and latest production ranking price/technical inputs come from raw event-time projection and ignore poisoned `stock_data` convenience rows;
- Prime and non-Prime percentile population behavior is explicit;
- invalid basis lineage returns the unified 409 `adjusted_metrics_pit` recovery response;
- no production module imports research outcome or bundle code.

### Repository verification

- affected unit and integration suites;
- all Daily Ranking research consumer tests;
- package unit tests;
- Ruff and Pyright;
- research guardrails and strict skill audit;
- OpenAPI contract sync proving payload compatibility and the additive 409 response;
- web tests for Ranking response consumption;
- full pre-push research suite;
- GitHub merge-ref CI after push.

## Migration and Commit Sequence

1. Add failing shared-core conformance and selection-first tests.
2. Add the pure shared core and migrate production classifications without changing the API contract.
3. Add failing event-time relation and lifecycle tests.
4. Extract and optimize the generic event-time price builder.
5. Replace the research base with typed namespaced signal/outcome relations.
6. Migrate shared overlay builders out of experiment-private modules.
7. Migrate all consumers, removing unused capabilities and legacy relation references.
8. Fix the four known selection-first violations through the common contract.
9. Convert Technical Fit to the narrow long evaluation frame.
10. Delete the old builder, aliases, fixed constants, unsafe paths, and obsolete tests.
11. Fix CI routing and publication digest verification.
12. Run the full verification matrix, rerun affected publications, and refresh the PR description and canonical readouts.

Each behavior change follows red-green-refactor: the regression test must be observed failing before production code changes.

## Completion Criteria

The clean cut is complete only when all of the following are proven:

- production and research use the shared signal core;
- production wire contracts are unchanged;
- the canonical research base has no `stock_data` fallback and no non-event-time valuation mode;
- signal relations contain no forward outcomes;
- all 30 current consumers use typed namespaced relations and public feature builders;
- no consumer imports a cross-experiment private builder or references legacy `ranking_color_*` infrastructure names;
- all known and newly discovered selection-first regressions pass;
- optional work is demonstrably skipped and relation cardinalities satisfy the performance contract;
- fast and mapped research tests run in GitHub CI;
- affected publications have verified immutable lineage;
- full local and GitHub merge-ref verification is green;
- an independent final review reports no blocking findings.
