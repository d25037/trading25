# Ranking Family Contract Boundary Design

## Goal

Move the complete Ranking response family from the FastAPI transport layer to a
single application-owned contract module, remove the deprecated
`liquidityState` query compatibility surface, converge handwritten TypeScript
response types on generated OpenAPI types, and reduce the exact
application-to-HTTP-schema dependency baseline from 33 to 21.

The migration must not change ranking calculations, PIT semantics, market
filter behavior, database access, response payloads, or UI behavior. The only
intentional public API change is removal of the deprecated `liquidityState`
query parameter.

## Current Problem

`src.entrypoints.http.schemas.ranking` currently owns application data types and
one application validation helper. Twelve application modules import that HTTP
module directly, accounting for 12 of the 33 remaining dependency-baseline
rows. The same response graph is duplicated in handwritten TypeScript for the
Fundamental and Value Composite ranking families, and those copies have already
drifted from generated OpenAPI optionality.

The deprecated `liquidityState` query also keeps a combined compatibility type
and translation function alive even though current consumers use `regimeState`
and `riskState` separately.

## Decision

Create one canonical module:

```text
apps/bt/src/application/contracts/ranking.py
```

The module owns all application-facing Ranking models, aliases, and
normalization behavior. A single flat module is preferred because the daily,
fundamental, and value-composite response graphs share common types throughout
the 12 application consumers. Splitting the 304-line schema into multiple
contract modules would introduce cross-module imports without removing runtime
coupling and would diverge from the existing flat application-contract pattern.

Delete:

```text
apps/bt/src/entrypoints/http/schemas/ranking.py
```

No compatibility alias, re-export, forwarding module, subclass, duplicate
model, or conversion wrapper is permitted.

## Canonical Python Contract

Move these 12 Pydantic models unchanged:

- `RankingItem`
- `Rankings`
- `IndexPerformanceItem`
- `MarketRankingResponse`
- `MarketRankingSymbolResponse`
- `FundamentalRankingItem`
- `FundamentalRankings`
- `MarketFundamentalRankingResponse`
- `ValueCompositeTechnicalMetrics`
- `ValueCompositeRankingItem`
- `ValueCompositeRankingResponse`
- `ValueCompositeScoreResponse`

Move these application aliases unchanged:

- `ValueCompositeScoreMethod`
- `ValueCompositeProfileId`
- `ValueCompositeForwardEpsMode`
- `ValueCompositeScoreUnavailableReason`
- `LiquidityRegime`
- `RankingRiskFlag`
- `RankingTechnicalFlag`
- `RankingRegimeStateFilter`
- `RankingRiskStateFilter`
- `RankingTechnicalStateFilter`
- `RankingFundamentalStateFilter`
- `SectorStrengthBucket`
- `SectorStrengthFamily`

Move `normalize_sector_strength_family` unchanged. It is application validation
used by the ranking service and index-performance builder as well as the route;
it is not an HTTP schema concern.

Do not migrate `RankingStateFilter`. Its only purpose is the deprecated combined
`liquidityState` query, which is removed completely.

Preserve exact model class docstrings, field declaration order, annotations,
Literal value order, defaults, default factories, `Field` descriptions,
serialized payloads, and mutable Pydantic behavior. Ranking services enrich
`RankingItem` instances in place, so models must not become frozen.

## Backend Cutover

Migrate all twelve application consumers to the canonical module:

- `ranking_collection_filters.py`
- `ranking_daily_queries.py`
- `ranking_daily_technical_metrics.py`
- `ranking_index_performance.py`
- `ranking_liquidity.py`
- `ranking_response_items.py`
- `ranking_service.py`
- `ranking_state_flags.py`
- `ranking_technical_flags.py`
- `ranking_valuation.py`
- `ranking_value_composite_config.py`
- `ranking_value_composite_metrics.py`

Use module-qualified references:

```python
from src.application.contracts import ranking as ranking_contracts
```

`analytics_complex.py` must also use module-qualified application contract
references for query annotations, response models, and return annotations.
Direct class aliases in the HTTP route are not retained.

Tests that currently import the deleted HTTP module must import the canonical
application module instead.

## Deprecated Query Removal

Remove `liquidityState` from `GET /api/analytics/ranking` immediately. Also
remove:

- `RankingStateFilter`
- `_DEPRECATED_RANKING_RISK_STATES`
- `_normalize_ranking_state_filters`
- the route translation from `liquidityState` into `regimeState` or
  `riskState`
- `MarketRankingParams.liquidityState` in the TypeScript API client
- serialization of `liquidityState` in `AnalyticsClient.getMarketRanking`
- backend and TypeScript compatibility tests for the old query

`regimeState` and `riskState` become the only supported state-filter inputs and
are passed to the application service directly.

Do not add a 410 route, deprecated alias, request rewrite, warning response, or
other compatibility behavior. After removal, FastAPI treats the old key as an
unknown query parameter under its existing request behavior; it no longer
affects ranking results.

This is the only intentional OpenAPI breaking change in the slice.

## Endpoint Contract Preservation

Preserve the paths, operation IDs, response components, parameter order,
constraints, defaults, descriptions, tags, summaries, and error responses for:

- `GET /api/analytics/ranking`
- `GET /api/analytics/ranking/symbol/{code}`
- `GET /api/analytics/fundamental-ranking`
- `GET /api/analytics/value-composite-ranking`
- `GET /api/analytics/value-composite-score/{code}`

For `GET /api/analytics/ranking`, preserve all parameters except
`liquidityState`:

- `date`: optional `YYYY-MM-DD`
- `limit`: default `20`, range `0..1000`
- `markets`: default `prime`
- `lookbackDays`: default `1`, range `1..100`
- `periodDays`: default `250`, range `1..250`
- `sector33Name`: optional
- `sector17Name`: optional
- `includeValuation`: default `false`
- `includeSectorStrength`: default `false`
- `sectorStrengthFamily`: default `balanced_sector_strength`
- `forwardEpsDisclosedWithinDays`: default `0`, range `0..3650`
- `regimeState`: optional
- `fundamentalState`: optional
- `riskState`: optional
- `technicalState`: optional

Preserve the audited defaults and constraints for the other four endpoints.
The route migration does not alter ranking computation or error mapping.

## OpenAPI Contract

All 12 Pydantic model names are unique across `apps/bt/src` and currently
publish as plain OpenAPI component names. There are no collision-sensitive
`DateRange`, `IndexMatch`, or equivalent components, so moving model ownership
must not require an `openapi_config.py` stabilizer.

Run `bt:sync` after the backend change. The expected OpenAPI and generated
TypeScript diff is limited to removal of `liquidityState` from the ranking
endpoint query parameters. All 12 component schemas and every remaining
endpoint contract must be byte-equivalent after normalization.

Add contract assertions that compare the pre-migration and post-migration
normalized OpenAPI after deleting only the audited `liquidityState` parameter
from the pre-migration snapshot. Any other difference fails the migration.

## TypeScript Convergence

Daily Ranking core types already use generated schema aliases and remain
unchanged:

- `RankingItem`
- `Rankings`
- `IndexPerformanceItem`
- `MarketRankingResponse`
- `MarketRankingSymbolResponse`

Replace handwritten Fundamental and Value Composite response definitions in
`packages/contracts/src/types/api-response-types.ts` with generated aliases:

- `FundamentalRankingItem`
- `FundamentalRankings`
- `MarketFundamentalRankingResponse`
- `ValueCompositeTechnicalMetrics`
- `ValueCompositeRankingItem`
- `ValueCompositeRankingResponse`
- `ValueCompositeScoreResponse`

Derive their associated response enum aliases from generated shapes using
indexed access and `NonNullable` rather than duplicating Literal unions.

`packages/api-clients/src/analytics/types.ts` must import and re-export these
contract aliases instead of redefining response DTOs. Public export names stay
stable. Request types remain locally authored because they intentionally
describe caller input rather than server output; only the removed
`liquidityState` property is deleted.

This convergence fixes the existing drift where generated contracts allow
omission of:

- `FundamentalRankings.ratioHigh`
- `FundamentalRankings.ratioLow`
- `ValueCompositeRankingResponse.items`
- `ValueCompositeScoreResponse.weights`

Add compile-time fixtures for each valid omission.

## Architecture Guard and Baseline

Add all migrated model, alias, and helper names to the ranking-specific
application-contract ownership guard. Enforce absence of the deleted HTTP
module throughout production Python source and reject HTTP ownership,
re-export, or direct canonical binding of migrated names.

The exact dependency baseline removes only the 12 ranking rows and changes from
33 to 21. The exact-set ratchet remains authoritative; no unrelated baseline
row changes are allowed.

Add explicit checks that:

- `src/entrypoints/http/schemas/ranking.py` does not exist;
- production source contains no import of
  `src.entrypoints.http.schemas.ranking`;
- `RankingStateFilter`, `liquidityState`, and the translation helper are absent
  from production backend and TypeScript client source;
- the 12 canonical components have one application owner.

## Error and Data Semantics

This slice does not change ranking calculations or data selection. Existing PIT
ordering, as-of filters, universe resolution, market-code normalization,
valuation enrichment, liquidity classification, technical flags, and
value-composite scoring remain untouched.

Existing errors remain unchanged for supported requests. Invalid supported
enum values continue to use FastAPI validation. Removing `liquidityState` does
not introduce a special legacy error response.

## Implementation Sequence

1. Add the canonical Python ranking contract with exact schema/parity tests.
2. Add failing architecture ownership, deleted-module, baseline-21, and
   `liquidityState` removal tests.
3. Migrate all application and route consumers, remove compatibility logic,
   delete the HTTP schema, and shrink the baseline.
4. Sync OpenAPI/generated TypeScript and prove the diff is limited to the one
   removed query parameter.
5. Add failing TypeScript compile fixtures, replace handwritten response DTOs
   with generated aliases, and remove the client request property.
6. Run focused backend, architecture, OpenAPI, API-client, web Ranking, type,
   dependency, and generated-contract gates.
7. Run independent task reviews and a final whole-slice review.

Backend ownership and TypeScript convergence are separate commits within this
single design so the intentional OpenAPI change remains auditable.

## Testing

Backend tests cover:

- exact old/new `model_json_schema()` and class-docstring parity before deletion;
- complete model serialization and default-factory behavior;
- mutable `RankingItem` enrichment behavior;
- all Literal values and `normalize_sector_strength_family` success/failure;
- all five Ranking endpoints and their unchanged supported parameter contracts;
- direct `regimeState`/`riskState` forwarding;
- absence of `liquidityState` from OpenAPI and compatibility logic;
- exact normalized OpenAPI diff limited to the removed parameter;
- architecture ownership and exact baseline count 21;
- existing ranking service behavior, including PIT/future-row exclusion tests.

TypeScript tests cover:

- generated-valid omission of the four drifted default-backed fields;
- absence of `liquidityState` from `MarketRankingParams` and request URLs;
- stable exports from `@trading25/api-clients/analytics`;
- Ranking hooks, Ranking page, symbol snapshot, tables, filters, and
  value-composite score consumers;
- workspace typechecking and dependency audit.

Final verification includes Ruff, Pyright, skill reference freshness,
contract-sync behavior, generated artifact review, `git diff --check`, baseline
21, legacy-path absence, and a clean worktree.

## Out of Scope

- Ranking calculation or scoring changes
- PIT/as-of behavior changes
- New Ranking filters or response fields
- UI layout or presentation changes
- Factor-regression, DB, ROE, margin, or other contract-family migrations
- A generic strict-query-parameter rejection policy
- Compatibility support for `liquidityState`

## Completion Criteria

- Ranking models, aliases, and normalization behavior have one canonical
  application owner.
- The old HTTP schema and every compatibility surface are deleted.
- `liquidityState` is absent from backend OpenAPI and the TypeScript client.
- The application-to-HTTP dependency baseline is exactly 21.
- All 12 response component schemas remain unchanged.
- The only intended public OpenAPI change is removal of `liquidityState`.
- Fundamental and Value Composite response types use generated aliases rather
  than handwritten duplicates.
- Ranking runtime, PIT, API response, and UI behavior remain unchanged for all
  supported inputs.
- All backend, contract, TypeScript, web, and independent review gates pass.
