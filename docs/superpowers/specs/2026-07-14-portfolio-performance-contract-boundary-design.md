# Portfolio Performance Contract Boundary Design

## Context

The portfolio-performance HTTP schema module owns nine Pydantic models that are
constructed directly by two application services:

- `portfolio_performance_service.py` imports seven portfolio analysis models;
- `watchlist_prices_service.py` imports two watchlist price models.

These imports account for two of the 35 remaining application-to-HTTP dependency
baseline rows. The portfolio and watchlist routes return the service models
unchanged, so the models describe application output rather than an HTTP-only
transport wrapper.

The family also exposes a `DateRange` model whose `{from,to}` schema is published
as the plain OpenAPI component `DateRange`. Several unrelated domains define
models with the same Python class name. The current OpenAPI stabilizer recognizes
the portfolio model through its old HTTP module-qualified component key. Moving
the model without changing the stabilizer would leak an application-qualified
component name and change generated TypeScript references.

The handwritten TypeScript portfolio contracts have two existing discrepancies
from the backend OpenAPI contract:

- `PortfolioPerformanceResponse` omits required `analysisDate`;
- `WatchlistStockPrice.prevClose` and `changePercent` are required-nullable even
  though the backend properties are optional-nullable.

## Decision

Move the complete family atomically into two application-owned modules:

- `src.application.contracts.portfolio_performance`
  - `PerformanceSummary`
  - `HoldingDetail`
  - `TimeSeriesPoint`
  - `BenchmarkResult`
  - `BenchmarkTimeSeriesPoint`
  - `DateRange`
  - `PortfolioPerformanceResponse`
- `src.application.contracts.watchlist_prices`
  - `WatchlistStockPrice`
  - `WatchlistPricesResponse`

Delete `src.entrypoints.http.schemas.portfolio_performance`. No compatibility
alias, re-export, forwarding module, subclass, duplicate model, or conversion
wrapper is permitted.

Application services and HTTP routes import the canonical modules and use
module-qualified references. The routes remain thin adapters and return service
results unchanged.

## OpenAPI DateRange Stabilization

Remove the portfolio-performance stabilizer's dependency on the old HTTP module
path. Instead, derive the portfolio date-range component from
`PortfolioPerformanceResponse.properties.dateRange.anyOf[*].$ref` in the raw
FastAPI schema:

1. Read the component name referenced by the portfolio response.
2. Confirm the referenced schema has `from` and `to` properties.
3. Publish that schema as plain `DateRange`.
4. Rewrite the portfolio response reference to `#/components/schemas/DateRange`.
5. Remove the now-redundant module-qualified portfolio component.

This is owner-independent: a future internal module move does not require
another path constant. Existing dataset, DB, factor-regression, and
portfolio-factor-regression `DateRange` stabilization remains unchanged.

The final normalized OpenAPI and generated TypeScript files must have zero diff.
The public component remains `DateRange`; this migration does not introduce a
breaking `PortfolioPerformanceDateRange` component.

## TypeScript Contract Repair

Align the handwritten TS interfaces with the already-published backend contract:

- add required `analysisDate: string` to `PortfolioPerformanceResponse`;
- change `prevClose` to `prevClose?: number | null`;
- change `changePercent` to `changePercent?: number | null`.

These changes do not alter backend OpenAPI or generated TS. They tighten the
portfolio response to require an existing backend field and correctly allow
watchlist price responses that omit unavailable comparison values. Contract
tests must include a portfolio fixture with `analysisDate` and a valid watchlist
price fixture that omits both optional fields.

No web behavior changes are planned. `useWatchlistPrices` continues to consume
`WatchlistPricesResponse`; existing null-aware display behavior and API paths are
unchanged.

## Dependency Graph

```text
MarketDbReader + PortfolioDb
             ↓
portfolio/watchlist application services
             ↓
application contract modules
             ↓
qualified FastAPI route response models
             ↓
stable OpenAPI → generated TS + handwritten TS facade
```

## Contract Preservation

Preserve all nine public component identities, class docstrings, field names,
field order, required-field order, annotations, defaults, alias behavior, and
serialized payloads. In particular:

- `DateRange.from_` remains serialized as `from` and keeps
  `populate_by_name=True`;
- `HoldingDetail.account` remains optional-nullable;
- portfolio `portfolioDescription`, `benchmark`, `benchmarkTimeSeries`, and
  `dateRange` remain optional-nullable;
- watchlist `prevClose` and `changePercent` remain optional-nullable in OpenAPI;
- portfolio route defaults remain `benchmarkCode="0000"` and
  `lookbackDays=252`;
- route paths, operation IDs, parameter order, tags, summaries, descriptions,
  error mappings, and response model names remain unchanged.

The required OpenAPI field order remains:

- `PerformanceSummary`: `totalCost`, `currentValue`, `totalPnL`, `returnRate`
- `HoldingDetail`: `code`, `companyName`, `quantity`, `purchasePrice`,
  `currentPrice`, `cost`, `marketValue`, `pnl`, `returnRate`, `weight`,
  `purchaseDate`
- `TimeSeriesPoint`: `date`, `dailyReturn`, `cumulativeReturn`
- `BenchmarkResult`: `code`, `name`, `beta`, `alpha`, `correlation`, `rSquared`,
  `benchmarkReturn`, `relativeReturn`
- `BenchmarkTimeSeriesPoint`: `date`, `portfolioReturn`, `benchmarkReturn`
- `DateRange`: `from`, `to`
- `PortfolioPerformanceResponse`: `portfolioId`, `portfolioName`, `summary`,
  `holdings`, `timeSeries`, `analysisDate`, `dataPoints`, `warnings`
- `WatchlistStockPrice`: `code`, `close`, `volume`, `date`
- `WatchlistPricesResponse`: `prices`

## Ownership Guard

Add the eight unique model names to the forbidden HTTP ownership set and enforce
them across HTTP schemas and routes. `DateRange` cannot be globally forbidden
because other HTTP families still legitimately own classes with that name.

Add a portfolio-specific repository guard that rejects:

- the deleted `entrypoints.http.schemas.portfolio_performance` import path;
- any `DateRange` definition or direct canonical binding in a replacement HTTP
  portfolio-performance module;
- HTTP route binding of the unique canonical names.

Qualified application-module imports are allowed. Delete exactly the two stale
baseline rows, reducing the non-comment count from 35 to 33.

## Migration Sequence

1. Add both canonical contract modules with tests that encode complete payloads,
   mutable-default behavior, aliases, required ordering, and exact legacy schema
   parity.
2. Add failing ownership and raw-OpenAPI regression tests.
3. Migrate both services and both routes, update owner-independent `DateRange`
   stabilization, delete the old HTTP module, and reduce the baseline to 33.
4. Add failing TypeScript contract tests, then repair the handwritten interfaces.
5. Run combined backend, OpenAPI, generated-contract, TS contract, hook, and web
   type gates before independent review.

The cutover is atomic at completion. Splitting watchlist from portfolio would
leave partial ownership and retain the misnamed HTTP file, so it is not used.

## Testing

Backend TDD covers:

- complete serialization for portfolio and watchlist responses;
- required list fields remain required and preserve their serialized order;
- `DateRange` input by both `from` and `from_`, with serialized key `from`;
- all model property sets and required-field order;
- exact old/new `model_json_schema()` and docstring parity before deletion;
- portfolio and watchlist service behavior;
- both FastAPI routes and existing error mappings;
- owner-independent raw OpenAPI `DateRange` resolution;
- normalized OpenAPI component/ref stability;
- architecture ownership and baseline count 33.

TypeScript TDD covers:

- `analysisDate` as a required string property;
- omission of `prevClose` and `changePercent` from a valid watchlist price;
- existing full portfolio and watchlist fixtures;
- `useWatchlistPrices` behavior and workspace typechecking.

Final gates include focused pytest, Ruff, Pyright, dependency-direction checks,
FastAPI router reference freshness, skill reference freshness, contract sync,
backend-source OpenAPI equality, generated TS zero diff, contracts tests,
contracts/workspace typechecks, web hook tests, `git diff --check`, and a clean
worktree.

## Out of Scope

- Portfolio or watchlist CRUD behavior
- Performance, benchmark, or latest-price calculation changes
- Database schema changes
- UI layout or data-fetching changes
- Renaming public OpenAPI components
- Migrating unrelated `DateRange` families

## Completion Criteria

- Nine models have one canonical application owner across two cohesive modules.
- The old HTTP portfolio-performance schema is deleted without compatibility
  surfaces.
- Application-to-HTTP baseline decreases from 35 to 33.
- DateRange stabilization no longer depends on the deleted module path.
- Handwritten TS portfolio/watchlist types match the published backend contract.
- Runtime API behavior and normalized OpenAPI/generated TS remain unchanged.
- All backend, architecture, contract, TS, and final review gates pass.
