# Screening PIT and Factor Regression Contract Boundary Design

## Goal

Complete two sequential maintenance slices without changing successful
financial-analysis calculations or user-facing payloads:

1. make Screening universe resolution strictly point-in-time and fail closed;
2. move the stock and portfolio Factor Regression response contracts out of
   the HTTP layer and converge TypeScript consumers on generated contracts.

The PIT slice is a correctness prerequisite. The contract-boundary slice starts
only after the PIT slice passes its focused review.

## Current Problems

### Screening can substitute current membership for a historical snapshot

`load_stock_universe` resolves an effective date and reads
`stock_master_daily` only when that exact date exists. Otherwise it reads the
current `stocks` table. A historical Screening request can therefore evaluate
today's listed companies and market classifications against historical price
data.

The same path accepts an omitted `as_of_date`, consults multiple latest-date
callbacks, and documents the current-table branch as a legacy/unit-test
fallback. This compatibility behavior conflicts with the repository rule that
Screening universes are PIT-resolved from `stock_master_daily`.

`ScreeningService.run_screening` also uses a `9999-12-31` sentinel after its
latest-date fallbacks. That converts missing market data into an artificial
future date instead of reporting that Screening cannot run.

### Factor Regression contracts have two owners and a TypeScript drift

The application services import response DTOs from:

- `entrypoints/http/schemas/factor_regression.py`;
- `entrypoints/http/schemas/portfolio_factor_regression.py`.

These imports account for two entries in the temporary
`application -> entrypoints.http.schemas` baseline.

The stock and portfolio APIs intentionally expose different match shapes:

- stock match: `indexCode`, `indexName`, `category`, `rSquared`, `beta`;
- portfolio match: `code`, `name`, `rSquared`.

The handwritten TypeScript portfolio response incorrectly reuses the stock
match type. The generated OpenAPI types preserve the two distinct shapes, so
the handwritten layer currently hides a real contract mismatch.

### Canonical application contracts do not trigger contract CI

Changes under `apps/bt/src/application/contracts/` currently select product CI
but not contract CI. As more HTTP DTOs move into this directory, a change to a
canonical FastAPI response model can avoid the OpenAPI/generated-TypeScript
sync gate.

## Chosen Approach

Use two independently reviewable slices in this order.

### Slice A: strict Screening PIT resolution

Screening receives one authoritative reference date:

```text
explicit reference_date
        or
latest stock_data market date
```

If neither exists, Screening raises an actionable input/data error. It does not
use the latest stock-master date and does not manufacture a sentinel date.

For non-empty market filters, `load_stock_universe` requires an explicit
`as_of_date` and requires an exact `stock_master_daily` snapshot for that date.
Missing table, query failure, or missing date all fail closed with an error that
identifies the date and tells the operator to synchronize the Market DB. The
function never queries `stocks`.

An empty market-code input continues to return an empty universe without a
database lookup. This is input semantics, not a data-source fallback.

The asynchronous Screening job contract does not change. A fail-closed error is
recorded through the existing job failure path and unified API error behavior.
No OpenAPI or frontend change is required.

### Slice B: Factor Regression contract convergence

Create two canonical application modules so the same Python class names can be
preserved without conflating their distinct shapes:

- `application/contracts/factor_regression.py`;
- `application/contracts/portfolio_factor_regression.py`.

Each module owns the complete response graph consumed by its application
service. Services and routes use module-qualified imports. The two old HTTP
schema modules are deleted completely; no re-export, compatibility shim, or
forwarder remains. The architecture baseline decreases from 21 to 19 in the
same cutover.

Before the cutover, parity tests freeze model field order, defaults, aliases,
serialization, JSON schema, and representative nested payloads. OpenAPI parity
must demonstrate that the ownership move changes neither endpoint payload nor
the existing stabilized component document.

In TypeScript, committed generated schemas remain the wire-contract source.
`@trading25/contracts` exposes stable, distinct aliases for:

- stock factor match, date range, and response;
- portfolio weight, excluded stock, match, date range, and response.

`@trading25/api-clients/analytics` re-exports the canonical response aliases
and retains only local request parameter types. The web Factor Regression hook
and panel import the canonical contract aliases. The handwritten `Api*` and
analytics response definitions are removed rather than retained as deprecated
aliases.

## Contract CI Guardrail

Add `apps/bt/src/application/contracts/` to the contract-path taxonomy. A
change limited to a canonical application contract must produce:

```text
product_ci=true
contracts_ci=true
```

Focused taxonomy tests lock this behavior before the Factor Regression owner is
moved. Existing governance-only and docs-only classifications remain unchanged.

## Approaches Considered

### 1. Sequential PIT then Factor Regression slices — chosen

This keeps a correctness fix separate from a cross-language ownership
migration. Each slice has a clear rollback boundary and independent review.

### 2. One combined implementation commit

This is faster mechanically, but a PIT behavior change, CI taxonomy change,
OpenAPI ownership move, and TypeScript correction would be difficult to review
or bisect. Rejected.

### 3. Factor Regression first, PIT cleanup later

This would address maintainability while knowingly retaining a future-leak
path that the repository has explicitly prohibited. Rejected.

## Data and Error Flow

### Screening

```text
request referenceDate or latest stock_data date
  -> exact stock_master_daily(date) existence check
    -> market-filtered PIT universe
      -> strategy preset PIT filtering
        -> Screening evaluation
```

Any missing exact stock-master snapshot stops before evaluation. Current
`stocks` membership never participates in this flow.

### Factor Regression

```text
domain regression result
  -> application canonical Pydantic response
    -> FastAPI route/OpenAPI
      -> generated TypeScript component
        -> contracts stable alias
          -> api-client/web consumer
```

No financial calculation is added to TypeScript.

## Testing Strategy

All production changes follow red-green-refactor.

### Screening PIT

- a historical request with a future/current row in `stocks` and no exact
  `stock_master_daily` date fails instead of returning that row;
- a missing `stock_master_daily` table fails closed;
- an explicit exact-date snapshot returns only that date and excludes both
  current and next-day rows;
- an omitted reference date uses the latest `stock_data` date;
- missing explicit and market reference dates fail without a sentinel;
- empty market codes still return an empty list;
- async Screening job failure behavior remains covered.

### Contract CI

- canonical application-contract paths select contract and product CI;
- docs-only and governance-only controls retain their current results.

### Factor Regression backend

- all canonical models and nested shapes have frozen parity tests;
- the stock and portfolio match schemas remain intentionally different;
- application services and routes have no old HTTP-schema imports;
- recreating either deleted schema module or adding a forwarder fails the
  architecture guard;
- the exact baseline is 19;
- focused service/route tests, Ruff, and Pyright pass.

### TypeScript and OpenAPI

- source OpenAPI before and after the ownership cutover is equal;
- generated committed types are synchronized;
- stable aliases resolve to the generated components;
- a type test rejects a stock-shaped match inside a portfolio response;
- api-client runtime endpoint tests and focused web Factor Regression tests
  pass;
- workspace typecheck and dependency audit pass.

## Non-Goals

- Changing regression calculations, lookback interpretation, endpoint paths,
  request query parameters, or response JSON.
- Renaming stabilized OpenAPI component keys in this slice.
- Migrating Margin, ROE, Fundamentals, DB, Dataset, Lab, Chart, or Strategy
  Authoring contracts.
- Retaining legacy Screening database support.
- Adding frontend financial calculations or frontend-local validation.

## Acceptance Criteria

1. Screening never reads `stocks` to resolve a universe.
2. Screening requires an exact `stock_master_daily` snapshot for its effective
   market date and fails with an actionable error when unavailable.
3. The latest-stock-master and `9999-12-31` reference-date fallbacks are gone.
4. Canonical application-contract changes trigger contract CI.
5. Both Factor Regression response graphs are owned by application contracts;
   the old HTTP schema files and all imports of them are absent.
6. The application-to-HTTP-schema baseline is exactly 19.
7. OpenAPI and JSON payloads are unchanged by the ownership migration.
8. TypeScript represents stock and portfolio match shapes separately through
   generated aliases, with no handwritten response compatibility aliases.
9. Focused backend/frontend tests, architecture tests, Ruff, Pyright,
   TypeScript typecheck, dependency audit, skill audit, and contract sync pass.
