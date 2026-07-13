# Screening Family Contract Boundary Design

## Context

Five application modules still import screening DTOs from HTTP schemas. The
dependency baseline is currently 42. Screening semantics are also duplicated:
`EntryDecidability`, `ScreeningSupport`, `ScreeningSortBy`, and `SortOrder`
exist in domain modules and again in `entrypoints/http/schemas/screening.py`.

Application services construct screening results, retain job requests, and
persist job payloads. These values are application contracts rather than HTTP
transport ownership.

## Decision

Use existing domain aliases as the semantic source of truth:

- `domains.strategy.runtime.screening_profile.EntryDecidability`
- `domains.strategy.runtime.screening_profile.ScreeningSupport`
- `domains.analytics.screening_results.ScreeningSortBy`
- `domains.analytics.screening_results.SortOrder`

Create `src.application.contracts.screening` as the sole Pydantic definition
site for:

- `MatchedStrategyItem`
- `ScreeningResultItem`
- `ScreeningSummary`
- `MarketScreeningResponse`
- `ScreeningJobRequest`
- `ScreeningJobPayload`

Keep only `ScreeningJobResponse` in the HTTP screening-job schema because it
inherits `BaseJobResponse` and is a transport wrapper.

Delete `entrypoints/http/schemas/screening.py` and the unused
`ScreeningDataSource` alias. Do not add compatibility aliases, re-exports,
forwarding modules, duplicate models, subclasses, or conversion wrappers.

## Alias Canonicalization

The four domain aliases already have the same values as their HTTP duplicates.
`ScreeningSortBy` currently lists those values in a different order. Reorder
the domain Literal to the current HTTP/OpenAPI order before using it in the
application contract:

```python
Literal[
    "bestStrategyScore",
    "matchedDate",
    "stockCode",
    "matchStrategyCount",
]
```

This is an ordering-only source change with no runtime acceptance change and
preserves the generated OpenAPI enum order.

## Contract Graph

`MarketScreeningResponse` nests the three result models and continues using
module-qualified `analytics_contracts.DataProvenance` and
`analytics_contracts.ResponseDiagnostics`.

`ScreeningJobRequest` preserves all existing field names, defaults,
constraints, date pattern, camelCase names, and `extra="forbid"` behavior.

`ScreeningJobPayload.response` remains `dict[str, Any]`. Strengthening it to a
Pydantic result type would change persisted raw-result recovery semantics and
is outside this slice.

## Ownership and Consumers

Application services import the result/job contracts directly from
`src.application.contracts.screening` and semantic aliases from the domain
modules. The five dependency-baseline rows removed are:

- `screening_execution.py|...schemas.screening`
- `screening_job_service.py|...schemas.screening_job`
- `screening_response_builder.py|...schemas.screening`
- `screening_service.py|...schemas.screening`
- `screening_strategy_runtime.py|...schemas.screening`

The resulting baseline is exactly 37.

HTTP routes use application contracts directly for request parsing, response
models, job-request restoration, and persisted-payload validation.
`screening_job.py` uses module-qualified application/domain references while
retaining only `ScreeningJobResponse`. `strategy.py` and the strategies route
use module-qualified domain aliases.

No HTTP schema module may bind or export the ten canonical names: the six
Pydantic contracts plus the four domain aliases.

## OpenAPI Preservation

The following component names and shapes remain unchanged:

- `MatchedStrategyItem`
- `ScreeningResultItem`
- `ScreeningSummary`
- `MarketScreeningResponse`
- `ScreeningJobRequest`
- `ScreeningJobResponse`

`ScreeningJobPayload` remains internal and is not an OpenAPI component.

Class names, docstrings, fields, defaults, requiredness, constraints, nested
references, and enum ordering must match the committed schema. Contract sync
and generated TypeScript zero diff are blockers.

## Architecture Guard

Extend ownership enforcement to the six application contract names and four
domain alias names. Reject HTTP definitions, assignments, direct imports,
aliases, and `__all__` exports of those names. HTTP code must refer through
qualified module aliases.

Delete exactly the five stale baseline rows. No unrelated baseline entry may
change.

## Testing Strategy

Use three TDD tasks:

1. Reorder the domain sort alias and add canonical application contract tests.
   Witness missing-module RED, then verify serialization, list/dict factory
   independence, request constraints, `extra="forbid"`, and schema parity.
2. Extend ownership guards first and witness RED against existing HTTP
   definitions/imports. Migrate application consumers and HTTP routes/schemas,
   delete `screening.py`, and reduce the baseline to 37.
3. Re-run the screening domain, job service, screening service/helper/default
   market/strategy-selection, analytics complex route, strategies route,
   architecture, and OpenAPI suites.

Run Ruff, Pyright, dependency-direction checks, contract sync, TypeScript
contract/client checks, generated-file zero diff, skill-reference check, and
`git diff --check` before completion.

## Non-Goals

- No screening calculation or selection behavior changes.
- No endpoint, status-code, polling, or job-lifecycle changes.
- No raw-result payload strengthening or migration.
- No compatibility surface for deleted HTTP imports.
- No ranking, DB, J-Quants, or portfolio contract migration.

## Completion Criteria

- Six Pydantic contracts have one application definition site.
- Four semantic aliases have one domain definition site.
- HTTP retains only genuine transport wrappers and no canonical-name binding.
- The old screening schema module is deleted.
- Baseline is exactly 37.
- Relevant tests and static checks pass.
- OpenAPI and generated TypeScript artifacts have zero diff.
- Independent reviews have no unresolved Critical or Important findings.
