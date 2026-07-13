# Analytics Common Contract Boundary Design

## Context

The application layer still imports `DataProvenance` and `ResponseDiagnostics`
from `src.entrypoints.http.schemas.analytics_common`. Seven entries in
`apps/bt/tests/unit/architecture/application_http_schema_imports.txt` preserve
this inverted dependency temporarily.

These models are not HTTP transport wrappers. Application services construct
them directly and return them as part of analytics results. Their canonical
ownership therefore belongs in the application layer.

The current dependency baseline contains 49 entries. This slice removes the
seven `analytics_common` entries and reduces it to 42.

## Decision

Create `src.application.contracts.analytics` as the sole definition site for:

- `AnalyticsSourceKind`
- `ResponseDiagnostics`
- `DataProvenance`

Move the definitions without semantic changes. Preserve every field name,
annotation, default, default factory, class docstring, JSON schema component
name, and serialized payload shape.

Do not provide compatibility aliases, re-exports, subclasses, duplicate
models, or forwarding modules under `src.entrypoints.http.schemas`.

## Ownership Boundary

### Application contracts

`apps/bt/src/application/contracts/analytics.py` owns the shared analytics
result vocabulary. It depends only on `typing` and Pydantic.

The seven application consumers import the canonical names directly:

- `analytics_provenance.py`
- `fundamentals_service.py`
- `indicator_service.py`
- `margin_analytics_service.py`
- `roe_service.py`
- `screening_response_builder.py`
- `signal_service.py`

### HTTP schemas

The HTTP schema modules remain responsible for endpoint-specific response
wrappers. They import the application contract module and use qualified
annotations and default factories:

```python
from src.application.contracts import analytics as analytics_contracts

provenance: analytics_contracts.DataProvenance
diagnostics: analytics_contracts.ResponseDiagnostics = Field(
    default_factory=analytics_contracts.ResponseDiagnostics
)
```

Qualified references prevent HTTP schema modules from binding the migrated
contract names and make ownership visible at each field.

The following HTTP schema modules are migrated:

- `analytics_margin.py`
- `analytics_roe.py`
- `fundamentals.py`
- `indicators.py`
- `screening.py`
- `signals.py`

`analytics_common.py` retains only the endpoint-specific market bubble
footprint response models. Renaming or splitting those models is outside this
slice.

### Tests

Tests that construct provenance or diagnostics payloads import the canonical
application contracts. They must not use the deleted HTTP ownership path.

## Data Flow

The resulting dependency direction is:

```text
application/contracts/analytics.py
             ↑
    application services
             ↑
 HTTP routes and response schemas
```

Application services continue constructing identical Pydantic values. HTTP
response wrappers continue nesting those values under the same fields. No
runtime conversion layer is introduced.

## Contract Preservation

The OpenAPI components must remain named `DataProvenance` and
`ResponseDiagnostics`. Their property schemas, required fields, defaults, and
all existing `$ref` edges must remain unchanged.

`AnalyticsSourceKind` remains exactly:

```python
Literal["market", "dataset"]
```

`ResponseDiagnostics` retains fresh-list factories for
`missing_required_data`, `used_fields`, and `warnings`, with
`effective_period_type=None`.

`DataProvenance` retains required `source_kind`, optional snapshot/reference/
strategy fields defaulting to `None`, and fresh-list factories for
`loaded_domains` and `warnings`.

Contract sync and generated TypeScript checks are blockers. The committed
OpenAPI snapshot and generated API types must have zero diff after regeneration.

## Architecture Guard

Add all three migrated names to the application-contract ownership guard:

- `AnalyticsSourceKind`
- `ResponseDiagnostics`
- `DataProvenance`

The guard must reject direct application imports from HTTP schemas and reject
HTTP schema definitions, assignments, aliases, imports, and `__all__`
re-exports using those names.

Delete exactly the seven `analytics_common` rows from the application-to-HTTP
dependency baseline. The resulting non-comment count must be 42. No unrelated
baseline row may change.

## Testing Strategy

Use TDD in two independently reviewed tasks.

1. Add canonical contract tests before creating the new module. Witness an
   import failure, then add the exact three definitions and verify complete
   serialization, minimal defaults, fresh list factories, required
   `source_kind`, and invalid source-kind rejection.
2. Extend the ownership guard before deleting HTTP ownership. Witness failures
   for the old definitions/imports, then migrate all consumers and remove the
   seven baseline rows.

The focused behavior suite includes:

- application analytics contract tests
- architecture boundary tests
- analytics provenance tests
- signal route tests
- indicator schema tests
- analytics fundamentals route tests
- relevant fundamentals, indicator, margin, ROE, screening, and signal service
  tests
- OpenAPI tests

Run Ruff, Pyright, dependency-direction checks, contract sync, contracts and
API-client TypeScript checks, generated-file zero-diff checks, and
`git diff --check` before completion.

## Error Handling

No error behavior changes. Pydantic validation remains the only validation for
these value objects. Invalid `source_kind` values continue to fail validation,
and omitted list fields continue to receive independent empty lists.

## Non-Goals

- No changes to analytics calculations or provenance resolution.
- No field renames or casing changes.
- No new compatibility surface.
- No changes to endpoint paths, status codes, or response wrappers.
- No migration of market bubble footprint HTTP models.
- No migration of screening, ranking, DB, or J-Quants contracts in this slice.

## Completion Criteria

- The three names have one definition site under application contracts.
- No HTTP schema module binds or exports the migrated names.
- All seven application imports point inward to application contracts.
- The dependency baseline is exactly 42.
- Relevant Python tests and static checks pass.
- OpenAPI and generated TypeScript files have zero diff.
- Independent reviewers report no unresolved Critical or Important findings.
