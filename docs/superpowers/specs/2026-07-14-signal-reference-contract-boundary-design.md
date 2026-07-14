# Signal Reference Contract Boundary Design

## Context

The signal-reference family still lets application services import transport-owned
types from `src.entrypoints.http.schemas.signal_reference`:

- `signal_reference_service.py` imports `SignalFieldTypeValue`;
- `strategy_authoring_service.py` imports `FieldConstraints` and
  `SignalCategorySchema`.

The HTTP module owns two literal aliases and seven Pydantic models that describe
application output rather than HTTP-specific behavior. This accounts for two of
the 37 remaining application-to-HTTP dependency baseline rows. The same models
are also consumed by the signal-reference route and the strategy-authoring HTTP
schema.

## Decision

Move the complete signal-reference contract graph to
`src.application.contracts.signal_reference` and delete
`src.entrypoints.http.schemas.signal_reference`.

The canonical application module owns these aliases:

- `SignalFieldTypeValue`
- `SignalExecutionSemantics`

It also owns these models:

- `FieldConstraints`
- `SignalFieldSchema`
- `SignalChartCapability`
- `SignalReferenceSchema`
- `SignalAvailabilityProfile`
- `SignalCategorySchema`
- `SignalReferenceResponse`

`CompiledSignalScope` and `CompiledSignalAvailability` remain domain-owned in
the strategy compiler. The application contracts import and expose their values
through model fields without redefining them. The two literal aliases remain
application-contract concepts because they are stable serialized projections;
replacing them with enums would change the committed OpenAPI shape.

## Dependency Graph

```text
strategy compiler domain enums
            ↓
application/contracts/signal_reference.py
       ↙             ↓                 ↘
signal reference  strategy authoring   HTTP route/schema
service           service              adapters
```

Application services may import only the application contract module and domain
modules. HTTP modules use module-qualified references such as
`signal_reference_contracts.SignalReferenceResponse`; they must not bind the
canonical class or alias names locally.

## Migration

The migration has two atomic implementation stages:

1. Add the canonical application contract graph and prove exact parity with the
   existing HTTP definitions. No consumer moves in this stage.
2. Extend the ownership guard, migrate all source and test consumers, delete the
   HTTP schema module, and remove exactly the two signal-reference baseline rows.

The final baseline count is 35. No compatibility alias, re-export, forwarding
module, subclass, conversion wrapper, or duplicate model is permitted.

## Contract Preservation

The migration preserves every class name, docstring, field name, annotation,
default, default factory, constraint, serialized payload, forward-reference
relationship, and OpenAPI component identity. In particular:

- `SignalReferenceSchema.availability_profiles` continues to resolve
  `SignalAvailabilityProfile`;
- chart and list defaults remain independent factories;
- literal value order remains unchanged;
- compiler-owned enum schemas remain unchanged;
- `/api/signals/reference` keeps the same request-free route and response model;
- strategy-authoring responses continue to embed the same constraints and
  category schemas.

The committed OpenAPI JSON and generated TypeScript types must have zero diff.

## Guardrails

Add all nine canonical names to the HTTP ownership guard. The guard must reject:

- direct imports from any HTTP schema path;
- top-level HTTP schema definitions or alias assignments;
- local HTTP re-exports through `__all__`;
- renamed imports that rebind a canonical name.

Qualified imports from `src.application.contracts.signal_reference` are allowed.
The repository scan must prove that the deleted HTTP module is not imported by
source or tests.

## Testing

Use TDD in both stages.

Canonical contract tests first fail because the application module is missing,
then verify:

- complete response serialization;
- independent mutable defaults;
- accepted and rejected literal values and their ordering;
- exact schema and docstring parity with the old seven models;
- forward-reference resolution and compiler enum fields.

The migration guard first fails against the old ownership and imports. After the
cutover, run signal-reference service/schema-sync tests, signal-reference route
tests, strategy-authoring service and route tests, architecture tests, and
OpenAPI tests. Final gates are Ruff, Pyright, dependency-direction validation,
contract sync, contracts and API-client typechecks, generated-file zero diff,
baseline count 35, and a clean worktree.

## Out of Scope

- Signal registry behavior or copy content
- Signal computation endpoints and request/response contracts
- Strategy-authoring contract migration beyond replacing its shared
  signal-reference types
- Frontend behavior or generated contract changes
- Changes to execution-policy semantics

## Completion Criteria

- The seven models and two aliases have one canonical application owner.
- The old HTTP signal-reference schema module is deleted.
- No compatibility surface is introduced.
- The application-to-HTTP baseline decreases from 37 to 35.
- Runtime behavior and OpenAPI/TypeScript contracts remain unchanged.
- Focused tests, architecture checks, static checks, and contract checks pass.
