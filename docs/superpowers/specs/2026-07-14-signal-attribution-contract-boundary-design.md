# Signal Attribution Contract Boundary Design

**Date:** 2026-07-14
**Status:** Approved

## Objective

Move the complete `SignalAttributionResult` model graph from the HTTP schema layer into the application contract layer without changing analyzer output, artifact persistence, resolver precedence, FastAPI responses, OpenAPI components, or generated TypeScript types.

The old internal Python import path is removed completely. Compatibility aliases, schema-layer re-exports, wrapper contracts, and subclasses are forbidden.

## Scope

### Included

Move these nine Pydantic models into `src.application.contracts.backtest`:

- `SignalAttributionMetrics`
- `SignalAttributionLooResult`
- `SignalAttributionShapleyResult`
- `SignalAttributionSignalResult`
- `SignalAttributionTopNScore`
- `SignalAttributionTopNSelection`
- `SignalAttributionTiming`
- `SignalAttributionShapleyMeta`
- `SignalAttributionResult`

Migrate `run_registry` to the canonical application contract, make HTTP response wrappers use module-qualified application annotations, remove the legacy schema package export, extend the application-contract architecture guard for all nine names, and remove the resulting stale baseline entry.

### Excluded

- `SignalAttributionRequest`
- `SignalAttributionJobResponse`
- `SignalAttributionResultResponse`
- attribution artifact HTTP response models
- analyzer and service conversion from dictionaries to Pydantic models
- endpoint, persistence-envelope, canonical-result, or artifact-format changes

These excluded models and data paths remain HTTP- or domain-specific and do not create an application-to-HTTP dependency after the result graph moves.

## Ownership and Dependency Direction

The nine model definitions live with `BacktestResultSummary` in:

```text
src/application/contracts/backtest.py
```

The allowed direction is:

```text
domain analyzer dictionaries
          ↓
artifact / canonical / raw candidates
          ↓
application run registry model validation
          ↓
application-owned attribution contracts
          ↑
HTTP job/result response wrappers
```

`src.entrypoints.http.schemas.backtest` imports the application contract module and uses `backtest_contracts.SignalAttributionResult`. It must not bind any of the nine model names locally. `src.entrypoints.http.schemas.__init__` must not export them.

## Canonical Model Graph

All class names, docstrings, annotations, `Literal` values, field descriptions, defaults, and requiredness remain exactly as they are today.

Important invariants:

- `SignalAttributionLooResult.status` is required `Literal["ok", "error"]`.
- `SignalAttributionShapleyResult.status` is required `Literal["ok", "error"]`; `method` is required `str`.
- `SignalAttributionSignalResult.scope` is required `Literal["entry", "exit"]`.
- `SignalAttributionTopNSelection.scores` alone uses `default_factory=list`.
- `selected_signal_ids` and `signals` are required lists with no default.
- all four fields in `SignalAttributionShapleyMeta` default to `None`.
- all five fields in `SignalAttributionResult` are required.
- no discriminator, aliases, validators, `model_config`, or serialization hooks are added.

## Runtime Data Flow

The domain analyzer continues to produce plain dictionaries. `BacktestAttributionService` continues to persist the raw result under the existing artifact envelope and job `raw_result` fields.

`run_registry.resolve_signal_attribution_result` keeps the current candidate order:

1. artifact `result`;
2. canonical result payload;
3. raw job result.

Each candidate is validated through the application-owned `SignalAttributionResult.model_validate`. Invalid candidates continue to be skipped. Missing optional keys continue to resolve to `None`; a missing `scores` key continues to resolve to `[]`; missing required lists remain validation errors.

The job and detailed-result HTTP wrappers embed the same application class. The nine OpenAPI component names and their `$ref` graph remain unchanged because all class names and JSON schemas remain unchanged.

## Boundary Enforcement

The architecture guard adds all nine names to `FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES`. It must fail on:

- direct imports of any of the nine names from an HTTP schema module;
- definitions, assignments, aliases, or control-flow bindings under `entrypoints/http/schemas`;
- package exports or `__all__` entries for any of the nine names.

The exact dependency baseline removes:

```text
application/services/run_registry.py|src.entrypoints.http.schemas.backtest
```

The baseline decreases from 50 to 49 entries.

## Error and Compatibility Behavior

There is no compatibility layer. Old imports such as `src.entrypoints.http.schemas.backtest.SignalAttributionResult` fail intentionally.

Validation behavior remains identical. Literal violations, missing required fields, and invalid nested values still raise Pydantic validation errors. Resolver fallback behavior remains unchanged because it catches the same validation failures around the same candidate loop.

Any OpenAPI snapshot or generated TypeScript diff is a regression and blocks completion.

## Testing Strategy

Implementation follows red-green-refactor:

1. Add application-contract tests for a complete nested payload, optional defaults, `scores=[]`, required list failures, and Literal failures. Witness import failure before adding the models.
2. Add all nine names to the architecture guard and witness current HTTP definitions/import/export fail.
3. Move all nine definitions, migrate `run_registry`, update HTTP wrapper annotations, remove the package export, and shrink the baseline.
4. Run resolver precedence, route job/result, analyzer shape, attribution service, architecture, and OpenAPI tests.
5. Run Ruff, Pyright, dependency-direction checks, contract sync, TypeScript contract/API-client typechecks, and require zero generated diff.

## Acceptance Criteria

1. The nine models have one definition site: `src.application.contracts.backtest`.
2. No compatibility alias, schema-layer re-export, wrapper contract, or subclass remains.
3. `run_registry` has no HTTP schema dependency.
4. The application-to-HTTP-schema baseline decreases from 50 to 49 entries.
5. Artifact, canonical payload, and raw-result precedence and validation semantics remain unchanged.
6. All nine OpenAPI components and generated TypeScript definitions are unchanged.
7. Focused tests, architecture checks, Ruff, Pyright, dependency checks, contract sync, and TypeScript typechecks pass with a clean worktree.
