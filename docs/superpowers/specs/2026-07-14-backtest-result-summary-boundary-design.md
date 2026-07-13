# Backtest Result Summary Boundary Design

**Date:** 2026-07-14
**Status:** Approved

## Objective

Move `BacktestResultSummary` from the HTTP schema layer into the application contract layer without changing the FastAPI wire contract, persisted job payloads, artifact resolution, or generated TypeScript types.

The migration removes the obsolete internal Python import path completely. Compatibility aliases, schema-layer re-exports, wrapper models, and subclasses are forbidden.

## Scope

### Included

- Create the canonical `BacktestResultSummary` model in `src.application.contracts.backtest`.
- Preserve all eight fields, types, requiredness, defaults, and descriptions exactly.
- Migrate application services, workers, routes, and tests to the canonical application contract.
- Make HTTP response schemas refer to the application model through a module-qualified inward dependency.
- Delete `BacktestResultSummary` from `src.entrypoints.http.schemas.backtest` and from the schema package exports.
- Add an architecture guard that prevents the old schema-layer definition or import path from returning.
- Remove the four stale application-to-HTTP-schema baseline entries that disappear completely.
- Verify that OpenAPI and generated TypeScript contracts do not change.

### Excluded

- Moving `SignalAttributionResult` or its nested attribution models.
- Moving request DTOs or unrelated backtest response models.
- Changing endpoint paths, response payloads, persistence format, artifact precedence, or result calculation.
- Adding compatibility imports or transitional deprecation paths.

## Ownership and Dependency Direction

The canonical model lives at:

```text
src/application/contracts/backtest.py
```

The allowed direction is:

```text
entrypoints/http schemas and routes
                 ↓
src/application/contracts/backtest.py
                 ↑
application services and workers
```

Application code must never import `BacktestResultSummary` from `src.entrypoints.http.schemas`. HTTP schemas import the application contract module and use `backtest_contracts.BacktestResultSummary` in response fields. They do not bind that class to a same-named schema-module symbol.

Routes and tests import the application contract directly. `src.entrypoints.http.schemas.__init__` no longer exports the model.

## Canonical Model Contract

The application-owned Pydantic model retains the exact current shape:

```python
class BacktestResultSummary(BaseModel):
    total_return: float = Field(description="トータルリターン (%)")
    sharpe_ratio: float = Field(description="シャープレシオ")
    sortino_ratio: float | None = Field(default=None, description="ソルティノレシオ")
    calmar_ratio: float = Field(description="カルマーレシオ")
    max_drawdown: float = Field(description="最大ドローダウン (%)")
    win_rate: float = Field(description="勝率 (%)")
    trade_count: int = Field(description="取引回数")
    html_path: str | None = Field(default=None, description="結果HTMLファイルのパス")
```

No `model_config`, aliases, validators, coercion changes, or serialization hooks are added.

## Runtime Data Flow

Backtest services and workers construct the application-owned model. `JobInfo` stores the same model instance and continues to serialize with `model_dump(mode="json")` and restore with `model_validate`. Artifact-backed summary resolution continues to return the same model type and preserves the existing artifact-first fallback order.

FastAPI response models embed the same class in `BacktestJobResponse.result` and `BacktestResultResponse.summary`. Because the class name and JSON schema are unchanged, the OpenAPI component remains `BacktestResultSummary` with identical references and field semantics.

## Boundary Enforcement

The architecture guard must fail if any of these return:

- an application import of `BacktestResultSummary` from an HTTP schema module;
- a class definition named `BacktestResultSummary` under `entrypoints/http/schemas`;
- a direct schema-layer re-export or package export of `BacktestResultSummary`.

The exact dependency baseline removes these entries:

```text
application/services/backtest_result_summary.py|src.entrypoints.http.schemas.backtest
application/services/backtest_service.py|src.entrypoints.http.schemas.backtest
application/services/job_manager.py|src.entrypoints.http.schemas.backtest
application/workers/backtest_worker.py|src.entrypoints.http.schemas.backtest
```

`application/services/run_registry.py|src.entrypoints.http.schemas.backtest` remains because it still consumes `SignalAttributionResult`. Its `BacktestResultSummary` import moves to the application contract in this slice.

## Error and Compatibility Behavior

There is no compatibility layer. Old internal imports fail immediately, which is intentional.

The HTTP wire contract remains compatible: endpoint response models, JSON field names, requiredness, nullable fields, validation behavior, and OpenAPI component identity must remain unchanged. Any OpenAPI or generated TypeScript diff is a regression and blocks completion.

## Testing Strategy

Implementation follows red-green-refactor:

1. Add canonical-contract serialization and JSON-schema tests that initially fail because the application model does not exist.
2. Extend architecture tests to reject the legacy definition/import/export surfaces and witness failure against the current tree.
3. Add the canonical model and migrate consumers.
4. Shrink the exact dependency baseline.
5. Run focused contract, persistence, artifact resolver, registry, worker, route, and schema tests.
6. Run Ruff and Pyright on application and HTTP code.
7. Run the OpenAPI contract-sync check and require zero generated diff.

## Acceptance Criteria

1. `src.application.contracts.backtest.BacktestResultSummary` is the only definition.
2. No compatibility alias, re-export, wrapper, or subclass remains in the HTTP schema layer.
3. Application services and workers have no HTTP-schema import for this model.
4. The application-to-HTTP-schema baseline decreases from 54 to 50 entries.
5. Job serialization, deserialization, artifact resolution, and FastAPI responses retain existing behavior.
6. OpenAPI and generated TypeScript contracts are unchanged.
7. Focused tests, architecture tests, Ruff, Pyright, dependency checks, and contract sync pass with a clean worktree.
