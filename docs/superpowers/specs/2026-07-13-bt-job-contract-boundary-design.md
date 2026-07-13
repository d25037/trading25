# bt Job Contract Boundary Design

## Goal

Make application-owned job lifecycle types independent of FastAPI schema modules, remove duplicated and legacy Python import paths, and reduce the exact application-to-HTTP-schema dependency baseline without changing the public HTTP contract.

## Current Problem

Job lifecycle types are currently defined in the HTTP layer even though application services and workers own and use the lifecycle behavior:

- `src.entrypoints.http.schemas.common` defines `JobStatus` and `SSEJobEvent`.
- `src.entrypoints.http.schemas.job` defines a second, distinct `JobStatus` plus `JobProgress`.
- `src.entrypoints.http.schemas.backtest` re-exports the common `JobStatus`.
- Seventeen application service and worker modules import one or more of these HTTP schema modules for job lifecycle types.

The two `JobStatus` enums have identical string values but different Python identities. Their cross-type comparisons work only because both inherit from `str`. Identity-sensitive checks and serialization branches can therefore behave differently depending on the import path.

The architecture ratchet records 69 application-to-HTTP-schema dependencies. It prevents growth, but the job-related entries remain migration debt until the application owns these types.

## Chosen Architecture

Create `src/application/contracts/jobs.py` as the single source of truth for:

- `JobStatus`: the five existing string enum values, unchanged.
- `JobProgress`: the existing staged progress model, preserving its 0–100 percentage scale.
- `JobEvent`: the transport-neutral job event model currently represented by `SSEJobEvent`, preserving its 0.0–1.0 progress scale and JSON shape.

These types remain Pydantic models where applicable. Existing job orchestration directly uses Pydantic serialization, so replacing them with dataclasses plus adapters would add mapping code without reducing current coupling.

All application services, workers, HTTP routes, and tests must import the canonical types from `src.application.contracts.jobs`. HTTP schema modules may refer to the canonical contract through a module-qualified import when defining response fields, but they must not define, alias, or re-export the removed legacy names.

## Legacy Removal

Delete these internal Python definitions and exports:

- `src.entrypoints.http.schemas.common.JobStatus`
- `src.entrypoints.http.schemas.common.SSEJobEvent`
- `src.entrypoints.http.schemas.job.JobStatus`
- `src.entrypoints.http.schemas.job.JobProgress`
- `src.entrypoints.http.schemas.backtest.JobStatus`
- `src.entrypoints.http.schemas.JobStatus`

No compatibility aliases or transitional re-exports will remain. Every repository consumer is migrated in the same slice. This intentionally makes old internal Python imports fail so future code cannot continue using the obsolete boundary.

## Dependency Direction and Data Flow

The dependency direction becomes:

```text
entrypoints/http -> application/contracts/jobs <- application/services/workers
```

Application code creates and consumes canonical job contracts. HTTP routes serialize those values or embed them in HTTP response schemas. The application layer never imports `src.entrypoints.http.schemas` for `JobStatus`, `JobProgress`, or `JobEvent`.

This slice does not migrate unrelated HTTP models such as `BacktestResultSummary`. A module that imports both a migrated job type and an unrelated HTTP result schema may retain only the unrelated HTTP import, so its baseline entry remains until that later DTO migration.

## Contract Preservation

The following externally observable behavior must remain unchanged:

- Job status values are `pending`, `running`, `completed`, `failed`, and `cancelled`.
- Persisted job status values in `portfolio.db` remain lowercase strings.
- Job event JSON fields remain `job_id`, `status`, `progress`, `message`, and `data`.
- The SSE queue terminal sentinel remains `None`.
- Job event progress remains on the 0.0–1.0 scale.
- Dataset-build `JobProgress.percentage` remains on the 0–100 scale.
- FastAPI OpenAPI output and generated TypeScript contracts have no semantic diff.

Internal Python import-path compatibility is explicitly not a requirement.

## Architecture Enforcement

The architecture test must enforce both directions of the cleanup:

1. Application modules cannot add new HTTP schema imports under the existing exact-set ratchet.
2. Every baseline entry made stale by this migration is removed in the same commit.
3. HTTP schema modules no longer define or export the removed job contract names.
4. Canonical job contracts preserve the expected enum values and model serialization.

The baseline should shrink only for imports actually eliminated. It must not be manually reduced for files that still import unrelated types from the same HTTP schema module.

## Testing Strategy

Implementation follows test-driven development:

1. Add contract and architecture tests that initially fail because the canonical application module does not exist and legacy schema exports still exist.
2. Add the canonical application contracts and migrate consumers.
3. Run focused schema, job-manager, SSE, generic-job, dataset-build, worker, lab, optimization, screening, and architecture tests.
4. Run Ruff and Pyright for the affected source tree.
5. Run OpenAPI contract synchronization in check mode and confirm no generated TypeScript diff.

The migration is behavior-preserving; any OpenAPI or generated-contract change is treated as a defect unless it is solely a non-semantic schema ordering difference that the existing contract check already accepts.

## Out of Scope

- Migrating `BacktestResultSummary` or other backtest result DTOs.
- Migrating ranking, analytics, database sync, dataset response, or strategy-authoring schema families.
- Changing job persistence, queue behavior, API paths, response field names, or progress scales.
- Adding new compatibility modules, aliases, or deprecation periods.

## Completion Criteria

- One canonical application-owned definition exists for each of `JobStatus`, `JobProgress`, and `JobEvent`.
- All repository production and test imports use the canonical job contract path.
- The listed legacy schema symbols no longer exist.
- The application-to-HTTP-schema exact baseline shrinks by the exact set of eliminated module dependencies.
- Focused tests, architecture tests, Ruff, Pyright, and contract synchronization checks pass.
- The working tree is clean after the implementation commits.
