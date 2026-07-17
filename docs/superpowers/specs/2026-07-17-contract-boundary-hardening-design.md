# Contract Boundary Hardening Design

## Goal

Make FastAPI/Pydantic the enforceable source of truth for every Python-to-TypeScript HTTP wire contract while preserving the existing runtime fetch clients and frontend view models.

## Scope

This work completes the existing source OpenAPI to TypeScript pipeline rather than replacing the HTTP client stack. It covers:

- deterministic source OpenAPI export;
- non-destructive local and CI drift checks;
- strict failure when canonical source export is unavailable;
- CI coverage for every FastAPI product change that can affect OpenAPI;
- base-branch compatibility checks for breaking OpenAPI changes;
- generated-schema aliases for TypeScript wire DTOs;
- generated-path typing for request bodies, query parameters, and successful responses;
- static prevention of new handwritten duplicates.

It does not introduce `openapi-fetch` or another runtime client, change endpoint behavior, redesign UI view models, or require API versioning unrelated to a detected breaking change.

## Architecture

The contract flow remains:

```text
FastAPI routes + Pydantic models
  -> deterministic source OpenAPI export
  -> committed normalized OpenAPI snapshot
  -> openapi-typescript generated schemas and paths
  -> stable contracts aliases and typed endpoint helpers
  -> existing api-clients runtime fetch implementation
  -> frontend-only view models
```

FastAPI/Pydantic is the only wire-contract source of truth. The committed OpenAPI snapshot is a reviewable generated artifact, not an independently authored contract. TypeScript stable names may alias generated schemas or derive nested shapes from them, but may not restate server DTO fields manually.

The existing fetch implementations remain responsible for URLs, correlation IDs, error handling, cancellation, and response parsing. Compile-time helpers bind their method, path, query, body, and success response types to generated `paths` without adding a runtime dependency.

## Deterministic synchronization

`bt:sync` uses the local `apps/bt` source export as its canonical input. Canonical export fixes OpenAPI-affecting feature flags, including research route availability, so developer shell state cannot change the schema.

Normal synchronization fails if source export fails. It does not silently reuse a stale snapshot and does not contact an arbitrary running server. An explicitly named offline command may regenerate TypeScript from the committed snapshot, but it cannot claim to synchronize Python and TypeScript.

`bt:check` is non-destructive. It compares source export with the committed snapshot and invokes `openapi-typescript --check` against the committed generated file. A check command must never rewrite a tracked contract artifact.

## CI gates

Contract checks run for all product changes, not only files already classified as direct contract paths. This deliberately favors correctness over narrow path inference because route wiring, OpenAPI customization, dependencies, domain response models, and shared models can all alter the schema indirectly.

The drift gate verifies:

1. canonical source OpenAPI equals the committed normalized snapshot;
2. the committed generated TypeScript equals current `openapi-typescript` output;
3. contract validation commands leave the worktree unchanged.

The `openapi-typescript` version is exact in the package manifest and lockfile.

## Breaking-change policy

Pull requests compare the candidate OpenAPI snapshot with the merge-base version. CI fails for at least these changes:

- removal of a path, HTTP operation, or documented success response;
- removal of a component schema used by the API;
- removal of an object property;
- addition of a required request or response property;
- a property type, format, nullable state, array item type, or reference change;
- narrowing an enum;
- changing or removing a required parameter;
- making an optional parameter required.

Intentional breaking changes require a repository-tracked approval entry. Each entry contains a stable finding fingerprint, a non-empty reason, and an ISO date expiration. Expired, unused, duplicated, or malformed approvals fail validation. This keeps exceptions narrow and prevents a permanent blanket bypass.

The compatibility checker accepts explicit base and candidate files for deterministic unit tests. CI materializes the merge-base snapshot and passes both files to the checker; it does not depend on network access.

## TypeScript contract boundary

`@trading25/contracts` owns stable public names. Server wire DTOs in `api-response-types.ts` and `api-types.ts` become aliases to generated schemas. Nested types are derived from generated parents when OpenAPI does not give the nested value its own component name.

Frontend-only models remain handwritten when they represent normalized display state, form state, URL state, or a composition that is not transmitted over HTTP. Those types use names that do not collide with OpenAPI component schema names.

`packages/api-clients` imports or re-exports wire types from `@trading25/contracts`. It does not duplicate response/request interfaces. A type-only endpoint utility derives:

- operation type from a literal generated path and HTTP method;
- path and query parameters;
- JSON request body;
- JSON response for an explicit success status.

Existing runtime request methods adopt these derived types incrementally by client domain, but the completed change leaves no public FastAPI request/response signature typed by a handwritten duplicate or an unconstrained caller-provided response generic.

## Static duplicate prevention

A repository test compares exported TypeScript interface/type names in designated wire-contract files against generated OpenAPI component names. A colliding handwritten declaration fails unless it is an alias or derived indexed-access type rooted in generated contracts. The test also prevents api-client files from declaring known server DTO names locally.

The rule applies only to HTTP wire boundaries. It does not reject UI-specific models or structurally similar internal types with distinct names.

## Error handling

Synchronization failures identify which source-export command failed and direct the developer to fix the Python environment. Offline generation is reported as offline generation, never successful synchronization.

Compatibility failures print a stable fingerprint, location, change category, and concise remediation. Approved findings are reported separately. Invalid approval metadata fails before compatibility results are accepted.

Generated endpoint helpers produce TypeScript compile errors; they add no runtime exceptions or response coercion.

## Testing

Implementation follows red-green-refactor for non-generated code.

- Bun tests cover source-export failure, deterministic environment flags, absence of implicit HTTP/stale fallback, and explicit offline behavior.
- Python or shell tests cover non-destructive contract commands and CI path classification.
- Compatibility checker fixtures cover every breaking category, compatible additive changes, fingerprints, expiry, malformed approvals, and unused approvals.
- Type-level tests cover representative GET/POST endpoints, path/query/body extraction, status-specific responses, and rejection of incorrect shapes.
- Contract facade tests ensure migrated aliases accept generated payloads.
- Static duplicate tests prove that a newly introduced handwritten OpenAPI DTO name fails.
- Final verification runs contract synchronization checks, contracts tests, api-client tests, TypeScript typecheck, relevant Python tests, lint, and the repository contract gate.

## Migration sequence

1. Make synchronization deterministic, strict, and non-destructive.
2. Expand CI coverage and add compatibility enforcement.
3. Add generated endpoint type helpers and their type tests.
4. Replace stable-facade handwritten wire DTOs with generated aliases by domain.
5. Replace api-client duplicate DTOs and unconstrained response generics.
6. Add duplicate-prevention enforcement after migrations are complete.
7. Regenerate artifacts and run the full verification matrix.

Each migration step keeps runtime HTTP behavior unchanged and must compile before the next domain is migrated.
