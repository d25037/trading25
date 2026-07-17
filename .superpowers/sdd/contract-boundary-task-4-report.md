# Task 4 Report: Generated wire DTO aliases

## Outcome

- Replaced the stable `api-response-types.ts` and `api-types.ts` HTTP DTO facades with `BtApiSchemas[...]`, endpoint response extraction, or generated-parent indexed access while preserving their public export names.
- Added compile-only exactness fixtures for screening, indices, N225 options, sync/jobs, datasets, portfolio/watchlist, stock lookup, DB stats/validation, refresh, fundamentals, margin, provenance, and lab contracts.
- Kept handwritten definitions only for view/form/URL/configuration models without generated FastAPI wire equivalents.
- Resolved generated optional/null/opaque shapes at web adapter boundaries instead of strengthening wire aliases or casting whole responses.

`apps/ts/packages/contracts/src/index.ts` already re-exported the affected type modules with `export type *`, so no source change was necessary there.

## TDD evidence

RED:

```text
bun run --filter @trading25/contracts typecheck
exit 2
44 TS2344 exactness failures (`false` did not satisfy the `true` constraint)
```

GREEN:

```text
bun test packages/contracts/src/types/api-response-types.test.ts
14 passed, 0 failed, 37 assertions

bun run --filter @trading25/contracts typecheck
exit 0

bun run quality:typecheck
exit 0 (root, contracts generation, api-clients, extension, web, dependency audit)
```

The brief's `bun --cwd apps/ts test ...` spelling is not supported by the repository's Bun 1.3.14 invocation (`Script not found "test"`). The equivalent command was run from `apps/ts` and passed as shown above.

## Additional verification

```text
bun run --filter @trading25/web test -- <18 focused files>
18 files passed, 224 tests passed

bun run quality:lint
499 files checked, exit 0

bun run --filter @trading25/contracts bt:check
OpenAPI generated snapshot PASS, exit 0

python3 scripts/skills/refresh_skill_references.py --check
exit 0

git diff --check
exit 0
```

The first direct `bun test` attempt for web files was intentionally discarded because it bypassed the web package's Vitest DOM setup and failed with `window`/`document` unavailable. The package test script above is the valid web verification.

## Widened file scope

Generated schema optionality required existing web adapters and their tests to change:

- Dataset adapters default optional warnings/errors, normalize the generated opaque dataset progress record, and materialize request defaults.
- Fundamentals components normalize optional numeric wire values and take the lookback field from the generated response level where it is actually defined.
- Screening adapters default optional collections, normalize nullable entry-decidability values, and validate SSE JSON as unknown data without a whole-response assertion.
- DB sync/settings adapters materialize generated required request defaults and handle optional category/fetch detail collections.
- Options, indices, and page adapters normalize nullable sort values, chart volume, reference dates, and related display-only values.

No API route, OpenAPI schema, generated snapshot, or api-client ownership was changed. The `useScreening` work was limited to safe normalization and did not preempt the later api-client migration task.

## Review

The delegated diff review reported no Critical, Important, or Minor findings after `ScreeningSupport` was derived from `StrategyMetadataResponse['screening_support']` and `DatasetJobProgress` was derived from `DatasetJobResponse['progress']`.
