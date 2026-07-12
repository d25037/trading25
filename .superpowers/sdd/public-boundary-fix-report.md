# Shikiho Public Boundary Fix Report

## Status

COMPLETE

## Blocker and design

`createShikihoRepository().get()` intentionally exposes internal `successfulObservedAt` so the coordinator can use persisted successful-observation freshness for its 24-hour TTL. Returning that internal object directly from the background runtime leaked a third key into the localhost bridge, whose exact-key parser accepts only `snapshot` and `diagnostic`.

The fix adds a typed background-boundary projection, `resolvePublicShikihoState`. It awaits the real coordinator resolver, destructures only `snapshot` and `diagnostic`, and constructs a new exact two-key public object. `successfulObservedAt` remains persisted and available internally; storage and TTL behavior are unchanged.

## TDD RED

Command:

```text
cd apps/ts
bun test packages/shikiho-extension/src/localhost-content.test.ts --test-name-pattern "real repository-backed background resolve"
```

Result before implementation: exit 1, `0 pass`, `1 fail`, `1 error` with:

```text
SyntaxError: Export named 'resolvePublicShikihoState' not found in module .../background-capture.ts
```

The regression uses a real in-memory `createShikihoRepository`, saves a snapshot (thereby persisting `successfulObservedAt`), resolves through a real background coordinator, feeds the runtime response into the localhost bridge, and requires the runtime object keys to be exactly `diagnostic,snapshot` before asserting the public bridge publication.

## Focused GREEN

```text
cd apps/ts
bun test packages/shikiho-extension/src/localhost-content.test.ts packages/shikiho-extension/src/background-capture.test.ts packages/shikiho-extension/src/storage.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/web typecheck
bun run --filter @trading25/shikiho-extension build
```

Results: 39 tests passed, 0 failed, 97 assertions; both typechecks and extension build exited 0.

## Complete verification

```text
cd apps/ts
bun run quality:lint
bun run workspace:test
bun run workspace:build
```

Results: overall exit 0. Lint checked 471 files with 0 errors (same unrelated one warning and three infos). Workspace tests passed: root 15, contracts 30, utils 152, api-clients 87, extension 64, web 1359; total 1707 passed, 0 failed. Workspace production build succeeded; Vite transformed 565 modules.

## Diff and privacy audit

```text
git diff --check
git diff 3eb71a4f..HEAD --check
rg -n "fetch\(|XMLHttpRequest|dangerouslySetInnerHTML|permissions.*cookies|webRequest|declarativeNetRequest" apps/ts/packages/shikiho-extension apps/ts/packages/web/src/components/SymbolWorkbench apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts
```

Results: both diff checks exit 0 with no output. Privacy grep exits 1 with zero matches.

## Concerns

None specific to this boundary fix. Live Atlas acceptance was outside this fix request.
