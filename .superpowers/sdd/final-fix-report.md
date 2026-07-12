# Final Shikiho Review Fix Report

## Status

COMPLETE_AUTOMATED_FIX_WAVE (live Atlas acceptance intentionally not run in this subtask)

## Design details

1. Capture timeout now persists a current `page_changed` diagnostic before resolving the job. The returned stale snapshot is preserved, and the diagnostic timestamp drives the existing 60-second automatic retry suppression.
2. The coordinator exposes `onTabRemoved(tabId)`. Production registers `chrome.tabs.onRemoved`; only a matching currently-owned generated tab records `page_changed`, resolves its job, releases FIFO, and is marked so `finally` does not remove it again. Unknown/user-owned tab IDs are ignored.
3. Successful freshness is persisted separately in `shikihoSuccessfulObservationsV1` and exposed as `successfulObservedAt` from repository state. TTL uses this successful observation timestamp, falling back to snapshot `capturedAt` for migration. Same-hash success updates only the observation/diagnostic maps and never rewrites the snapshot payload or its original `capturedAt`.
4. Same-code singleflight still begins immediately, but repository freshness/suppression is resolved before joining the generated-tab FIFO. Only stale/unsuppressed captures serialize.
5. The matching availability timeout sets `bridgeStatus=unavailable` and clears `isRefreshing`, exposing `extension_unavailable` while re-enabling Update.

## TDD RED evidence

Initial focused command:

```text
cd apps/ts
bun test packages/shikiho-extension/src/background-capture.test.ts packages/shikiho-extension/src/storage.test.ts
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx
```

Observed before implementation (exit non-zero):

- Finding 1: the timeout regression expected a newly persisted `page_changed` diagnostic and suppression, but the old timer only resolved; the result had no diagnostic.
- Finding 2: the user-close regression called missing `coordinator.onTabRemoved`, so ownership removal handling was absent.
- Finding 3: storage suite failed to load because `SHIKIHO_SUCCESSFUL_OBSERVATIONS_STORAGE_KEY` was not exported; the stale-snapshot/fresh-observation coordinator test timed out because it incorrectly opened a tab.
- Finding 4: `returns a fresh different-code request before an active capture completes` timed out because repository resolution waited behind the active FIFO job.
- Finding 5: availability timeout regression failed because `isRefreshing` remained `true` instead of expected `false` after `extension_unavailable`.

The first combined Bun run reported `Cannot find module export SHIKIHO_SUCCESSFUL_OBSERVATIONS_STORAGE_KEY`, `0 pass / 1 fail / 1 error` for that module load, and the persisted-freshness coordinator regression timed out at 5000 ms. These failures were caused by the missing behavior, not test syntax.

## Focused GREEN evidence

```text
cd apps/ts
bun test packages/shikiho-extension/src/background-capture.test.ts packages/shikiho-extension/src/storage.test.ts
```

Result: exit 0, 26 passed, 0 failed, 67 assertions.

```text
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx
```

Result: exit 0, 1 file passed, 21 tests passed.

```text
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/web typecheck
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/pages/SymbolWorkbenchPage.test.tsx
bun run --filter @trading25/shikiho-extension build
```

Results: both typechecks exit 0; extension 63 passed / 0 failed / 172 assertions; covering web 3 files / 69 tests passed; extension build exit 0.

## Complete verification

```text
cd apps/ts
bun run quality:lint
bun run workspace:test
bun run workspace:build
```

Result: overall exit 0.

- lint: 471 files checked, 0 errors; one pre-existing unused-import warning and three configuration/source infos remain.
- workspace tests: root 15, contracts 30, utils 152, api-clients 87, extension 63, web 1359; total 1706 passed, 0 failed.
- workspace build: api-clients, local contracts generation, utils, and web production build all exit 0; Vite transformed 565 modules.

## Privacy and diff audits

```text
git diff --check
git diff 3eb71a4f..HEAD --check
rg -n "fetch\(|XMLHttpRequest|dangerouslySetInnerHTML|permissions.*cookies|webRequest|declarativeNetRequest" apps/ts/packages/shikiho-extension apps/ts/packages/web/src/components/SymbolWorkbench apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts
```

Results: both diff checks exit 0 with no output. Privacy grep exits 1 with zero matches, confirming no extension fetch/XHR, raw HTML rendering, cookie permission, webRequest, or declarativeNetRequest addition.

## Concerns

- Live normal-Atlas acceptance was not run, per final-fix subtask instruction.
- Repository lint still reports the same unrelated contracts unused-import warning and Biome schema/deprecation infos; there are no lint errors.
