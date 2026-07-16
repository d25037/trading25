# One-shot owned tab final review fixes

## Status

DONE

## Findings addressed

### 1. Persist terminal state before closing an owned tab

- `tab-acquisition` now finishes the terminal broker trace and returns an idempotent `releaseOwnedTab` handoff for extension-owned captures.
- Exact user-owned captures return `releaseOwnedTab: null` and remain outside all owned cleanup paths.
- `background-capture` persists the snapshot or diagnostic, merges and persists terminal trace storage timing, reads the repository result, and only then invokes the cleanup handoff in `finally`.
- Storage and repository failures still invoke the cleanup handoff, preventing leaked extension-owned tabs.
- Acquisition failures that cannot return a handoff retain immediate failure cleanup after their synthetic terminal trace is finished.
- A timing publication failure before handoff also retains immediate failure cleanup.

### 2. Protect user-adopted tabs across reconciliation/alarm awaits

- `closeExact` now requires an explicit adoption epoch rather than sampling one after arbitrary awaits.
- `reconcile` snapshots adoption epochs synchronously before its first storage/tab await.
- `onAlarm` snapshots the target tab adoption epoch synchronously before its first storage await.
- `onActivated` continues to invalidate ownership synchronously before clearing persisted ownership asynchronously.
- Release paths use the epoch recorded when the active capture was created.
- Deterministic interleaving tests prove activation during `tabs.get` reconciliation and during alarm storage lookup prevents tab removal.

## TDD evidence

### RED: persistence and adoption races

Command:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/warm-tab-lease.test.ts src/background-capture.test.ts
```

Result: expected failure, 4 tests failed:

- reconciliation activation race removed tab 100
- alarm activation race removed tab 44
- snapshot/trace order omitted owned release
- snapshot storage failure omitted owned release

### RED: pre-handoff exception cleanup

Command:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/tab-acquisition.test.ts -t 'releases an owned tab if timing publication fails before cleanup handoff'
```

Result: expected failure; `releaseFailure` received 0 calls instead of 1.

### GREEN: focused regression suite

Command:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/warm-tab-lease.test.ts src/background-capture.test.ts src/tab-acquisition.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Result: 95 tests passed, 0 failed, 344 assertions; typecheck exited 0. The later full suite includes the additional diagnostic-order and pre-handoff cleanup cases.

## Full verification

Commands:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bunx biome check extensions/shikiho
bun run quality:deps:audit
git diff --check
```

Results:

- extension tests: 260 passed, 0 failed, 928 assertions across 15 files
- typecheck: passed
- build: passed
- Biome: passed after formatting one changed function signature
- dependency audit: passed, 6 manifests checked
- `git diff --check`: passed
- manifest/privacy contract coverage remains in the full extension suite

## Concerns

None.

## Follow-up review: activation-first cleanup race

### Finding

The original epoch fix protected cleanup that started before activation. It did not protect the inverse ordering: `onActivated(tabId)` synchronously advanced the epoch, then paused while removing session metadata; a later `reconcile` or `onAlarm` sampled that already-advanced epoch as its expected value and could close the adopted tab.

### RED

Command:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/warm-tab-lease.test.ts -t 'starting after activation'
```

Result: expected failure, 2 tests failed. Reconciliation removed tab 100 and the legacy alarm removed tab 44 while their activation-first metadata removals were deliberately paused.

### Fix

- Added an explicit synchronous `adoptedTabs` state in addition to the generation epoch.
- Activation and other ownership-abandonment paths mark the tab adopted before their first await.
- Every close path requires both the expected generation epoch and absence from `adoptedTabs`.
- Tab removal forgets the adopted marker.
- A newly extension-created tab begins a new owned generation and clears a stale marker for that reused ID. Chrome cannot reuse an ID while the adopted tab still exists; after removal, the new generation prevents late work from the old generation from clearing or closing the new lease.
- Persisted ownership removal also verifies its captured generation after storage awaits.

### GREEN

Command:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/warm-tab-lease.test.ts src/tab-acquisition.test.ts src/background-capture.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Result: 100 tests passed, 0 failed, 356 assertions; typecheck passed. This includes both cleanup-first and activation-first interleavings, tab-ID reuse, release/exception cleanup, persistence ordering, and exact-user-tab acquisition invariants.

Full extension verification after the follow-up passed 263 tests with 937 assertions. Build, Biome, dependency audit, and `git diff --check` also passed.
