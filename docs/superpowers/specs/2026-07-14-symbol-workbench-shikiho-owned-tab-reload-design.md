# Symbol Workbench Shikiho Owned-Tab Reload Recovery Design

## Status

Approved for implementation planning on 2026-07-14.

## Context

The Shikiho extension reuses an extension-owned inactive tab when no exact user-owned Shikiho tab is available. Some first visits render unusually slowly. The current acquisition waits for the content script and DOM until the existing 25-second acquisition deadline. Reloading the same slow tab manually often makes the page render quickly, but the extension cannot currently perform that recovery before the final timeout.

The recovery must preserve the existing privacy and ownership boundaries:

- never reload, navigate, or close a user-owned or exact-match user tab;
- do not add cookie access, Shikiho network requests, page clicks, raw HTML storage, or telemetry;
- keep a hard 25-second total acquisition deadline;
- reload the extension-owned tab at most once.

## Decision

For an extension-owned capture only, acquisition uses two phases measured from the moment the warm-tab lease is acquired:

1. Wait up to 7 seconds for the original `capture_now` request to complete.
2. If it has not completed, revalidate exact ownership and reload the same tab once.
3. Send a new `capture_now` request with a new request ID and use the remaining time before the original 25-second absolute deadline. With an immediate reload call this is at most 18 seconds; reload latency consumes part of those 18 seconds.

The 7-second recovery point and 25-second deadline are absolute timestamps derived from the injected clock. Timer scheduling drift must not extend the deadline.

## Ownership Boundary

The reload operation belongs to `WarmTabLeaseManager`, not to the generic acquisition dependency surface. The manager exposes an owned-handle operation such as `reloadOwned(handle)` and calls `chrome.tabs.reload` only after all of the following remain true:

- persisted lease identity matches the handle's tab ID, owner token, and generation;
- the lease is still in the `capturing` phase;
- the capture identity is still active in the current manager;
- the tab has not been removed or adopted by the user.

Activation abandons extension ownership as it does today. A stale handle, replaced generation, removed tab, or adopted tab must not reach `tabs.reload`. Existing exact-match user-tab acquisition stays outside this interface and can never trigger reload.

## Acquisition Flow

The owned-tab acquisition keeps one outer 25-second deadline and permits one recovery transition:

```text
owned lease acquired
  -> first capture request
     -> completes before 7s: use result, no reload
     -> still pending at 7s:
        -> ownership invalid: do not reload; fail through normal owned cleanup
        -> ownership valid: reload once
           -> second capture request with fresh request ID
              -> completes before absolute 25s: use second result
              -> misses absolute 25s: timeout and normal owned cleanup
```

Receiver-not-ready errors continue to use the existing 100-millisecond retry behavior. General errors, invalid responses, diagnostics, and terminal extraction results do not trigger reload. The reload is only a recovery for an unfinished owned capture at the 7-second milestone.

The first request cannot be cancelled after losing the race. Its promise remains isolated to its original request ID. A delayed first response or rejection must be observed but ignored and cannot satisfy the second request.

## Reload and Runtime Coordination

`chrome.tabs.reload` resolves when reload is initiated, not when the new content script is ready. The second phase therefore relies on the existing receiver-not-ready retry loop until the new `document_idle` content script registers.

The current `tabs.onUpdated(status=complete)` probe has a race with this registration: a single missing receiver immediately after reload can otherwise abandon the live capturing lease. Runtime reconciliation must become phase-aware:

- a `capturing` lease is not abandoned solely because the receiver is temporarily missing after a completed navigation;
- final acquisition failure remains responsible for `releaseFailure` and exact-match cleanup;
- an idle owned tab that is no longer hosted on a valid Shikiho stock page retains the existing abandonment behavior.

This change must not allow a stale or user-adopted tab to regain ownership.

## Error Handling

- Reload rejection or tab disappearance is a terminal owned-capture error.
- Ownership loss before reload prevents reload and enters normal safe cleanup; exact-match lease checks prevent closing a user-adopted tab.
- Timeout after reload emits the existing timeout outcome once and releases failure once.
- A successful or partial result uses the existing success transition. Diagnostics retain the existing failure-release behavior.
- Workbench receives the existing null public response on terminal acquisition failure, so it does not remain in the loading state.

## Observability

Existing capture timing logs remain content-free. Internal timing may record whether reload recovery occurred and how long reload initiation took, but it must not include symbol content, snapshot fields, page text, or URLs.

No public bridge schema, backend API, database, or OpenAPI contract changes are required.

## Required Tests

1. An owned first capture still pending at 7 seconds reloads the exact owned tab once, sends a fresh request ID, succeeds in phase two, and releases success once.
2. Success, partial success, diagnostic, general error, or invalid response before 7 seconds performs no reload.
3. Receiver-not-ready retry remains 100 milliseconds in both phases.
4. Phase-two timeout occurs at the original 25-second deadline, never at 25 seconds after reload, and releases failure/logs timeout once.
5. Reload latency reduces the available phase-two time. A hanging reload is still capped by the original deadline.
6. A delayed phase-one response after reload is ignored; only the phase-two request can complete recovery.
7. Exact user-tab capture never reloads, including when it is slow or times out.
8. `reloadOwned` reloads only the exact current capturing handle. Stale owner tokens, generations, activation abandonment, and removed tabs never reload another tab.
9. `tabs.onUpdated(status=complete)` with a temporarily missing receiver preserves a live capturing lease, while invalid idle ownership is still abandoned.
10. Activation and removal immediately before or after the recovery point cannot resurrect ownership or close a user-adopted tab.

## Non-Goals

- More than one automatic reload per acquisition.
- Reloading exact-match user tabs.
- Extending the total deadline beyond 25 seconds.
- Retrying general Chrome errors or malformed capture responses.
- Adding Shikiho-side fetches, login automation, page clicks, or persistent capture queues.

