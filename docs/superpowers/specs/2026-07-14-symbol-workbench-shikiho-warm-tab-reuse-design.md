# Symbol Workbench Shikiho Warm Tab Reuse Design

## Summary

Reduce Company Shikiho acquisition latency without navigating or closing user-owned tabs. Resolution keeps the existing cache policy, then prefers an already-rendered exact-code Shikiho tab, then reuses one short-lived extension-owned inactive tab, and creates a new inactive tab only as the final fallback.

The extension-owned tab remains warm for three minutes after a successful capture and has a five-minute maximum-age target from creation. If that boundary arrives during a capture or while the platform delays an alarm, the verified owned tab closes immediately after the capture or at the next extension event. It is still temporary rather than a permanent worker tab. A user-owned tab is eligible only when it already displays the requested code; the extension may capture its current rendered DOM but must not navigate, reload, activate, or close it.

This design supersedes the immediate-close requirement in `2026-07-12-symbol-workbench-shikiho-background-refresh-design.md`. Its cache, privacy, selected-symbol-only, FIFO, singleflight, storage, and backend-isolation constraints remain binding.

## Goals

- Reduce the time from `resolve_snapshot` to a validated capture when a useful Shikiho tab is already loaded.
- Preserve the 24-hour article cache, 15-minute current-day quote cache, 60-second retry suppression, same-code singleflight, and different-code FIFO queue.
- Prefer non-destructive capture from an exact-code user-owned tab.
- Reuse at most one short-lived extension-owned inactive tab.
- Make capture completion generation-safe across tab reuse and A -> B -> A navigation.
- Recover safely when the Manifest V3 service worker is stopped and restarted.
- Keep all extraction inside the Shikiho content script and issue no extension-owned Shikiho fetch/XHR.
- Emit local timing diagnostics sufficient to compare cache, exact-tab, warm-tab, and new-tab paths without logging captured content.

## Non-goals

- Do not navigate, reload, activate, pin, group, or close a user-owned Shikiho tab.
- Do not keep a permanent worker tab.
- Do not automate page clicks or depend on Shikiho's internal SPA router.
- Do not prefetch another symbol, watchlist, ranking, comparison company, or queue beyond the selected Workbench symbol.
- Do not add `cookies`, `tabs`, `activeTab`, `scripting`, `webRequest`, `declarativeNetRequest`, history, downloads, or native-messaging permission.
- Do not read tab URLs through the Tabs API.
- Do not change the extractor's approved fields or store raw HTML, credentials, capture payloads, or remote telemetry.
- Do not change FastAPI, OpenAPI, DuckDB, SQLite, datasets, portfolios, or the Workbench bridge's public schema.
- Do not optimize DOM readiness heuristics in this change. Timing evidence may justify a separate follow-up.

## Alternatives Considered

### Exact-code tab capture only

This is the smallest and safest change. It is very fast when the requested tab is already open, but all other misses retain the current create-and-close cost.

### Storage-only warm tab with best-effort timers

This preserves `permissions: ["storage"]`. The service worker and content script can both arm timers, and every later event can reconcile an expired lease. It cannot guarantee prompt closure after the service worker is stopped or the inactive tab is frozen, so a visible orphan tab may remain longer than promised.

### Exact-code capture plus alarm-backed warm tab

This is the selected approach. It adds only the non-content `alarms` permission so Chrome can wake the service worker to close an expired owned tab. Ownership and generation metadata remain in `chrome.storage.session`; every alarm and lifecycle event revalidates the token before closing anything. The permission expansion is narrow and avoids the broader `tabs` or host permissions.

## Resolution Order

For a normalized four-digit code:

1. Join an existing same-code singleflight, if present.
2. Read extension storage.
3. Return immediately when the stored state satisfies the existing article and quote freshness policy, unless `forceRefresh` is true.
4. Probe open tabs for a Shikiho content script already displaying the exact requested code.
5. If found, request a generation-bound capture from that exact-code tab without navigating it.
6. Otherwise acquire the extension-owned warm tab. Reuse it if its lease is valid; create a new inactive tab when no valid lease exists.
7. Navigate only the extension-owned tab to the canonical `https://shikiho.toyokeizai.net/stocks/{code}` URL when necessary.
8. After the owned content script reports the requested code and reaches its existing DOM-ready capture point, request a generation-bound capture.
9. Save the validated result. A successful or partial snapshot returns the owned tab to the idle pool; a terminal diagnostic or exception closes it immediately.

The different-code FIFO queue still covers steps 4 through 9. Cache hits for another code continue to return without waiting for an active capture.

## Components

### Exact tab discovery

The service worker calls `chrome.tabs.query({})` without URL filters. It receives tab identifiers but does not inspect sensitive `url`, `pendingUrl`, `title`, or favicon fields. It sends a bounded `probe_shikiho_code` message to candidate tab IDs in parallel. Tabs without the Shikiho content script reject or time out and are ignored.

The content script derives its response from `window.location.pathname` using the existing `normalizeShikihoCode` function. It returns only the current normalized code and a ready marker. The service worker selects an exact-code response deterministically, preferring the current extension-owned tab when it already shows the code and otherwise the lowest responsive tab ID.

The probe budget is 500 milliseconds. Probe failure is a cache miss, not a user-visible diagnostic.

### Generation-bound capture

Every capture job receives a `requestId` generated by `crypto.randomUUID()`. The service worker sends:

```ts
interface CaptureNowRequest {
  type: 'capture_now';
  requestId: string;
  code: string;
  waitForReady: boolean;
}
```

The content script verifies the requested code both before scheduling extraction and immediately before returning the result. Its direct response contains the same `requestId`, code, and the typed extractor result. The service worker accepts a job result only when all three match the current job and the responding tab is the selected lease tab.

An exact-code tab uses `waitForReady: false` because its current DOM is already rendered. A newly created or navigated owned tab uses `waitForReady: true` and the existing 500-millisecond quiet period / 10-second initial maximum wait before responding.

Passive captures caused by normal Shikiho DOM mutations continue updating storage, but they do not complete an explicit acquisition job. This prevents an old passive message from completing a later A -> B -> A reuse cycle.

### Warm tab lease manager

The lease manager is separate from cache/FIFO orchestration. It owns at most one record in `chrome.storage.session`:

```ts
interface ShikihoWarmTabLeaseV1 {
  version: 1;
  tabId: number;
  ownerToken: string;
  generation: number;
  phase: 'capturing' | 'idle';
  code: string | null;
  createdAt: number;
  idleDeadline: number | null;
}
```

Constants:

- `SHIKIHO_WARM_TAB_IDLE_MS = 3 * 60 * 1000`
- `SHIKIHO_WARM_TAB_MAX_AGE_MS = 5 * 60 * 1000`
- `SHIKIHO_CAPTURE_TIMEOUT_MS = 25 * 1000`

The 25-second timeout matches the current implementation and replaces the stale 15-second statement in the superseded design.

On successful or partial capture, the lease becomes idle and an alarm is scheduled for the earlier of the idle deadline and absolute maximum age. On reuse, the generation increments, the phase becomes `capturing`, and the previous alarm becomes harmless because its token/generation/deadline no longer match.

On `login_required`, `page_changed`, timeout, storage failure, navigation failure, or an invalid direct response, the extension closes the owned tab and removes the lease. It never puts a failed page into the warm pool.

### Ownership boundaries

Only a tab created by this lease manager can enter `chrome.storage.session` as extension-owned. A probed user tab can never acquire ownership.

Before navigating or closing an owned tab, the service worker reloads the session record and verifies `tabId`, `ownerToken`, generation, phase, and deadline as applicable. A missing or mismatched record makes the operation a no-op.

If the user activates the owned warm tab, ownership is abandoned immediately: cancel its alarm, remove the session record, and never navigate or close that tab later. The tab becomes user-owned. The same abandonment occurs when the tab navigates outside the canonical Shikiho stock path.

If the user closes the owned tab, `tabs.onRemoved` removes matching lease metadata and releases an active job through the existing diagnostic path.

## Manifest V3 Lifecycle

The in-memory FIFO and same-code singleflight remain process-local and apply while the service worker is alive. Durable tab ownership does not rely on those globals.

On each bridge request, alarm, tab lifecycle event, and extension startup, the lease manager reconciles `chrome.storage.session` with `chrome.tabs.get(tabId)`:

- missing tab: remove the lease;
- expired idle lease: close the verified owned tab and remove the lease;
- valid idle lease: make it available for reuse;
- stale `capturing` lease after worker restart: close the verified owned tab and remove the lease, because the original promise cannot be recovered;
- missing or invalid lease record: close nothing.

An alarm may be delayed by device sleep, but reconciliation preserves the maximum-age policy at the next extension event. A delayed alarm cannot close a reused or user-adopted tab because ownership token and generation validation are mandatory.

## Error Handling

- Exact user-tab probe or capture failure silently falls back to the owned-tab path unless it produced a validated Shikiho diagnostic.
- A validated exact user-tab diagnostic may be stored and returned, but the tab is never closed or reloaded.
- Owned navigation and capture retain the existing `login_required`, `page_changed`, and storage-error classification.
- The previous valid snapshot remains visible when refresh fails.
- Automatic failure retains the existing 60-second retry suppression; manual refresh bypasses it.
- A stale request ID, stale code, stale generation, old alarm, or response from another tab is ignored.
- All cleanup paths are idempotent.

## Local Timing Diagnostics

Each non-cache acquisition emits one local structured console event after completion:

```ts
interface ShikihoCaptureTiming {
  event: 'shikiho_capture_timing';
  mode: 'exact_user_tab' | 'warm_owned_same_code' | 'warm_owned_navigation' | 'new_owned_tab';
  outcome: 'success' | 'partial' | 'diagnostic' | 'timeout' | 'error';
  probeMs: number;
  navigationMs: number;
  captureMs: number;
  totalMs: number;
}
```

The event contains no article text, quote values, DOM, URL, account data, cookies, authorization data, or remote destination. It is not persisted. Cache hits do not log because they create no browser acquisition cost.

## Permissions and Privacy

The manifest changes from `permissions: ["storage"]` to `permissions: ["storage", "alarms"]`.

- No `tabs` permission is added.
- No host permission is added.
- Tab discovery does not query by URL or inspect URL/title/favicon fields.
- No page click, hidden interaction, or extension-owned Shikiho network request is introduced.
- Only rendered DOM from an already authenticated Atlas profile is extracted.
- Existing snapshot retention, approved-field validation, latest-200 cap, and localhost origin checks remain unchanged.

## Documentation Changes

Update `apps/ts/extensions/shikiho/README.md` to explain:

- exact-code user tabs may be read immediately but are never modified or closed;
- one extension-owned inactive tab may remain for up to three idle minutes and five total minutes;
- activating the warm tab transfers it to the user and prevents later automatic navigation/closure;
- failures close the owned tab immediately;
- the extension adds `alarms` only for reliable temporary-tab cleanup;
- no cookies, tab URLs, raw HTML, fetch/XHR, or page clicks are accessed.

Update the superseded background-refresh design's status or cross-reference so readers do not rely on its immediate-close and 15-second timeout statements.

## Testing

### Content script

- probe returns only the normalized current code;
- exact-code `capture_now` returns a matching request ID and result;
- wrong code is rejected before and after extraction;
- `waitForReady: false` captures immediately;
- `waitForReady: true` uses the existing quiet/max-wait controller;
- passive capture still saves snapshots but cannot complete an explicit job.

### Exact user-tab path

- exact tab success creates, navigates, activates, and closes no tab;
- exact tab diagnostic closes no tab;
- wrong-code, missing receiver, timeout, and code-change races fall back;
- multiple exact responses are resolved deterministically;
- stale request IDs and responses from unselected tabs are ignored.

### Warm tab lifecycle

- first miss creates one inactive owned tab;
- successful capture retains it idle and schedules cleanup;
- same-code reuse creates and navigates no tab;
- different-code reuse navigates the owned tab and creates no tab;
- A -> B -> A delayed results cannot complete the current job;
- terminal diagnostics, timeout, exceptions, and invalid responses close immediately;
- idle alarm closes only a matching idle lease;
- reuse makes an old alarm harmless;
- absolute maximum age closes after an active job completes;
- user activation and external navigation abandon ownership without closing;
- user removal clears the lease and releases the active job;
- a tab without a valid session lease is never closed.

### MV3 recovery

- valid idle lease rehydrates after worker restart;
- expired idle lease is cleaned up;
- stale capturing lease is cleaned up;
- missing tab clears metadata;
- malformed or missing session metadata closes nothing;
- delayed alarm validates token and generation.

### Regression

- 24-hour article and 15-minute quote freshness boundaries remain unchanged;
- manual refresh still bypasses freshness and retry suppression;
- same-code requests still singleflight;
- different-code requests still serialize FIFO;
- another code's fresh cache hit does not wait for an active capture;
- old Workbench symbol responses remain discarded;
- snapshot retention, extractor, contract, typecheck, lint, extension build, and web tests remain green.

## Acceptance Criteria

1. An exact-code user tab serves a Workbench refresh without tab creation, navigation, activation, reload, or closure.
2. Two uncached acquisitions within three minutes reuse one extension-owned tab when the user has not adopted it.
3. The extension-owned tab is closed after three idle minutes or five total minutes, except for platform-delayed alarms or an active capture; cleanup occurs at the next extension event in those cases.
4. No stale response or alarm can complete the wrong capture or close a reused/user-owned tab.
5. Only `storage` and `alarms` are declared permissions.
6. No Shikiho fetch/XHR, automated click, raw HTML storage, credential access, backend integration, or speculative symbol acquisition is introduced.
7. Local timing events distinguish exact-user, warm-same-code, warm-navigation, and new-tab paths without captured content.
