# Symbol Workbench Shikiho Background Refresh Design

## Summary

Extend the local Atlas Shikiho bridge so Symbol Workbench can obtain the selected symbol automatically without requiring the user to press the Shikiho source link first. The extension may open one inactive Shikiho stock tab, capture the rendered DOM, and close only the tab it created. A successful snapshot is reused for 24 hours unless the user explicitly requests a refresh.

This design supersedes the explicit-open and no-additional-navigation constraints in `2026-07-10-symbol-workbench-shikiho-bridge-design.md`. All other privacy, validation, storage, presentation, and backend-isolation constraints remain binding.

> **Superseded tab lifecycle:** The approved [`2026-07-14-symbol-workbench-shikiho-warm-tab-reuse-design.md`](2026-07-14-symbol-workbench-shikiho-warm-tab-reuse-design.md) supersedes this document's immediate-close lifecycle and 15-second capture timeout. The current lifecycle prefers an exact-code rendered tab, may temporarily reuse one extension-owned inactive tab, and uses a 25-second outer capture timeout. This document remains authoritative only for constraints the newer design explicitly preserves.

## Goals

- Automatically resolve the selected Workbench symbol from a valid extension-local snapshot or an inactive authenticated Shikiho tab.
- Reuse a successful snapshot for 24 hours to avoid unnecessary page loads.
- Provide a manual refresh action that bypasses the 24-hour cache.
- Serialize background capture and singleflight duplicate requests for the same symbol.
- Close only tabs created by this extension workflow.
- Correctly classify the current Shikiho login and paid-plan prompt as `login_required` rather than `page_changed`.
- Preserve the last valid snapshot when automatic refresh fails.

## Non-goals

- Do not fetch Shikiho HTML from a service worker or Trading25 backend.
- Do not copy, inspect, export, or persist cookies, authorization headers, passwords, or browser storage credentials.
- Do not keep a permanent worker tab or navigate a user-owned Shikiho tab.
- Do not prefetch watchlists, rankings, comparison companies, or symbols other than the currently selected Workbench symbol.
- Do not run scheduled or bulk capture.
- Do not add FastAPI, OpenAPI, DuckDB, SQLite, dataset, portfolio, or remote-sync state.
- Do not remove the original-page link; it remains the source and troubleshooting path.

## Architecture

The Workbench bridge request becomes a cache-resolution request rather than a storage-only read.

```text
Workbench selects code
  -> localhost content script sends resolve_snapshot(code, forceRefresh)
  -> service worker checks the latest valid snapshot
  -> fresh snapshot (< 24 hours) returns immediately
  -> otherwise enqueue one background capture job
  -> chrome.tabs.create({ active: false, url: /stocks/{code} })
  -> Shikiho content script waits for stable DOM and sends capture result
  -> service worker stores snapshot or diagnostic
  -> service worker closes its own generated tab
  -> localhost bridge publishes the matching result
  -> Workbench validates code/schema and updates only the selected symbol
```

The service worker owns orchestration because it can create and close tabs without exposing navigation capability to the web page. The Shikiho content script remains the only component that reads rendered Shikiho DOM. The localhost page can request only a normalized four-digit symbol and cannot supply an arbitrary URL.

## Cache and Request Policy

- `SHIKIHO_CACHE_TTL_MS` is exactly `24 * 60 * 60 * 1000`.
- A snapshot is fresh when `now - capturedAt < SHIKIHO_CACHE_TTL_MS`.
- A snapshot at exactly the 24-hour boundary is expired.
- `forceRefresh: true` bypasses freshness but still joins an already-running job for the same code.
- Duplicate requests for the same code share one promise and one generated tab.
- Capture jobs for different codes run one at a time in request order.
- Only the currently selected Workbench symbol is requested; there is no speculative prefetch.
- After `login_required`, `page_changed`, or `timeout`, the same code has a 60-second retry suppression window unless the user uses manual refresh.
- Manual refresh bypasses both the 24-hour cache and retry suppression.

## Generated Tab Lifecycle

The service worker creates an inactive tab at the exact canonical URL `https://shikiho.toyokeizai.net/stocks/{code}`. It records the returned tab ID as extension-owned before awaiting capture.

The content script sends a validated runtime message containing the tab's derived code and one of:

- `success` with `ShikihoSnapshotV1`;
- `login_required`;
- `page_changed`.

The service worker accepts the result only when sender tab ID, requested code, and current job all match. It ignores messages from user-owned tabs for job completion, although passive capture from user-opened Shikiho tabs continues to update storage.

The generated tab is closed in a `finally` path after success, diagnostic, timeout, or cancellation. The extension never closes a tab whose ID it did not create for the current job. If the user closes the generated tab first, the job records `page_changed` and releases the queue.

The capture timeout is 15 seconds from tab creation. DOM quiet/max-wait behavior inside the content script remains unchanged and must complete within that outer timeout.

## Login and Page-State Classification

The current logged-out/paywalled stock page contains a visible login button and the heading `ベーシック・プレミアムプランでは、記事本文など、すべての情報が閲覧できます`. The extractor must classify this state as `login_required` before enforcing commentary anchors.

A visible `ログイン` button alone is sufficient only when the page also contains the paid-plan prompt or another existing login-required phrase. A navigation header login button on an otherwise fully authenticated rendered page must not by itself force `login_required`.

The sanitized real-page fixture must include the current stock identity, public summary, login button, and paid-plan heading. It must not contain account data, cookies, tokens, advertisements, or copyrighted full-page content.

## Bridge Contract

The page-to-extension request adds an explicit refresh flag:

```ts
interface ShikihoGetSnapshotRequestV1 {
  channel: 'trading25-shikiho-bridge';
  direction: 'page-to-extension';
  type: 'get_snapshot';
  requestId: string;
  code: string;
  forceRefresh: boolean;
}
```

The localhost content script converts this to the internal runtime request:

```ts
interface ResolveShikihoSnapshotRequest {
  type: 'resolve_snapshot';
  code: string;
  forceRefresh: boolean;
}
```

Arbitrary URLs, hostnames, paths, tab IDs, and credential material are not accepted from the page. Existing exact-key validation remains mandatory.

The extension response keeps the existing selected-code snapshot/diagnostic payload. A request ID still binds the response to the current Workbench request, and old responses are discarded after symbol changes.

## Workbench UI

The panel automatically requests the selected symbol on mount and on symbol change. The former source button is no longer part of the normal acquisition flow.

The header retains:

- status;
- edition/update metadata;
- capture time;
- `四季報で開く` source link;
- a compact `更新` button.

`更新` sends `forceRefresh: true`. While resolution is running, the button is disabled and status shows `取得中`. The existing snapshot remains visible during refresh. Automatic failure shows the diagnostic while retaining the snapshot as stale. `Login required` explains that the user must sign in to Company Shikiho Online in the same Atlas profile; it does not repeatedly open tabs during the 60-second suppression period.

## Permissions and Privacy

The extension adds only the minimum browser capabilities required to create and close its own inactive tabs. Host access remains limited to Company Shikiho stock pages and Trading25 localhost ports `5173` and `4173`.

- No `cookies`, `webRequest`, `declarativeNetRequest`, history, downloads, or native-messaging permission.
- No fetch/XHR issued by extension code.
- No credential or raw HTML capture.
- No capture payload logging or telemetry.
- Snapshots remain in `chrome.storage.local`, capped at 200 symbols.
- Source attribution remains visible.

Because automatic resolution performs a real Shikiho page navigation, the 24-hour TTL, one-at-a-time queue, same-code singleflight, selected-symbol-only policy, and 60-second failure suppression are mandatory safeguards.

## Testing

### Extractor

- classify the current paid-plan/login fixture as `login_required`;
- retain `page_changed` for authenticated-looking pages missing required commentary anchors;
- ensure a standalone navigation login button does not misclassify a valid authenticated fixture.

### Orchestrator

- return a snapshot at 23:59:59.999 without opening a tab;
- expire a snapshot at exactly 24:00:00.000;
- bypass freshness and retry suppression with manual refresh;
- singleflight duplicate same-code requests;
- serialize different-code jobs;
- accept completion only from the generated tab and matching code;
- close the generated tab after success, diagnostic, timeout, and thrown errors;
- never close a user-owned tab;
- preserve the previous valid snapshot after a failed refresh;
- stop a retry loop for 60 seconds after automatic failure.

### Bridge and Web

- validate `forceRefresh` as a required boolean exact key;
- request automatically on initial symbol and symbol change;
- discard a completed old-symbol response;
- keep an existing snapshot visible while refreshing;
- send force refresh from the update button once per click;
- disable the update button while resolving;
- render `取得中`, `Login required`, stale, partial, and captured states accessibly.

### Live Atlas acceptance

1. Load the rebuilt unpacked extension in the normal Atlas profile.
2. Open only `/symbol-workbench?symbol=7203`; do not press the Shikiho source link.
3. Confirm one inactive Shikiho tab opens and closes automatically.
4. Confirm the 7203 panel shows the validated snapshot or `Login required` when signed out.
5. Sign in when required, use `更新`, and confirm the panel fills without pressing the source link.
6. Reload Workbench within 24 hours and confirm no Shikiho tab opens.
7. Switch to another symbol and confirm one separate background capture with no 7203 content flash.
8. Press `更新` and confirm one forced refresh while the existing snapshot remains visible.

## Acceptance Criteria

1. Selecting an uncached or expired Workbench symbol automatically resolves it through one inactive Shikiho tab without using the source link.
2. A successful snapshot is reused for strictly less than 24 hours; manual refresh bypasses the cache.
3. Duplicate requests singleflight, different codes serialize, and automatic failures are suppressed for 60 seconds.
4. The extension closes only tabs created for its own capture jobs and closes them on every terminal path.
5. The current paid-plan/login page is reported as `Login required`, not `Page changed`.
6. Failed refresh never destroys the last valid snapshot or displays another symbol's data.
7. No credentials, raw HTML, backend state, remote sync, bulk crawl, or additional extension fetch/XHR is introduced.
8. Source, status, edition/update date, capture time, and manual update remain visible in the compact panel.
