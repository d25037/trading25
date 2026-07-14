# Symbol Workbench Shikiho Progressive Capture Diagnostics Design

Date: 2026-07-14

## Status

Approved for implementation design. Chrome is the only supported browser runtime and manual verification target.

## Problem

The first Company Shikiho capture can consume the 25-second outer timeout. Existing timing reports only `probeMs`, `navigationMs`, `captureMs`, and `totalMs`. `captureMs` combines content-script receiver availability, DOM waiting, extraction, and message delivery, so it cannot identify the bottleneck.

The current owned-tab path injects the Shikiho content script at `document_idle`, waits for the entire document to remain mutation-quiet for 500 ms (up to 10 seconds), then extracts once. Unrelated advertising, analytics, or dynamic-widget mutations can therefore delay capture even after useful Shikiho fields are visible. The UI receives one terminal snapshot and cannot show fields that are already available.

## Goals

- Identify whether a slow capture is dominated by Chrome navigation, content-script/receiver availability, meaningful Shikiho field appearance, unrelated DOM churn, extraction, delivery, or storage.
- Show recognizable Shikiho fields incrementally in Symbol Workbench while capture continues.
- Preserve the last canonical snapshot during refresh and avoid flicker or data loss.
- Make terminal timeout/error diagnostics visible in the Workbench without requiring DevTools.
- Keep the 25-second outer deadline and 100 ms receiver-unavailable retry.
- Preserve the existing ownership rule: user tabs are never navigated, reloaded, activated, or closed.
- Keep all processing local to the Chrome extension and localhost Workbench.

## Non-goals

- No Shikiho network request, cookie access, backend endpoint, OpenAPI change, or database persistence.
- No raw HTML, selector dump, article text, quote value, or URL in timing traces.
- No automatic reload recovery.
- No use of provisional content for chart overlays, freshness TTL, diagnostics suppression, or canonical storage.
- No attempt to optimize the slow phase until a real slow capture trace identifies it.

## Chosen approach

Use three strictly separated data lanes.

### Canonical lane

The existing `ShikihoSnapshotV1` and `ShikihoCaptureDiagnosticV1` remain the source of truth for freshness, storage, chart overlay, and completed display. Only terminal results enter this lane.

### Provisional lane

An active capture emits a bounded provisional `ShikihoSnapshotV1` candidate whenever recognizable field coverage advances. Candidates are transported in memory, are scoped by attempt ID, code, and tab ID, and are never written to canonical snapshot storage. The Workbench merges candidate fields monotonically for the active attempt. A later transient absence cannot erase a field already observed.

If a canonical snapshot exists, missing candidate fields continue to display the canonical value and the panel labels the result as updating. If none exists, available candidate fields render immediately. Terminal success promotes the canonical response atomically. Timeout/error discards the candidate and preserves the prior canonical snapshot.

### Trace lane

The extension stores one metadata-only `ShikihoCaptureTraceV1` per code, limited to the same latest-200 policy as snapshots. A trace remains available after success, timeout, or error. It contains no captured values.

## Capture lifecycle

1. Chrome injects the Shikiho content script at `document_start` so the message receiver is registered before the page finishes loading. The passive capture controller remains disabled until `DOMContentLoaded` or a recognizable Shikiho identity appears, so early injection cannot persist a loading-state diagnostic.
2. Background acquisition creates an attempt ID, records queue/probe/acquire timing, and registers the expected code and tab ID before sending `capture_now`.
3. Receiver-unavailable errors continue to retry every 100 ms under the original 25-second absolute deadline. Attempts and elapsed receiver wait are recorded.
4. After the receiver accepts the request, the content script observes DOM mutations and samples at a bounded cadence: debounce 250 ms, with at most one forced sample per second while mutations continue.
5. Each sample runs the extractor once, measures extraction time, and compares a field-presence/content fingerprint. It emits progress only when phase, field coverage, or trace counters materially advance.
6. Unrelated mutations increment `mutationBatches` but do not reset completion unless the extracted field fingerprint changes.
7. Once the core fields (`features`, `consolidatedBusinesses`, and `commentary`) are present and the field fingerprint remains stable for 500 ms, the capture returns a terminal canonical result.
8. At the 25-second deadline, recognizable content becomes a terminal partial snapshot. If no recognizable content exists, the final inspection classifies login-required, page-changed, timeout, or error without converting an intermediate loading state into a canonical diagnostic.

## Field milestones

The trace records elapsed first-seen milliseconds for these field groups only:

- identity/company name
- quote
- features
- consolidated businesses
- commentary
- score
- comparison companies
- industries
- market themes
- profile
- edition label
- page-updated timestamp
- core-ready

The trace records field names and times, never field values.

## Trace contract

`ShikihoCaptureTraceV1` is versioned and strictly parsed. It includes:

- identity: schema version, attempt ID, canonical code, capture mode
- lifecycle: phase, start/update timestamps, outcome, wait-end reason
- receiver: attempts and ready elapsed milliseconds
- Chrome navigation timing: response start, DOM interactive, DOMContentLoaded end, and load end as relative milliseconds when available
- DOM: `document.readyState`, first-sample time, mutation batches, meaningful field changes, samples, present/missing field names, and first-seen milestones
- extraction: sample count, last/max/total milliseconds
- pipeline: probe, acquisition/navigation API, receiver, DOM observation, storage, and total milliseconds

Phases are bounded to `queued`, `probing_tabs`, `acquiring_tab`, `waiting_receiver`, `observing_dom`, `core_partial`, `core_ready`, `settling`, `saving`, `complete`, `timeout`, and `error`.

Wait-end reasons are bounded to `field_stable`, `deadline`, `login_confirmed`, `navigation_changed`, `invalid_response`, and `error`.

All arrays, identifiers, numbers, and timestamps use the existing contract size and validation discipline. Progress with a mismatched code, tab ID, attempt ID, stale sequence, or invalid payload is ignored.

## Transport

- Shikiho content script sends internal `capture_progress` runtime messages containing the attempt ID, monotonically increasing sequence, provisional candidate, and trace delta.
- Background validates `sender.tab.id` against the registered active attempt before accepting progress.
- The localhost content script maintains a long-lived runtime Port subscription for the currently selected code and forwards validated progress to the page through the existing origin-checked `window.postMessage` bridge.
- The Workbench page request ID remains the browser-page correlation key. The localhost bridge maps background attempt progress onto only the current page request and code.
- Provisional article content is never written to `chrome.storage.local` or `chrome.storage.session`. Only the metadata-only latest trace is persisted.
- Terminal snapshot delivery remains the existing request/response path for compatibility.

## Workbench behavior

The hook state becomes:

- `stableSnapshot`: last canonical snapshot for the selected code
- `candidate`: active attempt provisional snapshot
- `trace`: latest validated trace for the selected code
- `isRefreshing`: whether the active page request is awaiting a terminal result

The Company Shikiho panel:

- keeps the stable body mounted during refresh;
- overlays newly observed candidate fields without erasing stable fields;
- labels mixed content as `更新中` and reports candidate coverage;
- shows the active phase and elapsed time in the status row;
- provides a compact expandable `取得診断` section with the phase breakdown, receiver attempts, mutation/meaningful-change counts, first-seen field times, extraction metrics, and terminal reason;
- never uses candidate quote data for chart overlays.

Progress updates are ignored after symbol change, attempt replacement, terminal completion, timeout, or request-ID change.

## Failure handling

- A refresh failure does not clear a valid stable snapshot.
- A service-worker restart invalidates in-memory candidates. The stored metadata trace may show the last terminal/interrupted state, but no provisional article content survives.
- A content-script exception produces an error trace without exposing exception text to the page; only a bounded error category is retained.
- Navigation to another code cancels the attempt and does not save a `page_changed` diagnostic for the originally requested code.
- Login-required is terminal only after a stable marker on a sufficiently loaded document or at the deadline.

## Privacy and security

- Chrome only; remove Atlas wording from active Shikiho documentation.
- Keep manifest permissions at `storage` and `alarms`; add no host permission beyond the existing content-script match.
- No cookie reads, credential access, network fetches, backend calls, OpenAPI changes, or database writes.
- Validate every cross-context payload with exact keys, canonical codes, bounded sizes, attempt identity, and sender tab identity.
- Trace storage contains timings, counters, phases, field names, and result categories only.

## Testing

- Contract parser tests for valid/invalid traces and progress messages, exact keys, bounds, code/attempt mismatches, and oversized payloads.
- Content tests with deterministic timers for receiver registration, bounded sampling, unrelated mutation churn, field-first milestones, core stabilization, deadline partial, login confirmation, navigation cancellation, and extraction timing.
- Background broker tests for sender-tab/attempt/code/sequence validation and cleanup on terminal paths.
- Localhost bridge tests for Port subscription cleanup, current-request mapping, stale progress rejection, and origin safety.
- Hook tests for monotonic candidate merging, symbol/attempt replacement, stable snapshot preservation, terminal promotion, timeout discard, and no quote-overlay use.
- Panel tests for incremental fields, updating labels, trace details, accessibility, and no flicker.
- Full extension tests, extension typecheck/build, web tests/typecheck/build, Biome, dependency audit, and `git diff --check`.
- Manual Chrome acceptance: rebuild and reload the unpacked extension, reproduce one slow first capture, and confirm the trace identifies the dominant phase while fields appear progressively.

## Success criteria

- A slow first capture reports a dominant, separately measured phase rather than one combined `captureMs` value.
- The panel displays each recognizable field group before terminal completion when that group appears early.
- Continuous unrelated DOM mutation does not by itself delay core-ready completion.
- Timeout/error preserves the prior canonical snapshot and leaves a readable metadata trace.
- No provisional content affects freshness, storage, chart overlays, or another symbol/attempt.
- No new privacy, network, permission, backend, or persistence boundary is introduced.
