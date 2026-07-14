# Symbol Workbench Shikiho Progressive Capture Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show Company Shikiho fields incrementally in Symbol Workbench and persist a metadata-only trace that identifies the exact slow capture phase in Chrome.

**Architecture:** Keep canonical snapshots, in-memory provisional candidates, and persisted metadata traces as separate lanes. A `document_start` content script performs bounded field-aware sampling, sends attempt-scoped progress to a background broker, and the broker streams it over a Chrome runtime Port to the localhost bridge; the React hook preserves the canonical snapshot and composes a display-only candidate.

**Tech Stack:** Chrome Manifest V3, TypeScript, Bun test, React 19, Vite, `chrome.runtime.Port`, `chrome.storage.local`, MutationObserver, Navigation Timing.

## Global Constraints

- Chrome is the only supported browser runtime and manual verification target.
- Keep the 25-second absolute capture deadline and 100 ms receiver-unavailable retry.
- Never navigate, reload, activate, or close a user-owned Shikiho tab.
- Do not add Shikiho network requests, cookie access, backend endpoints, OpenAPI changes, or database persistence.
- Do not put raw HTML, selectors, URLs, article text, quote values, or exception text in traces.
- Provisional content must remain in memory and must never affect canonical storage, TTL, diagnostics suppression, or chart overlays.
- Keep manifest permissions at `storage` and `alarms` and add no host permissions.
- Every cross-context payload must use exact-key parsing, canonical four-digit codes, bounded arrays/strings/numbers, attempt identity, sender tab identity, and monotonically increasing sequence validation.
- TDD is mandatory: add the focused failing test, observe the expected failure, implement the smallest production change, rerun focused tests, then run the task gate.

---

## File map

- `apps/ts/extensions/shikiho/src/contract.ts`: public trace, progress, and page-bridge contracts with strict parsers.
- `apps/ts/extensions/shikiho/src/capture-progress.ts`: active-attempt registry, trace merging, sender validation, Port subscriptions, and terminal cleanup.
- `apps/ts/extensions/shikiho/src/progressive-capture.ts`: bounded DOM sampling, field milestones, candidate emission, field-stability completion, and deadline classification.
- `apps/ts/extensions/shikiho/src/shikiho-tab-bridge.ts`: request correlation and instrumented terminal response.
- `apps/ts/extensions/shikiho/src/shikiho-content.ts`: `document_start` receiver wiring, safe passive-controller start, navigation timing, and progress publication.
- `apps/ts/extensions/shikiho/src/tab-acquisition.ts`: receiver attempts/timing and broker lifecycle integration under the existing deadline.
- `apps/ts/extensions/shikiho/src/storage.ts`: latest-200 metadata trace persistence only.
- `apps/ts/extensions/shikiho/src/background.ts`: production broker, runtime message, runtime Port, acquisition, and repository wiring.
- `apps/ts/extensions/shikiho/src/localhost-content.ts`: current-code Port subscription and origin-checked page progress forwarding.
- `apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts`: stable/candidate/trace state, stale progress rejection, and display-only merge.
- `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.tsx`: compact phase and timing diagnostics.
- `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`: updating label, progressive display, and diagnostics composition.
- `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`: pass stable and display-only Shikiho state separately.
- `apps/ts/extensions/shikiho/README.md`: Chrome installation, Reload, diagnostics, and manual reproduction instructions.

---

### Task 1: Define strict trace and progress contracts

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/contract.ts`
- Test: `apps/ts/extensions/shikiho/src/contract.test.ts`

**Interfaces:**
- Produces: `ShikihoCaptureTraceV1`, `ShikihoCaptureProgressV1`, `ShikihoFieldMilestonesV1`, `ShikihoTraceMode`, `parseShikihoCaptureTrace`, `parseShikihoCaptureProgress`.
- Produces: `ShikihoBridgeResponseV1` variant `capture_progress` containing the current page `requestId`.
- Produces: terminal `snapshot` bridge responses with `trace: ShikihoCaptureTraceV1 | null`.
- Consumes: existing `ShikihoSnapshotV1`, canonical code parser, timestamp parser, exact-key validation, and 64 KiB public payload limit.

- [ ] **Step 1: Add failing contract tests**

Add fixtures with no captured values:

```ts
const trace: ShikihoCaptureTraceV1 = {
  schemaVersion: 1,
  attemptId: 'attempt-1',
  code: '7203',
  mode: 'new_owned_tab',
  phase: 'observing_dom',
  startedAt: '2026-07-14T00:00:00.000Z',
  updatedAt: '2026-07-14T00:00:03.000Z',
  outcome: null,
  waitEndReason: null,
  receiverAttempts: 3,
  receiverReadyMs: 210,
  documentReadyState: 'interactive',
  navigation: { responseStartMs: 80, domInteractiveMs: 900, domContentLoadedMs: null, loadEndMs: null },
  dom: {
    firstSampleMs: 230,
    mutationBatches: 40,
    meaningfulChanges: 2,
    samples: 4,
    presentFields: ['identity', 'features'],
    missingFields: ['consolidatedBusinesses', 'commentary'],
    firstSeenMs: { identity: 230, quote: null, features: 1100, consolidatedBusinesses: null, commentary: null,
      score: null, comparisonCompanies: null, industries: null, marketThemes: null, profile: null,
      editionLabel: null, pageUpdatedAt: null, coreReady: null },
  },
  extraction: { samples: 4, lastMs: 3, maxMs: 5, totalMs: 14 },
  timings: { probeMs: 20, acquisitionMs: 35, receiverMs: 210, domObservationMs: 2760, storageMs: 0, totalMs: 3000 },
};

expect(parseShikihoCaptureTrace(trace)).toEqual(trace);
expect(parseShikihoCaptureTrace({ ...trace, code: '72030' })).toBeNull();
expect(parseShikihoCaptureTrace({ ...trace, extra: true })).toBeNull();
expect(parseShikihoCaptureTrace({ ...trace, receiverAttempts: -1 })).toBeNull();
expect(parseShikihoCaptureTrace({ ...trace, dom: { ...trace.dom, presentFields: ['features', 'features'] } })).toBeNull();
```

Test `capture_progress` exact keys, request/attempt IDs, code agreement, monotonically bounded sequence, candidate code agreement, and rejection above 64 KiB.

- [ ] **Step 2: Run the contract tests and verify RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/contract.test.ts
```

Expected: FAIL because the trace/progress types and parsers do not exist.

- [ ] **Step 3: Implement the versioned types and parsers**

Use fixed unions and nullable numbers; do not accept arbitrary phase/field strings:

```ts
export type ShikihoTracePhase =
  | 'queued' | 'probing_tabs' | 'acquiring_tab' | 'waiting_receiver'
  | 'observing_dom' | 'core_partial' | 'core_ready' | 'settling'
  | 'saving' | 'complete' | 'timeout' | 'error';

export type ShikihoTraceOutcome = 'success' | 'partial' | 'login_required' | 'page_changed' | 'timeout' | 'error';
export type ShikihoTraceMode = 'exact_user_tab' | 'new_owned_tab' | 'warm_owned_same_code' | 'warm_owned_navigated';
export type ShikihoWaitEndReason =
  | 'field_stable' | 'deadline' | 'login_confirmed' | 'navigation_changed' | 'invalid_response' | 'error';

export interface ShikihoCaptureProgressV1 {
  schemaVersion: 1;
  attemptId: string;
  code: string;
  sequence: number;
  candidate: ShikihoSnapshotV1 | null;
  trace: ShikihoCaptureTraceV1;
}
```

Add a `capture_progress` `ShikihoBridgeResponseV1` variant with exact fields `channel`, `direction`, `type`, `requestId`, `code`, `attemptId`, `sequence`, `candidate`, and `trace`. Reuse `parseShikihoSnapshot` for candidates and reject code disagreement. Add `trace` to the terminal `snapshot` response and its exact-key parser so the last persisted trace remains visible after completion or timeout.

- [ ] **Step 4: Run focused tests and extension typecheck**

```bash
cd apps/ts
bun test extensions/shikiho/src/contract.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: contract tests PASS and typecheck exits 0.

- [ ] **Step 5: Commit Task 1**

```bash
git add apps/ts/extensions/shikiho/src/contract.ts apps/ts/extensions/shikiho/src/contract.test.ts
git commit -m "feat(shikiho): define capture progress trace"
```

---

### Task 2: Persist terminal metadata traces and broker active progress

**Files:**
- Create: `apps/ts/extensions/shikiho/src/capture-progress.ts`
- Create: `apps/ts/extensions/shikiho/src/capture-progress.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/storage.ts`
- Modify: `apps/ts/extensions/shikiho/src/storage.test.ts`

**Interfaces:**
- Consumes: `ShikihoCaptureProgressV1`, `ShikihoCaptureTraceV1`, `ShikihoTraceMode`.
- Produces: `SHIKIHO_TRACES_STORAGE_KEY`, repository `getTrace(code)` and `saveTrace(trace)`.
- Produces: `CaptureProgressBroker.registerAttempt`, `recordReceiverAttempt`, `acceptContentProgress`, `finishAttempt`, `attachPort`, and `abandonAttempt`.

- [ ] **Step 1: Add failing trace repository tests**

Verify terminal traces retain the newest `updatedAt`, reject older writes, cap at 200 codes, and never share the canonical snapshot/diagnostic keys:

```ts
await repository.saveTrace(trace('7203', '2026-07-14T00:00:02.000Z'));
await repository.saveTrace(trace('7203', '2026-07-14T00:00:01.000Z'));
expect(await repository.getTrace('7203')).toMatchObject({ updatedAt: '2026-07-14T00:00:02.000Z' });
```

- [ ] **Step 2: Add failing broker security/race tests**

Cover:

```ts
broker.registerAttempt({ attemptId: 'a1', tabId: 41, code: '7203', mode: 'new_owned_tab', startedAtMs: 100 });
expect(await broker.acceptContentProgress(progress({ attemptId: 'a1', code: '7203', sequence: 1 }), 41)).toBe(true);
expect(await broker.acceptContentProgress(progress({ attemptId: 'a1', code: '7203', sequence: 1 }), 41)).toBe(false); // stale
expect(await broker.acceptContentProgress(progress({ attemptId: 'a1', code: '7203', sequence: 2 }), 42)).toBe(false); // wrong sender
expect(await broker.acceptContentProgress(progress({ attemptId: 'a1', code: '6758', sequence: 2 }), 41)).toBe(false); // wrong code
```

Also prove terminal/abandon removes the active attempt, disconnected Ports are removed idempotently, a Port receives only its subscribed code, and candidate content is never passed to trace persistence.

- [ ] **Step 3: Run repository and broker tests and verify RED**

```bash
cd apps/ts
bun test extensions/shikiho/src/storage.test.ts extensions/shikiho/src/capture-progress.test.ts
```

Expected: FAIL because trace repository and broker do not exist.

- [ ] **Step 4: Implement the repository and pure broker**

Use a small Port abstraction to keep Chrome globals out of tests:

```ts
export interface ProgressPort {
  postMessage(message: unknown): void;
  onMessage: ListenerEvent<(message: unknown) => void>;
  onDisconnect: ListenerEvent<() => void>;
}

export interface CaptureProgressBroker {
  registerAttempt(input: ActiveCaptureAttempt): void;
  recordReceiverAttempt(attemptId: string, elapsedMs: number): void;
  acceptContentProgress(progress: ShikihoCaptureProgressV1, senderTabId: number): Promise<boolean>;
  finishAttempt(attemptId: string, trace: ShikihoCaptureTraceV1): Promise<void>;
  abandonAttempt(attemptId: string): void;
  attachPort(port: ProgressPort): () => void;
}
```

Subscription input must be exactly `{type:'subscribe_capture_progress', code:string}`. Outbound messages must be exactly `{type:'capture_progress', progress:ShikihoCaptureProgressV1}`. Store only `trace` at terminal; broadcast candidate in memory only.

- [ ] **Step 5: Run focused tests and typecheck**

```bash
cd apps/ts
bun test extensions/shikiho/src/storage.test.ts extensions/shikiho/src/capture-progress.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: all focused tests PASS and typecheck exits 0.

- [ ] **Step 6: Commit Task 2**

```bash
git add apps/ts/extensions/shikiho/src/capture-progress.ts apps/ts/extensions/shikiho/src/capture-progress.test.ts apps/ts/extensions/shikiho/src/storage.ts apps/ts/extensions/shikiho/src/storage.test.ts
git commit -m "feat(shikiho): broker capture progress traces"
```

---

### Task 3: Replace whole-document quiet waiting with progressive field sampling

**Files:**
- Create: `apps/ts/extensions/shikiho/src/progressive-capture.ts`
- Create: `apps/ts/extensions/shikiho/src/progressive-capture.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/shikiho-tab-bridge.ts`
- Modify: `apps/ts/extensions/shikiho/src/shikiho-tab-bridge.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/shikiho-content.ts`
- Create: `apps/ts/extensions/shikiho/src/manifest.test.ts`
- Modify: `apps/ts/extensions/shikiho/manifest.json`

**Interfaces:**
- Consumes: `extractShikihoPage`, `ShikihoCaptureProgressV1`, `ShikihoCaptureTraceV1`.
- Produces: `createProgressiveShikihoCapture(options).run(request)`.
- Produces: terminal `{result: ShikihoExtractionResult, trace: ShikihoCaptureTraceV1}` and progress callback events.
- Produces: `ProgressiveCaptureCancelledError` for code/navigation replacement; callers must not persist it as `page_changed`.

- [ ] **Step 1: Add failing deterministic sampler tests**

Use injected `now`, scheduler, observer, current code, ready state, navigation timing, and extractor. Cover:

- first sample immediately after request acceptance;
- 250 ms debounce and at most one forced sample per 1,000 ms under continuous mutation;
- no progress when only mutation count changes and field fingerprint does not;
- provisional candidate when recognizable coverage advances;
- first-seen milestone written once;
- core fields plus unchanged field fingerprint for 500 ms returns captured terminal result;
- deadline with recognizable content returns partial;
- deadline with stable login marker returns `login_required`;
- code change returns navigation-changed cancellation without a canonical diagnostic;
- extraction last/max/total timings remain finite and nonnegative;
- stop disconnects observers and cancels every timer.

Representative test:

```ts
test('continuous unrelated mutations do not delay a stable core capture', async () => {
  const h = harness({ samples: [partialFeatures, completeCore, completeCore] });
  const running = h.capture.run(request('7203'));
  h.fireMutation(100);
  h.fireMutation(200);
  h.advanceTo(750);
  await expect(running).resolves.toMatchObject({ result: { kind: 'success', snapshot: { status: 'captured' } } });
  expect(h.progressCandidates()).toHaveLength(2);
});
```

- [ ] **Step 2: Run sampler/bridge/manifest tests and verify RED**

```bash
cd apps/ts
bun test extensions/shikiho/src/progressive-capture.test.ts extensions/shikiho/src/shikiho-tab-bridge.test.ts extensions/shikiho/src/manifest.test.ts
```

Expected: FAIL because progressive capture and `document_start` wiring do not exist.

- [ ] **Step 3: Implement the pure progressive sampler**

Expose the bounded constants and a single run interface:

```ts
export const SHIKIHO_SAMPLE_DEBOUNCE_MS = 250;
export const SHIKIHO_SAMPLE_MAX_INTERVAL_MS = 1_000;
export const SHIKIHO_FIELD_STABLE_MS = 500;

export interface ProgressiveCaptureRequest {
  attemptId: string;
  code: string;
  mode: ShikihoTraceMode;
  deadlineMs: number;
  receiverAttempts: number;
  receiverReadyMs: number;
}

export interface ProgressiveCaptureResult {
  result: ShikihoExtractionResult;
  trace: ShikihoCaptureTraceV1;
}

export class ProgressiveCaptureCancelledError extends Error {
  readonly reason = 'navigation_changed' as const;
}
```

Derive `presentFields` and `firstSeenMs` from parsed snapshot fields. Compare field/content fingerprints, not raw DOM mutation activity, for stability. Do not include extracted values in trace.

- [ ] **Step 4: Wire bridge and `document_start` safely**

Change `capture_now` to include `attemptId`, mode, deadline, and receiver metrics. Return terminal result and trace with exact request identity. `shikiho-content.ts` must register the bridge immediately, but start passive canonical capture only after `DOMContentLoaded` or recognizable identity. Replace `waitForDomQuiet` with the sampler.

Set:

```json
"run_at": "document_start"
```

for only the Shikiho content script; localhost remains `document_start`.

- [ ] **Step 5: Run focused and extension tests**

```bash
cd apps/ts
bun test extensions/shikiho/src/progressive-capture.test.ts extensions/shikiho/src/shikiho-tab-bridge.test.ts extensions/shikiho/src/manifest.test.ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: focused and full extension suites PASS; typecheck exits 0.

- [ ] **Step 6: Commit Task 3**

```bash
git add apps/ts/extensions/shikiho/manifest.json apps/ts/extensions/shikiho/src/manifest.test.ts apps/ts/extensions/shikiho/src/progressive-capture.ts apps/ts/extensions/shikiho/src/progressive-capture.test.ts apps/ts/extensions/shikiho/src/shikiho-tab-bridge.ts apps/ts/extensions/shikiho/src/shikiho-tab-bridge.test.ts apps/ts/extensions/shikiho/src/shikiho-content.ts
git commit -m "feat(shikiho): capture fields progressively"
```

---

### Task 4: Instrument acquisition and production background routing

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/tab-acquisition.ts`
- Modify: `apps/ts/extensions/shikiho/src/tab-acquisition.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/background.ts`
- Modify: `apps/ts/extensions/shikiho/src/background-capture.ts`
- Modify: `apps/ts/extensions/shikiho/src/background-capture.test.ts`

**Interfaces:**
- Consumes: `CaptureProgressBroker`, instrumented `capture_now`, terminal trace.
- Produces: one acquisition attempt ID across receiver retries, receiver attempt count/timing, terminal trace completion, and `{snapshot, diagnostic, trace}` resolution.

- [ ] **Step 1: Add failing acquisition lifecycle tests**

Verify:

- broker registration occurs before the first `sendTabMessage`;
- one attempt ID survives multiple 100 ms receiver retries;
- receiver attempts/ready time merge into content trace;
- the same absolute 25-second deadline reaches content;
- terminal success/partial/diagnostic/error/timeout always calls finish or abandon exactly once;
- late progress cannot survive terminal cleanup;
- exact user tabs remain untouched and get mode `exact_user_tab`;
- a failed refresh preserves the repository snapshot in the returned public state.

```ts
expect(h.events.slice(0, 2)).toEqual(['register:attempt-1', 'send:attempt-1']);
expect(captureRequests.map((request) => request.attemptId)).toEqual(['attempt-1', 'attempt-1']);
expect(captureRequests.at(-1)?.deadlineMs).toBe(startMs + SHIKIHO_CAPTURE_TIMEOUT_MS);
```

- [ ] **Step 2: Run acquisition/background tests and verify RED**

```bash
cd apps/ts
bun test extensions/shikiho/src/tab-acquisition.test.ts extensions/shikiho/src/background-capture.test.ts
```

Expected: FAIL because broker lifecycle and trace resolution are not wired.

- [ ] **Step 3: Integrate attempt lifecycle**

Extend dependencies explicitly:

```ts
export interface ShikihoTabAcquisitionDeps {
  // existing dependencies
  progress: Pick<CaptureProgressBroker,
    'registerAttempt' | 'recordReceiverAttempt' | 'finishAttempt' | 'abandonAttempt'>;
}
```

Generate one attempt ID per chosen tab acquisition, register before messaging, reuse it across receiver retries, and pass the unchanged absolute deadline. Preserve request ID validation for each message response. Finish trace before releasing the owned lease; abandon in every non-terminal exception path.

- [ ] **Step 4: Wire Chrome runtime messages and Ports in `background.ts`**

Create one broker. Handle only strictly parsed internal `capture_progress` messages and require `sender.tab.id`. Attach only Ports named `shikiho-capture-progress-v1`. Wire trace repository reads into public resolve state without exposing provisional content.

- [ ] **Step 5: Run focused and extension gates**

```bash
cd apps/ts
bun test extensions/shikiho/src/tab-acquisition.test.ts extensions/shikiho/src/background-capture.test.ts extensions/shikiho/src/capture-progress.test.ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: all tests PASS and typecheck exits 0.

- [ ] **Step 6: Commit Task 4**

```bash
git add apps/ts/extensions/shikiho/src/tab-acquisition.ts apps/ts/extensions/shikiho/src/tab-acquisition.test.ts apps/ts/extensions/shikiho/src/background.ts apps/ts/extensions/shikiho/src/background-capture.ts apps/ts/extensions/shikiho/src/background-capture.test.ts
git commit -m "feat(shikiho): trace acquisition phases"
```

---

### Task 5: Stream progress through the localhost bridge

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/localhost-content.ts`
- Modify: `apps/ts/extensions/shikiho/src/localhost-content.test.ts`

**Interfaces:**
- Consumes: Port messages `{type:'capture_progress', progress}` and public `capture_progress` bridge variant.
- Produces: one current-code Port subscription and page-request-scoped progress forwarding.

- [ ] **Step 1: Add failing Port and stale-message tests**

Cover approved localhost origins, exact Port name, subscription on `get_snapshot`, resubscription on code/request replacement, stale code/attempt/sequence rejection, request ID mapping, disconnect cleanup, malformed Port messages, and no progress after bridge stop.

```ts
h.pageGetSnapshot('7203', 'page-1');
expect(h.port.postMessage).toHaveBeenCalledWith({ type: 'subscribe_capture_progress', code: '7203' });
h.port.emit(progressMessage('7203', 'attempt-1', 1));
expect(h.windowMessages.at(-1)).toMatchObject({ type: 'capture_progress', requestId: 'page-1' });
```

- [ ] **Step 2: Run localhost tests and verify RED**

```bash
cd apps/ts
bun test extensions/shikiho/src/localhost-content.test.ts
```

Expected: FAIL because Port support does not exist.

- [ ] **Step 3: Add an injected Port adapter and strict forwarding**

Extend `LocalhostBridgeOptions` with `connectProgressPort(): ProgressPort`. Open one Port for the bridge lifetime, subscribe only after a valid `get_snapshot`, map accepted progress onto `currentRequest.requestId`, and preserve the existing `event.source === window` and origin restrictions.

- [ ] **Step 4: Run focused/full extension tests and typecheck**

```bash
cd apps/ts
bun test extensions/shikiho/src/localhost-content.test.ts extensions/shikiho/src/contract.test.ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: tests PASS and typecheck exits 0.

- [ ] **Step 5: Commit Task 5**

```bash
git add apps/ts/extensions/shikiho/src/localhost-content.ts apps/ts/extensions/shikiho/src/localhost-content.test.ts
git commit -m "feat(shikiho): stream capture progress to web"
```

---

### Task 6: Preserve stable state and compose display-only candidates

**Files:**
- Modify: `apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts`
- Modify: `apps/ts/packages/web/src/hooks/useShikihoSnapshot.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`

**Interfaces:**
- Consumes: public `capture_progress` events.
- Produces: hook fields `snapshot` (canonical only), `displaySnapshot`, `candidate`, `trace`, and `isRefreshing`.
- Guarantees: chart overlay continues consuming only `snapshot`.

- [ ] **Step 1: Add failing pure merge and hook race tests**

Test a pure helper:

```ts
const display = mergeShikihoDisplaySnapshot(stable, candidate1);
expect(display?.features).toBe(candidate1.features);
expect(display?.consolidatedBusinesses).toBe(stable.consolidatedBusinesses);
expect(display?.quote).toEqual(stable.quote);
```

Cover monotonic candidate sequence, same-attempt merge, new-attempt reset, symbol change, stale request ID, terminal promotion, timeout/error candidate discard, stable snapshot preservation, and `snapshot` never receiving candidate quote/content.

- [ ] **Step 2: Run hook/page tests and verify RED**

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx src/pages/SymbolWorkbenchPage.test.tsx
```

Expected: FAIL because progress state and display snapshot do not exist.

- [ ] **Step 3: Implement stable/candidate/trace state**

Return the explicit split:

```ts
export interface ShikihoSnapshotResult {
  bridgeStatus: ShikihoBridgeStatus;
  snapshot: ShikihoSnapshotV1 | null;        // canonical, downstream-safe
  displaySnapshot: ShikihoSnapshotV1 | null; // panel-only composition
  candidate: ShikihoSnapshotV1 | null;
  trace: ShikihoCaptureTraceV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  captureState: ShikihoCaptureState;
  isRefreshing: boolean;
  refresh(): void;
}
```

Merge arrays and scalar groups only when the candidate field is present according to `missingFields`. Never promote candidate quote to canonical. Preserve stable state when runtime returns `{ok:false}` or a terminal trace reports failure.

- [ ] **Step 4: Wire panel-only display separately from chart overlay**

`SymbolWorkbenchPage` must continue using `shikihoSnapshot.snapshot` for `composeShikihoDailyOverlay`. Pass `displaySnapshot`, `candidate`, and `trace` only to `SymbolWorkbenchHeader`/`ShikihoPanel`.

- [ ] **Step 5: Run focused web tests and typecheck**

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx src/pages/SymbolWorkbenchPage.test.tsx
bun run --filter @trading25/web typecheck
```

Expected: focused tests PASS and typecheck exits 0.

- [ ] **Step 6: Commit Task 6**

```bash
git add apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts apps/ts/packages/web/src/hooks/useShikihoSnapshot.test.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx
git commit -m "feat(web): preserve progressive Shikiho state"
```

---

### Task 7: Render progressive content and actionable diagnostics

**Files:**
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.tsx`
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.test.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`

**Interfaces:**
- Consumes: `displaySnapshot`, `candidate`, `trace`, canonical capture state, and refresh state.
- Produces: live phase badge, progressive coverage label, and accessible expandable diagnostics.

- [ ] **Step 1: Add failing panel/diagnostics tests**

Cover:

- candidate-only fields render before terminal snapshot;
- stable fallback fields remain during refresh;
- label `更新中（新規 2項目）` appears without claiming capture completion;
- active phase displays `DOM確認 6.2秒`;
- diagnostics disclose receiver attempts/wait, navigation milestones, mutation versus meaningful-change counts, first-seen field times, extraction totals/max, and terminal reason;
- null timings display `—` rather than fabricated zero;
- disclosure button has `aria-expanded`/`aria-controls`;
- candidate quote is not marked as chart provenance.

```tsx
expect(screen.getByText('Receiver待ち')).toBeInTheDocument();
expect(screen.getByText('1.8秒（3回）')).toBeInTheDocument();
expect(screen.getByText('DOM更新 384 / 有効変化 3')).toBeInTheDocument();
```

- [ ] **Step 2: Run component tests and verify RED**

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx
```

Expected: FAIL because diagnostics/progressive props do not exist.

- [ ] **Step 3: Implement compact diagnostics UI**

Use a focused component with pure formatting helpers. Keep the panel's current compact header; put details behind a button labelled `取得診断`. Do not add a chart or animation. Map phases to Japanese labels without hiding the raw bounded phase value in a `data-phase` attribute for tests.

- [ ] **Step 4: Compose the progressive panel**

Render `displaySnapshot` through the existing `SnapshotBody`. Keep canonical edition/status timestamps distinct from active trace timestamps. When stable and candidate values are mixed, show one muted explanatory label rather than per-field badges.

- [ ] **Step 5: Run focused web tests, typecheck, and Biome**

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/hooks/useShikihoSnapshot.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.tsx packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx
```

Expected: tests PASS; typecheck and Biome exit 0.

- [ ] **Step 6: Commit Task 7**

```bash
git add apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.test.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx
git commit -m "feat(web): show Shikiho capture diagnostics"
```

---

### Task 8: Chrome documentation, end-to-end regression, and acceptance handoff

**Files:**
- Modify: `apps/ts/extensions/shikiho/README.md`
- Modify: `docs/superpowers/plans/2026-07-14-symbol-workbench-shikiho-progressive-capture-diagnostics.md`
- Test: all extension and web files changed by Tasks 1-7.

**Interfaces:**
- Consumes: completed feature and all verification commands.
- Produces: Chrome-only operator instructions and verified repository state.

- [ ] **Step 1: Update README for Chrome only**

Document:

- `bun run --filter @trading25/shikiho-extension build`;
- load/reload `apps/ts/extensions/shikiho/dist` in `chrome://extensions`;
- reproduce a forced refresh in Symbol Workbench;
- read `取得診断` without opening DevTools;
- interpret receiver, navigation, DOM field, unrelated mutation, extraction, and terminal-reason measurements;
- confirm progress is local-only and no cookie/network/backend access exists.

Remove active Atlas wording. Keep historical design records unchanged unless they are presented as current operator instructions.

- [ ] **Step 2: Run complete extension gates**

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bunx biome check extensions/shikiho
```

Expected: all tests PASS; typecheck, build, and Biome exit 0.

- [ ] **Step 3: Run complete web/workspace gates**

```bash
cd apps/ts
bun run --filter @trading25/web test
bun run quality:typecheck
bun run workspace:build
bun run quality:deps:audit
```

Expected: all tests PASS and every command exits 0.

- [ ] **Step 4: Verify privacy and diff scope**

```bash
git diff --check
git diff -- apps/ts/extensions/shikiho/manifest.json
rg -n "cookies|host_permissions|fetch\(|XMLHttpRequest|Atlas" apps/ts/extensions/shikiho apps/ts/packages/web/src/components/SymbolWorkbench apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts
git status --short
```

Expected: manifest still has only `storage` and `alarms`; no Shikiho network/cookie/backend addition; no active Atlas operator wording; clean diff check.

- [ ] **Step 5: Perform manual Chrome acceptance**

In Chrome:

1. Reload the unpacked extension from `chrome://extensions`.
2. Open a Symbol Workbench code with no fresh Shikiho snapshot.
3. Confirm the panel shows fields before terminal completion when they appear in the Shikiho tab.
4. Open `取得診断` and record the dominant phase for one slow first capture.
5. Confirm a forced refresh preserves prior content until new fields arrive.
6. Confirm changing symbols rejects stale progress.
7. Confirm no tab reload occurs and a user-opened Shikiho tab is not navigated or closed.

Expected: the trace identifies one dominant phase and progressive content remains attempt/code scoped.

- [ ] **Step 6: Mark only verified plan boxes and commit Task 8**

```bash
git add apps/ts/extensions/shikiho/README.md docs/superpowers/plans/2026-07-14-symbol-workbench-shikiho-progressive-capture-diagnostics.md
git commit -m "docs(shikiho): explain Chrome capture diagnostics"
```

Do not mark manual Chrome acceptance complete unless it was actually performed against the rebuilt unpacked extension.
