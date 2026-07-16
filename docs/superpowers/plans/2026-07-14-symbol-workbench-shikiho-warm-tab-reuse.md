# Symbol Workbench Shikiho Warm Tab Reuse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Shikiho refresh prefer a non-destructive exact-code tab and otherwise reuse one generation-safe extension-owned inactive tab for three idle minutes.

**Architecture:** A content-script tab bridge provides exact-code probing and request-ID-bound extraction. A dedicated warm-tab lease manager owns `chrome.storage.session`, navigation, alarm cleanup, and ownership transfer; a tab acquisition service composes exact-tab discovery with the lease. The existing background coordinator retains freshness, retry suppression, FIFO, and singleflight while delegating each uncached capture to that service.

**Tech Stack:** TypeScript 6, Bun test/build, Chrome Manifest V3 `tabs`, `storage.session`, and `alarms` APIs, existing rendered-DOM extractor.

## Global Constraints

- Resolution order is fresh cache -> exact-code rendered tab -> warm extension-owned tab -> new extension-owned tab.
- Article TTL remains `24 * 60 * 60 * 1000`; current-day quote TTL remains `15 * 60 * 1000`; retry suppression remains `60 * 1000`.
- Duplicate same-code requests singleflight; uncached different-code captures remain FIFO; a fresh other-code cache hit does not wait for the queue.
- Exact user tabs are never navigated, reloaded, activated, pinned, grouped, or closed.
- One owned tab may stay idle for `3 * 60 * 1000` and has a `5 * 60 * 1000` maximum-age target.
- Explicit captures use `crypto.randomUUID()` request IDs; stale request IDs, codes, generations, tabs, and alarms are ignored.
- Only `storage` and `alarms` permissions are declared. Do not add `tabs`, host, `activeTab`, `scripting`, cookie, request, history, download, or native-messaging permissions.
- Do not query tab URLs, automate clicks, issue Shikiho fetch/XHR, store raw HTML or credentials, add telemetry, or change backend/OpenAPI/data-plane code.
- Preserve passive DOM capture, latest-200 storage, selected-symbol checks, previous-snapshot-on-failure behavior, and all existing diagnostics.
- Use the current 25-second outer capture timeout; update the superseded 15-second documentation statement.

---

### Task 1: Add a request-ID-bound Shikiho tab bridge

**Files:**
- Create: `apps/ts/extensions/shikiho/src/shikiho-tab-bridge.ts`
- Create: `apps/ts/extensions/shikiho/src/shikiho-tab-bridge.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/shikiho-content.ts`

**Interfaces:**
- Consumes: `normalizeShikihoCode(value)`, `ShikihoExtractionResult`, and the existing `extractShikihoPage` call in `shikiho-content.ts`.
- Produces: `startShikihoTabBridge(options): () => void`, `ProbeShikihoCodeResponse`, and `CaptureNowResponse` for Task 3.

- [x] **Step 1: Write failing tests for exact-code probing**

Create a dependency-injected bridge test with these message shapes:

```ts
export type ShikihoTabRequest =
  | { type: 'probe_shikiho_code' }
  | { type: 'capture_now'; requestId: string; code: string; waitForReady: boolean };

export type ProbeShikihoCodeResponse = {
  type: 'shikiho_code';
  code: string | null;
};

export type CaptureNowResponse = {
  type: 'capture_result';
  requestId: string;
  code: string;
  result: ShikihoExtractionResult;
};
```

Test that `probe_shikiho_code` derives `7203` from `getCode()` and returns no URL, title, DOM, or snapshot fields. Test invalid/non-stock paths as `code: null`.

- [x] **Step 2: Write failing tests for direct and ready-wait capture**

Use injected `capture`, `waitUntilReady`, `addMessageListener`, and `removeMessageListener` functions. Cover:

```ts
expect(await request({
  type: 'capture_now',
  requestId: 'job-1',
  code: '7203',
  waitForReady: false,
})).toEqual({
  type: 'capture_result',
  requestId: 'job-1',
  code: '7203',
  result: success7203,
});
expect(waitUntilReady).not.toHaveBeenCalled();
```

For `waitForReady: true`, assert `waitUntilReady()` runs before `capture()`. Change `getCode()` between the initial check and extraction and assert the bridge rejects with `undefined` instead of returning another symbol. Also reject empty/oversized request IDs, extra keys, non-normalized codes, and malformed requests.

- [x] **Step 3: Run the test and prove RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/shikiho-tab-bridge.test.ts
```

Expected: FAIL because `shikiho-tab-bridge.ts` does not exist.

- [x] **Step 4: Implement the strict tab bridge**

Implement:

```ts
export interface ShikihoTabBridgeOptions {
  getCode(): string | null;
  capture(): ShikihoExtractionResult | Promise<ShikihoExtractionResult>;
  waitUntilReady(): Promise<void>;
  addMessageListener(listener: RuntimeMessageListener): void;
  removeMessageListener(listener: RuntimeMessageListener): void;
}

export function startShikihoTabBridge(options: ShikihoTabBridgeOptions): () => void;
```

Use exact-key validation. Check `normalizeShikihoCode(request.code) === request.code`, compare `getCode()` before waiting and again after extraction, and echo only a validated request ID/code/result. Return `true` from the Chrome listener only for the asynchronous capture request.

- [x] **Step 5: Wire the bridge without changing passive capture**

Refactor `shikiho-content.ts` so extraction returns `ShikihoExtractionResult`, while a separate `publishPassiveResult(result)` preserves the current `capture_success` / `capture_diagnostic` messages. Start the existing `createCaptureController` unchanged for passive capture. Start the new bridge with:

```ts
startShikihoTabBridge({
  getCode: currentCode,
  capture: extractCurrentPage,
  waitUntilReady: () => waitForDomQuiet(document, 500, 10_000),
  addMessageListener: (listener) => chrome.runtime.onMessage.addListener(listener),
  removeMessageListener: (listener) => chrome.runtime.onMessage.removeListener(listener),
});
```

Keep `waitForDomQuiet` private to `shikiho-content.ts` unless the test requires injection; it must resolve after 500 milliseconds without mutations or after 10 seconds maximum and always disconnect its observer and clear both timers.

- [x] **Step 6: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/shikiho-tab-bridge.test.ts extensions/shikiho/src/capture-controller.test.ts extensions/shikiho/src/extractor.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: all focused tests and typecheck pass.

Commit:

```bash
git add apps/ts/extensions/shikiho/src/shikiho-tab-bridge.ts \
  apps/ts/extensions/shikiho/src/shikiho-tab-bridge.test.ts \
  apps/ts/extensions/shikiho/src/shikiho-content.ts
git commit -m "feat(shikiho): add generation-bound tab capture"
```

---

### Task 2: Implement the alarm-backed warm-tab lease manager

**Files:**
- Create: `apps/ts/extensions/shikiho/src/warm-tab-lease.ts`
- Create: `apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts`

**Interfaces:**
- Consumes: canonical four-digit Shikiho codes and injected Chrome tab/session/alarm operations.
- Produces: `createWarmTabLeaseManager(deps)`, `WarmTabHandle`, and `WarmTabMode` for Task 3.

- [x] **Step 1: Write failing creation and reuse tests**

Define the public records:

```ts
export const SHIKIHO_WARM_TAB_IDLE_MS = 3 * 60 * 1000;
export const SHIKIHO_WARM_TAB_MAX_AGE_MS = 5 * 60 * 1000;

export type WarmTabMode = 'warm_owned_same_code' | 'warm_owned_navigation' | 'new_owned_tab';

export interface ShikihoWarmTabLeaseV1 {
  version: 1;
  tabId: number;
  ownerToken: string;
  generation: number;
  phase: 'capturing' | 'idle';
  code: string | null;
  createdAt: number;
  idleDeadline: number | null;
}

export interface WarmTabHandle {
  lease: ShikihoWarmTabLeaseV1;
  mode: WarmTabMode;
}
```

Test first acquisition creates `chrome.tabs.create({active:false,url:canonical})`, stores a `capturing` lease, and returns `new_owned_tab`. After `releaseSuccess`, same-code acquisition must create/navigate zero tabs and return `warm_owned_same_code`; different-code acquisition must call `tabs.update(tabId,{active:false,url:canonical})`, increment generation, and return `warm_owned_navigation`.

- [x] **Step 2: Write failing cleanup and ownership tests**

Cover these exact behaviors:

- success/partial -> idle lease plus one cleanup alarm;
- login/page-changed/timeout/storage/invalid response -> `releaseFailure(handle)` closes and clears;
- idle alarm closes only when tab ID, owner token, generation, phase, and deadline match;
- an old alarm after reuse closes nothing;
- maximum age reached during capture closes immediately after `releaseSuccess`;
- `onActivated(tabId)` abandons ownership, clears alarm/metadata, and does not close;
- `abandonIfOwned(tabId)` performs the same abandonment after runtime probing proves the owned tab no longer hosts a Shikiho stock content script;
- `onRemoved(tabId)` clears matching metadata;
- no session lease or malformed lease never closes a tab.

- [x] **Step 3: Write failing MV3 reconciliation tests**

Create a new manager instance against the same fake `storage.session` to simulate worker restart. Assert:

```ts
await restarted.reconcile(); // valid idle lease remains reusable
await expired.reconcile();   // expired idle lease closes
await stale.reconcile();     // capturing lease with no active in-memory handle closes
```

Also cover missing tabs and delayed alarms. `tabs.get` failure removes metadata without a second close attempt.

- [x] **Step 4: Run the suite and prove RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/warm-tab-lease.test.ts
```

Expected: FAIL because `warm-tab-lease.ts` does not exist.

- [x] **Step 5: Implement the lease state machine**

Implement this surface:

```ts
export interface WarmTabLeaseManager {
  reconcile(): Promise<void>;
  acquire(code: string): Promise<WarmTabHandle>;
  releaseSuccess(handle: WarmTabHandle, code: string): Promise<void>;
  releaseFailure(handle: WarmTabHandle): Promise<void>;
  onAlarm(name: string): Promise<void>;
  onActivated(tabId: number): Promise<void>;
  abandonIfOwned(tabId: number): Promise<void>;
  onRemoved(tabId: number): Promise<void>;
}
```

Persist under one versioned `chrome.storage.session` key. Track active owner-token/generation pairs in memory so `reconcile()` distinguishes a live capture from a stale restarted-worker capture. Encode owner token and generation into the alarm name, but reload and compare the session record before every close/navigation. Cleanup must be idempotent and must remove matching metadata even when `tabs.remove` rejects because the user already closed the tab.

- [x] **Step 6: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/warm-tab-lease.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: lease tests and typecheck pass.

Commit:

```bash
git add apps/ts/extensions/shikiho/src/warm-tab-lease.ts \
  apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts
git commit -m "feat(shikiho): manage temporary warm capture tab"
```

---

### Task 3: Compose exact-tab discovery with warm-tab acquisition

**Files:**
- Create: `apps/ts/extensions/shikiho/src/tab-acquisition.ts`
- Create: `apps/ts/extensions/shikiho/src/tab-acquisition.test.ts`

**Interfaces:**
- Consumes: `WarmTabLeaseManager.acquire/releaseSuccess/releaseFailure`, Task 1 tab request/response types, repository save callbacks, and injected monotonic time.
- Produces: `createShikihoTabAcquisition(deps).capture(code)` and `ShikihoCaptureTiming` for Task 4.

- [x] **Step 1: Write failing exact-tab tests**

Define:

```ts
export type ShikihoCaptureMode =
  | 'exact_user_tab'
  | 'warm_owned_same_code'
  | 'warm_owned_navigation'
  | 'new_owned_tab';

export interface ShikihoCaptureTiming {
  event: 'shikiho_capture_timing';
  mode: ShikihoCaptureMode;
  outcome: 'success' | 'partial' | 'diagnostic' | 'timeout' | 'error';
  probeMs: number;
  navigationMs: number;
  captureMs: number;
  totalMs: number;
}

export interface AcquiredShikihoResult {
  result: ShikihoExtractionResult;
  timing: ShikihoCaptureTiming;
}
```

Mock `queryTabs()` to return IDs only and `sendTabMessage()` to respond to `probe_shikiho_code`. Assert an exact-code tab receives `capture_now` with `waitForReady:false`, returns `exact_user_tab`, and never calls warm acquire/release or tab create/update/remove. A validated diagnostic also must not close that tab.

- [x] **Step 2: Write failing fallback and race tests**

Cover wrong code, no receiver, 500-millisecond probe timeout, tab code changing before capture, malformed direct response, mismatched request ID, wrong response code, and a response from an unselected tab. Each non-diagnostic exact-tab failure falls back to the lease manager.

For owned acquisition, assert `waitForReady:true`. Simulate A -> B -> A responses and prove only the current request ID resolves. Multiple exact matches must select the extension-owned exact tab first when its ID matches a valid lease; otherwise select the lowest tab ID.

- [x] **Step 3: Write failing timing tests**

Inject `now()` and assert all four modes populate nonnegative `probeMs`, `navigationMs`, `captureMs`, and `totalMs`. The logger receives only the timing object above; assert serialized logs contain no snapshot fields, quote values, source URL, article text, or code.

- [x] **Step 4: Run the suite and prove RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/tab-acquisition.test.ts
```

Expected: FAIL because `tab-acquisition.ts` does not exist.

- [x] **Step 5: Implement acquisition and strict response parsing**

Implement:

```ts
export interface ShikihoTabAcquisition {
  capture(code: string): Promise<AcquiredShikihoResult>;
}

export function createShikihoTabAcquisition(deps: ShikihoTabAcquisitionDeps): ShikihoTabAcquisition;
```

Use `Promise.allSettled` plus an injected 500-millisecond timeout to probe tabs without URL filters. Validate direct responses with exact keys and existing contract parsers before returning. Wrap owned capture in `try/finally`: `success` including partial snapshot calls `releaseSuccess`; diagnostics and thrown/invalid/timeout outcomes call `releaseFailure`. Emit exactly one local timing event for each non-cache acquisition.

Wrap each explicit `capture_now` request in the existing `SHIKIHO_CAPTURE_TIMEOUT_MS = 25 * 1000` outer timeout. Timeout is an acquisition error for cleanup and becomes the existing `page_changed` diagnostic at the coordinator boundary so retry suppression remains unchanged.

- [x] **Step 6: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/tab-acquisition.test.ts extensions/shikiho/src/warm-tab-lease.test.ts extensions/shikiho/src/shikiho-tab-bridge.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: all acquisition/lease/bridge tests and typecheck pass.

Commit:

```bash
git add apps/ts/extensions/shikiho/src/tab-acquisition.ts \
  apps/ts/extensions/shikiho/src/tab-acquisition.test.ts
git commit -m "feat(shikiho): prefer exact and warm tabs"
```

---

### Task 4: Refactor the background coordinator onto direct acquisition

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/background-capture.ts`
- Modify: `apps/ts/extensions/shikiho/src/background-capture.test.ts`
- Create: `apps/ts/extensions/shikiho/src/background-runtime.ts`
- Create: `apps/ts/extensions/shikiho/src/background-runtime.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/background.ts`
- Modify: `apps/ts/extensions/shikiho/manifest.json`

**Interfaces:**
- Consumes: `ShikihoTabAcquisition.capture(code)`, repository `get/saveSnapshot/saveDiagnostic`, and `WarmTabLeaseManager` lifecycle handlers.
- Produces: unchanged public `resolvePublicShikihoState` result for the localhost bridge; passive `capture_success`/`capture_diagnostic` storage remains accepted.

- [x] **Step 1: Replace lifecycle tests with direct-acquisition tests**

Keep every freshness test unchanged. Change coordinator dependencies from `createTab/closeTab/setTimer/clearTimer` to:

```ts
capture(code: string): Promise<AcquiredShikihoResult>;
```

Assert stale/manual requests call `capture(code)` once, save its success or diagnostic, then return `get(code)`. Preserve tests for same-code promise identity, different-code FIFO, fresh other-code bypass, retry suppression, prior snapshot retention, and storage errors. Keep passive `acceptSnapshot`/`acceptDiagnostic` tests but assert they only save and never complete an explicit direct-capture promise.

- [x] **Step 2: Run coordinator tests and prove RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/background-capture.test.ts
```

Expected: FAIL because the coordinator still owns generated-tab pending state.

- [x] **Step 3: Simplify the coordinator**

Retain cache helpers, FIFO tail, and `Map<string, Promise<StoredShikihoState>>`. Replace the old pending-tab capture function with:

```ts
async function capture(code: string): Promise<StoredShikihoState> {
  const acquired = await deps.capture(code);
  if (acquired.result.kind === 'success') await deps.saveSnapshot(acquired.result.snapshot);
  else {
    await deps.saveDiagnostic({
      schemaVersion: 1,
      code,
      observedAt: new Date(deps.now()).toISOString(),
      status: acquired.result.kind,
    });
  }
  return deps.get(code);
}
```

Remove coordinator-owned tab IDs, pending resolvers, removed-tab bookkeeping, and timers. Keep `acceptSnapshot`/`acceptDiagnostic` as passive-storage methods used by existing content messages.

- [x] **Step 4: Write failing service-worker lifecycle tests**

Create `background-runtime.test.ts` around an injected listener registry. Assert module initialization and `runtime.onStartup` reconcile the lease; alarms delegate by name; activation/removal delegate by tab ID; and a matching owned-tab `onUpdated` event verifies ownership by sending `probe_shikiho_code` rather than reading URL fields. If the probe no longer reaches a Shikiho content script or returns `code:null`, assert ownership is abandoned without closing the tab.

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/background-runtime.test.ts
```

Expected: FAIL because `background-runtime.ts` does not exist.

- [x] **Step 5: Wire Chrome dependencies and lifecycle listeners**

Implement `startShikihoBackgroundRuntime(deps): () => void` in `background-runtime.ts` so listener registration and cleanup are testable. In `background.ts`, construct the lease manager with `chrome.storage.session`, `chrome.tabs.create/get/update/remove/query/sendMessage`, and `chrome.alarms.create/clear`. Construct acquisition and inject it into the coordinator, then start the runtime.

Register:

```ts
chrome.alarms.onAlarm.addListener((alarm) => void leaseManager.onAlarm(alarm.name));
chrome.tabs.onActivated.addListener(({ tabId }) => void leaseManager.onActivated(tabId));
chrome.tabs.onRemoved.addListener((tabId) => void leaseManager.onRemoved(tabId));
chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (changeInfo.status === 'complete') void runtime.verifyOwnedTab(tabId);
});
chrome.runtime.onStartup.addListener(() => void leaseManager.reconcile());
void leaseManager.reconcile();
```

`verifyOwnedTab` first checks whether `tabId` matches the lease, then sends `probe_shikiho_code`; a missing receiver or `code:null` abandons ownership without closing. Do not inspect `tab.url`, `changeInfo.url`, title, or favicon. Log timings with `console.debug(timing)` and no payload/code.

- [x] **Step 6: Add only the alarms permission**

Change the manifest line to:

```json
"permissions": ["storage", "alarms"]
```

Do not add `tabs` or `host_permissions`.

- [x] **Step 7: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/background-capture.test.ts extensions/shikiho/src/tab-acquisition.test.ts extensions/shikiho/src/warm-tab-lease.test.ts extensions/shikiho/src/shikiho-tab-bridge.test.ts
bun test extensions/shikiho/src/background-runtime.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
```

Expected: focused tests, typecheck, and extension build pass; `dist/manifest.json` contains only `storage` and `alarms` permissions.

Commit:

```bash
git add apps/ts/extensions/shikiho/src/background-capture.ts \
  apps/ts/extensions/shikiho/src/background-capture.test.ts \
  apps/ts/extensions/shikiho/src/background-runtime.ts \
  apps/ts/extensions/shikiho/src/background-runtime.test.ts \
  apps/ts/extensions/shikiho/src/background.ts \
  apps/ts/extensions/shikiho/manifest.json
git commit -m "refactor(shikiho): acquire captures through reusable tabs"
```

---

### Task 5: Document, validate, and manually prove the tab lifecycle

**Files:**
- Modify: `apps/ts/extensions/shikiho/README.md`
- Modify: `docs/superpowers/specs/2026-07-12-symbol-workbench-shikiho-background-refresh-design.md`
- Modify: `docs/superpowers/plans/2026-07-14-symbol-workbench-shikiho-warm-tab-reuse.md` (check completed steps only during execution)

**Interfaces:**
- Consumes: completed Tasks 1-4 and the Atlas unpacked extension workflow.
- Produces: accurate operator documentation, full automated evidence, and a live exact/warm/new-tab acceptance record.

- [x] **Step 1: Update user-facing lifecycle and privacy documentation**

Replace the README immediate-close statement with:

```text
同じ銘柄を表示済みの四季報タブがある場合は、そのDOMを再取得します。そのタブを遷移・再読み込み・閉じることはありません。表示済みタブが無い場合、拡張機能が作成したinactive tabを取得成功後3分間だけ再利用し、生成から5分を上限として閉じます。ユーザーがそのtabを開いた場合は以後ユーザー所有として扱い、自動遷移・自動終了しません。
```

State that permissions are `storage` and `alarms`, with `alarms` used only to end the temporary owned tab. Preserve the no-cookie/no-fetch/no-click/no-raw-HTML/no-telemetry statements.

Add a supersession note to the 2026-07-12 design pointing to the approved 2026-07-14 design for tab lifecycle and the 25-second timeout.

- [x] **Step 2: Run the complete extension gates**

Run:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bun run quality:deps:audit
bunx biome check extensions/shikiho
```

Expected: all commands exit 0. Confirm:

```bash
rg -n '"permissions"|cookies|host_permissions|"tabs"|fetch\(|XMLHttpRequest|dangerouslySetInnerHTML' \
  extensions/shikiho/manifest.json extensions/shikiho/src extensions/shikiho/README.md
```

Expected: manifest shows only `storage`/`alarms`; no prohibited permission or Shikiho network implementation is introduced.

- [x] **Step 3: Run the relevant workspace gates**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test
bun run quality:typecheck
bun run workspace:build
```

Expected: web tests, all TypeScript typechecks/audits, and production build pass. No OpenAPI sync is needed because the backend contract is unchanged.

- [x] **Step 4: Commit documentation and final automated fixes**

```bash
git add apps/ts/extensions/shikiho/README.md \
  docs/superpowers/specs/2026-07-12-symbol-workbench-shikiho-background-refresh-design.md \
  docs/superpowers/plans/2026-07-14-symbol-workbench-shikiho-warm-tab-reuse.md
git commit -m "docs(shikiho): explain temporary warm tab reuse"
```

- [ ] **Step 5: Rebuild and reload the unpacked extension**

Run `cd apps/ts && bun run --filter @trading25/shikiho-extension build`, then reload `apps/ts/extensions/shikiho/dist` from Atlas extension management. Reload both Trading25 and any already-open Shikiho tabs so the new content listener is present.

- [ ] **Step 6: Perform live acceptance in Atlas**

With an authenticated Atlas profile and Symbol Workbench:

1. Open a Shikiho `7203` tab, then force-refresh `7203` in Workbench. Confirm no new tab appears and the service-worker log reports `exact_user_tab`.
2. Close user Shikiho tabs, force-refresh another code, and confirm one inactive owned tab appears with `new_owned_tab`.
3. Within three minutes force-refresh a different code and confirm the same tab ID is reused with `warm_owned_navigation`.
4. Activate that warm tab, then request another code. Confirm the activated tab is not navigated or closed and a new owned tab is used.
5. Leave an owned tab inactive and confirm alarm cleanup after three idle minutes, allowing normal platform scheduling delay.
6. Inspect timing logs and record exact/new/warm total durations without copying captured content.

Expected: all six behaviors match the design; the Workbench continues showing only the selected code and preserves the previous snapshot on a failed refresh.

- [ ] **Step 7: Final repository check**

Run:

```bash
git status --short
git log -6 --oneline
```

Expected: only intentional plan checkbox changes, if any, remain; implementation commits are scoped to Shikiho extension/docs.
