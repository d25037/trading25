# Symbol Workbench Shikiho Owned-Tab Reload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover an unusually slow first Shikiho render by reloading the exact extension-owned tab once at 7 seconds and retrying only until the original 25-second acquisition deadline.

**Architecture:** `WarmTabLeaseManager` exclusively owns the guarded `chrome.tabs.reload` boundary. The background runtime delegates completed-navigation reconciliation to that manager so a live `capturing` lease survives the temporary receiver gap after reload. Owned acquisition races its first request against a 7-second milestone, invalidates the old attempt, reloads once, and starts a fresh request under one absolute 25-second deadline.

**Tech Stack:** TypeScript 6, Chrome Manifest V3 extension APIs, Bun test, Biome.

## Global Constraints

- Reload only a current extension-owned `capturing` lease; never reload an exact user tab or a user-adopted tab.
- Trigger recovery once at 7 seconds and keep the original hard 25-second total deadline; reload latency consumes the remaining 18 seconds.
- Keep receiver-not-ready retries at 100 milliseconds and never retry general errors or malformed responses.
- Do not add cookie access, Shikiho fetch/XHR, page clicks, raw HTML storage, telemetry, backend APIs, or OpenAPI changes.
- Preserve exact-match lease cleanup so activation, removal, and stale generations cannot close or resurrect user-owned tabs.

---

### Task 1: Add an ownership-guarded reload operation

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/warm-tab-lease.ts`
- Test: `apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts`

**Interfaces:**
- Consumes: `WarmTabHandle`, persisted `ShikihoWarmTabLeaseV1`, and the manager's `activeCaptures` identity set.
- Produces: `WarmTabLeaseDeps.tabs.reload(tabId: number): Promise<void>` and `WarmTabLeaseManager.reloadOwned(handle: WarmTabHandle): Promise<void>`.

- [ ] **Step 1: Write failing reload-ownership tests**

Extend the existing harness with a `reloads: number[]` collection and a `tabs.reload` fake:

```ts
const reloads: number[] = [];

reload: async (tabId) => {
  if (!tabs.has(tabId)) throw new Error('missing tab');
  reloads.push(tabId);
},
```

Return `reloads` from `harness()`. Add tests that express the wished-for API and cover the exact identity boundary:

```ts
test('reloads only the exact active capturing handle', async () => {
  const h = harness();
  const handle = await h.manager.acquire('7203');

  await h.manager.reloadOwned(handle);

  expect(h.reloads).toEqual([handle.lease.tabId]);
});

test('refuses reload after generation replacement', async () => {
  const h = harness();
  const handle = await h.manager.acquire('7203');
  h.session.set(SHIKIHO_WARM_TAB_LEASE_KEY, { ...handle.lease, generation: 2 });

  await expect(h.manager.reloadOwned(handle)).rejects.toThrow('no longer owned');
  expect(h.reloads).toEqual([]);
});

test('refuses reload after activation or removal', async () => {
  for (const event of ['activated', 'removed'] as const) {
    const h = harness();
    const handle = await h.manager.acquire('7203');
    if (event === 'activated') await h.manager.onActivated(handle.lease.tabId);
    else await h.manager.onRemoved(handle.lease.tabId);

    await expect(h.manager.reloadOwned(handle)).rejects.toThrow('no longer owned');
    expect(h.reloads).toEqual([]);
  }
});

test('refuses reload after the lease becomes idle', async () => {
  const h = harness();
  const handle = await h.manager.acquire('7203');
  await h.manager.releaseSuccess(handle, '7203');

  await expect(h.manager.reloadOwned(handle)).rejects.toThrow('no longer owned');
  expect(h.reloads).toEqual([]);
});
```

Use the existing harness session and activation/removal methods. Also assert that changing only the owner token produces the same rejection as generation replacement, so a stale handle cannot reload a replacement lease for the same tab ID.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/warm-tab-lease.test.ts
```

Expected: FAIL because `tabs.reload` and `manager.reloadOwned` do not exist.

- [ ] **Step 3: Implement the guarded operation**

Extend the public dependency and manager interfaces:

```ts
tabs: {
  create(properties: { active: false; url: string }): Promise<{ id?: number }>;
  update(tabId: number, properties: { active: false; url: string }): Promise<unknown>;
  reload(tabId: number): Promise<void>;
  remove(tabId: number): Promise<void>;
  get(tabId: number): Promise<unknown>;
};

reloadOwned(handle: WarmTabHandle): Promise<void>;
```

Implement exact validation before the Chrome call:

```ts
async function reloadOwned(handle: WarmTabHandle): Promise<void> {
  const epoch = adoptionEpoch(handle.lease.tabId);
  const current = await readLease();
  if (
    current === null ||
    !sameLease(current, handle.lease) ||
    current.phase !== 'capturing' ||
    !activeCaptures.has(activeIdentity(handle.lease)) ||
    adoptionEpoch(current.tabId) !== epoch
  ) {
    throw new Error('Shikiho warm tab is no longer owned');
  }
  await deps.tabs.get(current.tabId);
  if (adoptionEpoch(current.tabId) !== epoch) {
    throw new Error('Shikiho warm tab is no longer owned');
  }
  return deps.tabs.reload(current.tabId);
}
```

The final epoch check and `deps.tabs.reload` invocation must have no `await` between them, so an activation callback cannot interleave after validation but before the Chrome call. Do not recreate metadata or catch-and-ignore reload errors. Add a test that defers `tabs.get`, calls `onActivated` while it is pending, then proves `tabs.reload` was not invoked.

- [ ] **Step 4: Run focused tests and quality checks**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/warm-tab-lease.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bunx biome check extensions/shikiho/src/warm-tab-lease.ts extensions/shikiho/src/warm-tab-lease.test.ts
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit Task 1**

```bash
git add apps/ts/extensions/shikiho/src/warm-tab-lease.ts apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts
git commit -m "feat(shikiho): guard owned tab reload"
```

---

### Task 2: Preserve capturing ownership across reload completion

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/warm-tab-lease.ts`
- Modify: `apps/ts/extensions/shikiho/src/background-runtime.ts`
- Modify: `apps/ts/extensions/shikiho/src/background-runtime.test.ts`
- Test: `apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts`

**Interfaces:**
- Consumes: the current persisted lease phase and `hasShikihoStockContentScript` dependency.
- Produces: `WarmTabLeaseManager.onUpdatedComplete(tabId: number): Promise<void>`. `ShikihoBackgroundRuntimeDeps` no longer needs `sendTabMessage` for navigation reconciliation.

- [ ] **Step 1: Write failing phase-aware reconciliation tests**

Add manager tests:

```ts
test('keeps a live capturing lease when the receiver is missing after reload', async () => {
  const h = harness({ hasShikihoStockContentScript: async () => false });
  const handle = await h.manager.acquire('7203');

  await h.manager.onUpdatedComplete(handle.lease.tabId);

  expect(storedLease(h.session)).toEqual(handle.lease);
  expect(h.tabs.remove).not.toHaveBeenCalled();
});

test('abandons an idle lease that is no longer hosted', async () => {
  const h = harness({ hasShikihoStockContentScript: async () => false });
  const handle = await h.manager.acquire('7203');
  await h.manager.releaseSuccess(handle, '7203');

  await h.manager.onUpdatedComplete(handle.lease.tabId);

  expect(storedLease(h.session)).toBeUndefined();
  expect(h.tabs.remove).not.toHaveBeenCalled();
});
```

Update runtime tests to expect `tabs.onUpdated(status='complete')` to call `leaseManager.onUpdatedComplete(tabId)` and to perform no direct probe.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/warm-tab-lease.test.ts extensions/shikiho/src/background-runtime.test.ts
```

Expected: FAIL because `onUpdatedComplete` is missing and runtime still probes directly.

- [ ] **Step 3: Move reconciliation behind the manager**

Add the manager method:

```ts
async function onUpdatedComplete(tabId: number): Promise<void> {
  const lease = await readLease();
  if (lease?.tabId !== tabId || lease.phase === 'capturing') return;
  await abandonIfOwned(tabId);
}
```

In `background-runtime.ts`, remove `normalizeShikihoCode`, `ShikihoTabRequest`, `TabMessageReply`, `sendTabMessage`, `hasShikihoCode`, and `verifyOwnedTab`. Delegate only completed events:

```ts
const updatedListener: UpdatedListener = (tabId, changeInfo) => {
  if (changeInfo.status === 'complete') run(deps.leaseManager.onUpdatedComplete(tabId));
};
```

Keep activation, removal, alarms, and startup behavior unchanged.

- [ ] **Step 4: Run focused tests and quality checks**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/warm-tab-lease.test.ts extensions/shikiho/src/background-runtime.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bunx biome check extensions/shikiho/src/warm-tab-lease.ts extensions/shikiho/src/warm-tab-lease.test.ts extensions/shikiho/src/background-runtime.ts extensions/shikiho/src/background-runtime.test.ts
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit Task 2**

```bash
git add apps/ts/extensions/shikiho/src/warm-tab-lease.ts apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts apps/ts/extensions/shikiho/src/background-runtime.ts apps/ts/extensions/shikiho/src/background-runtime.test.ts
git commit -m "fix(shikiho): preserve capturing lease on reload"
```

---

### Task 3: Add the 7-second reload recovery phase

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/tab-acquisition.ts`
- Test: `apps/ts/extensions/shikiho/src/tab-acquisition.test.ts`

**Interfaces:**
- Consumes: `WarmTabLeaseManager.reloadOwned(handle)`, `WarmTabHandle`, injected `now()` and `delay(ms)`.
- Produces: exported `SHIKIHO_RELOAD_AFTER_MS = 7_000`; owned acquisition with one phase transition and the existing `SHIKIHO_CAPTURE_TIMEOUT_MS = 25_000` hard deadline.

- [ ] **Step 1: Write failing two-phase recovery tests**

Extend the acquisition harness with a default `reloadOwned = mock(async () => undefined)` method on its lease manager and return that mock. Add these local test helpers:

```ts
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function controlledDelays() {
  const pending = new Map<number, Array<() => void>>();
  return {
    delay: (ms: number) =>
      new Promise<void>((resolve) => {
        pending.set(ms, [...(pending.get(ms) ?? []), resolve]);
      }),
    resolve(ms: number) {
      const queue = pending.get(ms) ?? [];
      const next = queue.shift();
      if (next === undefined) throw new Error(`No pending ${ms}ms delay`);
      pending.set(ms, queue);
      next();
    },
  };
}

function ownedCaptureRequests(sendTabMessage: {
  mock: { calls: Array<[number, ShikihoTabRequest]> };
}): Array<Extract<ShikihoTabRequest, { type: 'capture_now' }>> {
  return sendTabMessage.mock.calls.flatMap(([, message]) =>
    message.type === 'capture_now' ? [message] : []
  );
}
```

Then add an explicit deferred-promise recovery test:

```ts
test('reloads once at seven seconds and accepts only the fresh second request', async () => {
  const first = deferred<TabMessageReply>();
  const second = deferred<TabMessageReply>();
  const timers = controlledDelays();
  let captureAttempt = 0;
  const h = harness({
    queryTabs: async () => [],
    delay: timers.delay,
    sendTabMessage: async (tabId, message) => {
      if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
      captureAttempt += 1;
      return captureAttempt === 1 ? first.promise : second.promise;
    },
  });

  const capture = h.acquisition.capture('7203');
  await new Promise((resolve) => setTimeout(resolve, 0));
  timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(h.reloadOwned).toHaveBeenCalledTimes(1);
  const requestIds = ownedCaptureRequests(h.sendTabMessage).map((request) => request.requestId);
  expect(requestIds).toHaveLength(2);
  expect(requestIds[0]).not.toBe(requestIds[1]);

  first.resolve(captureReply(99, requestIds[0], '7203', success('7203')));
  expect(h.releaseSuccess).not.toHaveBeenCalled();
  second.resolve(captureReply(99, requestIds[1], '7203', success('7203')));
  await expect(capture).resolves.toMatchObject({ result: { kind: 'success' } });
});
```

Add separate tests for:

- success, partial, diagnostic, general error, and malformed response before 7 seconds: zero reloads;
- reload rejection and ownership loss: terminal failure and one `releaseFailure`;
- reload latency consuming phase-two time;
- phase two receiver-missing retries every 100 milliseconds;
- timeout exactly at original 25 seconds with one reload and no third phase;
- exact user-tab timeout: zero reloads.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/tab-acquisition.test.ts
```

Expected: new recovery tests FAIL because no 7-second milestone or reload call exists.

- [ ] **Step 3: Implement one-shot recovery under one deadline**

Add constants and an internal cancellation guard:

```ts
export const SHIKIHO_RELOAD_AFTER_MS = 7 * 1000;

interface CaptureAttemptState {
  superseded: boolean;
}
```

Refactor the owned capture helper to accept an absolute deadline and attempt state. It must stop receiver retries after the first attempt is superseded:

```ts
async function captureUntilReady(
  tabId: number,
  code: string,
  deadline: number,
  state: CaptureAttemptState
): Promise<ShikihoExtractionResult> {
  while (!state.superseded) {
    if (deps.now() >= deadline) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    const result = await tryOwnedCapture(tabId, code);
    if (result !== null) return result;
    if (state.superseded) throw new SupersededCaptureError();
    await waitForReceiverRetry(deadline);
  }
  throw new SupersededCaptureError();
}
```

Change `captureOwnedTab` to accept the acquired handle and wrap the complete operation in one 25-second timeout:

```ts
async function captureOwnedTab(handle: WarmTabHandle, code: string): Promise<ShikihoExtractionResult> {
  const deadline = deps.now() + SHIKIHO_CAPTURE_TIMEOUT_MS;
  const execute = async () => {
    const firstState = { superseded: false };
    const first = captureUntilReady(handle.lease.tabId, code, deadline, firstState);
    const phase = await Promise.race([
      first.then((result) => ({ kind: 'result' as const, result })),
      deps.delay(SHIKIHO_RELOAD_AFTER_MS).then(() => ({ kind: 'reload' as const })),
    ]);
    if (phase.kind === 'result') return phase.result;

    firstState.superseded = true;
    await deps.leaseManager.reloadOwned(handle);
    return captureUntilReady(handle.lease.tabId, code, deadline, { superseded: false });
  };
  return withTimeout(execute(), SHIKIHO_CAPTURE_TIMEOUT_MS);
}
```

Attach the first promise to the race before it can be superseded so delayed resolution/rejection remains observed. Do not add a second 25-second timer after reload. Pass the full `handle` from `captureOwned`.

- [ ] **Step 4: Run focused tests and quality checks**

Run:

```bash
cd apps/ts
bun test extensions/shikiho/src/tab-acquisition.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bunx biome check extensions/shikiho/src/tab-acquisition.ts extensions/shikiho/src/tab-acquisition.test.ts
```

Expected: all commands exit 0; the focused output includes the 7-second, stale-response, exact-tab, and 25-second deadline cases.

- [ ] **Step 5: Commit Task 3**

```bash
git add apps/ts/extensions/shikiho/src/tab-acquisition.ts apps/ts/extensions/shikiho/src/tab-acquisition.test.ts
git commit -m "feat(shikiho): reload slow owned capture once"
```

---

### Task 4: Wire Chrome reload, document behavior, and run final gates

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/background.ts`
- Modify: `apps/ts/extensions/shikiho/README.md`
- Modify: `docs/superpowers/plans/2026-07-14-symbol-workbench-shikiho-owned-tab-reload.md`

**Interfaces:**
- Consumes: `WarmTabLeaseDeps.tabs.reload` and the phase-aware `ShikihoBackgroundRuntimeDeps` from Tasks 1-2.
- Produces: production `chrome.tabs.reload(tabId)` wiring, operator documentation, rebuilt `apps/ts/extensions/shikiho/dist`.

- [ ] **Step 1: Wire the Chrome API and remove obsolete runtime wiring**

Update `background.ts`:

```ts
tabs: {
  create: (properties) => chrome.tabs.create(properties),
  update: (tabId, properties) => chrome.tabs.update(tabId, properties),
  reload: (tabId) => chrome.tabs.reload(tabId),
  remove: (tabId) => chrome.tabs.remove(tabId),
  get: (tabId) => chrome.tabs.get(tabId),
},
```

Remove `sendTabMessage` from `startShikihoBackgroundRuntime` arguments after Task 2. Keep it for acquisition and the lease manager's hosted-page probe.

- [ ] **Step 2: Document the one-shot recovery**

Add to the README lifecycle description:

```text
拡張機能所有のinactive tabでDOM取得が7秒以内に完了しない場合、そのtabが現在も拡張機能所有であることを再確認して1回だけreloadします。reload後の再取得を含む全体上限は25秒です。ユーザーが開いたtabや既存の四季報tabはreloadしません。
```

Preserve the current privacy and permission statements. No manifest permission is added.

- [ ] **Step 3: Run complete extension gates**

Run:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bun run quality:deps:audit
bunx biome check extensions/shikiho
```

Expected: all commands exit 0. Confirm manifest permissions remain only `storage` and `alarms`:

```bash
rg -n '"permissions"|cookies|host_permissions|fetch\(|XMLHttpRequest' extensions/shikiho/manifest.json extensions/shikiho/src extensions/shikiho/README.md
```

- [ ] **Step 4: Run relevant workspace gates**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test
bun run quality:typecheck
bun run workspace:build
git diff --check
```

Expected: web tests, all TypeScript checks/audits, production builds, and diff check exit 0. No OpenAPI sync is required.

- [ ] **Step 5: Mark completed plan steps and commit**

Mark only verified checkboxes in this plan, then run:

```bash
git add apps/ts/extensions/shikiho/src/background.ts apps/ts/extensions/shikiho/README.md docs/superpowers/plans/2026-07-14-symbol-workbench-shikiho-owned-tab-reload.md
git commit -m "docs(shikiho): explain slow tab reload recovery"
```

- [ ] **Step 6: Final repository check**

Run:

```bash
git status --short
git log -6 --oneline
```

Expected: clean worktree and four scoped implementation commits after the design/plan commits.
