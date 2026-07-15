# Shikiho One-Shot Owned Tabs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every extension-owned Company Shikiho tab after one capture and never reuse or navigate it for a later capture.

**Architecture:** Keep the existing ownership-safe lease and progressive diagnostics pipeline, but change the lease manager from warm reuse to one-shot cleanup. Exact user-owned tabs remain outside the lease release path. Legacy idle leases remain parseable only so startup or the next acquisition can close them safely.

**Tech Stack:** Chrome Manifest V3, TypeScript, Bun test, Happy DOM, Biome

## Global Constraints

- Never navigate, reload, or close an exact Shikiho tab that the user already opened.
- Preserve the 25-second absolute capture deadline, progressive diagnostics, memoized DOM extraction, local-only storage, and alphanumeric stock-code support.
- Persist terminal capture state before closing an extension-owned tab.
- Keep legacy warm trace modes and lease parsing compatible for stored-state cleanup.

---

### Task 1: Make extension-owned leases one-shot

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/warm-tab-lease.ts`

**Interfaces:**
- Consumes: `WarmTabLeaseManager.acquire`, `releaseSuccess`, `releaseFailure`, and legacy `ShikihoWarmTabLeaseV1` records.
- Produces: a fresh `new_owned_tab` handle for every acquisition and ownership-safe tab removal after terminal release.

- [ ] **Step 1: Write failing one-shot lifecycle tests**

Add tests that assert:

```ts
test('successful capture closes its owned tab without creating an idle lease', async () => {
  const h = harness();
  const handle = await h.manager.acquire('7203');
  await h.manager.releaseSuccess(handle, '7203');
  expect(h.removes).toEqual([handle.lease.tabId]);
  expect(storedLease(h.session)).toBeUndefined();
  expect(h.alarmCreates).toEqual([]);
});

test('sequential captures create fresh tabs and never navigate a prior tab', async () => {
  const h = harness();
  const first = await h.manager.acquire('7203');
  await h.manager.releaseSuccess(first, '7203');
  const second = await h.manager.acquire('285A');
  expect([first.lease.tabId, second.lease.tabId]).toEqual([100, 101]);
  expect(h.updates).toEqual([]);
});

test('reconcile closes a legacy idle reusable tab immediately', async () => {
  const session = new Map<string, unknown>();
  const h = harness(session);
  h.tabs.add(44);
  session.set(SHIKIHO_WARM_TAB_LEASE_KEY, {
    version: 1,
    tabId: 44,
    ownerToken: 'legacy-owner',
    generation: 1,
    phase: 'idle',
    code: '7203',
    createdAt: NOW,
    idleDeadline: NOW + SHIKIHO_WARM_TAB_IDLE_MS,
  } satisfies ShikihoWarmTabLeaseV1);
  await h.manager.reconcile();
  expect(h.removes).toEqual([44]);
  expect(storedLease(h.session)).toBeUndefined();
});
```

Retain or add an activation test proving that a tab adopted by the user before release is not removed.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/warm-tab-lease.test.ts
```

Expected: failures show successful release persists an idle lease, sequential acquisition reuses/navigates it, and reconciliation retains it.

- [ ] **Step 3: Implement one-shot cleanup**

In `warm-tab-lease.ts`:

```ts
async function reconcile(): Promise<void> {
  const lease = await readLease();
  if (lease === null) return;
  try {
    await deps.tabs.get(lease.tabId);
  } catch {
    await removeMetadataIfCurrent(lease);
    return;
  }
  if (lease.phase === 'capturing') {
    if (!activeCaptures.has(activeIdentity(lease))) await closeExact(lease);
    return;
  }
  await closeExact(lease);
}

async function acquireSerialized(code: string): Promise<WarmTabHandle> {
  if (!isCanonicalCode(code)) throw new Error(`Expected a canonical four-character Shikiho code: ${code}`);
  await reconcile();
  const current = await readLease();
  if (current?.phase === 'capturing') throw new Error('A warm-tab capture is already active');
  return createOwnedTab(code);
}

async function releaseSuccess(handle: WarmTabHandle, code: string): Promise<void> {
  if (!isCanonicalCode(code)) throw new Error(`Expected a canonical four-character Shikiho code: ${code}`);
  activeCaptures.delete(activeIdentity(handle.lease));
  await closeExact(handle.lease);
}
```

Remove now-unreachable reuse/navigation and idle-transition helpers. Retain legacy lease parsing, alarm parsing, and ownership-abandonment hooks required for old-state cleanup.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the Task 1 command again.

Expected: all warm-tab lease tests pass, with no tab update during sequential captures.

- [ ] **Step 5: Commit Task 1**

```bash
git add apps/ts/extensions/shikiho/src/warm-tab-lease.ts apps/ts/extensions/shikiho/src/warm-tab-lease.test.ts
git commit -m "fix(shikiho): make owned tabs one-shot"
```

---

### Task 2: Verify acquisition boundaries and update operator guidance

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/tab-acquisition.test.ts`
- Modify: `apps/ts/extensions/shikiho/README.md`
- Modify: `docs/superpowers/plans/2026-07-15-shikiho-one-shot-owned-tabs.md`

**Interfaces:**
- Consumes: one-shot `WarmTabLeaseManager` behavior from Task 1.
- Produces: regression evidence that owned terminal paths close tabs while exact user tabs remain untouched, plus current Chrome instructions.

- [x] **Step 1: Add acquisition regression tests**

Add or strengthen tests that assert:

```ts
expect(releaseSuccess).toHaveBeenCalledWith(handle, '7203');
expect(releaseFailure).not.toHaveBeenCalled();
```

for successful/partial owned captures, and retain exact-user-tab tests proving neither release method is called. Verify terminal trace persistence occurs before `releaseSuccess`.

- [x] **Step 2: Run acquisition tests**

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test -- src/tab-acquisition.test.ts
```

Expected: all tests pass without changing the exact-user-tab path.

- [x] **Step 3: Update current README wording**

Replace active warm-reuse guidance with:

```md
同じ銘柄のユーザー所有タブがある場合はその表示済みDOMを読み、遷移・再読み込み・閉鎖はしません。表示済みタブがない場合はinactive tabを1回の取得専用に作成し、取得終了後に閉じます。次回取得では新しいtabを作成します。
```

Remove statements that a generated tab is reused for 3 minutes or up to 5 minutes. Keep historical design records unchanged.

- [x] **Step 4: Run complete gates**

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bunx biome check extensions/shikiho
bun run quality:deps:audit
git diff --check
```

Expected: every command exits 0; manifest permissions, local-only privacy, and snapshot contracts are unchanged.

- [ ] **Step 5: Perform Chrome acceptance**

After rebuilding and reloading the unpacked extension:

1. Capture a symbol without an exact user-owned Shikiho tab.
2. Confirm data appears and the generated tab disappears after terminal capture.
3. Capture a second symbol and confirm a new tab is created rather than navigating the first.
4. Open an exact Shikiho tab manually, force refresh that symbol, and confirm the user tab remains open at the same URL.

- [ ] **Step 6: Mark verified boxes and commit Task 2**

```bash
git add apps/ts/extensions/shikiho/README.md apps/ts/extensions/shikiho/src/tab-acquisition.test.ts docs/superpowers/plans/2026-07-15-shikiho-one-shot-owned-tabs.md
git commit -m "docs(shikiho): record one-shot tab acceptance"
```
