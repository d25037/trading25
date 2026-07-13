# Symbol Workbench Shikiho Background Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the selected Symbol Workbench company automatically through a 24-hour extension-local cache and one inactive authenticated Shikiho tab, while retaining a manual refresh and safe diagnostics.

**Architecture:** A dependency-injected background capture coordinator owns cache freshness, retry suppression, same-code singleflight, the one-at-a-time queue, and extension-owned tab lifecycle. The existing Shikiho content script remains the rendered-DOM reader; the localhost bridge requests only a normalized code plus `forceRefresh`, and React renders the matching response while exposing one refresh action.

**Tech Stack:** TypeScript, Bun test/build, Chrome Manifest V3 extension APIs, React 19, Vitest/Testing Library, Biome.

## Global Constraints

- Successful snapshots are fresh for strictly less than `24 * 60 * 60 * 1000` milliseconds; the exact boundary is expired.
- Automatic failures suppress another automatic attempt for 60 seconds; manual refresh bypasses cache and suppression.
- Duplicate same-code requests singleflight; different-code jobs execute one at a time in request order.
- The extension closes only the inactive tab ID it created for the current job, on every terminal path.
- The page can request only a normalized four-digit symbol and a boolean refresh flag, never an arbitrary URL or tab ID.
- No `cookies`, `webRequest`, `declarativeNetRequest`, history, downloads, native messaging, fetch/XHR, raw HTML, backend state, bulk crawl, or telemetry.
- Failed refresh never replaces the last valid snapshot, and Workbench never displays another symbol's data.
- Source, status, edition/update date, capture time, and manual update remain visible.

---

### Task 1: Classify the current paid-plan page as login-required

**Files:**
- Create: `apps/ts/packages/shikiho-extension/src/fixtures/7203-login-plan-required.html`
- Modify: `apps/ts/packages/shikiho-extension/src/extractor.test.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/extractor.ts`

**Interfaces:**
- Consumes: `extractShikihoPage(document, location, now, extractorVersion)`.
- Produces: current paid-plan prompt returns `{ kind: 'login_required', code: '7203' }`; valid authenticated fixtures with a navigation login control still extract normally.

- [ ] **Step 1: Add the sanitized real-page fixture**

Create this minimal fixture; do not add tables, advertisements, account information, or full page HTML:

```html
<!doctype html>
<html lang="ja">
  <body>
    <nav><button>ログイン</button></nav>
    <main>
      <span>7203</span>
      <h1>トヨタ自動車</h1>
      <h3>ベーシック・プレミアムプランでは、記事本文など、すべての情報が閲覧できます</h3>
    </main>
  </body>
</html>
```

- [ ] **Step 2: Write failing classification tests**

Add these tests using the test file's existing `parseFixture`, `STOCK_URL`, and `NOW` helpers:

```ts
test('classifies the current paid-plan prompt as login required', () => {
  const document = parseFixture('7203-login-plan-required.html');
  expect(extractShikihoPage(document, STOCK_URL, NOW, '1.0.0')).toEqual({
    kind: 'login_required',
    code: '7203',
  });
});

test('does not treat a navigation login control as sufficient on a valid page', () => {
  const document = parseFixture('7203-authenticated.html');
  document.querySelector('body')?.insertAdjacentHTML('afterbegin', '<nav><button>ログイン</button></nav>');
  expect(extractShikihoPage(document, STOCK_URL, NOW, '1.0.0').kind).toBe('success');
});
```

- [ ] **Step 3: Run the focused test and prove RED**

Run: `cd apps/ts && bun test packages/shikiho-extension/src/extractor.test.ts`

Expected: the paid-plan fixture is incorrectly returned as `page_changed` before implementation.

- [ ] **Step 4: Extend login detection narrowly**

In `isLoginRequired`, add the exact visible paid-plan phrase while keeping the existing password and login-required phrases:

```ts
return (
  /ログインして.*閲覧/.test(pageText) ||
  /ログインが必要/.test(pageText) ||
  /会員ログイン/.test(pageText) ||
  /ベーシック・プレミアムプランでは、記事本文など、すべての情報が閲覧できます/.test(pageText) ||
  hasVisiblePassword
);
```

Do not classify on a standalone `ログイン` button.

- [ ] **Step 5: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/extractor.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: all extractor tests and typecheck pass.

Commit:

```bash
git add apps/ts/packages/shikiho-extension/src/extractor.ts \
  apps/ts/packages/shikiho-extension/src/extractor.test.ts \
  apps/ts/packages/shikiho-extension/src/fixtures/7203-login-plan-required.html
git commit -m "fix(extension): detect current Shikiho login prompt"
```

---

### Task 2: Add the background capture coordinator

**Files:**
- Create: `apps/ts/packages/shikiho-extension/src/background-capture.ts`
- Create: `apps/ts/packages/shikiho-extension/src/background-capture.test.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/background.ts`

**Interfaces:**
- Consumes: repository `get`, `saveSnapshot`, and `saveDiagnostic`; validated capture messages and sender tab IDs.
- Produces: `createBackgroundCaptureCoordinator(deps)`, with `resolve(code, forceRefresh)`, `acceptSnapshot(snapshot, senderTabId)`, and `acceptDiagnostic(diagnostic, senderTabId)`.

- [ ] **Step 1: Define dependency and coordinator interfaces in the test**

Use deterministic injected dependencies:

```ts
interface BackgroundCaptureDeps {
  now(): number;
  get(code: string): Promise<StoredShikihoState>;
  saveSnapshot(snapshot: ShikihoSnapshotV1): Promise<void>;
  saveDiagnostic(diagnostic: ShikihoCaptureDiagnosticV1): Promise<void>;
  createTab(url: string): Promise<{ id: number }>;
  closeTab(tabId: number): Promise<void>;
  setTimer(callback: () => void, delayMs: number): unknown;
  clearTimer(timer: unknown): void;
}

interface StoredShikihoState {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
}
```

The production module exports:

```ts
export const SHIKIHO_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
export const SHIKIHO_RETRY_SUPPRESSION_MS = 60 * 1000;
export const SHIKIHO_CAPTURE_TIMEOUT_MS = 25 * 1000;
```

- [ ] **Step 2: Write failing freshness and manual-refresh tests**

Cover snapshot ages `TTL - 1` and exactly `TTL`, plus manual refresh bypassing both a fresh snapshot and a recent diagnostic. Assert the fresh automatic path creates zero tabs.

- [ ] **Step 3: Write failing concurrency and lifecycle tests**

Cover:

```ts
const first = coordinator.resolve('7203', false);
const duplicate = coordinator.resolve('7203', false);
expect(createTab).toHaveBeenCalledTimes(1);
expect(first).toBe(duplicate);
```

Also assert different codes serialize, only the matching generated tab/code completes a job, and `closeTab(ownedId)` runs after success, diagnostic, timeout, user-close simulation, and thrown storage errors. Assert a non-owned sender tab is never passed to `closeTab`.

- [ ] **Step 4: Run the coordinator suite and prove RED**

Run: `cd apps/ts && bun test packages/shikiho-extension/src/background-capture.test.ts`

Expected: FAIL because `background-capture.ts` does not exist.

- [ ] **Step 5: Implement the minimal coordinator**

Implement one FIFO promise tail for different codes and a `Map<string, Promise<StoredShikihoState>>` for same-code singleflight. A job creates only:

```ts
const url = `https://shikiho.toyokeizai.net/stocks/${code}`;
const { id } = await deps.createTab(url);
ownedTabId = id;
```

Store the pending resolver keyed by code and tab ID. Complete only from a matching sender. In `finally`, clear the timeout, delete pending/singleflight entries, and call `closeTab(ownedTabId)` once when defined. Read and return the repository state after terminal capture.

- [ ] **Step 6: Integrate the coordinator into `background.ts`**

Change the internal request to exact-key validated:

```ts
{ type: 'resolve_snapshot'; code: unknown; forceRefresh: unknown }
```

Pass `sender.tab?.id ?? null` to capture acceptance. Passive captures from user-owned Shikiho tabs still save through the repository but do not complete or close a generated job unless tab ID and code match. Production tab dependencies use `chrome.tabs.create({ active: false, url })` and `chrome.tabs.remove(tabId)`; no `tabs` permission is added because create/remove do not require access to sensitive tab properties.

- [ ] **Step 7: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/background-capture.test.ts packages/shikiho-extension/src/storage.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
```

Expected: all focused tests, typecheck, and MV3 build pass.

Commit:

```bash
git add apps/ts/packages/shikiho-extension/src/background-capture.ts \
  apps/ts/packages/shikiho-extension/src/background-capture.test.ts \
  apps/ts/packages/shikiho-extension/src/background.ts
git commit -m "feat(extension): resolve Shikiho snapshots in background"
```

---

### Task 3: Extend the bridge and React hook for automatic and forced refresh

**Files:**
- Modify: `apps/ts/packages/shikiho-extension/src/contract.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/contract.test.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/localhost-content.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/localhost-content.test.ts`
- Modify: `apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts`
- Modify: `apps/ts/packages/web/src/hooks/useShikihoSnapshot.test.tsx`

**Interfaces:**
- Consumes: background `{ type: 'resolve_snapshot', code, forceRefresh }`.
- Produces: required boolean `forceRefresh` in `ShikihoBridgeRequestV1`; hook result adds `isRefreshing: boolean` and `refresh(): void`.

- [ ] **Step 1: Write failing contract and localhost bridge tests**

Require the exact page request:

```ts
{
  channel: SHIKIHO_BRIDGE_CHANNEL,
  direction: 'page-to-extension',
  type: 'get_snapshot',
  requestId: 'request-1',
  code: '7203',
  forceRefresh: false,
}
```

Reject missing, string, numeric, or extra-key `forceRefresh`. Assert the runtime call is exactly `{ type: 'resolve_snapshot', code: '7203', forceRefresh: false }`.

- [ ] **Step 2: Write failing hook tests**

Cover initial request with `forceRefresh: false`, one request after symbol change, `refresh()` sending `forceRefresh: true`, old request IDs being ignored, and `isRefreshing` staying true until the matching snapshot response. Keep the prior owned snapshot visible during forced refresh.

- [ ] **Step 3: Run RED**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/contract.test.ts \
  packages/shikiho-extension/src/localhost-content.test.ts \
  packages/web/src/hooks/useShikihoSnapshot.test.tsx
```

Expected: failures for the missing refresh contract and hook API.

- [ ] **Step 4: Implement the contract and localhost translation**

Add `forceRefresh: boolean` to the get-snapshot request variant and exact-key validator. Translate it to `resolve_snapshot`. Storage notifications may re-read the repository with `forceRefresh: false`; they must not create a forced loop.

- [ ] **Step 5: Implement a stable hook refresh API**

Factor request dispatch into a stable callback that generates a new request ID, posts ping plus get-snapshot, and records refreshing state. Initial/symbol-change calls use `false`; returned `refresh` uses `true`. Only a matching current request/code clears `isRefreshing` and updates owned state.

- [ ] **Step 6: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/contract.test.ts \
  packages/shikiho-extension/src/localhost-content.test.ts \
  packages/web/src/hooks/useShikihoSnapshot.test.tsx
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/web typecheck
```

Commit:

```bash
git add apps/ts/packages/shikiho-extension/src/contract.ts \
  apps/ts/packages/shikiho-extension/src/contract.test.ts \
  apps/ts/packages/shikiho-extension/src/localhost-content.ts \
  apps/ts/packages/shikiho-extension/src/localhost-content.test.ts \
  apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts \
  apps/ts/packages/web/src/hooks/useShikihoSnapshot.test.tsx
git commit -m "feat(web): request automatic Shikiho refresh"
```

---

### Task 4: Add refresh presentation and operator documentation

**Files:**
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`
- Modify: `apps/ts/packages/shikiho-extension/README.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: hook `isRefreshing` and `refresh()`.
- Produces: compact `更新` action, accessible `取得中` state, automatic-flow documentation.

- [ ] **Step 1: Write failing panel/page tests**

Assert the panel renders a button with accessible name `会社四季報を更新`, calls `onRefresh` once, disables it while `isRefreshing`, shows `取得中`, and retains existing snapshot text during refresh. Assert `SymbolWorkbenchPage` passes the hook refresh API into the panel.

- [ ] **Step 2: Run RED**

Run:

```bash
cd apps/ts
bun test packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx \
  packages/web/src/pages/SymbolWorkbenchPage.test.tsx
```

- [ ] **Step 3: Implement the compact action**

Extend panel props with:

```ts
isRefreshing: boolean;
onRefresh(): void;
```

Place a small text/icon `更新` button beside the source link, before the collapse button. Keep the existing body rendered while refreshing. The status badge shows `取得中`; source, capture time, and edition metadata remain visible.

- [ ] **Step 4: Update documentation**

Replace the button-first workflow with: selecting a symbol triggers an inactive background tab only when no fresh 24-hour snapshot exists; the extension closes only its generated tab; `更新` forces a refresh; login must exist in the same normal Atlas profile. Retain privacy and troubleshooting sections.

- [ ] **Step 5: Run GREEN and commit**

Run:

```bash
cd apps/ts
bun test packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx \
  packages/web/src/pages/SymbolWorkbenchPage.test.tsx
bun run --filter @trading25/web typecheck
bun run quality:lint
```

Commit:

```bash
git add apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx \
  apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx \
  apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx \
  apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx \
  apps/ts/packages/shikiho-extension/README.md README.md
git commit -m "feat(web): refresh Shikiho automatically"
```

---

### Task 5: Complete branch and Atlas acceptance verification

**Files:**
- No planned tracked file changes; any discovered defect must be fixed only in a file already named by Tasks 1-4 and covered by its focused test.
- Modify: `.superpowers/sdd/progress.md` (ignored coordination ledger).

**Interfaces:**
- Consumes: completed background refresh flow.
- Produces: fresh automated and normal-Atlas evidence for every acceptance criterion.

- [ ] **Step 1: Run the complete automated gate**

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bun run --filter @trading25/web test -- \
  src/hooks/useShikihoSnapshot.test.tsx \
  src/components/SymbolWorkbench/ShikihoPanel.test.tsx \
  src/pages/SymbolWorkbenchPage.test.tsx
bun run --filter @trading25/web typecheck
bun run quality:deps:audit
bun run quality:lint
bun run workspace:test
bun run workspace:build
```

Expected: every command exits 0 without weakening existing tests.

- [ ] **Step 2: Audit the privacy and tab boundary**

Run:

```bash
git diff 3eb71a4f..HEAD --check
rg -n "fetch\(|XMLHttpRequest|dangerouslySetInnerHTML|permissions.*cookies|webRequest|declarativeNetRequest" \
  apps/ts/packages/shikiho-extension \
  apps/ts/packages/web/src/components/SymbolWorkbench \
  apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts
```

Expected: no Shikiho network request, raw-HTML rendering, credential permission, or whitespace error.

- [ ] **Step 3: Reload the unpacked extension and run normal-Atlas acceptance**

Use the normal Atlas profile, not Codex Web Preview. Open only `http://localhost:4173/symbol-workbench?symbol=7203`. Verify one inactive Shikiho tab opens/closes without pressing `四季報で開く`. Signed out must render `Login required`; after signing in, `更新` must populate the snapshot. Reload within 24 hours and prove no tab opens. Switch to a second symbol and prove separate capture/no foreign-symbol flash. Force refresh and prove the prior snapshot remains during refresh.

- [ ] **Step 4: Final acceptance audit**

Check all eight acceptance criteria in `docs/superpowers/specs/2026-07-12-symbol-workbench-shikiho-background-refresh-design.md` against tests, current source, built manifest, and live Atlas evidence. Do not claim completion while any item is indirect or missing.
