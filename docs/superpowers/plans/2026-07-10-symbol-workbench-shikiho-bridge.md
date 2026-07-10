# Symbol Workbench Shikiho Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local-only Atlas extension that automatically captures approved fields from an authenticated Company Shikiho stock page and renders the latest matching capture in a compact Symbol Workbench panel.

**Architecture:** A Manifest V3 extension observes already-rendered Shikiho DOM, validates a dependency-free `ShikihoSnapshotV1`, and stores the latest valid snapshot plus separate diagnostics in `chrome.storage.local`. A localhost content script provides a narrow `window.postMessage` bridge; one React hook validates matching responses and one compact panel renders escaped text. FastAPI and all Trading25 data stores remain unchanged.

**Tech Stack:** TypeScript 6, Bun build/test, Manifest V3 browser extension APIs, happy-dom fixtures, React 19, Vitest, Testing Library, TanStack Router route state.

## Global Constraints

- Personal, local use only; no remote sync, telemetry, redistribution, scheduled crawl, or bulk capture.
- Read only the rendered `https://shikiho.toyokeizai.net/stocks/*` DOM; issue no Shikiho fetch/XHR and click no hidden tabs.
- Request only the extension `storage` permission; never request `cookies`, `tabs`, or `scripting`.
- Store no cookies, authorization headers, raw HTML, images, charts, or capture payload logs.
- Keep only the latest valid snapshot for at most 200 symbols; failed observations never replace valid data.
- Add no FastAPI route, OpenAPI contract, DuckDB/SQLite storage, chart-store setting, or panel-order setting.
- Render captured strings as React text; never use `dangerouslySetInnerHTML`.
- Do not edit or stage the pre-existing dirty Ranking snapshot files while implementing this feature.

---

### Task 1: Add the extension workspace and shared runtime contract

**Files:**
- Create: `apps/ts/packages/shikiho-extension/package.json`
- Create: `apps/ts/packages/shikiho-extension/tsconfig.json`
- Create: `apps/ts/packages/shikiho-extension/src/contract.ts`
- Create: `apps/ts/packages/shikiho-extension/src/contract.test.ts`
- Modify: `apps/ts/package.json`
- Modify: `apps/ts/packages/web/package.json`
- Modify: `apps/ts/packages/web/tsconfig.json`
- Modify: `apps/ts/scripts/dependency-audit.ts`
- Modify: `apps/ts/biome.json`
- Modify: `apps/ts/bun.lock`

**Interfaces:**
- Produces: `ShikihoSnapshotV1`, `ShikihoCaptureDiagnosticV1`, `ShikihoBridgeRequestV1`, `ShikihoBridgeResponseV1`.
- Produces: `normalizeShikihoCode(value): string | null`, `parseShikihoSnapshot(value): ShikihoSnapshotV1 | null`, `parseShikihoDiagnostic(value): ShikihoCaptureDiagnosticV1 | null`, and `parseShikihoBridgeResponse(value): ShikihoBridgeResponseV1 | null`.

- [ ] **Step 1: Write failing contract tests**

```ts
import { describe, expect, test } from 'bun:test';
import { normalizeShikihoCode, parseShikihoBridgeResponse, parseShikihoSnapshot } from './contract';

describe('Shikiho bridge contract', () => {
  test('normalizes compatible stock codes', () => {
    expect(normalizeShikihoCode('7203')).toBe('7203');
    expect(normalizeShikihoCode('72030')).toBe('7203');
    expect(normalizeShikihoCode('720A')).toBeNull();
  });

  test('rejects foreign hosts and code/source mismatches', () => {
    expect(parseShikihoSnapshot(validSnapshot({ sourceUrl: 'https://example.com/stocks/7203' }))).toBeNull();
    expect(parseShikihoSnapshot(validSnapshot({ code: '6758' }))).toBeNull();
  });

  test('accepts only extension-to-page protocol messages', () => {
    expect(parseShikihoBridgeResponse(validBridgeResponse())).not.toBeNull();
    expect(parseShikihoBridgeResponse({ ...validBridgeResponse(), direction: 'page-to-extension' })).toBeNull();
  });
});
```

- [ ] **Step 2: Run the test and verify the missing module failure**

Run: `cd apps/ts && bun test packages/shikiho-extension/src/contract.test.ts`

Expected: FAIL because the package and `contract.ts` do not exist.

- [ ] **Step 3: Add the workspace package and dependency-free contract**

Use package name `@trading25/shikiho-extension` with this source export:

```json
{
  "name": "@trading25/shikiho-extension",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "exports": { "./contract": "./src/contract.ts" },
  "scripts": {
    "build": "bun scripts/build.ts",
    "clean": "rm -rf dist",
    "test": "bun test",
    "test:coverage": "bun test --coverage",
    "typecheck": "tsc --noEmit"
  },
  "devDependencies": {
    "@types/chrome": "0.2.2",
    "happy-dom": "^20.10.6"
  }
}
```

Define exact v1 message directions and types:

```ts
export const SHIKIHO_BRIDGE_CHANNEL = 'trading25.shikiho.v1';
export type ShikihoBridgeRequestV1 =
  | { channel: typeof SHIKIHO_BRIDGE_CHANNEL; direction: 'page-to-extension'; type: 'ping'; requestId: string }
  | { channel: typeof SHIKIHO_BRIDGE_CHANNEL; direction: 'page-to-extension'; type: 'get_snapshot'; requestId: string; code: string };

export type ShikihoBridgeResponseV1 =
  | { channel: typeof SHIKIHO_BRIDGE_CHANNEL; direction: 'extension-to-page'; type: 'ready'; requestId: string }
  | {
      channel: typeof SHIKIHO_BRIDGE_CHANNEL;
      direction: 'extension-to-page';
      type: 'snapshot';
      requestId: string;
      code: string;
      snapshot: ShikihoSnapshotV1 | null;
      diagnostic: ShikihoCaptureDiagnosticV1 | null;
    };
```

Manual runtime validators must enforce schema version, four-digit code, exact Shikiho HTTPS host, `/stocks/{code}` equality, ISO timestamps, finite scores in `0..5`, list/string limits, and a 64 KiB serialized snapshot ceiling.

Add the workspace and scripts to `apps/ts/package.json`, add `@trading25/shikiho-extension: workspace:*` to Web dependencies and TypeScript paths, and add the extension manifest to `dependency-audit.ts`. Include `packages/*/scripts/**/*.ts` in Biome inputs. Run `bun install` to update `bun.lock`.

- [ ] **Step 4: Run contract, type, and dependency checks**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/contract.test.ts
bun run --filter @trading25/shikiho-extension typecheck
bun run quality:deps:audit
```

Expected: all commands PASS; dependency audit reports the extension manifest among checked manifests.

- [ ] **Step 5: Commit the workspace contract**

```bash
git add apps/ts/package.json apps/ts/bun.lock apps/ts/biome.json apps/ts/scripts/dependency-audit.ts \
  apps/ts/packages/web/package.json apps/ts/packages/web/tsconfig.json apps/ts/packages/shikiho-extension
git commit -m "feat(extension): add Shikiho bridge contract"
```

### Task 2: Implement label-anchored Shikiho extraction

**Files:**
- Create: `apps/ts/packages/shikiho-extension/src/extractor.ts`
- Create: `apps/ts/packages/shikiho-extension/src/extractor.test.ts`
- Create: `apps/ts/packages/shikiho-extension/src/fixtures/7203-authenticated.html`
- Create: `apps/ts/packages/shikiho-extension/src/fixtures/login-required.html`
- Create: `apps/ts/packages/shikiho-extension/src/fixtures/page-changed.html`

**Interfaces:**
- Consumes: `ShikihoSnapshotV1` and `normalizeShikihoCode` from Task 1.
- Produces: `extractShikihoPage(document, location, now, extractorVersion): ShikihoExtractionResult` where result is `{ kind: 'success'; snapshot } | { kind: 'login_required' | 'page_changed'; code }`.

- [ ] **Step 1: Add sanitized fixture-driven failing tests**

```ts
test('extracts approved 7203 fields without retaining markup', () => {
  const document = parseFixture('7203-authenticated.html');
  const result = extractShikihoPage(document, new URL('https://shikiho.toyokeizai.net/stocks/7203'), NOW, '1.0.0');
  expect(result.kind).toBe('success');
  if (result.kind !== 'success') throw new Error('expected success');
  expect(result.snapshot.features).toContain('4輪世界首位');
  expect(result.snapshot.commentary.map((item) => item.heading)).toEqual(['連続減益', '対応策']);
  expect(result.snapshot.score).toMatchObject({ overall: 4, growth: 5, profitability: 5, safety: 2 });
  expect(result.snapshot.comparisonCompanies).toContainEqual({ code: '7201', name: '日産自動車' });
  expect(result.snapshot.features).not.toContain('<strong>');
});

test('distinguishes login and page-shape failures', () => {
  expect(extractFixture('login-required.html').kind).toBe('login_required');
  expect(extractFixture('page-changed.html').kind).toBe('page_changed');
});
```

- [ ] **Step 2: Run the extractor test and verify failure**

Run: `cd apps/ts && bun test packages/shikiho-extension/src/extractor.test.ts`

Expected: FAIL because `extractor.ts` is missing.

- [ ] **Step 3: Implement DOM helpers and extraction**

Implement small helpers with explicit responsibilities:

```ts
function normalizeText(value: string | null | undefined): string;
function findExactLabel(root: ParentNode, label: string): Element | null;
function extractLabelValue(root: ParentNode, label: string): string | null;
function extractStockLinks(root: ParentNode): Array<{ code: string | null; name: string }>;
function parseScore(value: string | null): number | null;
function computeContentHash(snapshotWithoutCaptureTime: unknown): Promise<string>;
```

Use visible Japanese labels and section ancestors for `特色`, `連結事業`, `四季報スコア`, `所属業界`, `市場テーマ`, `比較会社`, and commentary. Parse bracketed commentary headings such as `【連続減益】` only inside the anchored commentary region. Treat missing core identity/commentary anchors as `page_changed`; treat missing optional sections as `partial` and list stable field keys in `missingFields`.

Plain text means DOM markup is discarded by `textContent` and later rendered only as escaped React text. Preserve literal source characters such as `<`, `>`, and `&`; do not HTML-entity encode or otherwise rewrite the source wording in the stored snapshot.

- [ ] **Step 4: Run extractor tests and typecheck**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/extractor.test.ts
bun run --filter @trading25/shikiho-extension typecheck
```

Expected: PASS.

- [ ] **Step 5: Commit extraction**

```bash
git add apps/ts/packages/shikiho-extension/src/extractor.ts \
  apps/ts/packages/shikiho-extension/src/extractor.test.ts \
  apps/ts/packages/shikiho-extension/src/fixtures
git commit -m "feat(extension): extract rendered Shikiho fields"
```

### Task 3: Add automatic capture, safe storage, and buildable extension entry points

**Files:**
- Create: `apps/ts/packages/shikiho-extension/manifest.json`
- Create: `apps/ts/packages/shikiho-extension/scripts/build.ts`
- Create: `apps/ts/packages/shikiho-extension/src/storage.ts`
- Create: `apps/ts/packages/shikiho-extension/src/storage.test.ts`
- Create: `apps/ts/packages/shikiho-extension/src/capture-controller.ts`
- Create: `apps/ts/packages/shikiho-extension/src/capture-controller.test.ts`
- Create: `apps/ts/packages/shikiho-extension/src/background.ts`
- Create: `apps/ts/packages/shikiho-extension/src/shikiho-content.ts`
- Create: `apps/ts/packages/shikiho-extension/src/localhost-content.ts`
- Create: `apps/ts/packages/shikiho-extension/src/localhost-content.test.ts`

**Interfaces:**
- Consumes: Task 1 contracts and Task 2 `extractShikihoPage`.
- Produces: `createCaptureController(options): { start(): void; stop(): void }`.
- Produces: background messages `capture_success`, `capture_diagnostic`, and `get_snapshot`.
- Produces: a localhost bridge on ports `5173` and `4173` only.

- [ ] **Step 1: Write failing storage/controller/bridge tests**

```ts
test('keeps a valid snapshot when a newer diagnostic is recorded', async () => {
  await repository.saveSnapshot(snapshot7203);
  await repository.saveDiagnostic({ schemaVersion: 1, code: '7203', observedAt: LATER, status: 'page_changed' });
  expect(await repository.get('7203')).toEqual({ snapshot: snapshot7203, diagnostic: expect.any(Object) });
});

test('debounces DOM mutations and recaptures after URL code change', async () => {
  controller.start();
  mutateThreeTimes();
  await advanceQuietPeriod();
  expect(capture).toHaveBeenCalledTimes(1);
  navigateTo('/stocks/6758');
  await advanceQuietPeriod();
  expect(capture).toHaveBeenLastCalledWith('6758');
});

test('does not activate the localhost bridge on an unapproved port', () => {
  expect(isAllowedTrading25Origin(new URL('http://localhost:3002'))).toBe(false);
  expect(isAllowedTrading25Origin(new URL('http://localhost:5173'))).toBe(true);
});
```

- [ ] **Step 2: Run tests and verify missing implementations**

Run:

```bash
cd apps/ts
bun test packages/shikiho-extension/src/storage.test.ts \
  packages/shikiho-extension/src/capture-controller.test.ts \
  packages/shikiho-extension/src/localhost-content.test.ts
```

Expected: FAIL because the modules do not exist.

- [ ] **Step 3: Implement storage and capture control**

Use two storage maps, `shikihoSnapshotsV1` and `shikihoDiagnosticsV1`. `saveSnapshot` ignores an unchanged `contentHash`, clears older diagnostics after a successful capture, and evicts the least-recently-captured symbol above 200. `saveDiagnostic` never mutates snapshots.

The controller uses one `MutationObserver`, a quiet-period timer, a 10-second initial maximum timer, and URL/code comparison on each scheduled pass. `stop()` disconnects the observer and clears both timers.

- [ ] **Step 4: Implement MV3 entry points and manifest**

Use only `permissions: ["storage"]`. Chromium match patterns cannot constrain ports, so localhost content-script matches use `http://localhost/*` and `http://127.0.0.1/*`; `localhost-content.ts` must return before adding listeners unless the port is exactly `5173` or `4173`.

The Shikiho content script sends normalized captures/diagnostics to the background. The localhost script checks `event.source === window`, parses exact page-to-extension protocol messages, forwards `get_snapshot`, and emits only exact extension-to-page responses. It subscribes to `chrome.storage.onChanged` and notifies only the currently requested code.

Build all three entries as browser-target IIFEs with `Bun.build`, copy `manifest.json` into `dist`, and throw on any build error. Expected artifacts are `background.js`, `shikiho-content.js`, `localhost-content.js`, and `manifest.json`.

- [ ] **Step 5: Run extension tests, typecheck, and build verification**

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
test -f packages/shikiho-extension/dist/manifest.json
test -f packages/shikiho-extension/dist/background.js
test -f packages/shikiho-extension/dist/shikiho-content.js
test -f packages/shikiho-extension/dist/localhost-content.js
```

Expected: all commands PASS and all four artifacts exist.

- [ ] **Step 6: Commit the working extension**

```bash
git add apps/ts/packages/shikiho-extension
git commit -m "feat(extension): capture and store Shikiho snapshots"
```

### Task 4: Implement the Workbench bridge hook

**Files:**
- Create: `apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts`
- Create: `apps/ts/packages/web/src/hooks/useShikihoSnapshot.test.tsx`

**Interfaces:**
- Consumes: shared protocol parsers from `@trading25/shikiho-extension/contract`.
- Produces: `useShikihoSnapshot(symbol): { bridgeStatus; snapshot; diagnostic; captureState }`.

- [ ] **Step 1: Write failing hook tests**

```tsx
test('requests the selected code and accepts only its matching response', async () => {
  const { result, rerender } = renderHook(({ symbol }) => useShikihoSnapshot(symbol), { initialProps: { symbol: '7203' } });
  const request7203 = lastPageRequest();
  rerender({ symbol: '6758' });
  emitExtensionResponse(snapshotResponse(request7203.requestId, snapshot7203));
  expect(result.current.snapshot).toBeNull();
  emitExtensionResponse(snapshotResponse(lastPageRequest().requestId, snapshot6758));
  expect(result.current.snapshot?.code).toBe('6758');
});

test('retains a valid snapshot when a newer diagnostic marks it stale', () => {
  emitExtensionResponse(responseWith(snapshot7203, newerPageChangedDiagnostic));
  expect(result.current.snapshot?.code).toBe('7203');
  expect(result.current.captureState).toBe('stale');
});
```

Also test the ping handshake, extension timeout, `event.source`, protocol direction, schema/host/path/payload rejection, listener cleanup, and live storage notification.

- [ ] **Step 2: Run the hook test and verify failure**

Run: `cd apps/ts && bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx`

Expected: FAIL because the hook does not exist.

- [ ] **Step 3: Implement one stable listener plus symbol-specific requests**

```ts
export type ShikihoCaptureState =
  | 'checking_extension'
  | 'extension_unavailable'
  | 'not_captured'
  | 'captured'
  | 'partial'
  | 'stale'
  | 'login_required'
  | 'page_changed'
  | 'storage_error';
```

Mount the global `message` listener once. Keep current normalized code and request ID in refs. A separate `[symbol]` effect clears foreign-symbol state, creates a request ID with `crypto.randomUUID()`, posts `ping` and `get_snapshot`, and starts a short availability timeout. Accept a response only when `event.source === window`, the shared parser succeeds, request/current code match, and the Shikiho source code equals the selected symbol.

- [ ] **Step 4: Run hook tests and Web typecheck**

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx
bun run --filter @trading25/web typecheck
```

Expected: PASS.

- [ ] **Step 5: Commit the Web bridge hook**

```bash
git add apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts \
  apps/ts/packages/web/src/hooks/useShikihoSnapshot.test.tsx
git commit -m "feat(web): connect to local Shikiho extension"
```

### Task 5: Add the compact Shikiho panel and Symbol Workbench integration

**Files:**
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`

**Interfaces:**
- Consumes: Task 4 hook result and existing `handleSelectSymbol(symbol)` route-state callback.
- Produces: `ShikihoPanel({ symbol, snapshot, diagnostic, captureState, onSelectSymbol })`.

- [ ] **Step 1: Write failing panel tests**

```tsx
test('renders a compact captured snapshot and comparison navigation', async () => {
  const onSelectSymbol = vi.fn();
  render(<ShikihoPanel symbol="7203" snapshot={snapshot7203} diagnostic={null} captureState="captured" onSelectSymbol={onSelectSymbol} />);
  expect(screen.getByText('特色')).toBeInTheDocument();
  expect(screen.getByText(/4輪世界首位/)).toBeInTheDocument();
  await userEvent.click(screen.getByRole('button', { name: /7201 日産自動車/ }));
  expect(onSelectSymbol).toHaveBeenCalledWith('7201');
});

test('renders source text literally and supports collapse', async () => {
  renderPanel(snapshotWithFeatures('<img src=x onerror=alert(1)>'));
  expect(screen.getByText('<img src=x onerror=alert(1)>')).toBeInTheDocument();
  expect(document.querySelector('img')).toBeNull();
  await userEvent.click(screen.getByRole('button', { name: /会社四季報を折りたたむ/ }));
  expect(screen.queryByText('特色')).not.toBeInTheDocument();
});
```

Cover unavailable, not captured, partial, stale, login required, page changed, source link, capture time, edition, mobile-safe link, and omitted empty optional sections.

- [ ] **Step 2: Run the panel test and verify failure**

Run: `cd apps/ts && bun run --filter @trading25/web test -- src/components/SymbolWorkbench/ShikihoPanel.test.tsx`

Expected: FAIL because `ShikihoPanel` does not exist.

- [ ] **Step 3: Implement the compact panel**

Use one bordered section inside the existing header surface, not a nested `Surface`. The header is one row containing `Company Shikiho`, status, capture/edition text, a mobile-safe source link, and an `aria-expanded` collapse button. The body is `grid gap-3 lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]`; use simple dividers, headings, and chips rather than nested cards.

Render primary content (features, consolidated businesses, commentary) left and secondary content (score, industries, themes, comparison companies, profile) right. Omit empty optional sections. Comparison entries without a valid code render as text, not buttons.

- [ ] **Step 4: Integrate the hook without touching panel-order state**

Call `useShikihoSnapshot(selectedSymbol)` once in `SymbolWorkbenchPage`. Pass its result and existing `handleSelectSymbol` through `ChartHeader`, then render `ShikihoPanel` immediately after `DailyRankingSnapshot` and before provenance warnings in `SymbolWorkbenchHeader.tsx`.

Do not modify `DailyRankingSnapshot.tsx`, `chartStore.ts`, or `SymbolWorkbenchPanels.tsx`. Do not stage the pre-existing dirty `SymbolWorkbenchPage.test.tsx`.

- [ ] **Step 5: Run focused and current Workbench tests**

```bash
cd apps/ts
bun run --filter @trading25/web test -- \
  src/hooks/useShikihoSnapshot.test.tsx \
  src/components/SymbolWorkbench/ShikihoPanel.test.tsx \
  src/pages/SymbolWorkbenchPage.test.tsx
bun run --filter @trading25/web typecheck
```

Expected: PASS, including the user's current uncommitted Symbol Workbench test changes.

- [ ] **Step 6: Commit the UI integration only**

```bash
git add apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx \
  apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx \
  apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx \
  apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx
git commit -m "feat(web): show captured Shikiho context in workbench"
```

### Task 6: Document installation and complete automated/live verification

**Files:**
- Create: `apps/ts/packages/shikiho-extension/README.md`
- Modify: `README.md`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: built `apps/ts/packages/shikiho-extension/dist` extension and the completed Workbench integration.
- Produces: reproducible Atlas installation, rebuild, privacy, and troubleshooting instructions.

- [ ] **Step 1: Write exact operator instructions**

Document:

```text
cd apps/ts
bun run --filter @trading25/shikiho-extension build

Atlas -> Settings -> Web browsing -> Extensions -> Manage extensions
Enable Developer mode -> Load unpacked
Select apps/ts/packages/shikiho-extension/dist
```

Explain automatic capture, allowed origins, no-cookie permission, latest-200 retention, rebuilding/reloading after changes, and `Extension unavailable`, `Login required`, `Page changed`, and `Partial capture` states.

- [ ] **Step 2: Run the complete automated gate**

```bash
cd apps/ts
bun run --filter @trading25/shikiho-extension test
bun run --filter @trading25/shikiho-extension typecheck
bun run --filter @trading25/shikiho-extension build
bun run --filter @trading25/web test -- src/hooks/useShikihoSnapshot.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx
bun run --filter @trading25/web typecheck
bun run quality:deps:audit
bun run quality:lint
bun run workspace:test
bun run workspace:build
```

Expected: every command PASS. If an unrelated pre-existing dirty Ranking test fails, prove whether the same failure exists without this branch before classifying it; do not weaken the Shikiho tests.

- [ ] **Step 3: Perform live Atlas acceptance with 7203**

Build and load the unpacked extension. Start Trading25 Web and open `/symbol-workbench?symbol=7203`. Verify the uncaptured state, use the existing Shikiho link, allow the authenticated page to settle, and confirm the Workbench panel updates automatically without a capture click. Compare features, commentary, score, edition/update date, comparison companies, source URL, and capture time to the visible page.

Navigate to a second Shikiho symbol and confirm separate storage. Use a comparison-company button in Workbench and confirm route-backed symbol isolation. Sign out or use the login fixture to confirm a newer failure preserves the last valid capture as stale.

- [ ] **Step 4: Review the full diff against the design**

Run:

```bash
git diff HEAD~5..HEAD --check
git status --short
rg -n "fetch\(|XMLHttpRequest|dangerouslySetInnerHTML|permissions.*cookies" apps/ts/packages/shikiho-extension apps/ts/packages/web/src/components/SymbolWorkbench apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts
```

Expected: no whitespace errors; no Shikiho network call, raw-HTML rendering, or cookie permission; unrelated user changes remain unstaged and unchanged.

- [ ] **Step 5: Commit documentation**

```bash
git add README.md AGENTS.md apps/ts/packages/shikiho-extension/README.md
git commit -m "docs: explain local Shikiho extension setup"
```

- [ ] **Step 6: Final completion audit**

Check every acceptance criterion in `docs/superpowers/specs/2026-07-10-symbol-workbench-shikiho-bridge-design.md` against the built manifest, tests, current source, and live Atlas result. Do not claim completion while any criterion lacks direct evidence.
