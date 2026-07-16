# Final Shikiho Review Fix Report

## Scope

- Moved the capture phase badge and `取得診断` trigger from the clipped left metadata zone into the protected right action zone.
- Kept expanded diagnostic details immediately below the two-zone header grid.
- Added `earningsAnnouncementDate` to acquisition-level synthetic trace `missingFields`.
- Added the `決算予定日` capture milestone label to expanded diagnostics.
- Base commit: `cbeb3b35`.

## TDD evidence

### 1. Protected diagnostics controls

RED test contract:

- phase badge and diagnostics trigger must be outside `shikiho-header-left`;
- both must be inside `shikiho-header-right`;
- expanded `Tab探索` details must be outside both header zones.

After correcting an unsupported matcher in the test itself, the focused RED failed on the intended layout assertion:

```text
expected true to be false
apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx:190
```

GREEN result after moving the controlled diagnostics trigger:

```text
Test Files  1 passed (1)
Tests       28 passed (28)
```

### 2. Synthetic trace field parity

RED added a focused assertion that an acquisition timeout trace includes `earningsAnnouncementDate` in `dom.missingFields`:

```text
Expected to contain: "earningsAnnouncementDate"
Received: ["identity", ..., "editionLabel", "pageUpdatedAt", "coreReady"]
36 pass, 1 fail
```

GREEN after extending `tab-acquisition.ts` `TRACE_FIELDS`:

```text
37 pass
0 fail
```

### 3. Earnings milestone label

RED required expanded diagnostics to render `決算予定日`:

```text
Unable to find an element with the text: 決算予定日
1 passed, 1 failed
```

GREEN after adding the typed milestone label:

```text
Test Files  1 passed (1)
Tests       2 passed (2)
```

## Final validation

Commands and results:

```text
bun run --filter @trading25/web test -- src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.test.tsx
2 files / 30 tests passed

bun test src/tab-acquisition.test.ts
37 tests passed / 184 assertions / 0 failures

bun run --filter @trading25/web typecheck
exit 0

bun run --filter @trading25/shikiho-extension typecheck
exit 0

bunx biome check packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.tsx packages/web/src/components/SymbolWorkbench/ShikihoCaptureDiagnostics.test.tsx extensions/shikiho/src/tab-acquisition.ts extensions/shikiho/src/tab-acquisition.test.ts
Checked 6 files. No fixes applied.

git diff --check
exit 0
```

## Self-review

- The header remains one row with the source, refresh, collapse, date badge, and metadata behavior preserved.
- The right action zone owns the phase/diagnostics controls; the left zone remains the only clipped zone.
- Expanded details remain outside both header zones and retain the existing `aria-controls` disclosure relationship.
- Synthetic timeout/error traces now use the same earnings milestone field represented by the trace contract and progressive capture path.
- No API, persistence, privacy, or market-data source-of-truth behavior changed.
- No material findings remain in the scoped diff.

## Commit

- Scoped commit message: `fix(shikiho): address final diagnostics review`
- This report is included in that commit; the resulting hash is recorded in the final task handoff because a commit cannot embed its own hash.
