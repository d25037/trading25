# Task 3 Report: Header cleanup and no-wrap layout

## Scope

- Removed the duplicate header-level exact-name `四季報` button from Symbol Workbench.
- Retained panel-local `四季報で開く`, `会社四季報を更新`, diagnostics, earnings-date badge, and collapse controls.
- Converted the Shikiho panel header to a single-row, two-zone grid: truncating metadata on the left and non-wrapping actions on the right.

## TDD evidence

### RED

Added assertions before production changes for:

1. The header-level exact-name `四季報` button being absent.
2. The panel-local `四季報で開く` link remaining available.
3. Separate left/right header groups exposing non-wrapping layout classes.

The brief's raw `bun test ...` command did not load this package's Vitest/jsdom setup and failed before test execution with `window is not defined` / `document is not defined`. Running the same focused files through the package test script produced the intended RED result:

```text
bun run test src/pages/SymbolWorkbenchPage.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx
2 failed | 52 passed
```

The two expected failures were the still-present exact-name button and the missing left/right header zones.

### GREEN

After the minimal implementation, the same focused Vitest command passed:

```text
Test Files  2 passed (2)
Tests       54 passed (54)
```

## Implementation details

- Deleted the stale `BookOpen` import, `openCompanyPage` helper, header button, and `window.open` test mock.
- Replaced the wrapping panel header with `grid-cols-[minmax(0,1fr)_auto]`.
- Added a `flex-nowrap` left metadata zone with overflow clipping and truncation for edition/capture metadata.
- Added a `flex-nowrap whitespace-nowrap` right action zone for source, refresh, and collapse controls.
- Removed the obsolete `ml-auto` from the collapse control because right-zone placement now owns alignment.

## Self-review

- Diff is limited to the four Task 3 implementation/test files plus this requested report.
- No backend, OpenAPI, extension contract, state, or data-flow changes were introduced.
- Existing focused tests continue to cover refresh, disabled refresh state, collapse/reset behavior, diagnostics input, source fallback normalization, and earnings-date display.
- No material correctness, accessibility, or source-of-truth findings remain in the scoped diff.

## Validation

- Focused Vitest: `bun run test src/pages/SymbolWorkbenchPage.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Web typecheck: `bun run typecheck`
- Focused Biome: `bunx biome check packages/web/src/pages/SymbolWorkbenchHeader.tsx packages/web/src/pages/SymbolWorkbenchPage.test.tsx packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Whitespace validation: `git diff --check`

## Fix Review: expanded diagnostics placement

### Review finding

The first two-zone implementation placed the fragment returned by `ShikihoCaptureDiagnostics` inside the left `flex-nowrap overflow-hidden` zone. Its trigger stayed in the header, but its expanded `basis-full` details remained a sibling in that same clipped flex row, so diagnostic content could not render below the header as intended.

### Regression test and RED

Added a panel-level test that clicks `取得診断`, confirms `aria-expanded="true"`, confirms the detail content is not hidden, and requires `Tab探索` to be outside `shikiho-header-left`. Before the fix, the focused panel run failed with:

```text
expected true to be false
```

The failing value was `shikiho-header-left.contains(detailLabel)`.

### Fix

- Split `ShikihoCaptureDiagnostics` into reusable controlled trigger and details components while preserving the original self-contained component API and tests.
- Kept the phase badge and `取得診断` trigger in the single-row left header zone.
- Rendered the controlled details block immediately below the two-zone header grid, outside the clipped metadata container.
- Preserved the trigger/details `aria-controls` relationship and the existing disclosure behavior.

### Fix validation

- Diagnostics + Panel unit tests: 30/30 passed.
- Diagnostics + Panel + Page focused tests: 57/57 passed.
- Web typecheck: passed.
- Focused Biome: passed after formatter application.
- `git diff --check`: passed.
