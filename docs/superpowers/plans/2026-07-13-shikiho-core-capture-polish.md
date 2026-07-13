# Shikiho Core Capture Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the current authenticated Shikiho page produce distinct core fields and a Japanese, non-redundant `取得済み` Workbench presentation.

**Architecture:** Keep the existing snapshot contract. Tighten semantic label selection in the DOM extractor, derive capture status only from the three core fields, and adjust the existing React panel copy without changing its layout or bridge flow.

**Tech Stack:** TypeScript, Bun test, React 19, Testing Library, Biome

## Global Constraints

- The three core fields are `features`, `consolidatedBusinesses`, and at least one `commentary` item.
- Missing optional fields must not downgrade a snapshot from `captured` to `partial`.
- Nested-span `dt` labels must resolve to their own adjacent `dd` values.
- The panel header is exactly `会社四季報`, complete status is exactly `取得済み`, and commentary has no redundant internal `会社四季報` heading.
- Preserve the existing extension contract, privacy boundary, 24-hour cache, refresh behavior, and panel layout.

---

### Task 1: Correct core extraction and status

**Files:**
- Modify: `apps/ts/packages/shikiho-extension/src/extractor.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/extractor.test.ts`
- Modify: `apps/ts/packages/shikiho-extension/src/fixtures/7203-current-authenticated.html`

**Interfaces:**
- Consumes: `findExactLabel(root, label)` and `extractShikihoPage(document, location, now, extractorVersion)`.
- Produces: unchanged `ShikihoSnapshotV1`, with `status` based on the three core fields.

- [ ] **Step 1: Write failing tests**

Add a test whose fixture uses semantic `dt` labels split across nested spans and asserts that `features` and `consolidatedBusinesses` equal their distinct adjacent `dd` text. Add assertions that the same core-complete fixture is `captured` even though optional fields appear in `missingFields`. Add a core-missing case that remains `partial`.

- [ ] **Step 2: Run tests and confirm RED**

Run: `bun test src/extractor.test.ts`

Expected: the nested semantic label test returns a shared `dl` value and the optional-field status assertion receives `partial`.

- [ ] **Step 3: Implement the minimal extractor change**

In `findExactLabel`, immediately accept an exact visible match from the semantic selector pass; retain the child-repeat guard only for the wildcard fallback pass. Derive `status` from `features !== null && consolidatedBusinesses !== null && commentary.length > 0`, while retaining all optional names in `missingFields`.

- [ ] **Step 4: Run focused tests and confirm GREEN**

Run: `bun test src/extractor.test.ts`

Expected: all extractor tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/ts/packages/shikiho-extension/src/extractor.ts apps/ts/packages/shikiho-extension/src/extractor.test.ts apps/ts/packages/shikiho-extension/src/fixtures/7203-current-authenticated.html
git commit -m "fix(extension): classify core Shikiho capture"
```

### Task 2: Polish Japanese panel copy

**Files:**
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeaderShikiho.test.tsx`

**Interfaces:**
- Consumes: unchanged `ShikihoPanelProps` and `ShikihoCaptureState`.
- Produces: the same panel with Japanese title and direct commentary rendering.

- [ ] **Step 1: Write failing UI tests**

Assert the region and visible heading are `会社四季報`, the captured badge is `取得済み`, and only one `会社四季報` text occurrence exists when commentary is present. Update the header integration mock to use the same Japanese label.

- [ ] **Step 2: Run tests and confirm RED**

Run: `bun test src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/pages/SymbolWorkbenchHeaderShikiho.test.tsx`

Expected: tests fail on the English panel title and duplicate commentary heading.

- [ ] **Step 3: Implement the minimal React change**

Set the panel `aria-label` and `h3` text to `会社四季報`. Render the commentary list in a plain spaced container rather than `Section title="会社四季報"`. Do not alter state, hooks, data fetching, or layout calculation.

- [ ] **Step 4: Run focused tests and confirm GREEN**

Run: `bun test src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/pages/SymbolWorkbenchHeaderShikiho.test.tsx`

Expected: all focused web tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchHeaderShikiho.test.tsx
git commit -m "fix(web): polish Shikiho capture panel"
```

### Task 3: Verify the integrated extension and web build

**Files:**
- Modify only if verification exposes a scoped defect.

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: rebuilt extension output and evidence for live acceptance.

- [ ] **Step 1: Run extension tests and build**

Run: `bun run --filter @trading25/shikiho-extension test && bun run --filter @trading25/shikiho-extension build`

Expected: all tests pass and `dist/` builds successfully.

- [ ] **Step 2: Run web tests, typecheck, and lint**

Run: `bun test apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchHeaderShikiho.test.tsx && bun run quality:typecheck && bun run quality:lint`

Expected: tests and typecheck pass; lint has no new error.

- [ ] **Step 3: Run workspace regression tests**

Run: `bun run workspace:test`

Expected: all workspace tests pass.

- [ ] **Step 4: Perform live acceptance**

Reload the unpacked extension, refresh Symbol Workbench for `7203`, and confirm distinct `特色` / `連結事業`, status `取得済み`, title `会社四季報`, no second internal title, and inactive Shikiho tab closure.
