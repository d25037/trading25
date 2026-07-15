# Symbol Workbench Shikiho Layout Refinement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a two-row Company Shikiho header, a prominent earnings date, and a compact three-row score card in the panel's desktop aside.

**Architecture:** Keep acquisition data and state unchanged. Recompose `ShikihoPanel` into a primary header row plus metadata row, and recompose `SnapshotBody` into primary content plus a right-side aside that owns the score and optional secondary content.

**Tech Stack:** TypeScript, React 19, Vitest, Testing Library, Tailwind CSS v4, inline SVG.

## Global Constraints

- Modify only the web Shikiho panel, score card, and their tests.
- Preserve extension contracts, acquisition logic, urgency thresholds, diagnostics, source link, refresh, collapse, and disclosure content.
- Add no dependencies and no backend, OpenAPI, database, extension, or storage changes.
- Keep the earnings badge non-wrapping and the metadata row readable at approximately 1180px desktop width.
- Keep the radar accessible and omit industry median data.

---

### Task 1: Two-row panel header and prominent earnings date

**Files:**
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Test: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`

**Interfaces:**
- Preserves: `EarningsAnnouncementBadge({ date }: { date: string | null })` and all action accessible names.
- Produces: `data-testid="shikiho-header-primary"` and `data-testid="shikiho-header-meta"` layout zones.

- [ ] **Step 1: Write the failing header tests**

```tsx
expect(screen.getByTestId('shikiho-header-primary')).toContainElement(
  screen.getByLabelText('決算発表予定日 2026年7月18日 あと3日')
);
expect(screen.getByTestId('shikiho-header-meta')).toHaveTextContent('2026年3集');
expect(screen.getByTestId('shikiho-header-meta')).toHaveTextContent('取得 2026/07/10');
expect(screen.getByTestId('shikiho-header-meta')).toContainElement(screen.getByRole('button', { name: '取得診断' }));
expect(screen.getByLabelText('決算発表予定日 2026年7月18日 あと3日')).toHaveClass(
  'whitespace-nowrap',
  'text-sm'
);
```

- [ ] **Step 2: Run the tests and verify RED**

Run from `apps/ts/packages/web`:

```bash
bun run test src/components/SymbolWorkbench/ShikihoPanel.test.tsx
```

Expected: FAIL because the two new header test IDs do not exist and the badge lacks the larger text class.

- [ ] **Step 3: Implement the minimal two-row header**

```tsx
<div data-testid="shikiho-header-primary" className="flex min-w-0 items-center justify-between gap-3">
  <div className="flex min-w-0 items-center gap-2">
    <h3 className="shrink-0 text-sm font-semibold text-foreground">会社四季報</h3>
    <StatusBadge {...statusProps} />
    <EarningsAnnouncementBadge date={snapshot?.earningsAnnouncementDate ?? null} />
  </div>
  <div className="flex shrink-0 items-center gap-1 whitespace-nowrap">{actions}</div>
</div>
<div data-testid="shikiho-header-meta" className="mt-1 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
  <EditionMeta snapshot={canonicalSnapshot} />
  <StatusMeta snapshot={canonicalSnapshot} diagnostic={diagnostic} />
  {trace ? <ShikihoCaptureDiagnosticsTrigger {...diagnosticTriggerProps} /> : null}
</div>
```

Give the badge `px-2.5 py-1 text-sm`, render the date portion with `font-bold tabular-nums`, and keep the remaining-day copy visible.

- [ ] **Step 4: Run the focused test and verify GREEN**

```bash
bun run test src/components/SymbolWorkbench/ShikihoPanel.test.tsx
```

Expected: 28 or more tests pass with no failures.

- [ ] **Step 5: Commit the header slice**

```bash
git add apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx
git commit -m "fix(web): clarify Shikiho panel header"
```

### Task 2: Desktop primary/aside body and three-row score card

**Files:**
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Test: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoScoreCard.tsx`
- Test: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx`

**Interfaces:**
- Produces: `data-testid="shikiho-aside"` containing score first and optional `SecondaryContent` second.
- Preserves: `ShikihoScoreCard({ score }: { score: ShikihoSnapshotV1['score'] })`.

- [ ] **Step 1: Write the failing body and score tests**

```tsx
expect(screen.getByTestId('shikiho-body')).toHaveClass(
  'lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]'
);
expect(screen.getByTestId('shikiho-body').firstElementChild).toBe(screen.getByTestId('shikiho-primary'));
expect(screen.getByTestId('shikiho-aside').firstElementChild).toBe(screen.getByTestId('shikiho-score-card'));
expect(screen.getByTestId('shikiho-score-card')).not.toHaveClass('col-span-full');
expect(screen.getByTestId('shikiho-score-body')).toHaveClass('flex-col');
expect(screen.getByTestId('shikiho-score-body')).not.toHaveClass(
  'md:grid-cols-[minmax(220px,260px)_minmax(0,1fr)]'
);
```

- [ ] **Step 2: Run the tests and verify RED**

Run from `apps/ts/packages/web`:

```bash
bun run test src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx
```

Expected: FAIL because the score is still the first full-width body child and uses the desktop horizontal score grid.

- [ ] **Step 3: Implement the primary/aside body**

```tsx
const hasPrimary = hasPrimaryContent(snapshot);
const hasAside = hasScore || hasSecondaryContent(snapshot);
const twoColumn = hasPrimary && hasAside;

<div className={cn('grid min-w-0 gap-3', twoColumn && 'lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]')}>
  <PrimaryContent snapshot={snapshot} divided={twoColumn} />
  {hasAside ? (
    <aside data-testid="shikiho-aside" className="min-w-0 space-y-3">
      {hasScore ? <ShikihoScoreCard score={snapshot.score} /> : null}
      <SecondaryContent snapshot={snapshot} onSelectSymbol={onSelectSymbol} />
    </aside>
  ) : null}
</div>
```

- [ ] **Step 4: Implement the vertical score card**

Remove `col-span-full`, make the card header compact, and use:

```tsx
<div data-testid="shikiho-score-body" className="mt-3 flex flex-col gap-3">
  {completeValues ? <ScoreRadar values={completeValues} label={`四季報スコア ${radarLabel}`} /> : null}
  <dl data-testid="shikiho-score-values" className="grid grid-cols-2 gap-x-4 text-xs">{metricRows}</dl>
</div>
```

Limit the radar to the aside with `max-w-[230px]` and retain all axis labels, vertices, accessible text, missing-axis behavior, stars, and numeric values.

- [ ] **Step 5: Run focused tests and verify GREEN**

```bash
bun run test src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx
```

Expected: all Shikiho panel and score tests pass.

- [ ] **Step 6: Commit the body slice**

```bash
git add apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoScoreCard.tsx apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx
git commit -m "fix(web): compact Shikiho score layout"
```

### Task 3: Verification and live acceptance

**Files:**
- Verify only; modify the four Task 1/2 files only if a failing check identifies a scoped defect.

**Interfaces:**
- Verifies: the complete rendered panel for `6737` without changing acquisition data.

- [ ] **Step 1: Run focused formatting and type checks**

```bash
cd apps/ts/packages/web
bunx biome check src/components/SymbolWorkbench/ShikihoPanel.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx src/components/SymbolWorkbench/ShikihoScoreCard.tsx src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx
bun run typecheck
```

Expected: both commands exit 0.

- [ ] **Step 2: Run the full web tests and build**

```bash
cd apps/ts/packages/web
bun run test
bun run build
```

Expected: all tests pass and Vite reports a successful production build.

- [ ] **Step 3: Run workspace gates**

```bash
cd apps/ts
bun run quality:lint
bun run quality:deps:audit
git diff --check
```

Expected: all commands exit 0; existing unrelated Biome informational output may remain unchanged.

- [ ] **Step 4: Verify Chrome at `6737`**

Confirm at approximately 1180px content width:

- the header first row shows title, status, prominent `2026/07/31 · あと15日`, and actions;
- the second row shows the edition, full `取得 2026/07/16 2:57` timestamp, and `取得完了` duration;
- primary disclosure content is left and the score card is right;
- the score card is vertically ordered as header, radar, then six values;
- refresh, source link, diagnostics, and collapse remain operable.

- [ ] **Step 5: Review the scoped diff and status**

```bash
git diff --check
git status --short
git log -3 --oneline
```

Expected: no unstaged implementation changes remain after the scoped commits.
