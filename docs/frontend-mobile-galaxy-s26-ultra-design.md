# Frontend mobile design note: Galaxy S26 Ultra

Date: 2026-04-26

## Why this note exists

The current web frontend is usable on narrow viewports, but most primary workflows are still desktop-first: split panes, sticky sidebars, dense tables, and large fixed-height chart panels. The Galaxy S26 Ultra is large for a phone, but it is still a one-column touch device in portrait. Treating it like a small desktop will make symbol analysis and backtest operations feel cramped.

This note defines a mobile target and design direction for optimizing `apps/ts/packages/web` without losing the desktop analyst workflow.

## Device target

Primary reference device: **Samsung Galaxy S26 Ultra**.

Public specs checked:

- Samsung official product page exists for Galaxy S26 Ultra: <https://www.samsung.com/us/smartphones/galaxy-s26-ultra/>
- GSMArena review reports a **6.9-inch LTPO OLED** panel with **1440 × 3120 px** resolution and up to **120 Hz** refresh: <https://www.gsmarena.com/samsung_galaxy_s26_ultra-review-2939p3.php>
- Other spec-index/search sources consistently report **6.9 in / 3120 × 1440 / 19.5:9**.

Practical CSS target:

- Physical pixels: `1440 × 3120` portrait.
- Expected Android/Chrome CSS viewport: roughly **411–412 CSS px wide** at high DPR.
- Height varies with browser chrome and install mode; design for **~840–915 CSS px visible height**.
- Do not hard-code to one phone. Use S26 Ultra as the high-end Android portrait benchmark and keep the range **360–430 CSS px width** healthy.

## Current frontend constraints

Observed patterns in `apps/ts/packages/web/src`:

- Main pages use `SplitLayout`, `SplitSidebar`, and `SplitMain` with `lg:flex-row` desktop layouts.
- On mobile, many pages collapse to `flex-col`, but the sidebar remains a full-width block above the content.
- Tables and virtualized lists are central to Ranking / Screening / Portfolio / Watchlist flows.
- Symbol Workbench has a large primary chart (`min-h-[34rem]`) plus many sub-panels; this is expensive in vertical space.
- Settings and control panels often use desktop-like density and long dialogs.

This means the first mobile problem is not simply breakpoint support. It is **task prioritization**: on a phone, the user needs one active task surface at a time.

## Design principles for S26 Ultra optimization

### 1. Mobile is a dedicated shell, not only `flex-col`

At `max-width: 430px`, use a mobile shell that separates:

1. top app/navigation affordance,
2. current page controls,
3. main content,
4. optional secondary panels.

Avoid rendering desktop sidebars as large full-width cards above the main result. They push the actual chart/table below the fold.

Recommended pattern:

- Desktop: keep `SplitSidebar + SplitMain`.
- Mobile: transform sidebars into one of:
  - a sticky compact filter bar,
  - a bottom sheet / drawer,
  - segmented tabs for page modes,
  - an inline collapsed accordion only when controls are rarely used.

### 2. Optimize for 412 px width first

Use `412px` as the primary QA viewport.

Suggested viewport set:

- `360 × 800`: small Android baseline.
- `390 × 844`: common iPhone-like baseline.
- `412 × 892`: Galaxy S26 Ultra CSS-width approximation.
- `430 × 932`: large Android/iPhone Max class.
- `915 × 412`: S26 Ultra landscape approximation for chart-heavy flows.

Acceptance goal: no horizontal page scroll outside intentionally scrollable tables/charts.

### 3. Preserve analyst density, but make disclosure explicit

Trading/analysis UIs need dense information. Do not simply make everything huge.

Instead:

- Use compact metric cards in 2 columns where values are short.
- Collapse secondary metadata under `Details` / `Diagnostics` accordions.
- Keep primary decision data visible: symbol, latest price/date, key signal state, selected timeframe.
- Use progressive disclosure for API diagnostics, provenance, warnings, and advanced settings.

### 4. Charts need mobile-specific interaction contracts

For chart panels:

- Reserve most vertical space for the active chart; controls should be compact and sticky above it.
- Prefer one visible chart/panel at a time on portrait mobile.
- Provide a chart/panel picker instead of stacking every enabled sub-chart vertically.
- Use horizontal swipe or segmented tabs for `Primary`, `PPO`, `Risk`, `Volume`, `Fundamentals`, etc.
- Keep tooltips finger-friendly and avoid hover-only interactions.
- Use `touch-action` intentionally: chart pan/zoom must not fight page scroll.

For Galaxy S26 Ultra portrait, a good chart target is:

- Header/controls: 80–120 px.
- Active chart viewport: 420–560 px.
- Below-chart summary/actions: remaining space.

The current `min-h-[34rem]` primary chart is close to a full screen by itself on mobile; that is acceptable only if the page intentionally becomes a chart-first screen.

### 5. Tables need card or hybrid views

Dense financial tables are the hardest mobile surface.

Recommended approach:

- Keep table semantics on desktop.
- On mobile, use either:
  - card rows with top 3–5 fields, expandable details, or
  - a horizontal-scroll table with sticky first column and obvious scroll affordance.
- For Ranking / Screening, default mobile row content should include:
  - symbol/code and company name,
  - primary score/rank,
  - latest return/performance metric,
  - one risk or liquidity metric,
  - action affordance to open details/workbench.

Avoid forcing users to horizontally scan 10+ columns on a 412 px viewport.

### 6. Dialogs should become sheets on mobile

Current dialogs are fine on desktop, but on phone:

- Use full-screen or near-full-screen sheets for settings/editors.
- Keep action buttons sticky at the bottom.
- Ensure form fields are at least 44 px touch targets.
- Avoid nested scrolling inside a dialog unless the outer page is locked.

For Symbol Workbench settings, the recent unification of panel/sub-chart settings into Panel Layout is a good direction. On mobile, make that layout a bottom sheet with per-panel accordions.

### 7. Use safe-area and browser chrome aware spacing

Add mobile shell spacing with modern viewport units:

- Prefer `100dvh` over `100vh` for full-height mobile panels.
- Add `env(safe-area-inset-bottom)` to bottom nav/sheets.
- Keep primary bottom actions above the gesture navigation area.

### 8. Maintain desktop behavior by introducing mobile-only components gradually

Do not rewrite every page at once. Add reusable primitives and migrate page by page.

Candidate primitives:

- `MobilePageShell`
- `MobileControlSheet`
- `MobileSegmentedSurface`
- `ResponsiveDataView` (`table` desktop, `cards` mobile)
- `ChartPanelPager`
- `StickyMobileActionBar`

## Page-specific recommendations

### Symbol Workbench

Highest priority. It is the most likely mobile verification page for the current workflow.

Recommended mobile layout:

1. Sticky compact header:
   - selected symbol,
   - timeframe selector,
   - refresh/status action,
   - settings button.
2. Active panel pager:
   - `Primary`, enabled sub-charts, fundamentals panels.
   - One panel visible at a time in portrait.
3. Bottom sheet for settings:
   - symbol search,
   - chart settings,
   - panel layout/order,
   - signal overlay.
4. Convert chart/panel stack into an ordered tab/pager using existing `workbenchPanelOrder`.

Important: keep `workbenchPanelOrder` as the source of truth. Mobile should change presentation, not duplicate settings state.

### Ranking / Screening

Recommended mobile layout:

- Top sticky summary + filter button.
- Filter/sidebar as full-screen sheet.
- Results as mobile cards by default.
- Keep a “table mode” toggle for power users.
- Sticky sort selector for common ranking dimensions.

### Backtest

Recommended mobile layout:

- Split the workflow into steps: `Strategy`, `Config`, `Run`, `Results`, `Artifacts`.
- Use tabs/stepper instead of multi-column panels.
- YAML/editor surfaces should open full-screen and use monospace with a minimum 14 px font.

### Portfolio / Watchlist

Recommended mobile layout:

- List/detail as separate screens or nested route state.
- Summary metric cards first.
- Holdings/positions as cards with expandable metrics.

### Settings

Recommended mobile layout:

- Group settings into accordions.
- Avoid 2–3 column grids below `sm`; keep one column with compact vertical rhythm.
- Put destructive/admin actions behind confirmation sheets.

## Implementation plan

### Phase 1: Baseline and guardrails

- Add Playwright/mobile viewport presets for `412 × 892` and `360 × 800`.
- Add visual smoke checks for main routes: Symbol Workbench, Ranking, Screening, Backtest, Portfolio.
- Add a “no unintended body horizontal scroll” assertion.
- Audit fixed heights above `400px` on mobile.

### Phase 2: Mobile shell primitives

- Introduce reusable mobile shell/sheet primitives in `components/Layout`.
- Make `SplitLayout` consumers opt into a mobile sidebar mode.
- Convert dialogs to full-screen sheets at `max-width: 430px`.

### Phase 3: Symbol Workbench mobile rewrite

- Add mobile-only `ChartPanelPager` using `workbenchPanelOrder`.
- Move ChartControls into a sheet on mobile.
- Keep primary chart/panel height dynamic with `dvh`.
- Verify FastAPI/Vite restart after code changes before mobile QA.

### Phase 4: Data-heavy pages

- Add `ResponsiveDataView` and migrate Ranking/Screening tables.
- Preserve desktop tables and power-user density.
- Add mobile card snapshots/tests.

## CSS/Tailwind guidance

Suggested breakpoint semantics:

- Default styles should work at 360–430 px.
- `sm` is still phone/large-phone adjacent; do not assume tablet.
- Use `md` for wider phone landscape / small tablet improvements.
- Use `lg` for desktop split layouts.

Useful patterns:

```tsx
// mobile-first full-height shell
<div className="flex min-h-[100dvh] flex-col overflow-hidden lg:min-h-0">
  <header className="shrink-0" />
  <main className="min-h-0 flex-1 overflow-y-auto" />
</div>
```

```tsx
// mobile sheet padding safe for gesture nav
<div className="pb-[calc(env(safe-area-inset-bottom)+1rem)]">
  ...
</div>
```

```tsx
// desktop split, mobile control sheet trigger
<aside className="hidden lg:block lg:w-[18rem]">...</aside>
<Button className="lg:hidden">Filters / Settings</Button>
```

## Definition of done for S26 Ultra mobile support

A page is S26 Ultra-ready when:

- It works at `412 × 892` without unintended horizontal page scroll.
- Primary task content appears above or near the first fold.
- Touch targets are at least ~44 px where practical.
- Dialogs/sheets can be completed with one thumb and do not trap nested scroll awkwardly.
- Tables either become cards or have an explicit horizontal-scroll design.
- Charts support touch interaction without breaking vertical page scroll.
- Desktop `lg+` layout remains unchanged unless intentionally improved.

## Recommendation

Start with **Symbol Workbench**. It already has the strongest need for phone verification, and the recent panel/sub-chart unification gives a clean state model for a mobile panel pager. After that, apply the same shell/sheet patterns to Ranking and Screening.
