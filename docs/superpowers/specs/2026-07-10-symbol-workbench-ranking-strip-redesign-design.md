# Symbol Workbench Ranking Strip Redesign

## Summary

Replace the large card-grid `Daily Ranking Snapshot` in Symbol Workbench with the approved **D: two-row micro strip**. The ranking data and presentation semantics remain unchanged; only the Workbench composition becomes substantially denser.

This design supersedes the `Header Layout` portion of `2026-07-10-symbol-workbench-daily-ranking-snapshot-design.md`. Daily Ranking remains the source of truth for metric order, labels, formatting, evidence colors, Regime, and Signals.

## Problem

At the normal desktop working width, the current snapshot occupies roughly 440–480 px. Its separate basic-information grid, repeated rounded metric cards, and full-width Regime and Signals rows push the primary chart below the first viewport. Daily Ranking presents the same information more efficiently with table-like cells and subtle separators.

## Goals

- Keep every currently exposed ranking parameter visible without an expand action.
- Reduce the ranking area to approximately 92–108 px, excluding the existing page controls above it.
- Make the Daily Chart visible much earlier in the first viewport.
- Remove card styling and unnecessary headings rather than shrinking text until it becomes unreadable.
- Preserve the shared Daily Ranking presentation layer and all loading, error, retry, and unavailable behavior.

## Approved Composition

### Identity line

Keep the existing symbol code and company name as the primary identity. Move the compact classification summary into a single secondary meta line immediately below or beside that identity:

- Market
- Index membership
- Sector 17
- Sector 33
- Market capitalization
- Free-float market capitalization
- Snapshot date

All seven values remain visibly available; none may be moved to a tooltip or disclosure. The meta line may wrap on narrow screens, but it must not create a separate overview panel or field-card grid.

### Two-row metric strip

Render the snapshot as a semantic definition list with seven columns at the normal desktop width. Cells use only a thin left divider and compact horizontal padding. Do not use card backgrounds, rounded metric tiles, or shadows.

Row 1:

1. Current Price
2. Change Percentage
3. PER
4. Forward PER
5. Forward OP / OP
6. PBR
7. Value Score

Row 2:

1. Sector Strength
2. PSR / Forward PSR
3. Liquidity Z
4. Trading Value
5. SMA5 5D
6. Regime
7. Signals

PSR and Forward PSR may share one cell because they are a directly comparable pair. Their formatter and evidence colors still come from the shared Daily Ranking presentation definitions.

### Typography and spacing

- Labels: 9–10 px, muted, single line where possible.
- Values: 12–14 px, semibold/bold, tabular numerals for numeric metrics.
- Vertical cell padding: approximately 2–4 px.
- Strip padding: approximately 6–8 px.
- Classification/date text: 10–11 px, visually secondary, with compact separators between values.
- Preserve the existing semantic evidence colors; do not introduce new color rules.

### Container model

The ranking strip is part of the existing Symbol Workbench header, not a nested card. It may use one top or bottom divider to separate it from controls and the chart, but it must not add a new rounded outer panel.

Remove the visible `Daily Ranking Snapshot` heading when the relationship is already clear from the metric labels and date. Provide an accessible label on the section so screen-reader context is retained.

## Responsive Behavior

- Normal desktop: seven columns and two metric rows.
- Narrow desktop/tablet: allow four columns and wrapping while preserving metric order.
- Mobile: two columns; classification metadata may wrap under the identity line.
- Never require horizontal scrolling in the approved D design.
- Do not truncate numeric values. Signals may wrap within their cell when necessary.

## Shared Source of Truth

Continue using `DAILY_RANKING_VALUE_METRICS`, `DailyRankingMetricValue`, `DailyRankingRegimeChip`, and `DailyRankingSignalChips` from the shared Daily Ranking presentation module.

The Workbench component may define only layout groupings such as the two row composition and the paired PSR cell. It must not duplicate formatters, labels, evidence-color thresholds, Regime mappings, or Signal mappings.

## States and Accessibility

- Keep the current loading, unavailable, and retry-capable error states inside the same compact region.
- Preserve `aria-live` behavior and existing test IDs used by Symbol Workbench tests.
- Use `<dl>`, `<dt>`, and `<dd>` for metric semantics.
- Give the retry control a visible keyboard focus state.
- Keep unavailable values in their stable positions and render `-`.

## Testing and Visual Acceptance

- Existing shared-formatting and Daily Ranking tests remain unchanged.
- Update layout assertions that currently require Regime and Signals to span full rows.
- Add or update Symbol Workbench tests for the paired PSR cell and retained accessibility states.
- Verify at approximately 1180 × 800 that the Daily Chart heading is visible in the initial viewport for a populated symbol.
- Verify the selected symbol against Daily Ranking for values, colors, Regime, Signals, and date.
- Verify a narrow/mobile viewport has no horizontal overflow.

## Acceptance Criteria

1. The snapshot uses the approved two-row micro-strip at the normal desktop width.
2. All current parameters remain visible without expansion or horizontal scrolling.
3. The ranking region is approximately 92–108 px tall in the normal populated state.
4. Daily Ranking remains the sole owner of presentation semantics.
5. The Daily Chart begins materially higher and is visible in an approximately 1180 × 800 first viewport.
6. Loading, unavailable, error, retry, keyboard focus, and mobile behavior remain functional.
