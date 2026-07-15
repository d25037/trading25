# Symbol Workbench Shikiho Layout Refinement Design

## Goal

Refine the Company Shikiho panel so acquisition metadata remains readable, the earnings announcement date is visually prominent, and the score visualization uses the panel's existing desktop two-column body instead of occupying the full width.

## Scope

- Limit changes to `ShikihoPanel`, `ShikihoScoreCard`, and their web tests.
- Preserve the current Shikiho extension contract, acquisition behavior, diagnostics, source link, refresh, collapse behavior, urgency thresholds, textual disclosure content, and accessible names.
- Add no dependencies and make no FastAPI, OpenAPI, database, extension, or storage changes.

## Header Layout

Use a two-row header at the normal approximately 1180px desktop work area.

The first row contains:

- left: `会社四季報`, capture status, and the earnings announcement date;
- right: `四季報で開く`, refresh, and collapse controls.

The earnings announcement date is the dominant metadata element. Render it as a non-wrapping rounded badge with a calendar icon, a small `決算発表予定日` label, a bold date, and the remaining-day text. Keep the existing neutral/yellow/orange/red/past color states and text meaning.

The second row contains the edition, capture timestamp, acquisition duration/diagnostics trigger, and any diagnostic metadata. It may wrap on narrow screens, but must show the full capture timestamp at the normal desktop width rather than truncating it behind the first-row content.

Diagnostics details remain below both header rows when expanded.

## Body Layout

At desktop width, use the existing two-column body consistently when a score and primary disclosure content exist:

- left column, approximately two thirds: features, consolidated businesses, and commentary;
- right column, approximately one third: the score card followed by industries, themes, comparison companies, and profile when present.

When only one side has content, use the full available width. At narrow widths, stack the primary content and right-side content vertically.

## Score Card

The score card is a compact three-row vertical component inside the right column:

1. header row: `四季報スコア`, five decorative stars, and readable `総合 3 / 5` text;
2. centered six-axis orange radar chart with Japanese axis labels;
3. six score values in a compact two-column by three-row definition list.

The card must not use `col-span-full` or a desktop horizontal radar/value layout. Missing dimensions render as `—`; the radar appears only when all six dimensions are available. Industry median data remains omitted.

## Accessibility and Responsive Behavior

- Preserve `role="img"` and the Japanese radar summary.
- Preserve readable numeric score values independently of the radar and stars.
- Preserve action focus states and accessible button/link names.
- Do not communicate earnings urgency by color alone.
- Keep the large date badge non-wrapping; allow the lower-priority second metadata row to wrap only when required.

## Testing

- Assert separate primary and metadata header rows.
- Assert the edition, full capture timestamp, and acquisition duration remain present in the metadata row.
- Assert the earnings badge uses larger text and stays non-wrapping.
- Assert the body places primary content first and an aside containing the score and secondary content second.
- Assert the score card has three vertical sections and no full-width or desktop horizontal-grid classes.
- Preserve existing urgency, radar accessibility, missing-axis, refresh, collapse, diagnostics, and disclosure-content tests.

## Acceptance Criteria

1. The panel header uses two rows and shows the full capture timestamp at the user's normal desktop width.
2. The earnings announcement date is larger and more prominent than edition/capture metadata.
3. The desktop body uses a two-column primary/aside layout whenever score plus primary content are available.
4. The score card uses header, radar, and values as three vertical rows inside the aside.
5. Focused and full web tests, typecheck, build, lint, dependency audit, `git diff --check`, and Chrome acceptance pass.
