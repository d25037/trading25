# Symbol Workbench Shikiho Panel Polish Design

## Goal

Polish the Symbol Workbench Company Shikiho panel by removing the redundant header button, keeping the panel header on one line, replacing the plain score list with a Shikiho-like radar score card, and capturing/displaying the next earnings announcement date with proximity-based urgency colors.

## Scope

- Web UI changes are limited to Symbol Workbench and the Company Shikiho panel.
- Extension changes add one nullable `earningsAnnouncementDate` field to the existing local-only snapshot contract.
- No FastAPI, OpenAPI, DuckDB, portfolio, or market-data persistence changes are required.
- The panel's existing `四季報で開く`, manual refresh, diagnostics, collapse behavior, textual company content, and quote overlay remain available.

## Chosen Layout

Remove the standalone `四季報` button beside `Add to Watchlist`; the panel-local source link remains the canonical outbound action.

Render the Company Shikiho header as a two-zone, non-wrapping row. The left zone contains the title, capture status, edition, capture time, and earnings announcement badge. The right zone contains diagnostics, source link, refresh, and collapse controls. Low-priority metadata may truncate at constrained widths, but the header must not create a second row at the normal approximately 1180px desktop work area.

Render the score as a full-width disclosure block before the textual body. Its header shows five stars and the overall integer. Its body shows a six-axis inline SVG radar for growth, profitability, safety, scale, value, and price momentum plus a two-column numeric list. Missing dimensions render as `—`; the radar is shown only when all six dimensions exist so absence is never misrepresented as zero. Industry median data is intentionally omitted.

## Earnings Announcement Date

Add `earningsAnnouncementDate: string | null` to `ShikihoSnapshotV1`. The canonical serialized value is `YYYY-MM-DD`. Existing stored snapshots that omit the field parse as `null` for backward compatibility.

Extraction is anchored by the visible Japanese label `決算発表予定日` and its adjacent visible date text. The observed live DOM is a visible container whose text label is followed by a child date value such as `2026/07/31`; implementation must not depend solely on generated CSS class names. Invalid calendar dates and ambiguous values are rejected as `null`.

The date participates in stable content hashing, progressive field detection, candidate merging, and snapshot storage like other optional Shikiho metadata. It does not affect captured/partial status.

## Date Presentation

Show a compact calendar badge labeled `決算発表予定日` with the formatted date. Determine urgency from the local JST calendar date:

- 15 or more days remaining: neutral;
- 8 through 14 days: yellow;
- 4 through 7 days: orange;
- 0 through 3 days: red;
- past date: muted gray.

The badge must expose the same meaning through text, not color alone. The calculation is a pure helper accepting an explicit current date so boundary tests are deterministic.

## Accessibility and Responsive Behavior

- The radar SVG uses `role="img"` and an accessible Japanese label summarizing all six scores.
- The numeric score list remains present for users who cannot interpret the radar.
- Stars are decorative; the overall score is readable text.
- Header actions keep their existing accessible names and focus behavior.
- At narrow widths, low-priority metadata truncates or hides before action controls overflow. The panel body may stack, while the header itself remains one row.

## Testing

### Extension

- extract `2026/07/31` from a sanitized fictional fixture using the Japanese label;
- reject malformed or impossible dates;
- parse older snapshots without the property as `null`;
- include the field in progressive capture and stable hash behavior.

### Web

- remove the standalone old button and retain `四季報で開く`;
- assert the two-zone no-wrap header structure;
- test date urgency boundaries and past dates;
- test overall stars, six-axis radar, metric values, and missing-dimension fallback;
- preserve refresh, collapse, diagnostics, and textual disclosure tests.

### Live Acceptance

Build/reload the Chrome extension, open Symbol Workbench for `6737`, refresh the snapshot, and verify `2026/07/31`, the neutral 16-day badge, the single-line header, the absence of the old button, and the new score card against the visible Shikiho page.

## Acceptance Criteria

1. The old header-level `四季報` button is gone while the panel source link still works.
2. The Company Shikiho header stays on one row at the user's normal desktop width.
3. The score UI shows overall stars/number, a correct six-axis radar, and six numeric values without industry medians.
4. The extension captures the visible earnings announcement date and older snapshots remain readable.
5. The date badge uses the approved five proximity states and text remains understandable without color.
6. Focused tests, typecheck, builds, lint, dependency audit, and Chrome acceptance pass.
