# Symbol Workbench Shikiho Bridge Design

## Summary

Add a local-only Atlas browser extension that passively captures selected company information from an already-open, authenticated Company Shikiho Online stock page and displays the latest capture in a compact Symbol Workbench panel.

The extension reads only the rendered page. It does not copy credentials, call Shikiho APIs, automate login, click hidden tabs, or add backend storage. Captures remain inside the Atlas browser profile and are never written to FastAPI, DuckDB, dataset bundles, or `portfolio.db`.

## Goals

- Use the user's existing authenticated Atlas session without exporting cookies or tokens.
- Capture the approved Shikiho fields automatically when a stock page finishes rendering.
- Show only the selected Symbol Workbench symbol's latest capture.
- Keep the Workbench integration compact: one extension bridge hook and one collapsible panel.
- Keep source, capture time, edition, freshness, and extraction status visible.
- Degrade safely when login expires or the Shikiho DOM changes.

## Non-goals

- Do not reproduce or archive a full Shikiho page.
- Do not capture HTML, images, charts, advertisements, or credentials.
- Do not crawl symbols in bulk or make additional Shikiho network requests.
- Do not add a Shikiho FastAPI route, OpenAPI contract, database table, or market-data source of truth.
- Do not add Shikiho to the existing Workbench panel-order settings.
- Do not automate clicks on Shikiho tabs that the user has not opened.
- Do not support sharing captures between browsers or users.

## Current State

`SymbolWorkbenchHeader.tsx` already opens `https://shikiho.toyokeizai.net/stocks/{symbol}` in a separate browser tab. Symbol selection is URL-backed through `/symbol-workbench?symbol=...`. There is no existing extension, userscript, `postMessage`, or Shikiho backend integration.

FastAPI cannot safely reuse an Atlas login session because authentication cookies are origin-scoped and may be `HttpOnly`. An iframe would not give Trading25 cross-origin DOM access. The browser extension is therefore the only component that reads Shikiho content.

## Architecture

The implementation has three small units:

1. **Atlas extension**: observes authenticated Shikiho stock pages, extracts an allowlisted snapshot, and stores it in extension-local storage.
2. **Workbench bridge hook**: exchanges narrowly scoped messages with the extension content script running on localhost.
3. **Shikiho panel**: renders a compact, source-labelled view of the selected symbol's snapshot.

```text
Authenticated Shikiho tab
  -> rendered DOM becomes stable
  -> extension extracts and validates ShikihoSnapshotV1
  -> chrome.storage.local stores latest snapshot by code
  -> extension notifies localhost tab
  -> useShikihoSnapshot validates code and schema
  -> ShikihoPanel renders plain text
```

The extension has host access only to:

- `https://shikiho.toyokeizai.net/stocks/*`
- `http://localhost:5173/*`
- `http://127.0.0.1:5173/*`
- the equivalent Vite preview origins on port `4173`

It does not request the browser `cookies` permission.

## Automatic Capture

The Shikiho content script activates only on a stock detail path. It derives the four-digit stock code from the canonical page URL and requires that the rendered page represents the same code.

The extractor waits for relevant content and observes DOM mutations. Capture runs after mutations have been quiet for a short debounce interval, with a maximum initial wait of 10 seconds. Client-side navigation to another stock restarts observation and capture for the new code.

The extractor reads the current rendered DOM only. It makes no fetch/XHR request and does not click unselected tabs. If the user later opens another supported Shikiho tab section, the resulting DOM mutation adds any newly rendered approved fields and replaces the same symbol's latest snapshot.

A content hash excludes `capturedAt`. If the normalized content has not changed, the extension does not rewrite storage or notify Workbench.

## Snapshot Contract

`ShikihoSnapshotV1` is a private extension/frontend contract, not an OpenAPI contract.

```ts
interface ShikihoSnapshotV1 {
  schemaVersion: 1;
  extractorVersion: string;
  code: string;
  companyName: string | null;
  sourceUrl: string;
  capturedAt: string;
  pageUpdatedAt: string | null;
  editionLabel: string | null;
  contentHash: string;
  status: 'captured' | 'partial';
  features: string | null;
  consolidatedBusinesses: string | null;
  commentary: Array<{ heading: string | null; body: string }>;
  score: {
    overall: number | null;
    growth: number | null;
    profitability: number | null;
    safety: number | null;
    scale: number | null;
    value: number | null;
    priceMomentum: number | null;
  };
  comparisonCompanies: Array<{ code: string | null; name: string }>;
  industries: string[];
  marketThemes: string[];
  profile: Array<{ label: string; value: string }>;
  missingFields: string[];
}
```

Failed observations are stored separately so they cannot replace valid content:

```ts
interface ShikihoCaptureDiagnosticV1 {
  schemaVersion: 1;
  code: string;
  observedAt: string;
  status: 'login_required' | 'page_changed' | 'storage_error';
}
```

Workbench marks a valid snapshot stale only when a newer diagnostic exists for the same code. Capture age is always shown but does not invent an arbitrary time-based stale threshold.

Extraction uses visible Japanese labels and section boundaries as primary anchors instead of generated CSS class names. Each string, list, and total snapshot receives a conservative size limit. Normalization trims whitespace and removes duplicated labels without rewriting the source wording.

`features`, `consolidatedBusinesses`, and at least one `commentary` item are the three core fields. A snapshot is `captured` when all three core fields are present. Missing score, comparison-company, industry, theme, profile, edition, or update-date fields remain listed in `missingFields` when useful, but do not downgrade the snapshot to `partial`. A snapshot is `partial` only when at least one core field is missing while some approved content was extracted.

Semantic label elements such as `dt` and `th` remain the extraction anchor even when the visible label is split across nested spans. The extractor must resolve each label to its own adjacent value element so one `dl` block cannot be repeated as both `features` and `consolidatedBusinesses`.

Extension storage keeps the latest valid snapshot for at most 200 symbols. Least-recently-captured entries are removed when the limit is exceeded. A new failed extraction never overwrites the last valid snapshot.

## Workbench Bridge

The localhost content script announces bridge availability and responds to a request containing the normalized selected symbol. It returns only the matching snapshot and extraction status. Storage changes for the visible symbol trigger an immediate update message.

The Web hook validates `schemaVersion`, payload size, source host, source-path code, and selected-symbol equality before accepting a response. Invalid or mismatched messages are ignored. The page never receives extension storage for other symbols.

The bridge has no capability to write to Shikiho, invoke browser navigation, or access credentials. Opening the source page continues to use the existing explicit Workbench button.

## Implementation Layout

- `apps/ts/packages/shikiho-extension/`: manifest, Shikiho content script, localhost content script, background storage, extractor, and the exported bridge contract.
- `apps/ts/packages/web/src/hooks/useShikihoSnapshot.ts`: availability handshake, selected-symbol request, runtime validation, and live updates.
- `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`: the single compact presentation component.
- `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`: one panel insertion below the Daily Ranking snapshot.

The extension package owns one runtime validator used by both the extension and Web hook. Do not duplicate the contract or add it to generated API contracts.

## Compact Workbench UI

Place one collapsible `会社四季報` panel immediately below the Daily Ranking snapshot. It is expanded by default. Collapse state remains component-local; it does not extend `chartStore` or panel-order configuration.

The panel uses a dense two-column desktop composition:

- **Primary column**: features, consolidated businesses, and commentary.
- **Secondary column**: score, industries, themes, comparison companies, and captured profile rows.

On narrow screens the columns stack. The panel uses section dividers and compact typography rather than nested cards. Empty optional sections are omitted rather than rendered as large placeholder blocks.

The panel header is `会社四季報`. Commentary is rendered directly after `特色` and `連結事業`; it does not add a second internal `会社四季報` heading. A complete core capture uses the Japanese status `取得済み`.

The header always shows:

- source label and original-page link;
- capture status;
- capture time;
- edition or page update date when available;
- the existing action to open Shikiho.

Comparison-company codes navigate to `/symbol-workbench?symbol={code}`. All captured content is rendered as escaped React text; `dangerouslySetInnerHTML` is prohibited.

## States and Failure Handling

- **Extension unavailable**: show a compact installation/connection hint and retain the existing Shikiho link.
- **Not captured**: explain that opening the selected stock's Shikiho page will capture it automatically.
- **Login required**: show `Login required` without replacing the last valid capture.
- **Partial capture**: when any of the three core fields is missing, show available sections and the small status `一部取得`.
- **Page changed**: keep and mark the last valid snapshot stale when required anchors cannot be resolved.
- **Stale**: show the prior capture time and invite the user to reopen the source page.
- **Symbol mismatch**: discard the response and show no foreign-symbol content.
- **Storage or quota failure**: retain the current in-memory result when possible and show a compact diagnostic.

Required-anchor failure, code mismatch, invalid schema, or oversize content must not overwrite a previous valid snapshot.

## Privacy and Operational Boundaries

- Personal, local use only.
- No bulk crawl, scheduled capture, remote sync, telemetry, or redistribution.
- No cookies, authorization headers, local-storage credentials, or raw HTML are captured.
- No capture payloads are logged.
- No additional request is sent to Shikiho beyond the page the user opened.
- Source attribution and the original-page link remain visible beside captured content.

## Testing

### Extension unit tests

- extract every approved field from sanitized HTML fixtures;
- classify all three core fields as captured even when optional fields are missing;
- classify a missing core field as a partial capture;
- resolve nested-span `dt` labels to their own adjacent `dd` values;
- detect login-required and required-anchor failure;
- normalize and validate four-digit codes;
- debounce initial rendering and SPA mutations;
- recapture after client-side symbol navigation;
- suppress identical content by hash;
- preserve the last valid snapshot after extraction failure;
- enforce field, payload, and 200-symbol storage limits.

### Web tests

- distinguish unavailable, not-captured, captured, partial, stale, login-required, and page-changed states;
- reject invalid schema, source host, source code, and selected-symbol mismatch;
- update the visible panel after a matching storage notification;
- render captured text without HTML interpretation;
- navigate comparison-company codes through Symbol Workbench route state;
- preserve the existing Shikiho source link and the rest of the Workbench header.

### Live acceptance

1. Install the unpacked extension in Atlas.
2. Open Symbol Workbench for `7203` and confirm the uncaptured state.
3. Use the existing Shikiho button and allow the authenticated page to finish rendering.
4. Return to or keep open Symbol Workbench and confirm automatic capture without a manual capture action.
5. Compare the displayed fields, score, edition, source URL, and timestamp with the open page.
6. Navigate within Shikiho to another stock and confirm a separate capture.
7. Navigate through a comparison-company code in Workbench and confirm symbol isolation.
8. Verify login-required and a fixture-driven DOM-change failure retain the last valid capture.

## Acceptance Criteria

1. Opening an authenticated Shikiho stock page automatically captures the approved fields after the rendered DOM stabilizes.
2. The extension performs no additional Shikiho request and never exports credentials or raw HTML.
3. Symbol Workbench displays only the selected symbol's latest validated capture in one compact collapsible panel.
4. Source, capture time, edition/update date when available, and status remain visible; a complete core capture displays `取得済み`.
5. A partial or failed extraction never destroys a prior valid snapshot.
6. Captures remain in Atlas extension-local storage and never enter Trading25 backend data stores.
7. The integration adds no FastAPI, OpenAPI, DuckDB, portfolio, or panel-order complexity.
