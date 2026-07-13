# Symbol Workbench Shikiho Live Overlay Design

## Goal

Extend the local Shikiho bridge so Symbol Workbench consistently reflects the visible 15-minute-delayed Shikiho quote in its chart, price-derived fundamentals, Daily Ranking snapshot metrics, and technical judgments. Also finish the previously planned Shikiho score extraction, move the extension to a clearer workspace location, and integrate the completed branch with the compact Symbol Workbench UI currently present on local `main`.

## Scope

The overlay applies only inside `/symbol-workbench` for the selected Japanese stock.

It does not modify:

- Ranking or Screening list results;
- backtests, research bundles, optimization, or Lab;
- FastAPI, DuckDB, Parquet, datasets, or portfolio storage;
- historical J-Quants rows or their source-of-truth semantics;
- any other page that consumes the shared market APIs.

## Chosen Approach

Use a transient, extension-local Shikiho quote overlay. The existing J-Quants daily series remains immutable. When a valid quote for the selected symbol and current JST trading date exists, Symbol Workbench constructs one provisional daily bar and overlays it in memory for Workbench calculations and rendering only.

Rejected alternatives:

1. Replacing only the displayed current price leaves the chart, SMA5, valuation ratios, and signals internally inconsistent.
2. Persisting or overwriting the J-Quants daily row would contaminate historical analysis and blur provisional versus official data.

## Extension Layout

Move the intact package from:

`apps/ts/packages/shikiho-extension`

to:

`apps/ts/extensions/shikiho`

Keep the package name `@trading25/shikiho-extension` and its contract export so existing web imports and Bun `--filter` commands remain stable. Update workspace globs, TypeScript aliases, dependency-audit paths, Biome coverage, lockfile entries, documentation, and unpacked-extension instructions. The new load-unpacked target is `apps/ts/extensions/shikiho/dist`.

Do not split the bridge contract into a separate package in this change.

## Snapshot Contract

Add an optional quote object to `ShikihoSnapshotV1`:

```ts
interface ShikihoQuoteV1 {
  tradingDate: string;
  observedAt: string;
  delayMinutes: 15;
  currentPrice: number;
  open: number;
  high: number;
  low: number;
  previousClose: number;
  volume: number | null;
  openTime: string | null;
  highTime: string | null;
  lowTime: string | null;
  sourceLabel: '会社四季報オンライン';
}
```

`quote` is optional and does not participate in the three-field article capture status. Missing or malformed quote data must not downgrade a valid article snapshot. Quote extraction uses visible Japanese labels and the visible current-price/updated-time region. It does not use generated CSS class names, hidden state, APIs, XHR, cookies, or raw HTML.

The quote is accepted only when all required numeric invariants hold:

- all prices are finite and greater than zero;
- `low <= currentPrice <= high`;
- `low <= open <= high`;
- `tradingDate` and `observedAt` are valid and represent the visible quote;
- the quote symbol matches the selected four-digit code.

## Shikiho Score

Support the observed live structure:

- the overall score is the numeric sibling of the visible `四季報スコア` title;
- `成長性`, `収益性`, `安全性`, `規模`, `割安度`, and `値上がり` use visible `dt` labels with adjacent `dd` values;
- every score must be an integer from 0 through 5;
- the score region may render after the core article content, so capture stabilization must allow the later visible mutation to replace the earlier snapshot;
- a missing or inaccessible score remains optional and does not change `取得済み` to `一部取得`.

Use a minimal fictional fixture matching the observed structure. Do not store copied page HTML or real article text.

## Provisional Daily Bar

Create a pure Workbench helper that combines the official J-Quants daily series with `ShikihoQuoteV1`:

1. Determine the current JST calendar date.
2. If the latest official bar already has the same date or a later date, keep the official series unchanged.
3. If the quote date is the current JST date and is newer than the latest official bar, append one provisional bar:
   - open = quote open;
   - high = quote high;
   - low = quote low;
   - close = quote current price;
   - volume = quote volume when available, otherwise a missing provisional volume value;
   - adjustment factor remains neutral because this is not persisted or used for historical adjustment.
4. Never insert a provisional bar between historical rows or retain it after an official same-date bar arrives.

The helper returns both the combined series and explicit provenance metadata so consumers cannot mistake the provisional bar for an official row.

## Workbench Calculations

Within Symbol Workbench only, use the combined series for calculations that depend on the latest price or latest daily bar:

- chart OHLC and current-price marker;
- current price and day change versus `previousClose`;
- SMA5 and other chart indicators already derived from the displayed series;
- current price versus SMA5 judgments and related visible signal state;
- price-derived valuation values such as PER, forward PER, PBR, PSR, forward PSR, market capitalization, and price-based yields where their existing inputs are available;
- any Workbench-local computation that already consumes the latest daily close.

Do not alter statement-derived values such as EPS, BPS, revenue, operating profit, cash flow, margins, ROE, or payout ratios. Recalculate only the price-dependent presentation layer from unchanged fundamental inputs.

No generic shared API model is mutated. The overlay is applied after the selected-symbol API data has entered the Symbol Workbench page boundary.

## UI and Provenance

The compact Daily Ranking UI from local `main` remains the final layout.

When the overlay is active:

- show `四季報 15分遅延・当日暫定` beside the current quote or Workbench market metadata;
- show the quote update time;
- visually distinguish the provisional final chart bar without changing historical bars;
- tooltips and accessible text identify the provisional source;
- the Company Shikiho panel may show the extracted OHLC, previous close, volume, and update time in a compact section;
- official-as-of dates remain visible where already shown, so the user can distinguish official fundamentals from the provisional market price.

When no valid current-date quote exists, all existing J-Quants behavior remains unchanged and no warning occupies large layout space.

## Data Flow

```text
Authenticated Shikiho DOM
  -> extension extracts article + score + quote
  -> extension-local 24-hour article snapshot storage
  -> localhost bridge returns selected-symbol snapshot
  -> useShikihoSnapshot validates quote and symbol
  -> SymbolWorkbenchPage builds provisional daily overlay in memory
  -> chart + Workbench-local price-derived metrics use one consistent series
```

Manual `更新` continues to bypass the 24-hour cache. Automatic resolution retains the existing singleflight, FIFO, retry suppression, and extension-owned background-tab lifecycle.

Because intraday quotes change during the session, quote freshness is separate from article freshness. Article content retains its 24-hour TTL. A quote has a 15-minute freshness TTL measured from `observedAt`; on Workbench entry or selected-symbol change, the background resolver may refresh an otherwise fresh article snapshot when its quote is missing, is from a different JST date, or is at least 15 minutes old. Explicit `更新` still bypasses both TTLs. There is no timer-based polling, scheduled capture, or multi-symbol refresh.

## Failure Handling

- Article success with quote failure: keep the article and use official J-Quants prices.
- Quote success with optional score failure: apply the quote and omit the score section.
- Stale or non-current-date quote: retain it in the article snapshot if valid, but do not apply it as a Workbench overlay.
- Malformed OHLC relationship: reject only the quote object.
- Login or page-shape failure: preserve the last valid snapshot and existing diagnostics.
- Extension unavailable: use the existing Workbench data with no calculation changes.

## Local Main Integration

Local `main` currently has uncommitted compact Daily Ranking UI changes in:

- `DailyRankingSnapshot.tsx`;
- `DailyRankingSnapshot.test.tsx`;
- `SymbolWorkbenchPage.test.tsx`.

Integration order:

1. Preserve and commit the three-file compact UI change on local `main` as its own commit after verifying its focused tests.
2. Rebase or merge `codex/shikiho-workbench-bridge` onto that commit.
3. Keep the compact Daily Ranking component and its focused test unchanged.
4. Resolve only the overlapping `SymbolWorkbenchPage.test.tsx` assertions by combining the Shikiho hook/refresh setup with the compact UI assertions and the exact `四季報` button selector.
5. Implement and verify score, quote overlay, and package relocation on the integrated branch.
6. Return to `main` only after all automated and live acceptance checks pass, then complete the final integration without discarding either workstream.

## Testing

### Extension

- parse the observed overall-score sibling and six `dt`/`dd` score values;
- recapture when the score region appears after the core article snapshot;
- parse a minimal fictional quote fixture including current price, OHLC, previous close, volume, and times;
- reject hidden, malformed, out-of-range, symbol-mismatched, and internally inconsistent quote values;
- keep article capture valid when score or quote is missing;
- preserve privacy, payload-size, hash, storage, cache, and background-tab tests.

### Web

- validate the optional quote contract strictly;
- append exactly one current-date provisional bar;
- avoid overlay on stale dates or when an official same-date row exists;
- recalculate Workbench-local SMA5, day change, and price-dependent valuation presentation consistently;
- leave statement-derived metrics and other pages unchanged;
- render accessible provisional-source and update-time labels;
- retain the compact Daily Ranking UI and Japanese Company Shikiho presentation.

### Live Acceptance

1. Load `apps/ts/extensions/shikiho/dist` as the unpacked extension.
2. Open only Symbol Workbench for `7203` and confirm the background tab closes after capture.
3. Confirm all seven Shikiho score values match the visible source page.
4. Confirm current price, OHLC, previous close, volume, quote date, and update time match the visible Shikiho page.
5. Confirm the provisional chart bar and current-price-dependent metrics use the same quote.
6. Confirm SMA5 judgment changes consistently when the provisional close changes in a fixture-driven test.
7. Confirm Ranking, Screening, Backtest, and stored market data are unchanged.
8. Confirm reload within the article TTL does not crawl or bulk-fetch symbols.

## Acceptance Criteria

1. Symbol Workbench uses one consistent, clearly labelled Shikiho 15-minute-delayed provisional daily overlay for its chart and price-derived calculations.
2. The overlay is never persisted to Trading25 data stores and never affects other pages or analytical workflows.
3. All seven visible Shikiho score values are captured from the observed semantic DOM when available.
4. Missing, stale, or malformed quote data safely falls back to existing J-Quants behavior.
5. The extension resides at `apps/ts/extensions/shikiho` with stable package imports and build commands.
6. The compact local-main Daily Ranking UI and completed Shikiho functionality coexist without losing either set of tests or behavior.
