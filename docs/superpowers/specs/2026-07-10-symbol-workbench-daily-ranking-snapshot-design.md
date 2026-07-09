# Symbol Workbench Daily Ranking Snapshot Design

## Summary

Replace the Symbol Workbench header's separate basic-information grid and `Prime Liquidity` strip with one compact `Daily Ranking Snapshot`. Daily Ranking is the source of truth for both the snapshot values and the presentation semantics used for labels, formatting, evidence colors, regime badges, and signal badges.

The snapshot shows the latest available Daily Ranking observation for the selected symbol. It does not follow the chart timeframe or the strategy `Matched Date`.

## Goals

- Show the selected symbol's Daily Ranking parameters in the Symbol Workbench header.
- Keep Daily Ranking and Symbol Workbench labels, formatting, evidence colors, badges, and metric order synchronized.
- Reduce duplicated header content instead of adding another independent panel.
- Preserve the existing header actions and execution context.
- Keep `Index Membership` and free-float market capitalization visible in the header for now.

## Non-goals

- Do not copy the full Daily Ranking table into Symbol Workbench.
- Do not calculate ranking metrics independently in Symbol Workbench or the fundamentals service.
- Do not make the snapshot follow `Matched Date`, chart timeframe, or an arbitrary historical date.
- Do not add Daily Ranking filters, presets, sorting, or ranking navigation to the snapshot.
- Do not remove `Index Membership` or free-float market capitalization from the header in this change.

## Current State

Symbol Workbench gets `liquidityProfile` from the fundamentals endpoint and renders a dedicated `Prime Liquidity` strip. It does not fetch or retain the selected symbol's `RankingItem`. A transition from Daily Ranking to Symbol Workbench carries only the symbol code, so TanStack Query cache state and the originating Ranking page's date or filters cannot be treated as inputs.

Daily Ranking rendering is centralized in `EquityRankingTable`, but the desktop row, mobile card, formatting helpers, and badges are still coupled to their table layouts. Reusing the entire one-row table would preserve presentation behavior but would create a wide, table-shaped header with duplicated symbol identity fields.

## Architecture

### Backend snapshot endpoint

Add `GET /api/analytics/ranking/symbol/{code}`. It returns a `MarketRankingSymbolResponse` containing:

- `date: string | null`, the latest available Daily Ranking date;
- `item: RankingItem | null`, the requested normalized symbol's enriched row;
- `lastUpdated: string`.

The endpoint must reuse the existing Daily Ranking application services and enrichment path. It must not introduce a second calculation path for valuation, liquidity, sector strength, risk flags, technical flags, or value score.

The endpoint accepts the same canonical 4-digit and compatible 5-digit symbol forms used by the existing market readers. It searches Prime, Standard, and Growth using the default Daily Ranking calculation parameters. When the market has ranking data but the requested symbol is not ranked, it returns HTTP 200 with the latest `date` and `item: null`. When no ranking date is available, it returns HTTP 200 with `date: null` and `item: null`. Individual unavailable enrichments remain nullable fields on a present item.

The existing `/api/analytics/ranking` collection endpoint remains unchanged. Symbol Workbench must not fetch an unlimited all-market ranking collection and filter it in the browser.

### Shared presentation model

Extract a shared Daily Ranking metric-presentation layer from `EquityRankingTable`. The layer describes each reusable metric or badge group with a stable key and its presentation behavior:

- label;
- value formatter;
- evidence-tier or warning-class resolver;
- the tooltip text defined for metrics that require explanation;
- display group and order;
- badge rendering semantics for Regime and Signals.

The shared layer operates on the existing `RankingItem`-compatible item shape. It must remain independent of table sorting, filtering, virtualization, routing, and Symbol Workbench data fetching.

`EquityRankingTable` uses the shared presentation layer for its current desktop and mobile output. A new compact snapshot component uses the same presentation layer to render a header-oriented grid. Layout markup may differ, but semantic labels, formatted values, colors, badges, and order come from the same source.

Adding or changing a Daily Ranking metric requires one shared presentation change. Layout-specific code changes are allowed only when the metric introduces a new structural group rather than another member of an existing group.

### Frontend data flow

Symbol Workbench fetches the symbol-scoped snapshot whenever the selected symbol changes. The query key includes the normalized symbol. Previous-symbol data must not remain visible while the next symbol is loading.

The snapshot uses the endpoint's latest available Daily Ranking date and displays it as `As of YYYY-MM-DD`. It does not consume the page's `matchedDate` value.

## Header Layout

The top portion of the Symbol Workbench header remains unchanged:

- symbol code and company name;
- Watchlist, Shikiho, Refresh, settings, and timeframe actions;
- Overlay, Matched Date, Market Snapshot, and Signal Domains context.

Replace both the existing six-field basic-information grid and the `Prime Liquidity` strip with one `Daily Ranking Snapshot` section.

### Snapshot header

- Title: `Daily Ranking Snapshot`
- Reference date: `As of YYYY-MM-DD`

### Basic information group

- Market
- Index Membership
- Sector 17
- Sector 33
- Market Capitalization
- Free-Float Market Capitalization

Market, sector, and market-capitalization information appears only in this snapshot section. The existing independent grid is removed.

Market, Sector 33, and Market Capitalization use the ranking item while it is available. Index Membership, Sector 17, and Free-Float Market Capitalization are not part of `RankingItem`; they remain supplemental values from the existing stock-info and fundamentals data paths. If the ranking item is unavailable, existing stock-info and fundamentals values keep the whole basic-information group visible. This fallback does not calculate or approximate any Daily Ranking metric.

### Daily Ranking metrics group

- Sector Strength
- Current Price
- Change Percentage
- SMA5 5D
- PER
- Forward PER
- Forward OP / OP
- PSR
- Forward PSR
- PBR
- Value Score
- Liquidity Z
- Regime
- Signals
- Trading Value

Regime and Signals use the same badges as Daily Ranking. Numeric evidence uses the same formatting and color tiers as Daily Ranking.

The desktop layout is a dense wrapping grid sized for the application's normal working width. The mobile layout uses a two-column grid. Regime and Signals span the available row when their badges do not fit inside a normal metric cell.

### Removed header diagnostics

Remove the dedicated `Prime Liquidity` heading and its Workbench-only diagnostics from the header:

- median ADV / free-float ratio;
- ADV20 / ADV60 comparison;
- liquidity-implied price and gap;
- the duplicate liquidity residual/regime presentation;
- the duplicate recent 20-day and 60-day return line.

The fundamentals API continues returning `liquidityProfile`; removing that contract is outside this change. Symbol Workbench stops passing it to the header.

## Loading, Missing Data, and Errors

- Loading: render a compact loading state within the snapshot area.
- Symbol change: clear the prior symbol's snapshot before showing the next loading state.
- Ranking unavailable: keep the basic information group and show `Daily Ranking data unavailable` in the metrics area.
- Partially missing metric: preserve the metric position and render `-`.
- Request error: keep the rest of the header functional and show a retry-capable error state inside the snapshot.
- Unsupported or non-ranked symbol: treat it as unavailable rather than falling back to independently calculated fundamentals diagnostics.

## Contract and Type Changes

`MarketRankingSymbolResponse` reuses the existing `RankingItem` schema rather than defining a second metric payload. OpenAPI is the contract source of truth; generated TypeScript contracts must be refreshed after the endpoint is added.

The Web query hook exposes the generated response type and keeps transformation limited to symbol normalization and view-state derivation. Presentation formatting belongs in the shared metric-presentation layer.

## Testing

### Backend

- resolves the latest available ranking date;
- returns the same enriched values as the existing Daily Ranking service for a symbol on that date;
- normalizes compatible 4-digit and 5-digit codes;
- supports Prime, Standard, and Growth symbols;
- returns HTTP 200 with `item: null` for an unranked symbol and with both `date` and `item` null when no ranking date exists;
- preserves nullable enrichment fields;
- does not change the existing collection endpoint behavior.

### Shared frontend presentation

- verifies labels and order for all shared metrics;
- verifies number, ratio, currency, percentage, and missing-value formatting;
- verifies evidence colors;
- verifies Regime and Signals badges;
- keeps the existing Daily Ranking desktop and mobile rendering tests passing.

### Symbol Workbench

- fetches a snapshot for the selected symbol;
- shows the endpoint reference date rather than `Matched Date`;
- renders the retained basic information and all requested ranking metrics;
- removes the old independent basic-information grid and `Prime Liquidity` strip;
- handles loading, unavailable, partial, error, and retry states;
- does not show stale data after a symbol change;
- renders the intended desktop and mobile structures.

### Live verification

- open a Daily Ranking row and navigate to its Symbol Workbench page;
- compare values, evidence colors, Regime, Signals, and reference date across both surfaces;
- repeat for at least one Prime symbol and one non-Prime symbol;
- verify the header at the normal desktop working width and a mobile viewport;
- confirm the chart and header actions remain usable.

## Implementation Boundaries

- Keep the symbol-scoped endpoint inside the existing analytics/ranking architecture.
- Keep ranking calculations in the backend Daily Ranking service path.
- Keep data fetching in a focused Web query hook.
- Keep semantic metric presentation in the shared ranking component layer.
- Keep Symbol Workbench responsible only for arranging its header and handling snapshot states.
- Avoid unrelated Ranking filters, route-state, or fundamentals-panel refactors.

## Acceptance Criteria

1. Symbol Workbench shows the latest available Daily Ranking values for the selected symbol with an explicit reference date.
2. The header has one integrated snapshot instead of a separate basic-information grid plus `Prime Liquidity` strip.
3. Index Membership and free-float market capitalization remain visible.
4. Daily Ranking and Symbol Workbench share labels, formatting, evidence colors, Regime badges, Signal badges, and metric order through a common presentation layer.
5. Workbench does not independently calculate or approximate Daily Ranking values.
6. Missing or failed ranking data does not break the rest of the header.
7. Existing Daily Ranking behavior remains unchanged after the presentation refactor.
