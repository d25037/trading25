# Fundamental Ranking UI Restoration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development and superpowers:verification-before-completion while executing these steps.

**Goal:** Restore the deleted Fundamental Ranking view on `/ranking` without reverting the current Daily Ranking workspace.

**Architecture:** Keep FastAPI and `@trading25/api-clients/analytics` as the calculation and contract SoT. Add a URL-backed Daily/Fundamental page mode, enable the fundamental query only in that mode, and restore the small filter/table/summary presentation layer from `287e830f^` using current generated types.

**Tech Stack:** React 19, TanStack Router/Query, TypeScript, Vitest, FastAPI generated OpenAPI types.

## Global Constraints

- Do not change the backend fundamental calculation or response contract.
- Do not restore Value Scores or any compatibility layer removed by `287e830f`.
- Preserve all current Daily Ranking controls, filters, watchlists, presets, and Bubble Footprint behavior.
- Keep `/ranking` selection and filter state URL-backed.
- Frontend code only maps API data; it must not calculate financial metrics.

---

### Task 1: Lock the regression with failing tests

**Files:**
- Modify: `apps/ts/packages/web/src/pages/RankingPage.test.tsx`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`

**Interfaces:**
- Consumes: existing `RankingPage`, `AnalyticsClient.getFundamentalRanking`.
- Produces: regression coverage for the restored tab and `metricKey` forwarding.

- [ ] Replace the assertion that `Fundamental Ranking` is absent with an assertion that its page-mode button is present.
- [ ] Add a test whose mocked route state selects `fundamentalRanking`, asserts the fundamental hook is enabled, the daily hook is disabled, and an API item is rendered.
- [ ] Extend the client test input with `metricKey: 'eps_forecast_to_actual'` and require that exact query parameter in the request URL.
- [ ] Run `bun test packages/web/src/pages/RankingPage.test.tsx packages/api-clients/src/analytics/AnalyticsClient.test.ts` from `apps/ts` and confirm failures are caused by the deleted UI and missing query forwarding.

### Task 2: Restore the API hook and presentation components

**Files:**
- Create: `apps/ts/packages/web/src/types/fundamentalRanking.ts`
- Create: `apps/ts/packages/web/src/hooks/useFundamentalRanking.ts`
- Create: `apps/ts/packages/web/src/hooks/useFundamentalRanking.test.tsx`
- Create: `apps/ts/packages/web/src/components/FundamentalRanking/FundamentalRankingFilters.tsx`
- Create: `apps/ts/packages/web/src/components/FundamentalRanking/FundamentalRankingTable.tsx`
- Create: `apps/ts/packages/web/src/components/FundamentalRanking/FundamentalRankingSummary.tsx`
- Create: `apps/ts/packages/web/src/components/FundamentalRanking/index.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`

**Interfaces:**
- Consumes: `FundamentalRankingParams` and response aliases from `@trading25/api-clients/analytics`.
- Produces: `useFundamentalRanking(params, enabled)` and three display components.

- [ ] Re-export the current generated API aliases and define only the optional UI request params needed by the endpoint.
- [ ] Restore the query hook with a stable query key, lookback normalization to `1..20`, and conditional query enablement.
- [ ] Restore the market/limit/forecast filter, ratio-high/ratio-low table, and summary from `287e830f^`, updating imports to current generated aliases.
- [ ] Forward `metricKey` in `AnalyticsClient.getFundamentalRanking`.
- [ ] Run the hook, component, and client tests and confirm they pass.

### Task 3: Add URL-backed page mode and integrate the current Ranking page

**Files:**
- Modify: `apps/ts/packages/web/src/types/ranking.ts`
- Modify: `apps/ts/packages/web/src/lib/routeSearch.ts`
- Modify: `apps/ts/packages/web/src/lib/routeSearch.test.ts`
- Modify: `apps/ts/packages/web/src/hooks/usePageRouteState.ts`
- Modify: `apps/ts/packages/web/src/hooks/usePageRouteState.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/RankingPage.tsx`

**Interfaces:**
- Produces: `RankingPageTab = 'ranking' | 'fundamentalRanking'`, `tab`, `fundamentalLimit`, `fundamentalMarkets`, `forecastAboveRecentFyActuals`, and `forecastLookbackFyCount` URL fields.
- Consumes: Task 2 hook/components.

- [ ] Add validate/deserialize/serialize coverage for the fundamental URL fields, defaulting to Daily Ranking and `limit=20`, `markets='prime'`, filter disabled, lookback `3`.
- [ ] Extend `useRankingRouteState` with page-mode and fundamental-param setters while preserving current daily state.
- [ ] Add a compact Daily/Fundamental segmented control to the current page header.
- [ ] Keep current Daily Ranking content unchanged; render restored fundamental filters, table, and summary only for `fundamentalRanking`.
- [ ] Enable only the query for the selected page mode and update intro metadata/description accordingly.
- [ ] Run the focused page, route-state, and route-search tests until green.

### Task 4: Verification

**Files:** no production changes.

- [ ] Run `bun --cwd="$PWD/apps/ts" run --filter @trading25/contracts bt:check`.
- [ ] Run `bun --cwd="$PWD/apps/ts" run quality:typecheck`.
- [ ] Run `bun --cwd="$PWD/apps/ts" run workspace:test`.
- [ ] After initial sync completes, call `/api/analytics/fundamental-ranking?limit=3&markets=prime` and confirm the UI contract fields are populated.
