# Indices Valuation SoT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PER, forward PER, PBR, and market cap correct and share-adjusted across product APIs, then reuse a common equity table surface for Ranking Daily and Indices sector constituents.

**Architecture:** Do not create an independent valuation calculator from scratch. The existing Symbol Workbench path already uses `FundamentalsCalculator` and `dailyValuation`; this becomes the product valuation SoT. Shared low-level valuation primitives define denominator validity, positive-ratio/null semantics, share-basis adjustment, and market-cap calculation; product display APIs use a `FundamentalsCalculator` resolver; batch loaders can use vectorized helpers that implement the same primitives without calling per-symbol UI code. Strategy signals and research modules remain separately scoped only where their input contracts or reproducibility require it, but their root valuation semantics must be audited against the shared primitives.

**Tech Stack:** Python 3.12, FastAPI, DuckDB, Pydantic, React 19, TanStack Query, Bun, generated OpenAPI contracts.

---

## Confirmed Existing SoT

The frontend Symbol Workbench path already uses backend-calculated fundamentals:

- `apps/ts/packages/web/src/hooks/useFundamentals.ts`
- `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- `apps/ts/packages/web/src/components/Chart/FundamentalsPanel.tsx`
- `apps/bt/src/entrypoints/http/routes/analytics_market.py`
- `apps/bt/src/application/services/fundamentals_service.py`
- `apps/bt/src/domains/fundamentals/calculator.py`

The relevant existing calculator is:

- `FundamentalsCalculator._calculate_daily_valuation()`

It already computes:

- `per`
- `pbr`
- `marketCap`
- `freeFloatMarketCap`

and already uses:

- `ShareAdjustmentEvent`
- `adjust_share_count_to_price_basis()`
- latest quarterly share baseline
- applicable FY row as of each daily price date

So the implementation must reuse and harden this path. The earlier idea of creating a new standalone `valuation_metrics.py` from scratch is rejected.

---

## Inventory From Repo-Wide Audit

### Product/API Surfaces To Unify Now

1. `apps/bt/src/domains/fundamentals/calculator.py`
   - Current product SoT for Symbol Workbench `dailyValuation`.
   - Contains `_calculate_per()`, `_calculate_pbr()`, `_calculate_daily_valuation()`, `_update_latest_with_daily_valuation()`, and `_apply_fy_data_to_metrics()`.
   - Action: expose a small public valuation resolver from this class or adjacent fundamentals-domain module, preserving this logic.

2. `apps/bt/src/application/services/chart_service.py`
   - `get_sector_stocks()` currently has inline SQL for `per`, `forward_per`, `pbr`, and `market_cap`.
   - This path is broken for split/share-basis because it divides adjusted close by raw EPS/BPS and raw shares.
   - Action: delete inline valuation SQL and enrich sector rows via the fundamentals valuation SoT.

3. `apps/bt/src/application/services/ranking_service.py`
   - `_load_value_composite_scored_frame()` computes adjusted `pbr`, `forward_per`, and `market_cap_bil_jpy` with its own helper chain.
   - It is closer to correct than `chart_service.py`, but it is still a separate product implementation.
   - Action: replace value-composite valuation enrichment with the same fundamentals valuation SoT.

4. `apps/bt/src/entrypoints/http/schemas/ranking.py` and `apps/bt/src/entrypoints/http/schemas/chart.py`
   - Product API schema must expose the shared metrics consistently.
   - Action: add optional valuation fields to `RankingItem`; keep `SectorStockItem` compatible.

### Product-Support Surfaces To Commonize At The Primitive/Batch Layer

5. `apps/bt/src/domains/strategy/signals/fundamental_valuation.py`
   - Computes PER/PBR/PEG from already prepared `close`, `eps`, `bps`, and forecast series.
   - This is signal logic, not display valuation enrichment.
   - Action: do not route this through Symbol Workbench daily valuation. Instead, make it use shared ratio/null helpers, and assert that upstream loaders provide adjusted per-share series.

6. `apps/bt/src/application/services/screening_market_loader.py`
   - Uses `resolve_adjusted_shared_baseline_shares()`.
   - This is upstream signal/backtest data preparation.
   - Action: share the same baseline-share and per-share adjustment primitives. Do not call a per-symbol UI resolver in a vectorized loader, but do remove duplicated denominator/ratio semantics if present.

7. `apps/bt/src/infrastructure/data_access/loaders/data_preparation.py`
   - Also uses adjusted shared baseline shares for backtest data preparation.
   - Action: share the same baseline-share and per-share adjustment primitives. Preserve the loader contract and performance shape.

### Research Modules To Inventory, Then Migrate Selectively

Research code has many valuation variants because it captures experiment-specific entry timing and reproducibility:

- `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- `apps/bt/src/domains/analytics/annual_value_periodic_rebalance.py`
- `apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py`
- `apps/bt/src/domains/analytics/forward_eps_trade_archetype_decomposition.py`
- `apps/bt/src/domains/analytics/new_high_momentum_research.py`
- `apps/bt/src/domains/analytics/standard_missing_forecast_cfo_non_positive_deep_dive.py`
- `apps/bt/src/domains/analytics/topix500_positive_eps_missing_forecast_cfo_positive_deep_dive.py`
- and related annual value/factor-family modules.

Action: do not bulk rewrite these in this product/UI fix. Instead, create an audit note listing which research modules use independent valuation math and whether they are intentionally experiment-specific. If a research module is an active reusable panel provider, schedule a follow-up migration to shared primitives. If a module is a published experiment, preserve bundle semantics unless a dedicated reproducibility rerun is part of the task.

---

## Task 1: Extract Shared Valuation Primitives

**Files:**
- Create: `apps/bt/src/domains/fundamentals/valuation_primitives.py`
- Modify: `apps/bt/src/domains/fundamentals/__init__.py`
- Test: `apps/bt/tests/unit/domains/fundamentals/test_valuation_primitives.py`

- [ ] Add shared primitive helpers.

Required functions:

```python
def positive_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    value = numerator / denominator
    return value if math.isfinite(value) and value > 0 else None


def valuation_ratio(price: float | None, per_share_value: float | None) -> float | None:
    return positive_ratio(price, per_share_value)


def market_cap_from_price_and_shares(price: float | None, shares: float | None) -> float | None:
    if price is None or shares is None or price <= 0 or shares <= 0:
        return None
    value = price * shares
    return value if math.isfinite(value) else None
```

The helper names can be adjusted during implementation, but these semantics cannot change.

- [ ] Add tests for null and non-positive semantics.

Required cases:

- `valuation_ratio(500, 100) == 5`
- denominator `0`, negative denominator, missing numerator, missing denominator return `None`
- negative numerator returns `None`
- `market_cap_from_price_and_shares(500, 1_000_000) == 500_000_000`
- non-positive price/share returns `None`

- [ ] Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/fundamentals/test_valuation_primitives.py -q
uv run --project apps/bt ruff check apps/bt/src/domains/fundamentals/valuation_primitives.py apps/bt/tests/unit/domains/fundamentals/test_valuation_primitives.py
uv run --project apps/bt pyright apps/bt/src/domains/fundamentals
```

Expected: all pass.

---

## Task 2: Add A Valuation Inventory Test/Guardrail

**Files:**
- Create: `apps/bt/tests/unit/domains/fundamentals/test_valuation_sot_inventory.py`
- Modify: `docs/superpowers/plans/2026-05-09-indices-valuation-sot.md` only if audit discoveries change the task list.

- [ ] Add a test that documents the allowed product valuation calculation locations.

Test intent:

```python
from pathlib import Path


PRODUCT_FILES_THAT_MAY_CONTAIN_DIRECT_VALUATION_MATH = {
    "apps/bt/src/domains/fundamentals/calculator.py",
    "apps/bt/src/domains/fundamentals/valuation_primitives.py",
    "apps/bt/src/domains/strategy/signals/fundamental_valuation.py",
}

PRODUCT_FILES_THAT_MUST_NOT_CONTAIN_DIRECT_VALUATION_MATH = {
    "apps/bt/src/application/services/chart_service.py",
    "apps/bt/src/application/services/ranking_service.py",
}


def test_product_services_do_not_reintroduce_direct_per_pbr_math() -> None:
    banned_fragments = (
        "curr.close / actual_statement.earnings_per_share",
        "curr.close / forecast_statement.forward_eps",
        "curr.close / actual_statement.bps",
        "price * baseline_shares",
        "current_price /",
    )
    repo_root = Path(__file__).resolve().parents[5]
    for relative in PRODUCT_FILES_THAT_MUST_NOT_CONTAIN_DIRECT_VALUATION_MATH:
        text = (repo_root / relative).read_text()
        for fragment in banned_fragments:
            assert fragment not in text, f"{relative} contains duplicated valuation math: {fragment}"
```

- [ ] Run it before implementation to verify it fails on current `chart_service.py` / `ranking_service.py`.

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/fundamentals/test_valuation_sot_inventory.py -q
```

Expected before implementation: FAIL.

---

## Task 3: Promote Fundamentals Daily Valuation Into A Product Resolver

**Files:**
- Modify: `apps/bt/src/domains/fundamentals/calculator.py`
- Modify: `apps/bt/src/domains/fundamentals/models.py`
- Modify: `apps/bt/src/domains/fundamentals/__init__.py`
- Test: `apps/bt/tests/server/services/test_fundamentals_service.py`

- [ ] Add a public method to `FundamentalsCalculator`.

Method shape:

```python
def calculate_latest_valuation(
    self,
    statements: list[JQuantsStatement],
    *,
    close: float,
    price_date: str,
    prefer_consolidated: bool,
    share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
) -> DailyValuationDataPoint | None:
    values = self._calculate_daily_valuation(
        statements,
        {price_date: close},
        prefer_consolidated,
        share_adjustment_events=share_adjustment_events,
    )
    return values[-1] if values else None
```

- [ ] Add forward PER support without duplicating old ranking logic.

Extend the domain result with optional fields:

```python
forwardPer: float | None = None
forwardEps: float | None = None
forwardEpsDisclosedDate: str | None = None
forwardEpsSource: Literal["revised", "fy"] | None = None
```

Implementation must reuse the same adjusted per-share mechanics already used by:

- `_annotate_latest_fy_with_revision()`
- `_apply_share_adjustments()`
- `_resolve_display_forecast_eps()`

Do not introduce a second forecast precedence rule. The precedence remains:

```text
revised forecast EPS > adjusted FY forecast EPS > raw FY forecast EPS
```

- [ ] Tighten PER denominator behavior.

For product display:

- `per = null` when EPS is missing or `<= 0`
- `forwardPer = null` when forecast EPS is missing or `<= 0`
- `pbr = null` when BPS is missing or `<= 0`

This aligns with value-factor usage and avoids negative PER display.

- [ ] Replace local ratio and market-cap formulas with shared primitives where possible.

Use `valuation_ratio()` for PER/PBR/forward PER and `market_cap_from_price_and_shares()` for gross market cap. Keep `calc_market_cap_scalar()` only where treasury-share/free-float semantics are explicitly required.

- [ ] Add/adjust tests in `test_fundamentals_service.py`.

Required tests:

- daily valuation after split returns adjusted PER/PBR/marketCap.
- daily valuation returns `per is None` for negative EPS.
- latest valuation includes `forwardPer` using revised forecast EPS when available.
- latest valuation falls back to FY forecast EPS when no revised forecast exists.

- [ ] Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/server/services/test_fundamentals_service.py -q
uv run --project apps/bt ruff check apps/bt/src/domains/fundamentals apps/bt/tests/server/services/test_fundamentals_service.py
uv run --project apps/bt pyright apps/bt/src/domains/fundamentals
```

Expected: all pass.

---

## Task 4: Use The SoT In RankingService

**Files:**
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`

- [ ] Add a conversion helper from DuckDB statement rows to `JQuantsStatement` or a fundamentals-domain accepted row type.

Do not copy valuation formulas into `ranking_service.py`. The service may load rows, group rows, and pass them to `FundamentalsCalculator`; it may not divide price by EPS/BPS itself.

- [ ] Replace `_load_value_composite_scored_frame()` valuation enrichment.

Current inline fields to remove from ranking service:

```python
bps = _adjust_per_share_value(...)
market_cap_bil_jpy = price * baseline_shares / 1_000_000_000.0
pbr = _positive_ratio(price, bps)
forward_per = _positive_ratio(price, forward_eps)
```

Replacement:

```python
valuation = self._fundamentals_calculator.calculate_latest_valuation(
    statements_for_code,
    close=price,
    price_date=target_date,
    prefer_consolidated=True,
    share_adjustment_events=adjustment_events_by_code.get(code, []),
)
```

Map fields:

- `pbr = valuation.pbr`
- `forward_per = valuation.forwardPer`
- `market_cap_bil_jpy = valuation.marketCap / 1_000_000_000.0`
- `forward_eps = valuation.forwardEps`

- [ ] Add ranking tests.

Required cases:

- value-composite row matches `FundamentalsCalculator.calculate_latest_valuation()` for PBR, forward PER, and market cap.
- stock split adjustment changes the value-composite ratios in the expected direction.
- non-positive forecast EPS makes `forward_per` unavailable and keeps the value-score unsupported reason stable.

- [ ] Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_ranking_service.py -q
uv run --project apps/bt ruff check apps/bt/src/application/services/ranking_service.py
uv run --project apps/bt pyright apps/bt/src/application/services/ranking_service.py
```

Expected: all pass.

---

## Task 5: Use The SoT In Sector Stocks

**Files:**
- Modify: `apps/bt/src/application/services/chart_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/chart.py`
- Test: `apps/bt/tests/unit/server/services/test_chart_service_branches.py`

- [ ] Delete the valuation SQL CTEs from `get_sector_stocks()`.

Remove:

- `actual_statement`
- `forecast_statement`
- `share_statement`
- `valuation_select_fields`

- [ ] Load sector current rows and statements separately.

Keep SQL responsible for:

- selected sector filter
- selected market filter
- current price/volume
- base price / change %
- trading value average

Move valuation enrichment to Python through `FundamentalsCalculator.calculate_latest_valuation()`.

- [ ] Add sort fields.

Support:

```text
tradingValue
changePercentage
code
per
forwardPer
pbr
marketCap
```

Null sorting rule:

```text
finite values first; nulls last for both asc and desc
```

- [ ] Add sector-stock tests.

Required cases:

- sector stocks use split-adjusted PER/PBR/marketCap from the fundamentals calculator.
- `forwardPer` matches the same forecast precedence as Symbol Workbench.
- sorting by `per`, `forwardPer`, `pbr`, and `marketCap` works with nulls last.
- `chart_service.py` no longer contains direct `curr.close / EPS/BPS` SQL fragments.

- [ ] Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_chart_service_branches.py -q
uv run --project apps/bt ruff check apps/bt/src/application/services/chart_service.py apps/bt/src/entrypoints/http/schemas/chart.py
uv run --project apps/bt pyright apps/bt/src/application/services/chart_service.py
```

Expected: all pass.

---

## Task 6: Commonize Strategy Signal And Loader Root Semantics

**Files:**
- Modify: `apps/bt/src/domains/strategy/signals/fundamental_valuation.py`
- Modify: `apps/bt/src/application/services/screening_market_loader.py`
- Modify: `apps/bt/src/infrastructure/data_access/loaders/data_preparation.py`
- Test: `apps/bt/tests/unit/strategies/signals/test_fundamental.py`
- Test: relevant focused loader tests identified during implementation.

- [ ] Update strategy valuation signals to call shared ratio primitives.

Keep function signatures unchanged:

- `is_undervalued_by_per(close, eps, ...)`
- `is_undervalued_by_pbr(close, bps, ...)`
- `is_undervalued_growth_by_peg(close, eps, next_year_forecast_eps, ...)`

The signal functions still accept prepared series; they do not load statements or adjustment events. They should share denominator validity and null semantics with product valuation.

- [ ] Audit `screening_market_loader.py`.

If it already uses `resolve_adjusted_shared_baseline_shares()` correctly, leave the data-shaping logic intact. Replace only duplicated positive-ratio or denominator checks with shared primitives. Do not change screening job API semantics in this task.

- [ ] Audit `data_preparation.py`.

If it already prepares adjusted per-share series correctly, leave the loader contract intact. Replace only duplicated root ratio/null helpers with shared primitives.

- [ ] Add tests that signal and product primitive semantics agree.

Required cases:

- PER signal excludes EPS `<= 0`, matching `valuation_ratio()`.
- PBR signal excludes BPS `<= 0`, matching `valuation_ratio()`.
- loader-prepared adjusted EPS/BPS can feed strategy signals without redoing share adjustment inside the signal.

- [ ] Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/strategies/signals/test_fundamental.py -q
uv run --project apps/bt ruff check apps/bt/src/domains/strategy/signals/fundamental_valuation.py apps/bt/src/application/services/screening_market_loader.py apps/bt/src/infrastructure/data_access/loaders/data_preparation.py
uv run --project apps/bt pyright apps/bt/src/domains/strategy/signals apps/bt/src/application/services/screening_market_loader.py apps/bt/src/infrastructure/data_access/loaders/data_preparation.py
```

Expected: all pass.

---

## Task 7: Contract Sync

**Files:**
- Modify: `apps/bt/src/entrypoints/http/schemas/ranking.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/chart.py`
- Generated: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Generated: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`

- [ ] Add optional valuation fields to `RankingItem`.

```python
per: float | None = None
forwardPer: float | None = None
pbr: float | None = None
marketCap: float | None = None
```

- [ ] Keep `SectorStockItem` compatible and document accepted sort fields in route parameter docs.

- [ ] Run:

```bash
bun run --filter @trading25/contracts bt:sync
bun run --filter @trading25/contracts bt:check
```

Expected: generated contracts are clean.

---

## Task 8: Shared Frontend Equity Table

**Files:**
- Create: `apps/ts/packages/web/src/components/EquityRankingTable/EquityRankingTable.tsx`
- Create: `apps/ts/packages/web/src/components/EquityRankingTable/index.ts`
- Test: `apps/ts/packages/web/src/components/EquityRankingTable/EquityRankingTable.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingTable.tsx`
- Modify: `apps/ts/packages/web/src/pages/IndicesPage.tsx`
- Modify: `apps/ts/packages/web/src/hooks/useSectorStocks.ts`
- Modify: `apps/ts/packages/web/src/types/ranking.ts`

- [ ] Create `EquityRankingTable` as a rendering component only.

It must not calculate valuation metrics. It only formats values received from the backend.

Required sort fields:

```ts
type EquityRankingSortField =
  | 'tradingValue'
  | 'changePercentage'
  | 'code'
  | 'per'
  | 'forwardPer'
  | 'pbr'
  | 'marketCap';
```

- [ ] Update `RankingTable.tsx`.

Keep the existing ranking-type tabs and pass the selected item list into `EquityRankingTable`.

- [ ] Update `IndicesPage.tsx`.

Replace the ad hoc sector table with `EquityRankingTable` and keep the chart/table compact layout.

- [ ] Update `useSectorStocks.ts`.

Extend `sortBy`:

```ts
sortBy?: 'tradingValue' | 'changePercentage' | 'code' | 'per' | 'forwardPer' | 'pbr' | 'marketCap';
```

- [ ] Add frontend tests.

Required cases:

- valuation columns render for Indices sector constituents.
- clicking PER/Fwd PER/PBR/時価総額 headers changes sort field.
- Ranking Daily still shows existing tabs and rows.
- frontend tests assert no valuation arithmetic exists in `EquityRankingTable`.

- [ ] Run:

```bash
bun run --filter @trading25/web test -- EquityRankingTable.test.tsx RankingTable.test.tsx IndicesPage.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/components/EquityRankingTable packages/web/src/components/Ranking/RankingTable.tsx packages/web/src/pages/IndicesPage.tsx packages/web/src/hooks/useSectorStocks.ts
```

Expected: all pass.

---

## Task 9: Audit Report For Remaining Valuation Math

**Files:**
- Create: `docs/valuation-sot-audit.md`

- [ ] Document all remaining valuation calculations found by:

```bash
rg -n "close\\s*/\\s*[^\\n]*(eps|bps|earnings|forecast)|current_price\\s*/\\s*|price\\s*/\\s*|close\\s*\\*\\s*.*shares|current_price\\s*\\*\\s*.*shares|market_cap_bil_jpy" apps/bt/src -g '*.py'
```

- [ ] Classify each hit into one of:

```text
product-soT
product-consumer
strategy-signal
research-experiment
shared-financial-helper
false-positive
```

- [ ] Document why research modules are not rewritten in this product change.

Required wording:

```text
Shared primitive semantics for denominator validity, positive-ratio/null handling, share-basis adjustment, and market-cap calculation are common infrastructure. Product-facing valuation display and ranking APIs must use FundamentalsCalculator valuation SoT. Strategy signals and batch loaders consume adjusted series or shared batch primitives, not UI-oriented single-symbol resolvers. Research modules may intentionally preserve experiment-specific entry timing, price basis, and bundle semantics; migrating published research requires a separate reproducibility review.
```

---

## Task 10: Browser Verification

**Files:**
- No committed files.

- [ ] Verify `http://localhost:5173/indices?code=0045`.

Checks:

- PER/PBR/forward PER no longer show split-stale values.
- negative EPS rows show `-` for PER.
- sorting by PER, Fwd PER, PBR, and 時価総額 works.
- nulls stay last.
- compact chart remains non-overlapping.

- [ ] Verify `http://localhost:5173/ranking?tab=ranking&dailyView=stocks`.

Checks:

- Daily ranking still renders all ranking tabs.
- shared table row click still navigates to `/symbol-workbench`.
- valuation columns appear only where configured.

- [ ] Compare one overlapping code against Symbol Workbench.

For a code visible in Indices:

1. Open `/symbol-workbench?symbol=<code>`.
2. Read latest Fundamentals panel `PER`, `PBR`, and market cap.
3. Confirm Indices sector row matches backend values within display rounding.

---

## Final Validation

```bash
uv run --project apps/bt pytest apps/bt/tests/server/services/test_fundamentals_service.py apps/bt/tests/unit/server/services/test_ranking_service.py apps/bt/tests/unit/server/services/test_chart_service_branches.py apps/bt/tests/unit/domains/fundamentals/test_valuation_sot_inventory.py -q
uv run --project apps/bt ruff check apps/bt/src/domains/fundamentals apps/bt/src/application/services/ranking_service.py apps/bt/src/application/services/chart_service.py apps/bt/src/entrypoints/http/schemas
uv run --project apps/bt pyright apps/bt/src/domains/fundamentals apps/bt/src/application/services/ranking_service.py apps/bt/src/application/services/chart_service.py
bun run --filter @trading25/contracts bt:check
bun run --filter @trading25/web test -- EquityRankingTable.test.tsx RankingTable.test.tsx IndicesPage.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/components/EquityRankingTable packages/web/src/components/Ranking/RankingTable.tsx packages/web/src/pages/IndicesPage.tsx packages/web/src/hooks/useSectorStocks.ts
git diff --check
```

Expected: all pass.
