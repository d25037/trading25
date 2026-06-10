# Daily Ranking Research Base

Status: `living`
Owner surface: Daily Ranking / Ranking research
Tracking issue: https://github.com/d25037/trading25/issues/431
Last reviewed: 2026-06-10

This document is the cross-cutting source of truth for using current Daily
Ranking production semantics as the base layer for future parameter research.
It does not replace individual experiment READMEs. Published conclusions still
come from `apps/bt/docs/experiments/*/*/README.md` `## Published Readout`.

## Purpose

Daily Ranking is no longer a raw market ranking table. It is a production
investment-decision surface built from multiple research readouts:

- valuation percentiles: `PER`, `Fwd PER`, `Fwd P/OP`, `PBR`
- liquidity and rerating state: `liquidityResidualZ`, `liquidityRegime`
- sector strength families: `balanced_sector_strength`, `long_hybrid_leadership`
- technical and risk states: `overheat`, `stale_rally_fade`,
  `atr20_acceleration`, `momentum_20_60_top20`
- market regime overlays: bubble footprint / rerating regime context
- long-side and short-side evidence layers

Future parameter research must start from this structure. A candidate parameter
should be evaluated as an addition to a specific axis, not as a detached
average-return study.

## Initial Plan Snapshot

The initial plan for this base layer was:

1. Create a Daily Ranking production evidence map.
2. Separate the semantic axes: valuation, liquidity/rerating, sector,
   technical, market regime, and short/red.
3. Fix a reusable evaluation protocol for new parameters.
4. Build future runners from a common Daily Ranking state panel where practical.
5. Define promotion gates before a readout can change production Ranking.
6. Track the work in GitHub Issues and keep repo Markdown as documentation, not
   an active queue.

The completed document refines that plan by making the production field map,
readout map, candidate-evaluation gates, and reusable research panel API
explicit.

## Implemented Artifacts

| Artifact | Status | Use |
| --- | --- | --- |
| `apps/bt/src/domains/analytics/daily_ranking_research_base.py` | implemented | Public Daily Ranking research panel builder and query-bound helpers |
| `create_daily_ranking_research_panel(...)` | implemented | Creates reusable as-of Daily Ranking state temp views for future parameter runners |
| `daily_ranking_research_panel` | implemented temp view | Base as-of stock-day panel with valuation, liquidity, recent returns, TOPIX forward/excess returns |
| `daily_ranking_research_ranked` | implemented temp view | Market/date valuation percentiles and forward valuation relation percentiles |
| `daily_ranking_research_liquidity_ranked` | implemented temp view | Liquidity-regime scoped ranked panel for regime x valuation research |
| `forecast_operating_profit_growth_ratio` fast column | implemented | Daily valuation-basis `p_op / forward_p_op` equivalent of PIT latest forecast operating profit / operating profit |
| `per_to_fop_growth_ratio` / `forward_per_to_fop_growth_ratio` fast columns | implemented | Reusable valuation-to-growth ratio features for future Daily Ranking parameter studies |
| `valuation_signal` and valuation booleans | implemented | Base SoT for UI-aligned `Deep Value`, `Undervalued`, `Overvalued`, `Very Overvalued`, `No Earnings`, and `no_value_confirmation` semantics. Canonical internal columns are `overvalued_warning` / `very_overvalued_warning`; do not add ambiguous positive-sounding aliases. |
| `include_liquidity_ranked` option | implemented | Allows valuation-only or market-scope studies to skip liquidity-scope percentile reranking |
| `ranking_short_red_evidence.py` | migrated | Uses the public research base instead of importing Ranking Color private panel builders |
| `ranking_sector_strength_evidence.py` | migrated | Uses the public research base for its Daily Ranking state panel |
| `test_ranking_color_evidence.py` | updated | Covers public panel aliases and market-scope normalization |

The public temp views intentionally mirror the proven `ranking_color_*` legacy
tables. Existing research runners remain compatible, while new runners should
prefer the `daily_ranking_research_*` names and helper functions.

## Source Priority

Use sources in this order when deciding what Daily Ranking means:

| Priority | Source | Use |
| ---: | --- | --- |
| 1 | `apps/bt/docs/experiments/*/*/README.md` `## Published Readout` | Published conclusions, decisions, production implications, caveats |
| 2 | `docs/research-pit-invalidation-register.md` | Blocks invalidated or rerun-required evidence from production use |
| 3 | `apps/bt/docs/experiments/research-catalog-metadata.toml` | Discovery, related experiments, compact decision metadata |
| 4 | `apps/bt/src/domains/analytics/*` and `apps/bt/scripts/research/*` | Calculation logic, table names, rerun path |
| 5 | Bundle output `manifest.json + results.duckdb + summary.md` | Numerical confirmation and diagnostics after the readout is identified |
| 6 | UI/API implementation | Current production behavior, not a substitute for research evidence |

`summary.json`, legacy digest fields, and chat summaries are not publication
sources.

## Production Structure

### Backend Contract

The Daily Ranking API response is `MarketRankingResponse`, with individual
stock rows represented by `RankingItem` in
`apps/bt/src/entrypoints/http/schemas/ranking.py`.

Current production fields used by research-backed interpretation:

| Axis | API fields | Backend source |
| --- | --- | --- |
| Valuation | `per`, `perPercentile`, `forwardPer`, `forwardPerPercentile`, `pOp`, `forwardPOp`, `forwardPOpPercentile`, `pbr`, `pbrPercentile`, `marketCap` | `ranking_valuation.py`, `daily_valuation`, adjusted fallback only when required |
| Liquidity / rerating | `liquidityResidualZ`, `liquidityRegime`, `adv60ToFreeFloatPct` | `ranking_liquidity.py` |
| Risk flags | `riskFlags` containing `overheat`, `stale_rally_fade` | `ranking_liquidity.py`, `ranking_state_flags.py` |
| Technical flags | `technicalFlags` containing `atr20_acceleration`, `momentum_20_60_top20` | `ranking_technical_flags.py` |
| Sector | `sectorStrengthScore`, `sectorStrengthBucket`, `sectorStrengthFamily` | `ranking_index_performance.py` |
| Index / sector page context | `indexPerformance[*]` and sector strength diagnostics | `ranking_index_performance.py` |

`RankingService.get_rankings()` enriches valuation, liquidity, technical flags,
sector strength, filters, and reranking. When a new parameter needs production
use, it must be clear whether it is:

- a new `RankingItem` field,
- a derived UI-only interpretation of existing fields,
- a backend filter state,
- a table display filter,
- a research-only diagnostic.

### Frontend Contract

Current user-facing Daily Ranking semantics are primarily held in:

| File | Role |
| --- | --- |
| `apps/ts/packages/web/src/components/Ranking/rankingEvidenceTiers.ts` | Valuation and liquidity evidence tier rules, valuation signal chips |
| `apps/ts/packages/web/src/components/Ranking/rankingState.ts` | Preset labels and preset-to-filter mapping |
| `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx` | Visible table and mobile card rendering for valuation, sector, liquidity, and signals |
| `apps/ts/packages/web/src/components/Ranking/RankingFilters.tsx` | API-level Ranking filters and sector strength family selection |
| `apps/ts/packages/web/src/pages/RankingPage.tsx` | Daily Ranking tab wiring, API params, market regime banner context |

`rankingState.ts` is the source of truth for preset labels such as
`Momentum Value`, `Neutral Good`, `Crowded Good`, `Rally Fade`, and `Stale`.

## Evidence Map

| Production axis | Controlling readout | Production interpretation |
| --- | --- | --- |
| Valuation color tiers | `market-behavior/ranking-color-evidence` | Use target-date Prime percentile evidence. Low PBR / low Fwd PER are favorable UI evidence; high PBR / high Fwd PER / high PER / high Fwd P/OP are caution or red. Absolute PER/PBR thresholds are not the Ranking coloring base. |
| Fwd PER / PER and Fwd P/OP / PER relations | `market-behavior/ranking-color-evidence`, `market-behavior/ranking-good-forward-valuation-chain` | Exact low-PER `Fwd PER / PER <= 0.8` can strengthen value confirmation. `PER > Fwd PER > Fwd P/OP` is not a broad hard filter; at most a `Crowded Good` badge or tie-breaker. |
| Forecast operating profit growth | `market-behavior/ranking-forecast-operating-profit-growth-evidence` | Growth alone does not justify high PER / high Fwd PER and is not a long hard filter. Long-side use is `Deep Value + Long Hybrid Leadership + ATR20 Accel`, with growth as continuation badge / tie-breaker. Short-side use reads low/missing growth and contraction under `Overvalued / Very Overvalued + Sector Weak`, then escalates pure-short priority with `Crowded No Value` / `Crowded Overvalued` and ATR overheat. |
| Liquidity Z and rerating | `market-behavior/ranking-color-evidence`, `market-behavior/free-float-liquidity-regime-decomposition` | Raw `liquidityResidualZ` is not "higher is better". Interpret `neutral_rerating` / `crowded_rerating` through value confirmation. `stale_liquidity` is investability/capacity caution. |
| Balanced Sector Strength family | `market-behavior/ranking-sector-strength-evidence` | `Balanced Sector Strength` is average of official sector-index strength and constituent/breadth strength. Use as confidence/priority overlay, not a standalone alpha claim. |
| Long Sector Leadership family | `market-behavior/ranking-long-sector-leadership-horizon-decomposition` | Long-side candidate family. Useful especially ex Banks, but not a replacement for Balanced Sector Strength. Needs sector-cap or sector-balanced portfolio lens before stronger adoption. |
| Momentum Value annual/factor context | `market-behavior/ranking-core-factor-regime-breakdown` | `Momentum Value + Balanced Sector Strength: Strong` is bank-concentration-sensitive and should be treated as a diagnostic or sector bet until portfolio constraints prove otherwise. `NT 60D Regime` is a benchmark-headwind diagnostic, not a trading rule. |
| ATR20 acceleration | `market-behavior/atr-expansion-forward-response`, Ranking technical-state implementation | `atr20_acceleration` is a separate technical confirmation state. Do not fold it into liquidity regime or generic risk flags. |
| Overheat | `market-behavior/short-term-shock-forward-response`, Ranking constants | `overheat` is `recent_return_20d_pct >= 30.0`, a price-rally risk flag. It is not the same as ATR expansion. |
| Market bubble footprint | `market-behavior/market-bubble-footprint`, `market-behavior/rerating-bubble-regime-forward-response` | Market overlay for exposure and holding-horizon caution. `blowoff_watch` weakens broad rerating exposure, especially `crowded_rerating` and no-value cases. |
| Short / red candidates | `market-behavior/ranking-short-red-evidence`, `market-behavior/ranking-short-sector-strength-evidence` | Short-side evidence is independent of long-side colors. Reader-facing wording should use `Stale Overvalued`, `Stale Rally Fade`, `Crowded Overvalued`, and `Crowded No Value`; new Base-backed research should use `overvalued_warning` / `very_overvalued_warning` internally. `Crowded Overvalued / No Value + Sector Weak` is cleaner pure-short evidence. |

## Semantic Axes

### Valuation

Valuation is a relative percentile layer, not an absolute threshold layer.

Rules of thumb:

- `PBR` and `Fwd PER` have the clearest direct low/high percentile evidence.
- `PER` cheapness is weaker than PBR/Fwd PER, but high PER remains cautionary.
- `Fwd P/OP` is mainly a quality check for low-forward-PER candidates.
- `forecast_operating_profit_growth_ratio` should be read as a valuation
  denominator or relative-quality overlay, not as a standalone growth signal.
- `PER / growth` and `Fwd PER / growth` low percentile buckets can be
  evaluated as tie-breaker/badge candidates; high growth must not rescue
  Overvalued / No Value states by itself.
- Missing positive earnings valuation must not silently fall into optimistic
  buckets.
- New valuation parameters must be evaluated against existing value tiers, not
  only across the full market.
- Reader-facing research and UI copy must use `Overvalued` / `Very Overvalued`,
  and must avoid wording that can sound like good value. New research code should
  use `overvalued_warning` / `very_overvalued_warning`; do not add new internal
  aliases with wording that can be misread as positive value.

### Liquidity / Rerating

Liquidity is an investability and rerating-state layer.

Rules of thumb:

- `neutral_rerating` is the cleaner long-side base, especially with value
  confirmation.
- `crowded_rerating` can have right-tail upside but left-tail risk is heavier.
- `distribution_stress` is caution.
- `stale_liquidity` is not undervaluation; it is stale/capacity caution.
- New liquidity parameters must show whether they improve `neutral_rerating`,
  de-risk `crowded_rerating`, identify stale/fade cases, or only explain
  capacity.

### Sector

Sector is an overlay layer. It should not rescue weak value evidence.

Rules of thumb:

- `Balanced Sector Strength` remains the production baseline.
- `sector_strong` raises priority for value-backed long candidates.
- `sector_weak` confirms selected short/red candidates, but is not a universal
  short gate.
- `Long Hybrid Leadership` is a long-side research candidate, not a full
  replacement.
- Sector research must report bank share, sector concentration, and preferably
  sector-capped or sector-balanced portfolio diagnostics before strategy-level
  promotion.

### Technical / Risk

Technical and risk flags are separate from liquidity.

Rules of thumb:

- `overheat` means price rally risk.
- `atr20_acceleration` is continuation/volatility confirmation after excluding
  extreme overheat.
- `momentum_20_60_top20` is a momentum confirmation used by `Momentum Value`.
- `stale_rally_fade` is a stale Overvalued state after recent positive
  20D/60D return.
- New technical parameters must declare whether they are timing, confirmation,
  warning, or sizing features.

### Market Regime

Market regime is an exposure and interpretation overlay.

Rules of thumb:

- Bubble footprint does not make a standalone buy/sell signal.
- `narrowing` can still support value-backed `neutral_rerating`.
- `blowoff_watch` is a holding-horizon and sizing caution, especially for broad
  rerating and crowded/no-value cases.
- New market-regime parameters must be tested as conditioning overlays, not
  mixed into individual-stock value/liquidity tiers by default.

### Short / Red

Short-side research must use short-side outcome language.

Rules of thumb:

- Report raw, TOPIX, and TOPIX-excess return.
- Use negative return rate, downside tail, and upside tail instead of long-side
  win-rate-only language.
- Separate pure short, relative short / hedge, long avoidance, and caution.
- Do not infer short rules by inverting green/blue long rules.

## New Parameter Evaluation Protocol

Every new Daily Ranking parameter proposal should answer these questions before
implementation:

1. Which semantic axis does it belong to?
2. Which current production states does it refine?
3. Is it observable at the intended decision time?
4. Which existing readout is the nearest baseline?
5. Does it improve a live production decision, or is it only explanatory?
6. Is the required data already in `market.duckdb` with PIT-safe availability?
7. Is the outcome observation-level, portfolio-level, or execution-level?
8. What production surface would it change if accepted?

Minimum slicing for first-pass evidence:

| Slice | Required read |
| --- | --- |
| Market | `prime` first; do not extrapolate to Standard/Growth without evidence |
| Horizon | 5D / 10D / 20D / 60D where applicable; 20D is the usual primary UI evidence horizon |
| Outcome | TOPIX excess primary for Ranking color/evidence; raw and TOPIX also shown for short/red |
| Presets | `Neutral Good`, `Crowded Good`, `Momentum Value`, `Crowded All`, `Stale`, `Rally Fade` when relevant |
| Sector | `sector_strong`, `sector_neutral`, `sector_weak`; include bank share for sector-led results |
| Market regime | normal / narrowing / crowded / blowoff where the parameter claims regime dependence |
| Tail | severe loss or downside/upside tail; not only mean or median |
| Coverage | observation count, code count, date range, missingness |

Promotion classification:

| Classification | Meaning | Typical production target |
| --- | --- | --- |
| `hard_filter` | Strong enough to remove candidates | Backend filter or strategy rule; requires stronger evidence and usually portfolio lens |
| `confidence_overlay` | Improves prioritization but should not remove candidates alone | Badge, chip, score overlay, tie-breaker |
| `caution_overlay` | Warns about tail, market, or execution risk | Warning chip, color downgrade, sizing note |
| `diagnostic` | Explains results or market state but does not alter selection | Readout only, Workbench panel, Research detail |
| `rejected` | Tested and not useful for current production | Readout conclusion; do not reintroduce without new hypothesis |

## Runner Base Requirements

Future Daily Ranking parameter runners should prefer a shared state panel shape.
The panel should be built as-of `signal_date` and include:

- normalized code and PIT market/sector membership
- target-date valuation fields from `daily_valuation`
- existing Daily Ranking value-confirmation state
- liquidity regime and recent return inputs
- sector strength family and sector bucket
- technical/risk flags
- TOPIX and raw forward returns for requested horizons
- optional market-regime context when the hypothesis needs it

Implementation guidance:

- Use `create_daily_ranking_research_panel(...)` from
  `apps/bt/src/domains/analytics/daily_ranking_research_base.py` before adding
  custom parameter joins.
- Use `daily_ranking_query_start_date(...)` and
  `daily_ranking_query_end_date(...)` for warmup and forward-horizon bounds
  instead of reimplementing date padding in each runner.
- Put reusable calculation logic in `apps/bt/src/domains/analytics/`.
- Put thin runner entrypoints in `apps/bt/scripts/research/`.
- Use `apps/bt/src/shared/utils/pit_guard.py` helpers where latest-per-group
  selection is needed.
- Prefer DuckDB SQL for panel construction and aggregation.
- Keep vectorbt / portfolio construction as a separate step when the question is
  portfolio-level.
- Publish durable conclusions in the experiment README `## Published Readout`.

Minimal new-runner shape:

```python
from src.domains.analytics.daily_ranking_research_base import (
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)

market_scopes = normalize_daily_ranking_market_scopes(("prime",))
query_start = daily_ranking_query_start_date(start_date, warmup_calendar_days=720)
query_end = daily_ranking_query_end_date(end_date, max_horizon=max(horizons))

assert_daily_ranking_research_tables(conn)
panel = create_daily_ranking_research_panel(
    conn,
    query_start=query_start,
    query_end=query_end,
    analysis_start_date=start_date,
    analysis_end_date=end_date,
    horizons=horizons,
    market_scopes=market_scopes,
    include_liquidity_ranked=False,  # set True when liquidity-scope reranking is needed
)
```

Custom studies should then join additional PIT-safe parameter features onto
`panel.ranked_table` or `panel.liquidity_ranked_table`.

When a candidate parameter can be expressed from `daily_valuation`, prefer the
base fast columns before adding a `statements` as-of join. Examples now exposed
on `panel.ranked_table`:

| Column | Formula | Use |
| --- | --- | --- |
| `forecast_operating_profit_growth_ratio` | `p_op / forward_p_op` | PIT daily valuation-basis forecast operating profit / operating profit |
| `forecast_operating_profit_growth_pct` | `(forecast_operating_profit_growth_ratio - 1) * 100` | Human-readable growth rate |
| `per_to_fop_growth_ratio` | `per / forecast_operating_profit_growth_ratio` | PER adjusted by forecast OP growth |
| `forward_per_to_fop_growth_ratio` | `forward_per / forecast_operating_profit_growth_ratio` | Fwd PER adjusted by forecast OP growth |

Use `statement_metrics_adjusted` or `statements` only when the research needs
disclosed-date provenance, document-level diagnostics, or a metric that is not
already materialized into `daily_valuation`.

## Promotion Gates

A parameter can influence production Daily Ranking only if all applicable gates
are satisfied:

| Gate | Required evidence |
| --- | --- |
| Published readout | Complete `## Published Readout` with Decision, Main Findings, Interpretation, Production Implication, Caveats, Source Artifacts |
| PIT safety | Not blocked by `docs/research-pit-invalidation-register.md`; explicit signal-date universe/as-of rules |
| Existing-axis fit | Clear placement in valuation, liquidity, sector, technical, market regime, or short/red |
| Production target | Explicit target: API field, UI chip, filter, preset, score family, warning, or research-only diagnostic |
| Regression surface | Tests or focused checks for backend schema, contracts, UI helper, and route state if touched |
| Live QA | Ranking presets, historical dates, and concrete Symbol Workbench drilldowns when UI semantics change |
| Portfolio gate | Required before claims about sizing, turnover, capacity, sector caps, or strategy adoption |

## Live QA Surface

When production semantics change, validate more than one current row:

- Ranking page `Individual Stocks`, Prime, `Lookback Days: 1 day`
- Presets: `Crowded All`, `Crowded Good`, `Neutral Good`, `Momentum Value`,
  `Stale`, `Rally Fade`
- Historical dates used in recent regressions: `2026-05-20`, `2026-05-27`,
  `2026-03-18`; add the current latest trading date
- Inspect right-side columns: `Signals`, `流動性Z`, market cap, trading value,
  change columns, sector strength
- Drill selected symbols into `Symbol Workbench` to confirm chart, metadata,
  valuation, liquidity, and panel context
- If sector or market-regime work changed, also inspect `Indices` and the
  market-regime banner

## Maintenance Rules

- Update this document when a new readout changes production Daily Ranking
  semantics.
- Do not add new active tasks here. Use GitHub Issues for open work.
- If a readout is invalidated or rerun-required, update the evidence map or mark
  the axis as blocked by `docs/research-pit-invalidation-register.md`.
- Keep product labels aligned with `rankingState.ts` and
  `rankingEvidenceTiers.ts`; do not let research-only helper names become user
  labels without review.
- Keep Standard/Growth implications separate until a readout explicitly covers
  those markets.
