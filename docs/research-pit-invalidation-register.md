# Research PIT Invalidation Register

This register tracks published research whose conclusions depend on historical universe membership. It complements each experiment README; the README remains the Published Readout SoT, and this file is the cross-experiment rerun queue.

## Invalidation Rule

A research readout is invalid for production, Ranking, Screening, or strategy
selection evidence when it fixes a latest or current membership set across
historical dates, or when TOPIX500 membership is approximated without an
explicit proxy label.

A valid rerun must use physical `market.duckdb` schema v5 with
`stock_price_adjustment_mode=provider_adjusted_v1`. It must resolve
each signal date with `stock_master_daily` and, for TOPIX500-dependent
universes, `index_membership_daily`. The run must verify provider plan/as-of,
effective coverage, source fingerprint, and event-ledger integrity from
`providerVintage`. Share-adjusted statement metrics and valuations must come
from current-basis `statement_metrics_adjusted` and the PIT `daily_valuation`
relation. Missing provider coverage, stale current-basis state, a future
disclosure join, or raw/current fallback invalidates the run.

## Status Classes

| Status | Meaning |
|---|---|
| `invalidated` | Published headline is withdrawn and removed from active research surfaces. |
| `rerun_required` | A runner/readout must be rebuilt with the Market v5 provider-adjusted contract and resolver-backed PIT universes before any downstream use. |
| `pit_safe` | Readout records physical Market schema v5, `provider_adjusted_v1`, signal-date universe resolution, exact provider-vintage lineage, and PIT disclosure provenance. |
| `historical_archive` | Run provenance is retained for audit only and cannot support current production, Ranking, Screening, or strategy-selection evidence. |

## Current Queue

| Experiment | Status | Blocker | Required rerun |
|---|---|---|---|
| `market-behavior/annual-large-universe-value-profile` | `rerun_required` | TOPIX500 / Prime ex TOPIX500 evidence must be checked against exact membership | rerun on Market v5 with `index_membership_daily.index_code = TOPIX500` and provider-vintage lineage |
| `market-behavior/annual-large-universe-factor-family` | `rerun_required` | TOPIX500 / Prime ex TOPIX500 evidence must be checked against exact membership | rerun on Market v5 with `index_membership_daily.index_code = TOPIX500` and provider-vintage lineage |
| `market-behavior/topix-gap-intraday-distribution` | `rerun_required` | the prior run predates the current Market v5 evidence contract | rerun with `stock_master_daily,index_membership_daily` and record the v5 provider-vintage verification |
| `market-behavior/stop-limit-daily-classification` | `rerun_required` | the prior run predates the current Market v5 evidence contract | rerun with signal-date `stock_master_daily` and record the v5 provider-vintage verification |
| `market-behavior/stop-limit-buy-only-next-close-followthrough` | `rerun_required` | the prior run predates the current Market v5 evidence contract | rerun with signal-date `stock_master_daily` and record the v5 provider-vintage verification |
| `market-behavior/topix-close-stock-overnight` | `rerun_required` | the prior run predates the current Market v5 evidence contract | rerun with `stock_master_daily,index_membership_daily` and record the v5 provider-vintage verification |
| `market-behavior/accumulation-flow-followthrough` | `rerun_required` | the prior run predates the current Market v5 evidence contract | rerun with `stock_master_daily,index_membership_daily` and record the v5 provider-vintage verification |

## Archived Historical Fallback-Removal Triage

2026-06-08 の fallback removal で、legacy README はすべて repo-side
`Published Readout` に移行した。移行は旧 headline の有効化ではなく、以下の
当時の扱いを記録したものである。この節の分類とrun IDは archived historical
provenanceであり、現行の`pit_safe`判定やproduction evidenceには使用しない。

| Historical class | Experiments | Archived disposition |
|---|---:|---|
| `invalidated` | 3 | 旧 headline を撤回し、PIT-safe rerun まで production / Ranking / Screening evidence にしない |
| `rerun_required` | 6 | high-value 以外は旧 headline を source markdown 上で保留する |
| `pit_safe` (historical label) | 5 | 当時のhigh-value rerun。現行v4 contractでは`historical_archive`として扱う |
| `archive` | 2 | descriptive / historical context として残し、現行導線では使わない |

### Current High-Value Market v5 Rerun Queue

Deleted invalid research must not be revived merely to preserve old headlines.
The following retained high-value readouts require a new Market v5 run before
they can regain `pit_safe` status.

| Priority | Experiment | Reason | Required fix before rerun |
|---:|---|---|---|
| 1 | `market-behavior/topix-gap-intraday-distribution` | active intraday universe evidence | exact TOPIX500 membership plus v5 provider-vintage evidence |
| 2 | `market-behavior/stop-limit-daily-classification` | active limit-event classification evidence | signal-date market membership plus v5 provider-vintage evidence |
| 3 | `market-behavior/stop-limit-buy-only-next-close-followthrough` | active follow-through evidence | signal-date parent membership plus v5 provider-vintage evidence |
| 4 | `market-behavior/topix-close-stock-overnight` | active overnight universe evidence | exact TOPIX500 membership plus v5 provider-vintage evidence |
| 5 | `market-behavior/accumulation-flow-followthrough` | active flow follow-through evidence | exact TOPIX500 membership plus v5 provider-vintage evidence |

### Archived Historical High-Value Reruns

The following are archived historical Market schema v3 runs. Their run IDs
retain the historical `pit_safe` label for provenance only; the rows are not
current `pit_safe` evidence.

| Experiment | Archived run ID | Historical universe source | Current disposition |
|---|---|---|---|
| `market-behavior/topix-gap-intraday-distribution` | `20260608_pit_safe_topix500` | `stock_master_daily,index_membership_daily` | `historical_archive`; Market v5 rerun required |
| `market-behavior/stop-limit-daily-classification` | `20260608_pit_safe_stock_master_daily` | `stock_master_daily` | `historical_archive`; Market v5 rerun required |
| `market-behavior/stop-limit-buy-only-next-close-followthrough` | `20260608_pit_safe_parent_stock_master_daily_v2` | `stock_master_daily` | `historical_archive`; Market v5 rerun required |
| `market-behavior/topix-close-stock-overnight` | `20260608_pit_safe_topix500` | `stock_master_daily,index_membership_daily` | `historical_archive`; Market v5 rerun required |
| `market-behavior/accumulation-flow-followthrough` | `20260608_pit_safe_topix500_v2` | `stock_master_daily,index_membership_daily` | `historical_archive`; Market v5 rerun required |

### Deleted Invalid Research

| Removed | Contamination classes |
|---|---|
| 2026-07-13 | future-derived parameter selection (including fixed-3/53 retrospective transfer); future-conditioned feature rows; historical universe membership leak |

## Publication Requirement

When a rerun is completed, update the experiment README `## Published Readout` with:

- `Data plane`: physical `market.duckdb` schema v5.
- `Adjustment mode`: `stock_price_adjustment_mode=provider_adjusted_v1`.
- `Universe source`: `stock_master_daily` or `stock_master_daily,index_membership_daily`.
- `As-of policy`: signal-date membership, no latest fallback.
- `Provider vintage lineage`: provider plan/as-of, effective coverage, source
  fingerprint, provider-window/event-ledger validity, and current-basis date.
- `Provider-basis sources`: when consumed, identify
  `statement_metrics_adjusted` / `daily_valuation` and prove every row uses a
  disclosure known at the research cutoff without service-local recomputation
  or fallback.
- `Invalidation disposition`: old headline withdrawn, replaced, or confirmed.

If exact TOPIX500 membership is unavailable, the runner must fail or the readout must use a `proxy` name. Do not publish a `TOPIX500` or `Prime ex TOPIX500` headline from scale-category approximation.
