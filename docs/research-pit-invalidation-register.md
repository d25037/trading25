# Research PIT Invalidation Register

This register tracks published research whose conclusions depend on historical universe membership. It complements each experiment README; the README remains the Published Readout SoT, and this file is the cross-experiment rerun queue.

## Invalidation Rule

A research readout is invalid for production, Ranking, Screening, or strategy selection evidence when it fixes a latest or current membership set across historical dates, or when TOPIX500 membership is approximated without an explicit proxy label. A valid rerun must use `market.duckdb` schema v3, resolve each signal date with `stock_master_daily` and, for TOPIX500-dependent universes, `index_membership_daily`.

## Status Classes

| Status | Meaning |
|---|---|
| `invalidated` | Published headline is withdrawn and removed from active research surfaces. |
| `rerun_required` | A runner/readout should be rebuilt with resolver-backed PIT universes before any downstream use. |
| `pit_safe` | Readout explicitly uses schema v3 PIT universe resolution or does not depend on historical membership. |

## Current Queue

| Experiment | Status | Blocker | Required rerun |
|---|---|---|---|
| `market-behavior/annual-large-universe-value-profile` | `rerun_required` | TOPIX500 / Prime ex TOPIX500 evidence must be checked against exact membership | rerun with `index_membership_daily.index_code = TOPIX500` |
| `market-behavior/annual-large-universe-factor-family` | `rerun_required` | TOPIX500 / Prime ex TOPIX500 evidence must be checked against exact membership | rerun with `index_membership_daily.index_code = TOPIX500` |
| `market-behavior/topix-gap-intraday-distribution` | `pit_safe` | rerun completed with schema v3 `stock_master_daily,index_membership_daily` | run `20260608_pit_safe_topix500` replaces the old headline |

## Fallback Removal Triage

2026-06-08 の fallback removal で、legacy README はすべて repo-side
`Published Readout` に移行した。移行は旧 headline の有効化ではなく、以下の
扱いを明示するものとする。

| Class | Experiments | Action |
|---|---:|---|
| `invalidated` | 3 | 旧 headline を撤回し、PIT-safe rerun まで production / Ranking / Screening evidence にしない |
| `rerun_required` | 6 | high-value 以外は旧 headline を source markdown 上で保留する |
| `pit_safe` | 5 | high-value rerun 済み。旧 headline を新 readout で置き換えた |
| `archive` | 2 | descriptive / historical context として残し、現行導線では使わない |

### High-Value PIT-Safe Rerun Queue

The rerun queue is intentionally smaller than the legacy set. Do not rerun
archived or invalidated work just to preserve old headlines.

| Priority | Experiment | Reason | Required fix before rerun |
|---:|---|---|---|
| (none) | (completed) | all high-value fallback-removal reruns are complete | see completed table below |

### Completed High-Value PIT-Safe Reruns

| Experiment | Run ID | Universe source | Disposition |
|---|---|---|---|
| `market-behavior/topix-gap-intraday-distribution` | `20260608_pit_safe_topix500` | `stock_master_daily,index_membership_daily` | old TOPIX500 / Prime ex TOPIX500 headline replaced |
| `market-behavior/stop-limit-daily-classification` | `20260608_pit_safe_stock_master_daily` | `stock_master_daily` | old latest-market grouping headline replaced |
| `market-behavior/stop-limit-buy-only-next-close-followthrough` | `20260608_pit_safe_parent_stock_master_daily_v2` | `stock_master_daily` | old parent-grouping-dependent headline replaced |
| `market-behavior/topix-close-stock-overnight` | `20260608_pit_safe_topix500` | `stock_master_daily,index_membership_daily` | old universe-policy-unconfirmed headline replaced |
| `market-behavior/accumulation-flow-followthrough` | `20260608_pit_safe_topix500_v2` | `stock_master_daily,index_membership_daily` | old current-market / scale-proxy headline replaced |

### Deleted Invalid Research

| Removed | Contamination classes |
|---|---|
| 2026-07-13 | future-derived parameter selection (including fixed-3/53 retrospective transfer); future-conditioned feature rows; historical universe membership leak |

## Publication Requirement

When a rerun is completed, update the experiment README `## Published Readout` with:

- `Universe source`: `stock_master_daily` or `stock_master_daily,index_membership_daily`.
- `As-of policy`: signal-date membership, no latest fallback.
- `Schema`: observed `market_schema_version`.
- `Invalidation disposition`: old headline withdrawn, replaced, or confirmed.

If exact TOPIX500 membership is unavailable, the runner must fail or the readout must use a `proxy` name. Do not publish a `TOPIX500` or `Prime ex TOPIX500` headline from scale-category approximation.
