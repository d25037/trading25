# Ranking Individual Stocks Evidence-Based Coloring Design

## Summary

Ranking page の `Individual Stocks` table で、`PER` / `Fwd PER` / `Fwd P/OP` / `PBR` / `流動性Z` のセル色を research evidence に基づく 5 tier へ変更する。

色は return interpretation に対応させる。

| Tier | Meaning | UI color |
| --- | --- | --- |
| `excellent` | return が特に良い evidence | green |
| `good` | return が良い evidence | sky/blue |
| `neutral` | 普通または evidence が弱い | no added color |
| `bad` | return 悪化または警戒 evidence | yellow |
| `very_bad` | return が特に悪い / tail risk が重い evidence | red |

## Approved Direction

採用するのは frontend-only の direct evidence rules。FastAPI / OpenAPI 契約は変更しない。Ranking API がすでに返す `per`, `forwardPer`, `forwardPOp`, `pbr`, `liquidityResidualZ`, `liquidityRegime` を使い、`EquityRankingTable` の表示 helper でセルごとに class を決める。

## Evidence Base

Primary evidence は既存 Published Readout を使う。

| Source | Relevant read |
| --- | --- |
| `apps/bt/docs/experiments/market-behavior/recent-return-threshold-forward-response/README.md` | General daily forward-return response. `PBR cheapest_10pct` and `Fwd PER cheapest_10pct` are positive; expensive PBR is clearly bad; `Fwd PER <= 20 AND Fwd P/OP >= 20` worsens median/win rate; rerating + liquidity state changes return distribution. |
| `apps/bt/docs/experiments/market-behavior/annual-fundamental-confounder-analysis/README.md` | Low PBR, small cap, and low forward PER keep independent positive effects; ADV60 is capacity/execution diagnostic, not alpha. |
| `apps/bt/docs/experiments/market-behavior/annual-market-specific-value-score-profile/README.md` | Ranking surface should treat Standard as PBR-heavy and Prime as size/Fwd-PER-heavy; PBR and Fwd PER are real ranking axes, not decorative fields. |
| `apps/bt/docs/experiments/market-behavior/free-float-liquidity-regime-decomposition/README.md` | `rerating_participation` / `neutral_rerating` is favorable; `distribution_stress` and `stale_liquidity` are unfavorable states. |
| `apps/bt/docs/experiments/market-behavior/pre-earnings-eps120-proxy/README.md` | Absolute valuation buckets already use practical bands: PER/Fwd PER/P/OP/Fwd P/OP `<=10`, `10-15`, `15-20`, `20-30`, `>30`; PBR `<=0.8`, `0.8-1.0`, `1.0-1.5`, `1.5-2.0`, `>2.0`. This is EPS-target evidence, so it is secondary for forward-return coloring. |

## Rule Matrix

The implementation should classify each cell independently, with row context only where the research explicitly requires it.

### PER

PER has weaker forward-return evidence than `Fwd PER` and `PBR`, but low PER is still a value proxy in the annual large-universe and bucket-family reads. Treat low positive PER as good, very high or non-positive PER as bad.

| Condition | Tier |
| --- | --- |
| `0 < per <= 10` | `excellent` |
| `10 < per <= 15` | `good` |
| `15 < per <= 25` or missing | `neutral` |
| `25 < per <= 40` | `bad` |
| `per <= 0` or `per > 40` | `very_bad` |

### Fwd PER

Replace the current `forwardPer > per` red / `forwardPer < per` green comparison. The evidence says low forward PER is positive, but low forward PER alone is weaker than low PBR in rerating slices.

| Condition | Tier |
| --- | --- |
| `0 < forwardPer <= 10` | `excellent` |
| `10 < forwardPer <= 20` | `good` |
| `20 < forwardPer <= 30` or missing | `neutral` |
| `30 < forwardPer <= 40` | `bad` |
| `forwardPer <= 0` or `forwardPer > 40` | `very_bad` |

### Fwd P/OP

`Fwd P/OP` is not a standalone replacement for `Fwd PER`; it is a quality check for low-forward-PER candidates. Therefore use `forwardPer` as context.

| Condition | Tier |
| --- | --- |
| `forwardPer <= 20` and `0 < forwardPOp <= 15` | `excellent` |
| `forwardPer <= 20` and `15 < forwardPOp < 20` | `good` |
| missing, `forwardPer > 20`, or `20 <= forwardPOp <= 30` without a low-forward-PER context | `neutral` |
| `30 < forwardPOp <= 40` | `bad` |
| `forwardPer <= 20` and `forwardPOp >= 20`, or `forwardPOp <= 0`, or `forwardPOp > 40` | `very_bad` |

If both `bad` and contextual `very_bad` match, contextual `very_bad` wins.

### PBR

PBR has the strongest direct evidence among the requested valuation columns in rerating participation. Cheap PBR gets stronger color than cheap Fwd PER; expensive PBR is a clear red risk.

| Condition | Tier |
| --- | --- |
| `0 < pbr <= 0.8` | `excellent` |
| `0.8 < pbr <= 1.0` | `good` |
| `1.0 < pbr <= 2.0` or missing | `neutral` |
| `2.0 < pbr < 3.0` | `bad` |
| `pbr <= 0` or `pbr >= 3.0` | `very_bad` |

### 流動性Z

Raw `liquidityResidualZ` alone is ambiguous; the research interpretation is state-based. Color the numeric `流動性Z` cell using `liquidityRegime` when present, with raw-Z fallback only for stale-style low residual.

| Condition | Tier |
| --- | --- |
| `liquidityRegime === "neutral_rerating"` | `excellent` |
| `liquidityRegime === "crowded_rerating"` | `good` |
| `liquidityRegime === "neutral"` or missing regime with `-1 < liquidityResidualZ < 1` | `neutral` |
| `liquidityRegime === "distribution_stress"` | `bad` |
| `liquidityRegime === "stale_liquidity"` or `liquidityResidualZ <= -1` | `very_bad` |

## UI Scope

Change only the `Individual Stocks` / equity ranking rendering path:

- `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx`
- nearby ranking table tests

Apply the same color helper on desktop table cells and mobile cards. Mobile currently shows `PER`, `Fwd PER`, `Fwd P/OP`, and `流動性Z`; add `PBR` to mobile valuation metrics so all target metrics are visible in both layouts.

Do not recolor `Market Cap`, `Med ADV60/FF`, liquidity state chips, row background, or sort headers.

## Implementation Shape

Introduce a small local helper near `EquityRankingTable`:

- `type EvidenceColorTier = "excellent" | "good" | "neutral" | "bad" | "very_bad"`
- `getEvidenceTierClass(tier)` maps to Tailwind classes.
- `getPerEvidenceTier(per)`
- `getForwardPerEvidenceTier(forwardPer)`
- `getForwardPOpEvidenceTier(forwardPOp, forwardPer)`
- `getPbrEvidenceTier(pbr)`
- `getLiquidityEvidenceTier(liquidityResidualZ, liquidityRegime)`

Use classes:

| Tier | Class |
| --- | --- |
| `excellent` | `text-green-600 dark:text-green-400` |
| `good` | `text-sky-600 dark:text-sky-400` |
| `neutral` | no added class |
| `bad` | `text-yellow-600 dark:text-yellow-400` |
| `very_bad` | `text-red-600 dark:text-red-400` |

`neutral` should return `undefined` so existing foreground text remains unchanged.

## Testing

Add or update focused frontend tests:

1. PER/PBR/Fwd PER/Fwd P/OP/流動性Z cells receive the expected classes for all five tiers where applicable.
2. Current `Fwd PER` comparison behavior is removed: `forwardPer < per` should not be the reason for green if the absolute `forwardPer` tier is neutral/bad.
3. `Fwd P/OP` contextual bad case is red when `forwardPer <= 20 AND forwardPOp >= 20`.
4. Mobile cards show PBR and use the same evidence color helpers.
5. Existing liquidity regime chip colors remain unchanged.

Run:

```bash
bun run --filter @trading25/web test -- RankingTable.test.tsx
```

If the package script does not support the file argument, use the repo's closest existing web test command and report the exact command.

## Non-Goals

- No backend API or OpenAPI schema change.
- No new research runner in this implementation slice.
- No attempt to make color tiers sector-relative or market-specific yet.
- No score-method change for value composite Ranking.

## Caveats

These rules are an evidence-backed interpretation layer, not a trading rule. The evidence is strongest for Prime/rerating daily response and annual value studies. Standard/Growth and sector-relative calibration may deserve a later research pass, but this change should avoid overfitting by using stable, readable thresholds already present in existing research.
