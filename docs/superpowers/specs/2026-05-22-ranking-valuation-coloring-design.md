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

採用するのは Prime-only の percentile-based evidence rules。PER / Fwd PER / Fwd P/OP / PBR の絶対値は年ごとの valuation regime に強く依存するため、色分けには使わない。Ranking API が target date の Prime cross-section で計算した percentile を返し、frontend はその percentile と `liquidityRegime` を使ってセルごとに class を決める。

Standard / Growth への外挿はこの初期実装の根拠にしない。

## Evidence Base

Primary evidence は既存 Published Readout を使う。

| Source | Relevant read |
| --- | --- |
| `apps/bt/docs/experiments/market-behavior/ranking-color-evidence/README.md` | Prime-only UI coloring evidence. PBR/Fwd PER high percentile is clearly bad; low percentile is relatively better; `Fwd PER / PER` and `Fwd P/OP / PER` relation percentiles were checked but did not beat standalone percentile evidence; `crowded_rerating` is caution rather than green. |
| `apps/bt/docs/experiments/market-behavior/recent-return-threshold-forward-response/README.md` | General daily forward-return response. `PBR cheapest_10pct` and `Fwd PER cheapest_10pct` are positive; expensive PBR is clearly bad; rerating + liquidity state changes return distribution. The follow-up for this feature must add relative `Fwd PER x Fwd P/OP` evidence instead of absolute `Fwd PER <= 20 AND Fwd P/OP >= 20`. |
| `apps/bt/docs/experiments/market-behavior/annual-fundamental-confounder-analysis/README.md` | Low PBR, small cap, and low forward PER keep independent positive effects; ADV60 is capacity/execution diagnostic, not alpha. |
| `apps/bt/docs/experiments/market-behavior/annual-market-specific-value-score-profile/README.md` | Ranking surface should treat Standard as PBR-heavy and Prime as size/Fwd-PER-heavy; PBR and Fwd PER are real ranking axes, not decorative fields. |
| `apps/bt/docs/experiments/market-behavior/free-float-liquidity-regime-decomposition/README.md` | Background for liquidity residual semantics. For this UI coloring task, the Prime-only `ranking-color-evidence` readout overrides the older green interpretation of crowded rerating. |
| `apps/bt/docs/experiments/market-behavior/pre-earnings-eps120-proxy/README.md` | Older absolute valuation buckets exist, but they are EPS-target evidence and are not suitable as UI coloring thresholds. Keep this source as background only. |

## Rule Matrix

The implementation should classify valuation cells by percentile, with row context only where the research explicitly requires it. Percentiles are raw rank percentiles in `[0, 1]`, where lower means cheaper valuation. They must be computed against the full target-date Prime universe on the backend, not against the limited rows currently visible in the frontend table.

### PER

PER has weaker Prime forward-return evidence than `Fwd PER` and `PBR`, but high PER is still a bad bucket and low PER is a weak value proxy. Use the target-date Prime-relative `perPercentile`.

| Condition | Tier |
| --- | --- |
| `perPercentile <= 0.10` | `good` |
| `0.10 < perPercentile <= 0.20` | `good` |
| `0.20 < perPercentile < 0.80` or missing | `neutral` |
| `0.80 <= perPercentile < 0.90` | `bad` |
| `perPercentile >= 0.90` | `very_bad` |

### Fwd PER

Replace the current `forwardPer > per` red / `forwardPer < per` green comparison. Prime evidence says low forward PER is relatively favorable and expensive forward PER is clearly poor. Use `forwardPerPercentile`.

The follow-up `Fwd PER / PER` percentile check did not support reviving the old comparison rule by itself. However, the exact-ratio check under low PER did support a narrow positive condition: `perPercentile <= 0.20 AND forwardPer / per <= 0.8` was materially better than standalone low PER. Use that as a green override, with `<= 1.0` as blue, then fall back to standalone Prime-relative `forwardPerPercentile`.

| Condition | Tier |
| --- | --- |
| `perPercentile <= 0.20` and `forwardPer / per <= 0.80` | `excellent` |
| `perPercentile <= 0.20` and `0.80 < forwardPer / per <= 1.00` | `good` |
| `forwardPerPercentile <= 0.10` | `excellent` |
| `0.10 < forwardPerPercentile <= 0.20` | `good` |
| `0.20 < forwardPerPercentile < 0.80` or missing | `neutral` |
| `0.80 <= forwardPerPercentile < 0.90` | `bad` |
| `forwardPerPercentile >= 0.90` | `very_bad` |

### Fwd P/OP

`Fwd P/OP` is not a standalone replacement for `Fwd PER`; Prime evidence is weaker standalone. It is mainly a quality check for low-forward-PER candidates. Therefore use `forwardPerPercentile` as context and `forwardPOpPercentile` for the operating-profit side.

The follow-up `Fwd P/OP / PER` exact-ratio check does not create a good condition: `<= 0.8` was only roughly neutral. It does add a weak caution condition under low PER when `forwardPOp / per > 1.25`.

| Condition | Tier |
| --- | --- |
| `forwardPerPercentile <= 0.20` and `forwardPOpPercentile <= 0.10` | `good` |
| `forwardPerPercentile <= 0.20` and `0.10 < forwardPOpPercentile <= 0.20` | `good` |
| missing, or no low-forward-PER context and `0.20 < forwardPOpPercentile < 0.80` | `neutral` |
| `perPercentile <= 0.20` and `forwardPOp / per > 1.25` | `bad` |
| `0.80 <= forwardPOpPercentile < 0.90` | `bad` |
| `forwardPOpPercentile >= 0.90` | `very_bad` |

If both neutral/good and contextual `bad` match, contextual `bad` wins.

### PBR

PBR has the strongest direct Prime evidence among the requested valuation columns. Cheap PBR gets the strongest green; expensive PBR is the clearest red risk. Use `pbrPercentile`.

| Condition | Tier |
| --- | --- |
| `pbrPercentile <= 0.10` | `excellent` |
| `0.10 < pbrPercentile <= 0.20` | `good` |
| `0.20 < pbrPercentile < 0.80` or missing | `neutral` |
| `0.80 <= pbrPercentile < 0.90` | `bad` |
| `pbrPercentile >= 0.90` | `very_bad` |

### 流動性Z

Raw `liquidityResidualZ` alone is ambiguous; Prime evidence says liquidity state is a rerating/crowding/investability diagnostic rather than "higher is better". Color the numeric `流動性Z` cell using `liquidityRegime x value confirmation` when present, with raw-Z fallback only for stale-style low residual.

Green is intentionally narrow. For `neutral_rerating`, green requires `perPercentile <= 0.20 AND forwardPer / per <= 0.80`. For `crowded_rerating`, green requires either `pbrPercentile <= 0.20 AND forwardPerPercentile <= 0.20`, or `perPercentile <= 0.20 AND forwardPer / per <= 0.80`. Medium value confirmation for `crowded_rerating` remains blue and includes `pbrPercentile <= 0.20` or `perPercentile <= 0.20 AND forwardPer / per <= 1.00`, but high valuation percentiles (`PER` / `Fwd PER` / `Fwd P/OP` / `PBR >= 0.80`) and missing positive earnings valuation (`PER` and `Fwd PER` percentiles both null, including negative-PER cases) veto blue unless the narrow green confirmation already matched.

| Condition | Tier |
| --- | --- |
| `liquidityRegime === "neutral_rerating"` and `perPercentile <= 0.20 AND forwardPer / per <= 0.80` | `excellent` |
| `liquidityRegime === "neutral_rerating"` otherwise | `good` |
| `liquidityRegime === "crowded_rerating"` and green confirmation | `excellent` |
| `liquidityRegime === "crowded_rerating"` and both `perPercentile` / `forwardPerPercentile` are null | `bad` |
| `liquidityRegime === "crowded_rerating"` and any valuation percentile `>= 0.80` | `bad` |
| `liquidityRegime === "crowded_rerating"` and medium value confirmation | `good` |
| `liquidityRegime === "crowded_rerating"` without value confirmation | `bad` |
| `liquidityRegime === "neutral"` or missing regime with `-1 < liquidityResidualZ < 1` | `neutral` |
| `liquidityRegime === "distribution_stress"` | `bad` |
| `liquidityRegime === "stale_liquidity"` or `liquidityResidualZ <= -1` | `bad` |

## UI Scope

Change the Ranking API contract and the `Individual Stocks` / equity ranking rendering path:

- `apps/bt/src/entrypoints/http/schemas/ranking.py`
- `apps/bt/src/application/services/ranking_service.py`
- `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx`
- nearby ranking table tests

Apply the same color helper on desktop table cells and mobile cards. Mobile currently shows `PER`, `Fwd PER`, `Fwd P/OP`, and `流動性Z`; add `PBR` to mobile valuation metrics so all target metrics are visible in both layouts.

Do not recolor `Market Cap`, `Med ADV60/FF`, liquidity state chips, row background, or sort headers.

## Implementation Shape

Backend must add these optional `RankingItem` fields:

- `perPercentile`
- `forwardPerPercentile`
- `forwardPOpPercentile`
- `pbrPercentile`

For the first production pass these percentile fields should be Prime-relative. If the row is not Prime, either return `null` percentiles or treat the color tier as neutral until non-Prime evidence is separately accepted.

Frontend should introduce a small local helper near `EquityRankingTable`:

- `type EvidenceColorTier = "excellent" | "good" | "neutral" | "bad" | "very_bad"`
- `getEvidenceTierClass(tier)` maps to Tailwind classes.
- `getCheapValuationPercentileTier(percentile)`
- `getForwardPOpEvidenceTier(forwardPOpPercentile, forwardPerPercentile)`
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

1. Research output includes percentile-based `ranking_color_evidence_df` and relative `forward_per_pop_interaction_df`.
2. Backend Ranking items receive target-date Prime-relative percentile fields.
3. PER/PBR/Fwd PER/Fwd P/OP/流動性Z cells receive the expected classes from percentiles/regimes.
4. Current `Fwd PER` comparison behavior is removed: `forwardPer < per` should not be a coloring input.
5. `Fwd P/OP` high-percentile cases are yellow at `>= 0.80` and red at `>= 0.90`, regardless of low-forward-PER context.
6. Mobile cards show PBR and use the same evidence color helpers.
7. The liquidity state chip uses the same evidence tier as the `流動性Z` numeric cell.

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_recent_return_threshold_forward_response.py apps/bt/tests/unit/server/services/test_ranking_service.py -q
bun run --filter @trading25/contracts bt:sync
bun run --filter @trading25/web test -- apps/ts/packages/web/src/components/Ranking/RankingTable.test.tsx
bun run --filter @trading25/web typecheck
```

If the package script does not support the file argument, use the repo's closest existing web test command and report the exact command.

## Non-Goals

- No absolute PER/PBR/Fwd PER/Fwd P/OP thresholds for coloring.
- No attempt to make color tiers sector-relative or market-specific yet.
- No score-method change for value composite Ranking.

## Caveats

These rules are an evidence-backed interpretation layer, not a trading rule. The first production mapping should use broad Prime percentile buckets to avoid overfitting. Standard/Growth and sector-relative calibration may deserve a later research pass, but absolute valuation thresholds should not be reintroduced for this UI coloring.
