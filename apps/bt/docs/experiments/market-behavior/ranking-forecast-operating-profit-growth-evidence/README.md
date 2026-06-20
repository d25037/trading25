# Ranking Forecast Operating Profit Growth Evidence

## Published Readout

### Decision

Daily Ranking Research Base に `forecast_operating_profit_growth_ratio = p_op / forward_p_op` を fast column として追加し、さらに Base 側で UI と同じ valuation signal (`Deep Value`, `Undervalued`, `Overvalued`, `Very Overvalued`, `No Earnings`) を使えるようにした。この readout は、その Base SoT を使って予想営業利益成長率が long / short 判断を改善するかを検証する。

結論:

- Growth 単体は broad long hard filter ではない。
- Long 側は `Deep Value + Long Hybrid Leadership + ATR20 Accel` が最も明確。2026-06-21 rerun では 20D median TOPIX excess は `+1.414%`、win rate は `61.11%`。
- この最強 long 群に限ると `forecast_operating_profit_growth_ratio >= 1.2` は 20D/60D の median と win rate を改善する。ただし 20D obs は `405`、`>= 1.5` は `138` まで細るため、production の broad hard filter ではなく、Inspect Top K の優先順位・badge として扱う。
- `Overvalued + Long Hybrid + ATR20 Accel` は high growth で一部改善するが、median はまだマイナス。高PER/高FwdPERを broad に正当化しない。
- Short 側は「高成長を救うか」ではなく、「低成長 / contraction を short 選択性に使えるか」で読む。`Overvalued + Sector Weak` は低成長でも悪いが、低成長だけで劇的に悪化するわけではない。より強い short confirmation は `Crowded + No Value/Overvalued + Sector Weak` と ATR overheat。
- したがって production 候補は、long は `Deep Value + Long Hybrid + ATR20 Accel`、short は `Overvalued/Very Overvalued + Sector Weak` を broad caution、`Crowded no-value/overvalued + Sector Weak (+ overheat)` を pure-short priority とする。

### Main Findings

#### 結論: Base SoT の `Deep Value` は low PER より自然な long axis

`Deep Value` は Base の `valuation_signal = 'strong_value_confirmation'`、つまり UI filter と同じ定義で読む。20D all では median `-0.011%` まで改善し、high growth では median `+0.137%`。`Undervalued` は `Deep Value` より弱く、growth >= 1.5x でも median `-0.582%`。

| Scope | Growth condition | 20D obs | 20D mean excess | 20D median excess | 20D win rate | Severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Deep Value | all | 312,155 | +0.627% | -0.011% | 49.92% | 4.02% |
| Deep Value | growth >= 1.5x | 23,300 | +0.875% | +0.137% | 50.89% | 5.64% |
| Deep Value | low/missing growth | 179,831 | +0.600% | -0.016% | 49.88% | 3.89% |
| Undervalued | all | 466,276 | +0.091% | -0.404% | 46.91% | 4.82% |
| Undervalued | growth >= 1.5x | 26,043 | +0.048% | -0.582% | 45.71% | 5.86% |

#### 結論: Long は `Deep Value + Long Hybrid + ATR20 Accel` を主条件にする

`Long Hybrid + ATR20 Accel` 全体は high growth で改善するが、Base valuation signal で切ると差がはっきりする。`Deep Value + Long Hybrid + ATR20 Accel` は 20D median `+1.341%`、low/missing growth でも `+1.359%`。これは growth-first ではなく、Deep Value / leadership / ATR-first の候補選択性が強いという結果。

| Long scope | Growth condition | 20D obs | 20D mean excess | 20D median excess | 20D win rate | Severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Long Hybrid + ATR20 Accel | all | 46,560 | +0.222% | -0.620% | 46.37% | 9.91% |
| Long Hybrid + ATR20 Accel | growth >= 1.5x | 2,902 | +1.897% | +0.122% | 50.59% | 11.96% |
| Deep Value + Long Hybrid + ATR20 Accel | all | 3,567 | +2.545% | +1.341% | 60.64% | 2.24% |
| Deep Value + Long Hybrid + ATR20 Accel | low/missing growth | 2,701 | +2.708% | +1.359% | 60.90% | 1.70% |

#### 結論: 2026-06-21 rerun では `Deep Value + Long Hybrid + ATR20 Accel` 内の growth hard filter は効くが、sample constrained

`Fwd P/OP` standalone が弱い代替として、同じ Daily Ranking Research Base の `forecast_operating_profit_growth_ratio = p_op / forward_p_op` を「来季予想営業利益 / 実績営業利益」として hard filter した。Broad な `Long Hybrid + ATR20 Accel` に `growth >= 1.5x` を足すと 20D median は `+0.096%` まで改善するが、60D median は `-0.028%` でまだ主条件には弱い。一方で `Deep Value + Long Hybrid + ATR20 Accel` に重ねると 20D/60D とも明確に改善する。ただし観測数は小さい。

| Long scope | Growth condition | 20D obs | 20D median excess | 20D win rate | 20D severe loss | 60D obs | 60D median excess | 60D win rate | 60D severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Long Hybrid + ATR20 Accel | all | 46,245 | -0.608% | 46.46% | 10.00% | 45,607 | -1.630% | 44.88% | 24.79% |
| Long Hybrid + ATR20 Accel | growth >= 1.2x | 6,982 | -0.457% | 47.75% | 12.65% | 6,881 | -1.331% | 46.37% | 25.29% |
| Long Hybrid + ATR20 Accel | growth >= 1.5x | 2,882 | +0.096% | 50.45% | 12.18% | 2,834 | -0.028% | 49.86% | 25.79% |
| Deep Value + Long Hybrid + ATR20 Accel | all | 3,487 | +1.414% | 61.11% | 2.12% | 3,460 | +1.765% | 55.81% | 16.21% |
| Deep Value + Long Hybrid + ATR20 Accel | growth >= 1.2x | 405 | +3.655% | 67.41% | 4.94% | 403 | +3.210% | 65.51% | 16.87% |
| Deep Value + Long Hybrid + ATR20 Accel | growth >= 1.5x | 138 | +5.823% | 70.29% | 7.25% | 136 | +7.707% | 67.65% | 19.85% |

この結果は、`forecast_operating_profit_growth_ratio` を「Deep Value overlap の中で優先順位を上げる」用途なら支持する。一方で `growth >= 1.5x` は severe loss が上がりやすく、観測数も少ないため、単独の green rule や broad long hard filter にするには過剰。

#### 結論: `growth < 0.8x` は broad long / overvalued long では弱い。Deep Value overlap でも60Dは崩れる

`growth < 1.0x` では `Deep Value + Long Hybrid + ATR20 Accel` が弱くなかったが、より深い `growth < 0.8x` まで切ると読みが変わる。Broad な `Long Hybrid + ATR20 Accel` では 20D/60D とも all より悪く、`Overvalued + Long Hybrid + ATR20 Accel` でも 20D median と win rate が悪化する。最強 long 群の `Deep Value + Long Hybrid + ATR20 Accel` では 20D はまだ positive だが、60D median は `-0.599%` まで落ちるため、`growth < 0.8x` は medium-term hold の警戒条件として扱う。

| Long scope | Growth condition | 20D obs | 20D median excess | 20D win rate | 60D obs | 60D median excess | 60D win rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | all | 3,487 | +1.414% | 61.11% | 3,460 | +1.765% | 55.81% |
| Deep Value + Long Hybrid + ATR20 Accel | growth < 0.8x | 161 | +1.142% | 63.35% | 145 | -0.599% | 44.83% |
| Long Hybrid + ATR20 Accel | all | 46,245 | -0.608% | 46.46% | 45,607 | -1.630% | 44.88% |
| Long Hybrid + ATR20 Accel | growth < 0.8x | 5,045 | -1.149% | 42.79% | 4,999 | -2.251% | 43.11% |
| Overvalued + Long Hybrid + ATR20 Accel | all | 16,533 | -1.256% | 43.31% | 16,248 | -2.131% | 44.10% |
| Overvalued + Long Hybrid + ATR20 Accel | growth < 0.8x | 2,519 | -1.887% | 39.10% | 2,489 | -2.400% | 43.23% |

#### 結論: `Overvalued + Long Hybrid + ATR20 Accel` は broad long 拡大に使わない

`Overvalued + Long Hybrid + ATR20 Accel` は high growth で mean は改善するが、20D median は `-0.199%` のまま。all では median `-1.269%`、severe loss `14.61%`。これは「高い valuation が high growth で広く正当化される」ではなく、限定的な continuation badge に留めるべき結果。

| Long scope | Growth condition | 20D obs | 20D mean excess | 20D median excess | 20D win rate | Severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Overvalued + Long Hybrid + ATR20 Accel | all | 16,556 | -0.337% | -1.269% | 43.19% | 14.61% |
| Overvalued + Long Hybrid + ATR20 Accel | growth >= 1.5x | 1,635 | +1.685% | -0.199% | 48.99% | 13.82% |
| Overvalued + Long Hybrid + ATR20 Accel | contraction | 4,161 | -0.903% | -1.490% | 41.14% | 12.62% |

#### 結論: Short の broad axis は `Overvalued / Very Overvalued + Sector Weak`

Base の `Overvalued` / `Very Overvalued` を `Balanced Sector Strength: Weak` と重ねると、20D/60D とも broad caution として機能する。低成長は追加確認になるが、broad overvalued では高成長より常に悪いわけではない。short では growth 単独ではなく valuation signal + sector weak + liquidity/overheat を優先する。

| Short scope | Growth condition | 20D obs | 20D median excess | 20D severe loss | 60D median excess | 60D severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Overvalued + Sector Weak | all | 139,431 | -0.909% | 12.05% | -3.158% | 29.39% |
| Overvalued + Sector Weak | low/missing growth | 52,705 | -0.870% | 11.15% | -3.041% | 27.36% |
| Overvalued + Sector Weak | contraction | 29,309 | -1.031% | 10.27% | -3.416% | 27.10% |
| Very Overvalued + Sector Weak | all | 83,293 | -1.103% | 13.73% | -3.515% | 31.15% |
| Very Overvalued + Sector Weak | contraction | 16,804 | -1.112% | 11.34% | -3.756% | 28.58% |

#### 結論: Pure-short priority は `Crowded + no-value/overvalued + Sector Weak`

`crowded_no_value_sector_weak` と `crowded_overvalued_sector_weak` は broad `Overvalued + Sector Weak` より悪い。低成長でも 20D/60D は十分悪く、high growth ではさらに悪化するケースがある。したがって short 側の順序は `Base valuation signal -> Sector Weak -> crowded/no-value/overvalued -> growth/overheat`。

| Short scope | Growth condition | 20D obs | 20D median excess | 20D severe loss | 60D median excess | 60D severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Crowded no-value + Sector Weak | all | 8,139 | -2.051% | 21.18% | -5.855% | 40.89% |
| Crowded no-value + Sector Weak | low/missing growth | 3,499 | -1.814% | 18.66% | -4.292% | 38.07% |
| Crowded no-value + Sector Weak | growth >= 1.2x | 2,709 | -3.228% | 25.03% | -7.891% | 44.69% |
| Crowded Overvalued + Sector Weak | all | 6,387 | -2.065% | 23.05% | -6.153% | 42.21% |
| Crowded Overvalued + Sector Weak | low/missing growth | 2,810 | -1.679% | 20.46% | -4.745% | 39.67% |
| Crowded Overvalued + Sector Weak | growth >= 1.2x | 2,201 | -3.369% | 26.62% | -8.136% | 45.19% |

#### 結論: ATR overheat は short-side priority を上げる

`Crowded + Sector Weak + overheat` は 60D severe loss が 43-47% 台で、pure-short priority として最も強い。ここでも「growth で正当化」ではなく、overvalued/crowded/no-value に sector weak と overheat が重なるかを先に見る。

| Short scope | Growth condition | 20D obs | 20D median excess | 20D severe loss | 60D median excess | 60D severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Crowded Overvalued + overheat + Sector Weak | all | 1,345 | -3.077% | 25.87% | -7.803% | 46.60% |
| Crowded Overvalued + overheat + Sector Weak | low/missing growth | 588 | -2.120% | 21.94% | -6.084% | 43.13% |
| Crowded no-value + overheat + Sector Weak | all | 1,636 | -3.225% | 24.94% | -6.801% | 44.49% |
| Crowded no-value + overheat + Sector Weak | low/missing growth | 711 | -2.133% | 20.96% | -4.963% | 41.23% |

### Interpretation

Daily Ranking Research Base を SoT 化する意味は、研究ごとに `low PER` や `Overvalued` を再発明しないこと。今回の v8 では `Deep Value` / `Overvalued` / `Very Overvalued` を Base feature として使い、growth はその上に乗せる補助変数として読んだ。

Long 側では、growth は `Deep Value + Long Hybrid + ATR20 Accel` の確認材料にはなるが、主役ではない。2026-06-21 rerun では同 scope 内の `growth >= 1.2x` / `>= 1.5x` は median と win rate を改善したが、sample constrained で severe loss も上がる。反対に `growth < 0.8x` は broad long / overvalued long で弱く、Deep Value overlap でも60Dが崩れる。したがって「高成長だから買う」ではなく、「Deep Value かつ leadership/ATR があるものを優先し、その中で growth を Inspect Top K の優先順位・補助 badge として使い、`<0.8x` は中期保有の警戒条件として見る」が正しい。

Short 側では、問いは「高成長で救うか」ではなく「低成長を狙うと short 選択性が強まるか」。Broad な `Overvalued + Sector Weak` では contraction がやや悪いが、低成長だけでは決定打ではない。実用上は `crowded_no_value` / `crowded_overvalued` / ATR overheat を重ねる方が short priority として強い。

### Production Implication

- Daily Ranking Research Base の `valuation_signal` を今後の Ranking research の valuation axis SoT とする。
- Long は `Deep Value + Long Hybrid Leadership + ATR20 Accel` を priority overlay 候補にする。
- Growth は broad long hard filter にしない。`Deep Value + Long Hybrid + ATR20 Accel` 内では `growth >= 1.2x` を Inspect priority / badge として使う候補にする。`growth >= 1.5x` はさらに強いが sample constrained なので tie-breaker に留める。`growth < 0.8x` は broad long / overvalued long の除外寄り警戒、Deep Value overlap では60D hold caution とする。
- `Overvalued + Long Hybrid + ATR20 Accel` は broad long 拡大に使わない。
- Short は `Overvalued / Very Overvalued + Sector Weak` を broad caution、`Crowded no-value/overvalued + Sector Weak` を pure-short priority、ATR overheat を short-side escalation とする。
- Short 側の growth 表示は `low_or_missing_growth` / `contraction_lt_1_0` を主比較にし、低成長狙いの確認として扱う。

### Reusable Research Contract

今後の Daily Ranking research では、この readout で追加した列を含む既存 SoT 資産を先に使う。新しい runner 内で `low PER`、`Overvalued`、`No Value`、sector leadership、ATR state を再定義しない。

| Axis | SoT asset to reuse | Reader-facing wording |
| --- | --- | --- |
| Valuation signal | `daily_ranking_research_ranked.valuation_signal` and booleans | `Deep Value`, `Undervalued`, `Overvalued`, `Very Overvalued`, `No Earnings` |
| Forecast OP growth | `forecast_operating_profit_growth_ratio`, `per_to_fop_growth_ratio`, `forward_per_to_fop_growth_ratio` | forecast operating-profit growth / growth-adjusted valuation |
| Long sector leadership | `ranking_long_sector_leadership_horizon_decomposition.py` tables | `Long Hybrid Leadership` |
| Current sector strength | `ranking_sector_strength_evidence.py` / `ranking_sector_daily_state` | `Balanced Sector Strength: Strong/Weak` |
| Short/red states | `ranking_short_red_evidence.py`, `ranking_short_sector_strength_evidence.py` | `Crowded No Value`, `Crowded Overvalued`, `Stale Rally Fade` |
| Technical state | `atr_expansion_forward_response.py` / Ranking technical flags | `ATR20 Accel`, `ATR overheat` |

Reader-facing readouts should avoid wording that can be misread as good value. Use `Overvalued` / `Very Overvalued`. New research code should use canonical internal columns `overvalued_warning` / `very_overvalued_warning`; the Base SoT does not emit positive-sounding compatibility aliases.

### Caveats

- outcome は 5D/10D/20D/60D close-to-close TOPIX excess return。primary read は 20D、durability check は 60D。
- Prime-only の UI evidence layer であり、portfolio rule ではない。
- `forecast_operating_profit_growth_ratio` は `daily_valuation.p_op / daily_valuation.forward_p_op` から導出した fast path。これは同じ market-cap basis 上では `予想営業利益 / 営業利益` と同値になる。
- local `market.duckdb` coverage は Prime 4,779,812 observations / 2,438 codes / 2,458 dates。growth ratio coverage は 72.28%。
- Long Hybrid / ATR / sector overlay を明示したため、runner は初期版より重い。次回以降の高速化対象は Base ではなく sector/ATR/long leadership temp table の再利用。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_forecast_operating_profit_growth_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_forecast_operating_profit_growth_evidence.py` |
| reusable base | `apps/bt/src/domains/analytics/daily_ranking_research_base.py` |
| base fast columns | `forecast_operating_profit_growth_ratio`, `valuation_signal`, `strong_value_confirmation`, `overvalued_warning`, `very_overvalued_warning`, `per_to_fop_growth_ratio`, `forward_per_to_fop_growth_ratio` on `daily_ranking_research_ranked` |
| bundle | `~/.local/share/trading25/research/market-behavior/ranking-forecast-operating-profit-growth-evidence/20260610_ranking_forecast_op_growth_prime_v8` |
| 2026-06-21 rerun bundle | `~/.local/share/trading25/research/market-behavior/ranking-forecast-operating-profit-growth-evidence/20260621_ranking_forecast_op_growth_long_hard_filter` |
| 2026-06-21 min100 bundle | `~/.local/share/trading25/research/market-behavior/ranking-forecast-operating-profit-growth-evidence/20260621_ranking_forecast_op_growth_long_hard_filter_min100` |
| 2026-06-21 lt08 min100 bundle | `~/.local/share/trading25/research/market-behavior/ranking-forecast-operating-profit-growth-evidence/20260621_ranking_forecast_op_growth_lt08_long_hard_filter_min100` |
| result tables | `growth_bucket_evidence_df`, `valuation_growth_ratio_evidence_df`, `decision_scope_growth_evidence_df`, `long_deep_dive_growth_evidence_df`, `short_deep_dive_growth_evidence_df` |
| command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_forecast_operating_profit_growth_evidence.py --horizons 5,10,20,60 --markets prime --run-id 20260610_ranking_forecast_op_growth_prime_v8 --notes "Daily Ranking Research Base canonical overvalued internal columns"` |
| 2026-06-21 command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_forecast_operating_profit_growth_evidence.py --horizons 5,10,20,60 --markets prime --run-id 20260621_ranking_forecast_op_growth_long_hard_filter --notes "Long strong groups hard-filtered by next-year forecast OP / actual OP growth ratio"` |
| 2026-06-21 min100 command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_forecast_operating_profit_growth_evidence.py --horizons 5,10,20,60 --markets prime --min-observations 100 --run-id 20260621_ranking_forecast_op_growth_long_hard_filter_min100 --notes "Long strong groups hard-filtered by next-year forecast OP / actual OP growth ratio; min observations 100 for Deep Value overlap"` |
| 2026-06-21 lt08 min100 command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_forecast_operating_profit_growth_evidence.py --horizons 5,10,20,60 --markets prime --min-observations 100 --run-id 20260621_ranking_forecast_op_growth_lt08_long_hard_filter_min100 --notes "Long strong groups hard-filtered by next-year forecast OP / actual OP growth ratio including growth < 0.8"` |
