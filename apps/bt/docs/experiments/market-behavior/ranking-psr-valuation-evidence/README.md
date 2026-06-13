# Ranking PSR Valuation Evidence

## Published Readout

### Decision

Daily Ranking Research Base を使い、実績 FY 売上ベースの `PSR = market_cap / latest actual FY sales as-of price date` が Daily Ranking の long / short 判断を改善するかを検証した。対象の「all」は Prime 全体を意味するため、runner は `--markets prime` で実行した。

結論:

- PSR は `PER` 系 valuation signal の置換ではなく、追加の valuation diagnostic として扱う。
- Low PSR は単体で Deep Value を置き換えない。20D median TOPIX excess は Low PSR 20% が `-0.453%`、Deep Value が `-0.010%`、`Deep Value + Long Hybrid Leadership + ATR20 Accel` が `+1.341%`。
- Long 側で PSRを使うなら、`Deep Value` の代替ではなく、`Deep Value or Low PSR + Long Hybrid Leadership + ATR20 Accel` の候補拡張として限定的に読む。ただし Deep Value 単独の方が強い。
- High PSR は short-side caution として有効。`High PSR 20%` は 20D/60D とも Low PSR / middle PSR より悪く、特に `Crowded + PSR Overvalued + Sector Weak` は 20D median `-3.432%`、60D median `-8.930%`。
- Fwd PSR は現時点の bt Data Plane では未実装。repo の J-Quants TS contract には `FSales` / `NxFSales` 候補があるが、bt の `statements` / `daily_valuation` に forecast sales SoT がないため、今回の production 追加候補には含めない。

### Main Findings

#### 結論: PSR単体は方向性があるが、long hard filterではない

Low PSR は High PSR より明確に良い。特に60Dでは High PSR 20% の median `-2.502%`、High PSR 10% の median `-2.837%` に対して、Low PSR 20% は `-1.166%`。ただし Low PSR 20% 自体も median はマイナスで、単独の long signal にはしない。

| PSR bucket | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Low PSR 20% | 20D | 820,677 | +0.147% | -0.453% | 46.72% | 6.07% |
| Middle PSR 60% | 20D | 2,462,155 | -0.056% | -0.580% | 46.03% | 7.14% |
| High PSR 20% | 20D | 823,078 | -0.232% | -0.879% | 45.32% | 12.45% |
| High PSR 10% | 20D | 412,257 | -0.306% | -1.008% | 45.02% | 14.53% |
| Low PSR 20% | 60D | 801,349 | +0.454% | -1.166% | 45.44% | 18.57% |
| High PSR 20% | 60D | 808,549 | -0.823% | -2.502% | 42.89% | 28.67% |
| High PSR 10% | 60D | 405,198 | -1.065% | -2.837% | 42.66% | 31.13% |

#### 結論: Long側は Deep Value をPSRで置換しない

`Deep Value + Long Hybrid Leadership + ATR20 Accel` は引き続き最も強い。PSR undervalued へ置換すると観測数は増えるが、20D median は `+0.025%` まで落ちる。`Deep Value or Low PSR` の候補拡張は中間的で、20D median `+0.492%`、60D median `+0.035%`。

| Long scope | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Long Hybrid + ATR20 Accel | 20D | 46,645 | +0.222% | -0.619% | 46.38% | 9.92% |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | 3,569 | +2.544% | +1.341% | 60.63% | 2.24% |
| Low PSR + Long Hybrid + ATR20 Accel | 20D | 5,318 | +0.921% | +0.025% | 50.21% | 6.04% |
| Deep Value or Low PSR + Long Hybrid + ATR20 Accel | 20D | 7,904 | +1.428% | +0.492% | 53.40% | 4.57% |
| High PSR + Long Hybrid + ATR20 Accel | 20D | 11,555 | -0.322% | -1.064% | 44.37% | 14.64% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | 3,553 | +4.724% | +1.533% | 55.11% | 16.24% |
| Low PSR + Long Hybrid + ATR20 Accel | 60D | 5,290 | +1.771% | -0.459% | 48.17% | 20.59% |
| High PSR + Long Hybrid + ATR20 Accel | 60D | 11,508 | +0.272% | -1.895% | 44.93% | 28.35% |

#### 結論: 最良long scaffoldでは Low PSR は補助、Deep Valueの本体は low PBR + low Fwd PER

ここでは long側の比較条件を `Neutral Rerating + Long Hybrid Leadership >= 0.8 + ATR20 Accel ex-overheat + Prime` に固定した。Low PSR は base より改善するが、Deep Value / low PBR / low PER改善系には届かない。High PSR は同じ良いscaffold上でも20D/60Dのmedianがマイナスで、long候補からの除外・警戒としての価値が高い。

| Long fixed condition add-on | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Base only | 20D | 16,215 | +0.681% | +0.156% | 50.87% | 7.82% |
| Deep Value | 20D | 1,858 | +3.339% | +2.443% | 69.91% | 1.18% |
| Undervalued only | 20D | 1,542 | +1.353% | +1.053% | 56.87% | 3.11% |
| Low PSR 20% | 20D | 1,977 | +1.731% | +0.806% | 54.98% | 5.82% |
| High PSR 20% | 20D | 4,316 | +0.016% | -0.470% | 47.57% | 11.08% |
| Base only | 60D | 16,133 | +0.673% | -0.778% | 47.57% | 22.90% |
| Deep Value | 60D | 1,851 | +6.735% | +3.155% | 63.32% | 8.16% |
| Undervalued only | 60D | 1,530 | +3.287% | +1.123% | 54.18% | 16.21% |
| Low PSR 20% | 60D | 1,968 | +3.242% | +1.332% | 55.13% | 17.12% |
| High PSR 20% | 60D | 4,306 | -0.372% | -1.330% | 46.08% | 25.45% |

Deep Value の定義は `(PBR percentile <= 0.2 AND forward PER percentile <= 0.2) OR (PER percentile <= 0.2 AND forward PER / PER <= 0.8)`。OR脚を分解すると、単純な成績は後者が強いがサンプルが薄い。実運用の主力は前者で、両方を満たす overlap は最も強いがさらに薄い。

| Deep Value leg | Horizon | Obs | Codes | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A: low PBR + low Fwd PER | 20D | 1,767 | 98 | +3.269% | +2.400% | 69.84% | 1.25% |
| B: low PER + Fwd PER/PER <= 0.8 | 20D | 169 | 25 | +4.152% | +3.218% | 72.78% | 0.00% |
| A only | 20D | 1,689 | 97 | +3.258% | +2.309% | 69.63% | 1.30% |
| B only | 20D | 91 | 13 | +4.712% | +3.328% | 71.43% | 0.00% |
| A and B overlap | 20D | 78 | 12 | +3.498% | +3.194% | 74.36% | 0.00% |
| Deep Value total | 20D | 1,858 | 110 | +3.339% | +2.443% | 69.91% | 1.18% |
| A: low PBR + low Fwd PER | 60D | 1,760 | 97 | +7.019% | +3.280% | 63.13% | 8.01% |
| B: low PER + Fwd PER/PER <= 0.8 | 60D | 169 | 25 | +9.646% | +4.537% | 74.56% | 7.10% |
| A only | 60D | 1,682 | 96 | +6.442% | +3.044% | 62.19% | 8.26% |
| B only | 60D | 91 | 13 | +1.240% | +2.892% | 67.03% | 10.99% |
| A and B overlap | 60D | 78 | 12 | +19.453% | +12.038% | 83.33% | 2.56% |
| Deep Value total | 60D | 1,851 | 109 | +6.735% | +3.155% | 63.32% | 8.16% |

#### 結論: Short側では High PSR が悪化確認として使える

`Overvalued + Sector Weak` に対して `PSR Overvalued + Sector Weak` は20D/60Dのmean・median・severe lossがやや悪い。さらに crowded と重ねると差が大きく、`Crowded + PSR Overvalued + Sector Weak` は 60D median `-8.930%`、severe loss `47.77%`。

| Short scope | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Overvalued + Sector Weak | 20D | 140,506 | -0.562% | -0.897% | 44.89% | 12.00% |
| PSR Overvalued + Sector Weak | 20D | 82,125 | -0.756% | -0.990% | 44.81% | 13.56% |
| Crowded Overvalued + Sector Weak | 20D | 6,387 | -0.920% | -2.065% | 41.85% | 23.05% |
| Crowded + PSR Overvalued + Sector Weak | 20D | 3,299 | -2.568% | -3.432% | 38.28% | 28.49% |
| Distribution Stress + PSR Overvalued + Sector Weak | 20D | 15,820 | -0.573% | -1.307% | 44.61% | 19.54% |
| Overvalued + Sector Weak | 60D | 137,675 | -1.801% | -3.122% | 40.49% | 29.30% |
| PSR Overvalued + Sector Weak | 60D | 81,009 | -2.145% | -3.242% | 40.29% | 30.77% |
| Crowded Overvalued + Sector Weak | 60D | 6,301 | -2.499% | -6.159% | 37.93% | 42.22% |
| Crowded + PSR Overvalued + Sector Weak | 60D | 3,276 | -5.284% | -8.930% | 33.52% | 47.77% |
| Distribution Stress + PSR Overvalued + Sector Weak | 60D | 15,608 | -0.769% | -3.783% | 41.74% | 36.92% |

#### 結論: Crowded Rerating + Sector Weakでは High PSR が最も強い単独short軸

`Crowded Rerating + Sector Weak + Prime` に固定し、各valuation軸を単独で `>= 0.8` にした。High PSR は既存の broad `Overvalued` より観測数は少ないが、20D/60Dともmedian悪化とsevere lossが最も強い。`Overvalued` は PER / Fwd PER / Fwd P/OP / PBR のORなので広く拾える一方、High PSR単独の悪化を薄める。

| Short fixed condition add-on | Horizon | Obs | Median excess | Severe loss |
| --- | ---: | ---: | ---: | ---: |
| High PSR 20% | 20D | 3,299 | -3.432% | 28.49% |
| High PER 20% | 20D | 2,909 | -2.628% | 26.37% |
| High Fwd PER 20% | 20D | 2,609 | -2.391% | 25.30% |
| High Fwd P/OP 20% | 20D | 2,335 | -2.689% | 26.90% |
| High PBR 20% | 20D | 4,497 | -2.172% | 24.86% |
| Any Overvalued warning | 20D | 5,670 | -2.244% | 24.00% |
| High PSR 20% | 60D | 3,276 | -8.930% | 47.77% |
| High PER 20% | 60D | 2,855 | -8.451% | 46.51% |
| High Fwd PER 20% | 60D | 2,552 | -7.130% | 43.50% |
| High Fwd P/OP 20% | 60D | 2,290 | -7.122% | 44.32% |
| High PBR 20% | 60D | 4,439 | -7.285% | 44.72% |
| Any Overvalued warning | 60D | 5,592 | -7.120% | 44.24% |

### Interpretation

PSR は売上赤字がないため、PER / Fwd PER が欠損する銘柄も評価できる利点がある。ただし「売上に対して安い」は収益性・資本効率を無視するため、Low PSR を Deep Value の代替にすると long の質が落ちる。今回の結果では、long は既存の `Deep Value + Long Hybrid Leadership + ATR20 Accel` を維持し、PSR は候補拡張または補助診断に留めるのが妥当。

Deep Value をさらに分解すると、`low PBR + low Fwd PER` がサンプル数の大半を占める安定した本体で、`low PER + Fwd PER/PER <= 0.8` は強いが薄い上乗せである。両方を満たす overlap は最も強いが、単独production ruleにするには銘柄数が少ない。Low PSR はこの構造を置き換えず、High PSR の除外・警戒の方がlong品質改善には使いやすい。

Short 側は逆に読みやすい。High PSR は高いPER/PBR/Fwd P/OPと同じく overvalued caution に近く、`Sector Weak` と `Crowded Rerating` に重ねると左尾が厚くなる。特に `Crowded + PSR Overvalued + Sector Weak` は既存の `Crowded Overvalued + Sector Weak` より悪く、単独valuation軸の比較でも High PSR が最も強い悪化確認になった。

### Production Implication

- 現時点で `daily_valuation` にPSR SoTを追加する前に、まず research-only diagnostic として扱う。
- Long production rule は変更しない。`Deep Value + Long Hybrid Leadership + ATR20 Accel` を維持し、Low PSR は broad replacement にしない。
- Deep Value の主力分解は `low PBR + low Fwd PER`。`low PER + Fwd PER/PER <= 0.8` は強いが薄いため、単独hard filterではなく上乗せconfirmationとして扱う。
- Long候補では High PSR を caution / exclusion として検討する。Low PSR は補助診断であり、Deep Value の置換にはしない。
- Short-side では `PSR Overvalued + Sector Weak`、特に `Crowded + PSR Overvalued + Sector Weak` を pure-short priority / caution overlay 候補として次に検討する。
- Short候補では `Crowded Rerating + Sector Weak` 固定時に High PSR が最も強い単独valuation軸なので、既存 `Overvalued` broad OR とは別の escalation chip として扱う余地がある。
- PSRをproduction化するなら、`daily_valuation` または新しい valuation materialization に `actual_psr` と `actual_sales_disclosed_date` を追加し、UI表示は `PSR Overvalued` の警戒chipから始めるのが自然。
- Fwd PSR は bt Data Plane に forecast sales SoT を追加してから再検証する。J-Quants rawには `FSales` / `NxFSales` 系候補があるが、現在のbt ingest/model/schemaは未対応。

### Caveats

- outcome は 20D/60D close-to-close TOPIX excess return。5D/10Dは今回のPublished Readoutでは未使用。
- 対象は Prime 全体。ユーザー指定の「all」は Prime全体を意味するものとして扱った。
- PSR は latest actual FY `statements.sales` を price date as-of でjoinし、`daily_valuation.market_cap` から研究内で一時算出した。現時点では `market.duckdb` の永続SoT列ではない。
- coverage は 4,784,506 observations / 2,438 codes / 2,461 dates、PSR coverage は 86.67%。
- Portfolio sizing、sector cap、turnover、borrow/cost は未検証。short-sideの結論はranking caution / priorityであり、単体short strategyの実装判断ではない。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_psr_valuation_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_psr_valuation_evidence.py` |
| reusable base | `apps/bt/src/domains/analytics/daily_ranking_research_base.py` |
| bundle | `/private/tmp/trading25-research/market-behavior/ranking-psr-valuation-evidence/20260613_ranking_psr_valuation_prime_20_60_v1` |
| result tables | `coverage_diagnostics_df`, `psr_bucket_evidence_df`, `decision_scope_psr_evidence_df`, `long_deep_dive_psr_evidence_df`, `short_deep_dive_psr_evidence_df` |
| command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_psr_valuation_evidence.py --horizons 20,60 --markets prime --output-root /private/tmp/trading25-research --run-id 20260613_ranking_psr_valuation_prime_20_60_v1 --notes "Daily Ranking Research Base PSR actual FY sales Prime universe primary horizons 20D/60D"` |
