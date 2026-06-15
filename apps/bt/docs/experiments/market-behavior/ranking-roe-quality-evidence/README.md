# Ranking ROE Quality Evidence

## Published Readout

### Decision

Daily Ranking Research Base を使い、`ROE` と `FwdROE` の Prime 日次 percentile（0.2 / 0.8）が Daily Ranking の投資判断を改善するかを検証した。`ROE = adjusted_eps / adjusted_bps * 100`、`FwdROE = adjusted_forecast_eps / adjusted_bps * 100` とし、`statement_metrics_adjusted` の latest as-of row を Ranking date に PIT join した。

結論:

- ROE / FwdROE の high percentile は、Daily Ranking の long 判断を改善しない。2024-01-01 以降の Prime では High ROE / High FwdROE が 20D/60D とも全体より悪く、high quality を `Deep Value` の代替や拡張 hard filter にしない。
- Long 側の主条件は引き続き `Deep Value + Long Hybrid Leadership + ATR20 Accel`。この scaffold では `High ROE` / `High FwdROE` へ置き換えるより、`Deep Value` や低ROE側の安値・回復候補の方が良い。
- Low ROE / Low FwdROE は単体では意外に悪くないが、これは「低採算でも安い・リカバリー余地がある」銘柄が混ざるためで、quality signal として positive に昇格しない。value context なしに low quality を買うルールにも使わない。
- Short 側では `Sector Weak` や `Overvalued + Sector Weak` が ROE/FwdROE より支配的。`Crowded + Low Quality + Sector Weak` は悪いが観測数が薄く、既存の High PSR / overvalued sector weak short-side caution を置き換える根拠にはしない。
- Production implication は `ROE/FwdROE percentile` を Daily Ranking の主色や hard filter には入れず、必要なら diagnostics / tooltip / secondary chip に留める。

### Main Findings

#### 結論: High ROE / High FwdROE は全体より悪く、long quality filterにならない

2024年以降の Prime では、High ROE / High FwdROE は 20D/60D とも middle / low より悪い。特に60Dは High FwdROE 20% が median `-4.061%`、High ROE 20% が `-3.812%` で、Low FwdROE 20% の `-1.591%`、Low ROE 20% の `-1.968%` より悪い。

| Bucket | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Low ROE 20% | 20D | 178,638 | -0.013% | -0.865% | 44.59% | 8.90% |
| Middle ROE 60% | 20D | 534,995 | -0.334% | -0.883% | 44.17% | 7.95% |
| High ROE 20% | 20D | 178,764 | -0.759% | -1.432% | 42.57% | 13.89% |
| High ROE 10% | 20D | 89,509 | -0.975% | -1.805% | 41.28% | 16.21% |
| Low ROE 20% | 60D | 165,778 | +0.176% | -1.968% | 43.60% | 24.73% |
| High ROE 20% | 60D | 166,122 | -1.939% | -3.812% | 40.13% | 33.16% |
| High ROE 10% | 60D | 83,152 | -2.705% | -4.821% | 38.47% | 36.62% |
| Low FwdROE 20% | 20D | 167,865 | +0.127% | -0.776% | 44.99% | 7.62% |
| High FwdROE 20% | 20D | 168,013 | -0.965% | -1.534% | 41.88% | 14.18% |
| Low FwdROE 20% | 60D | 155,832 | +0.816% | -1.591% | 44.85% | 22.48% |
| High FwdROE 20% | 60D | 156,187 | -2.479% | -4.061% | 39.06% | 33.50% |

#### 結論: Deep Value に High ROE/FwdROE を足しても改善しない

Deep Value 全体は60D median `+0.359%`。High FwdROE を重ねると `+0.353%` とほぼ横ばい、High ROE を重ねると `-0.652%` に悪化する。一方、`ROE or FwdROE low` を重ねた Deep Value は20D/60Dとも改善しており、これは quality ではなく value/recovery の混在として読む。

| Decision scope | Quality condition | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| All market | All | 20D | 935,278 | -0.296% | -0.974% | 43.89% | 9.26% |
| All market | High ROE | 20D | 178,764 | -0.759% | -1.432% | 42.57% | 13.89% |
| All market | High FwdROE | 20D | 168,013 | -0.965% | -1.534% | 41.88% | 14.18% |
| Deep Value | All | 20D | 72,041 | +0.551% | -0.160% | 48.92% | 4.43% |
| Deep Value | High ROE | 20D | 5,752 | +1.160% | -0.567% | 46.78% | 7.42% |
| Deep Value | High FwdROE | 20D | 2,666 | +1.330% | -0.137% | 49.32% | 7.01% |
| Deep Value | Low ROE or Low FwdROE | 20D | 24,297 | +1.248% | +0.430% | 52.85% | 3.83% |
| Deep Value | All | 60D | 67,180 | +2.318% | +0.359% | 51.30% | 15.67% |
| Deep Value | High ROE | 60D | 5,399 | +3.136% | -0.652% | 48.19% | 19.41% |
| Deep Value | High FwdROE | 60D | 2,520 | +2.722% | +0.353% | 51.59% | 16.98% |
| Deep Value | Low ROE or Low FwdROE | 60D | 22,912 | +4.322% | +2.157% | 57.09% | 13.65% |

#### 結論: Long Hybrid + ATR scaffold でも High ROE/FwdROE は置換候補にならない

`Long Hybrid Leadership >= 0.8 + ATR20 Accel ex-overheat` に固定すると、`Deep Value` が 20D median `+2.244%`、60D median `+4.020%`。High ROE は20D median `-0.582%`、High FwdROE は `-1.100%` で、良いscaffold上でも quality high は上乗せにならない。

| Long scope | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Long Hybrid + ATR20 Accel | 20D | 8,401 | +1.540% | +0.608% | 53.49% | 5.50% |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | 1,534 | +2.968% | +2.244% | 67.60% | 1.69% |
| High ROE + Long Hybrid + ATR20 Accel | 20D | 733 | +1.513% | -0.582% | 46.93% | 8.46% |
| High FwdROE + Long Hybrid + ATR20 Accel | 20D | 719 | +0.394% | -1.100% | 43.81% | 12.38% |
| Low ROE + Long Hybrid + ATR20 Accel | 20D | 2,645 | +1.880% | +1.307% | 58.26% | 5.10% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | 1,531 | +8.481% | +4.020% | 64.99% | 8.23% |
| High ROE + Long Hybrid + ATR20 Accel | 60D | 701 | +4.143% | +0.451% | 51.36% | 22.25% |
| High FwdROE + Long Hybrid + ATR20 Accel | 60D | 692 | +1.811% | -0.217% | 48.84% | 27.89% |
| Low ROE + Long Hybrid + ATR20 Accel | 60D | 2,582 | +5.497% | +2.927% | 59.18% | 14.68% |

#### 結論: Short側は ROE/FwdROE より Sector Weak / Overvalued が支配的

`Sector Weak` は20D median `-1.644%`、60D median `-4.689%`。`Overvalued + Sector Weak` はさらに悪く、60D median `-7.172%`。Low quality を足しても `Overvalued + Sector Weak` 自体を上回る悪化確認にはならない。`Crowded + Low Quality + Sector Weak` は20D/60Dとも悪いが、観測数は 668 / 658 と薄い。

| Short scope | Horizon | Obs | Mean excess | Median excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Sector Weak | 20D | 97,043 | -1.254% | -1.644% | 40.02% | 11.72% |
| Overvalued + Sector Weak | 20D | 40,688 | -2.308% | -2.384% | 36.97% | 16.82% |
| Low ROE + Sector Weak | 20D | 18,099 | -1.254% | -1.720% | 39.06% | 11.21% |
| Low FwdROE + Sector Weak | 20D | 16,089 | -0.952% | -1.475% | 40.01% | 8.55% |
| Overvalued + Low Quality + Sector Weak | 20D | 12,613 | -1.498% | -1.838% | 38.32% | 11.84% |
| Crowded + Low Quality + Sector Weak | 20D | 668 | -3.709% | -3.941% | 30.69% | 23.80% |
| Sector Weak | 60D | 93,667 | -2.938% | -4.689% | 35.57% | 32.19% |
| Overvalued + Sector Weak | 60D | 39,787 | -5.954% | -7.172% | 29.79% | 41.19% |
| Low Quality + Sector Weak | 60D | 22,052 | -2.040% | -3.233% | 39.48% | 27.83% |
| Overvalued + Low Quality + Sector Weak | 60D | 12,212 | -4.160% | -4.921% | 33.71% | 33.20% |
| Crowded + Low Quality + Sector Weak | 60D | 658 | -5.301% | -7.950% | 30.24% | 45.44% |

### Interpretation

ROE は一般には quality 指標だが、この Daily Ranking surface では「高ROE = 良い forward excess」とはならなかった。高ROE・高FwdROE銘柄は PBR / valuation percentile も高くなりやすく、2024年以降の Prime では quality premium より valuation crowding の悪化が勝っている可能性が高い。

逆に Low ROE / Low FwdROE が Deep Value や Long Hybrid scaffold の中で改善して見えるのは、低品質を買うべきという意味ではなく、低ROEの中に安値・回復・再評価候補が混ざっているためと読む。これを production rule にするなら、ROE単体ではなく、Deep Value、forecast revision、sector strength、ATR continuation を同時に見る必要がある。

Short 側では low quality が単独の強い悪化確認にならず、既存の `Sector Weak`、`Overvalued + Sector Weak`、PSR系 short caution の方が解釈しやすい。`Crowded + Low Quality + Sector Weak` は悪いが薄いため、priority escalation に使う前により長い履歴か別 window で確認する。

### Production Implication

- Daily Ranking の主判断に `High ROE` / `High FwdROE` を long positive color として追加しない。
- `Deep Value` の置換や拡張として `High ROE` / `High FwdROE` を使わない。既存の `Deep Value + Long Hybrid Leadership + ATR20 Accel` を維持する。
- ROE/FwdROE を UI に出す場合は、quality confirmation ではなく diagnostic として扱う。特に High ROE/FwdROE は「quality good」ではなく、valuation crowding と併読する。
- Low ROE/FwdROE は単独採用しない。使うなら Deep Value / recovery / revision 系の次研究で候補拡張として検証する。
- Short-side では ROE/FwdROE を既存 `Overvalued + Sector Weak` や High PSR caution の置換にしない。`Crowded + Low Quality + Sector Weak` は薄い watch candidate に留める。

### Caveats

- Published run は `analysis_start_date=2024-01-01` の recency-bounded Prime study。unbounded full-history run は shared short-red feature panel build が対話時間内に完了しなかったため、この readout では採用していない。
- outcome は 20D/60D close-to-close TOPIX excess return。5D/10Dは今回の Published Readout では未使用。
- coverage は 969,295 observations / 1,702 codes / 595 dates。ROE coverage は 95.32%、FwdROE coverage は 89.59%。
- ROE/FwdROE は `statement_metrics_adjusted` の adjusted EPS/BPS 由来であり、自己資本の期間平均ではなく latest disclosed BPS に対する per-share proxy。
- Portfolio sizing、turnover、cost、sector cap は未検証。Ranking surface の観察であり、単体 strategy evidence ではない。

### Source Artifacts

| Artifact | Path |
| --- | --- |
| runner | `apps/bt/scripts/research/run_ranking_roe_quality_evidence.py` |
| domain module | `apps/bt/src/domains/analytics/ranking_roe_quality_evidence.py` |
| reusable base | `apps/bt/src/domains/analytics/daily_ranking_research_base.py` |
| bundle | `/private/tmp/trading25-research/market-behavior/ranking-roe-quality-evidence/20260613_ranking_roe_quality_prime_2024_20_60_v1` |
| result tables | `coverage_diagnostics_df`, `roe_bucket_evidence_df`, `forward_roe_bucket_evidence_df`, `decision_scope_quality_evidence_df`, `long_deep_dive_quality_evidence_df`, `short_deep_dive_quality_evidence_df` |
| command | `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_roe_quality_evidence.py --start-date 2024-01-01 --horizons 20,60 --markets prime --output-root /private/tmp/trading25-research --run-id 20260613_ranking_roe_quality_prime_2024_20_60_v1 --notes "Daily Ranking Research Base ROE FwdROE Prime universe bounded from 2024-01-01 primary horizons 20D/60D"` |
