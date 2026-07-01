# Ranking Liquidity Z Long Evidence

Daily Ranking Research Base を使い、`liquidity_residual_z` を long-side diagnostic として再評価する。既存の `neutral_rerating` は `-1 < z < 1`、`crowded_rerating` は `z >= 1` で二分されているが、強い long scaffold の内側では `z=1..2` の crowded rerating を捨てすぎている可能性がある。そこで `z` bucket と `z` upper-cap sweep を分け、どこまで許容できるかを検証する。

## Published Readout

### Decision

Run: `20260701_liquidity_z_long_prime_full_history_z1bins`

対象は Prime 全期間、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。対象 price action は rerating 系に合わせて `recent_return_20d_pct >= 0` かつ `recent_return_60d_pct >= 0`。`liquidity_residual_z` は `z < -3`, `-3..-2`, `-2..-1`, `-1..0`, `0..1`, `1..2`, `2..3`, `>=3` の1刻みに分け、既存 long scaffold には `-1 < z < cap` の cap sweep (`cap=1,1.5,2,2.5,3`) も重ねた。

結論:

- `z < 1` の二分割だけでは強い crowded rerating を捨てすぎる。`Deep Value + Long Hybrid Leadership + ATR20 Accel` では、cap を `1` から `2` へ広げると 60D median TOPIX excess が `+3.239%` から `+3.652%` へ改善し、観測数も `1,818` から `2,006` へ増える。
- 最も実用的な上限は `z < 2` 近辺。`z < 2.5` / `z < 3` まで広げても強い scaffold の median は大きく改善せず、left-tail と severe loss は少し悪化しやすい。
- `z=1..2` は long候補として残す価値がある。thin-bucket companion では `Deep Value + Long Hybrid + ATR20 Accel` の `z_1_to_2` が 20D median `+5.840%`、60D median `+6.413%`。
- 低 z 側は `z=-1..0` と `z=0..1` が中心。`z=-2..-1` は scaffold によって弱く、`z<-2` は broad でも強い long scaffold でも主役にならない。
- `z=2..3` は scaffold により割れる。`Deep Value + Long Hybrid + ATR20 Accel` では良いが n=40 / 7 codes と薄く、`Deep Value + Sector Strong + ATR20 Accel` では60D median が `-1.679%` まで落ちる。
- `z>=3` は強い scaffold 内では実質 n=2 で、production threshold の根拠にしない。broad rerating 全体では `z>=3` の60D severe loss が `42.08%` まで悪化するため、基本は tail / over-crowded caution。

### Main Findings

#### 結論: broad rerating では z が高すぎるほど左尾が悪くなる

全期間 Prime の rerating price action 全体では、`z_1_to_2` から severe loss が明確に増え、`z>=3` は60D median も悪化する。したがって z は standalone alpha ではなく、強い scaffold の内側だけで許容幅を広げる diagnostic と読む。

| z bucket | horizon | obs | code count | median excess | p10 excess | win rate | severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `z_lt_minus3` | 20D | 1,334 | 16 | -0.955% | -6.450% | 41.75% | 2.32% |
| `z_minus3_to_minus2` | 20D | 20,350 | 221 | -0.684% | -7.430% | 44.58% | 4.80% |
| `z_minus2_to_minus1` | 20D | 219,681 | 1,050 | -0.760% | -7.612% | 43.97% | 4.94% |
| `z_minus1_to_0` | 20D | 663,077 | 1,795 | -0.526% | -8.088% | 46.28% | 5.93% |
| `z_0_to_1` | 20D | 478,144 | 1,768 | -0.462% | -9.620% | 47.32% | 9.22% |
| `z_1_to_2` | 20D | 144,527 | 1,036 | -0.983% | -13.289% | 45.83% | 16.95% |
| `z_2_to_3` | 20D | 28,332 | 464 | -1.731% | -16.219% | 44.45% | 23.17% |
| `z_ge_3` | 20D | 10,659 | 176 | -1.866% | -19.019% | 44.94% | 27.95% |
| `z_lt_minus3` | 60D | 1,274 | 16 | -3.205% | -12.582% | 35.87% | 17.43% |
| `z_minus3_to_minus2` | 60D | 19,915 | 215 | -1.708% | -13.232% | 42.27% | 17.45% |
| `z_minus2_to_minus1` | 60D | 215,866 | 1,032 | -2.125% | -14.069% | 40.85% | 19.20% |
| `z_minus1_to_0` | 60D | 652,724 | 1,786 | -1.577% | -14.569% | 44.03% | 20.26% |
| `z_0_to_1` | 60D | 471,727 | 1,759 | -1.363% | -17.059% | 45.86% | 24.14% |
| `z_1_to_2` | 60D | 142,149 | 1,029 | -2.515% | -22.321% | 44.54% | 32.50% |
| `z_2_to_3` | 60D | 27,571 | 459 | -4.058% | -26.305% | 42.68% | 37.30% |
| `z_ge_3` | 60D | 10,340 | 173 | -4.753% | -32.120% | 43.56% | 42.08% |

#### 結論: Primary long scaffold では `z < 2` まで広げる価値がある

`Deep Value + Long Hybrid Leadership + ATR20 Accel` では、現行 neutral 相当の `-1 < z < 1` より `-1 < z < 2` の方が20D/60Dとも median が改善する。`z < 2.5` 以上は追加効果が小さく、severe loss はわずかに悪化する。

| z cap | horizon | obs | code count | median excess | p10 excess | win rate | severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `z_cap_minus1_to_1` | 20D | 1,838 | 111 | +2.521% | -4.078% | 70.40% | 1.25% |
| `z_cap_minus1_to_1_5` | 20D | 1,961 | 114 | +2.531% | -4.252% | 69.91% | 1.68% |
| `z_cap_minus1_to_2` | 20D | 2,026 | 118 | +2.632% | -4.281% | 70.48% | 1.68% |
| `z_cap_minus1_to_2_5` | 20D | 2,055 | 118 | +2.733% | -4.213% | 70.85% | 1.70% |
| `z_cap_minus1_to_3` | 20D | 2,066 | 120 | +2.696% | -4.327% | 70.62% | 1.69% |
| `z_cap_minus1_to_1` | 60D | 1,818 | 108 | +3.239% | -9.164% | 63.81% | 8.09% |
| `z_cap_minus1_to_1_5` | 60D | 1,941 | 111 | +3.196% | -9.545% | 63.27% | 8.71% |
| `z_cap_minus1_to_2` | 60D | 2,006 | 115 | +3.652% | -9.551% | 64.01% | 8.67% |
| `z_cap_minus1_to_2_5` | 60D | 2,035 | 115 | +3.690% | -9.578% | 64.13% | 8.75% |
| `z_cap_minus1_to_3` | 60D | 2,046 | 117 | +3.677% | -9.588% | 63.88% | 8.80% |

#### 結論: sector strong を重ねても `z < 2` が妥当な上限

`Deep Value + Sector Strong + ATR20 Accel` でも cap を `2` へ広げると改善するが、`2.5` / `3` への追加効果は小さい。sector strong は `z=2..3` の tail を完全には吸収しない。

| z cap | horizon | obs | code count | median excess | p10 excess | win rate | severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `z_cap_minus1_to_1` | 20D | 2,759 | 233 | +1.803% | -7.249% | 62.81% | 4.02% |
| `z_cap_minus1_to_2` | 20D | 2,889 | 241 | +1.879% | -7.223% | 63.34% | 4.15% |
| `z_cap_minus1_to_3` | 20D | 2,923 | 243 | +1.893% | -7.230% | 63.43% | 4.14% |
| `z_cap_minus1_to_1` | 60D | 2,708 | 223 | +2.588% | -11.550% | 58.20% | 13.07% |
| `z_cap_minus1_to_2` | 60D | 2,838 | 231 | +2.764% | -11.702% | 58.49% | 13.46% |
| `z_cap_minus1_to_3` | 60D | 2,872 | 233 | +2.758% | -11.706% | 58.36% | 13.54% |

#### 結論: `z=1..2` は良い crowded rerating、`z=2..3` は thin / scaffold-dependent

Thin-bucket companion (`min_observations=30`) では、強い scaffold の `z_1_to_2` は20D/60Dとも良い。一方、`z_2_to_3` は観測が薄く、sector strong 系では60Dが割れる。`z>=3` は all-bucket diagnostic でも n=2 しかなく、閾値判断には使わない。低 z 側では `z_minus2_to_minus1` が `-1..0` / `0..1` に劣り、強い scaffold の中でも stale side を積極的に拾う根拠は弱い。

| scaffold | z bucket | horizon | obs | code count | median excess | p10 excess | win rate | severe loss |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | `z_minus2_to_minus1` | 20D | 166 | 25 | +1.161% | -5.728% | 60.84% | 0.60% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_minus1_to_0` | 20D | 1,253 | 73 | +2.174% | -3.706% | 70.55% | 1.20% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_0_to_1` | 20D | 585 | 73 | +3.197% | -4.797% | 70.09% | 1.37% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_1_to_2` | 20D | 188 | 21 | +5.840% | -6.870% | 71.28% | 5.85% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_2_to_3` | 20D | 40 | 7 | +6.843% | -5.728% | 77.50% | 2.50% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_minus2_to_minus1` | 60D | 154 | 25 | -0.090% | -11.867% | 48.70% | 12.34% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_minus1_to_0` | 60D | 1,249 | 72 | +2.788% | -9.190% | 60.93% | 7.85% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_0_to_1` | 60D | 569 | 70 | +5.373% | -9.133% | 70.12% | 8.61% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_1_to_2` | 60D | 188 | 21 | +6.413% | -11.989% | 65.96% | 14.36% |
| Deep Value + Long Hybrid + ATR20 Accel | `z_2_to_3` | 60D | 40 | 7 | +34.243% | -10.957% | 57.50% | 15.00% |
| Deep Value + Sector Strong + ATR20 Accel | `z_1_to_2` | 20D | 130 | 27 | +5.117% | -6.345% | 74.62% | 6.92% |
| Deep Value + Sector Strong + ATR20 Accel | `z_2_to_3` | 20D | 34 | 10 | +4.260% | -7.681% | 70.59% | 2.94% |
| Deep Value + Sector Strong + ATR20 Accel | `z_1_to_2` | 60D | 130 | 27 | +10.466% | -15.886% | 64.62% | 21.54% |
| Deep Value + Sector Strong + ATR20 Accel | `z_2_to_3` | 60D | 34 | 10 | -1.679% | -12.325% | 47.06% | 20.59% |

### Interpretation

`liquidity_residual_z` は、broad universe では上がるほど良いという単純な signal ではない。全体の rerating price action では `z>=1` から left-tail が厚くなり、`z>=3` は明確に over-crowded tail として扱うべき。低 z 側も `z<-2` が反転優位になるわけではなく、`-1..0` と `0..1` が厚い中核になる。

ただし、既存 long scaffold が十分強い場合は話が変わる。`Deep Value + Long Hybrid Leadership + ATR20 Accel` のように valuation / leadership / ATR が揃っているなら、`z=1..2` はむしろ強い参加・再評価の状態として残る。現行の `neutral_rerating` hard split (`z < 1`) はこの部分を捨てており、long shortlist では機会損失になる。

一方、`z=2..3` は「さらに強い」とは言い切れない。Deep Value + Long Hybrid だけでは良く見えるが、sector strong を含めた別 scaffold では60Dが割れ、code count も薄い。`z>=3` は強い scaffold 内でも観測がほぼなく、broad では左尾が重い。したがって、最適な liquidity z の読みは `z<1` ではなく、primary long scaffold では `-1 < z < 2`、`-2..-1` は priority-down、`2..3` は exception review、`>=3` は over-crowded caution。

### Production Implication

- Daily Ranking の long-side filter では、`neutral_rerating` の `z < 1` を hard upper bound として使わない。強い long scaffold では `z < 2` まで許容する。
- `z=1..2` は `strong crowded rerating` として positive / priority-up diagnostic にできる。ただし standalone ではなく、Deep Value + Long Hybrid + ATR20 Accel などの scaffold 内に限定する。
- `z=-2..-1` は stale side の priority-down。`z<-2` は sample / quality ともに主導線にしない。
- `z=2..3` は automatic include ではなく exception review。sector / valuation / tail を確認し、sample-thin として扱う。
- `z>=3` は long候補の基本導線では over-crowded caution。強い scaffold 内でも観測が薄すぎるため、positive rule にしない。
- UI / Research 表示では、既存 regime label (`neutral_rerating` / `crowded_rerating`) に加えて raw `liquidity_residual_z` bucket または `z<2` cap diagnostic を出す方が実用的。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio construction、turnover、cost、capacity は未反映。
- signal-date market scope は `stock_master_daily_exact_date` を使う。latest membership 固定ではない。
- `z=2..3` と `z>=3` は強い scaffold 内で sample が薄い。thin-bucket / all-bucket companion は threshold 判断ではなく、tail diagnostic 用。
- `liquidity_residual_z` は free-float cap に対する liquidity residual。約定容易性そのものではなく、参加・混雑・capacity の複合 diagnostic として読む。
- N225 excess はこの runner の primary output には入れていない。N225 benchmark readout との整合は scaffold 選定で参照したが、数値は TOPIX excess で読む。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_liquidity_z_long_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_liquidity_z_long_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_liquidity_z_long_evidence.py`
- Main bundle: `~/.local/share/trading25/research/market-behavior/ranking-liquidity-z-long-evidence/20260701_liquidity_z_long_prime_full_history_z1bins/`
- Thin-bucket companion: `~/.local/share/trading25/research/market-behavior/ranking-liquidity-z-long-evidence/20260701_liquidity_z_long_prime_full_history_z1bins_thin/`
- All-bucket diagnostic: `~/.local/share/trading25/research/market-behavior/ranking-liquidity-z-long-evidence/20260701_liquidity_z_long_prime_full_history_all_buckets/`
- Results tables: `z_bucket_evidence_df`, `long_scaffold_z_bucket_evidence_df`, `long_scaffold_z_cap_evidence_df`, `coverage_diagnostics_df`
