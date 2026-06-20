# Ranking SMA5 Count Long Evidence

Daily Ranking Research Base を使い、`sma5_above_count_5d` を long-side diagnostic として検証する。定義は short-side 研究と同じで、「当日を含む直近5営業日のうち、終値がその日の SMA5 を上回った日数」。単独 count ではなく、`0/1`、`2/3`、`4/5` の3群で既存 long scaffold を再評価する。

## Published Readout

### Decision

Run: `20260620_sma5_count_long_evidence_prime_2024_v1`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。前回の short-side SMA5 と同じ3群 (`0/1`, `2/3`, `4/5`) を使い、既存 long 研究で強かった scaffold に重ねた。

結論:

- `sma5_above_count_5d` は long-side でも standalone signal ではない。全体では `4/5` が `0/1` より少し良いが、母集団全体の median TOPIX excess は20D/60Dともマイナスで、SMA5だけでは long 候補を作れない。
- 既存の強い long 条件、特に `Deep Value + Long Hybrid Leadership + ATR20 Accel` と `Neutral Rerating + Deep Value + Long Hybrid Leadership + ATR20 Accel` の内側では、`0/1` は明確に弱く、`2/3` と `4/5` が有効圏に残る。
- `4/5` は continuation confirmation として読めるが、hard filter にはしない。`Neutral + Deep Value + Long Hybrid + ATR20 Accel` では60D median が `2/3: +5.228%`、`4/5: +5.933%` と近く、`4/5` だけを残すと sample を減らしすぎる。
- `5D` では `0/1` がむしろ良いことがある。これは短期 rebound / mean-reversion の混入で、20D/60D の主判断とは分ける。
- Crowded long は引き続き value confirmation が主役。`Crowded + Long Hybrid` 全体は60D median が全SMA5群でマイナスだが、`Crowded + low10 PBR` や `Crowded + low10 PBR + low10 Fwd PER` では `2/3` と `4/5` の両方が強い。SMA5 は tail pruning の主条件ではなく、短期状態の補助表示に留める。

### Main Findings

#### 結論: 全体では `4/5` が少し良いが、long signal にはならない

Prime 全体では `sma5_above_count_4_5` が `0/1` より median / win rate で少し良い。ただし全体の TOPIX excess median は20D/60Dともマイナスで、SMA5 3群だけでは候補生成には弱い。

| horizon | SMA5 group | obs | median excess | win rate | severe loss | median 20D return | median 60D return |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | `0/1` | 258,582 | -1.179% | 42.56% | 9.67% | -2.653% | -1.530% |
| 20D | `2/3` | 364,959 | -1.024% | 43.57% | 9.16% | +0.613% | +2.422% |
| 20D | `4/5` | 319,360 | -0.821% | 44.90% | 9.26% | +4.171% | +6.391% |
| 60D | `0/1` | 238,048 | -2.555% | 41.78% | 25.89% | -2.568% | -1.301% |
| 60D | `2/3` | 336,580 | -2.243% | 42.69% | 25.01% | +0.826% | +2.686% |
| 60D | `4/5` | 300,461 | -2.135% | 43.16% | 25.00% | +4.235% | +6.502% |

#### 結論: 既存の最良 long scaffold では `0/1` を避け、`2/3` と `4/5` を残す

`Deep Value + Long Hybrid Leadership + ATR20 Accel` は既存 long 研究で強かった主条件。SMA5 で割ると `0/1` は20D/60Dとも劣り、`2/3` と `4/5` が残る。`4/5` は60Dで最良だが、20Dの severe loss は `2/3` よりやや高い。

| horizon | SMA5 group | obs | median excess | mean excess | win rate | severe loss | p10 excess |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | `0/1` | 218 | +1.299% | +1.279% | 57.80% | 1.38% | -6.548% |
| 20D | `2/3` | 682 | +2.050% | +2.822% | 66.42% | 1.32% | -5.196% |
| 20D | `4/5` | 645 | +2.677% | +3.678% | 71.94% | 2.17% | -4.307% |
| 60D | `0/1` | 217 | +0.692% | +2.647% | 53.46% | 10.14% | -10.011% |
| 60D | `2/3` | 675 | +4.016% | +8.199% | 64.44% | 9.48% | -9.853% |
| 60D | `4/5` | 639 | +5.697% | +10.761% | 69.48% | 6.26% | -7.705% |

#### 結論: Primary scaffold は `Neutral + Deep Value + Long Hybrid + ATR20 Accel`

PSR / growth / N225 benchmark 系の既存 long readout と整合する primary scaffold は `Neutral Rerating + Deep Value + Long Hybrid Leadership + ATR20 Accel`。ここでも `0/1` は劣り、`2/3` と `4/5` は20D/60Dの両方で強い。`4/5` は少し上だが、差は hard filter 化するほどではない。

| horizon | SMA5 group | obs | median excess | mean excess | win rate | severe loss | p10 excess |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | `0/1` | 121 | +1.562% | +1.473% | 63.64% | 0.83% | -6.268% |
| 20D | `2/3` | 508 | +2.783% | +3.427% | 72.44% | 0.79% | -3.936% |
| 20D | `4/5` | 562 | +2.940% | +3.840% | 74.38% | 1.25% | -3.752% |
| 60D | `0/1` | 121 | +0.928% | +3.262% | 55.37% | 9.09% | -9.417% |
| 60D | `2/3` | 508 | +5.228% | +9.301% | 71.06% | 5.71% | -7.252% |
| 60D | `4/5` | 561 | +5.933% | +10.260% | 71.66% | 5.35% | -7.081% |

#### 結論: `Sector Strong` まで絞ると `2/3` の方が安定する

`Neutral + Deep Value + Sector Strong + ATR20 Accel` では `2/3` が20D/60Dとも `4/5` を上回る。`4/5` は「強いほど良い」という単純な continuation filter ではなく、既存 scaffold の中でも sector / valuation / sample によって役割が変わる。

| horizon | SMA5 group | obs | median excess | mean excess | win rate | severe loss | p10 excess |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | `2/3` | 419 | +2.476% | +2.864% | 68.50% | 0.48% | -4.667% |
| 20D | `4/5` | 503 | +1.679% | +2.376% | 64.61% | 2.39% | -4.675% |
| 60D | `2/3` | 418 | +4.157% | +6.437% | 67.22% | 5.26% | -8.055% |
| 60D | `4/5` | 501 | +3.070% | +4.625% | 63.67% | 8.58% | -9.258% |

#### 結論: Crowded long は value confirmation が主で、SMA5 は補助

`Crowded + Long Hybrid` 全体は平均が大きい一方で median は20D/60Dともマイナス、severe loss も重い。SMA5 3群はこの問題を解決しない。一方、既存 crowded-long readout と同じく `low10 PBR` や `low10 PBR + low10 Fwd PER` を重ねると、`2/3` と `4/5` はどちらも強い。

| scaffold | horizon | SMA5 group | obs | median excess | win rate | severe loss | p10 excess |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Crowded + Long Hybrid | 60D | `0/1` | 527 | -1.180% | 48.20% | 35.67% | -26.194% |
| Crowded + Long Hybrid | 60D | `2/3` | 1,303 | -0.796% | 48.66% | 32.92% | -26.206% |
| Crowded + Long Hybrid | 60D | `4/5` | 1,672 | -1.198% | 47.79% | 30.98% | -24.655% |
| Crowded + low10 PBR | 60D | `2/3` | 185 | +10.781% | 64.86% | 20.54% | -19.100% |
| Crowded + low10 PBR | 60D | `4/5` | 246 | +11.371% | 66.26% | 15.85% | -14.981% |
| Crowded + low10 PBR + low10 Fwd PER | 60D | `2/3` | 110 | +11.434% | 68.18% | 20.00% | -18.081% |
| Crowded + low10 PBR + low10 Fwd PER | 60D | `4/5` | 155 | +11.327% | 67.10% | 14.84% | -13.433% |

### Interpretation

`sma5_above_count_5d` は、long-side では「短期上向き確認」ではあるが、銘柄選別の主役にはならない。全体では `4/5` が相対的に良いものの、母集団の median excess はまだマイナスで、Deep Value / Long Hybrid / ATR / liquidity regime の既存 scaffold が必要。

最も有用な読みは、`0/1` を弱い timing / pullback 状態として扱い、既存 high-conviction long scaffold の中で優先度を下げること。特に `Neutral + Deep Value + Long Hybrid + ATR20 Accel` では `0/1` が60Dで大きく劣り、`2/3` と `4/5` が実用圏に残る。

一方で `4/5` だけを hard filter にするのは過剰。`Sector Strong` まで重ねると `2/3` の方が安定し、5Dでは `0/1` が短期反発的に良い局面もある。したがって、SMA5 count は「既存 long候補の表示・優先順位調整」には使えるが、Primary long condition にはしない。

Crowded long では、SMA5 で上向きだから買うのではなく、既存通り `low10 PBR` と `low10 Fwd PER` の value confirmation を先に見る。`4/5` は value-confirmed crowded long の continuation 補助にはなるが、unconfirmed crowded long の left tail を消すほど強くない。

### Production Implication

- Daily Ranking に出すなら `SMA5 Above Count 5D` は long/short 共通の diagnostic column / tooltip として扱う。
- Long-side triage では `0/1` を high-conviction long scaffold の優先度下げに使う。特に `Neutral + Deep Value + Long Hybrid + ATR20 Accel` では `2/3` と `4/5` を主に見る。
- `4/5` は continuation confirmation だが、hard filter にはしない。`2/3` も十分強く、sector strong 条件ではむしろ安定する。
- Crowded long では `low10 PBR` / `low10 PBR + low10 Fwd PER` を主条件にし、SMA5 は短期状態の補助表示に留める。
- 5D は短期反発と continuation が混ざるため、production判断は20D/60Dを主にする。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio construction、turnover、cost、capacity は未反映。
- `sma5_above_count_5d` は当日を含む rolling feature。終値基準の Daily Ranking research であり、pre-open screening 可能性は別途扱う。
- `min_observations=100` で strict scaffold を残している。`Crowded + low10 PBR + low10 Fwd PER` は code count が薄いため、portfolio lens 前に hard rule 化しない。
- 対象は 2024年以降の Prime。Standard/Growth や過去全期間には外挿しない。
- N225 excess はこの runner の primary output には入れていない。N225 benchmark readout との整合は scaffold 選定で参照したが、数値は TOPIX excess で読む。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_count_long_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_count_long_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_count_long_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-count-long-evidence/20260620_sma5_count_long_evidence_prime_2024_v1/`
- Results tables: `long_scaffold_evidence_df`, `sma5_count_group_evidence_df`, `long_scaffold_sma5_count_group_evidence_df`, `coverage_diagnostics_df`
