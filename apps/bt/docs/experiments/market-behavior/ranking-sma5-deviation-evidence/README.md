# Ranking SMA5 Deviation Evidence

Daily Ranking Research Base を使い、`sma5_deviation_pct = (close / SMA5 - 1) * 100` を short-term overheat / timing diagnostic として検証する。先行研究の `sma5_above_count_5d` は「直近5日のうち何日 SMA5 を上回ったか」だったが、この研究は当日の SMA5 乖離率そのものを扱う。

## Published Readout

### Decision

Run: `20260623_sma5_deviation_evidence_prime_2024_v2`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。SMA5 乖離率は `below_sma5_le_neg2`、`below_sma5_neg2_to_0`、`above_sma5_0_to_2`、`above_sma5_2_to_5`、`above_sma5_gt_5` に固定 bucket 化した。strict scaffold の tail を残すため、publication run は `min_observations=100` を使う。

結論:

- `SMA5乖離率` は standalone long / short selector ではない。全体 bucket では `>5%` が平均 return を押し上げるが、20D/60D median TOPIX excess はなおマイナスで、severe loss は最も重い。
- Long 側では、既存の `Deep Value + Long Hybrid Leadership + ATR20 Accel` 内でも `0〜2%` が一番きれいで、`2〜5%` は continuation confirmation というより短期過熱寄り。`<-2%` は明確な priority-down。
- Short 側では、`High PSR + Sector Weak` や `Overvalued + Sector Weak` が主条件で、SMA5 乖離率は主条件を置換しない。`dual_positive_crowded + High PSR + Sector Weak` はどの SMA5 bucket でも60Dが悪く、`0〜5%` の上振れ bucket でも十分 short/caution になる。
- `>5%` は Daily Ranking の既存 `Overheat` と同じ「価格過熱 risk flag」候補ではあるが、単体で short entry を作るほどではない。右尾も残るため、hard exclude ではなく caution / sizing / review badge として扱う。

### Main Findings

#### 結論: 全体では `>5%` が平均だけ良く、median と tail は悪い

Prime 全体では SMA5 乖離が高いほど mean は上がるが、median excess は20D/60Dともマイナスのまま。`above_sma5_gt_5` は severe loss が最大で、Overheat 的な tail risk を持つ。

| horizon | SMA5 bucket | obs | median excess | mean excess | win rate | severe loss | p10 excess |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | `below_sma5_le_neg2` | 103,363 | -1.276% | -0.372% | 43.18% | 12.34% | -11.058% |
| 20D | `below_sma5_neg2_to_0` | 317,669 | -1.017% | -0.558% | 43.25% | 8.66% | -9.384% |
| 20D | `above_sma5_0_to_2` | 352,712 | -0.941% | -0.400% | 43.85% | 8.15% | -9.153% |
| 20D | `above_sma5_2_to_5` | 101,795 | -0.913% | -0.030% | 45.18% | 11.71% | -10.810% |
| 20D | `above_sma5_gt_5` | 19,246 | -0.972% | +1.032% | 46.09% | 17.52% | -13.853% |
| 60D | `below_sma5_le_neg2` | 94,266 | -2.879% | -0.382% | 41.87% | 29.59% | -20.228% |
| 60D | `above_sma5_0_to_2` | 331,145 | -2.325% | -0.892% | 42.09% | 24.31% | -16.563% |
| 60D | `above_sma5_gt_5` | 16,673 | -1.998% | +2.124% | 45.65% | 31.52% | -23.267% |

#### 結論: primary long scaffold では `0〜2%` が最もきれいで、`2〜5%` はやや過熱

`Deep Value + Long Hybrid Leadership + ATR20 Accel` では `below_sma5_le_neg2` が弱い。`below_sma5_neg2_to_0` と `above_sma5_0_to_2` は20D/60Dで強く、特に60Dは `0〜2%` が最良。`2〜5%` もプラスだが、median / win rate / p10 のバランスは落ちる。

| scaffold | horizon | SMA5 bucket | obs | median excess | win rate | severe loss | p10 excess |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | `below_sma5_le_neg2` | 145 | -0.641% | 46.90% | 6.21% | -9.178% |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | `below_sma5_neg2_to_0` | 476 | +3.127% | 70.38% | 1.05% | -4.686% |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | `above_sma5_0_to_2` | 583 | +2.733% | 74.27% | 1.54% | -4.771% |
| Deep Value + Long Hybrid + ATR20 Accel | 20D | `above_sma5_2_to_5` | 302 | +0.855% | 59.27% | 0.99% | -4.543% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | `below_sma5_le_neg2` | 144 | -3.905% | 37.50% | 22.22% | -13.915% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | `below_sma5_neg2_to_0` | 475 | +3.694% | 65.26% | 6.53% | -8.591% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | `above_sma5_0_to_2` | 577 | +6.010% | 70.36% | 5.37% | -6.771% |
| Deep Value + Long Hybrid + ATR20 Accel | 60D | `above_sma5_2_to_5` | 294 | +3.409% | 66.33% | 10.20% | -10.101% |

#### 結論: neutral primary scaffold でも `0〜2%` を中心に見る

`Neutral Rerating + Deep Value + Long Hybrid + ATR20 Accel` でも同じ。`below_sma5_neg2_to_0` と `above_sma5_0_to_2` が主力で、`above_sma5_2_to_5` は20D/60Dとも劣る。

| horizon | SMA5 bucket | obs | median excess | win rate | severe loss | p10 excess |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | `below_sma5_neg2_to_0` | 356 | +3.382% | 74.16% | 0.84% | -3.878% |
| 20D | `above_sma5_0_to_2` | 485 | +3.060% | 78.97% | 1.44% | -3.906% |
| 20D | `above_sma5_2_to_5` | 258 | +0.896% | 60.08% | 0.39% | -4.407% |
| 60D | `below_sma5_neg2_to_0` | 356 | +3.794% | 68.26% | 5.06% | -7.741% |
| 60D | `above_sma5_0_to_2` | 485 | +6.967% | 73.81% | 4.33% | -5.041% |
| 60D | `above_sma5_2_to_5` | 257 | +3.584% | 68.87% | 8.95% | -9.538% |

#### 結論: short 側は SMA5 乖離より `High PSR + Sector Weak` が支配的

`high_liquidity_z_ge_1 + dual_positive_crowded + High PSR + Sector Weak` は、SMA5 bucket を問わず60Dが大きく悪い。`0〜2%` と `2〜5%` はどちらも severe が重く、SMA5 乖離率は「過熱したら初めて short」ではなく、既存 short overlay の補助診断に留める。

| price action | horizon | SMA5 bucket | obs | median excess | win rate | severe loss | p10 excess |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| dual positive crowded | 20D | `below_sma5_le_neg2` | 149 | -5.266% | 31.54% | 37.58% | -19.973% |
| dual positive crowded | 20D | `above_sma5_0_to_2` | 294 | -6.715% | 27.55% | 32.31% | -22.778% |
| dual positive crowded | 60D | `below_sma5_neg2_to_0` | 259 | -13.248% | 23.55% | 58.69% | -31.427% |
| dual positive crowded | 60D | `above_sma5_0_to_2` | 292 | -14.350% | 22.60% | 62.67% | -31.981% |
| dual positive crowded | 60D | `above_sma5_2_to_5` | 178 | -14.349% | 32.58% | 57.30% | -34.742% |
| dual negative stress | 60D | `below_sma5_neg2_to_0` | 820 | -8.358% | 31.34% | 44.39% | -27.974% |
| dual negative stress | 60D | `above_sma5_0_to_2` | 504 | -6.080% | 35.71% | 40.28% | -27.907% |

### Interpretation

SMA5 乖離率は「短期位置」を見るには有用だが、単体では long / short の方向を決めない。全体では `above_sma5_gt_5` に右尾がある一方、severe loss も最大で、Daily Ranking の既存 `Overheat` と同じく risk flag として読むのが自然。

Long 側の実務読みは、`Deep Value + Long Hybrid Leadership + ATR20 Accel` など既存 high-conviction scaffold の内側で `below_sma5_le_neg2` を priority-down に使い、`below_sma5_neg2_to_0` と `above_sma5_0_to_2` を主な usable timing として残すこと。`above_sma5_2_to_5` はまだ買えるケースもあるが、`0〜2%` より質が落ちるため、積極的な continuation confirmation にはしない。

Short 側では、`High PSR + Sector Weak`、`Overvalued + Sector Weak`、price action bucket の方が支配的。SMA5 乖離率は short-side overlay を強める補助ではあるが、`>5%` tail は sample が薄く右尾も残るため、primary short trigger にはしない。

### Production Implication

- Daily Ranking に出すなら、`SMA5 Deviation %` は diagnostic column / badge として扱う。
- Long triage では `below_sma5_le_neg2` を priority-down / pullback caution に使い、`above_sma5_0_to_2` を最も自然な short-term healthy state として読む。
- `above_sma5_2_to_5` と `above_sma5_gt_5` は Overheat-style caution。hard exclude ではなく、position sizing / review / entry delay の候補。
- Short triage では SMA5 乖離率を主条件にせず、`High PSR + Sector Weak`、`Overvalued + Sector Weak`、`Crowded/No Value` 系の既存 short overlay を先に見る。
- 既存 `sma5_above_count_5d` と併用する場合、count は micro-trend persistence、deviation は当日の短期過熱度として役割を分ける。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio construction、turnover、cost、capacity は未反映。
- `SMA5` は当日終値を含む rolling feature。pre-open screening 可能性は別研究が必要。
- `min_observations=100` は strict scaffold の tail を残すための publication setting。`above_sma5_gt_5` は特に sample が薄く、hard rule 化しない。
- 対象は 2024年以降の Prime。Standard/Growth や過去全期間には外挿しない。
- N225 excess はこの runner の primary output には入れていない。数値は TOPIX excess で読む。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_deviation_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_deviation_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_deviation_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-deviation-evidence/20260623_sma5_deviation_evidence_prime_2024_v2/`
- Results tables: `sma5_deviation_bucket_evidence_df`, `long_scaffold_sma5_deviation_evidence_df`, `short_overlay_sma5_deviation_evidence_df`, `coverage_diagnostics_df`
