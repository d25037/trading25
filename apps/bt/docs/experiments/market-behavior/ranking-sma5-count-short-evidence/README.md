# Ranking SMA5 Count Short Evidence

Daily Ranking Research Base を使い、`sma5_above_count_5d` を short-side diagnostic として検証する。定義は「当日を含む直近5営業日のうち、終値がその日の SMA5 を上回った日数」。値域は `0..5` で、`5` は直近5日すべてが SMA5 上、`0` は直近5日すべてが SMA5 以下を意味する。

## Published Readout

### Decision

Run: `20260620_sma5_count_short_evidence_prime_2024_v2`

Grouped follow-up run: `20260620_sma5_count_short_evidence_prime_2024_grouped`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。既存の short-side rerun と同じく `liquidity_residual_z` band、strict 20D x 60D price-action bucket、`High PSR` / `Sector Weak` overlay を使い、その内側で `sma5_above_count_5d = 0..5` を比較した。

結論:

- `sma5_above_count_5d` は standalone の優秀な short selector ではない。全体では `0/1` が 20D/60D の weak-trend confirmation になりやすいが、既存の `High PSR + Sector Weak` や price-action bucket を置換するほど強くない。
- 5D target では差は出るが小さい。worst robust row でも 5D median excess はおおむね `-1%` 台で、主要判断は 20D/60D の方が安定する。
- `high liquidity + dual_negative_stress + High PSR + Sector Weak + sma5_count=0` は 20D/60D とも素直に悪い。20D median `-4.120%`、60D median `-8.386%`。
- 一方で、もともと最強だった `high liquidity + dual_positive_crowded + High PSR + Sector Weak` は count split 後に sample が薄い。min100 exploratory では 60D median が `-12%` から `-15%` 台まで悪いが、count ごとの sample は `110..232` 程度で、`0/1/5` のどれかを hard rule にする根拠は弱い。
- `0 or 1` / `2 or 3` / `4 or 5` の3群にすると sample 問題はかなり改善する。実務上は `0/1` を「弱い値動きの確認」、`4/5` を「上昇中でも valuation/sector が悪い blowoff caution」として分ける。どちらも primary short 条件ではなく tie-breaker。

### Main Findings

#### 結論: 全体では `0/1` が broad weak-trend short confirmation

| liquidity band | horizon | count 0 median | count 5 median | count 0 severe | count 5 severe | interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `z >= 1` | 5D | -0.410% | -0.248% | 3.08% | 4.09% | 5D では差が小さい |
| `z >= 1` | 20D | -1.312% | -0.775% | 17.12% | 17.10% | count 0 がやや悪い |
| `z >= 1` | 60D | -3.571% | -2.440% | 34.64% | 32.51% | count 0 が悪い |
| `-1 < z < 1` | 5D | -0.324% | -0.355% | 1.06% | 1.13% | ほぼ同等 |
| `-1 < z < 1` | 20D | -1.215% | -0.648% | 9.20% | 8.54% | count 0 が悪い |
| `-1 < z < 1` | 60D | -2.440% | -2.081% | 24.94% | 24.64% | count 0/1 がやや悪い |
| `z < -1` | 60D | -2.581% | -3.113% | 22.08% | 23.98% | low liquidity では count 5 も悪く、別物として扱う |

#### 結論: 3群化では `0/1` が broad weak-trend、`4/5` は blowoff 側

Grouped run では sample が厚くなり、全体像は読みやすくなった。全体では `z >= 1` と `-1 < z < 1` の 20D/60D で `0/1` が最も悪く、短期弱さの確認として使いやすい。一方、`z < -1` は低流動性・stale 側の別物で、`4/5` も悪化しやすい。

| liquidity band | horizon | 0/1 obs | 0/1 median | 2/3 median | 4/5 median | 0/1 severe | 4/5 severe |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `z >= 1` | 5D | 40,400 | -0.398% | -0.380% | -0.319% | 3.16% | 4.06% |
| `z >= 1` | 20D | 39,057 | -1.225% | -1.080% | -0.952% | 16.94% | 17.22% |
| `z >= 1` | 60D | 36,475 | -3.357% | -2.918% | -2.425% | 34.25% | 32.69% |
| `-1 < z < 1` | 20D | 174,000 | -1.162% | -0.969% | -0.713% | 8.78% | 8.50% |
| `-1 < z < 1` | 60D | 160,521 | -2.484% | -2.144% | -2.034% | 24.98% | 24.41% |
| `z < -1` | 60D | 28,164 | -2.809% | -2.620% | -2.854% | 22.06% | 22.90% |

#### 結論: `High PSR + Sector Weak` の内側では count は補助診断に留まる

`min_observations=500` の robust table では、SMA5 count split 後に残る row が限られる。残った範囲では `sma5_count=0/1/2` の weak-trend 系が悪い一方、`mid liquidity + dual_positive_crowded` では `sma5_count=5` も悪い。

| horizon | liquidity band | bucket | best/worst count | obs | median excess | severe loss |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 5D | `z >= 1` | dual_negative_stress | 1 | 593 | -1.332% | 5.90% |
| 20D | `z >= 1` | dual_negative_stress | 0 | 657 | -4.120% | 31.96% |
| 60D | `z >= 1` | dual_negative_stress | 0 | 654 | -8.386% | 44.95% |
| 20D | `-1 < z < 1` | dual_positive_crowded | 5 | 784 | -3.603% | 21.30% |
| 60D | `-1 < z < 1` | dual_positive_crowded | 5 | 766 | -10.024% | 50.13% |
| 60D | `-1 < z < 1` | dual_negative_stress | 1 | 1,553 | -8.450% | 44.88% |
| 60D | `-1 < z < 1` | recent20_negative_60d_positive | 2 | 583 | -8.221% | 43.91% |

#### 結論: `High PSR + Sector Weak` の3群比較では、`0/1` と `4/5` が役割分担する

3群化すると、robust sample で比較できる row が増える。`dual_negative_stress` では `0/1` が一貫して悪く、weak-trend confirmation として自然。一方、`dual_positive_crowded` や `recent20_positive_60d_negative` では `4/5` が悪いケースもあり、これは上昇中の short ではなく `High PSR + Sector Weak` と重なった blowoff caution と読む。

| horizon | liquidity band | bucket | worst group | obs | median excess | severe loss |
| ---: | --- | --- | --- | ---: | ---: | ---: |
| 20D | `z >= 1` | dual_negative_stress | `0/1` | 1,241 | -3.922% | 28.12% |
| 60D | `z >= 1` | dual_negative_stress | `0/1` | 1,228 | -7.446% | 43.32% |
| 20D | `-1 < z < 1` | dual_positive_crowded | `0/1` | 661 | -3.930% | 21.03% |
| 60D | `-1 < z < 1` | dual_positive_crowded | `4/5` | 1,607 | -9.391% | 48.10% |
| 60D | `-1 < z < 1` | recent20_positive_60d_negative | `4/5` | 535 | -10.157% | 50.84% |
| 60D | `-1 < z < 1` | dual_negative_stress | `0/1` | 3,228 | -8.080% | 43.53% |

#### 結論: 最強 scaffold の count split は sample が薄い

Supplemental run: `20260620_sma5_count_short_evidence_prime_2024_min100`

既存 research で最も悪かった `z >= 1 + dual_positive_crowded + High PSR + Sector Weak` は、SMA5 count で割ると各 count の sample が `110..235` 程度まで薄くなる。60D は全 count がかなり悪いが、count そのものの優劣はまだ hard rule にできない。

| count | 20D obs | 20D median | 20D severe | 60D obs | 60D median | 60D severe |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 111 | -4.722% | 36.94% | 110 | -13.005% | 54.55% |
| 2 | 204 | -4.432% | 30.88% | 199 | -13.866% | 60.30% |
| 3 | 235 | -5.203% | 34.47% | 232 | -15.617% | 63.36% |
| 4 | 185 | -5.361% | 34.59% | 184 | -12.115% | 53.80% |
| 5 | 159 | -6.731% | 33.33% | 158 | -14.885% | 59.49% |

### Interpretation

`sma5_above_count_5d` は、既存の 20D/60D return bucket よりも短い micro-trend confirmation である。`0` は「直近5日すべて SMA5 以下」で、短期的な弱さを示すため、broad な high/mid liquidity universe では 20D/60D の left-tail 悪化と整合する。

ただし、short-side の主役は引き続き `High PSR` と `Sector Weak` の overlap、そして liquidity/price-action の文脈である。SMA5 count はそれらを置換せず、同じ short scaffold の中で priority を少し動かす程度に使う。

`sma5_count=5` は「上昇しているから long」という意味ではない。`dual_positive_crowded + High PSR + Sector Weak` のように、上がっているが売上対比で高く、セクターが弱い群では 60D 左尾が重い。これは continuation ではなく blowoff / crowded rerating caution と読む。

3群化後の実務的な読みは、`0/1` は weak-trend short confirmation、`2/3` は中立寄り、`4/5` は単独では short ではないが crowded / dual-positive 系の overvalued-sector-weak と重なった時だけ caution、でよい。

### Production Implication

- Daily Ranking に出すなら `SMA5 Above Count 5D` は diagnostic column / tooltip で十分。primary short filter や preset にはまだ昇格しない。
- short-side triage では、`sma5_count 0/1` を weak-trend confirmation として使う。特に `dual_negative_stress + High PSR + Sector Weak` では 20D/60D の補助優先度を上げてよい。
- `sma5_count 4/5` は単独では short ではない。`Crowded / dual-positive + High PSR + Sector Weak` と重なったときだけ、blowoff caution として扱う。
- 5D RETURN target は runner default に追加したが、5D は差が小さく sample split のノイズも大きい。short候補の足切り判断は 20D/60D を主、5D を tactical confirmation とする。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。short trade の borrow / execution / stop / sizing は含まない。
- `sma5_above_count_5d` は当日を含む rolling feature。終値基準の Daily Ranking research であり、pre-open screening 可能性は別途扱う。
- 単独 count に割ると最重要 scaffold は sample が薄い。3群化は改善するが、それでも primary short 条件ではなく補助診断に留める。
- 対象は 2024年以降の Prime。Standard/Growth や過去全期間には外挿しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_count_short_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_count_short_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_count_short_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-count-short-evidence/20260620_sma5_count_short_evidence_prime_2024_v2/`
- Grouped bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-count-short-evidence/20260620_sma5_count_short_evidence_prime_2024_grouped/`
- Supplemental bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-count-short-evidence/20260620_sma5_count_short_evidence_prime_2024_min100/`
- Results tables: `sma5_count_evidence_df`, `sma5_count_group_evidence_df`, `short_overlay_sma5_count_evidence_df`, `short_overlay_sma5_count_group_evidence_df`, `short_overlay_evidence_df`
