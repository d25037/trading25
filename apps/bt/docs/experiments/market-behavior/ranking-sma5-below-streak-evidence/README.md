# Ranking SMA5 Below-Streak Evidence

## Published Readout

Daily Ranking Research Base を使い、`close < SMA5` が3営業日連続した状態 (`below_sma5_streak_ge3`) が、その他 (`below_sma5_streak_other`) より 5D / 20D / 60D forward TOPIX excess return で劣後するかを検証した。

Run: `20260702_sma5_below_streak_count_cross_prime_2018_v1`

対象は Prime、`analysis_start_date=2018-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。前回の SMA5 count long 研究と同じ long scaffold を使い、全体比較に加えて同日内比較も出した。same-day spread は `below_sma5_streak_ge3 - below_sma5_streak_other` なので、spread がマイナスなら「3日連続 SMA5 下回りの方が弱い」という意味。`ge3 x 0/1/2/3/4/5` の cross comparison では、`weak condition - comparison condition` に符号を統一した。

### Decision

結論:

- 全体では、3日連続 SMA5 下回りは「その他」より明確には劣後しない。5D はむしろ rebound / mean-reversion が残り、same-day spread も `+0.077%`。
- 20D/60D の全体差はほぼゼロで、SMA5 below-streak 単独を stop / sell signal にする根拠は弱い。
- 一方、既存の強い long scaffold 内では別。`Deep Value + Long Hybrid + ATR20 Accel` と `Neutral + Deep Value + Long Hybrid + ATR20 Accel` では、3日連続 below が 20D/60D で明確に劣後する。
- 特に `Neutral + Deep Value + Long Hybrid + ATR20 Accel` は same-day spread が 20D `-1.180%`、60D `-3.680%`。これは `0/1` と同様に、即時 liquidation ではなく `exit_watch` / 戻り売り検証の候補。
- `Neutral + Deep Value + Long Hybrid + ATR20 Accel` の cross comparison では、`ge3 + 0/1` は同日 `other + 2/3` / `other + 4/5` に対して60D `-3.014%` / `-5.485%`。`other + 0/1` も60D `-1.607%` / `-2.654%`。同じ scaffold 内の入れ替え候補としては、`ge3 + 0/1` が最も強い。
- `Sector Strong` まで絞ると一部で悪化しない。SMA5 below-streak は hard filter ではなく、scaffold と併用する timing / exit diagnostic として扱う。

### Main Findings

#### 結論: 全体では3日連続 below は劣後しない

Prime 全体では、`below_sma5_streak_ge3` は「その他」より median excess が悪くない。特に5Dは `-0.133%` vs `-0.232%` で、短期 rebound が出ている。60D も `-1.673%` vs `-1.955%` で、単独 stop signal としては逆方向。

| horizon | bucket | observations | median excess | mean excess | win rate | severe loss | median SMA5 deviation |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5D | `other` | 2,963,422 | -0.232% | -0.062% | 46.77% | 1.31% | +0.547% |
| 5D | `ge3` | 1,019,622 | -0.133% | +0.048% | 48.13% | 1.27% | -1.428% |
| 20D | `other` | 2,936,510 | -0.698% | -0.133% | 45.45% | 8.47% | +0.548% |
| 20D | `ge3` | 1,010,594 | -0.675% | -0.159% | 45.63% | 8.68% | -1.429% |
| 60D | `other` | 2,866,570 | -1.955% | -0.494% | 43.09% | 23.24% | +0.552% |
| 60D | `ge3` | 985,126 | -1.673% | -0.262% | 44.05% | 22.61% | -1.432% |

#### 結論: 同日内でも全体の劣後は確認できない

市場全体の下落・rebound 日を拾っている可能性を避けるため、同じ date / market_scope 内で `ge3 - other` を比較した。全体では 5D が `+0.077%`、20D/60D はほぼゼロ。したがって「3日連続 SMA5 下回りなら常に弱い」とは言えない。

| horizon | matched dates | other obs | ge3 obs | other daily median | ge3 daily median | ge3 - other | ge3 win days |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5D | 2,066 | 2,963,422 | 1,019,622 | -0.207% | -0.148% | +0.077% | 55.18% |
| 20D | 2,051 | 2,936,510 | 1,010,594 | -0.635% | -0.633% | +0.008% | 50.17% |
| 60D | 2,011 | 2,866,570 | 985,126 | -1.697% | -1.755% | -0.006% | 49.98% |

#### 結論: 強い long scaffold 内では 20D/60D の悪化が出る

全体では劣後しないが、既存 long scaffold の中では意味が変わる。`Deep Value + Long Hybrid + ATR20 Accel` は 60D same-day spread が `-2.628%`、`Neutral + Deep Value + Long Hybrid + ATR20 Accel` は 20D `-1.180%`、60D `-3.680%`。これは position を即時に落とすというより、戻りを待つ exit-watch signal として読むのが自然。

| long scaffold | 5D ge3-other | 20D ge3-other | 60D ge3-other | read |
| --- | ---: | ---: | ---: | --- |
| `deep_value` | +0.041% | +0.089% | +0.051% | Deep Value 単独では below-streak の悪化なし。 |
| `deep_value_long_hybrid_atr20_accel` | +0.052% | -0.058% | -2.628% | 60D で悪化。long continuation の劣化候補。 |
| `neutral_deep_value` | +0.071% | +0.030% | -0.138% | Broad neutral value では弱い caution。 |
| `neutral_long_hybrid_atr20_accel` | +0.023% | -0.115% | +0.050% | Mixed。hard rule には弱い。 |
| `neutral_deep_value_long_hybrid_atr20_accel` | -0.238% | -1.180% | -3.680% | Primary scaffold では 20D/60D の exit-watch 根拠。 |
| `neutral_deep_value_sector_strong_atr20_accel` | +0.391% | -0.362% | -1.499% | 5D rebound は残るが、20D/60D はやや悪化。 |
| `crowded_long_hybrid` | -0.175% | +0.303% | -0.469% | Crowded は value confirmation 優先。below-streak は補助。 |

#### 結論: Primary scaffold の raw bucket でも60D悪化が大きい

`Neutral + Deep Value + Long Hybrid + ATR20 Accel` の raw bucket では、60D median excess が `other +3.721%` に対し `ge3 +0.347%` まで低下する。20D も `+2.556%` から `+2.005%` へ低下するため、same-day 比較だけでなく通常集計でも方向は一致する。

| horizon | bucket | observations | median excess | mean excess | win rate | severe loss |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 5D | `other` | 1,637 | +0.401% | +0.844% | 56.08% | 0.12% |
| 5D | `ge3` | 220 | +0.459% | +0.320% | 57.73% | 0.00% |
| 20D | `other` | 1,623 | +2.556% | +3.609% | 71.04% | 1.05% |
| 20D | `ge3` | 218 | +2.005% | +1.835% | 65.60% | 2.75% |
| 60D | `other` | 1,604 | +3.721% | +7.507% | 65.34% | 7.79% |
| 60D | `ge3` | 214 | +0.347% | +2.392% | 52.34% | 10.28% |

#### 結論: `ge3 x SMA5 count` では `ge3 + 0/1` が最も強い入れ替え候補

符号はすべて `weak condition - comparison condition`。マイナスなら弱い条件が同じ日の比較対象より劣後したという意味。`Neutral + Deep Value + Long Hybrid + ATR20 Accel` では、`ge3 + 0/1` が `other + 2/3` / `other + 4/5` に対して20D/60Dで明確に弱い。`other + 0/1` も弱いが、`ge3 + 0/1` の60D劣後の方が大きい。

| horizon | weak condition | comparison | matched dates | weak obs | comparison obs | weak - comparison | weak underperform days |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 5D | `other + 0/1` | `other + 2/3` | 40 | 57 | 156 | -0.898% | 60.00% |
| 5D | `other + 0/1` | `other + 4/5` | 42 | 62 | 97 | -0.697% | 69.05% |
| 5D | `ge3 + 0/1` | `other + 2/3` | 38 | 83 | 170 | -0.646% | 57.89% |
| 5D | `ge3 + 0/1` | `other + 4/5` | 42 | 75 | 93 | +0.255% | 45.24% |
| 20D | `other + 0/1` | `other + 2/3` | 40 | 57 | 156 | -1.866% | 57.50% |
| 20D | `other + 0/1` | `other + 4/5` | 41 | 61 | 96 | -2.373% | 65.85% |
| 20D | `ge3 + 0/1` | `other + 2/3` | 38 | 83 | 170 | -1.097% | 65.79% |
| 20D | `ge3 + 0/1` | `other + 4/5` | 42 | 75 | 93 | -1.642% | 66.67% |
| 60D | `other + 0/1` | `other + 2/3` | 40 | 57 | 156 | -1.607% | 60.00% |
| 60D | `other + 0/1` | `other + 4/5` | 41 | 61 | 95 | -2.654% | 63.41% |
| 60D | `ge3 + 0/1` | `other + 2/3` | 36 | 80 | 168 | -3.014% | 66.67% |
| 60D | `ge3 + 0/1` | `other + 4/5` | 41 | 74 | 92 | -5.485% | 63.41% |

`ge3 + 2/3` は sample が薄く、20Dでは弱いが60Dでは劣後幅が小さい。`ge3 + 4/5` は構造上ほぼ出ない。したがって production candidate としては `ge3` 全体ではなく、まず `ge3 + 0/1` を強い rotation trigger として扱う。

### Interpretation

`close < SMA5` の3日連続は、全銘柄に対する standalone sell/stop signal ではない。全体では5D rebound が残り、20D/60D の差も小さい。これは前回の `0/1` 議論と同じで、短期の下振れは直後の戻りを含みやすい。

ただし、強い long scaffold の中で出る3日連続 below は意味がある。特に `Neutral + Deep Value + Long Hybrid + ATR20 Accel` では、本来強い条件なのに短期足が3日連続で SMA5 を下回ると、20D/60D の優位が大きく薄れる。さらに `ge3 + 0/1` まで重なると、同じ日の `other + 2/3` / `other + 4/5` に大きく劣後する。運用仮説としては、`ge3 + 0/1` 到達で同じ scaffold 内の `other + 2/3` / `other + 4/5` へ入れ替える rotation rule を次に検証するのが妥当。

### Production Implication

- Daily Ranking に表示するなら、`SMA5 Below Streak >=3` は standalone bad flag ではなく、long scaffold 内の exit-watch / priority-down diagnostic として扱う。
- `Neutral + Deep Value + Long Hybrid + ATR20 Accel` では、`below_sma5_streak_ge3` を 20D/60D 劣化の注意サインとして表示する価値がある。
- `ge3 + 0/1` は同じ scaffold 内の `other + 2/3` / `other + 4/5` への rotation trigger 候補。これは cash exit より、相対入れ替えとして検証する。
- ただし即時 liquidation rule にはしない。全体・5D では rebound があるため、入れ替え execution timing を別 runner で検証する。
- `Sector Strong` や crowded value-confirmed cases では挙動が一様でないため、SMA5 below-streak を hard filter にしない。

### Caveats

- observation-level forward response であり、portfolio construction、turnover、cost、capacity は未反映。
- `close < SMA5` と forward return は終値基準。pre-open screening 可能性や intraday execution は別途扱う。
- 対象は2018年以降の Prime。Standard/Growth やさらに古い期間には外挿しない。
- Same-day spread は daily median の差であり、実運用の position sizing や保有中銘柄の path-dependent exit ではない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_below_streak_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_below_streak_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_below_streak_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-below-streak-evidence/20260702_sma5_below_streak_count_cross_prime_2018_v1/`
- Results tables: `sma5_below_streak_evidence_df`, `long_scaffold_sma5_below_streak_evidence_df`, `long_scaffold_sma5_below_streak_count_cross_df`, `same_day_sma5_below_streak_spread_df`, `long_scaffold_same_day_sma5_below_streak_spread_df`, `long_scaffold_same_day_sma5_below_streak_count_cross_spread_df`, `coverage_diagnostics_df`
