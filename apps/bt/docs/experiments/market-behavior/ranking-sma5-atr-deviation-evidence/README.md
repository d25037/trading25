# Ranking SMA5 ATR Deviation Evidence

Daily Ranking Research Base を使い、`sma5_atrN_deviation = (close - SMA5) / ATRN` を short-term overheat / stop-review diagnostic として検証する。固定%の `sma5_deviation_pct` は銘柄ボラティリティ差を吸収しにくかったため、この研究では `ATR5` と `ATR20` で正規化し、上方向と下方向を別々に見る。

## Published Readout

### Decision

Run: `20260630_sma5_atr_deviation_prime_2024_v1`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。ATR 正規化は `ATR5` と `ATR20` を比較し、方向別 threshold は `0.5 / 1.0 / 1.5 / 2.0 ATR` を使う。strict scaffold の tail を残すため、publication run は `min_observations=100`。

結論:

- `SMA5 ATR deviation` も standalone long / short selector ではない。Prime 全体では上方向 `>2 ATR` も下方向 `<-2 ATR` も20D/60Dの median excess はマイナスで、tail risk が重い。
- Long entry 回避は「上方向なら即回避」ではない。既存の `Deep Value + Long Hybrid Leadership + ATR20 Accel` 内では `>=0.5 ATR` はむしろ良く、`>=1.0 ATR` から鈍化する。`>=1.5 ATR` 以上は sample が薄く、hard entry ban ではなく追いかけ買い抑制 / sizing caution が妥当。
- 早期損切り・縮小候補は下方向の方が読みやすい。`Deep Value + Long Hybrid Leadership + ATR20 Accel` では `ATR20 <= -1.0` が20D median `-1.74%`、60D median `-5.02%` まで悪化し、`<= -0.5` より明確に悪い。
- `ATR5` と `ATR20` は大枠で同じだが、下方向の損切り候補は `ATR20` の方が安定して悪化を拾う。`ATR5` は短期ノイズに反応しやすく、entry delay / intraday review 向き。

### Main Findings

#### 結論: Prime 全体では上下 extreme はどちらも tail が重い

全体 bucket では `0〜1 ATR` 近辺が相対的に無難で、`>2 ATR` と `<-2 ATR` は20D/60Dとも severe loss が重い。上方向 extreme は右尾も残るため、単純な short trigger にはしない。

| ATR | horizon | bucket | obs | median excess | mean excess | win rate | severe loss | p10 excess |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 20D | `< -2 ATR` | 3,591 | -1.53% | -0.38% | 42.6% | 13.3% | -11.27% |
| 5 | 20D | `0〜0.5 ATR` | 233,847 | -0.97% | -0.33% | 43.9% | 9.2% | -9.61% |
| 5 | 20D | `1〜2 ATR` | 81,656 | -0.88% | -0.08% | 44.8% | 9.8% | -9.90% |
| 5 | 20D | `> 2 ATR` | 3,315 | -1.22% | +0.12% | 43.9% | 14.6% | -11.95% |
| 20 | 20D | `< -2 ATR` | 11,807 | -1.87% | -1.11% | 39.7% | 11.7% | -10.57% |
| 20 | 20D | `0〜0.5 ATR` | 244,911 | -0.96% | -0.35% | 43.9% | 9.1% | -9.56% |
| 20 | 20D | `1〜2 ATR` | 75,151 | -0.88% | -0.07% | 44.9% | 9.8% | -9.88% |
| 20 | 20D | `> 2 ATR` | 12,174 | -0.97% | +0.28% | 44.7% | 12.7% | -11.28% |
| 20 | 60D | `< -2 ATR` | 11,166 | -3.28% | -1.52% | 38.8% | 26.8% | -17.60% |
| 20 | 60D | `> 2 ATR` | 11,145 | -2.51% | -0.11% | 43.0% | 28.8% | -19.36% |

#### 結論: strong long では上方向 `>=0.5 ATR` は回避条件ではない

`Deep Value + Long Hybrid Leadership + ATR20 Accel` では、上方向 `>=0.5 ATR` は20D/60Dとも良い。`>=1.0 ATR` では20Dが鈍化し、60Dの severe loss が上がる。したがって上方向は `0.5 ATR` で止めず、`1.0 ATR` 以上を「追いかけ買い注意」、`1.5 ATR` 以上を薄い tail として review する。

| scaffold | ATR | direction | threshold | horizon | obs | median excess | win rate | severe loss | p10 excess |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | 5 | above | 0.5 | 20D | 533 | +1.49% | 65.3% | 1.3% | -4.58% |
| Deep Value + Long Hybrid + ATR20 Accel | 5 | above | 1.0 | 20D | 248 | +0.90% | 58.1% | 0.8% | -5.14% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | above | 0.5 | 20D | 542 | +1.47% | 65.3% | 1.3% | -4.62% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | above | 1.0 | 20D | 262 | +0.86% | 56.9% | 0.8% | -5.10% |
| Deep Value + Long Hybrid + ATR20 Accel | 5 | above | 0.5 | 60D | 515 | +3.63% | 66.8% | 7.0% | -7.91% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | above | 1.0 | 60D | 254 | +3.11% | 64.2% | 9.4% | -9.60% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | 20 | above | 0.5 | 60D | 458 | +4.09% | 69.2% | 6.6% | -7.71% |
| Neutral + Deep Value + Long Hybrid + ATR20 Accel | 20 | above | 1.0 | 60D | 225 | +3.45% | 65.8% | 8.4% | -8.61% |

#### 結論: 下方向は `ATR20 <= -1.0` が損切り・縮小候補

同じ strong long scaffold で下方向を見ると、`<= -0.5 ATR` はまだ残せるが、`ATR20 <= -1.0` は20D/60Dとも明確に悪い。`ATR5 <= -1.0` は publication threshold では sample が100未満になることがあり、短期alertとして扱う。

| scaffold | ATR | direction | threshold | horizon | obs | median excess | win rate | severe loss | p10 excess |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Long Hybrid + ATR20 Accel | 5 | below | 0.5 | 20D | 235 | +1.08% | 58.3% | 4.3% | -6.65% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | below | 0.5 | 20D | 255 | +1.43% | 62.0% | 3.9% | -6.34% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | below | 1.0 | 20D | 100 | -1.74% | 42.0% | 8.0% | -9.45% |
| Deep Value + Long Hybrid + ATR20 Accel | 5 | below | 0.5 | 60D | 234 | -0.95% | 46.6% | 15.4% | -11.88% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | below | 0.5 | 60D | 253 | -0.04% | 49.8% | 13.8% | -11.38% |
| Deep Value + Long Hybrid + ATR20 Accel | 20 | below | 1.0 | 60D | 100 | -5.02% | 26.0% | 26.0% | -14.77% |
| Neutral + Long Hybrid + ATR20 Accel | 20 | below | 1.0 | 60D | 163 | -1.11% | 46.6% | 17.8% | -13.32% |

#### 結論: short 側は既存 `High PSR + Sector Weak` が支配的

`dual_positive_crowded + High PSR + Sector Weak` は、上方向ATR乖離を重ねるとさらに悪いが、これは SMA5 ATR deviation 単独ではなく既存 short overlay の内側で読むべき。`>=0.5 ATR` でも十分悪く、`>=1.0 ATR` 以上は sample が薄くなる。

| price action | overlay | ATR | direction | threshold | horizon | obs | median excess | win rate | severe loss | p10 excess |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| dual positive crowded | High PSR + Sector Weak | 5 | above | 0.5 | 20D | 286 | -6.29% | 31.1% | 35.0% | -23.00% |
| dual positive crowded | High PSR + Sector Weak | 5 | above | 1.0 | 20D | 107 | -7.34% | 31.8% | 38.3% | -20.62% |
| dual positive crowded | High PSR + Sector Weak | 20 | above | 0.5 | 60D | 279 | -15.46% | 29.4% | 59.1% | -41.58% |
| dual positive crowded | High PSR + Sector Weak | 20 | above | 1.0 | 60D | 127 | -16.00% | 30.7% | 57.5% | -45.10% |

### Interpretation

ATR 正規化で、固定%版の「銘柄ボラ差で同じ2%/5%の意味が違う」問題はかなり整理できる。ただし、SMA5からの距離だけで候補を作るほどの力はない。全体 bucket は相変わらず median がマイナスで、既存の valuation / liquidity / sector / ATR20 acceleration scaffold が必要。

上方向は、strong long 内では `>=0.5 ATR` を entry 回避にすると良い候補を落としすぎる。`>=1.0 ATR` で20Dの優位が落ち、`>=1.5 ATR` 以上は publication row が薄くなりやすいため、entry delay / sizing caution / TradingView確認の閾値に留める。

下方向は、strong long 内でも `ATR20 <= -1.0` が悪化しやすい。これは「即売り」ではなく、既存 long thesis が崩れていないかを確認し、position shrink / stop review を起動する実務線として使いやすい。`ATR5 <= -1.0` は早いがノイズが強いため、単独損切りより alert に向く。

### Production Implication

- Daily Ranking に出すなら `SMA5/ATR5` と `SMA5/ATR20` は diagnostic column / badge として扱う。
- Long entry は `above >= 0.5 ATR` で除外しない。`above >= 1.0 ATR` を追いかけ買い注意、`above >= 1.5 ATR` を薄い overextension review とする。
- Long holding / stop review は `ATR20 <= -1.0` を第一候補にする。`ATR5 <= -1.0` は早期alert、`ATR20 <= -0.5` は watch に留める。
- Short triage では SMA5 ATR deviation を主条件にせず、`High PSR + Sector Weak` / `Overvalued + Sector Weak` の既存 overlay を先に見る。
- 固定%版の `2% / 5%` threshold は UI上の参考に留め、実運用判断はATR正規化を優先する。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio construction、turnover、cost、capacity は未反映。
- `SMA5` と ATR は当日終値を含む rolling feature。pre-open screening 可能性は別研究が必要。
- `min_observations=100` は strict scaffold の tail を残すための publication setting。`>=1.5 ATR` / `>=2.0 ATR` は特に thin-sample diagnostic として読む。
- 対象は 2024年以降の Prime。Standard/Growth や過去全期間には外挿しない。
- `ATR5` と `ATR20` はどちらも日足ベースで、intraday stop ルールそのものではない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_sma5_atr_deviation_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_sma5_atr_deviation_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_atr_deviation_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-sma5-atr-deviation-evidence/20260630_sma5_atr_deviation_prime_2024_v1/`
- Results tables: `sma5_atr_deviation_bucket_evidence_df`, `long_scaffold_sma5_atr_threshold_evidence_df`, `short_overlay_sma5_atr_threshold_evidence_df`, `coverage_diagnostics_df`
