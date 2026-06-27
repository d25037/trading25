# Ranking Moving Average Replacement Evidence

Daily Ranking Research Base を使い、既存の定点比較 `recent_return_20d_pct` / `recent_return_60d_pct` を `close / SMA20|EMA20 - 1` / `close / SMA60|EMA60 - 1` で置換した場合に、現行判定の forward response がどう変わるかを検証する。`Overheat` と `stale_rally_fade` は bad / caution 指標なので、これらは return 改善ではなく悪い forward response や tail をどれだけ濃縮するかで読む。

## Published Readout

### Decision

Run: `20260626_ma_ema_neutral_long_prime_2024`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。現行判定は `20D>0,60D>0` などの strict sign bucket と `Overheat = recent_return_20d_pct >= 30`。SMA/EMA 代替は `SMA20/SMA60` と `EMA20/EMA60` に対する当日終値の乖離率で同じ符号 bucket を作る。`Overheat` だけは閾値スケールが違うため、literal `*_20_deviation_pct >= 30` と、現行 Overheat と同じ observation share に合わせた q-matched overheat を両方出した。q-matched 閾値は `sma20_deviation_pct >= 15.1437%`、`ema20_deviation_pct >= 13.1227%`。

結論:

- `20D>0,60D>0` を `SMA20/SMA60` または `EMA20/EMA60` に置換しても、20D/60D forward response の差は小さい。60D median は fixed `-2.342%`、SMA `-2.231%`、EMA `-2.225%` で、EMA がわずかに最良だが主条件を変えるほど大きくない。
- `20D<0,60D>0` は EMA 代替が最も良い。20D median は fixed `-1.242%`、SMA `-1.032%`、EMA `-0.994%`。60D median は fixed `-2.753%`、SMA `-2.576%`、EMA `-2.545%`。ただし still negative なので positive signal ではなく、弱さの分類を少し滑らかにする程度。
- `20D>0,60D<0` と `20D<0,60D<0` は EMA/SMA 代替で明確に改善しない。特に dual negative は60Dで fixed `-2.313%`、SMA `-2.536%`、EMA `-2.496%` と悪化寄り。
- `neutral_rerating` の long候補に絞ると、SMA/EMA sign は全体より使いやすい。`neutral_deep_value_long_hybrid_atr20_accel` では60D median が all `+4.981%`、SMA `>0,>0` が `+5.338%`、EMA `>0,>0` が `+5.230%`。Deep Value + Sector Strong + ATR20 Accel でも all `+3.413%`、SMA `+3.541%`、EMA `+3.530%` と小幅改善する。
- neutral long候補内では、`SMA/EMA <0,>0` は priority-down 寄り。`neutral_deep_value` の60D median は all `+1.564%` に対し、SMA `<0,>0` が `+0.309%`、EMA `<0,>0` が `+0.115%` まで落ちる。
- `Overheat` は bad / caution 指標として評価する。literal `EMA20乖離>=30%` は60D median `-2.517%`、severe `37.68%` と最も tail を濃縮するが、観測数は `560` と薄く、右尾 mean も大きい。q-matched EMA20 overheat は q-matched SMA20 より穏当で、現行 `20D>=30%` を置換するほどではない。
- `stale_rally_fade` candidate は fixed / SMA / EMA のどれでも明確に悪い。EMA 版は観測数がさらに増えるが、20D/60D の bad/caution 解釈はほぼ同じで、現行指標の意味は変わらない。

### Main Findings

#### 結論: 全体の sign bucket は約7割一致し、EMAでも分布変化は小さい

Prime 2024+ の観測数は `980,697`、code数は `1,693`、date数は `604`。定点 sign との一致率は SMA `68.70%`、EMA `69.04%`。EMA はSMAより少し滑らかだが、Daily Ranking の forward response を大きく変えるわけではない。

| Metric | Value |
| --- | ---: |
| Observation count | 980,697 |
| Code count | 1,693 |
| Date count | 604 |
| Median 20D fixed return | +0.765% |
| Median 60D fixed return | +2.395% |
| Median SMA20 deviation | +0.383% |
| Median SMA60 deviation | +1.031% |
| Median EMA20 deviation | +0.351% |
| Median EMA60 deviation | +0.981% |
| Fixed Overheat share | 1.195% |
| Literal SMA20>=30 share | 0.149% |
| Literal EMA20>=30 share | 0.080% |
| Q-matched SMA20 threshold | +15.144% |
| Q-matched EMA20 threshold | +12.714% |
| SMA sign match rate | 68.70% |
| EMA sign match rate | 69.04% |

#### 結論: `20D>0,60D>0` はEMA/SMA版が少し良いが、置換根拠としては弱い

| Horizon | Variant | Obs | Median excess | Delta vs fixed | Win | Severe |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | fixed `20D>0,60D>0` | 392,399 | -0.778% | - | 45.32% | 9.20% |
| 20D | SMA `>0,>0` | 421,341 | -0.754% | +0.024% | 45.39% | 9.13% |
| 20D | EMA `>0,>0` | 454,033 | -0.791% | -0.013% | 45.16% | 9.10% |
| 60D | fixed `20D>0,60D>0` | 372,359 | -2.342% | - | 42.85% | 25.95% |
| 60D | SMA `>0,>0` | 399,096 | -2.231% | +0.111% | 43.10% | 25.57% |
| 60D | EMA `>0,>0` | 428,375 | -2.225% | +0.117% | 43.11% | 25.53% |

60D では EMA が最も良いが、effect size は小さい。Daily Ranking の `crowded_rerating` / `neutral_rerating` の読みを変えるほどではなく、EMA/SMA sign は補助表示や tie-breaker の候補に留める。

#### 結論: `20D<0,60D>0` はEMA代替がいちばん改善する

| Horizon | Variant | Obs | Median excess | Delta vs fixed | Win | Severe |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | fixed `20D<0,60D>0` | 155,973 | -1.242% | - | 42.62% | 10.22% |
| 20D | SMA `<0,>0` | 115,698 | -1.032% | +0.209% | 43.78% | 9.54% |
| 20D | EMA `<0,>0` | 89,199 | -0.994% | +0.247% | 44.01% | 9.83% |
| 60D | fixed `20D<0,60D>0` | 145,555 | -2.753% | - | 41.40% | 26.81% |
| 60D | SMA `<0,>0` | 110,371 | -2.576% | +0.176% | 42.04% | 26.35% |
| 60D | EMA `<0,>0` | 83,845 | -2.545% | +0.208% | 42.13% | 26.40% |

ここは「定点の直近20Dだけが少し弱い」銘柄を EMA/SMA で平滑化すると、悪い subset を少し落とせている。EMA が最も改善するが、median はまだマイナスなので、long positive ではなく caution bucket の精度改善に近い。

#### 結論: neutral long 候補では `>0,>0` が小幅な priority-up、`<0,>0` は priority-down

`neutral_rerating` の Deep Value / Long Hybrid / Sector Strong long候補に絞ると、SMA/EMA sign は全体より判断に使いやすい。強い long scaffold では `SMA/EMA >0,>0` が少しだけ return/tail を改善する。一方、`SMA/EMA <0,>0` は broad `neutral_deep_value` で明確に劣るため、押し目というより priority-down と読む。

| Scaffold | Variant | Obs | 20D median | 20D win | 20D severe | 60D median | 60D win | 60D severe |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `neutral_deep_value` | all | 23,851 / 23,208 | +0.734% | 55.27% | 3.08% | +1.564% | 55.53% | 14.52% |
| `neutral_deep_value` | SMA `>0,>0` | 20,441 / 19,953 | +0.759% | 55.51% | 2.95% | +1.745% | 56.05% | 14.37% |
| `neutral_deep_value` | EMA `>0,>0` | 21,259 / 20,734 | +0.753% | 55.44% | 2.94% | +1.713% | 56.02% | 14.42% |
| `neutral_deep_value` | SMA `<0,>0` | 2,686 / 2,635 | +0.573% | 54.50% | 3.43% | +0.309% | 51.39% | 16.51% |
| `neutral_deep_value` | EMA `<0,>0` | 1,972 / 1,907 | +0.625% | 54.36% | 4.21% | +0.115% | 50.39% | 16.31% |
| `neutral_deep_value_long_hybrid_atr20_accel` | all | 1,194 / 1,190 | +2.742% | 72.53% | 1.01% | +4.981% | 69.75% | 5.88% |
| `neutral_deep_value_long_hybrid_atr20_accel` | SMA `>0,>0` | 1,093 / 1,089 | +2.803% | 73.83% | 1.01% | +5.338% | 71.72% | 4.87% |
| `neutral_deep_value_long_hybrid_atr20_accel` | EMA `>0,>0` | 1,120 / 1,116 | +2.750% | 73.30% | 0.98% | +5.230% | 70.97% | 5.47% |
| `neutral_deep_value_sector_strong_atr20_accel` | all | 1,021 / 1,002 | +2.041% | 65.92% | 1.67% | +3.413% | 64.37% | 7.09% |
| `neutral_deep_value_sector_strong_atr20_accel` | SMA `>0,>0` | 950 / 931 | +2.018% | 66.00% | 1.58% | +3.541% | 65.52% | 6.77% |
| `neutral_deep_value_sector_strong_atr20_accel` | EMA `>0,>0` | 967 / 948 | +2.032% | 65.98% | 1.55% | +3.530% | 65.30% | 6.75% |

SMA と EMA の差は僅差。`neutral_deep_value_long_hybrid_atr20_accel` では SMA がやや強く、`neutral_long_hybrid_atr20_accel` では EMA がやや強いが、どちらか一方へ production 置換するほどではない。UI では「移動平均 sign が両方 positive なら priority-up、20側だけ negative なら priority-down」という badge が最も自然。

#### 結論: Overheat は bad 指標として読み、EMA20にも機械置換しない

| Variant | Obs | 60D median | 60D mean | Win | Severe | Bad-indicator read |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| fixed `20D>=30` | 9,291 | -1.265% | +5.089% | 47.51% | 33.34% | 現行 risk flag |
| SMA20 q-matched | 9,347 | -1.715% | +4.523% | 46.46% | 32.96% | median は濃縮、severe は弱い |
| EMA20 q-matched | 9,269 | -1.520% | +4.678% | 46.82% | 32.47% | SMA q-matched より穏当 |
| literal `SMA20>=30` | 1,063 | -1.998% | +9.429% | 46.10% | 35.56% | tail は濃いが右尾も大きい |
| literal `EMA20>=30` | 560 | -2.517% | +7.618% | 45.00% | 37.68% | 最も濃いが薄すぎる |

`Overheat` は「悪い risk flag」なので、median がより低いこと自体は risk 濃縮として読める。literal `EMA20>=30%` は最も悪い tail を濃縮するが、観測数が `560` まで落ち、右尾 mean も大きい blowoff tail なので、hard exclude にはしない。q-matched EMA20 は q-matched SMA20 より穏当で、現行 fixed Overheat を置換するほどの追加濃縮ではない。現行の `recent_return_20d_pct >= 30` を維持し、SMA/EMA 乖離は別の「上方乖離の強さ」診断として扱う。

#### 結論: `stale_rally_fade` はEMA版でも悪く、結論は変わらない

| Horizon | Variant | Obs | Median excess | Win | Severe |
| ---: | --- | ---: | ---: | ---: | ---: |
| 20D | fixed | 15,066 | -1.842% | 37.44% | 10.10% |
| 20D | SMA | 16,170 | -1.863% | 37.07% | 9.89% |
| 20D | EMA | 17,207 | -1.839% | 37.28% | 9.76% |
| 60D | fixed | 14,292 | -4.367% | 33.82% | 28.87% |
| 60D | SMA | 15,294 | -4.329% | 33.95% | 28.97% |
| 60D | EMA | 16,258 | -4.253% | 34.39% | 28.85% |

EMA/SMA 版は対象を少し広げるが、実務上の bad/caution 結論は同じ。`stale_liquidity + overvalued/no earnings + positive technical state` は引き続き red/caution 側に置く。

### Interpretation

EMA20/EMA60 と SMA20/SMA60 は定点比較よりノイズを減らすが、Daily Ranking の既存 20D/60D 判定を一括置換するほど forward response を改善しない。最も良い使い方は「定点判定と移動平均判定が食い違うときの補助診断」。全体では `20D<0,60D>0` のEMA版が少し良いが、neutral long候補に絞ると `SMA/EMA >0,>0` が小幅な priority-up、`SMA/EMA <0,>0` が priority-down として使いやすい。

一方、`Overheat` と `stale_rally_fade` は悪い指標として読む。現行 `20D>=30%` は急騰リスクの旗で、SMA/EMA20乖離は「平均線からどれだけ離れたか」を見る旗。literal EMA20>=30 は tail を最も濃縮するが薄く、q-matched EMA20 は現行 Overheat を置換するほど安定しない。`stale_rally_fade` は fixed / SMA / EMA のどれでも悪く、現行 caution 解釈を維持する。

### Production Implication

- Daily Ranking の既存 `Overheat` は維持する。
- `20D/60D` sign bucket を production logic で即置換しない。EMA/SMA sign は表示・badge・review lens として併記する候補に留める。
- `20D<0,60D>0` の mixed bucket だけは、EMA20/EMA60 sign の不一致を補助診断として見る価値がある。
- `neutral_rerating` の Deep Value / Long Hybrid / Sector Strong long候補では、`SMA/EMA >0,>0` を priority-up、`SMA/EMA <0,>0` を priority-down として読む。
- `stale_rally_fade` は fixed / SMA / EMA のどれでも悪く、現行 red/caution 解釈を維持する。
- 次に UI 化するなら、`EMA20 deviation` / `EMA60 deviation` の数値列よりも、定点 sign と EMA sign の disagreement badge の方が判断に使いやすい。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio construction、turnover、cost、capacity は未反映。
- `SMA20` / `SMA60` / `EMA20` / `EMA60` は当日終値を含む trailing feature。pre-open screening 可能性は別途検証が必要。
- 対象は 2024年以降の Prime。Standard/Growth や過去全期間には外挿しない。
- `Overheat` の q-matched threshold はこの run の母集団比率に依存する。固定閾値として採用しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_moving_average_replacement_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_moving_average_replacement_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_moving_average_replacement_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-moving-average-replacement-evidence/20260626_ma_ema_neutral_long_prime_2024/`
- Results tables: `technical_condition_evidence_df`, `long_candidate_moving_average_evidence_df`, `replacement_delta_df`, `price_action_migration_df`, `overheat_overlap_df`, `coverage_diagnostics_df`
