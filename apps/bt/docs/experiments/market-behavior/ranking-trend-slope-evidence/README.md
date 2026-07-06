# Ranking Trend Slope Evidence

Daily Ranking Research Base を使い、既存の `recent_return_20d_pct` / `recent_return_60d_pct` の定点比較を、close の rolling OLS slope、OLS `R²`、`SMA20/SMA60` slope、`EMA20/EMA60` slope と比較する。目的は fixed `20D/60D` の即置換ではなく、endpoint return では見えない「上昇の直線性」「短期加速」「60D hold の安定性」を long scaffold の priority / review overlay として使えるかを検証すること。

## Published Readout

### Decision

Run: `20260706_trend_slope_prime_2024_v2`

対象は Prime、`analysis_start_date=2024-01-01`、forward outcome は 5D / 20D / 60D close-to-close TOPIX excess return。OLS slope は各銘柄の adjusted close を log 化し、20 / 60 営業日の rolling OLS で当日までの傾きを算出した。`price_lr_slope_N_pct` は `exp(slope_per_session * (N-1)) - 1` を percent 表示した値で、`price_lr_r2_N` は同 window 内の直線性を示す。SMA/EMA slope は `MA(today) / MA(lag) - 1` で、短期側は `SMA20/EMA20` の 5D slope、長期側は `SMA60/EMA60` の 20D slope を主比較にした。

結論:

- OLS slope は fixed `20D>0,60D>0` の wholesale replacement にはしない。全体 60D median は fixed `-2.375%`、OLS slope `>0,>0` が `-2.400%` で改善しない。
- `R²` high を足すと直線的な上昇だけに絞れるが、全体 60D median は `-2.464%` と悪化する。20D では少し改善するため、長期 hold filter ではなく短期 momentum quality / left-tail review に近い。
- conflict bucket は 20D では直感どおりで、`20D<=0 だが lr20>0` は `20D<=0 かつ lr20<=0` より良い。逆に `20D>0 だが lr20<=0` は `20D>0 かつ lr20>0` より弱い。endpoint return と slope の食い違いは短期 recovery / fade 診断として使える。
- 60D conflict は単純ではない。`60D>0 だが lr60<=0` の方が `60D>0 かつ lr60>0` より60D forward median が良く、滑らかな過去上昇は長期 forward では追随しすぎになる可能性がある。
- `neutral_deep_value_long_hybrid_atr20_accel` では `lr20_accel_over_lr60` が20D median `+4.141%` と短期で強い一方、60D median は `+2.909%` まで落ちる。逆に `lr20_decel_below_lr60` は20D `+2.445%` だが60D `+6.178%`。短期加速は entry / 5-20D priority、60D hold ではむしろ減速・長期傾き優位の方が良い。
- SMA/EMA slope は OLS slope より実務 badge にしやすい。全体では EMA slope `>0,>0` が60D median `-2.311%` と fixed / OLS より少し良いが、effect size は小さい。strong long scaffold では SMA/EMA slope `>0,>0` は既存 all / fixed と同程度で、hard filter にはしない。

### Main Findings

#### 結論: 全体では slope は fixed `20D/60D` を置換しない

Prime 2024+ の観測数は `990,020`、code数は `1,698`、date数は `610`。fixed sign と OLS sign の完全一致率は `74.32%`。OLS slope は endpoint return と十分違うが、全体の forward response を大きく改善するほどではない。

| Metric | Value |
| --- | ---: |
| Observation count | 990,020 |
| Code count | 1,698 |
| Date count | 610 |
| Median 20D fixed return | +0.778% |
| Median 60D fixed return | +2.381% |
| Median OLS slope20 | +0.789% |
| Median OLS slope60 | +2.245% |
| Median OLS R2 20 | 0.423 |
| Median OLS R2 60 | 0.423 |
| Fixed vs OLS sign exact match | 74.32% |
| Fixed20 positive / OLS20 negative | 6.87% |
| Fixed60 positive / OLS60 negative | 6.90% |

#### 結論: dual-positive の置換効果は小さく、R2 high は60Dでは逆効果

| Horizon | Variant | Obs | Median excess | Win | Severe | Median slope20 | Median slope60 | Median R2 20 / 60 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | fixed `20D>0,60D>0` | 395,131 | -0.779% | 45.32% | 9.25% | +5.765% | +10.725% | 0.517 / 0.527 |
| 20D | OLS `slope20>0,slope60>0` | 363,360 | -0.771% | 45.39% | 9.13% | +5.884% | +11.644% | 0.521 / 0.560 |
| 20D | OLS positive + R2 high | 110,685 | -0.646% | 46.23% | 8.68% | +9.815% | +20.103% | 0.744 / 0.775 |
| 20D | EMA slope positive | 397,303 | -0.766% | 45.36% | 9.07% | +5.547% | +11.297% | 0.504 / 0.544 |
| 60D | fixed `20D>0,60D>0` | 374,091 | -2.375% | 42.75% | 26.10% | +5.683% | +10.816% | 0.518 / 0.541 |
| 60D | OLS `slope20>0,slope60>0` | 344,894 | -2.400% | 42.67% | 26.08% | +5.793% | +11.675% | 0.522 / 0.572 |
| 60D | OLS positive + R2 high | 107,186 | -2.464% | 42.83% | 26.54% | +9.679% | +19.857% | 0.744 / 0.778 |
| 60D | EMA slope positive | 375,364 | -2.311% | 42.90% | 25.88% | +5.470% | +11.355% | 0.505 / 0.557 |

R2 high は20Dの median / win / severe を少し改善するが、60D median と severe は悪化する。直線的な過去上昇は「きれいな上昇」ではあるが、60D forward では追随しすぎになりやすい。

#### 結論: 20D conflict は recovery / fade 診断として素直に使える

| Horizon | Conflict | Obs | Median excess | P10 | Win | Severe | Recent | Slope | R2 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | `fixed20<=0 & lr20<=0` | 363,509 | -1.288% | -9.858% | 41.78% | 9.68% | -4.956% | -5.171% | 0.456 |
| 20D | `fixed20<=0 & lr20>0` | 67,621 | -0.907% | -9.511% | 44.34% | 8.98% | -1.442% | +1.578% | 0.073 |
| 20D | `fixed20>0 & lr20<=0` | 65,034 | -0.999% | -9.852% | 43.70% | 9.70% | +1.502% | -1.425% | 0.065 |
| 20D | `fixed20>0 & lr20>0` | 459,976 | -0.754% | -9.521% | 45.31% | 9.01% | +5.837% | +5.924% | 0.532 |

`20D<=0` でも OLS20 slope が上向きなら、完全な弱トレンドよりは短期 forward が改善する。`20D>0` でも OLS20 slope が下向きなら、反発後の失速候補として priority-down にできる。

#### 結論: 60D conflict は「滑らかに上がったほど良い」ではない

| Horizon | Conflict | Obs | Median excess | P10 | Win | Severe | Recent | Slope | R2 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 60D | `fixed60<=0 & lr60<=0` | 304,771 | -2.200% | -17.377% | 42.48% | 24.65% | -8.182% | -8.861% | 0.415 |
| 60D | `fixed60<=0 & lr60>0` | 58,362 | -2.529% | -17.137% | 41.70% | 25.05% | -2.617% | +2.899% | 0.068 |
| 60D | `fixed60>0 & lr60<=0` | 60,253 | -2.020% | -17.066% | 43.38% | 24.44% | +2.761% | -2.625% | 0.059 |
| 60D | `fixed60>0 & lr60>0` | 465,125 | -2.562% | -17.903% | 42.21% | 26.65% | +11.512% | +11.736% | 0.549 |

60D のきれいな上昇は forward 60D では良くない。60D endpoint は positive だが OLS60 が negative の薄い回復・乱高下 bucket の方が、median / p10 / severe はむしろ良い。60D slope は長期 hold の追随判定として単純化しない。

#### 結論: strong long scaffold では「短期加速」は20D向き、60D hold は減速側が良い

`neutral_deep_value_long_hybrid_atr20_accel` では、すでに fixed `20D>0,60D>0` がほぼ全件に近い。ここで slope を hard filter にすると、20D と60D の最適状態が分かれる。

| Horizon | Variant | Obs | Median excess | Win | Severe | Median slope20 | Median slope60 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20D | all | 1,209 | +2.747% | 72.46% | 1.16% | +8.547% | +12.438% |
| 20D | OLS `>0,>0` | 1,081 | +2.747% | 73.91% | 1.20% | +9.336% | +13.233% |
| 20D | OLS positive + R2 high | 552 | +2.408% | 74.82% | 0.91% | +11.005% | +18.793% |
| 20D | `slope20 > slope60` | 340 | +4.141% | 73.53% | 2.94% | +11.417% | +6.275% |
| 20D | `slope20 <= slope60` | 869 | +2.445% | 72.04% | 0.46% | +7.159% | +16.081% |
| 60D | all | 1,190 | +4.981% | 69.75% | 5.88% | +8.411% | +12.322% |
| 60D | OLS `>0,>0` | 1,063 | +4.915% | 70.37% | 4.99% | +9.253% | +13.195% |
| 60D | OLS positive + R2 high | 545 | +3.913% | 68.26% | 3.12% | +10.950% | +18.761% |
| 60D | `slope20 > slope60` | 328 | +2.909% | 67.07% | 5.79% | +11.177% | +6.137% |
| 60D | `slope20 <= slope60` | 862 | +6.178% | 70.77% | 5.92% | +7.141% | +15.992% |

短期加速は20D median を押し上げるが、60D hold では逆に弱い。`R²` high は tail を抑えるが median を落とすため、entry priority より risk-control / sizing overlay として読む。

### Interpretation

OLS slope は endpoint return の欠点を補うが、既存 `20D/60D` を置換するほど強い単独条件ではない。特に全体では fixed dual-positive と OLS dual-positive の差が小さく、R2 high は「きれいに上がっている」銘柄を絞るほど60Dの追随リスクを強める。

一方、20D の conflict は実務的に使いやすい。`20D<=0 & lr20>0` は初期回復、`20D>0 & lr20<=0` は反発後の失速として読める。これは Ranking の候補順・watchlist review・entry timing に向く。

strong long scaffold では `slope20 > slope60` を「短期加速」として20D priority-up に使える。ただし60D hold では `slope20 <= slope60` がより良く、短期加速を60D hold の hard positive にしてはいけない。SMA/EMA slope は OLS より表示しやすいが、今回の evidence では hard filter ではなく、OLS conflict / acceleration を説明する補助 badge に留める。

### Production Implication

- Daily Ranking の fixed `20D/60D` sign bucket は維持する。
- `price_lr_slope_20_pct` / `fixed20_lr20_conflict_bucket` は entry timing / recovery-fade 診断として有望。
- `price_lr_slope_60_pct` は単純な long continuation 判定にしない。60D slope positive / high R2 は追随リスクを含む。
- `neutral_deep_value_long_hybrid_atr20_accel` では `slope20 > slope60` を 5D/20D priority-up、`slope20 <= slope60` を 60D hold-friendly と分けて読む。
- `R²` high は「質が高いから買い」ではなく、left-tail / sizing / overextension review の補助診断として扱う。
- UI 化する場合は、数値列を大量追加する前に `20D endpoint vs OLS20 slope conflict` と `slope20>slope60 acceleration` の badge を Research/Ranking の review lens として検討する。

### Caveats

- outcome は 5D/20D/60D close-to-close TOPIX excess return。portfolio construction、turnover、cost、capacity は未反映。
- OLS slope / SMA slope / EMA slope は当日 close を含む trailing feature。pre-open screening 可否は別途検証が必要。
- 対象は 2024年以降の Prime。Standard/Growth や古い regime には外挿しない。
- R2 threshold は暫定 `0.5`。この run の conclusion は threshold optimization ではなく、方向性の evidence として読む。
- `slope20 > slope60` は短期加速を表すが、薄い subset では severe loss が増える場合がある。hard filter ではなく priority / review overlay に留める。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_trend_slope_evidence.py`
- Module: `apps/bt/src/domains/analytics/ranking_trend_slope_evidence.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-trend-slope-evidence/20260706_trend_slope_prime_2024_v2/`
- Results tables: `coverage_diagnostics_df`, `technical_condition_evidence_df`, `fixed_vs_slope_conflict_df`, `long_candidate_trend_slope_evidence_df`, `observation_sample_df`
