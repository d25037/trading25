# Short-Term Shock Forward Response

## Published Readout

### Decision

短期ショックは **raw return では強い market rebound に見えるが、TOPIX excess では銘柄選択 alpha としてはかなり弱い**。特に `2024-08-05` と `2025-04-07` のような shock day 直後は、Prime 全体の raw 20d return は大きく反発する一方、shock day 時点の `pullback_in_uptrend` は TOPIX excess でむしろ悪い。したがって、短期ショック周辺は「買い signal」ではなく、まず **market beta rebound と個別株 excess を分けて読む diagnostic** として扱う。

一般日次の `20d < 0 AND 60d >= 0` は、素朴な押し目としてはまだ弱い。Prime daily では 20d TOPIX excess median が `-0.811%`、win rate `44.71%` で、`persistent_runup` より明確に良いとは言えない。ただし、20d drawdown が深く、60d uptrend が強いほど mean と median は改善し、`20d <= -10% AND 60d >= +20%` では 20d excess median が `+0.465%` まで上がる。これは押し目仮説の候補だが、severe loss も `17.29%` と重く、単独 rule にはしない。

この readout は bundle `/private/tmp/trading25-research/market-behavior/short-term-shock-forward-response/20260520_short_term_shock_forward_response_prime_full_v2` に基づく。入力 DB は `~/.local/share/trading25/market-timeseries/market.duckdb`、対象 anchor は `2016-04-01` から `2026-05-14`。速度改善後の full run では、valuation は `daily_valuation`、liquidity は `daily_valuation.free_float_market_cap` と `med ADV60` の same-date residual を使う。

後続の ad-hoc check では、`Prime + overheat除外 + low PBR + low forward PER` に絞り、既存 Ranking の `rerating_participation` 相当（`20d > 0 AND 60d > 0 AND liquidity_residual_z >= 1`）と、`neutral` liquidity 内の `20d > 0 AND 60d > 0` を比較した。結論として、core 状態は `neutral_rerating`（`20d > 0 AND 60d > 0 AND -1 < liquidity_residual_z < 1`）として切り出し、既存の `rerating_participation` は意味上 `crowded_rerating` / high-participation satellite として扱うのが自然。`neutral` liquidity の内部を `-1..0` と `0..1` に分ける優先度は低く、切るべき主軸は liquidity の符号ではなく price action の維持。

さらに `high PBR + high forward PER` に反転すると、`neutral_rerating` も `crowded_rerating` も TOPIX excess が弱くなる。したがって、この finding は「price action だけの momentum」ではなく、**low valuation と positive price action が同時にあるときの re-rating continuation** として読む。`crowded_rerating` は右尾が太いが左尾も太く、TOPIX `20d < 0 AND 60d < 0` では右尾優位が消えるため、core ではなく satellite / sizing diagnostic に留める。

### Main Findings

#### 結論: 一般的な `20d < 0 AND 60d >= 0` は弱い押し目

`pullback_in_uptrend` は `20d < 0 AND 60d >= 0`。Prime daily の 20d TOPIX excess では、中央値も win rate も弱い。`persistent_runup` も強くはないため、単純な sign state だけで買い分けるのは粗い。

| price action | observations | 20d mean excess | 20d median excess | win rate | severe loss | median 20d | median 60d |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pullback_in_uptrend` | `269,247` | `-0.212%` | `-0.811%` | `44.71%` | `8.19%` | `-3.01%` | `+6.16%` |
| `downtrend_decline` | `424,344` | `-0.416%` | `-0.918%` | `43.54%` | `7.58%` | `-4.94%` | `-7.76%` |
| `persistent_runup` | `661,242` | `-0.003%` | `-0.574%` | `46.31%` | `7.99%` | `+5.63%` | `+10.94%` |
| `relief_bounce` | `214,538` | `-0.206%` | `-0.645%` | `45.40%` | `7.71%` | `+2.85%` | `-4.71%` |

#### 結論: 深い20d押し目 x 強い60d上昇だけは候補になる

`20d < 0 AND 60d >= 0` 全体では弱いが、20d drawdown と 60d uptrend を同時に強めると分布は改善する。特に `20d <= -10% AND 60d >= +20%` 以降は median がプラス化する。ただし severe loss は高く、rebound candidate であって quality filter ではない。

| condition | observations | 20d mean excess | 20d median excess | win rate | severe loss | median 20d | median 60d |
|---|---:|---:|---:|---:|---:|---:|---:|
| `20d < 0 AND 60d >= 0` | `269,247` | `-0.212%` | `-0.811%` | `44.71%` | `8.19%` | `-3.01%` | `+6.16%` |
| `20d <= -5% AND 60d >= +10%` | `27,147` | `+0.528%` | `-0.781%` | `46.33%` | `13.67%` | `-7.79%` | `+16.67%` |
| `20d <= -10% AND 60d >= +20%` | `3,371` | `+3.018%` | `+0.465%` | `51.50%` | `17.29%` | `-13.23%` | `+30.14%` |
| `20d <= -15% AND 60d >= +20%` | `1,196` | `+4.800%` | `+1.348%` | `54.18%` | `18.23%` | `-18.02%` | `+33.14%` |
| `20d <= -20% AND 60d >= 0` | `1,352` | `+3.097%` | `+0.914%` | `53.70%` | `17.53%` | `-22.52%` | `+10.23%` |

#### 結論: liquidity 高残差は右尾も出すが、左尾も重い

`pullback_in_uptrend` に liquidity residual を重ねると、`distribution_stress` は mean が改善する一方で median は悪く、severe loss がかなり重い。`stale_liquidity` は severe loss が軽くなるが、median / win rate は弱い。したがって liquidity は買い判定ではなく、**右尾と左尾が同時に太る crowding/participation diagnostic** として読む。

| condition | liquidity | observations | 20d mean excess | 20d median excess | win rate | severe loss | median liq z |
|---|---|---:|---:|---:|---:|---:|---:|
| `20d < 0 AND 60d >= 0` | `all_liquidity` | `269,247` | `-0.212%` | `-0.811%` | `44.71%` | `8.19%` | `-0.10` |
| `20d < 0 AND 60d >= 0` | `distribution_stress` | `36,453` | `+0.395%` | `-0.991%` | `45.61%` | `16.51%` | `+1.53` |
| `20d < 0 AND 60d >= 0` | `stale_liquidity` | `34,096` | `-0.437%` | `-0.698%` | `44.55%` | `4.55%` | `-1.31` |
| `20d <= -10% AND 60d >= +20%` | `distribution_stress` | `2,222` | `+3.877%` | `+0.806%` | `52.21%` | `19.71%` | `+2.35` |

#### 結論: low valuation 前提では `neutral_rerating` が core、既存 Rerating は crowded satellite

`Prime + overheat除外 + low PBR + low forward PER + 20d > 0 + 60d > 0` に絞ると、`liquidity_residual_z >= 1` は平均 return と右尾が大きい一方、標準偏差と severe loss が大きい。`-1 < liquidity_residual_z < 1` は平均の派手さは落ちるが、median / win rate / 左尾が安定する。したがって、既存 Ranking の `rerating_participation` は production label としては `crowded_rerating` または `high_participation_rerating` と読み替え、core 状態は `neutral_rerating` として追加定義するのがよい。

| condition | observations | 20d mean excess | 20d median excess | sd | p10 | win rate | severe loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| `20d > 0 AND 60d > 0 AND -1 < liq_z < 1` | `148,020` | `+1.81%` | `+1.17%` | `7.01` | `-6.14%` | `58.47%` | `2.77%` |
| `20d > 0 AND 60d > 0 AND 0 < liq_z < 1` | `52,500` | `+1.73%` | `+1.18%` | `7.49` | `-6.91%` | `57.93%` | `3.58%` |
| `20d > 0 AND 60d > 0 AND -1 < liq_z < 0` | `95,520` | `+1.86%` | `+1.16%` | `6.74` | `-5.72%` | `58.77%` | `2.33%` |
| `20d > 0 AND 60d > 0 AND liq_z >= 1` | `14,152` | `+3.05%` | `+1.06%` | `13.22` | `-8.91%` | `54.95%` | `8.08%` |

#### 結論: `neutral_rerating` は liquidity 符号で細分化しなくてよい

`neutral` liquidity を `-1 < liq_z < 0` と `0 < liq_z < 1` に分けても、20d median excess はほぼ同じ。むしろ `-1 < liq_z < 0` の方が severe loss は軽い。したがって、`neutral_rerating` は `-1 < liq_z < 1` 全体を stable な core bucket として扱う方がよい。

| condition | observations | 20d mean excess | 20d median excess | sd | p10 | win rate | severe loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| `20d > 0 AND 60d > 0 AND -1 < liq_z < 0` | `95,520` | `+1.86%` | `+1.16%` | `6.74` | `-5.72%` | `58.77%` | `2.33%` |
| `20d > 0 AND 60d > 0 AND 0 < liq_z < 1` | `52,500` | `+1.73%` | `+1.18%` | `7.49` | `-6.91%` | `57.93%` | `3.58%` |

#### 結論: low valuation の `neutral_rerating` は TOPIX が大崩れしていない時だけ core

`Prime + overheat除外 + low PBR + low forward PER` に固定して、TOPIX regime ごとに `neutral_rerating` / `crowded_rerating` / `stale` を比較した。TOPIX `20d < 0 AND 60d > 0` では `neutral_rerating` が median / win rate / severe loss のバランスで最も良い。TOPIX `20d < 0 AND 60d < 0` では全体に期待値が落ち、`crowded_rerating` は severe loss が極端に悪化する。TOPIX `20d > 0 AND 60d > 0` でも `neutral_rerating` が core、`crowded_rerating` は右尾 satellite、`stale` は低優先度。

| TOPIX regime | state | observations | 20d mean excess | 20d median excess | win rate | severe loss | 5d median excess |
|---|---|---:|---:|---:|---:|---:|---:|
| `20d < 0 AND 60d > 0` | `neutral_rerating` | `18,920` | `+2.60%` | `+1.74%` | `63.87%` | `2.43%` | `+0.46%` |
| `20d < 0 AND 60d > 0` | `crowded_rerating` | `1,884` | `+3.07%` | `+1.38%` | `53.93%` | `7.64%` | `+0.37%` |
| `20d < 0 AND 60d > 0` | `stale` | `10,044` | `+0.39%` | `+0.06%` | `50.74%` | `3.27%` | `+0.34%` |
| `20d < 0 AND 60d < 0` | `neutral_rerating` | `9,088` | `-0.71%` | `-1.62%` | `38.86%` | `5.63%` | `-0.48%` |
| `20d < 0 AND 60d < 0` | `crowded_rerating` | `700` | `-2.76%` | `-3.53%` | `37.71%` | `23.43%` | `-1.60%` |
| `20d < 0 AND 60d < 0` | `stale` | `9,988` | `-0.12%` | `-0.88%` | `43.25%` | `2.00%` | `-0.29%` |
| `20d > 0 AND 60d > 0` | `neutral_rerating` | `111,128` | `+1.90%` | `+1.33%` | `59.49%` | `2.53%` | `+0.32%` |
| `20d > 0 AND 60d > 0` | `crowded_rerating` | `10,956` | `+3.43%` | `+1.07%` | `55.60%` | `6.97%` | `+0.27%` |
| `20d > 0 AND 60d > 0` | `stale` | `29,572` | `+0.85%` | `+0.50%` | `54.61%` | `2.26%` | `+0.01%` |

#### 結論: `crowded_rerating` は右尾が太いが、左尾も太い

`crowded_rerating` は平均 return を押し上げる右尾を持つ。全期間では 20d TOPIX excess が `+20%` 以上になる比率が `neutral_rerating` の約4倍ある。一方で `-10%` 以下の左尾も約3倍。TOPIX が `20d < 0 AND 60d < 0` になると、右尾の差はほぼ消え、左尾だけが極端に悪くなる。したがって `crowded_rerating` は「強い状態」ではなく、**上振れも下振れもある crowded/high-participation 状態** として sizing で扱う。

| TOPIX regime | state | p90 | p95 | p99 | `>= +10%` | `>= +20%` | `<= -10%` |
|---|---|---:|---:|---:|---:|---:|---:|
| `all` | `neutral_rerating` | `+10.52%` | `+14.25%` | `+22.50%` | `11.03%` | `1.68%` | `2.77%` |
| `all` | `crowded_rerating` | `+16.76%` | `+22.19%` | `+40.42%` | `20.24%` | `6.56%` | `8.08%` |
| `20d > 0 AND 60d > 0` | `neutral_rerating` | `+10.53%` | `+14.20%` | `+22.12%` | `11.12%` | `1.60%` | `2.53%` |
| `20d > 0 AND 60d > 0` | `crowded_rerating` | `+17.05%` | `+22.92%` | `+41.88%` | `20.37%` | `6.83%` | `6.97%` |
| `20d < 0 AND 60d > 0` | `neutral_rerating` | `+11.55%` | `+15.10%` | `+23.85%` | `12.81%` | `1.99%` | `2.43%` |
| `20d < 0 AND 60d > 0` | `crowded_rerating` | `+17.60%` | `+21.84%` | `+32.49%` | `22.29%` | `7.86%` | `7.64%` |
| `20d < 0 AND 60d < 0` | `neutral_rerating` | `+8.53%` | `+12.69%` | `+19.86%` | `7.66%` | `0.88%` | `5.63%` |
| `20d < 0 AND 60d < 0` | `crowded_rerating` | `+8.86%` | `+13.57%` | `+20.56%` | `9.14%` | `1.14%` | `23.43%` |

#### 結論: Overvalued では rerating 状態の edge は消える

`Prime + overheat除外 + high PBR + high forward PER` に反転すると、同じ `neutral_rerating` / `crowded_rerating` / `stale` 分類でも 20d TOPIX excess は弱い。TOPIX `20d > 0 AND 60d > 0` の上昇局面ですら、`neutral_rerating` の 20d median excess は `-2.30%`。これは今回の signal が単なる price momentum ではなく、**low valuation guard と price action の同時成立**で初めて成立することを示す。

| TOPIX regime | state | observations | 20d mean excess | 20d median excess | win rate | severe loss | 5d median excess |
|---|---|---:|---:|---:|---:|---:|---:|
| `20d < 0 AND 60d > 0` | `neutral_rerating` | `19,928` | `-1.35%` | `-1.65%` | `41.07%` | `13.15%` | `-0.25%` |
| `20d < 0 AND 60d > 0` | `crowded_rerating` | `6,160` | `-0.81%` | `-2.40%` | `42.99%` | `26.23%` | `-1.09%` |
| `20d < 0 AND 60d > 0` | `stale` | `5,012` | `-1.37%` | `-1.69%` | `38.55%` | `8.78%` | `-0.12%` |
| `20d < 0 AND 60d < 0` | `neutral_rerating` | `16,984` | `-1.70%` | `-2.14%` | `39.59%` | `15.14%` | `-0.62%` |
| `20d < 0 AND 60d < 0` | `crowded_rerating` | `4,496` | `+0.07%` | `-0.47%` | `48.67%` | `18.59%` | `-0.16%` |
| `20d < 0 AND 60d < 0` | `stale` | `4,824` | `-1.40%` | `-1.94%` | `38.23%` | `6.30%` | `-0.72%` |
| `20d > 0 AND 60d > 0` | `neutral_rerating` | `112,064` | `-1.85%` | `-2.30%` | `37.71%` | `14.34%` | `-0.58%` |
| `20d > 0 AND 60d > 0` | `crowded_rerating` | `35,880` | `-0.58%` | `-2.22%` | `42.89%` | `24.98%` | `-0.44%` |
| `20d > 0 AND 60d > 0` | `stale` | `18,016` | `-1.84%` | `-1.60%` | `37.99%` | `9.88%` | `-0.45%` |

#### 結論: 2024/7-8 と 2025/3-4 でも crowded より neutral rerating が優位

2024/7-8 と 2025/3-4 のショック期に絞っても、`20d < 0 AND 60d > 0 AND liq_z > 1` は悪く、`20d > 0 AND 60d > 0 AND 0 < liq_z < 1` の方が明確にマシ。特に 2024/7-8 全体では、前者の 20d median excess は `-8.19%`、severe loss は `32.1%` まで悪化する。ショック期の `liq_z > 1` は rerating より distribution/crowding の混入を強く疑う。

| period / state | `20d<0,60d>0,liq_z>1` median | severe | `20d>0,60d>0,0<liq_z<1` median | severe |
|---|---:|---:|---:|---:|
| `2024-07..08 all` | `-8.19%` | `32.1%` | `+0.82%` | `5.4%` |
| `2024-07..08 TOPIX 20d<0` | `-8.74%` | `25.0%` | `-4.50%` | `11.5%` |
| `2025-03..04 all` | `-6.27%` | `21.1%` | `-3.18%` | `4.7%` |
| `2025-03..04 TOPIX 20d<0` | `-6.18%` | `23.9%` | `-2.83%` | `6.3%` |
| `combined TOPIX 20d<0,60d>=0` | `-8.10%` | `22.2%` | `+0.25%` | `0.0%` |

#### 結論: valuation を入れると、低PBR/低forward PER は shallow pullback の質を改善する

`pullback_in_uptrend` 全体では、`both_low` が 20d median `+0.057%`、severe loss `4.28%` で最も素直に改善する。`neither_low` は median `-1.059%`、severe loss `9.33%` で避けたい。深い押し目では低PBR単独が最も良いが、sample と tail risk を見ると、valuation は「押し目を買う理由」ではなく「落ちるナイフを多少選別する quality guard」として使うのが自然。

| condition | valuation bucket | observations | 20d mean excess | 20d median excess | win rate | severe loss | median PBR | median fPER |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `pullback_in_uptrend` | `both_low` | `18,347` | `+0.626%` | `+0.057%` | `50.39%` | `4.28%` | `0.51` | `7.73` |
| `pullback_in_uptrend` | `low_pbr_only` | `26,732` | `+0.355%` | `-0.436%` | `46.63%` | `4.53%` | `0.56` | `13.55` |
| `pullback_in_uptrend` | `low_forward_per_only` | `28,519` | `-0.094%` | `-0.595%` | `45.93%` | `6.98%` | `0.96` | `7.99` |
| `pullback_in_uptrend` | `neither_low` | `164,872` | `-0.521%` | `-1.059%` | `43.35%` | `9.33%` | `1.47` | `16.28` |
| `deep_pullback_in_uptrend` | `low_pbr_only` | `1,424` | `+2.361%` | `-0.011%` | `49.86%` | `5.76%` | `0.58` | `13.79` |

#### 結論: shock day raw rebound は強いが、excess では市場反発の比重が大きい

TOPIX 1d `<= -5%` / `<= -8%` の shock window では、raw return は強く見える。だが TOPIX excess にすると、`-5%` shock family は多くの offset で median がマイナスのまま。`-8%` は 2024-08-05 単発に近く、post `2-10d` は excess もプラスだが sample は狭い。

| shock | offset | observations | raw 20d median | excess 20d median | raw win | excess win | excess severe |
|---|---|---:|---:|---:|---:|---:|---:|
| `TOPIX <= -8%` | `shock_day` | `1,646` | `+19.056%` | `-3.669%` | `98.48%` | `35.72%` | `24.54%` |
| `TOPIX <= -8%` | `post_shock_2_5d` | `6,581` | `+4.539%` | `+0.590%` | `77.42%` | `54.08%` | `4.19%` |
| `TOPIX <= -8%` | `post_shock_6_10d` | `8,220` | `-0.818%` | `+1.825%` | `43.88%` | `62.62%` | `4.21%` |
| `TOPIX <= -5%` | `shock_day` | `4,929` | `+13.426%` | `-2.772%` | `91.80%` | `37.35%` | `16.11%` |
| `TOPIX <= -5%` | `post_shock_2_5d` | `13,118` | `+6.361%` | `-0.879%` | `81.82%` | `45.31%` | `11.44%` |
| `TOPIX <= -5%` | `post_shock_11_20d` | `32,745` | `+0.676%` | `-0.728%` | `54.30%` | `45.55%` | `9.64%` |

#### 結論: 2024-08-05 と 2025-04-07 は raw rebound だが excess は別物

指定日の case study では raw return は非常に強い。ただし shock day の `pullback_in_uptrend` は TOPIX excess で悪く、2025-04-07 は post `2-5d` でも excess がかなり弱い。2024-08-05 は post `2-5d` で excess もプラス化したが、これは market crash 後の特殊 rebound 色が強く、一般 rule にするには narrow。

| date | offset | state | observations | raw 20d median | excess 20d median | excess win | excess severe |
|---|---|---|---:|---:|---:|---:|---:|
| `2024-08-05` | `shock_day` | `downtrend_decline` | `1,489` | `+19.377%` | `-3.348%` | `37.21%` | `22.57%` |
| `2024-08-05` | `shock_day` | `pullback_in_uptrend` | `98` | `+15.763%` | `-6.962%` | `23.47%` | `38.78%` |
| `2024-08-05` | `post_2_5d` | `pullback_in_uptrend` | `1,091` | `+4.816%` | `+1.151%` | `57.10%` | `4.31%` |
| `2025-04-07` | `shock_day` | `downtrend_decline` | `1,419` | `+16.091%` | `-1.826%` | `41.79%` | `12.33%` |
| `2025-04-07` | `shock_day` | `pullback_in_uptrend` | `152` | `+13.599%` | `-4.318%` | `33.55%` | `23.68%` |
| `2025-04-07` | `post_2_5d` | `pullback_in_uptrend` | `1,202` | `+8.051%` | `-3.573%` | `33.69%` | `20.97%` |

### Interpretation

`20d < 0 AND 60d >= 0` は直感的には「中期上昇中の短期押し目」だが、全体ではまだ弱い。20d drawdown が浅いものは単に弱い銘柄を多く含み、60d uptrend が弱いものも rebound edge が薄い。

一方で、`20d <= -10%` 以上の短期下落と `60d >= +20%` 以上の中期上昇が同時にある場合、mean / median / win rate は改善する。これは「中期 trend が残った深い短期 pullback」の候補。ただし severe loss が高いため、falling-knife risk を別に抑える必要がある。valuation を足すなら、まず `both_low` / `low_pbr_only` で `neither_low` を避ける方向が自然。

market shock 周辺は、raw return と TOPIX excess の差が大きい。2024-08-05 / 2025-04-07 のような日は、個別株を買っても raw ではほぼ市場反発に乗れるが、銘柄選択として TOPIX を上回るとは限らない。特に shock day そのものの `pullback_in_uptrend` は、TOPIX excess では悪い。liquidity 高残差は一部の shock case で右尾を作るが、pooled では severe loss も高いので、position sizing なしの加点にはしない。

個別株の core setup は、`low PBR + low forward PER` と `stock 20d > 0 AND stock 60d > 0` が同時に必要。`neutral_rerating` は「割安で、価格に再評価が出始め、売買代金が過熱も枯渇もしていない」状態で、左尾が軽い。`crowded_rerating` は右尾が太いが左尾も太く、TOPIX downtrend では右尾の優位が消える。高PBR・高forward PERでは同じ price action でも excess が弱いため、valuation guard なしの momentum 追随にはしない。

### Production Implication

| 用途 | 推奨 |
|---|---|
| Daily Ranking の短期状態 | `20d < 0 AND 60d >= 0` を単独の買い状態にしない |
| Pullback candidate | `20d <= -10% AND 60d >= +20%` 以上を候補として別検証する |
| Valuation guard | `both_low` / `low_pbr_only` は候補維持、`neither_low` は caution |
| Liquidity residual | `distribution_stress` は右尾候補だが severe loss が重く、risk cap 前提 |
| Rerating state naming | `neutral_rerating = 20d > 0 AND 60d > 0 AND -1 < liq_z < 1` を core 候補、`crowded_rerating = 20d > 0 AND 60d > 0 AND liq_z >= 1` を satellite / sizing diagnostic とする |
| TOPIX regime | `20d < 0 AND 60d > 0` までは `neutral_rerating` を残せるが、`20d < 0 AND 60d < 0` では long exposure を落とす |
| High valuation | `high PBR + high forward PER` では rerating 状態の edge は消えるため、valuation guard を必須にする |
| Shock watch | TOPIX `1d <= -5%` 後は raw rebound と excess を必ず分離する |
| 2024/2025型 rebound | market beta rebound として扱い、selection alpha と混同しない |
| 次の研究 | Standard/Growth 分割、weekly/monthly non-overlap、falling-knife guard を追加する |

### Caveats

- 今回の published run は Prime full。valuation は `daily_valuation` を使い、古い `statements` interval join は速度最適化のため使っていない。
- liquidity residual は `daily_valuation.free_float_market_cap` がある場合はそれを使い、無い場合のみ `market_cap` fallback になる。
- `--markets all` の一括 run は重く、今回は採用していない。Standard/Growth は市場別に分割して rerun するのがよい。
- dense daily panel なので overlap を含む。今回の full run は `--sample-scopes daily` のみで、weekly/monthly non-overlap は未 published。
- case study は 2024-08-05 / 2025-04-07 の局所観察であり、rule 採用の根拠ではない。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/short_term_shock_forward_response.py`
- runner: `apps/bt/scripts/research/run_short_term_shock_forward_response.py`
- bundle experiment id: `market-behavior/short-term-shock-forward-response`
- latest result bundle: `/private/tmp/trading25-research/market-behavior/short-term-shock-forward-response/20260520_short_term_shock_forward_response_prime_full_v2`
- result tables: `market_shock_calendar_df`, `general_short_term_response_df`, `pullback_in_uptrend_response_df`, `market_shock_window_response_df`, `stock_market_interaction_df`, `liquidity_valuation_interaction_df`, `case_study_response_df`, `observation_sample_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_short_term_shock_forward_response.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,20 \
  --pullback-thresholds-20d 0,5,10,15,20,30 \
  --uptrend-thresholds-60d 0,10,20,30 \
  --market-shock-thresholds=-3,-5,-8 \
  --sample-scopes daily \
  --case-study-dates 2024-08-05,2025-04-07 \
  --case-study-window-sessions 5 \
  --min-observations 500 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260520_short_term_shock_forward_response_prime_full_v2
```

## Artifact Tables

- `market_shock_calendar_df`: TOPIX 1d shock date calendar by threshold.
- `general_short_term_response_df`: `20d/60d` sign state response.
- `pullback_in_uptrend_response_df`: `20d` drawdown x `60d` uptrend threshold response.
- `market_shock_window_response_df`: TOPIX shock day and post-shock offset response.
- `stock_market_interaction_df`: non-core stock state x market state response.
- `liquidity_valuation_interaction_df`: non-core pullback x valuation interaction.
- `case_study_response_df`: specified shock-date window response.
- `observation_sample_df`: bounded sample of the observation panel.
