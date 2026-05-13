# Free-Float Liquidity-Implied Price Convergence

## Published Readout

### Decision

`free_float_liquidity_implied_price_convergence` は、Symbol Workbench に載せた `流動性等価株価 Med ADV60` を「短期目標株価」と誤読しないための、Prime 限定の長期収束 research として追加する。

結論は明確。`liquidity_implied_price` が現在値の 3倍や 1/2 に見える銘柄は散見されるが、実現株価がその水準へ短期・中期に素直に寄っていくわけではない。特に正方向の大きな gap は、500営業日でも median fixed-target closure は小さく、gap 縮小の主因は **株価が示唆株価へ追いつくことではなく、ADVが剥落して implied price 側が下がること** である。

この run は bundle `/tmp/trading25-research/market-behavior/free-float-liquidity-implied-price-convergence/phase4_20260511_prime_implied_price_convergence_v2` に基づく。対象は Prime、`ADV60`、`2016-08-25` to `2026-05-08`、forward horizon は `20/60/120/250/500 sessions`、observation stride は `20 sessions`。

### Main Findings

#### 結論

| 観点 | 結果 | 判断 |
|---|---:|---|
| Daily Prime ADV60 regression R2 latest | 0.834 | Workbench の latest cross-section fit は十分強い |
| Positive gap `>200%` 60d median closure | 0.003 | 60営業日ではほぼ埋まらない |
| Positive gap `>200%` 500d median closure | 0.020 | 500営業日でも固定示唆株価にはほぼ到達しない |
| Positive gap `>200%` 500d median implied price change | -44.4% | gap縮小は implied price 側の剥落が主因 |
| Positive gap `>200%` 500d median rolling gap reduction | +60.4% | rolling gap は縮むが株価到達ではない |
| Negative gap `50-100%` 500d direction hit | 48.7% | 低流動性側も価格が下へ収束する signal ではない |
| 実務判断 | target price ではない | liquidity/capacity/regime diagnostic として表示する |

#### Gap Bucket

下表は `ADV60` の主要 horizon。`median_fixed_target_closure_ratio` は、観測日 `t` の固定 `liquidity_implied_price_t` に対して、将来株価がどれだけ gap を埋めたかを示す。`1.0` なら完全到達、`0.0` なら未収束。

| horizon | direction | gap bucket | obs | median gap | median excess | median closure | direction hit | rolling gap reduction | implied price change | future implied gap |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 60 | positive | `50-100%` | 17,184 | +72.0% | -1.16% | 0.023 | 54.6% | +17.2% | -7.0% | +58.7% |
| 60 | positive | `100-200%` | 18,604 | +141.0% | -0.89% | 0.013 | 54.7% | +13.8% | -7.4% | +121.6% |
| 60 | positive | `>200%` | 28,767 | +399.1% | -0.67% | 0.003 | 54.8% | +17.3% | -12.4% | +341.6% |
| 250 | positive | `50-100%` | 15,591 | +72.1% | -3.92% | 0.065 | 56.6% | +31.8% | -14.4% | +43.8% |
| 250 | positive | `100-200%` | 17,185 | +141.5% | -3.37% | 0.039 | 57.2% | +34.9% | -18.4% | +92.4% |
| 250 | positive | `>200%` | 27,070 | +401.2% | -1.08% | 0.013 | 58.8% | +42.6% | -29.7% | +251.5% |
| 500 | positive | `50-100%` | 13,559 | +72.3% | -9.41% | 0.100 | 57.2% | +37.6% | -20.6% | +31.8% |
| 500 | positive | `100-200%` | 15,260 | +141.4% | -8.99% | 0.048 | 56.7% | +51.6% | -29.9% | +65.9% |
| 500 | positive | `>200%` | 24,675 | +405.5% | -2.63% | 0.020 | 59.0% | +60.4% | -44.4% | +186.9% |
| 500 | negative | `50-100%` | 38,917 | -68.8% | -13.62% | -0.012 | 48.7% | +10.2% | +37.7% | -59.8% |

#### Regime

`rerating_participation` と `distribution_stress` はどちらも正方向の巨大 gap を持ちやすいが、固定 target への収束は弱い。差は「価格が示唆株価へ到達するか」ではなく、right-tail participation と drawdown/state の違いとして読む。

| horizon | regime | direction | obs | median gap | median excess | median closure | direction hit | rolling gap reduction | implied price change | future implied gap |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 60 | `rerating_participation` | positive | 10,968 | +394.8% | -0.32% | 0.003 | 54.1% | +4.2% | -2.3% | +387.1% |
| 60 | `distribution_stress` | positive | 18,733 | +384.7% | -0.95% | 0.003 | 54.7% | +23.2% | -16.6% | +299.3% |
| 250 | `rerating_participation` | positive | 9,767 | +411.3% | -1.11% | 0.013 | 58.1% | +36.3% | -24.9% | +277.8% |
| 250 | `distribution_stress` | positive | 17,412 | +393.0% | -1.96% | 0.013 | 58.7% | +43.3% | -29.7% | +235.7% |
| 500 | `rerating_participation` | positive | 8,855 | +428.7% | -2.02% | 0.024 | 60.9% | +57.6% | -42.1% | +205.0% |
| 500 | `distribution_stress` | positive | 15,091 | +409.8% | -3.95% | 0.024 | 60.4% | +59.8% | -42.2% | +187.8% |
| 500 | `stale_liquidity` | negative | 21,361 | -77.9% | -14.80% | -0.004 | 49.4% | +12.2% | +54.7% | -66.6% |

### Interpretation

`liquidity_implied_price` は price target ではない。現在の free-float 規模に対して ADV がどれだけ過大・過小かを、株価換算で直感的に見せるための scale diagnostic である。

正方向の大きな gap は、将来 return の平均では右尾を持つが、median excess は長期でも弱い。固定 target closure は 60d でほぼゼロ、500d でも `>200%` bucket の median が `0.020` に過ぎない。つまり「3倍に見えるから3倍へ行く」ではない。

一方で rolling gap は長期でかなり縮む。`>200%` positive gap の rolling gap reduction は 500d median `+60.4%` だが、同時に implied price change は `-44.4%`。これは、株価が示唆株価へ追いつくというより、出来高/参加が落ち着いて示唆株価側が下がることを示す。

負方向 gap は「株価が下がって流動性に見合う水準へ行く」という signal でもない。direction hit はおおむね 45-49% で、むしろ stale liquidity / capacity caution として読むべき。

### Production Implication

Symbol Workbench では `Med ADV60/FF` と `Liquidity Residual` を主指標にし、`流動性等価株価 Med ADV60` は scale diagnostic として補助表示に留める。

実務上は次の扱いが妥当。

| 用途 | 扱い |
|---|---|
| Prime high positive gap | price target ではなく participation / attention / right-tail diagnostic |
| Prime rerating participation | hard entry signal ではなく conviction / sizing / watchlist priority 候補 |
| Prime distribution stress | 出来高を伴う売り・イベント後の剥落 risk state |
| Negative gap / stale liquidity | undervaluation ではなく exit liquidity / capacity caution |
| Workbench 表示 | `Med ADV60/FF` と residual/regime を主役にし、`流動性等価株価` は補助表示に留める |

### Caveats

- 観測日の回帰は same-date Prime cross-section だけで推定するため、全期間 pooled regression より PIT-safe に寄せている。
- `ADV60` と recent return は observation close までを使うため、pre-open signal ではなく close 時点 diagnostic。
- 将来価格・将来 TOPIX・将来 implied price は outcome columns としてのみ使い、bucket/regime 作成には使っていない。
- free-float は `shares_outstanding - treasury_shares` proxy であり、真の浮動株比率ではない。
- result は portfolio execution / cost / turnover ではなく event-level convergence study。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/free_float_liquidity_implied_price_convergence.py`
- runner: `apps/bt/scripts/research/run_free_float_liquidity_implied_price_convergence.py`
- bundle experiment id: `market-behavior/free-float-liquidity-implied-price-convergence`
- latest result bundle: `/tmp/trading25-research/market-behavior/free-float-liquidity-implied-price-convergence/phase4_20260511_prime_implied_price_convergence_v2`
- result tables: `observation_df`, `convergence_by_gap_bucket_df`, `convergence_by_regime_df`, `daily_regression_diagnostics_df`, `latest_extreme_gap_df`
