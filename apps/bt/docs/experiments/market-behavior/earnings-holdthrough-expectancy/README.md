# Earnings Hold-Through Expectancy

## Published Readout

### Decision

`earnings_holdthrough_expectancy` は、いわゆる「決算またぎ」を、発表前までに観測できる条件で切り、その後に出た開示内容とリターン分布を見る event-study research として追加する。

意思決定用の主表は `precondition_outcome_df`。ここでは `event_strength` や `has_next_guidance` を条件に使わない。これらは発表後に初めて分かる outcome なので、主表では positive / negative event rate、next guidance rate として後から観察する。

今回の corrected run では、Prime に限定しても「決算またぎを一律に買う」根拠はまだ弱い。20-session TOPIX excess の中央値は Prime 全体で `-0.83%`、FY event で `-1.05%`。ただし、発表前 60d が極端に売られた `strong_drawdown` は FY / non-FY とも中央値が小幅プラスで、逆に FY の短期・中期 run-up は中央値が弱い。

この readout は bundle `/private/tmp/trading25-research/market-behavior/earnings-holdthrough-expectancy/20260516_earnings_holdthrough_liquidity_z_v2` に基づく。入力 DB は `/Users/shinjiroaso/.local/share/trading25/market-timeseries/market.duckdb`、対象は `2016-04-01` から `2026-05-14`。

### Main Findings

#### Coverage

Prime realized events は `80,428` events / `2,047` codes。20-session TOPIX excess の全体中央値は `-0.83%`、severe loss (`<= -10%`) は `12.92%`。

| scope | events | positive event rate | negative event rate | mean 20d excess | median 20d excess | severe loss |
|---|---:|---:|---:|---:|---:|---:|
| Prime all | `80,428` | `29.77%` | `19.56%` | `+0.11%` | `-0.83%` | `12.92%` |
| Prime FY | `25,769` | `48.95%` | `26.73%` | `+0.09%` | `-1.05%` | `14.56%` |
| Prime non-FY | `54,659` | `20.73%` | `16.17%` | `+0.12%` | `-0.73%` | `12.14%` |

#### Pre 60d Move

60d の事前値動きは、決算内容の outcome rate と事後リターンの両方を変える。FY では `strong_drawdown` が最も良く、`strong_runup` は positive event rate が高いにもかかわらず severe loss が最も大きい。

| FY | 60d pre bucket | events | positive rate | negative rate | next guidance rate | mean 20d excess | median 20d excess | severe loss |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| true | `strong_drawdown` | `1,573` | `35.86%` | `34.77%` | `41.26%` | `+1.59%` | `+0.28%` | `15.13%` |
| true | `runup` | `5,916` | `55.04%` | `23.23%` | `63.57%` | `+0.30%` | `-0.90%` | `15.48%` |
| true | `strong_runup` | `2,116` | `53.92%` | `19.05%` | `51.32%` | `+0.86%` | `-1.08%` | `21.79%` |
| true | `flat` | `8,401` | `51.78%` | `29.12%` | `65.52%` | `-0.27%` | `-1.17%` | `12.59%` |
| true | `drawdown` | `6,526` | `48.41%` | `31.18%` | `60.93%` | `-0.44%` | `-1.37%` | `14.74%` |
| false | `strong_drawdown` | `1,971` | `15.22%` | `23.03%` | `0.00%` | `+1.19%` | `+0.18%` | `17.91%` |
| false | `runup` | `15,636` | `23.09%` | `14.58%` | `0.00%` | `+0.31%` | `-0.60%` | `11.87%` |
| false | `strong_runup` | `5,861` | `27.37%` | `13.29%` | `0.00%` | `+0.90%` | `-0.79%` | `19.14%` |
| false | `flat` | `18,387` | `19.68%` | `16.48%` | `0.00%` | `-0.15%` | `-0.74%` | `9.41%` |
| false | `drawdown` | `11,882` | `17.79%` | `18.37%` | `0.00%` | `-0.20%` | `-0.90%` | `12.11%` |

#### Pre 20d Move

20d では、FY `strong_runup` が最も弱い。positive event rate は低くないが、事前に短期期待が乗った状態では、発表後の中央値と左尾が悪い。

| FY | 20d pre bucket | events | positive rate | negative rate | mean 20d excess | median 20d excess | severe loss |
|---:|---|---:|---:|---:|---:|---:|---:|
| true | `strong_runup` | `721` | `46.32%` | `24.69%` | `-0.07%` | `-2.10%` | `23.30%` |
| true | `runup` | `6,508` | `52.97%` | `26.32%` | `-0.26%` | `-1.52%` | `16.76%` |
| true | `flat` | `12,536` | `50.73%` | `28.65%` | `-0.04%` | `-1.06%` | `13.14%` |
| true | `drawdown` | `4,667` | `48.79%` | `27.26%` | `+0.62%` | `-0.58%` | `14.76%` |
| false | `strong_drawdown` | `493` | `20.49%` | `17.65%` | `+3.51%` | `+1.78%` | `16.02%` |
| false | `strong_runup` | `930` | `23.55%` | `15.38%` | `+2.75%` | `-0.37%` | `23.44%` |
| false | `runup` | `13,195` | `23.12%` | `15.38%` | `+0.64%` | `-0.39%` | `13.01%` |
| false | `flat` | `30,565` | `20.07%` | `16.12%` | `-0.10%` | `-0.72%` | `10.41%` |
| false | `drawdown` | `9,300` | `19.63%` | `17.77%` | `-0.38%` | `-1.41%` | `15.26%` |

#### Med ADV60 / Free Float

ADV60 / free-float は単独 alpha よりも、参加度・混雑・容量 proxy として読む。FY では高 ADV/FF ほど中央値が改善するが、severe loss も増える。低 ADV/FF は左尾は軽いが中央値も弱い。

| FY | ADV60 / FF bucket | events | positive rate | negative rate | mean 20d excess | median 20d excess | severe loss |
|---:|---|---:|---:|---:|---:|---:|---:|
| true | `lt0.1` | `1,687` | `57.26%` | `28.51%` | `-1.16%` | `-1.31%` | `9.60%` |
| true | `0.1-0.5` | `9,814` | `58.07%` | `29.26%` | `-0.77%` | `-1.73%` | `15.11%` |
| true | `0.5-1.0` | `3,157` | `56.83%` | `28.41%` | `+0.00%` | `-0.86%` | `18.40%` |
| true | `1.0-2.0` | `1,467` | `54.40%` | `29.58%` | `+1.63%` | `+0.02%` | `17.86%` |
| true | `ge2.0` | `761` | `53.61%` | `30.75%` | `+2.62%` | `+0.03%` | `20.50%` |
| false | `lt0.1` | `7,143` | `13.27%` | `11.86%` | `-0.47%` | `-0.99%` | `7.04%` |
| false | `0.1-0.5` | `29,590` | `20.73%` | `16.55%` | `-0.18%` | `-0.83%` | `11.07%` |
| false | `0.5-1.0` | `8,256` | `23.86%` | `17.74%` | `+0.56%` | `-0.33%` | `16.15%` |
| false | `1.0-2.0` | `3,805` | `27.60%` | `19.82%` | `+0.93%` | `-0.15%` | `16.90%` |
| false | `ge2.0` | `2,163` | `27.92%` | `22.52%` | `+2.19%` | `-0.62%` | `20.85%` |

#### Liquidity Residual Z / State

2026-05-16 follow-up で、Daily Ranking と同じ考え方の Prime `liquidity_residual_z` を追加した。これは発表前営業日 `pre_event_date` 時点の Prime cross-section で `log(Med ADV60) ~ log(free-float market cap)` を回帰し、残差を z-score 化したもの。`liquidity_regime` は `z >= 1` かつ 20d/60d return が両方プラスなら `rerating_participation`、`z >= 1` かつどちらかマイナスなら `distribution_stress`、`z <= -1` なら `stale_liquidity` とする。

`20d strong_runup` を除外した Prime FY では、`stale_liquidity` が弱く、high liquidity residual 系の state が強い。ただし severe loss も上がるため、単独の買い条件ではなく状態・sizing proxy として扱う。

| state | events | mean 20d excess | median 20d excess | win rate | severe loss |
|---|---:|---:|---:|---:|---:|
| `stale_liquidity` | `2,871` | `-0.67%` | `-1.24%` | `40.13%` | `10.48%` |
| `neutral` | `14,808` | `+0.20%` | `-0.90%` | `43.67%` | `13.57%` |
| `distribution_stress` | `1,989` | `+1.76%` | `+0.15%` | `48.57%` | `16.99%` |
| `rerating_participation` | `914` | `+2.27%` | `+0.64%` | `50.55%` | `16.74%` |
| `missing` | `5,511` | `-0.73%` | `-1.58%` | `40.61%` | `14.30%` |

#### Strong / Weak Precondition Combos

Sample `>=250` の precondition combo だけで見ると、強い側は non-FY の中位以上 ADV/FF と runup / flat 系に偏る。弱い側は FY の 20d runup と 60d drawdown / runup の混合、または low ADV/FF に偏る。

| rank | FY | 20d bucket | 60d bucket | ADV/FF | events | positive rate | negative rate | median 20d excess | severe loss |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| strong 1 | false | `runup` | `strong_runup` | `0.5-1.0` | `563` | `27.53%` | `16.87%` | `+0.77%` | `19.89%` |
| strong 2 | false | `runup` | `runup` | `1.0-2.0` | `331` | `30.21%` | `15.11%` | `+0.69%` | `14.50%` |
| strong 3 | false | `flat` | `flat` | `1.0-2.0` | `518` | `27.61%` | `20.66%` | `+0.64%` | `11.20%` |
| strong 4 | true | `flat` | `runup` | `0.5-1.0` | `351` | `62.11%` | `24.50%` | `+0.49%` | `14.53%` |
| weak 1 | true | `runup` | `drawdown` | `0.1-0.5` | `474` | `51.48%` | `32.28%` | `-3.20%` | `18.57%` |
| weak 2 | false | `drawdown` | `flat` | `lt0.1` | `292` | `15.41%` | `12.33%` | `-2.82%` | `13.70%` |
| weak 3 | true | `flat` | `drawdown` | `0.5-1.0` | `443` | `51.24%` | `34.76%` | `-2.28%` | `19.64%` |
| weak 4 | true | `runup` | `runup` | `0.1-0.5` | `1,100` | `62.91%` | `27.18%` | `-2.09%` | `17.45%` |

### Interpretation

ごく重要な整理として、`positive event` は投資時点では分からない。したがって「positive 決算をまたぐと強いか」ではなく、「発表前の状態ごとに、positive / negative がどれくらい出て、その後の分布がどうなるか」と読む。

Prime で最も読みやすいのは、「FY の短期 runup は避けたい」というリスク側の示唆。positive event rate が高くても、20d `strong_runup` は median `-2.10%`、severe loss `23.30%` で、事前期待が乗った決算の持ち越しは左尾が重い。

一方、60d `strong_drawdown` は FY でも non-FY でも中央値が小幅プラス。ただし negative event rate も高く、これは alpha というより「売られすぎ後の分布が右に歪むが、失敗時の損失も残る」候補として扱うのが妥当。

ADV60 / FF は容量と混雑の diagnostic。FY では `1.0%+` が中央値を改善するが、severe loss も `18-21%` に上がるため、単独の買い条件ではなく、position sizing / risk cap と組み合わせるべき。

### Production Implication

この Phase 1 だけで production strategy に「決算またぎ」entry を追加しない。

次に見る価値がある候補は以下。

| 用途 | 候補 |
|---|---|
| Avoid / risk cap | Prime FY の `20d strong_runup`、`20d runup x 60d runup x ADV/FF 0.1-0.5` |
| Mean-reversion candidate | Prime の `60d strong_drawdown`。ただし negative event rate と severe loss を別途管理 |
| Liquidity sizing | `ADV/FF >= 1.0%` は期待値改善と左尾増加が同居するため、entry condition ではなく sizing/risk state として扱う |
| Frontend surface | 発表前 features (`pre_return_20d_bucket`, `pre_return_60d_bucket`, `adv60_to_free_float_bucket`) と、事後 outcome rates (`positive_event_rate_pct`, `negative_event_rate_pct`) を分けて表示する |

### Caveats

- `event_strength` は outcome diagnostic。`guidance_metric` は `next_year_forecast_earnings_per_share > next_year_forecast_profit > forecast_eps`、`actual_metric` は `earnings_per_share > profit` で前回開示比を取っている。
- `has_next_guidance` は発表後 outcome。意思決定用の precondition group には使わず、`next_guidance_rate_pct` として観察する。
- `is_fy` は開示対象期の FY 判定。実運用で完全な事前条件にする場合は、earnings calendar / fiscal-period schedule 由来の FY 判定に寄せる余地がある。
- `statements` は開示時刻を持たないため、pre feature は開示日前営業日 close までに限定した。
- Entry/exit は event study であり、実際の約定時刻・決算発表時刻の昼/引け後差は未反映。
- `Med ADV60` は Daily Ranking と同じ trailing median `close * volume` を使う。`liquidity_residual_z` は Prime event のみで計算し、Standard / Growth には外挿しない。
- unknown market は PIT master coverage の外側を含むため、主判断から外す。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py`
- runner: `apps/bt/scripts/research/run_earnings_holdthrough_expectancy.py`
- bundle experiment id: `market-behavior/earnings-holdthrough-expectancy`
- latest result bundle: `/private/tmp/trading25-research/market-behavior/earnings-holdthrough-expectancy/20260516_earnings_holdthrough_liquidity_z_v2`
- result tables: `event_feature_df`, `coverage_diagnostics_df`, `precondition_outcome_df`, `bucket_expectancy_df`, `liquidity_interaction_df`, `signed_premove_df`, `holdthrough_return_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_earnings_holdthrough_expectancy.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --pre-windows 20,60 \
  --horizons 1,5,20 \
  --liquidity-window 60 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260516_earnings_holdthrough_liquidity_z_v2
```

## Artifact Tables

- `event_feature_df`: event-level PIT-safe pre features, outcome labels, and hold-through returns.
- `coverage_diagnostics_df`: market-level event / FY / guidance / liquidity coverage.
- `precondition_outcome_df`: ex-ante grouping table. Groups by market / FY / pre 20d and 60d return bucket / ADV60-to-free-float bucket / liquidity residual bucket / liquidity state, then reports event outcome rates and forward returns.
- `bucket_expectancy_df`: outcome diagnostic table grouped by `market x FY x next guidance x event_strength x pre-return buckets`. Do not use as an ex-ante decision table.
- `liquidity_interaction_df`: outcome diagnostic table grouped by `market x liquidity_regime x event_strength x FY x next guidance`.
- `signed_premove_df`: event direction and pre 60d signed pre-move interaction.
- `holdthrough_return_df`: raw and TOPIX-excess hold-through return by FY / next guidance. Do not use `next guidance` as an ex-ante filter.
