# Recent Return Threshold Forward Response

## Published Readout

### Decision

決算eventに限らない一般日次panelでも、`20d/60d runup` は「medianを素直に改善するsignal」ではない。Primeでは、runup閾値を上げるほど forward 5d/20d の **mean は改善しやすい** が、**median は概ねマイナスのまま** で、同時に severe loss rate が上がる。したがって `20d >= +20%` や `60d >= +30%` は、一般にも「期待値が弱い」というより「右裾依存でtail riskが重い状態」として扱うのが正しい。

この readout は bundle `/private/tmp/trading25-research/market-behavior/recent-return-threshold-forward-response/20260516_recent_return_threshold_forward_response_prime_v1` に基づく。入力 DB は `~/.local/share/trading25/market-timeseries/market.duckdb`、対象 anchor は `2016-04-01` から `2026-05-14`。初回runは実用上の主対象である Prime に限定した。

### Main Findings

#### 結論

Prime全体では、20d runupも60d runupも、閾値を上げるとmeanは改善するがmedianは改善しない。特に20d/60dの高runupでは、20d forwardの severe loss rate が明確に上がる。

| condition | observations | 5d median excess | 5d mean excess | 5d severe loss | 20d median excess | 20d mean excess | 20d severe loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| `20d >= +0%` | `928,169` | `-0.177%` | `-0.003%` | `1.12%` | `-0.552%` | `-0.031%` | `7.83%` |
| `20d >= +10%` | `198,629` | `-0.149%` | `+0.154%` | `2.07%` | `-0.566%` | `+0.277%` | `11.12%` |
| `20d >= +20%` | `47,309` | `-0.233%` | `+0.304%` | `3.64%` | `-0.527%` | `+0.716%` | `14.72%` |
| `20d >= +30%` | `15,484` | `-0.295%` | `+0.462%` | `5.79%` | `-0.899%` | `+0.850%` | `17.83%` |
| `60d >= +30%` | `84,562` | `-0.183%` | `+0.218%` | `3.05%` | `-0.739%` | `+0.564%` | `14.83%` |
| `60d >= +50%` | `21,282` | `-0.309%` | `+0.443%` | `5.35%` | `-0.804%` | `+1.807%` | `18.41%` |

#### 結論

non-overlapで見ても、結論は大きく変わらない。weekly/monthly anchorでも `20d >= +20%` と `60d >= +30%` はmedianを押し上げず、20d horizonでは severe loss が重い。

| sample | condition | observations | 5d median excess | 20d median excess | 20d severe loss |
|---|---|---:|---:|---:|---:|
| `weekly` | `20d >= +20%` | `9,585` | `-0.220%` | `-0.584%` | `14.94%` |
| `weekly` | `60d >= +30%` | `17,443` | `-0.166%` | `-0.796%` | `14.66%` |
| `weekly` | `60d >= +50%` | `4,397` | `-0.242%` | `-0.953%` | `18.29%` |
| `monthly` | `20d >= +20%` | `2,392` | `-0.443%` | `-0.838%` | `15.37%` |
| `monthly` | `60d >= +30%` | `4,169` | `-0.489%` | `-0.973%` | `15.78%` |
| `monthly` | `60d >= +40%` | `2,028` | `-0.673%` | `-1.438%` | `18.56%` |

#### 結論

drawdown側は、20dの深い下落ではreversalっぽい挙動が出る。ただしこちらもtail riskは重い。`20d <= -20%` は5d median `+0.236%`、20d median `+0.022%` まで改善するが、20d severe loss は `14.01%`。`20d <= -30%` はさらにreversalが強いが、観測数が `2,201` と少なく、20d severe loss は `19.92%`。

| condition | observations | 5d median excess | 5d mean excess | 20d median excess | 20d mean excess | 20d severe loss |
|---|---:|---:|---:|---:|---:|---:|
| `20d <= -10%` | `113,652` | `-0.186%` | `+0.085%` | `-0.790%` | `+0.131%` | `11.06%` |
| `20d <= -20%` | `15,080` | `+0.236%` | `+0.686%` | `+0.022%` | `+1.250%` | `14.01%` |
| `20d <= -30%` | `2,201` | `+0.794%` | `+1.682%` | `+1.289%` | `+2.190%` | `19.92%` |
| `60d <= -30%` | `11,270` | `-0.162%` | `+0.434%` | `-0.292%` | `+0.534%` | `17.59%` |
| `60d <= -40%` | `2,564` | `-0.254%` | `+0.767%` | `+0.189%` | `+0.789%` | `22.68%` |

#### 結論

liquidity regimeを重ねると、同じrunupでも性格が変わる。`rerating_participation` はmean/medianとも改善しやすいが、severe lossも同時に重い。`stale_liquidity` は同じrunupでも明確に悪い。

| state | liquidity | observations | 5d median excess | 5d mean excess | 20d median excess | 20d mean excess | 20d severe loss |
|---|---|---:|---:|---:|---:|---:|---:|
| `20d strong_runup` | `all_liquidity` | `47,309` | `-0.233%` | `+0.304%` | `-0.527%` | `+0.716%` | `14.72%` |
| `20d strong_runup` | `rerating_participation` | `11,576` | `+0.073%` | `+0.822%` | `+0.622%` | `+2.967%` | `18.15%` |
| `20d strong_runup` | `stale_liquidity` | `2,973` | `-0.697%` | `-0.424%` | `-1.221%` | `-1.113%` | `16.12%` |
| `60d strong_runup` | `all_liquidity` | `84,562` | `-0.183%` | `+0.218%` | `-0.739%` | `+0.564%` | `14.83%` |
| `60d strong_runup` | `rerating_participation` | `21,156` | `+0.052%` | `+0.786%` | `+0.255%` | `+2.438%` | `17.62%` |
| `60d strong_runup` | `stale_liquidity` | `3,105` | `-0.412%` | `-0.333%` | `-1.157%` | `-1.064%` | `16.26%` |

#### Forward P/OP Follow-Up

2026-05-17 follow-up で、`P/OP = market cap / operating_profit` と `forward P/OP = market cap / forecast_operating_profit` を研究内で派生し、`valuation_response_df` として `forward PER` / `P/OP` / `forward P/OP` の単独分位を追加した。

全Prime日次では、単独の `forward P/OP` は `forward PER` の完全な上位互換ではない。cheapest 10% の20d meanは `forward P/OP` が `+0.50%`、`forward PER` が `+0.38%` とP/OPが上回るが、20d medianは `-0.29%` vs `-0.10%`、win rateは `48.01%` vs `49.31%` でforward PERの方が安定する。

rerating participationでは、`forward P/OP` cheapest 10% の20d meanは `+5.72%` と強いが、median / win rateでは `forward PER` cheapest 10% が上回る。したがって `forward P/OP` は単独主役というより、右尾候補の補助指標と、低forward PERの品質フィルタとして扱うのが自然。

rerating participationかつoverheat除外で `forward_per <= 20` に広げると、母数は十分に増える。`forward_per <= 20` は `13,871` observations / `280` codes、20d mean `+2.66%`、median `+1.22%`、win rate `56.22%`。ここに `forward_p_op >= 20` を重ねると `374` observations / `18` codes、20d mean `+0.71%`、median `-0.87%`、win rate `44.39%` まで悪化する。つまり一般日次でも、`low forward PER` なのに `high forward P/OP` は割安の質が悪い警戒signalとして機能する。

| scope | condition | observations | codes | 20d mean | 20d median | win rate | severe loss |
|---|---|---:|---:|---:|---:|---:|---:|
| Prime daily | `forward_p_op cheapest 10%` | `44,804` | `369` | `+0.50%` | `-0.29%` | `48.01%` | `7.24%` |
| Prime daily | `forward_per cheapest 10%` | `46,376` | `467` | `+0.38%` | `-0.10%` | `49.31%` | `6.13%` |
| Rerate no overheat | `forward_per <= 20` | `13,871` | `280` | `+2.66%` | `+1.22%` | `56.22%` | `8.68%` |
| Rerate no overheat | `forward_per <= 20 AND forward_p_op >= 20` | `374` | `18` | `+0.71%` | `-0.87%` | `44.39%` | `12.83%` |

#### PBR Follow-Up

2026-05-18 follow-up で、`daily_valuation.pbr` を `valuation_response_df` に追加した。新bundle
`/tmp/trading25-research/market-behavior/recent-return-threshold-forward-response/20260518_recent_return_threshold_forward_response_prime_pbr_v1`
では、Prime daily の `PBR` coverage は all-liquidity `93.92%`、`rerating_participation` `99.36%` まで出ており、PBR bucket を読むには十分。

全Prime日次では、低PBRは低forward PERと似た方向に効く。`pbr cheapest_10pct` は 20d mean `+0.75%`、median `+0.04%`、win rate `50.28%` で、`forward_per cheapest_10pct` の mean `+0.64%`、median `+0.05%`、win rate `50.35%` とほぼ同じ。ただし `pbr expensive_10pct` は median `-1.95%`、severe loss `17.67%` まで悪化し、高PBR側は明確に避けたい分布になる。

`rerating_participation` では PBR の差がより大きい。`pbr cheapest_10pct` は 20d mean `+2.82%`、median `+2.11%`、win rate `61.52%` で、同じ条件の `forward_per cheapest_10pct`（mean `+1.57%`、median `+0.74%`、win rate `54.11%`）を上回る。一方で `pbr expensive_10pct` は median `-1.02%`、severe loss `21.83%`。この slice では、低PBRは単なる forward PER の劣化コピーではなく、rerating参加状態の補助軸として価値がある。

| scope | condition | observations | codes | 20d mean | 20d median | win rate | severe loss |
|---|---|---:|---:|---:|---:|---:|---:|
| Prime daily | `pbr cheapest 10%` | `157,663` | `444` | `+0.75%` | `+0.04%` | `50.28%` | `4.30%` |
| Prime daily | `forward_per cheapest 10%` | `146,677` | `670` | `+0.64%` | `+0.05%` | `50.35%` | `5.18%` |
| Prime daily | `pbr expensive 10%` | `158,294` | `386` | `-1.32%` | `-1.95%` | `41.05%` | `17.67%` |
| Rerating participation | `pbr cheapest 10%` | `8,087` | `82` | `+2.82%` | `+2.11%` | `61.52%` | `6.58%` |
| Rerating participation | `forward_per cheapest 10%` | `9,876` | `143` | `+1.57%` | `+0.74%` | `54.11%` | `7.67%` |
| Rerating participation | `pbr expensive 10%` | `16,814` | `187` | `+0.18%` | `-1.02%` | `46.36%` | `21.83%` |

#### PBR x Forward PER Interaction Follow-Up

2026-05-18 follow-up で、`valuation_interaction_df` を追加し、`PBR low20` と `forward PER low20` を同時に分解した。ここでの `low20` は既存 `valuation_response_df` の `cheapest_20pct` band ではなく、`year x market_scope` 内の累積下位20%（`percent_rank <= 0.2`）。新bundleは
`/tmp/trading25-research/market-behavior/recent-return-threshold-forward-response/20260518_recent_return_threshold_forward_response_prime_value_interaction_v1`。

結論として、`rerating_participation` では低PBRの効果は低forward PERをcontrolしても残る。20d close-to-closeでは `both_low` が最強だが、`low_pbr_only` も mean `+2.09%`、median `+1.20%`、win rate `55.82%` と十分に強い。一方 `low_forward_per_only` は mean `+0.27%`、median `-0.52%`、win rate `47.20%` まで落ち、rerating参加局面では「低forward PERだけ」より「低PBR」の方が残りやすい。したがって、このsliceでは「forward PERが主でPBRが補助」というより、**PBRが主で、forward PERは低PBR候補をさらに強くする副次条件** と読むのが自然。

| scope | bucket | observations | codes | 20d mean | 20d median | win rate | severe loss | median PBR | median forward PER |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Prime daily | `both_low` | `122,357` | `511` | `+1.00%` | `+0.35%` | `52.62%` | `3.41%` | `0.51` | `7.60` |
| Prime daily | `low_pbr_only` | `171,391` | `588` | `+0.42%` | `-0.24%` | `48.15%` | `4.16%` | `0.54` | `13.65` |
| Prime daily | `low_forward_per_only` | `171,154` | `776` | `+0.20%` | `-0.21%` | `48.51%` | `6.23%` | `0.95` | `8.00` |
| Prime daily | `neither_low` | `1,006,173` | `1,544` | `-0.43%` | `-0.90%` | `44.19%` | `8.83%` | `1.47` | `16.34` |
| Rerating participation | `both_low` | `6,923` | `80` | `+3.44%` | `+2.41%` | `64.16%` | `3.54%` | `0.45` | `7.55` |
| Rerating participation | `low_pbr_only` | `5,222` | `90` | `+2.09%` | `+1.20%` | `55.82%` | `8.08%` | `0.53` | `13.41` |
| Rerating participation | `low_forward_per_only` | `9,383` | `171` | `+0.27%` | `-0.52%` | `47.20%` | `9.44%` | `1.00` | `7.27` |
| Rerating participation | `neither_low` | `51,646` | `500` | `+0.98%` | `-0.10%` | `49.56%` | `14.10%` | `2.39` | `19.75` |

#### Long Trend Quadrant Follow-Up

2026-05-23 follow-up で、`20d / 60d` の符号に `120d` と `150d` を重ねた4象限を `long_trend_quadrant_response_df` として追加した。新bundleは
`/private/tmp/trading25-research/market-behavior/recent-return-threshold-forward-response/20260523_recent_return_long_trend_quadrants_prime_v1`。入力は active `market.duckdb`、Prime daily dense panel、20d close-to-close TOPIX excess return。今回の active DB では、long-window lag と `stock_master_daily` exact-date coverage の制約を受け、実効 window は `2022-04-04` から `2026-05-14` になった。

#### 結論

長期窓を足すと `persistent_rerating` が最も無難になるが、Prime 全体では median を大きくプラス化するほどではない。`relief_bounce` は 120d では persistent と近いが、150d では悪化し、`uptrend_pullback` と `short_bounce` は median / win rate が弱い。

| Long window | Quadrant | Condition | Observation | 20d mean | 20d median | Win rate | Severe loss |
|---|---|---|---:|---:|---:|---:|---:|
| 120d | `persistent_rerating` | `20d>0, 60d>0, 120d>0` | `512,274` | `-0.04%` | `-0.64%` | `45.96%` | `8.40%` |
| 120d | `relief_bounce` | `20d>0, 60d>0, 120d<=0` | `94,098` | `-0.19%` | `-0.60%` | `46.00%` | `7.85%` |
| 120d | `uptrend_pullback` | `20d<0, 60d>0, 120d>0` | `188,897` | `-0.33%` | `-1.00%` | `43.66%` | `8.86%` |
| 120d | `short_bounce` | `20d>0, 60d<=0, 120d<=0` | `119,030` | `-0.37%` | `-0.71%` | `44.87%` | `8.29%` |
| 150d | `persistent_rerating` | `20d>0, 60d>0, 150d>0` | `490,821` | `+0.04%` | `-0.58%` | `46.40%` | `8.20%` |
| 150d | `relief_bounce` | `20d>0, 60d>0, 150d<=0` | `98,660` | `-0.34%` | `-0.71%` | `45.23%` | `8.38%` |
| 150d | `uptrend_pullback` | `20d<0, 60d>0, 150d>0` | `178,484` | `-0.23%` | `-0.92%` | `44.23%` | `8.71%` |
| 150d | `short_bounce` | `20d>0, 60d<=0, 150d<=0` | `105,471` | `-0.45%` | `-0.75%` | `44.61%` | `8.66%` |

Ranking Color Evidence との接続では、長期窓そのものより `liquidity_regime` との掛け合わせが重要になる。`rerating_participation` かつ `persistent_rerating` は 120d/150d とも median がプラス圏まで改善するが、severe loss は 13% 前後とまだ重い。`stale_liquidity` は長期窓がプラスでも median が悪く、既存の stale caution を弱める材料にはならない。

| Long window | Quadrant | Liquidity | Observation | 20d mean | 20d median | Win rate | Severe loss |
|---|---|---|---:|---:|---:|---:|---:|
| 120d | `persistent_rerating` | `rerating_participation` | `64,637` | `+1.35%` | `+0.07%` | `50.4%` | `13.1%` |
| 120d | `relief_bounce` | `rerating_participation` | `10,387` | `+0.42%` | `-0.01%` | `50.0%` | `14.4%` |
| 120d | `uptrend_pullback` | `distribution_stress` | `28,667` | `+0.89%` | `-0.46%` | `47.7%` | `14.2%` |
| 150d | `persistent_rerating` | `rerating_participation` | `61,854` | `+1.45%` | `+0.16%` | `50.8%` | `12.9%` |
| 150d | `relief_bounce` | `rerating_participation` | `10,617` | `-0.02%` | `-0.41%` | `48.1%` | `15.2%` |
| 150d | `short_bounce` | `distribution_stress` | `13,498` | `-0.29%` | `-0.99%` | `44.6%` | `14.1%` |

### Interpretation

一般日次でも、runupは「強いほど買えばよい」ではない。高runupでは、右裾が太くなるためmeanは上がるが、typical tradeを表すmedianは改善しにくい。これは決算eventで見た構図と整合する。

一方で、`rerating_participation` を重ねると高runupでもmedianが改善する。つまり `runup` 単体は雑だが、流動性が時価総額対比で明確に増えている状態と組み合わせると、単純な過熱とは別の「参加型rerating」として読める。ただし severe loss は消えないので、position sizing / exit / event proximity の制御が必要。

`forward P/OP` は `forward PER` を置き換える単独rankerではなく、`forward PER` の営業利益品質を確認する補助指標として有効。特に `forward_per <= 20` なのに `forward_p_op >= 20` のような歪みは、rerating候補でもmedian / win rateを壊しやすい。

`PBR` は一般日次の低valuation軸として単独でも意味がある。特に `rerating_participation` では低PBR bucket の median / win rate が強く、forward PER の補助指標として使える。一方で Prime 全体の通常局面では低PBRと低forward PERの方向はかなり近いので、主役を置き換えるというより、低forward PER候補の追加確認と高PBR警戒に使う。

PBR x forward PER 分解では、低PBRは低forward PERをcontrolしても `rerating_participation` 内で残る。`both_low` は最も強く、`low_pbr_only` も十分に強いが、`low_forward_per_only` は弱い。したがって rerating参加候補では、forward PER単独よりも「低PBRを満たすか」を独立のquality/value checkとして扱う価値がある。実装・運用上の優先順位は、まず `low PBR`、その中で `low forward PER` ならさらに優先、`low forward PER only` は単独では強い買い根拠にしない、という順序が妥当。

120d/150d の4象限は、`20d/60d > 0` をそのまま強化するというより、「短中期の反発が長期上昇の中にあるか、長期下落からの戻りか」を分ける regime check として有効。150d の方が `persistent_rerating` と `relief_bounce` の差が明確で、UI/Ranking に近い実装候補としては 150d がやや読みやすい。一方で、長期窓単体では typical return を強く改善しないため、色を直接変えるより、`rerating_participation` / value confirmation と組み合わせる補助 overlay に留める。

### Production Implication

| 用途 | 推奨 |
|---|---|
| Daily Rankingの解釈 | `20d/60d runup` はmean改善ではなく分布拡大signalとして扱う |
| strong runup guard | 一般日次でも `20d >= +20%`, `60d >= +30%` はtail risk警戒 |
| rerating候補 | `runup + liquidity_residual_z >= 1` は別bucketとして扱う価値あり |
| valuation quality | `forward_p_op` はRankingに表示し、低forward PERの品質確認・警戒flagに使う |
| PBR補助 | `pbr` は低forward PER候補の補助軸として表示し、高PBRはtail risk警戒に使う |
| PBR x forward PER | PBRを主軸、forward PERを副軸として扱う。`both_low` を最優先、`low_pbr_only` は候補維持、`low_forward_per_only` は単独根拠にしない |
| long trend quadrant | `20d/60d>0` に `150d>0` を足した `persistent_rerating` は補助的に優先。`150d<=0` の `relief_bounce` は persistent と分けて caution 寄りに読む |
| stale除外 | `runup + stale_liquidity` は避けたい |
| reversal候補 | `20d <= -20%` は候補だが、severe lossが重く別researchが必要 |

### Caveats

- 初回runはPrime限定。runnerは `--markets all` / `--markets prime,standard,growth` を受け付けるが、全市場・全期間は重いため分割runを推奨する。
- `observation_sample_df` は全観測ではなく sample table。集計はDuckDB temp table上で行い、bundleにはsummary tablesを保存する。
- `liquidity_residual_z` は `med ADV60` と statements由来の free-float market cap を使う。Daily Rankingのshare adjustmentに完全一致する保証はないため、古いsplit eventでは残差がありうる。
- daily dense panelはoverlapを含むため、weekly/monthly non-overlap表を併読する。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/recent_return_threshold_forward_response.py`
- runner: `apps/bt/scripts/research/run_recent_return_threshold_forward_response.py`
- bundle experiment id: `market-behavior/recent-return-threshold-forward-response`
- latest result bundle: `/private/tmp/trading25-research/market-behavior/recent-return-threshold-forward-response/20260523_recent_return_long_trend_quadrants_prime_v1`
- result tables: `coverage_diagnostics_df`, `threshold_response_df`, `joint_threshold_response_df`, `percentile_response_df`, `valuation_response_df`, `valuation_interaction_df`, `long_trend_quadrant_response_df`, `nonoverlap_response_df`, `annual_threshold_response_df`, `liquidity_interaction_df`, `observation_sample_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_recent_return_threshold_forward_response.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --pre-windows 20,60,120,150 \
  --long-trend-windows 120,150 \
  --thresholds-20d 0,10,20,30 \
  --thresholds-60d 0,20,30,40,50 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260523_recent_return_long_trend_quadrants_prime_v1 \
  --min-observations 1000
```

## Artifact Tables

- `coverage_diagnostics_df`: observation coverage by market and liquidity scope.
- `threshold_response_df`: daily dense absolute threshold response.
- `joint_threshold_response_df`: daily dense joint `20d >= x` and `60d >= y` response.
- `percentile_response_df`: annual percentile bucket response.
- `valuation_response_df`: annual valuation percentile response for PER / forward PER / PBR / P/OP / forward P/OP.
- `valuation_interaction_df`: cumulative low20 PBR x cumulative low20 forward PER response.
- `long_trend_quadrant_response_df`: `20d/60d/120d` and `20d/60d/150d` sign quadrant response.
- `nonoverlap_response_df`: weekly/monthly anchor threshold response.
- `annual_threshold_response_df`: year-by-year threshold response.
- `liquidity_interaction_df`: momentum state x liquidity regime response.
- `observation_sample_df`: bounded sample of the observation panel.
