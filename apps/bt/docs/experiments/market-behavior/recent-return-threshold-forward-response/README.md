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

### Interpretation

一般日次でも、runupは「強いほど買えばよい」ではない。高runupでは、右裾が太くなるためmeanは上がるが、typical tradeを表すmedianは改善しにくい。これは決算eventで見た構図と整合する。

一方で、`rerating_participation` を重ねると高runupでもmedianが改善する。つまり `runup` 単体は雑だが、流動性が時価総額対比で明確に増えている状態と組み合わせると、単純な過熱とは別の「参加型rerating」として読める。ただし severe loss は消えないので、position sizing / exit / event proximity の制御が必要。

### Production Implication

| 用途 | 推奨 |
|---|---|
| Daily Rankingの解釈 | `20d/60d runup` はmean改善ではなく分布拡大signalとして扱う |
| strong runup guard | 一般日次でも `20d >= +20%`, `60d >= +30%` はtail risk警戒 |
| rerating候補 | `runup + liquidity_residual_z >= 1` は別bucketとして扱う価値あり |
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
- latest result bundle: `/private/tmp/trading25-research/market-behavior/recent-return-threshold-forward-response/20260516_recent_return_threshold_forward_response_prime_v1`
- result tables: `coverage_diagnostics_df`, `threshold_response_df`, `joint_threshold_response_df`, `percentile_response_df`, `nonoverlap_response_df`, `annual_threshold_response_df`, `liquidity_interaction_df`, `observation_sample_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_recent_return_threshold_forward_response.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --thresholds-20d 0,10,20,30 \
  --thresholds-60d 0,20,30,40,50 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260516_recent_return_threshold_forward_response_prime_v1 \
  --min-observations 1000
```

## Artifact Tables

- `coverage_diagnostics_df`: observation coverage by market and liquidity scope.
- `threshold_response_df`: daily dense absolute threshold response.
- `joint_threshold_response_df`: daily dense joint `20d >= x` and `60d >= y` response.
- `percentile_response_df`: annual percentile bucket response.
- `nonoverlap_response_df`: weekly/monthly anchor threshold response.
- `annual_threshold_response_df`: year-by-year threshold response.
- `liquidity_interaction_df`: momentum state x liquidity regime response.
- `observation_sample_df`: bounded sample of the observation panel.
