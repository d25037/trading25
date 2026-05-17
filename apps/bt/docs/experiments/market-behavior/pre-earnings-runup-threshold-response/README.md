# Pre-Earnings Runup Threshold Response

## Published Readout

### Decision

`20d runup` と `60d runup` に同一の固定閾値を当てるのは粗すぎる。今回の結果では、Prime の発生頻度ベースで見ると `20d +10%` は概ね `60d +15-20%`、`20d +20%` は概ね `60d +30%` に近い。したがって今後の決算前状態分類では、20d と 60d の threshold grid を別々に持ち、同一閾値の `runup` / `strong_runup` label だけで比較しない。

この readout は bundle `/private/tmp/trading25-research/market-behavior/pre-earnings-runup-threshold-response/20260516_pre_earnings_runup_threshold_response_v1` に基づく。入力 DB は `~/.local/share/trading25/market-timeseries/market.duckdb`、対象 event は `2016-04-01` から `2026-05-14`。

### Main Findings

#### 結論

20d と 60d は同一%でも母集団の希少性が違う。Prime FY では `20d +10%` が `3,489` 件、`60d +15%` が `3,450` 件で近く、Prime non-FY では `20d +10%` が `5,604` 件、`60d +20%` が `5,902` 件で近い。

| scope | threshold | FY events | non-FY events | FY post-entry 5d median excess | non-FY post-entry 5d median excess |
|---|---:|---:|---:|---:|---:|
| `20d >=` | `+10%` | `3,489` | `5,604` | `-0.64%` | `-0.27%` |
| `60d >=` | `+15%` | `3,450` | `9,151` | `-0.34%` | `-0.58%` |
| `60d >=` | `+20%` | `2,238` | `5,902` | `-0.44%` | `-0.62%` |
| `20d >=` | `+20%` | `802` | `955` | `-0.52%` | `-0.07%` |
| `60d >=` | `+30%` | `1,029` | `2,542` | `-0.87%` | `-0.58%` |

#### 結論

FY は runup が強いほど `EPS 1.2x positive` の発生率は上がるが、trade outcome は良くならない。特に 60d runup は EPS 1.2x proxy としては効く一方で、post-entry の left-tail risk も大きくなる。

| Prime FY threshold | events | EPS 1.2x eligible rate | limit-up no-fill | post-entry 5d median excess | post-entry 5d severe loss |
|---|---:|---:|---:|---:|---:|
| `20d >= +0%` | `14,257` | `17.34%` | `0.27%` | `-0.49%` | `2.86%` |
| `20d >= +10%` | `3,489` | `19.12%` | `0.49%` | `-0.64%` | `4.72%` |
| `20d >= +20%` | `802` | `20.00%` | `0.50%` | `-0.52%` | `8.76%` |
| `60d >= +10%` | `5,391` | `20.48%` | `0.46%` | `-0.35%` | `4.94%` |
| `60d >= +20%` | `2,238` | `24.29%` | `0.58%` | `-0.44%` | `7.93%` |
| `60d >= +30%` | `1,029` | `28.89%` | `0.97%` | `-0.87%` | `11.51%` |

#### 結論

Prime non-FY は FY よりまだましだが、median は大きく改善しない。高runup bucket は mean が上がりやすい一方で、severe loss rate も上がるため、右裾依存の性格が強い。

| Prime non-FY threshold | events | limit-up no-fill | post-entry 5d median excess | post-entry 5d mean excess | post-entry 5d severe loss |
|---|---:|---:|---:|---:|---:|
| `20d >= +0%` | `30,897` | `0.18%` | `-0.42%` | `-0.16%` | `2.45%` |
| `20d >= +10%` | `5,604` | `0.39%` | `-0.27%` | `+0.29%` | `4.21%` |
| `20d >= +20%` | `955` | `0.73%` | `-0.07%` | `+1.03%` | `6.77%` |
| `60d >= +20%` | `5,902` | `0.61%` | `-0.62%` | `-0.16%` | `6.07%` |
| `60d >= +30%` | `2,542` | `0.94%` | `-0.58%` | `-0.06%` | `8.72%` |
| `60d >= +40%` | `1,144` | `1.57%` | `-0.53%` | `+0.35%` | `10.87%` |

#### 結論

`rerating_participation` では、60d runup は EPS 1.2x proxy として強いが、post-entry のrisk/rewardは閾値を上げるほど悪化する。これは「良い決算が出やすい状態」と「買って儲かりやすい状態」が別物であることを示す。

| Prime FY rerating threshold | events | EPS 1.2x eligible rate | limit-up no-fill | post-entry 5d median excess | post-entry 5d severe loss |
|---|---:|---:|---:|---:|---:|
| `60d >= +0%` | `1,064` | `22.45%` | `0.75%` | `+0.09%` | `5.94%` |
| `60d >= +10%` | `682` | `26.33%` | `0.88%` | `+0.53%` | `7.37%` |
| `60d >= +20%` | `388` | `32.04%` | `1.29%` | `+0.09%` | `10.14%` |
| `60d >= +30%` | `239` | `38.66%` | `2.09%` | `-0.19%` | `14.22%` |
| `60d >= +50%` | `100` | `43.18%` | `5.00%` | `+0.02%` | `19.23%` |

#### Overheat Follow-Up

2026-05-16 の `overheat + adjusted valuation SoT` rerun では、`20d return >= +30%` を `overheat_state` として全表に追加した。`20d >= +30%` は定義上 `overheat` と一致し、Prime FY では post-entry 20d median `-2.34%`、hold-through 20d median `-2.39%`、どちらも severe loss が `27-28%` 近い。したがって、`20d >= +30%` は「短期過熱」として明確に避ける/サイズを落とす bucket でよい。

60d 側では、`60d >= +30%` 全体を一律に悪いと見るより、`overheat` 併発で大きく悪化する。Prime FY `60d >= +30%` かつ `overheat` は hold-through 20d median `-5.68%`、post-entry 20d median `-5.74%`、severe loss `36-37%`。一方で `not_overheat` は median の悪化が相対的に浅い。つまり「避けるべき 60d strong runup」は、単独閾値より `60d >= +30% AND 20d >= +30%` を強い risk guard として扱うのが自然。

| condition | FY | overheat state | events | EPS120 eligible rate | post-entry 20d median | post-entry 20d severe | hold-through 20d median | hold-through 20d severe |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `20d >= +30%` | true | `overheat` | `246` | `21.24%` | `-2.34%` | `26.92%` | `-2.39%` | `28.10%` |
| `20d >= +30%` | false | `overheat` | `259` | `NA` | `-0.74%` | `21.16%` | `-0.32%` | `27.87%` |
| `60d >= +30%` | true | `not_overheat` | `899` | `29.44%` | `-2.34%` | `20.93%` | `-1.10%` | `24.00%` |
| `60d >= +30%` | true | `overheat` | `130` | `25.00%` | `-5.74%` | `37.37%` | `-5.68%` | `36.63%` |
| `60d >= +30%` | false | `not_overheat` | `2,360` | `NA` | `-1.15%` | `18.08%` | `-1.15%` | `23.32%` |
| `60d >= +30%` | false | `overheat` | `182` | `NA` | `-2.13%` | `22.49%` | `-1.47%` | `29.07%` |

#### Forward P/OP Quality Filter

2026-05-17 follow-up で、EPS120 proxy側から `P/OP` / `forward P/OP` を統合パネルへ伝播した。`forward P/OP` は単独の低valuation指標としては `forward PER` を明確には超えないが、`low forward PER` の品質フィルタとして強い。特に `forward PER` が低いのに `forward P/OP` が高い状態は、EPSの低さが営業利益ではなくその他要因に寄っている疑いを示す。

FY event全体では、`forward_per <= 15` の post-entry 20d mean `-0.43%` / median `-1.21%` に対し、`forward_per <= 15 AND forward_p_op >= 20` は `141` eventsで mean `-2.49%` / median `-2.37%`、hold-through 20d mean `-2.62%` まで悪化した。

rerating participationかつoverheat除外に絞ると、`forward_per <= 20` は `171` events / post-entry 20d mean `+1.76%` / median `+0.93%` / hold-through 20d mean `+1.75%`。同じ範囲で `forward_p_op >= 20` を重ねると `6` eventsしか残らず、post-entry 20d mean `-2.74%` / median `-3.19%` / hold-through 20d mean `-8.04%`。母数は小さいためhard excludeには弱いが、低forward PERの営業利益品質を疑う警戒flagとして扱う価値がある。

### Interpretation

`20d +20%` は短期急騰で、希少性もriskもかなり高い。一方で `60d +20%` は中期上昇としてはまだ広めのbucketで、EPS 1.2x positive の事前proxyとしては効くが、決算後entryや決算またぎのmedianを改善するわけではない。

同じ `+20%` という名前で `strong_runup` と呼ぶと、20d は「短期過熱」、60d は「中期rerating途中」を混ぜてしまう。今後は absolute threshold と percentile bucket の両方を出し、年別/valuation regime 別の読み替えを標準にする。

### Production Implication

現時点では、`20d strong_runup` / `60d strong_runup` を同じ label としてproduction ruleに入れない。

| 用途 | 推奨 |
|---|---|
| 決算またぎ risk guard | `20d >= +30%` (`overheat`) は短期過熱として明確に警戒。`60d >= +30% AND 20d >= +30%` はさらに強い avoid bucket |
| EPS 1.2x proxy | `60d >= +20/+30%` はproxyとして有効。ただしtrade entryには直結しない |
| Low forward PER quality | `forward_p_op >= 20` や `forward_p_op / forward_per >= 1.5` は、low forward PERの営業利益品質が弱い警戒bucket |
| non-FY post-entry | `20d >= +10/+20%` はmean改善はあるがmedianは薄い。右裾依存として扱う |
| FY post-entry | runup強化でleft-tailが増えやすく、買い条件にはしない |
| 今後の標準表 | `threshold_response_df`、`joint_runup_response_df`、`percentile_response_df` を併記 |

### Caveats

- 閾値は最適化ではなく response curve として読む。今回の目的は、固定閾値の意味が20d/60dで同じかどうかの確認。
- return は `pre_event_date` close を基準にした発表前情報のみでbucket化している。event strength やEPS targetで事前にsliceしていない。
- post-entry return は寄らずのstop高/安を no-fill として分離し、executable event のreturnだけを集計している。
- `EPS 1.2x positive` はFY eventだけがeligibleなので、non-FYではrateを読まない。
- 60d高runup bucketは右裾によりmeanが改善する箇所がある。medianとsevere loss rateを併読する。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/pre_earnings_runup_threshold_response.py`
- runner: `apps/bt/scripts/research/run_pre_earnings_runup_threshold_response.py`
- bundle experiment id: `market-behavior/pre-earnings-runup-threshold-response`
- latest result bundle: `~/.local/share/trading25/research/market-behavior/pre-earnings-runup-threshold-response/20260516_overheat_adjusted_sot_v2`
- result tables: `event_feature_df`, `coverage_diagnostics_df`, `threshold_response_df`, `joint_runup_response_df`, `percentile_response_df`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_pre_earnings_runup_threshold_response.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260516_pre_earnings_runup_threshold_response_v1 \
  --min-events 100
```

## Artifact Tables

- `event_feature_df`: merged event panel from hold-through, post-entry, and EPS 1.2x proxy research.
- `coverage_diagnostics_df`: market / FY / liquidity scope coverage.
- `threshold_response_df`: single-window absolute threshold response for 20d and 60d, both runup and drawdown directions.
- `joint_runup_response_df`: joint `20d >= x` and `60d >= y` runup response grid.
- `percentile_response_df`: annual within-scope percentile bucket response for 20d and 60d returns.
