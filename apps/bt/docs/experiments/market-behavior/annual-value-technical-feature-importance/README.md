# Annual Value Technical Feature Importance

[`annual-value-composite-selection`](../annual-value-composite-selection/README.md)
の Standard / no-liquidity / top 10% value selection に対して、entry 前営業日時点で計算できる technical feature を横並びで比較する研究。`price > SMA250` のような単発 hard filter ではなく、どの technical family が銘柄選択に追加情報を持ちそうかを、bucket / residual correlation / walk-forward overlay で見る。

## Published Readout

### Decision

現時点で Ranking の value score に混ぜる第一候補は `price_to_sma250` ではない。Standard では、より優先して見るべき technical feature は `rebound_from_252d_low_pct`、`topix_volatility_60d_pct`、`return_20d_pct`、`volume_ratio_20_60`。ただし、今回の結果だけで hard filter 化はしない。Ranking では technical diagnostic として並べ、次の研究ではこれらを value score に対する additive score / risk overlay として検証する。

Prime に同じ結論をそのまま移植するのは危ない。追加 rerun では、Standard top 5% でも `rebound_from_252d_low_pct` と `topix_volatility_60d_pct` は残ったが、Prime top 10% / top 5% では効果がかなり弱くなった。したがって technical overlay は market-specific に扱う。

`fixed_55_25_20` は今回の focus score methods から明示的に除外した。readout は `equal_weight` と `walkforward_regression_weight` の共通性を重視する。

### Why This Research Was Run

前回の `SMA250` hard filter 研究では、`price > SMA250` も `price < SMA250` も baseline より弱く、さらに `2017` の `SMA250 missing` が DB 左端 warmup 問題だと分かった。次に知りたいのは「SMA250 だけを見るのではなく、どの technical indicator family が annual value selection の銘柄選択に効いていそうか」なので、複数 feature を同じ PIT entry 前営業日基準で比較した。

### Data Scope / PIT Assumptions

入力は positive-ratio value bundle `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`。分析対象は `standard` / `none` / top `10%` / `equal_weight`, `walkforward_regression_weight` の `1,658` selected events。technical feature は parent `market.duckdb` から selected event code の日足を読み、`entry_date` より前の最新 trading session だけで計算する。entry 当日以降の価格・出来高は使わない。

`2017` の DB 左端 warmup と short-history rows は削除せず、`history_class` として残す。`SMA250` 系の coverage は `1,457 / 1,658` rows。

### Main Findings

#### 結論

| Feature | Family | Avg importance | Avg high-low mean | Avg high-low p10 | 読み |
| --- | --- | ---: | ---: | ---: | --- |
| `topix_volatility_60d_pct` | market_regime | `12.27` | `-19.04pp` | `-1.98pp` | TOPIX 60d volatility が低い年初 regime が強い |
| `volatility_60d_pct` | volatility | `12.06` | `15.27pp` | `-5.39pp` | event mean は強いが左尾は悪化しやすく、向きは慎重に扱う |
| `rebound_from_252d_low_pct` | reversal | `11.53` | `12.98pp` | `-6.66pp` | 252日安値から一定以上戻っている銘柄が平均 return で強い |
| `downside_volatility_60d_pct` | volatility | `10.91` | `14.87pp` | `-1.11pp` | static bucket では強いが、risk feature としては要検証 |
| `return_252d_pct` | momentum | `9.24` | `9.04pp` | `-4.25pp` | 長期 momentum は平均では情報があるが左尾改善は弱い |
| `price_to_sma250` | trend | `8.47` | `5.13pp` | `-2.46pp` | SMA250 は上位ではあるが、主役にするほど安定しない |
| `volume_ratio_20_60` | volume | `8.45` | `8.73pp` | `6.49pp` | 出来高の短期/中期比は平均と p10 の両方で比較的素直 |

#### Walk-forward Overlay

過去年だけで best bucket を選び、対象年に適用する簡易 walk-forward overlay では、下表の feature が両 score method で平均 return を押し上げた。`kept` は対象年 events のうち残した割合で、おおむね 20% bucket の比較。

| Feature | Selected side | Avg delta mean | Avg delta p10 | Avg delta worst | Avg kept | 読み |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `rebound_from_252d_low_pct` | high / high | `+9.82pp` | `-0.66pp` | `+6.80pp` | `18.9%` | 平均 return は最も強いが、p10 は改善しない |
| `topix_volatility_60d_pct` | low / low | `+7.55pp` | `+4.74pp` | `+5.36pp` | `20.5%` | mean と left-tail の両方が改善し、regime feature として有望 |
| `return_20d_pct` | low / low | `+5.89pp` | `-1.51pp` | `+0.00pp` | `20.4%` | 短期 run-up を避ける reversal 方向。ただし左尾改善は弱い |
| `downside_volatility_60d_pct` | high / high | `+4.42pp` | `-5.01pp` | `+8.17pp` | `19.8%` | 平均は強いが p10 が悪化し、hard include には向かない |
| `volatility_60d_pct` | low / low | `+3.72pp` | `+1.88pp` | `+4.18pp` | `19.8%` | walk-forward では低ボラ側が安定寄り |
| `volume_ratio_20_60` | high / high | `+1.86pp` | `-1.58pp` | `+0.00pp` | `19.6%` | 補助 feature 候補。単独主役では弱い |

#### 弱い / 主役にしない feature

| Feature | Avg delta mean | Avg delta p10 | 読み |
| --- | ---: | ---: | --- |
| `price_to_sma250` | `-0.80pp` | `+2.41pp` | 左尾には少し効くが、平均を削る。前回の hard filter 結論と整合 |
| `price_to_sma50` | `-0.87pp` | `+2.74pp` | trend 水準そのものは主役にしにくい |
| `risk_adjusted_return_60d` | `-3.72pp` | `-1.91pp` | annual value selection ではこのまま使う優先度は低い |
| `rsi_14` | `-0.81pp` | `-4.29pp` | score method 間で向きが割れ、left-tail も悪い |

#### Market / Top 5% Sensitivity

同じ runner を `prime` と top `5%` に rerun した。入力 bundle には既に `prime` / `standard` と `0.05` / `0.10` の selected events が含まれていたため、upstream selection は再作成していない。

| Scenario | Selected events | SMA250 feature rows |
| --- | ---: | ---: |
| `standard_top10` | `1,658` | `1,457` |
| `standard_top5` | `836` | `744` |
| `prime_top10` | `2,874` | `2,598` |
| `prime_top5` | `1,442` | `1,304` |

Standard では top `5%` でも主要結論はかなり残った。`rebound_from_252d_low_pct` は top `10%` で avg delta mean `+9.82pp`、top `5%` で `+8.73pp`。`topix_volatility_60d_pct` も top `10%` で `+7.55pp`、top `5%` で `+5.52pp`。top `5%` では `drawdown_from_252d_high_pct` low side も `+7.75pp` と強く見えたが、これは `range_position_252d` よりも high から遠い depressed value 側の効果として別途確認が必要。

| Feature | Standard top10 mean | Standard top5 mean | Standard top10 p10 | Standard top5 p10 |
| --- | ---: | ---: | ---: | ---: |
| `rebound_from_252d_low_pct` | `+9.82pp` | `+8.73pp` | `-0.66pp` | `+1.94pp` |
| `topix_volatility_60d_pct` | `+7.55pp` | `+5.52pp` | `+4.74pp` | `+4.00pp` |
| `return_20d_pct` | `+5.89pp` | `+0.48pp` | `-1.51pp` | `-0.75pp` |
| `volume_ratio_20_60` | `+1.86pp` | `-0.29pp` | `-1.58pp` | `-7.67pp` |
| `price_to_sma250` | `-0.80pp` | `+0.66pp` | `+2.41pp` | `+1.85pp` |
| `range_position_252d` | `-1.88pp` | `-2.85pp` | `-0.22pp` | `+3.34pp` |

Prime では同じ feature はかなり弱い。top `10%` の `rebound_from_252d_low_pct` は avg delta mean `+1.58pp` に留まり、top `5%` では `-1.01pp`。`return_252d_pct` も Prime では top `10%` / top `5%` の両方でマイナスだった。Prime value は Standard value と同じ「反転進捗を強く足す」設計にはしない方がよい。

| Feature | Prime top10 mean | Prime top5 mean | Prime top10 p10 | Prime top5 p10 |
| --- | ---: | ---: | ---: | ---: |
| `topix_volatility_60d_pct` | `+2.46pp` | `-1.10pp` | `+1.19pp` | `+1.21pp` |
| `rebound_from_252d_low_pct` | `+1.58pp` | `-1.01pp` | `-3.54pp` | `-6.66pp` |
| `return_20d_pct` | `-0.79pp` | `+0.35pp` | `-0.63pp` | `-0.88pp` |
| `volume_ratio_20_60` | `+0.13pp` | `-1.00pp` | `-1.32pp` | `+0.43pp` |
| `return_252d_pct` | `-0.76pp` | `-2.78pp` | `-2.35pp` | `-4.70pp` |
| `range_position_252d` | `-1.84pp` | `+0.14pp` | `+0.53pp` | `+0.02pp` |

### Interpretation

今回の一番の知見は、`price_to_sma250` よりも「どれだけ安値から戻っているか」「市場が荒れていないか」「短期的に走りすぎていないか」の方が、Standard value top decile の銘柄選択補助として読みやすいこと。これは `SMA250` の上か下かという二値 trend 判定ではなく、value selection の中で「反転が始まっているが過熱しすぎていない」銘柄を選ぶ問題に近い。

この見方は Standard に強く、Prime では弱い。Prime は銘柄数が多く流動性も高い一方、低PBR・低forward PER・小型 composite の性格が Standard と違うため、同じ technical rebound feature が右尾捕捉として働きにくい可能性がある。

`topix_volatility_60d_pct` は market-wide regime なので、銘柄選択 factor というより risk overlay / capital allocation の候補として扱うべき。`rebound_from_252d_low_pct` は stock-specific feature として一番強いが、p10 は改善しないため、単独で左尾を削る feature ではない。

Stock volatility family は static importance では上位に来る一方、walk-forward での向きや p10 が安定しない。これは high-vol value rebound の右尾を拾っている可能性があり、hard filter にすると portfolio risk を上げる恐れがある。

### Production Implication

Ranking に入れるなら、Standard profile では次の順で diagnostic / score candidate として扱う。

1. `topix_volatility_60d_pct`: market regime 表示、または value score の risk-on/off overlay。
2. `rebound_from_252d_low_pct`: value stock の反転進捗 feature。
3. `return_20d_pct`: 短期 run-up 回避の reversal feature。
4. `volume_ratio_20_60`: 補助的な participation / attention feature。

次の research は、これらを単独 hard filter ではなく、`equal_weight` / `walkforward_regression_weight` value score に対する additive technical score として検証する。特に `topix_volatility_60d_pct` は銘柄 rank ではなく年初 exposure weight として別枠で見る。Prime profile には Standard の rebound overlay をそのまま入れず、Prime 専用に別 feature set を探す。

### Caveats

年次 rebalance 研究なので、年中の signal 更新、約定コスト、position sizing、capacity は未反映。`topix_*` feature は同一年内の cross-section では全銘柄同じ値になるため、residual correlation ではなく年別 regime bucket と walk-forward overlay で読む。`SMA250` 系 feature は 2017 DB 左端 warmup により欠損が出るため、coverage と `history_class` を必ず併記する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_value_technical_feature_importance.py`
- Runner: `apps/bt/scripts/research/run_annual_value_technical_feature_importance.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_technical_feature_importance_standard_positive_v2/`
- Results DB: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_technical_feature_importance_standard_positive_v2/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_technical_feature_importance_standard_positive_v2/summary.md`
- Standard top5 bundle: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_technical_standard_top5/`
- Prime top10 bundle: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_technical_prime_top10/`
- Prime top5 bundle: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_technical_prime_top5/`

## Current Surface

- Input bundle: `annual-value-composite-selection`
- Output tables:
  - `enriched_event_df`
  - `feature_bucket_summary_df`
  - `feature_importance_df`
  - `conditional_importance_df`
  - `walkforward_overlay_df`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_technical_feature_importance.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive \
  --output-root /tmp/trading25-research \
  --run-id 20260502_value_technical_feature_importance_standard_positive_v2
```
