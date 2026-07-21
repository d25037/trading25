# Pre-Disclosure Flow/Volatility

## Published Readout

> [!WARNING]
> **Status: `historical_archive`; `rerun_required`.** This Market v3 evidence
> is retained only as a historical candidate. It must not drive production,
> thresholds, or Ranking decisions before a physical Market v5
> `market.duckdb` rerun with
> `stock_price_adjustment_mode=provider_adjusted_v1`, signal-date PIT
> membership, and provider-vintage/current-basis provenance.

### Decision

`pre_disclosure_flow_volatility` は、公開済みの `statements` 開示イベントに対して、開示日前の価格・出来高・ATR がどの程度「先に動いていたか」を観察した historical Phase 1 readout として保存する。Market v3 の今回の結果は、Market v5 rerun 後に検証する候補であり、「高い事前 flow/volatility score を一律に除外すべき」という結論ではない。

全期間 run では、高 `informed_flow_score` bucket は平均 excess return を押し上げる傾向があり、特に `growth` で強かった。一方で median は多くの bucket でマイナスのままで、`standard` と `prime` では severe-loss rate も高 score 側でやや悪化した。従って、この指標は単独 alpha や bad-tail prune の根拠ではなく、Market v5 で再検証する **市場別の event participation / risk-state classifier 候補** として扱う。

この historical readout は bundle `/tmp/trading25-research/market-behavior/pre-disclosure-flow-volatility/published_readout_pre_disclosure_flow_volatility` に基づく。`market_source` は `stock_master_daily_as_of_disclosed_date` だったが、Market v5 `provider_adjusted_v1` contract を満たす rerun を要する。

### Main Findings

**Historical measurement only:** the following Market v3 measurements preserve
their prior dates and provenance, but must not drive production, thresholds, or
Ranking decisions before the required Market v5 `provider_adjusted_v1` rerun.

#### 結論

| 観点 | 結果 | 判断 |
|---|---:|---|
| 対象期間 | 2016-04-01 to 2026-05-01 | v3 `market.duckdb` の historical measurement。Market v5 rerun が必要 |
| 全イベント数 | 155,802 events / 3,729 codes | sample size は十分 |
| `prime` | 80,881 events / 2,048 codes | 高 score は平均改善するが、左尾率も少し悪化 |
| `standard` | 55,397 events / 1,655 codes | 高 score は平均改善、左尾率悪化が最も読みやすい |
| `growth` | 17,996 events / 807 codes | 高 score の平均改善が最大。ただし中央値はマイナスで高ボラ色が強い |
| 実務判断 | historical candidate | pooled `all` は sanity check。いずれの bucket も Market v5 rerun 前の production 判断には使わない |

#### Score Bucket

`informed_flow_score` は `directional_pre_abret_pct`、`pre_volume_z`、`pre_atr_z` の market-scope rank 合成。下表は high bucket と low bucket の 20-session forward excess return 比較。

| market | high events | low events | high mean % | low mean % | high-low pp | high median % | low median % | high severe % | low severe % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| all | 30,757 | 30,756 | 0.787 | -0.031 | 0.818 | -1.099 | -1.119 | 16.26 | 15.18 |
| prime | 16,125 | 16,124 | 0.529 | 0.095 | 0.434 | -0.692 | -0.717 | 13.78 | 13.14 |
| standard | 11,040 | 11,040 | 0.570 | -0.126 | 0.696 | -1.504 | -1.507 | 16.18 | 13.74 |
| growth | 3,592 | 3,592 | 2.375 | -0.534 | 2.909 | -1.854 | -3.170 | 28.28 | 28.48 |

高 score は「出尽くしで弱い」よりも、平均ではむしろ強い。ただし `standard` は high bucket の severe-loss rate が low bucket より `+2.44pp` 高く、平均改善と左尾悪化が同居している。`growth` は平均 spread が `+2.909pp` と大きいが、high bucket でも median は `-1.854%`、severe-loss rate は `28.28%` なので、上方外れ値を含む高分散 bucket と読むべき。

#### ATR x Volume

20-session horizon で、ATR と出来高の交差を見ると、市場ごとの意味がかなり違う。

| market | ATR | volume | events | mean excess % | median excess % | severe % |
|---|---|---|---:|---:|---:|---:|
| prime | high | high | 14,743 | 0.664 | -0.583 | 13.41 |
| prime | low | low | 13,507 | 0.024 | -0.833 | 14.22 |
| standard | high | high | 11,178 | 0.338 | -1.605 | 16.28 |
| standard | low | high | 2,326 | 1.011 | -0.522 | 9.00 |
| standard | low | low | 10,164 | -0.328 | -1.725 | 15.49 |
| growth | high | high | 3,949 | 1.841 | -2.196 | 28.90 |
| growth | low | low | 3,459 | -0.758 | -3.529 | 30.14 |

`prime` では high ATR x high volume は悪くなく、20d mean は `+0.664%`、severe は low-low より低い。`standard` では high-high も平均は改善するが、より注目すべきは low ATR x high volume で、20d mean `+1.011%`、severe `9.00%` と最もきれい。これは「荒れた噂」よりも「静かな参加増」の方が質が高い可能性を示す。`growth` は high-high の平均が強いが severe は約 `29%` と高く、単純な買い条件にはしにくい。

#### Signed Pre-Move

良悪イベント方向と pre move の一致だけでは、素直な signal にならない。

| market | horizon | direction | pre move | events | mean excess % | median excess % | severe % |
|---|---:|---|---|---:|---:|---:|---:|
| prime | 20 | negative | aligned | 8,865 | -2.331 | -2.979 | 19.34 |
| prime | 20 | negative | opposed | 7,256 | -2.580 | -3.230 | 20.92 |
| prime | 20 | positive | aligned | 12,060 | 2.193 | 0.840 | 11.17 |
| prime | 20 | positive | opposed | 12,430 | 2.016 | 0.676 | 9.89 |
| standard | 20 | negative | aligned | 5,420 | -2.256 | -2.872 | 18.60 |
| standard | 20 | negative | opposed | 4,078 | -3.530 | -4.605 | 25.75 |
| standard | 20 | positive | aligned | 6,134 | 1.886 | -0.879 | 14.80 |
| standard | 20 | positive | opposed | 6,746 | 2.352 | -0.134 | 9.89 |
| growth | 20 | negative | aligned | 2,487 | -2.945 | -4.886 | 34.32 |
| growth | 20 | negative | opposed | 1,517 | -3.079 | -6.207 | 38.74 |
| growth | 20 | positive | aligned | 1,958 | 3.304 | -1.649 | 27.08 |
| growth | 20 | positive | opposed | 2,506 | 3.131 | -0.587 | 22.41 |

negative event は事前に下げていても、下げていなくても弱い。特に `standard` の negative opposed は 20d mean `-3.530%`、median `-4.605%`、severe `25.75%` で、悪材料の事前織り込みがなかった銘柄ほど開示後に崩れやすい可能性がある。

positive event は pre move aligned が必ず良いわけではない。`standard` と `growth` では positive opposed の方が severe-loss rate が低く、pre-runup 済みの positive event はやや混雑した状態として扱うべき。

### Interpretation

この historical Phase 1 で一番重要だったのは、`informed_flow_score` が「怪しいから避ける」指標ではなかったこと。高 score は event participation の強さを拾い、平均 return にはプラスに出た。ただし、平均が良い一方で median が弱く、severe-loss rate が落ちないため、単独 entry signal の候補としても Market v5 での再検証が必要である。

Historical run では ATR を入れた差が観察された。出来高だけでは high participation を全部同じに見てしまうが、`standard` の low ATR x high volume が相対的にきれいで、high ATR x high volume は平均改善と左尾悪化が同居した。これは「静かな参加増」と「荒れたイベント賭け」を分ける軸として ATR が効くという、Market v5 rerun の仮説である。

市場別に分けた historical observation では、`growth` は high score の平均 spread が大きいが severe-loss rate が常に高く、pooled で読むと「高 score が強い」という雑な結論になりやすかった。`standard` と `prime` に関する記述も、Market v5 rerun の仮説であって current risk classifier / sizing decision ではない。

### Production Implication

以下は Market v5 rerun で検証する候補であり、現時点で production feature、threshold、Ranking decision として使わない。

| 用途 | 候補 |
|---|---|
| `standard` negative event risk | negative event x opposed pre move を開示後の避けるべき状態として検討 |
| `standard` event participation | low ATR x high volume を quality participation 候補として次段階で検証 |
| `prime` high score | hard exclude ではなく sizing cap / volatility-aware risk cap |
| `growth` high score | 平均は強いが severe が高いため、単独採用不可。portfolio lens が必須 |

Market v5 の次段階では、`event_direction` の粗い前回比ではなく、document type と forecast revision を厳密化し、positive revision / negative revision / 決算本体 / 配当修正を分ける。そのうえで `standard` の low ATR x high volume と negative opposed の2系統を portfolio lens で再検証する。

### Caveats

- `statements` は開示時刻を持たないため、開示日当日足を pre feature から除外している。
- `event_direction` は Phase 1 の代理変数で、`forecast_eps` / `next_year_forecast_earnings_per_share` / `profit` の前回比に依存する。文書種別ごとの revision semantics はまだ粗い。
- high score bucket は上方外れ値と左尾リスクを同時に含むため、平均 return だけで判断しない。
- `unknown` market は PIT master coverage の外側を含むため、主判断から外す。
- この research は違法行為の識別ではなく、公開データ上の事前織り込み・需給・リスク状態の定量化である。

### Source Artifacts

- module: `apps/bt/src/domains/analytics/pre_disclosure_flow_volatility.py`
- runner: `apps/bt/scripts/research/run_pre_disclosure_flow_volatility.py`
- bundle experiment id: `market-behavior/pre-disclosure-flow-volatility`
- historical bundle: `/tmp/trading25-research/market-behavior/pre-disclosure-flow-volatility/published_readout_pre_disclosure_flow_volatility`
- result tables: `event_feature_df`, `market_sample_diagnostics_df`, `market_score_bucket_forward_return_df`, `market_atr_volume_interaction_df`, `market_signed_move_df`
